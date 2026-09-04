from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_resource_archive_repair import repair  # noqa: E402
from pm_system_gateway import SemanticGateway  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class FakeTransport:
    def __init__(self) -> None:
        self.uploads: list[Path] = []
        self.submissions: list[tuple[dict, str]] = []

    def upload_file(self, path: Path, *, timeout: float):
        self.uploads.append(path)
        return {"result": {"temp_file_id": "temp-repair-1"}}

    def add_resource(self, body, *, timeout: float, idempotency_key: str):
        self.submissions.append((dict(body), idempotency_key))
        return {"status": "completed", "task_id": "ov-repair-1"}


class ArchiveRepairTests(unittest.TestCase):
    def test_repairs_failed_row_without_mutating_history_or_leaking_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            artifact = root / "report.md"
            artifact.write_text("durable report\n", encoding="utf-8")
            revision = hashlib.sha256(artifact.read_bytes()).hexdigest()
            store = PMSystemStore(root / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="viking://resources/project-docs/report.md",
                revision_id=revision,
                processing_mode="vectors_only",
                provider="openviking",
                profile="fast-vector",
                payload={
                    "file_path": str(artifact),
                    "target_uri": "viking://resources/project-docs/report.md",
                    "audit_reason": "local-only",
                },
            )
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE outbox_items SET status='failed',attempt=2,error_fingerprint='old-failure' WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                )

            transport = FakeTransport()
            report = repair(
                db_path=root / "pm-system.db",
                artifact_root=root / "resource-outbox",
                transport=transport,
                include_dead_letter=False,
            )

            self.assertEqual(report["selected"], 1)
            self.assertEqual(report["repaired"], 1)
            self.assertTrue(report["historical_rows_unchanged"])
            self.assertEqual(transport.uploads, [artifact])
            self.assertEqual(len(transport.submissions), 1)
            body, key = transport.submissions[0]
            self.assertEqual(key, accepted["idempotency_key"])
            self.assertNotIn("reason", json.dumps(body, ensure_ascii=False))
            self.assertEqual(body["to"], "viking://resources/project-docs/report.md")

            with store.connect() as connection:
                row = connection.execute(
                    "SELECT status,attempt,error_fingerprint FROM outbox_items WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
            self.assertEqual(tuple(row), ("failed", 2, "old-failure"))


if __name__ == "__main__":
    unittest.main()
