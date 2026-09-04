from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_task_reconcile import (  # noqa: E402
    classify_task,
    observe_tasks,
    scan_task_directory,
    stage_first_batch_snapshot,
    summarize,
)


class TaskReconcileTests(unittest.TestCase):
    def test_classifies_old_non_terminal_as_stale_without_mutation(self) -> None:
        now = 2_000_000.0
        item = classify_task({"task_id": "t1", "resource_id": "viking://resources/t1", "task_type": "add_resource", "status": "running", "created_at": now - 7200}, now=now, stale_after_seconds=3600)
        self.assertEqual(item["classification"], "stale")
        self.assertEqual(item["reason"], "non_terminal_age_seconds=7200")

    def test_failed_tasks_are_quarantined_and_missing_resources_are_orphans(self) -> None:
        failed = classify_task({"task_id": "failed", "resource_id": "viking://resources/x", "status": "failed"})
        orphan = classify_task({"task_id": "orphan", "status": "completed"})
        missing = classify_task({"task_id": "missing", "resource_id": "viking://resources/x", "status": "completed"}, resource_exists=False)
        self.assertEqual(failed["classification"], "quarantine")
        self.assertEqual(failed["reason"], "terminal_failure")
        self.assertEqual(orphan["classification"], "orphan")
        self.assertEqual(missing["classification"], "orphan")

    def test_scan_and_observe_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tasks = root / "tasks"
            tasks.mkdir()
            (tasks / "old.json").write_text(json.dumps({"task_id": "old", "resource_id": "viking://resources/old", "status": "running", "created_at": time.time() - 7200}), encoding="utf-8")
            (tasks / "done.json").write_text(json.dumps({"task_id": "done", "resource_id": "viking://resources/done", "status": "completed"}), encoding="utf-8")
            (tasks / "bad.json").write_text("not-json", encoding="utf-8")
            observations = scan_task_directory(tasks, now=time.time(), stale_after_seconds=3600)
            summary = summarize(observations)
            self.assertEqual(summary["files"], 3)
            self.assertEqual(summary["by_classification"], {"invalid": 1, "stale": 1, "terminal": 1})
            store = PMSystemStore(root / "pm-system.db")
            first = observe_tasks(store, observations, observed_at="2026-08-29T00:00:00Z")
            second = observe_tasks(store, observations, observed_at="2026-08-29T00:01:00Z")
            self.assertEqual(first, second)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM external_task_observations").fetchone()[0], 3)

    def test_first_batch_snapshot_is_staged_and_legacy_hashes_stay_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = {
                "doc-1": {"source": "databuilder-internal", "doc_guid": "doc-1", "target_uri": "viking://doc-1", "publishTime": "2026-02-01", "sha256": "abc", "sha256_mode": "markdown_without_sync_metadata"},
                "doc-2": {"source": "databuilder-internal", "doc_guid": "doc-2", "target_uri": "viking://doc-2", "publishTime": "2026-02-02", "sha256": "d" * 64, "sha256_mode": "content", "sha256_verified_at": "2026-08-28T22:00:00Z", "sha256_verified_by": "content_sha256"},
            }
            store = PMSystemStore(root / "pm-system.db")
            result = stage_first_batch_snapshot(store, ledger, source="databuilder-internal", captured_at="2026-08-29T00:00:00Z")
            self.assertEqual(result["selected"], 2)
            self.assertEqual(result["unknown_items"], 1)
            self.assertEqual(result["snapshot"]["status"], "committed")
            with store.connect() as connection:
                statuses = [row[0] for row in connection.execute("SELECT status FROM source_items ORDER BY resource_id")]
            self.assertEqual(statuses, ["unknown", "verified"])

    def test_first_batch_snapshot_does_not_trust_valid_legacy_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = {
                "legacy": {
                    "source": "databuilder-internal",
                    "doc_guid": "legacy",
                    "target_uri": "viking://legacy",
                    "publishTime": "2026-02-01",
                    "sha256": "a" * 64,
                    "sha256_mode": "markdown_without_sync_metadata",
                }
            }
            store = PMSystemStore(root / "pm-system.db")
            result = stage_first_batch_snapshot(store, ledger, source="databuilder-internal", captured_at="2026-08-29T00:00:00Z")
            self.assertEqual(result["unknown_items"], 1)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM source_items").fetchone()[0], "unknown")


if __name__ == "__main__":
    unittest.main()
