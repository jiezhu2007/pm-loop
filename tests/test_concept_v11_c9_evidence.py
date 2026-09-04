from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_c9_evidence import promote  # noqa: E402
from concept_v11_schema import migrate_schema, record_model_policy  # noqa: E402
from concept_v11_schema_v2 import migrate_schema_v2  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class ConceptV11C9EvidenceTests(unittest.TestCase):
    def _fixture(self):
        root = Path(tempfile.mkdtemp(prefix="concept-c9-evidence-"))
        store = PMSystemStore(root / "pm-system.db")
        store.set_migration_freeze(
            migration_id="v45-test",
            migration_epoch="v45-test",
            stage_id="G9",
            owner="test",
            deadline_at="2099-01-01T00:00:00Z",
            state="released",
        )
        lease = store.acquire_migration_lease(
            migration_id="concept-v1", stage_id="C-SCHEMA", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema(store, migration_id="concept-v1", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        record_model_policy(
            store,
            {
                "policy_version": "concept-v11-oneapi-auto-v1",
                "provider": "oneapi",
                "requested_model": "auto",
                "allowed_models": [],
                "capability_class": "concept-compiler-and-semantic",
                "privacy_scope": "local-private",
                "latency_limit_seconds": 900,
            },
        )
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        report = {
            "schema": "concept-v11.c6-provider-shadow.v1",
            "stage_id": "C6-PROVIDER-SHADOW",
            "observed_at": "2026-08-31T00:40:39Z",
            "status": "PASS_WITH_UNKNOWN_MODEL",
            "read_only_pm_database": True,
            "concept_admission_changed": False,
            "target_uri": "viking://resources/__pm_v11_provider_shadow__/test/sample",
            "namespace_isolated": True,
            "processing_mode": "semantic_and_vectors",
            "wait": False,
            "model_requested": "auto",
            "model_resolved": None,
            "model_resolution_status": "unknown",
            "source_hash": "sha256:" + "a" * 64,
            "read_back_hash": "sha256:" + "a" * 64,
            "errors": [],
            "external_provider_calls": 1,
            "accepted_latency_ms": 10.0,
            "semantic_latency_ms": 20.0,
            "task_id": "task-c9-test",
            "accepted": True,
            "remote_status": "completed",
            "semantic_projection": "completed",
            "content_verified": True,
            "queue_status": {"Semantic": {"processed": 1, "requeue_count": 0, "error_count": 0}},
            "task_terminal_response": {"status": "completed"},
        }
        report_path = root / "c6-report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return root, store, report_path

    def test_apply_and_idempotent_replay(self):
        root, store, report = self._fixture()
        backup_root = root / "backups"
        dry_run = promote(store.db_path, report, apply=False)
        self.assertEqual(dry_run["status"], "DRY_RUN")
        self.assertEqual(dry_run["model_resolution_gate"], "provider_configuration_trusted")
        self.assertEqual(dry_run["model_resolution_gate_status"], "not_required")
        with store.connect() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_capability_probes").fetchone()[0], 0)
        applied = promote(store.db_path, report, apply=True, backup_root=backup_root)
        self.assertEqual(applied["status"], "PASS")
        self.assertTrue(applied["backup"]["verified"])
        self.assertEqual(applied["db_writes"], {"concept_capability_probes": 2, "concept_model_resolutions": 1})
        replay = promote(store.db_path, report, apply=True, backup_root=backup_root)
        self.assertEqual(replay["status"], "PASS")
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(replay["db_writes"], {"concept_capability_probes": 0, "concept_model_resolutions": 0})
        with store.connect() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_capability_probes").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_model_resolutions").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT admission_state FROM concept_admissions").fetchone()[0], "disabled")

    def test_conflicting_existing_evidence_is_quarantined(self):
        root, store, report = self._fixture()
        backup_root = root / "backups"
        first = promote(store.db_path, report, apply=True, backup_root=backup_root)
        probe_id = first["probe_ids"][0]
        with store.transaction() as connection:
            connection.execute("UPDATE concept_capability_probes SET task_id='different-task' WHERE probe_id=?", (probe_id,))
        conflict = promote(store.db_path, report, apply=True, backup_root=backup_root)
        self.assertEqual(conflict["status"], "QUARANTINED")
        self.assertEqual(len(conflict["conflicts"]), 1)
        with store.connect() as connection:
            self.assertEqual(connection.execute("SELECT task_id FROM concept_capability_probes WHERE probe_id=?", (probe_id,)).fetchone()[0], "different-task")

    def test_model_must_remain_auto_and_unknown(self):
        root, store, report = self._fixture()
        payload = json.loads(report.read_text(encoding="utf-8"))
        payload["model_requested"] = "gpt-fixed"
        report.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(ValueError):
            promote(store.db_path, report, apply=False)


if __name__ == "__main__":
    unittest.main()
