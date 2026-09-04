from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_dependency_replay import PRODUCTION_TABLES, run_replay  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class DependencyReplayTests(unittest.TestCase):
    def test_replay_uses_worker_and_scheduler_without_production_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex = root / "codex"
            for path, value in (
                (codex / "skills" / "shengsuan-sync" / "state" / "ledger.json", {"sync": {"uri": "viking://resources/shengsuan/a", "doc_guid": "sync"}}),
                (codex / "skills" / "databuilder-public-docs" / "state" / "ledger.json", {"public": {"uri": "viking://resources/shengsuan/b", "doc_guid": "public"}}),
                (codex / "skills" / "shengsuan-concepts" / "state" / "concepts-ledger.json", {"concept-a": {"status": "active", "sources": ["viking://resources/shengsuan/a"]}}),
            ):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value), encoding="utf-8")

            db = root / "pm-system.db"
            store = PMSystemStore(db)
            with store.transaction() as connection:
                connection.execute("CREATE TABLE concept_admissions (namespace_epoch TEXT PRIMARY KEY, admission_state TEXT NOT NULL, version INTEGER NOT NULL, updated_at TEXT NOT NULL)")
                connection.execute("CREATE TABLE concept_versions (concept_id TEXT NOT NULL, namespace_epoch TEXT NOT NULL)")
                connection.execute("CREATE TABLE concept_source_map (concept_id TEXT NOT NULL, namespace_epoch TEXT NOT NULL, status TEXT NOT NULL)")
                connection.execute("INSERT INTO concept_admissions VALUES(?,?,?,?)", ("fixture", "disabled", 7, "2026-09-02T00:00:00Z"))
                connection.execute("INSERT INTO concept_versions VALUES(?,?)", ("concept-a", "fixture"))
                connection.execute("INSERT INTO concept_source_map VALUES(?,?,?)", ("concept-a", "fixture", "mapped"))

            import pm_dependency_replay

            original_root = pm_dependency_replay.CODEX_ROOT
            pm_dependency_replay.CODEX_ROOT = codex
            try:
                result = run_replay(
                    db_path=db,
                    registry_path=ROOT / "scripts" / "schedule-registry.json",
                    runtime_registry_path=ROOT / "scripts" / "schedule-registry.json",
                    canonical_registry_path=ROOT / "scripts" / "schedule-registry.json",
                    lock_path=root / "dispatcher.lock",
                    artifact_root=root / "runs",
                    replay_id="p9fixture01",
                )
            finally:
                pm_dependency_replay.CODEX_ROOT = original_root

            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["success"]["event_status"], "consumed")
            self.assertEqual(result["success"]["planner_status"], "completed")
            self.assertEqual(result["hash_mismatch"]["event_status"], "blocked_by_upstream")
            self.assertEqual(result["failed_upstream"]["event_status"], "blocked_by_upstream")
            self.assertEqual(result["production_table_counts"]["before"], result["production_table_counts"]["after"])
            self.assertEqual(set(result["production_table_counts"]["before"]), set(PRODUCTION_TABLES))


if __name__ == "__main__":
    unittest.main()
