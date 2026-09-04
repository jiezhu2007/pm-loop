from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_scheduler_migration import migrate  # noqa: E402
from pm_system_store import PMSystemStore, StoreUnavailable  # noqa: E402


class SchedulerMigrationTests(unittest.TestCase):
    def test_migrates_v7_to_v8_with_consistent_backup_and_releases_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db, max_schema_version=7)
            result = migrate(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")
            self.assertEqual(result["before_schema"], 7)
            self.assertEqual(result["after_schema"], 8)
            self.assertEqual(result["status"], "completed")
            self.assertEqual(PMSystemStore(db, auto_migrate=False).schema_version(), 8)
            self.assertEqual(PMSystemStore(db, auto_migrate=False).migration_freeze()["state"], "released")
            self.assertTrue(Path(result["backup"]["path"]).is_file())
            self.assertTrue((root / "manifest.json").is_file())

    def test_active_job_blocks_without_allow_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db, max_schema_version=7)
            connection = sqlite3.connect(db)
            connection.execute(
                "INSERT INTO jobs(job_id,idempotency_key,job_type,run_id,status,priority,profile,payload_json,attempt,queued_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("job-active", "active-key", "test", "run-active", "queued", 50, "report", "{}", 0, "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z"),
            )
            connection.commit()
            connection.close()
            with self.assertRaises(StoreUnavailable):
                migrate(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")


if __name__ == "__main__":
    unittest.main()
