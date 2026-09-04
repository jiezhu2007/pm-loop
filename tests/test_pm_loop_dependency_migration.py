from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_dependency_migration import migrate  # noqa: E402
from pm_loop_scheduler_migration import migrate as migrate_v8  # noqa: E402
from pm_system_store import PMSystemStore, SCHEMA_VERSION, StoreUnavailable  # noqa: E402


class DependencyMigrationTests(unittest.TestCase):
    def test_v10_to_v11_creates_verified_backup_and_releases_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db, max_schema_version=10)
            result = migrate(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")
            self.assertEqual(result["before_schema"], 10)
            self.assertEqual(result["after_schema"], SCHEMA_VERSION)
            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["verification"]["valid"])
            self.assertTrue(Path(result["backup"]["path"]).is_file())
            self.assertEqual(PMSystemStore(db, auto_migrate=False).migration_freeze()["state"], "released")

    def test_active_work_refuses_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db, max_schema_version=10)
            store.accept({"job_type": "fixture", "loop_id": "fixture", "idempotency_key": "active"})
            with self.assertRaises(StoreUnavailable):
                migrate(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")

    def test_interrupted_history_does_not_block_v11_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db, max_schema_version=10)
            accepted = store.accept({"job_type": "fixture", "loop_id": "fixture", "idempotency_key": "interrupted"})
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='interrupted' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='interrupted' WHERE run_id=?", (accepted["run_id"],))
            result = migrate(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")
            self.assertEqual(result["status"], "completed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "interrupted")

    def test_interrupted_history_does_not_block_legacy_v8_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db, max_schema_version=8)
            accepted = store.accept({"job_type": "fixture", "loop_id": "fixture", "idempotency_key": "legacy-interrupted"})
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='interrupted' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='interrupted' WHERE run_id=?", (accepted["run_id"],))
            result = migrate_v8(db_path=db, backup_root=root / "backups", manifest_path=root / "manifest.json")
            self.assertEqual(result["status"], "completed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "interrupted")


if __name__ == "__main__":
    unittest.main()
