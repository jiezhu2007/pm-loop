from __future__ import annotations

import sqlite3
import tempfile
import unittest
import json
import io
from contextlib import redirect_stdout
from pathlib import Path
import sys
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_migration import (  # noqa: E402
    _copy_backup,
    _heartbeat_snapshot,
    _runtime_hash_snapshot,
    _safe_component,
    main,
)
from concept_v11_runtime_migration import _core_schema_error, _snapshot  # noqa: E402
from pm_system_store import PMSystemStore, SCHEMA_VERSION  # noqa: E402


class ConceptV11MigrationTests(unittest.TestCase):
    def test_runtime_hash_snapshot_detects_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-runtime-hash-") as temp:
            root = Path(temp)
            canonical = root / "canonical"
            runtime = root / "runtime"
            for name in ("pm_system_gateway.py", "pm_resource_dispatcher.py", "pm_system_worker.py"):
                (canonical / "scripts").mkdir(parents=True, exist_ok=True)
                (runtime / "scripts").mkdir(parents=True, exist_ok=True)
                (canonical / "scripts" / name).write_text("same", encoding="utf-8")
                (runtime / "scripts" / name).write_text("same", encoding="utf-8")
            self.assertEqual(_runtime_hash_snapshot(canonical_root=canonical, runtime_root=runtime)["status"], "PASS")
            (runtime / "scripts" / "pm_system_worker.py").write_text("drift", encoding="utf-8")
            result = _runtime_hash_snapshot(canonical_root=canonical, runtime_root=runtime)
            self.assertEqual(result["status"], "HOLD")
            self.assertIn("runtime_drift:pm_system_worker.py", result["errors"])

    def test_heartbeat_snapshot_is_read_only_and_reports_staleness(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-heartbeat-") as temp:
            path = Path(temp) / "latest.json"
            path.write_text(json.dumps({"run_at": "2026-08-31 00:00:00", "checker_errors": []}), encoding="utf-8")
            result = _heartbeat_snapshot(path)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["run_at"], "2026-08-31 00:00:00")
            self.assertTrue(path.is_file())

    def test_stage_backups_are_immutable_and_independently_addressable(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-backup-") as temp:
            root = Path(temp)
            db_path = root / "pm-system.db"
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, schema_id TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (7, 'pm.v45', 'hash', '2026-08-31T00:00:00Z')"
                )
                connection.commit()

            first = _copy_backup(
                db_path,
                root / "backups",
                "concept-v11-test",
                stage_id="C-SCHEMA",
                migration_epoch="v45-test",
            )
            second = _copy_backup(
                db_path,
                root / "backups",
                "concept-v11-test",
                stage_id="C-LEGACY-IMPORT",
                migration_epoch="v45-test",
            )

            self.assertNotEqual(first["path"], second["path"])
            self.assertTrue(Path(first["path"]).is_file())
            self.assertTrue(Path(second["path"]).is_file())
            self.assertEqual(first["stage_id"], "C-SCHEMA")
            self.assertEqual(second["stage_id"], "C-LEGACY-IMPORT")
            self.assertEqual(first["migration_epoch"], "v45-test")
            self.assertEqual(first["integrity_check"], "ok")
            self.assertEqual(second["integrity_check"], "ok")
            self.assertEqual(first["core_schema_version"], 7)
            self.assertEqual(second["core_schema_version"], 7)

    def test_backup_path_components_cannot_escape_backup_root(self) -> None:
        self.assertEqual(_safe_component("../C-SCHEMA"), "C-SCHEMA")
        self.assertEqual(_safe_component("concept/v11"), "concept_v11")

    def test_shared_runtime_snapshot_is_safe_before_concept_schema_exists(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-snapshot-") as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            with mock.patch("concept_v11_runtime_migration._process_snapshot", return_value={}):
                snapshot = _snapshot(store)
            self.assertEqual(snapshot["integrity_check"], "ok")
            self.assertEqual(snapshot["core_schema_version"], SCHEMA_VERSION)
            self.assertIsNone(snapshot["concept_schema"])
            self.assertIsNone(snapshot["concept_admission"])

    def test_shared_runtime_accepts_v10_and_v11_without_implicit_schema_upgrade(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-schema-compat-") as temp:
            db_path = Path(temp) / "pm-system.db"
            PMSystemStore(db_path, max_schema_version=10)
            store = PMSystemStore(db_path, auto_migrate=False)
            with mock.patch("concept_v11_runtime_migration._process_snapshot", return_value={}):
                self.assertEqual(_snapshot(store)["core_schema_version"], 10)
            self.assertIsNone(_core_schema_error(10))
            self.assertIsNone(_core_schema_error(SCHEMA_VERSION))
            self.assertEqual(_core_schema_error(9), "core_schema_too_old:9")
            self.assertEqual(_core_schema_error(SCHEMA_VERSION + 1), f"core_schema_future:{SCHEMA_VERSION + 1}")

    def test_success_report_records_released_stage_lease(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concept-v11-runner-") as temp:
            root = Path(temp)
            db_path = root / "pm-system.db"
            store = PMSystemStore(db_path)
            store.set_migration_freeze(
                migration_id="v45-test",
                migration_epoch="v45-test",
                stage_id="G9",
                owner="test",
                deadline_at="2099-01-01T00:00:00Z",
                state="released",
            )
            report_path = root / "schema.json"
            output = io.StringIO()
            with (
                redirect_stdout(output),
                mock.patch("concept_v11_migration._runtime_hash_snapshot", return_value={"status": "PASS", "files": {}, "errors": []}),
                mock.patch("concept_v11_migration._runtime_process_snapshot", return_value={"orphan_processes": []}),
                mock.patch("concept_v11_migration._heartbeat_snapshot", return_value={"status": "PASS", "errors": []}),
            ):
                result = main(
                    [
                        "schema",
                        "--apply",
                        "--db-path",
                        str(db_path),
                        "--backup-root",
                        str(root / "backups"),
                        "--migration-id",
                        "concept-v11-test",
                        "--migration-epoch",
                        "v45-test",
                        "--owner",
                        "test-owner",
                        "--report",
                        str(report_path),
                    ]
                )
            self.assertEqual(result, 0)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["stage_lease"]["state"], "released")
            self.assertIn("released_at", payload["stage_lease"])


if __name__ == "__main__":
    unittest.main()
