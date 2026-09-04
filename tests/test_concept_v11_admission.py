from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_admission import run_admission  # noqa: E402
from concept_v11_schema import migrate_schema, record_model_policy  # noqa: E402
from concept_v11_schema_v2 import bind_profile_policy, migrate_schema_v2  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class ConceptAdmissionRunnerTests(unittest.TestCase):
    def _setup(self, root: Path) -> tuple[PMSystemStore, Path]:
        db = root / "pm-system.db"
        store = PMSystemStore(db)
        store.set_migration_freeze(
            migration_id="v45-test",
            migration_epoch="v45-test",
            stage_id="G9",
            owner="test",
            deadline_at="2099-01-01T00:00:00Z",
            state="released",
        )
        lease = store.acquire_migration_lease(migration_id="v1", stage_id="C1", migration_epoch="v45-test", owner="test")
        migrate_schema(store, migration_id="v1", migration_epoch="v45-test", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        lease = store.acquire_migration_lease(migration_id="v2", stage_id="C2", migration_epoch="v45-test", owner="test")
        migrate_schema_v2(store, migration_id="v2", migration_epoch="v45-test", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        record_model_policy(
            store,
            {"policy_version": "concept-v11-oneapi-auto-v1", "provider": "oneapi", "requested_model": "auto", "allowed_models": [], "status": "active"},
        )
        bind_profile_policy(store, namespace_epoch="v45-test")
        with store.connect() as connection:
            admission = dict(connection.execute("SELECT * FROM concept_admissions WHERE namespace_epoch='v45-test'").fetchone())
        now = datetime.now(timezone.utc)
        snapshot = {
            "schema": "concept-v11.bootstrap-admission-preflight.v1",
            "status": "PASS",
            "read_only": True,
            "concept_admission_changed": False,
            "namespace_epoch": "v45-test",
            "admission_snapshot_id": "bootstrap-test-1",
            "evidence_hash": "sha256:test-evidence",
            "observed_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "expires_at": (now + timedelta(minutes=15)).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "current_admission": admission,
        }
        snapshot_path = root / "snapshot.json"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        return store, snapshot_path

    def test_default_is_dry_run_and_backup_restore_is_verified(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, snapshot = self._setup(root)
            result = run_admission(store.db_path, snapshot, namespace_epoch="v45-test", backup_root=root / "backups")
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertTrue(result["backup"]["verified"])
            self.assertEqual(result["before"]["admission"]["admission_state"], "disabled")
            self.assertEqual(store.migration_freeze()["state"], "released")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT admission_state,version FROM concept_admissions").fetchone()[:], ("disabled", 1))
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_admission_events").fetchone()[0], 1)

    def test_apply_is_one_cas_transition_and_replay_of_old_snapshot_is_held(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, snapshot = self._setup(root)
            applied = run_admission(store.db_path, snapshot, namespace_epoch="v45-test", target_state="canary", operator="test-operator", backup_root=root / "backups", apply=True)
            self.assertEqual(applied["status"], "PASS")
            self.assertEqual(applied["after"]["admission"]["admission_state"], "canary")
            self.assertEqual(applied["after"]["admission"]["version"], 2)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_admission_events").fetchone()[0], 2)
            replay = run_admission(store.db_path, snapshot, namespace_epoch="v45-test", target_state="incremental", operator="test-operator", backup_root=root / "backups", apply=True)
            self.assertEqual(replay["status"], "HOLD")
            self.assertTrue(any("admission_snapshot_state_mismatch" in error for error in replay["errors"]))
            self.assertEqual(replay["before"]["admission"]["admission_state"], "canary")

    def test_incremental_snapshot_ttl_converts_once_to_continuous(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, snapshot = self._setup(root)
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE concept_admissions SET admission_state='incremental', expires_at='2099-01-01T00:00:00Z', renewal_policy='snapshot_ttl', version=2 WHERE namespace_epoch='v45-test'"
                )
            with store.connect() as connection:
                current = dict(connection.execute("SELECT * FROM concept_admissions WHERE namespace_epoch='v45-test'").fetchone())
            payload = json.loads(snapshot.read_text(encoding="utf-8"))
            payload["admission_snapshot_id"] = "bootstrap-test-continuous"
            payload["current_admission"] = current
            snapshot.write_text(json.dumps(payload), encoding="utf-8")
            applied = run_admission(
                store.db_path,
                snapshot,
                namespace_epoch="v45-test",
                target_state="incremental",
                renewal_policy="continuous",
                operator="test-operator",
                backup_root=root / "backups",
                apply=True,
            )
            self.assertEqual(applied["status"], "PASS")
            self.assertEqual(applied["after"]["admission"]["version"], 3)
            self.assertEqual(applied["after"]["admission"]["renewal_policy"], "continuous")
            self.assertIsNone(applied["after"]["admission"]["expires_at"])
            replay = run_admission(
                store.db_path,
                snapshot,
                namespace_epoch="v45-test",
                target_state="incremental",
                renewal_policy="continuous",
                operator="test-operator",
                backup_root=root / "backups",
                apply=True,
            )
            self.assertEqual(replay["status"], "HOLD")
            self.assertIn("incremental_continuous_already_active", replay["errors"])


if __name__ == "__main__":
    unittest.main()
