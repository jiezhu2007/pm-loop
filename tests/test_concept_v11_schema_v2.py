import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_schema import migrate_schema, record_model_policy  # noqa: E402
from concept_v11_schema_v2 import (  # noqa: E402
    TARGET_SCHEMA_VERSION,
    admission_is_live,
    bind_profile_policy,
    migrate_schema_v2,
    profile_accept_v2,
    record_model_resolution_append,
    schema_v2_state,
    set_admission_cas,
)
from pm_system_store import PMSystemStore  # noqa: E402


class ConceptSchemaV2Tests(unittest.TestCase):
    def _store(self):
        root = Path(tempfile.mkdtemp(prefix="concept-v2-test-"))
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
            migration_id="concept-v1",
            stage_id="C-SCHEMA",
            migration_epoch="v45-test",
            owner="test-owner",
        )
        migrate_schema(store, migration_id="concept-v1", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        return root, store

    def test_v2_rebuilds_hot_key_and_marks_legacy_rows(self):
        root, store = self._store()
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO concept_versions(version_id,concept_id,namespace_epoch,version,generation_id,content,content_hash,compiler_version,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("v1", "c1", "v45-test", "legacy", "legacy-import-v45-test", "body", "h1", "legacy-import", "active", "2026-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO concept_publish_ledger(publish_id,concept_id,namespace_epoch,version_id,current_generation,desired_hot_generation,projection_state,operator,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("p1", "c1", "v45-test", "v1", "legacy-import-v45-test", "legacy-import-v45-test", "applied", "test", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO concept_hot_projection(concept_id,namespace_epoch,generation_id,projection_state,observed_content_hash,updated_at) VALUES(?,?,?,?,?,?)",
                ("c1", "v45-test", "legacy-import-v45-test", "applied", "h1", "2026-01-01T00:00:00Z"),
            )
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        result = migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        self.assertEqual(result["after"]["schema_version"], TARGET_SCHEMA_VERSION)
        self.assertTrue(result["after"]["hot_projection_composite_key"])
        with store.connect() as connection:
            columns = {str(row[1]): int(row[5]) for row in connection.execute("PRAGMA table_info(concept_hot_projection)")}
            self.assertEqual(columns["concept_id"], 1)
            self.assertEqual(columns["namespace_epoch"], 2)
            self.assertEqual(tuple(connection.execute("SELECT provenance,projection_state FROM concept_hot_projection WHERE concept_id='c1'").fetchone()), ("legacy_import", "legacy_imported"))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_admission_events").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT pending_soft_limit FROM concept_profile_admissions").fetchone()[0], 2)

        second = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        rerun = migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=second["lease_id"])
        store.release_migration_lease(lease_id=second["lease_id"])
        self.assertFalse(rerun["changed"]["hot_projection_rebuilt"])
        self.assertEqual(rerun["after"]["legacy_provenance_rows"]["concept_hot_projection"], 1)

    def test_admission_cas_and_profile_soft_limit(self):
        _, store = self._store()
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        changed = set_admission_cas(
            store,
            namespace_epoch="v45-test",
            expected_state="disabled",
            expected_version=1,
            state="shadow",
            snapshot_id="snap-1",
            policy_version="policy-1",
            operator="test",
            evidence_hash="hash-1",
        )
        self.assertEqual(changed["admission_state"], "shadow")
        self.assertEqual(changed["version"], 2)
        with self.assertRaises(RuntimeError):
            set_admission_cas(
                store,
                namespace_epoch="v45-test",
                expected_state="disabled",
                expected_version=1,
                state="canary",
                snapshot_id="snap-2",
                policy_version="policy-1",
                operator="test",
                evidence_hash="hash-2",
            )
        first = profile_accept_v2(store, workload="concept-semantic", profile="pm-semantic", namespace_epoch="v45-test")
        second = profile_accept_v2(store, workload="concept-semantic", profile="pm-semantic", namespace_epoch="v45-test")
        third = profile_accept_v2(store, workload="concept-semantic", profile="pm-semantic", namespace_epoch="v45-test")
        self.assertTrue(first["accepted"])
        self.assertTrue(second["accepted"])
        self.assertFalse(third["accepted"])
        self.assertEqual(third["reason"], "soft_limit")

    def test_incremental_continuous_admission_has_no_expiry(self):
        _, store = self._store()
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        canary = set_admission_cas(
            store,
            namespace_epoch="v45-test",
            expected_state="disabled",
            expected_version=1,
            state="canary",
            snapshot_id="snap-canary",
            policy_version="policy-1",
            operator="test",
            evidence_hash="hash-canary",
        )
        continuous = set_admission_cas(
            store,
            namespace_epoch="v45-test",
            expected_state="canary",
            expected_version=int(canary["version"]),
            state="incremental",
            snapshot_id="snap-incremental",
            policy_version="policy-1",
            operator="test",
            evidence_hash="hash-incremental",
        )
        self.assertEqual(continuous["renewal_policy"], "continuous")
        self.assertIsNone(continuous["expires_at"])
        self.assertTrue(admission_is_live(continuous, at="2099-01-01T00:00:00Z"))
        with store.connect() as connection:
            event = connection.execute(
                "SELECT renewal_policy FROM concept_admission_events WHERE to_state='incremental'"
            ).fetchone()
        self.assertEqual(event[0], "continuous")
        with self.assertRaisesRegex(ValueError, "only valid for incremental"):
            set_admission_cas(
                store,
                namespace_epoch="v45-test",
                expected_state="incremental",
                expected_version=int(continuous["version"]),
                state="canary",
                snapshot_id="snap-invalid",
                policy_version="policy-1",
                operator="test",
                evidence_hash="hash-invalid",
                renewal_policy="continuous",
            )

    def test_profile_policy_binding_is_explicit_and_idempotent(self):
        _, store = self._store()
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        policy = record_model_policy(store, {"policy_version": "policy-auto", "provider": "oneapi", "requested_model": "auto", "allowed_models": [], "status": "active"})
        first = bind_profile_policy(store, namespace_epoch="v45-test", policy_version=policy["policy_version"])
        second = bind_profile_policy(store, namespace_epoch="v45-test", policy_version=policy["policy_version"])
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        with store.connect() as connection:
            self.assertEqual(connection.execute("SELECT policy_hash FROM concept_profile_admissions").fetchone()[0], policy["policy_hash"])

    def test_model_resolution_replay_is_append_only(self):
        _, store = self._store()
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        payload = {
            "resolution_id": "resolution-1",
            "run_id": "run-1",
            "call_id": "call-1",
            "stage": "provider-shadow",
            "attempt": 1,
            "model_requested": "auto",
            "model_resolved": "gpt-test",
            "resolution_status": "resolved",
            "policy_version": "policy-1",
            "provider": "oneapi",
            "resolution_changed": 0,
            "model_input_hash": "input-1",
            "evidence_hash": "evidence-1",
        }
        first = record_model_resolution_append(store, payload)
        second = record_model_resolution_append(store, payload)
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        altered = dict(payload, model_resolved="gpt-other")
        with self.assertRaises(RuntimeError):
            record_model_resolution_append(store, altered)


if __name__ == "__main__":
    unittest.main()
