from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_schema import migrate_schema, record_model_policy, record_probe  # noqa: E402
from concept_v11_schema_v2 import bind_profile_policy, migrate_schema_v2, set_admission_cas  # noqa: E402
from pm_resource_dispatcher import PMResourceDispatcher  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class FakeTransport:
    def __init__(self, *, readback: bool = True, response=None):
        self.url = "http://fake-openviking"
        self.response = response or {"status": "accepted", "task_id": "task-1"}
        self.readback = readback
        self.tasks = {}
        self.calls = []
        self.add_bodies = []

    def upload_file(self, path, *, timeout=None):
        self.calls.append(("upload", Path(path).name))
        return {"result": {"temp_file_id": "temp-1"}}

    def add_resource(self, body, *, timeout=None, idempotency_key=None):
        self.calls.append(("add", body.get("processing_mode"), body.get("to")))
        self.add_bodies.append(dict(body))
        return self.response

    def read_content(self, target_uri, *, timeout=None):
        self.calls.append(("read", target_uri))
        if not self.readback:
            return {"status": "ok", "result": {"content": "different"}}
        return {"status": "ok", "result": {"content_hash": self.expected_hash}}

    def get_task(self, task_id, *, timeout=None):
        return self.tasks.get(str(task_id), {"status": "running"})


class SharedRuntimeV11Tests(unittest.TestCase):
    def _concept_ready(self, root: Path, store: PMSystemStore) -> None:
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
        lease = store.acquire_migration_lease(
            migration_id="concept-v2", stage_id="C-SCHEMA-V2", migration_epoch="v45-test", owner="test-owner"
        )
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test-owner", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        policy = record_model_policy(
            store,
            {
                "policy_version": "concept-v11-oneapi-auto-v1",
                "provider": "oneapi",
                "requested_model": "auto",
                "allowed_models": [],
                "capability_class": "concept-compiler-and-semantic",
                "privacy_scope": "local-private",
                "status": "active",
            },
        )
        bind_profile_policy(
            store,
            namespace_epoch="v45-test",
            policy_version=policy["policy_version"],
        )
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(hours=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        set_admission_cas(
            store,
            namespace_epoch="v45-test",
            expected_state="disabled",
            expected_version=1,
            state="canary",
            snapshot_id="snapshot-1",
            policy_version=policy["policy_version"],
            operator="test",
            evidence_hash="evidence-1",
        )
        for mode in ("semantic_only", "semantic_and_vectors"):
            record_probe(
                store,
                {
                    "probe_id": f"probe-{mode}",
                    "probe_type": "client_accept_probe",
                    "namespace_epoch": "v45-test",
                    "profile": "pm-semantic",
                    "processing_mode": mode,
                    "provider": "oneapi",
                    "model_policy_version": policy["policy_version"],
                    "capability_state": "ready",
                    "observed_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "expires_at": expires,
                },
            )

    def test_concept_requires_admission_and_probe_then_reads_content_independently(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "concept.md"
            source.write_text("concept body", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            with self.assertRaisesRegex(RuntimeError, "concept admission blocked"):
                dispatcher.enqueue_concept_file(path=source, target_uri="viking://resources/concepts/c1", processing_mode="semantic_only", namespace_epoch="v45-test")
            self._concept_ready(root, store)
            revision = hashlib.sha256(source.read_bytes()).hexdigest()
            transport.expected_hash = revision
            accepted = dispatcher.enqueue_concept_file(
                path=source,
                target_uri="viking://resources/concepts/c1",
                processing_mode="semantic_only",
                namespace_epoch="v45-test",
            )
            replay = dispatcher.enqueue_concept_file(
                path=source,
                target_uri="viking://resources/concepts/c1",
                processing_mode="semantic_only",
                namespace_epoch="v45-test",
            )
            self.assertFalse(accepted["deduplicated"])
            self.assertTrue(replay["deduplicated"])
            with store.connect() as connection:
                row = connection.execute("SELECT kind,processing_mode,payload_json FROM outbox_items").fetchone()
                self.assertEqual(tuple(row[:2]), ("concept", "semantic_only"))
                payload = __import__("json").loads(row[2])
                self.assertEqual(payload["model_requested"], "auto")
                self.assertEqual(payload["model_policy_version"], "concept-v11-oneapi-auto-v1")
                self.assertTrue(payload["policy_bound"])
                self.assertEqual(connection.execute("SELECT pending_count FROM concept_profile_admissions").fetchone()[0], 1)
            result = dispatcher.dispatch_pending(limit=1)
            self.assertTrue(result[0]["content_read_back"]["verified"])
            self.assertNotIn("model_requested", transport.add_bodies[0])
            self.assertNotIn("model_policy_version", transport.add_bodies[0])
            self.assertNotIn("policy_hash", transport.add_bodies[0])
            with store.connect() as connection:
                projection = connection.execute("SELECT content_state,semantic_state FROM resource_projections").fetchone()
                self.assertEqual(tuple(projection), ("content_verified", "semantic_pending"))
                self.assertEqual(connection.execute("SELECT pending_count FROM concept_profile_admissions").fetchone()[0], 1)
            transport.tasks["task-1"] = {"status": "completed"}
            dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            with store.connect() as connection:
                projection = connection.execute("SELECT content_state,semantic_state FROM resource_projections").fetchone()
                self.assertEqual(tuple(projection), ("content_verified", "semantic_completed"))
                self.assertEqual(connection.execute("SELECT pending_count FROM concept_profile_admissions").fetchone()[0], 0)
            dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT pending_count FROM concept_profile_admissions").fetchone()[0], 0)

    def test_semantic_completion_does_not_promote_concept_content_without_readback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "concept.md"
            source.write_text("concept body", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            self._concept_ready(root, store)
            transport = FakeTransport(readback=False)
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            accepted = dispatcher.enqueue_concept_file(path=source, target_uri="viking://resources/concepts/c2", processing_mode="semantic_only", namespace_epoch="v45-test")
            dispatcher.dispatch_pending(limit=1)
            transport.tasks["task-1"] = {"status": "completed"}
            dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            with store.connect() as connection:
                self.assertEqual(
                    tuple(connection.execute("SELECT content_state,semantic_state FROM resource_projections").fetchone()),
                    ("content_pending", "semantic_completed"),
                )
            self.assertEqual(accepted["status"], "accepted")

    def test_concept_policy_payload_cannot_override_active_policy(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            self._concept_ready(root, store)
            with self.assertRaisesRegex(RuntimeError, "policy_override:model_requested"):
                PMResourceDispatcher(store, transport=FakeTransport(), artifact_root=root / "artifacts").gateway.enqueue(
                    resource_id="viking://resources/concepts/override",
                    revision_id="rev-1",
                    processing_mode="semantic_only",
                    provider="oneapi",
                    profile="pm-semantic",
                    namespace_epoch="v45-test",
                    kind="concept",
                    payload={"model_requested": "some-explicit-model"},
                )


if __name__ == "__main__":
    unittest.main()
