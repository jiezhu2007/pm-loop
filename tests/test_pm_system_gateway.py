from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_gateway import SemanticGateway, _parse_retry_after, provider_key  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from concept_v11_schema import migrate_schema, record_model_policy  # noqa: E402
from concept_v11_schema_v2 import bind_profile_policy, migrate_schema_v2  # noqa: E402


class GatewayTests(unittest.TestCase):
    @staticmethod
    def _concept_store(root: Path) -> PMSystemStore:
        store = PMSystemStore(root / "concept.db")
        store.set_migration_freeze(
            migration_id="gateway-test",
            migration_epoch="epoch-1",
            stage_id="test",
            owner="test",
            deadline_at="2099-01-01T00:00:00Z",
            state="released",
        )
        lease = store.acquire_migration_lease(migration_id="concept-v1", stage_id="C1", migration_epoch="epoch-1", owner="test")
        migrate_schema(store, migration_id="concept-v1", migration_epoch="epoch-1", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        lease = store.acquire_migration_lease(migration_id="concept-v2", stage_id="C2", migration_epoch="epoch-1", owner="test")
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="epoch-1", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        record_model_policy(store, {"policy_version": "concept-v11-oneapi-auto-v1", "provider": "oneapi", "requested_model": "auto", "allowed_models": [], "status": "active"})
        bind_profile_policy(store, namespace_epoch="epoch-1")
        with store.transaction() as connection:
            connection.execute(
                "UPDATE concept_admissions SET admission_state='canary',expires_at='2099-01-01T00:00:00Z',updated_at='2026-09-03T00:00:00Z' WHERE namespace_epoch='epoch-1'"
            )
        return store

    def test_vectors_only_concept_binds_local_policy_without_semantic_probe_or_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._concept_store(Path(temp))
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="viking://resources/concepts/__canary__/concept.md",
                revision_id="revision-1",
                kind="concept",
                processing_mode="vectors_only",
                provider="oneapi",
                endpoint="http://127.0.0.1:1933",
                model="resource-api",
                profile="pm-semantic",
                namespace_epoch="epoch-1",
            )
            with store.connect() as connection:
                payload = json.loads(connection.execute("SELECT payload_json FROM outbox_items WHERE outbox_id=?", (accepted["outbox_id"],)).fetchone()[0])
                profile_hash = connection.execute("SELECT policy_hash FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile='pm-semantic' AND namespace_epoch='epoch-1'").fetchone()[0]
                pending = connection.execute("SELECT pending_count FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile='pm-semantic' AND namespace_epoch='epoch-1'").fetchone()[0]
            self.assertEqual(payload["policy_hash"], profile_hash)
            self.assertTrue(payload["vectors_only_policy_bound"])
            self.assertNotIn("model_policy_version", payload)
            self.assertEqual(pending, 0)
            self.assertEqual([item["outbox_id"] for item in gateway.dispatch_once()], [accepted["outbox_id"]])

    def test_vectors_only_policy_repair_is_bounded_and_dispatchable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._concept_store(Path(temp))
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="viking://resources/concepts/__canary__/repair.md",
                revision_id="revision-1",
                kind="concept",
                processing_mode="vectors_only",
                provider="oneapi",
                endpoint="http://127.0.0.1:1933",
                model="resource-api",
                profile="pm-semantic",
                namespace_epoch="epoch-1",
            )
            with store.transaction() as connection:
                payload = json.loads(connection.execute("SELECT payload_json FROM outbox_items WHERE outbox_id=?", (accepted["outbox_id"],)).fetchone()[0])
                payload.pop("policy_hash")
                payload.pop("vectors_only_policy_bound")
                payload.pop("vectors_only_policy_bound_at")
                connection.execute("UPDATE outbox_items SET payload_json=? WHERE outbox_id=?", (json.dumps(payload), accepted["outbox_id"]))
            repaired = gateway.repair_vectors_only_concept_policy([accepted["outbox_id"]])
            self.assertEqual([item["outbox_id"] for item in repaired], [accepted["outbox_id"]])
            self.assertEqual([item["outbox_id"] for item in gateway.dispatch_once()], [accepted["outbox_id"]])

    def test_continuous_incremental_concept_enqueue_and_dispatch_ignore_expired_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._concept_store(Path(temp))
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE concept_admissions SET admission_state='incremental',expires_at=NULL,renewal_policy='continuous' WHERE namespace_epoch='epoch-1'"
                )
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="viking://resources/concepts/candidates/continuous.md",
                revision_id="revision-continuous",
                kind="concept",
                processing_mode="vectors_only",
                provider="oneapi",
                endpoint="http://127.0.0.1:1933",
                model="resource-api",
                profile="pm-semantic",
                namespace_epoch="epoch-1",
            )
            self.assertEqual([item["outbox_id"] for item in gateway.dispatch_once()], [accepted["outbox_id"]])
    def test_enqueue_is_idempotent_and_records_explicit_profile_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            args = {
                "resource_id": "doc-1",
                "revision_id": "rev-1",
                "processing_mode": "semantic_and_vectors",
                "provider": "oneapi",
                "profile": "pm-semantic",
                "payload": {"title": "fixture"},
                "endpoint": "https://oneapi/v1",
                "model": "gpt-5.6-sol",
            }
            first = gateway.enqueue(**args)
            second = gateway.enqueue(**args)
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(first["outbox_id"], second["outbox_id"])
            with store.connect() as connection:
                row = connection.execute("SELECT processing_mode,profile,payload_json FROM outbox_items").fetchone()
            self.assertEqual(row[0:2], ("semantic_and_vectors", "pm-semantic"))
            self.assertEqual(__import__("json").loads(row[2])["wait"], False)

    def test_pm_payload_rejects_non_empty_openviking_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            with self.assertRaisesRegex(ValueError, "must not set OpenViking reason"):
                gateway.enqueue(
                    resource_id="doc-1",
                    revision_id="rev-1",
                    processing_mode="vectors_only",
                    provider="oneapi",
                    profile="fast-vector",
                    payload={"reason": "archive audit note"},
                )

    def test_pm_payload_drops_empty_reason_compatibility_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="doc-1",
                revision_id="rev-1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="fast-vector",
                payload={"reason": "   ", "audit_reason": "local-only"},
            )
            with store.connect() as connection:
                payload = __import__("json").loads(
                    connection.execute(
                        "SELECT payload_json FROM outbox_items WHERE outbox_id=?",
                        (accepted["outbox_id"],),
                    ).fetchone()[0]
                )
            self.assertNotIn("reason", payload)
            self.assertEqual(payload["audit_reason"], "local-only")

    def test_pm_payload_rejects_nested_openviking_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            with self.assertRaisesRegex(ValueError, "must not set OpenViking reason"):
                gateway.enqueue(
                    resource_id="doc-1",
                    revision_id="rev-1",
                    processing_mode="vectors_only",
                    provider="oneapi",
                    profile="fast-vector",
                    payload={"metadata": {"reason": "nested audit note"}},
                )

    def test_pm_payload_drops_nested_empty_reason_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="doc-1",
                revision_id="rev-1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="fast-vector",
                payload={"metadata": {"reason": ""}, "items": [{"reason": None, "keep": True}]},
            )
            with store.connect() as connection:
                payload = __import__("json").loads(
                    connection.execute(
                        "SELECT payload_json FROM outbox_items WHERE outbox_id=?",
                        (accepted["outbox_id"],),
                    ).fetchone()[0]
                )
            self.assertNotIn("reason", __import__("json").dumps(payload))
            self.assertEqual(payload["items"], [{"keep": True}])

    def test_dispatch_creates_one_semantic_task_and_fast_vector_is_independent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            first = gateway.enqueue(resource_id="doc", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
            gateway.enqueue(resource_id="doc2", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            dispatched = gateway.dispatch_once()
            self.assertEqual(len(dispatched), 2)
            self.assertEqual(len(gateway.dispatch_once()), 0)
            with store.connect() as connection:
                count = connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0]
            self.assertEqual(count, 2)
            self.assertEqual(gateway.ack(first["outbox_id"], openviking_task_id="ov-1"), True)

    def test_dispatch_can_be_restricted_to_explicit_outbox_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            selected = gateway.enqueue(
                resource_id="selected",
                revision_id="r1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="pm-resource",
            )
            other = gateway.enqueue(
                resource_id="other",
                revision_id="r1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="pm-resource",
            )
            dispatched = gateway.dispatch_once(limit=10, lane="fast-vector", outbox_ids=[selected["outbox_id"]])
            self.assertEqual([item["outbox_id"] for item in dispatched], [selected["outbox_id"]])
            with store.connect() as connection:
                status = connection.execute("SELECT status FROM outbox_items WHERE outbox_id=?", (other["outbox_id"],)).fetchone()[0]
            self.assertEqual(status, "pending")

    def test_dispatch_recovers_in_flight_row_missing_its_lease(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="orphaned-dispatch",
                revision_id="r1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="pm-resource",
            )
            self.assertEqual(len(gateway.dispatch_once()), 1)
            with store.transaction() as connection:
                connection.execute(
                    "DELETE FROM outbox_dispatch_leases WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                )
            recovered = gateway.dispatch_once(outbox_ids=[accepted["outbox_id"]])
            self.assertEqual([row["outbox_id"] for row in recovered], [accepted["outbox_id"]])
            with store.connect() as connection:
                outbox = connection.execute(
                    "SELECT status,error_fingerprint FROM outbox_items WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
                lease = connection.execute(
                    "SELECT outbox_id FROM outbox_dispatch_leases WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
                task_count = connection.execute(
                    "SELECT COUNT(*) FROM semantic_tasks WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()[0]
            self.assertEqual(outbox[0], "in_flight")
            self.assertTrue(outbox[1])
            self.assertIsNotNone(lease)
            self.assertEqual(task_count, 1)

    def test_429_seconds_does_not_increment_attempt_or_requeue_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, circuit_threshold=5)
            key = provider_key("oneapi", "endpoint", "model")
            accepted = gateway.enqueue(resource_id="doc", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic", endpoint="endpoint", model="model")
            gateway.dispatch_once()
            result = gateway.fail(accepted["outbox_id"], category="429", retry_after="60", provider_key_value=key)
            self.assertEqual(result["status"], "retry_wait")
            self.assertEqual(result["attempt"], 0)
            self.assertFalse(gateway.can_dispatch(key))
            with store.connect() as connection:
                row = connection.execute("SELECT status,attempt FROM outbox_items").fetchone()
            self.assertEqual(tuple(row), ("retry_wait", 0))

    def test_429_retry_is_terminal_after_persisted_wall_clock_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, retry_deadline_seconds=3600)
            accepted = gateway.enqueue(
                resource_id="deadline",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
            )
            gateway.dispatch_once()
            waiting = gateway.fail(accepted["outbox_id"], category="429", retry_after="3600")
            self.assertEqual(waiting["status"], "retry_wait")
            self.assertEqual(waiting["attempt"], 0)
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE outbox_items SET retry_deadline_at='2000-01-01T00:00:00Z',next_attempt_at='2000-01-01T00:00:00Z' WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                )
            self.assertEqual(gateway.dispatch_once(), [])
            with store.connect() as connection:
                row = connection.execute(
                    "SELECT status,attempt,next_attempt_at,error_fingerprint FROM outbox_items WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
                semantic = connection.execute(
                    "SELECT status,attempt FROM semantic_tasks WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
            self.assertEqual(tuple(row[:3]), ("dead_letter", 0, None))
            self.assertTrue(row[3])
            self.assertEqual(tuple(semantic), ("dead_letter", 0))
            self.assertEqual(gateway.dispatch_once(), [])

    def test_429_retry_after_is_clamped_to_task_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, retry_deadline_seconds=60)
            accepted = gateway.enqueue(
                resource_id="clamp",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
            )
            gateway.dispatch_once()
            waiting = gateway.fail(accepted["outbox_id"], category="429", retry_after="3600")
            with store.connect() as connection:
                deadline, next_attempt = connection.execute(
                    "SELECT retry_deadline_at,next_attempt_at FROM outbox_items WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                ).fetchone()
            self.assertEqual(waiting["status"], "retry_wait")
            self.assertEqual(next_attempt, deadline)

    def test_429_http_date_and_missing_header_use_shared_window(self) -> None:
        current = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
        future = current + timedelta(seconds=45)
        self.assertGreaterEqual(_parse_retry_after(future.strftime("%a, %d %b %Y %H:%M:%S GMT"), now=current), 44)
        self.assertIsNone(_parse_retry_after("not-a-date", now=current))
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, circuit_threshold=2)
            key = provider_key("oneapi")
            first = gateway.record_429(key, retry_after=None, at=current)
            second = gateway.record_429(key, retry_after=None, at=current)
            self.assertEqual(first["retry_after_seconds"], 30)
            self.assertEqual(second["retry_after_seconds"], 60)
            self.assertEqual(second["circuit_state"], "open")
            self.assertFalse(gateway.can_dispatch(key, at=current.isoformat().replace("+00:00", "Z")))

    def test_vectors_only_is_not_blocked_by_semantic_provider_throttle(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, circuit_threshold=1)
            key = provider_key("oneapi")
            gateway.record_429(key, retry_after="3600")
            accepted = gateway.enqueue(resource_id="local", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            dispatched = gateway.dispatch_once()
            self.assertEqual([item["outbox_id"] for item in dispatched], [accepted["outbox_id"]])

    def test_expired_circuit_allows_one_probe_and_success_closes_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, circuit_threshold=1)
            key = provider_key("oneapi")
            gateway.record_429(key, retry_after="1")
            first = gateway.enqueue(resource_id="probe-1", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
            second = gateway.enqueue(resource_id="probe-2", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
            with store.transaction() as connection:
                connection.execute("UPDATE provider_buckets SET throttle_until='2000-01-01T00:00:00Z' WHERE provider_key=?", (key,))
            dispatched = gateway.dispatch_once(limit=10)
            self.assertEqual(len(dispatched), 1)
            self.assertEqual(dispatched[0]["outbox_id"], first["outbox_id"])
            self.assertEqual(gateway.dispatch_once(limit=10), [])
            self.assertTrue(gateway.ack(first["outbox_id"], openviking_task_id="probe-task"))
            with store.connect() as connection:
                bucket = connection.execute("SELECT circuit_state,consecutive_429 FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
            self.assertEqual(tuple(bucket), ("closed", 0))
            self.assertEqual(len(gateway.dispatch_once(limit=10)), 1)

    def test_late_ack_and_failure_cannot_resurrect_terminal_outbox(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(resource_id="terminal", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(accepted["outbox_id"], category="invalid_resource")["status"], "failed")
            self.assertFalse(gateway.ack(accepted["outbox_id"], openviking_task_id="late"))
            ignored = gateway.fail(accepted["outbox_id"], category="timeout")
            self.assertTrue(ignored["ignored"])
            self.assertEqual(ignored["status"], "failed")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM outbox_items WHERE outbox_id=?", (accepted["outbox_id"],)).fetchone()[0], "failed")

    def test_semantic_failure_preserves_content_verified_projection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="verified-before-semantic-failure",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
            )
            gateway.dispatch_once()
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE resource_projections SET content_state='content_verified',verified_at='2026-08-31T00:00:00Z' WHERE resource_id=? AND revision_id=?",
                    ("verified-before-semantic-failure", "r1"),
                )
            result = gateway.fail(accepted["outbox_id"], category="permanent", detail="semantic provider rejected request")
            self.assertEqual(result["status"], "failed")
            with store.connect() as connection:
                projection = connection.execute(
                    "SELECT content_state,semantic_state,verified_at FROM resource_projections WHERE resource_id=? AND revision_id=?",
                    ("verified-before-semantic-failure", "r1"),
                ).fetchone()
            self.assertEqual(projection[0], "content_verified")
            self.assertEqual(projection[1], "failed")
            self.assertEqual(projection[2], "2026-08-31T00:00:00Z")

    def test_retry_deadline_preserves_content_verified_projection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="verified-before-dead-letter",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
            )
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE resource_projections SET content_state='content_verified',verified_at='2026-08-31T00:00:00Z' WHERE resource_id=? AND revision_id=?",
                    ("verified-before-dead-letter", "r1"),
                )
                connection.execute(
                    "UPDATE outbox_items SET retry_deadline_at='2000-01-01T00:00:00Z',next_attempt_at='2000-01-01T00:00:00Z' WHERE outbox_id=?",
                    (accepted["outbox_id"],),
                )
            self.assertEqual(gateway.dispatch_once(), [])
            with store.connect() as connection:
                projection = connection.execute(
                    "SELECT content_state,semantic_state,verified_at FROM resource_projections WHERE resource_id=? AND revision_id=?",
                    ("verified-before-dead-letter", "r1"),
                ).fetchone()
            self.assertEqual(projection[0], "content_verified")
            self.assertEqual(projection[1], "dead_letter")
            self.assertEqual(projection[2], "2026-08-31T00:00:00Z")

    def test_permanent_error_never_retries_and_transient_budget_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, max_attempts=2)
            permanent = gateway.enqueue(resource_id="bad", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(permanent["outbox_id"], category="invalid_resource")["status"], "failed")
            transient = gateway.enqueue(resource_id="flaky", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(transient["outbox_id"], category="timeout")["attempt"], 1)
            with store.connect() as connection:
                connection.execute("UPDATE outbox_items SET next_attempt_at=NULL WHERE outbox_id=?", (transient["outbox_id"],))
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(transient["outbox_id"], category="timeout")["attempt"], 2)
            with store.connect() as connection:
                connection.execute("UPDATE outbox_items SET next_attempt_at=NULL WHERE outbox_id=?", (transient["outbox_id"],))
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(transient["outbox_id"], category="timeout")["status"], "dead_letter")
            self.assertEqual(gateway.retry_amplification(), 1.5)


if __name__ == "__main__":
    unittest.main()
