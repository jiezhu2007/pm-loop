from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_cockpit import CockpitReadModel  # noqa: E402
from pm_system_gateway import SemanticGateway, provider_key  # noqa: E402
from pm_system_scheduler import Scheduler  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class FaultInjectionTests(unittest.TestCase):
    def test_disconnect_504_retries_model_stage_without_new_source_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            scheduler = Scheduler(store, max_slots=1)
            accepted = store.accept({"job_type": "assessment", "loop_id": "disconnect", "idempotency_key": "disconnect:1"})
            scheduler.claim_next()
            first = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="source-hash", prompt_version="v1", provider="oneapi")
            self.assertEqual(scheduler.finish_model_call(first["call_id"], status="result_unknown"), "result_unknown")
            second = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="source-hash", prompt_version="v1", provider="oneapi")
            self.assertEqual(second["attempt"], 2)
            self.assertEqual(second["model_input_hash"], first["model_input_hash"])
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "running")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM model_calls WHERE run_id=?", (accepted["run_id"],)).fetchone()[0], 2)

    def test_openviking_restart_is_degraded_without_blocking_local_fast_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            semantic = gateway.enqueue(resource_id="semantic", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
            fast = gateway.enqueue(resource_id="fast", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            gateway.dispatch_once()
            self.assertEqual(gateway.fail(semantic["outbox_id"], category="504")["status"], "retry_wait")
            # A semantic outage must not make the already queued fast-vector
            # revision lose its local durable state.
            with store.connect() as connection:
                row = connection.execute("SELECT status FROM outbox_items WHERE outbox_id=?", (fast["outbox_id"],)).fetchone()
            self.assertEqual(row[0], "in_flight")

    def test_duplicate_revision_submission_has_one_effective_task(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            values = [gateway.enqueue(resource_id="doc", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic") for _ in range(5)]
            gateway.dispatch_once(limit=50)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0], 1)
            self.assertEqual(sum(1 for value in values if not value["deduplicated"]), 1)

    def test_permanent_invalid_resource_is_explicit_failed_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(resource_id="missing", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
            gateway.dispatch_once()
            result = gateway.fail(accepted["outbox_id"], category="invalid_resource", detail="not found")
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["attempt"], 0)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status,attempt FROM semantic_tasks").fetchone()[0:2], ("failed", 0))

    def test_cancel_and_late_response_leave_cancelled_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            scheduler = Scheduler(store, max_slots=1)
            accepted = store.accept({"job_type": "run", "loop_id": "cancel-race", "idempotency_key": "cancel-race"})
            scheduler.claim_next()
            call = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="race", prompt_version="v1", provider="oneapi")
            self.assertTrue(scheduler.cancel(accepted["run_id"], reason="fault-injection"))
            self.assertEqual(scheduler.finish_model_call(call["call_id"], status="response_received", artifact_uri="artifact://late"), "cancelled")
            detail = CockpitReadModel(store).run_detail(accepted["run_id"])
            self.assertEqual(detail["run"]["status"], "cancelled")
            self.assertEqual(detail["model_calls"][0]["status"], "cancelled")

    def test_expired_lease_reconcile_has_explicit_interrupted_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            scheduler = Scheduler(store, max_slots=1)
            accepted = store.accept({"job_type": "run", "loop_id": "restart", "idempotency_key": "restart"})
            scheduler.claim_next()
            with store.transaction() as connection:
                connection.execute("UPDATE execution_slots SET expires_at=?", ("2000-01-01T00:00:00Z",))
            result = scheduler.startup_reconcile(active_lease_ids=[])
            self.assertEqual(result["interrupted_runs"], 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "interrupted")
            self.assertEqual(scheduler.slot_snapshot()[0]["status"], "free")

    def test_429_window_does_not_increase_attempt_for_all_header_forms(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store, circuit_threshold=10)
            key = provider_key("oneapi", "endpoint", "model")
            for header in ("60", "Wed, 28 Aug 2026 12:01:00 GMT", None):
                result = gateway.record_429(key, retry_after=header, at=datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc))
                self.assertGreaterEqual(result["retry_after_seconds"], 0)
            accepted = gateway.enqueue(resource_id="429", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic", endpoint="endpoint", model="model")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT attempt FROM outbox_items WHERE outbox_id=?", (accepted["outbox_id"],)).fetchone()[0], 0)

    def test_cockpit_refresh_is_read_only_and_has_no_pending_claim_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            store.accept({"job_type": "run", "loop_id": "refresh", "idempotency_key": "refresh"})
            before = (Path(temp) / "pm-system.db").read_bytes()
            model = CockpitReadModel(store)
            first = model.snapshot()
            second = model.snapshot()
            self.assertTrue(first["read_only"] and second["read_only"])
            self.assertEqual(first["source_version"], second["source_version"])
            self.assertEqual(before, (Path(temp) / "pm-system.db").read_bytes())


if __name__ == "__main__":
    unittest.main()

