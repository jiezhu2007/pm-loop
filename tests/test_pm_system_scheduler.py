from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_scheduler import AdmissionFrozen, ModelRetryDeadlineExceeded, Scheduler  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class SchedulerTests(unittest.TestCase):
    def make_scheduler(self, root: Path, slots: int = 2) -> Scheduler:
        return Scheduler(PMSystemStore(root / "pm-system.db"), max_slots=slots, slot_ttl_seconds=60)

    def submit(self, store: PMSystemStore, key: str, *, priority: int = 50, profile: str = "interactive") -> dict:
        return store.accept({"job_type": "run", "loop_id": "test", "idempotency_key": key, "priority": priority, "profile": profile})

    def submit_scheduled(self, store: PMSystemStore, occurrence_id: str, *, deadline_at: str = "2999-09-07T05:52:00Z") -> dict:
        return store.accept_scheduled_occurrence({
            "schedule_key": "pm-timeline-daily",
            "occurrence_id": occurrence_id,
            "occurrence_key": f"pm-timeline-daily:{occurrence_id}",
            "scheduled_at": "2026-09-07T05:37:00Z",
            "local_scheduled_at": "2026-09-07T13:37:00+08:00",
            "deadline_at": deadline_at,
            "registry_hash": "sha256:scheduler-test",
            "lock_key": f"pm-timeline-daily:{occurrence_id}",
            "job_type": "scheduled.pm_timeline_daily",
            "loop_id": "pm-timeline-daily",
            "trigger_kind": "calendar",
            "payload": {"handler": "pm_timeline_daily"},
        })

    def test_two_slots_run_and_third_stays_queued(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root)
            runs = [self.submit(store, f"job-{i}") for i in range(3)]
            first = scheduler.claim_next(worker_id="w1")
            second = scheduler.claim_next(worker_id="w2")
            third = scheduler.claim_next(worker_id="w3")
            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            self.assertIsNone(third)
            self.assertEqual({first["run_id"], second["run_id"]}, {runs[0]["run_id"], runs[1]["run_id"]})
            self.assertEqual(store.get_run(runs[2]["run_id"])["status"], "queued")
            self.assertEqual(sum(item["status"] == "leased" for item in scheduler.slot_snapshot()), 2)

    def test_profile_tie_breaker_is_applied_before_queue_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            for index in range(40):
                self.submit(store, f"semantic-{index}", profile="pm-semantic")
            interactive = self.submit(store, "interactive-last", profile="interactive")
            claim = scheduler.claim_next(worker_id="fairness")
            self.assertIsNotNone(claim)
            self.assertEqual(claim["run_id"], interactive["run_id"])

    def test_admission_freeze_blocks_claim_without_mutating_queued_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp, patch.dict("os.environ", {"PM_V44_ADMISSION": "freeze", "PM_V44_MAX_CODEX_SLOTS": "0"}, clear=False):
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = Scheduler(store)
            accepted = self.submit(store, "frozen")
            self.assertIsNone(scheduler.claim_next())
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "queued")
            self.assertEqual(scheduler.admission_snapshot(), {"admission": "freeze", "max_slots": 0, "claim_enabled": False})

    def test_admission_canary_uses_bounded_env_slots(self) -> None:
        with tempfile.TemporaryDirectory() as temp, patch.dict("os.environ", {"PM_V44_ADMISSION": "canary", "PM_V44_MAX_CODEX_SLOTS": "2"}, clear=False):
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = Scheduler(store)
            runs = [self.submit(store, f"canary-{index}") for index in range(3)]
            self.assertEqual(scheduler.admission_snapshot(), {"admission": "on", "max_slots": 2, "claim_enabled": True})
            self.assertIsNotNone(scheduler.claim_next(worker_id="canary-1"))
            self.assertIsNotNone(scheduler.claim_next(worker_id="canary-2"))
            self.assertIsNone(scheduler.claim_next(worker_id="canary-3"))
            self.assertEqual(store.get_run(runs[2]["run_id"])["status"], "queued")

    def test_release_returns_slot_and_next_job_can_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root)
            first = self.submit(store, "first")
            second = self.submit(store, "second")
            third = self.submit(store, "third")
            claim = scheduler.claim_next()
            self.assertTrue(scheduler.release(claim["lease_id"], status="completed"))
            next_claim = scheduler.claim_next()
            self.assertIsNotNone(next_claim)
            self.assertEqual(store.get_run(first["run_id"])["status"], "completed")
            self.assertIn(next_claim["run_id"], {second["run_id"], third["run_id"]})

    def test_scheduled_claim_and_release_project_occurrence_in_same_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = self.submit_scheduled(store, "occ-claim-release")
            scheduler = self.make_scheduler(root, slots=1)
            claim = scheduler.claim_next(worker_id="scheduled-worker")
            self.assertIsNotNone(claim)
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM schedule_occurrences WHERE occurrence_id=?",
                        (accepted["occurrence_id"],),
                    ).fetchone()[0],
                    "running",
                )
            self.assertTrue(scheduler.release(claim["lease_id"], status="completed"))
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM schedule_occurrences WHERE occurrence_id=?",
                        (accepted["occurrence_id"],),
                    ).fetchone()[0],
                    "completed",
                )

    def test_reconcile_repairs_accepted_occurrence_after_job_run_terminal_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = self.submit_scheduled(store, "occ-reconcile")
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE jobs SET status='completed',completed_at='2026-09-07T06:00:00Z',updated_at='2026-09-07T06:00:00Z' WHERE job_id=?",
                    (accepted["job_id"],),
                )
                connection.execute(
                    "UPDATE runs SET status='completed',completed_at='2026-09-07T06:00:00Z',updated_at='2026-09-07T06:00:00Z' WHERE run_id=?",
                    (accepted["run_id"],),
                )
            result = store.reconcile_schedule_occurrences()
            self.assertEqual(result["updated"], 1)
            self.assertEqual(result["completed"], 1)
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM schedule_occurrences WHERE occurrence_id=?",
                        (accepted["occurrence_id"],),
                    ).fetchone()[0],
                    "completed",
                )

    def test_expired_scheduled_job_projects_occurrence_failure_before_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = self.submit_scheduled(store, "occ-deadline", deadline_at="2000-01-01T00:00:00Z")
            scheduler = self.make_scheduler(root, slots=1)
            self.assertIsNone(scheduler.claim_next(worker_id="deadline-worker"))
            with store.connect() as connection:
                state, reason = connection.execute(
                    "SELECT state,failure_reason FROM schedule_occurrences WHERE occurrence_id=?",
                    (accepted["occurrence_id"],),
                ).fetchone()
            self.assertEqual(state, "failed")
            self.assertEqual(reason, "deadline_exceeded")

    def test_retry_wait_reenters_claim_queue_after_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "retry")
            claim = scheduler.claim_next()
            self.assertTrue(scheduler.release(claim["lease_id"], status="retry_wait", retry_after_seconds=0))
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "retry_wait")
            retry_claim = scheduler.claim_next(worker_id="retry-worker")
            self.assertIsNotNone(retry_claim)
            self.assertEqual(retry_claim["run_id"], accepted["run_id"])
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT attempt,status FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()[0:2], (1, "running"))

    def test_scheduled_retry_wait_is_deferred_outside_business_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit_scheduled(store, "occ-retry-window")
            claim = scheduler.claim_next(worker_id="scheduled-first")
            self.assertIsNotNone(claim)
            self.assertTrue(scheduler.release(claim["lease_id"], status="retry_wait", retry_after_seconds=0))
            with patch("pm_system_scheduler.now_iso", return_value="2026-09-02T00:30:00Z"):
                self.assertIsNone(scheduler.claim_next(worker_id="scheduled-outside-window"))
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "retry_wait")

    def test_expired_job_is_failed_before_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = store.accept({
                "job_type": "run",
                "loop_id": "test",
                "idempotency_key": "expired-before-claim",
                "deadline_at": "2000-01-01T00:00:00Z",
            })
            self.assertIsNone(scheduler.claim_next(worker_id="deadline-test"))
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            self.assertEqual(scheduler.slot_snapshot()[0]["status"], "free")
            with store.connect() as connection:
                job = connection.execute("SELECT status,terminal_reason FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()
                events = connection.execute("SELECT event_type FROM run_events WHERE run_id=? ORDER BY seq", (accepted["run_id"],)).fetchall()
            self.assertEqual(tuple(job), ("failed", "deadline_exceeded"))
            self.assertEqual([row[0] for row in events], ["run/accepted", "run/failed"])

    def test_expired_model_retry_deadline_blocks_second_provider_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "expired-model-retry")
            claim = scheduler.claim_next()
            first = scheduler.begin_model_call(
                accepted["run_id"],
                stage="analysis",
                model_input_hash="hash-deadline",
                prompt_version="v1",
                provider="oneapi",
            )
            scheduler.finish_model_call(first["call_id"], status="result_unknown")
            scheduler.release(claim["lease_id"], status="retry_wait", retry_after_seconds=0)
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE model_calls SET retry_deadline_at='2000-01-01T00:00:00Z' WHERE call_id=?",
                    (first["call_id"],),
                )
            retry_claim = scheduler.claim_next(worker_id="deadline-retry")
            self.assertIsNotNone(retry_claim)
            with self.assertRaises(ModelRetryDeadlineExceeded):
                scheduler.begin_model_call(
                    accepted["run_id"],
                    stage="analysis",
                    model_input_hash="hash-deadline",
                    prompt_version="v1",
                    provider="oneapi",
                )
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM model_calls WHERE run_id=?", (accepted["run_id"],)).fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0], 0)

    def test_cancel_frees_slot_and_late_model_response_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "cancel-me")
            claim = scheduler.claim_next()
            call = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash-1", prompt_version="v1", provider="oneapi")
            self.assertTrue(scheduler.cancel(accepted["run_id"], reason="operator"))
            self.assertEqual(scheduler.finish_model_call(call["call_id"], status="completed", artifact_uri="artifact://late"), "cancelled")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "cancelled")
            self.assertEqual(scheduler.slot_snapshot()[0]["status"], "free")
            with self.assertRaises(AdmissionFrozen):
                scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash-1", prompt_version="v1", provider="oneapi")

    def test_model_call_checkpoint_binds_hash_attempt_and_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "model")
            scheduler.claim_next()
            first = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash-a", prompt_version="v1", provider="oneapi")
            self.assertEqual(first["attempt"], 1)
            self.assertEqual(scheduler.finish_model_call(first["call_id"], status="result_unknown"), "result_unknown")
            second = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash-a", prompt_version="v1", provider="oneapi")
            self.assertEqual(second["attempt"], 2)
            with store.connect() as connection:
                row = connection.execute("SELECT input_hash, payload_json FROM checkpoints WHERE run_id=? AND stage='analysis'", (accepted["run_id"],)).fetchone()
            self.assertEqual(row[0], "hash-a")
            self.assertIn('"attempt":2', row[1])

    def test_startup_reconcile_marks_stale_run_interrupted_and_releases_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "stale")
            claim = scheduler.claim_next()
            result = scheduler.startup_reconcile(active_lease_ids=[])
            self.assertEqual(result["expired_slots"], 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "interrupted")
            self.assertEqual(scheduler.slot_snapshot()[0]["status"], "free")
            self.assertIsNotNone(claim)

    def test_startup_reconcile_commits_complete_artifact_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit(store, "artifact")
            scheduler.claim_next()
            call = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash", prompt_version="v1", provider="oneapi")
            scheduler.finish_model_call(call["call_id"], status="completed", artifact_uri="artifact://result")
            result = scheduler.startup_reconcile(active_lease_ids=[])
            self.assertEqual(result["completed_from_checkpoint"], 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")

    def test_startup_reconcile_recovers_scheduled_completion_evidence(self) -> None:
        """A handler success must survive a stop before the final release."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit_scheduled(store, "occ-scheduled-evidence")
            claim = scheduler.claim_next(worker_id="scheduled-worker")
            self.assertIsNotNone(claim)
            artifact = root / "handler.json"
            artifact.write_text(json.dumps({"status": "completed", "returncode": 0}), encoding="utf-8")
            store.append_run_event(
                accepted["run_id"],
                "scheduled/completed",
                {"schedule_key": "pm-timeline-daily", "handler": "pm_timeline_daily", "returncode": 0, "artifact": str(artifact), "failure_reason": None},
                actor="coordination-worker",
            )

            result = scheduler.startup_reconcile(active_lease_ids=[])
            self.assertEqual(result["completed_from_scheduled_evidence"], 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")
            with store.connect() as connection:
                occurrence = connection.execute("SELECT state FROM schedule_occurrences WHERE occurrence_id=?", (accepted["occurrence_id"],)).fetchone()
                events = connection.execute("SELECT event_type FROM run_events WHERE run_id=? ORDER BY seq", (accepted["run_id"],)).fetchall()
            self.assertEqual(occurrence[0], "completed")
            self.assertEqual(events[-1][0], "run/reconciled_completed")
            self.assertEqual(scheduler.startup_reconcile(active_lease_ids=[])["completed_from_scheduled_evidence"], 0)

    def test_startup_reconcile_repairs_historical_interrupted_scheduled_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = self.make_scheduler(root, slots=1)
            accepted = self.submit_scheduled(store, "occ-historical-scheduled-evidence")
            artifact = root / "handler.json"
            artifact.write_text(json.dumps({"status": "completed", "returncode": 0}), encoding="utf-8")
            store.append_run_event(
                accepted["run_id"],
                "scheduled/completed",
                {"schedule_key": "pm-timeline-daily", "handler": "pm_timeline_daily", "returncode": 0, "artifact": str(artifact), "failure_reason": None},
                actor="coordination-worker",
            )
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='interrupted' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='interrupted' WHERE run_id=?", (accepted["run_id"],))
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='terminal_failure' WHERE occurrence_id=?", (accepted["occurrence_id"],))

            result = scheduler.startup_reconcile(active_lease_ids=[])
            self.assertEqual(result["completed_from_scheduled_evidence"], 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT state FROM schedule_occurrences WHERE occurrence_id=?", (accepted["occurrence_id"],)).fetchone()[0], "completed")


if __name__ == "__main__":
    unittest.main()
