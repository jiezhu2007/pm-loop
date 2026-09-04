from __future__ import annotations

import tempfile
import unittest
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_ops_attention import deliver_macos_notifications, project_ops_attention  # noqa: E402
from pm_system_cockpit import CockpitReadModel  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class OpsAttentionTests(unittest.TestCase):
    def test_projection_covers_occurrence_and_run_failure_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-alert-1",
                "occurrence_key": "pm-timeline-daily:20260907T053700Z",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "deadline_at": "2999-09-07T05:52:00Z",
                "registry_hash": "sha256:test-alert",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='failed',terminal_reason='handler_exit_7',error_fingerprint='handler_exit_7' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='failed',error='handler_exit_7' WHERE run_id=?", (accepted["run_id"],))
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='handler_exit_7' WHERE occurrence_id='occ-alert-1'")
            first = project_ops_attention(store)
            second = project_ops_attention(store)
            self.assertGreaterEqual(first["count"], 2)
            self.assertGreaterEqual(second["refreshed"], first["count"])
            snapshot = CockpitReadModel(PMSystemStore(Path(temp) / "pm-system.db", auto_migrate=False, read_only=True)).snapshot()
            self.assertTrue(snapshot["incidents"])
            self.assertIn("alert_id", snapshot["incidents"][0])

    def test_macos_notification_is_local_and_fingerprint_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            alert = store.upsert_ops_alert(fingerprint="sha256:notify-1", severity="P1", alert_type="run_failed", module="Worker", message="fixture")
            commands = []

            def fake_runner(command, **kwargs):
                commands.append((command, kwargs))
                return subprocess.CompletedProcess(command, 0, "", "")

            first = deliver_macos_notifications(store, runner=fake_runner)
            second = deliver_macos_notifications(store, runner=fake_runner)
            self.assertEqual(first["sent"], 1)
            self.assertEqual(second["deduplicated"], 1)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0][0][0], "/usr/bin/osascript")
            self.assertNotIn("curl", " ".join(commands[0][0]))

    def test_canonical_failure_recovery_resolves_alerts_and_recurrence_reopens(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-recovery-1",
                "occurrence_key": "pm-timeline-daily:20260907T053700Z",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "deadline_at": "2999-09-07T05:52:00Z",
                "registry_hash": "sha256:test-recovery",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {},
            })

            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='fixture' WHERE occurrence_id=?", ("occ-recovery-1",))
                connection.execute("UPDATE jobs SET status='failed',terminal_reason='fixture' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='failed',terminal_reason='fixture' WHERE run_id=?", (accepted["run_id"],))
            project_ops_attention(store)
            first_open = store.list_ops_alerts(limit=20, state="open")
            self.assertEqual({alert["alert_type"] for alert in first_open}, {"occurrence_failed", "job_failed", "run_failed"})

            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='completed',failure_reason=NULL WHERE occurrence_id=?", ("occ-recovery-1",))
                connection.execute("UPDATE jobs SET status='completed',terminal_reason=NULL WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='completed',terminal_reason=NULL WHERE run_id=?", (accepted["run_id"],))
            recovered = project_ops_attention(store)
            self.assertEqual(recovered["resolved"], 3)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))

            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='fixture' WHERE occurrence_id=?", ("occ-recovery-1",))
                connection.execute("UPDATE jobs SET status='failed',terminal_reason='fixture' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='failed',terminal_reason='fixture' WHERE run_id=?", (accepted["run_id"],))
            project_ops_attention(store)
            recurring_open = store.list_ops_alerts(limit=20, state="open")
            self.assertEqual({alert["alert_type"] for alert in recurring_open}, {"occurrence_failed", "job_failed", "run_failed"})
            self.assertEqual(len(store.list_ops_alerts(limit=20, state="resolved")), 3)

            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='completed',failure_reason=NULL WHERE occurrence_id=?", ("occ-recovery-1",))
                connection.execute("UPDATE jobs SET status='completed',terminal_reason=NULL WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='completed',terminal_reason=NULL WHERE run_id=?", (accepted["run_id"],))
            recovered_again = project_ops_attention(store)
            self.assertEqual(recovered_again["resolved"], 3)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))
            self.assertEqual(len(store.list_ops_alerts(limit=20, state="suppressed")), 3)

    def test_evidence_backed_suppression_preserves_failure_and_does_not_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-suppressed-1",
                "occurrence_key": "pm-timeline-daily:20260907T053700Z:suppressed",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "deadline_at": "2999-09-07T05:52:00Z",
                "registry_hash": "sha256:test-suppression",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='fixture' WHERE occurrence_id='occ-suppressed-1'")
                connection.execute("UPDATE jobs SET status='failed',terminal_reason='fixture' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='failed',terminal_reason='fixture' WHERE run_id=?", (accepted["run_id"],))
            project_ops_attention(store)
            open_alerts = store.list_ops_alerts(limit=20, state="open")
            suppressed = store.suppress_ops_alerts(
                alert_ids=[item["alert_id"] for item in open_alerts],
                reason="fixture successful replacement run",
                evidence={"kind": "fixture", "artifact": "/tmp/replacement.status.json"},
            )
            self.assertEqual(len(suppressed), 3)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")

            second = project_ops_attention(store)
            self.assertEqual(second["created"], 0)
            self.assertEqual(second["suppressed"], 3)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))
            history = store.list_ops_alerts(limit=20, state="suppressed")
            self.assertEqual(len(history), 3)
            self.assertTrue(all("suppression" in item["details_json"] for item in history))

    def test_unhealthy_health_snapshot_projects_and_healthy_snapshot_resolves(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                    ("Worker", "degraded", "2026-09-07T08:00:00Z", "{}", "fixture"),
                )
            projected = project_ops_attention(store, now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc))
            self.assertEqual(projected["created"], 1)
            open_alert = store.list_ops_alerts(limit=10, state="open")[0]
            self.assertEqual((open_alert["alert_type"], open_alert["severity"]), ("health_check", "P1"))

            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                    ("Worker", "healthy", "2026-09-07T08:02:00Z", "{}", "fixture"),
                )
            recovered = project_ops_attention(store, now=datetime(2026, 9, 7, 8, 3, tzinfo=timezone.utc))
            self.assertEqual(recovered["resolved"], 1)
            self.assertFalse(store.list_ops_alerts(limit=10, state="open"))

    def test_failed_macos_notification_is_not_retried_for_same_alert(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            store.upsert_ops_alert(fingerprint="sha256:notify-failed", severity="P1", alert_type="run_failed", module="Worker", message="fixture")
            commands = []

            def failing_runner(command, **kwargs):
                commands.append((command, kwargs))
                return subprocess.CompletedProcess(command, 1, "", "fixture failure")

            first = deliver_macos_notifications(store, runner=failing_runner)
            second = deliver_macos_notifications(store, runner=failing_runner)
            self.assertEqual(first["failed"], 1)
            self.assertEqual(second["deduplicated"], 1)
            self.assertEqual(len(commands), 1)

    def test_recovery_handles_open_fingerprint_with_resolved_and_suppressed_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            fingerprint = "sha256:registry-history"
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="old")
            store.resolve_ops_alerts(alert_types={"registry_invalid"}, active_fingerprints=set())
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="recurrent")
            store.suppress_ops_alerts(
                alert_ids=[store.list_ops_alerts(limit=10, state="open")[0]["alert_id"]],
                reason="verified historical duplicate",
                evidence={"kind": "fixture"},
            )
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="third")
            recovered = store.resolve_ops_alerts(alert_types={"registry_invalid"}, active_fingerprints=set())
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["state"], "acknowledged")
            self.assertFalse(store.list_ops_alerts(limit=10, state="open"))

    def test_recovery_handles_all_terminal_states_without_leaving_open_alert(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            fingerprint = "sha256:all-terminal-history"
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="first")
            store.resolve_ops_alerts(alert_types={"registry_invalid"}, active_fingerprints=set())
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="second")
            store.suppress_ops_alerts(
                alert_ids=[store.list_ops_alerts(limit=10, state="open")[0]["alert_id"]],
                reason="fixture suppression",
                evidence={"kind": "fixture"},
            )
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="third")
            store.resolve_ops_alerts(alert_types={"registry_invalid"}, active_fingerprints=set())
            store.upsert_ops_alert(fingerprint=fingerprint, severity="P0", alert_type="registry_invalid", module="Scheduler", message="fourth")
            recovered = store.resolve_ops_alerts(alert_types={"registry_invalid"}, active_fingerprints=set())
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["state"], "resolved")
            self.assertIn(":recovery:", recovered[0]["fingerprint"])
            self.assertFalse(store.list_ops_alerts(limit=10, state="open"))


if __name__ == "__main__":
    unittest.main()
