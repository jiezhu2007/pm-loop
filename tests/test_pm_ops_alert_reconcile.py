from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_ops_alert_reconcile import PLAN_SCHEMA, reconcile  # noqa: E402
from pm_ops_attention import project_ops_attention  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class OpsAlertReconciliationTests(unittest.TestCase):
    def test_completed_replacement_suppresses_only_prior_failure_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "product-docs-gap-report",
                "occurrence_id": "occ-reconcile-1",
                "occurrence_key": "product-docs-gap-report:20260901T060000Z",
                "scheduled_at": "2026-09-01T06:00:00Z",
                "deadline_at": "2026-09-01T07:00:00Z",
                "registry_hash": "sha256:fixture",
                "lock_key": "product-docs-gap-report",
                "job_type": "scheduled.product_docs_gap_report",
                "loop_id": "product-docs-gap-report",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='failed',updated_at='2026-09-01T06:10:00Z' WHERE occurrence_id='occ-reconcile-1'")
                connection.execute("UPDATE jobs SET status='failed',updated_at='2026-09-01T06:10:00Z' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='failed',updated_at='2026-09-01T06:10:00Z' WHERE run_id=?", (accepted["run_id"],))
            project_ops_attention(store)

            artifacts = {}
            for name in ("markdown", "html", "snapshot"):
                artifact = root / f"replacement.{name}"
                artifact.write_text(name, encoding="utf-8")
                artifacts[name] = str(artifact)
            status = root / "replacement.status.json"
            status.write_text(json.dumps({"status": "completed", "run_at": "2026-09-01T15:00:00+08:00", "artifacts": artifacts}), encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text(json.dumps({
                "schema_version": PLAN_SCHEMA,
                "entries": [{
                    "kind": "successful_rerun",
                    "schedule_key": "product-docs-gap-report",
                    "status_file": str(status),
                    "reason": "fixture replacement completed",
                }],
            }), encoding="utf-8")

            dry_run = reconcile(db_path=root / "pm-system.db", plan_path=plan)
            self.assertEqual(dry_run["selected_count"], 3)
            self.assertEqual(dry_run["applied_count"], 0)
            applied = reconcile(db_path=root / "pm-system.db", plan_path=plan, apply=True)
            self.assertEqual(applied["applied_count"], 3)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            projected = project_ops_attention(store)
            self.assertEqual(projected["suppressed"], 3)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))

    def test_pre_cutover_expired_selector_does_not_hide_post_cutover_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            for suffix, scheduled_at in (("old", "2026-09-01T06:00:00Z"), ("new", "2026-09-01T09:00:00Z")):
                store.record_schedule_occurrence({
                    "schedule_key": "pm-timeline-daily",
                    "occurrence_id": f"occ-expired-{suffix}",
                    "occurrence_key": f"pm-timeline-daily:{suffix}",
                    "scheduled_at": scheduled_at,
                    "local_scheduled_at": scheduled_at,
                    "deadline_at": "2026-09-01T09:15:00Z",
                    "registry_hash": "sha256:fixture",
                    "lock_key": "pm-timeline-daily",
                }, state="expired", reason="deadline_exceeded")
            project_ops_attention(store)
            manifest = root / "cutover.json"
            manifest.write_text("{}", encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text(json.dumps({
                "schema_version": PLAN_SCHEMA,
                "entries": [{
                    "kind": "pre_cutover_expired",
                    "cutover_at": "2026-09-01T08:00:00Z",
                    "cutover_manifest": str(manifest),
                    "reason": "fixture pre-cutover baseline",
                }],
            }), encoding="utf-8")
            applied = reconcile(db_path=root / "pm-system.db", plan_path=plan, apply=True)
            self.assertEqual(applied["applied_count"], 1)
            project_ops_attention(store)
            open_alerts = store.list_ops_alerts(limit=20, state="open")
            self.assertEqual(len(open_alerts), 1)
            self.assertEqual(open_alerts[0]["occurrence_id"], "occ-expired-new")

    def test_successful_handler_replacement_selects_only_explicit_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "product-docs-gap-report",
                "occurrence_id": "occ-handler-replacement",
                "occurrence_key": "product-docs-gap-report:manual-handler-replacement",
                "scheduled_at": "2026-09-01T08:00:00Z",
                "deadline_at": "2026-09-01T09:00:00Z",
                "registry_hash": "sha256:fixture",
                "lock_key": "product-docs-gap-report",
                "job_type": "scheduled.product_docs_gap_report",
                "loop_id": "product-docs-gap-report",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE schedule_occurrences SET state='failed',failure_reason='fixture' WHERE occurrence_id=?", (accepted["occurrence_id"],))
                connection.execute("UPDATE jobs SET status='failed',terminal_reason='fixture' WHERE job_id=?", (accepted["job_id"],))
                connection.execute(
                    "UPDATE runs SET status='interrupted',terminal_reason=NULL,updated_at='2026-09-01T08:30:00Z' WHERE run_id=?",
                    (accepted["run_id"],),
                )
            project_ops_attention(store)

            output = root / "handler-output.txt"
            output.write_text("completed", encoding="utf-8")
            handler = root / "handler.json"
            handler.write_text(json.dumps({
                "schema_version": "pm-loop.scheduled-handler.v1",
                "run_id": accepted["run_id"],
                "occurrence_id": accepted["occurrence_id"],
                "schedule_key": "product-docs-gap-report",
                "finished_at": "2026-09-03T00:00:00Z",
                "returncode": 0,
                "status": "completed",
                "output_path": str(output),
            }), encoding="utf-8")
            manifest = root / "cutover.json"
            manifest.write_text("{}", encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text(json.dumps({
                "schema_version": PLAN_SCHEMA,
                "entries": [{
                    "kind": "successful_handler_replacement",
                    "schedule_key": "product-docs-gap-report",
                    "handler_file": str(handler),
                    "replaced_run_ids": [accepted["run_id"]],
                    "reason": "fixture handler completed after worker interruption",
                }],
            }), encoding="utf-8")

            dry_run = reconcile(db_path=root / "pm-system.db", plan_path=plan)
            self.assertEqual(dry_run["selected_count"], 3)
            applied = reconcile(db_path=root / "pm-system.db", plan_path=plan, apply=True)
            self.assertEqual(applied["applied_count"], 3)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "interrupted")
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))

    def test_successful_schedule_rerun_suppresses_prior_expired_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            store.record_schedule_occurrence({
                "schedule_key": "competitive-radar-brief",
                "occurrence_id": "occ-expired-replacement",
                "occurrence_key": "competitive-radar-brief:20260901T083000Z",
                "scheduled_at": "2026-09-01T08:30:00Z",
                "local_scheduled_at": "2026-09-01T16:30:00+08:00",
                "deadline_at": "2026-09-01T09:30:00Z",
                "registry_hash": "sha256:fixture",
                "lock_key": "competitive-radar",
            }, state="expired", reason="deadline_exceeded")
            project_ops_attention(store, now=datetime(2026, 9, 2, tzinfo=timezone.utc))
            output = root / "handler-output.txt"
            output.write_text("completed", encoding="utf-8")
            handler = root / "handler.json"
            handler.write_text(json.dumps({
                "schema_version": "pm-loop.scheduled-handler.v1",
                "run_id": "run-replacement",
                "occurrence_id": "occ-new",
                "schedule_key": "competitive-radar-brief",
                "finished_at": "2026-09-02T03:33:10Z",
                "returncode": 0,
                "status": "completed",
                "output_path": str(output),
            }), encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text(json.dumps({
                "schema_version": PLAN_SCHEMA,
                "entries": [{
                    "kind": "successful_schedule_rerun",
                    "schedule_key": "competitive-radar-brief",
                    "handler_file": str(handler),
                    "reason": "completed handler replaced expired schedule window",
                }],
            }), encoding="utf-8")
            dry_run = reconcile(db_path=root / "pm-system.db", plan_path=plan)
            self.assertEqual(dry_run["selected_count"], 1)
            applied = reconcile(db_path=root / "pm-system.db", plan_path=plan, apply=True)
            self.assertEqual(applied["applied_count"], 1)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))

    def test_historical_health_baseline_selects_exact_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                    ("Worker", "maintenance", "2026-08-30T00:56:11Z", "{}", "g2-fixture"),
                )
            project_ops_attention(store, now=datetime(2026, 9, 1, tzinfo=timezone.utc))
            self.assertEqual(len(store.list_ops_alerts(limit=20, state="open")), 1)
            manifest = root / "cutover.json"
            manifest.write_text("{}", encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text(json.dumps({
                "schema_version": PLAN_SCHEMA,
                "entries": [{
                    "kind": "historical_health_baseline",
                    "modules": ["Worker"],
                    "source_version": "g2-fixture",
                    "observed_before": "2026-09-01T00:00:00Z",
                    "cutover_manifest": str(manifest),
                    "reason": "freeze-era maintenance snapshot before S5",
                }],
            }), encoding="utf-8")

            applied = reconcile(db_path=root / "pm-system.db", plan_path=plan, apply=True)
            self.assertEqual(applied["applied_count"], 1)
            self.assertFalse(store.list_ops_alerts(limit=20, state="open"))


if __name__ == "__main__":
    unittest.main()
