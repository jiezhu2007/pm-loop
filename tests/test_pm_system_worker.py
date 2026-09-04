from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_control_plane_server import ControlPlane  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_worker import PMSystemWorker  # noqa: E402
from artifact_registry_read_model import ArtifactRegistryReadModel  # noqa: E402


def fixture_snapshot(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": "pm-loop.snapshot.v1",
                "snapshot_id": "worker-fixture-001",
                "collected_at": "2026-08-29T00:00:00Z",
                "summary": {"launchd_jobs": 1, "skills": 2, "openviking_status": "healthy", "timeline_events": 0},
                "sources": {"launchd": {"status": "healthy", "count": 1}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


class PMSystemWorkerTests(unittest.TestCase):
    @staticmethod
    def _weekly_occurrence(store: PMSystemStore) -> dict:
        return store.accept_scheduled_occurrence({
            "schedule_key": "weekly-sync-and-refresh",
            "occurrence_id": "occ-weekly-source",
            "occurrence_key": "weekly-sync-and-refresh:20260907T080000Z",
            "scheduled_at": "2026-09-07T08:00:00Z",
            "local_scheduled_at": "2026-09-07T16:00:00+08:00",
            "deadline_at": "2999-09-07T20:00:00Z",
            "registry_hash": "sha256:weekly-source",
            "lock_key": "weekly-sync-and-refresh",
            "job_type": "scheduled.weekly_sync",
            "loop_id": "weekly-sync-and-refresh",
            "trigger_kind": "calendar",
            "payload": {"schedule_key": "weekly-sync-and-refresh", "handler": "weekly_sync_and_refresh"},
        })

    def test_successful_weekly_sync_appends_manifest_dependency_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex = root / "codex"
            for path, value in (
                (codex / "skills" / "shengsuan-sync" / "state" / "ledger.json", {"sync-1": {"uri": "viking://resources/shengsuan/doc-a", "doc_guid": "sync-1"}}),
                (codex / "skills" / "databuilder-public-docs" / "state" / "ledger.json", {"public-1": {"uri": "viking://resources/shengsuan/public/doc-b", "doc_guid": "public-1"}}),
                (codex / "skills" / "shengsuan-concepts" / "state" / "concepts-ledger.json", {"concept-a": {"status": "active", "sources": ["viking://resources/shengsuan/doc-a"]}}),
            ):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = self._weekly_occurrence(store)

            def successful(command, timeout, env=None):
                return subprocess.CompletedProcess(command, 0, "weekly complete", "")

            worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, codex_root=codex, max_slots=1, scheduled_invoker=successful)
            self.assertEqual(worker.run_once(), "completed")
            with store.connect() as connection:
                event = connection.execute("SELECT * FROM scheduled_dependency_events").fetchone()
            self.assertIsNotNone(event)
            self.assertEqual(event["status"], "pending")
            self.assertEqual(event["upstream_run_id"], accepted["run_id"])
            self.assertTrue(Path(event["source_manifest_path"]).is_file())
            self.assertTrue(Path(event["handler_evidence_path"]).is_file())
            self.assertTrue(str(event["source_manifest_hash"]).startswith("sha256:"))

    def test_failed_weekly_sync_appends_blocked_dependency_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._weekly_occurrence(store)

            def failed(command, timeout, env=None):
                return subprocess.CompletedProcess(command, 7, "", "fixture failure")

            worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, codex_root=root / "codex", max_slots=1, scheduled_invoker=failed)
            self.assertEqual(worker.run_once(), "failed")
            with store.connect() as connection:
                event = connection.execute("SELECT status,reason FROM scheduled_dependency_events").fetchone()
            self.assertEqual(event[0], "blocked_by_upstream")
            self.assertEqual(event[1], "handler_exit_7")

    def test_dependency_handler_rejects_missing_scheduler_context_before_child_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "concept-refresh-planner",
                "occurrence_id": "occ-concept-missing-context",
                "occurrence_key": "concept-refresh-planner:missing-context",
                "scheduled_at": "2026-09-07T08:00:00Z",
                "local_scheduled_at": "2026-09-07T16:00:00+08:00",
                "deadline_at": "2999-09-07T20:00:00Z",
                "registry_hash": "sha256:concept-context",
                "lock_key": "concept-refresh-planner",
                "job_type": "scheduled.concept_refresh_planner",
                "loop_id": "concept-refresh-planner",
                "trigger_kind": "dependency",
                "payload": {"schedule_key": "concept-refresh-planner", "handler": "concept_refresh_planner"},
            })
            calls = []

            def should_not_start(command, timeout, env=None):
                calls.append(command)
                raise AssertionError("dependency handler must be rejected before child process")

            worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, max_slots=1, scheduled_invoker=should_not_start)
            self.assertEqual(worker.run_once(), "failed")
            self.assertEqual(calls, [])
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            with store.connect() as connection:
                event = connection.execute(
                    "SELECT event_type,payload_json FROM run_events WHERE run_id=? AND event_type='scheduled/rejected' ORDER BY seq DESC LIMIT 1",
                    (accepted["run_id"],),
                ).fetchone()
                job = connection.execute("SELECT status,terminal_reason FROM jobs WHERE job_id=?", (accepted["job_id"],)).fetchone()
            self.assertEqual(event[0], "scheduled/rejected")
            self.assertIn("dependency_context_missing", event[1])
            self.assertEqual(tuple(job), ("failed", "dependency_context_missing:dependency"))

    def test_scheduled_job_uses_fixed_handler_and_writes_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-handler-001",
                "occurrence_key": "pm-timeline-daily:20260907T053700Z",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "local_scheduled_at": "2026-09-07T13:37:00+08:00",
                "deadline_at": "2999-09-07T05:52:00Z",
                "registry_hash": "sha256:test-handler",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "trigger_kind": "calendar",
                "payload": {
                    "schedule_key": "pm-timeline-daily",
                    "handler": "pm_timeline_daily",
                    "command": ["/bin/sh", "-c", "touch SHOULD_NOT_RUN"],
                    "evidence": {"marker": "/tmp/daily.marker"},
                },
            })
            calls = []

            def fake_scheduled(command, timeout, env=None):
                calls.append((command, timeout, env or {}))
                return subprocess.CompletedProcess(command, 0, "handler ok", "")

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, max_slots=1, scheduled_invoker=fake_scheduled)
                self.assertEqual(worker.run_once(), "completed")
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0][0], "/bin/bash")
            self.assertTrue(calls[0][0][-1].endswith("pm-timeline/scripts/daily.sh"))
            self.assertNotIn("SHOULD_NOT_RUN", " ".join(calls[0][0]))
            evidence = root / "runs" / accepted["run_id"] / "scheduled" / "handler.json"
            self.assertTrue(evidence.is_file())
            self.assertEqual(json.loads(evidence.read_text(encoding="utf-8"))["status"], "completed")
            run_root = root / "runs" / accepted["run_id"]
            self.assertTrue((run_root / "run-envelope.v1.json").is_file())
            self.assertTrue((run_root / "task-package.candidate.json").is_file())
            package_path = run_root / "task-package.v1.json"
            self.assertTrue(package_path.is_file())
            self.assertEqual(json.loads(package_path.read_text(encoding="utf-8"))["execution"]["run_id"], accepted["run_id"])
            manifest = json.loads(package_path.read_text(encoding="utf-8"))["artifact_manifest"]
            self.assertEqual(manifest["schema_version"], "pm-loop.artifact-manifest.v1")
            self.assertTrue(Path(manifest["path"]).is_file())
            checkpoint = store.get_checkpoint(accepted["run_id"], "scheduled", "handler")
            self.assertEqual(checkpoint["artifact_uri"], str(package_path.resolve()))
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")
            with store.connect() as connection:
                occurrence = connection.execute("SELECT state FROM schedule_occurrences WHERE occurrence_id=?", ("occ-handler-001",)).fetchone()
            self.assertEqual(occurrence[0], "completed")

    def test_scheduled_handler_failure_is_terminal_and_correlated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "product-intelligence-monitor",
                "occurrence_id": "occ-handler-002",
                "occurrence_key": "product-intelligence-monitor:20260907T060000Z",
                "scheduled_at": "2026-09-07T06:00:00Z",
                "deadline_at": "2999-09-07T10:00:00Z",
                "registry_hash": "sha256:test-handler",
                "lock_key": "product-intelligence-monitor",
                "job_type": "scheduled.product_intelligence",
                "loop_id": "product-intelligence-monitor",
                "payload": {"handler": "product_intelligence_weekly"},
            })

            def failed_scheduled(command, timeout, env=None):
                return subprocess.CompletedProcess(command, 7, "", "fixture failure")

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, max_slots=1, scheduled_invoker=failed_scheduled)
                self.assertEqual(worker.run_once(), "failed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            with store.connect() as connection:
                occurrence = connection.execute("SELECT state,failure_reason FROM schedule_occurrences WHERE occurrence_id=?", ("occ-handler-002",)).fetchone()
            self.assertEqual(occurrence[0], "failed")
            self.assertEqual(occurrence[1], "handler_exit_7")

    def test_scheduled_manifest_uses_canonical_project_root_not_runtime_mirror(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "canonical-project"
            runtime = root / "runtime-mirror"
            canonical.mkdir()
            runtime.mkdir()
            report_dir = canonical / "docs"
            report_dir.mkdir()
            markdown = report_dir / "weekly.md"
            html = report_dir / "weekly.html"
            markdown.write_text("# weekly", encoding="utf-8")
            html.write_text("<h1>weekly</h1>", encoding="utf-8")
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-canonical-root",
                "occurrence_key": "pm-timeline-daily:canonical-root",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "deadline_at": "2999-09-07T05:52:00Z",
                "registry_hash": "sha256:canonical-root",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "trigger_kind": "calendar",
                "payload": {"schedule_key": "pm-timeline-daily", "handler": "pm_timeline_daily"},
            })

            def successful(command, timeout, env=None):
                return subprocess.CompletedProcess(command, 0, json.dumps({"markdown": str(markdown), "html": str(html)}), "")

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(
                    db,
                    artifact_root=root / "runs",
                    project_root=canonical,
                    runtime_root=runtime,
                    max_slots=1,
                    scheduled_invoker=successful,
                )
                self.assertEqual(worker.run_once(), "completed")
            package = json.loads((root / "runs" / accepted["run_id"] / "task-package.v1.json").read_text(encoding="utf-8"))
            manifest_path = Path(package["artifact_manifest"]["path"])
            self.assertTrue(manifest_path.is_relative_to(canonical.resolve() / "state" / "pm-loop" / "artifact-manifests"), manifest_path)
            self.assertFalse((runtime / "state" / "pm-loop" / "artifact-manifests").exists())
            model = ArtifactRegistryReadModel(project_root=canonical)
            item = model.list_artifacts(limit=10)["items"][0]
            self.assertEqual({entry["kind"] for entry in item["open_representations"]}, {"html", "markdown"})
            self.assertEqual(model.open_path(item["artifact_id"], "html"), html.resolve())

    def test_delivery_uncertain_does_not_enter_generic_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "weekly-report-reminder",
                "occurrence_id": "occ-reminder-uncertain",
                "occurrence_key": "weekly-report-reminder:20260906T090000Z",
                "scheduled_at": "2026-09-06T09:00:00Z",
                "deadline_at": "2999-09-06T09:30:00Z",
                "registry_hash": "sha256:test-reminder",
                "lock_key": "weekly-report-reminder",
                "job_type": "scheduled.weekly_report_reminder",
                "loop_id": "weekly-report-reminder",
                "payload": {"handler": "weekly_report_reminder", "delivery_policy": "dry_run"},
            })

            def uncertain(command, timeout, env=None):
                return subprocess.CompletedProcess(command, 1, json.dumps({"send": {"status": "delivery_uncertain", "delivery_disposition": "no_retry_after_effect"}}), "timeout")

            worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, max_slots=1, scheduled_invoker=uncertain)
            self.assertEqual(worker.run_once(), "failed")
            run = store.get_run(accepted["run_id"])
            self.assertEqual(run["status"], "failed")
            with store.connect() as connection:
                job = connection.execute("SELECT status,attempt FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()
            self.assertEqual(tuple(job), ("failed", 0))

    def test_scheduled_run_refreshes_attention_after_terminal_outcome(self) -> None:
        for returncode, expected in ((0, "completed"), (7, "failed")):
            with self.subTest(returncode=returncode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                db = root / "pm-system.db"
                store = PMSystemStore(db)
                store.accept_scheduled_occurrence({
                    "schedule_key": "pm-timeline-daily",
                    "occurrence_id": f"occ-attention-{returncode}",
                    "occurrence_key": f"pm-timeline-daily:20260907T0537{returncode:02d}Z",
                    "scheduled_at": "2026-09-07T05:37:00Z",
                    "deadline_at": "2999-09-07T05:52:00Z",
                    "registry_hash": "sha256:test-attention",
                    "lock_key": "pm-timeline-daily",
                    "job_type": "scheduled.pm_timeline_daily",
                    "loop_id": "pm-timeline-daily",
                    "payload": {"handler": "pm_timeline_daily"},
                })

                def scheduled(command, timeout, env=None):
                    return subprocess.CompletedProcess(command, returncode, "", "fixture failure" if returncode else "")

                with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False), patch("pm_system_worker.refresh_ops_attention") as refresh:
                    worker = PMSystemWorker(db, artifact_root=root / "runs", project_root=root, max_slots=1, scheduled_invoker=scheduled)
                    self.assertEqual(worker.run_once(), expected)
                refresh.assert_called_once_with(worker.store)
    def test_worker_claims_and_completes_snapshot_only_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:snapshot-only",
                    "payload": {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {},
                        "loop_contract": {"id": "daily-radar"},
                        "analysis_mode": "snapshot-only",
                        "snapshot_path": str(fixture),
                    },
                }
            )
            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1)
                self.assertEqual(worker.run_once(), "completed")
            run = store.get_run(accepted["run_id"])
            self.assertEqual(run["status"], "completed")
            self.assertTrue((root / "runs" / accepted["run_id"] / "draft" / "report.md").is_file())
            self.assertEqual(worker.scheduler.slot_snapshot()[0]["status"], "free")

    def test_worker_records_model_call_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:model",
                    "payload": {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {},
                        "loop_contract": {"id": "daily-radar"},
                        "analysis_mode": "codex",
                        "snapshot_path": str(fixture),
                        "budget": {"max_seconds": 5},
                    },
                }
            )

            def fake_invoker(prompt: str, timeout: int, codex_root: Path):
                return 0, json.dumps({"answerability": "partial", "confidence": 0.4, "conclusion": {"headline": "fixture"}}), "", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=fake_invoker)
                self.assertEqual(worker.run_once(), "completed")
            run = store.get_run(accepted["run_id"])
            self.assertEqual(run["status"], "completed")
            with store.connect() as connection:
                call = connection.execute("SELECT status,attempt,model_input_hash FROM model_calls WHERE run_id=?", (accepted["run_id"],)).fetchone()
            self.assertEqual(call[0], "completed")
            self.assertEqual(call[1], 1)
            self.assertTrue(call[2].startswith("sha256:"))
            self.assertTrue((root / "runs" / accepted["run_id"] / "analysis" / "analysis.json").is_file())

    def test_model_timeout_is_bounded_by_persisted_deadlines(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            deadline = (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat(timespec="seconds").replace("+00:00", "Z")
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:model-deadline-bound",
                    "deadline_at": deadline,
                    "payload": {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {},
                        "loop_contract": {"id": "daily-radar"},
                        "analysis_mode": "codex",
                        "snapshot_path": str(fixture),
                        "budget": {"max_seconds": 900},
                    },
                }
            )
            timeouts = []

            def fake_invoker(prompt: str, timeout: int, codex_root: Path):
                timeouts.append(timeout)
                return 0, json.dumps({"answerability": "partial", "confidence": 0.4, "conclusion": {"headline": "bounded"}}), "", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=fake_invoker)
                self.assertEqual(worker.run_once(), "completed")
            self.assertEqual(len(timeouts), 1)
            self.assertGreater(timeouts[0], 0)
            self.assertLessEqual(timeouts[0], 5)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")

    def test_exhausted_model_deadline_skips_provider_and_releases_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:model-deadline-exhausted",
                    "payload": {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {},
                        "loop_contract": {"id": "daily-radar"},
                        "analysis_mode": "codex",
                        "snapshot_path": str(fixture),
                    },
                }
            )
            calls = []

            def forbidden_invoker(prompt: str, timeout: int, codex_root: Path):
                calls.append(timeout)
                raise AssertionError("provider must not be called after deadline")

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=forbidden_invoker)
                claim = worker.scheduler.claim_next(worker_id="test")
                self.assertIsNotNone(claim)
                original = worker._model_timeout
                worker._model_timeout = lambda run_id, request, call: 0  # type: ignore[method-assign]
                try:
                    self.assertEqual(worker.process_claim(claim), "failed")
                finally:
                    worker._model_timeout = original  # type: ignore[method-assign]
            self.assertEqual(calls, [])
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            self.assertEqual(worker.scheduler.slot_snapshot()[0]["status"], "free")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM model_calls WHERE run_id=?", (accepted["run_id"],)).fetchone()[0], "failed")
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0], 0)

    def test_model_timeout_at_deadline_finishes_model_call_terminally(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:model-timeout-at-deadline",
                    "payload": {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {},
                        "loop_contract": {"id": "daily-radar"},
                        "analysis_mode": "codex",
                        "snapshot_path": str(fixture),
                    },
                }
            )

            def timeout_at_deadline(prompt: str, timeout: int, codex_root: Path):
                return 1, "", "TimeoutExpired: provider call timed out", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=timeout_at_deadline)
                worker._model_timeout = lambda run_id, request, call: 1  # type: ignore[method-assign]
                with patch("pm_system_worker._remaining_seconds", return_value=0):
                    self.assertEqual(worker.run_once(), "failed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            self.assertEqual(worker.scheduler.slot_snapshot()[0]["status"], "free")
            with store.connect() as connection:
                model = connection.execute(
                    "SELECT status,response_state FROM model_calls WHERE run_id=?",
                    (accepted["run_id"],),
                ).fetchone()
                active_tokens = connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]
            self.assertEqual(tuple(model), ("failed", "failed"))
            self.assertEqual(active_tokens, 0)

    def test_control_plane_accepts_into_coordination_store_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = fixture_snapshot(root / "snapshot.json")
            PMSystemStore(root / "state" / "state" / "pm-system.db")
            controller = ControlPlane(root / "state", ROOT / "scripts" / "pm_loop_control_plane.py", ROOT, root / "codex", ROOT / "web" / "pm-loop-control-plane", fixture)
            with patch.dict(os.environ, {"PM_V44_COORDINATION_ACTIVE": "on", "PM_V44_ADMISSION": "on", "PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                result = controller.create_run({"loop_id": "daily-radar", "permission_mode": "draft"})
            self.assertTrue(result["coordination"])
            self.assertEqual(result["status"], "queued")
            self.assertFalse((root / "state" / "runs" / result["run_id"] / "request.json").exists())
            self.assertEqual(controller.coordination_store.get_run(result["run_id"])["status"], "queued")

    def test_coordination_events_are_projected_for_sse_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = fixture_snapshot(root / "snapshot.json")
            PMSystemStore(root / "state" / "state" / "pm-system.db")
            controller = ControlPlane(root / "state", ROOT / "scripts" / "pm_loop_control_plane.py", ROOT, root / "codex", ROOT / "web" / "pm-loop-control-plane", fixture)
            with patch.dict(os.environ, {"PM_V44_COORDINATION_ACTIVE": "on", "PM_V44_ADMISSION": "on", "PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                result = controller.create_run({"loop_id": "daily-radar", "permission_mode": "draft"})
                controller.coordination_store.append_run_event(result["run_id"], "run/test", {"ok": True}, actor="test")
                events = controller.coordination_events(result["run_id"])
                detail = controller.coordination_run_detail(result["run_id"])
            self.assertEqual(events[-1]["type"], "run/test")
            self.assertEqual(events[-1]["data"], {"ok": True})
            self.assertEqual(detail["events"][-1]["event_type"], "run/test")

    def test_model_retry_reuses_source_snapshot_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:retry-snapshot",
                    "payload": {"loop_id": "daily-radar", "permission_mode": "draft", "scope": {}, "loop_contract": {"id": "daily-radar"}, "analysis_mode": "codex", "snapshot_path": str(fixture)},
                }
            )
            calls = []

            def flaky_invoker(prompt: str, timeout: int, codex_root: Path):
                calls.append(1)
                if len(calls) == 1:
                    return 1, "", "connection lost", "fake-codex"
                return 0, json.dumps({"answerability": "partial", "confidence": 0.4, "conclusion": {"headline": "recovered"}}), "", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=flaky_invoker)
                self.assertEqual(worker.run_once(), "retry_wait")
                fixture.write_text(fixture.read_text(encoding="utf-8").replace("worker-fixture-001", "worker-fixture-002"), encoding="utf-8")
                with store.transaction() as connection:
                    connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
                self.assertEqual(worker.run_once(), "completed")
            run = store.get_run(accepted["run_id"])
            self.assertEqual(run["status"], "completed")
            self.assertEqual(run["snapshot_id"], "worker-fixture-001")
            self.assertEqual(len(calls), 2)

    def test_invoker_exception_enters_bounded_model_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept(
                {
                    "job_type": "pm-loop",
                    "loop_id": "daily-radar",
                    "idempotency_key": "worker:invoker-exception",
                    "payload": {"loop_id": "daily-radar", "permission_mode": "draft", "scope": {}, "loop_contract": {"id": "daily-radar"}, "analysis_mode": "codex", "snapshot_path": str(fixture)},
                }
            )
            calls = []

            def flaky_invoker(prompt: str, timeout: int, codex_root: Path):
                calls.append(1)
                if len(calls) == 1:
                    raise ConnectionError("provider disconnected")
                return 0, json.dumps({"answerability": "partial", "confidence": 0.4, "conclusion": {"headline": "recovered"}}), "", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=flaky_invoker)
                self.assertEqual(worker.run_once(), "retry_wait")
                with store.transaction() as connection:
                    connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
                self.assertEqual(worker.run_once(), "completed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")
            self.assertEqual(len(calls), 2)

    def test_model_429_uses_shared_bucket_without_consuming_job_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept({
                "job_type": "pm-loop",
                "loop_id": "daily-radar",
                "idempotency_key": "worker:model-429",
                "profile": "interactive",
                "payload": {
                    "loop_id": "daily-radar",
                    "permission_mode": "draft",
                    "scope": {},
                    "loop_contract": {"id": "daily-radar"},
                    "analysis_mode": "codex",
                    "snapshot_path": str(fixture),
                    "provider": "oneapi",
                    "provider_endpoint": "chat",
                    "model": "model-a",
                },
            })
            calls = []

            def rate_limited_then_success(prompt: str, timeout: int, codex_root: Path):
                calls.append(1)
                if len(calls) == 1:
                    return 1, "", "HTTP 429 Too Many Requests\nRetry-After: 60", "fake-codex"
                return 0, json.dumps({"answerability": "partial", "confidence": 0.4, "conclusion": {"headline": "recovered"}}), "", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=rate_limited_then_success)
                self.assertEqual(worker.run_once(), "retry_wait")
                with store.connect() as connection:
                    job = connection.execute("SELECT attempt,status FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()
                    bucket = connection.execute("SELECT provider_key,last_retry_after FROM provider_buckets").fetchone()
                    rate_events = connection.execute("SELECT COUNT(*) FROM provider_rate_limit_events").fetchone()[0]
                self.assertEqual(tuple(job), (0, "retry_wait"))
                self.assertEqual(bucket[0], "oneapi|chat|model-a|model|interactive")
                self.assertEqual(bucket[1], "60")
                self.assertEqual(rate_events, 1)

                # A premature scheduler wake-up must honor the shared bucket
                # and avoid a second provider submission.
                with store.transaction() as connection:
                    connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
                self.assertEqual(worker.run_once(), "retry_wait")
                self.assertEqual(len(calls), 1)

                with store.transaction() as connection:
                    connection.execute("UPDATE provider_buckets SET throttle_until='2000-01-01T00:00:00Z'")
                    connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
                self.assertEqual(worker.run_once(), "completed")
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "completed")
            self.assertEqual(len(calls), 2)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT attempt FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0], 0)

    def test_model_retry_deadline_stops_after_429_without_new_provider_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            fixture = fixture_snapshot(root / "snapshot.json")
            store = PMSystemStore(db)
            accepted = store.accept({
                "job_type": "pm-loop",
                "loop_id": "daily-radar",
                "idempotency_key": "worker:model-429-deadline",
                "profile": "interactive",
                "payload": {
                    "loop_id": "daily-radar",
                    "permission_mode": "draft",
                    "scope": {},
                    "loop_contract": {"id": "daily-radar"},
                    "analysis_mode": "codex",
                    "snapshot_path": str(fixture),
                },
            })
            calls = []

            def always_rate_limited(prompt: str, timeout: int, codex_root: Path):
                calls.append(1)
                return 1, "", "HTTP 429\nRetry-After: 60", "fake-codex"

            with patch.dict(os.environ, {"PM_V44_ADMISSION": "on", "PM_V44_MAX_CODEX_SLOTS": "1"}, clear=False):
                worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1, invoker=always_rate_limited)
                self.assertEqual(worker.run_once(), "retry_wait")
                with store.transaction() as connection:
                    connection.execute("UPDATE model_calls SET retry_deadline_at='2000-01-01T00:00:00Z' WHERE run_id=?", (accepted["run_id"],))
                    connection.execute("UPDATE provider_buckets SET throttle_until='2000-01-01T00:00:00Z'")
                    connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
                self.assertEqual(worker.run_once(), "failed")
            self.assertEqual(len(calls), 1)
            self.assertEqual(store.get_run(accepted["run_id"])["status"], "failed")
            self.assertEqual(worker.scheduler.slot_snapshot()[0]["status"], "free")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM model_calls WHERE run_id=?", (accepted["run_id"],)).fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
