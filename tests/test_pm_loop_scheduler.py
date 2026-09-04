from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_scheduler import PMLoopDispatcher, _single_instance
from pm_schedule_registry import RegistryError, load_registry
from pm_system_store import PMSystemStore


class DispatcherTests(unittest.TestCase):
    @staticmethod
    def _file_hash(path: Path) -> str:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()

    def _completed_weekly_run(self, store: PMSystemStore) -> dict:
        accepted = store.accept_scheduled_occurrence({
            "schedule_key": "weekly-sync-and-refresh",
            "occurrence_id": "occ-source-001",
            "occurrence_key": "weekly-sync-and-refresh:20260907T080000Z",
            "scheduled_at": "2026-09-07T08:00:00Z",
            "local_scheduled_at": "2026-09-07T16:00:00+08:00",
            "deadline_at": "2026-09-07T20:00:00Z",
            "registry_hash": "sha256:source-fixture",
            "lock_key": "weekly-sync-and-refresh",
            "job_type": "scheduled.weekly_sync",
            "loop_id": "weekly-sync-and-refresh",
            "trigger_kind": "calendar",
            "payload": {"schedule_key": "weekly-sync-and-refresh", "handler": "weekly_sync_and_refresh"},
        })
        with store.transaction() as connection:
            connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (accepted["job_id"],))
            connection.execute("UPDATE runs SET status='completed' WHERE run_id=?", (accepted["run_id"],))
        return accepted

    def _dependency_event(self, store: PMSystemStore, root: Path, *, manifest_hash: str | None = None) -> dict:
        upstream = self._completed_weekly_run(store)
        manifest = root / "source-manifest.json"
        evidence = root / "handler.json"
        manifest.write_text('{"schema_version":"concept-source-manifest.v1","metrics":{}}\n', encoding="utf-8")
        evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
        actual_manifest_hash = self._file_hash(manifest)
        return store.append_scheduled_dependency_event({
            "event_key": f"concept-refresh-planner:{upstream['run_id']}:{actual_manifest_hash}:concept-refresh-planner.v2",
            "dependent_schedule_key": "concept-refresh-planner",
            "upstream_schedule_key": "weekly-sync-and-refresh",
            "upstream_occurrence_id": "occ-source-001",
            "upstream_run_id": upstream["run_id"],
            "upstream_completed_at": "2026-09-07T08:00:00Z",
            "source_manifest_path": str(manifest),
            "source_manifest_hash": manifest_hash or actual_manifest_hash,
            "handler_evidence_path": str(evidence),
            "handler_evidence_hash": self._file_hash(evidence),
            "planner_version": "concept-refresh-planner.v2",
            "status": "pending",
        })

    def test_shadow_is_read_only_and_plans_all_registered_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dispatcher = PMLoopDispatcher(root / "pm-system.db", registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            result = dispatcher.tick(now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc), mode="shadow")
            self.assertEqual(result["status"], "shadow")
            calendar_count = sum(1 for task in load_registry(ROOT / "scripts" / "schedule-registry.json").tasks if task.is_calendar)
            self.assertEqual(len(result["planned"]), calendar_count)
            self.assertFalse((root / "pm-system.db").exists())

    def test_dependency_event_is_consumed_once_without_calendar_window_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            event = self._dependency_event(store, root)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            first = dispatcher.tick(now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc), mode="calendar")
            second = dispatcher.tick(now=datetime(2026, 9, 7, 8, 2, tzinfo=timezone.utc), mode="calendar")
            self.assertEqual(first["dependency"]["accepted"], 1)
            self.assertEqual(second["dependency"]["accepted"], 0)
            stored = store.get_scheduled_dependency_event(event["event_id"])
            self.assertEqual(stored["status"], "consumed")
            with store.connect() as connection:
                occurrence = connection.execute(
                    "SELECT trigger_kind,job_id,run_id FROM schedule_occurrences WHERE occurrence_id=?",
                    (stored["occurrence_id"],),
                ).fetchone()
            self.assertEqual(occurrence[0], "dependency")
            self.assertTrue(occurrence[1])
            self.assertTrue(occurrence[2])

    def test_dependency_only_replay_consumes_no_calendar_work(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            event = self._dependency_event(store, root)
            dispatcher = PMLoopDispatcher(
                db,
                registry_path=ROOT / "scripts" / "schedule-registry.json",
                runtime_registry_path=None,
                lock_path=root / "dispatcher.lock",
            )
            result = dispatcher.tick(
                now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc),
                mode="manual_replay",
                dependency_only=True,
            )
            self.assertEqual(result["accepted"], 0)
            self.assertTrue(result["dependency_only"])
            self.assertEqual(result["reconcile"], {})
            self.assertEqual(result["dependency"]["accepted"], 1)
            self.assertEqual(store.get_scheduled_dependency_event(event["event_id"])["status"], "consumed")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM schedule_occurrences").fetchone()[0], 2)

    def test_dependency_event_hash_mismatch_is_blocked_without_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            event = self._dependency_event(store, root, manifest_hash="sha256:not-the-file")
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            result = dispatcher.tick(now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc), mode="calendar")
            self.assertEqual(result["dependency"]["blocked"], 1)
            self.assertEqual(store.get_scheduled_dependency_event(event["event_id"])["status"], "blocked_by_upstream")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs WHERE schedule_key='concept-refresh-planner'").fetchone()[0], 0)

    def test_shadow_does_not_consume_dependency_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            event = self._dependency_event(store, root)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            result = dispatcher.tick(now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc), mode="shadow")
            self.assertTrue(result["read_only"])
            self.assertEqual(store.get_scheduled_dependency_event(event["event_id"])["status"], "pending")

    def test_calendar_tick_accepts_each_occurrence_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            PMSystemStore(root / "pm-system.db")
            db = root / "pm-system.db"
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            now = datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc)
            first = dispatcher.tick(now=now, mode="calendar")
            second = dispatcher.tick(now=now, mode="calendar")
            calendar_count = sum(1 for task in load_registry(ROOT / "scripts" / "schedule-registry.json").tasks if task.is_calendar)
            self.assertEqual(first["accepted"], 2)
            self.assertEqual(first["expired"], calendar_count - first["accepted"])
            self.assertEqual(second["deduplicated"], 2)
            store = PMSystemStore(db, auto_migrate=False)
            self.assertEqual(len(store.list_schedule_occurrences()), calendar_count)
            self.assertEqual(len(store.list_scheduler_ticks()), 2)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs WHERE occurrence_id IS NOT NULL").fetchone()[0], 2)

    def test_expired_window_is_recorded_without_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            result = dispatcher.tick(now=datetime(2026, 9, 8, 3, 0, tzinfo=timezone.utc), mode="catchup")
            self.assertGreaterEqual(result["expired"], 1)
            store = PMSystemStore(db, auto_migrate=False)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT schedule_key FROM schedule_occurrences WHERE state='accepted'").fetchone()[0], "databuilder-product-gap-report")
                self.assertGreaterEqual(connection.execute("SELECT COUNT(*) FROM schedule_occurrences WHERE state='expired'").fetchone()[0], 1)

    def test_due_occurrences_are_deferred_outside_business_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            # 18:01 Asia/Shanghai, after the 18:00 inclusive boundary.
            result = dispatcher.tick(now=datetime(2026, 9, 7, 10, 1, tzinfo=timezone.utc), mode="calendar")
            self.assertFalse(result["business_window_open"])
            self.assertEqual(result["accepted"], 0)
            self.assertGreaterEqual(result["deferred"], 1)
            with PMSystemStore(db, auto_migrate=False).connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
                self.assertGreaterEqual(connection.execute("SELECT COUNT(*) FROM schedule_occurrences WHERE state='deferred' AND failure_reason='outside_business_window'").fetchone()[0], 1)

    def test_catchup_records_known_old_window_as_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            dispatcher.tick(now=datetime(2026, 9, 7, 5, 40, tzinfo=timezone.utc), mode="calendar")
            result = dispatcher.tick(now=datetime(2026, 9, 9, 5, 40, tzinfo=timezone.utc), mode="catchup")
            self.assertGreaterEqual(result["suppressed"], 1)
            with PMSystemStore(db, auto_migrate=False).connect() as connection:
                row = connection.execute(
                    "SELECT state,failure_reason FROM schedule_occurrences WHERE occurrence_key='pm-timeline-daily:20260908T053700Z'"
                ).fetchone()
            self.assertEqual(tuple(row), ("suppressed", "coalesced_by:pm-timeline-daily:20260909T053700Z"))

    def test_catchup_records_old_window_as_expired_when_latest_is_expired(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            dispatcher.tick(now=datetime(2026, 9, 7, 5, 40, tzinfo=timezone.utc), mode="calendar")
            result = dispatcher.tick(now=datetime(2026, 9, 9, 7, 0, tzinfo=timezone.utc), mode="catchup")
            self.assertGreaterEqual(result["expired"], 1)
            with PMSystemStore(db, auto_migrate=False).connect() as connection:
                row = connection.execute(
                    "SELECT state,failure_reason FROM schedule_occurrences WHERE occurrence_key='pm-timeline-daily:20260908T053700Z'"
                ).fetchone()
            self.assertEqual(tuple(row), ("expired", "deadline_exceeded"))

    def test_invalid_registry_and_duplicate_scheduler_write_p0_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            invalid = root / "invalid-registry.json"
            invalid.write_text('{"registry_version": 1}\n', encoding="utf-8")
            lock = root / "dispatcher.lock"
            dispatcher = PMLoopDispatcher(db, registry_path=invalid, runtime_registry_path=None, lock_path=lock)
            with self.assertRaises(RegistryError):
                dispatcher.tick(now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc))
            with _single_instance(lock):
                with self.assertRaisesRegex(RuntimeError, "duplicate_scheduler"):
                    PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=lock).tick(
                        now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc)
                    )
            alerts = store.list_ops_alerts(limit=10)
            self.assertEqual({item["alert_type"] for item in alerts}, {"registry_invalid", "duplicate_scheduler"})
            self.assertTrue(all(item["severity"] == "P0" for item in alerts))

    def test_runtime_registry_drift_is_fail_closed_against_canonical_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            canonical = root / "canonical-registry.json"
            runtime = root / "runtime-registry.json"
            source = (ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8")
            canonical.write_text(source, encoding="utf-8")
            payload = json.loads(source)
            payload["tasks"][0]["priority"] = 99
            runtime.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            dispatcher = PMLoopDispatcher(
                db,
                registry_path=runtime,
                runtime_registry_path=None,
                canonical_registry_path=canonical,
                lock_path=root / "dispatcher.lock",
            )
            with self.assertRaisesRegex(RegistryError, "canonical/runtime registry hash mismatch"):
                dispatcher.tick(now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc))

            alerts = PMSystemStore(db, auto_migrate=False).list_ops_alerts(limit=10, state="open")
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["alert_type"], "registry_invalid")
            self.assertEqual(alerts[0]["severity"], "P0")
            with PMSystemStore(db, auto_migrate=False).connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)

    def test_database_write_failure_writes_database_unavailable_alert_when_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            with patch.object(PMSystemStore, "start_scheduler_tick", side_effect=sqlite3.OperationalError("database is locked")):
                with self.assertRaises(sqlite3.OperationalError):
                    dispatcher.tick(now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc))
            alert = next(item for item in store.list_ops_alerts(limit=10) if item["alert_type"] == "database_unavailable")
            self.assertEqual(alert["severity"], "P0")

    def test_attention_refresh_runs_after_completed_and_recoverable_failure_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            lock = root / "dispatcher.lock"
            with patch("pm_loop_scheduler.refresh_ops_attention") as refresh:
                dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=lock)
                dispatcher.tick(now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc))

                invalid = root / "invalid-registry.json"
                invalid.write_text('{"registry_version": 1}\n', encoding="utf-8")
                with self.assertRaises(RegistryError):
                    PMLoopDispatcher(db, registry_path=invalid, runtime_registry_path=None, lock_path=lock).tick(
                        now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc)
                    )

                with _single_instance(lock):
                    with self.assertRaisesRegex(RuntimeError, "duplicate_scheduler"):
                        PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=lock).tick(
                            now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc)
                        )
            self.assertEqual(refresh.call_count, 3)


if __name__ == "__main__":
    unittest.main()
