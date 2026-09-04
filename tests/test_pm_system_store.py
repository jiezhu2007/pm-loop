from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_runtime import RunStore  # noqa: E402
from pm_system_store import (  # noqa: E402
    LegacyRunStoreReadOnlyAdapter,
    PMSystemStore,
    ReadOnlyStoreError,
    SCHEMA_VERSION,
    open_coordination_store,
)


class PMSystemStoreTests(unittest.TestCase):
    def test_schema_migration_is_idempotent_and_uses_wal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pm-system.db"
            store = PMSystemStore(path, busy_timeout_ms=3210)
            self.assertEqual(store.schema_version(), SCHEMA_VERSION)
            first = store.pragmas()
            self.assertEqual(str(first["journal_mode"]).lower(), "wal")
            self.assertEqual(first["busy_timeout"], 3210)
            store.migrate()
            self.assertEqual(store.schema_version(), SCHEMA_VERSION)
            with sqlite3.connect(path) as connection:
                versions = connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
                self.assertEqual(versions, [(version,) for version in range(1, SCHEMA_VERSION + 1)])
                table_names = {
                    row[0]
                    for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertTrue({"jobs", "runs", "run_events", "checkpoints", "execution_slots", "model_calls", "provider_buckets", "outbox_items", "semantic_tasks", "semantic_task_observations", "error_events", "module_health_snapshots", "metric_rollups", "source_snapshots", "source_items", "generations", "evidence_refs", "timeline_events"}.issubset(table_names))

    def test_v12_upgrades_v10_occurrences_dependency_and_refresh_audits_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pm-system.db"
            old = PMSystemStore(path, max_schema_version=10)
            self.assertEqual(old.schema_version(), 10)
            upgraded = PMSystemStore(path)
            self.assertEqual(upgraded.schema_version(), SCHEMA_VERSION)
            with upgraded.connect() as connection:
                definition = connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='schedule_occurrences'"
                ).fetchone()[0]
                columns = {row[1] for row in connection.execute("PRAGMA table_info(scheduled_dependency_events)")}
                refresh_definition = connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='concept_refresh_runs'"
                ).fetchone()[0]
                refresh_columns = {row[1] for row in connection.execute("PRAGMA table_info(concept_refresh_items)")}
            self.assertIn("'dependency'", definition)
            self.assertTrue({"upstream_completed_at", "handler_evidence_path"}.issubset(columns))
            self.assertIn("'planned_canary'", refresh_definition)
            self.assertTrue({"target_uri", "idempotency_key", "outbox_item_id"}.issubset(refresh_columns))
            upgraded.migrate()
            self.assertEqual(upgraded.schema_version(), SCHEMA_VERSION)

    def test_v12_preserves_v11_disabled_plan_records_and_adds_projection_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pm-system.db"
            old = PMSystemStore(path, max_schema_version=11)
            with old.transaction() as connection:
                connection.execute(
                    "INSERT INTO scheduled_dependency_events(event_id,event_key,dependent_schedule_key,upstream_schedule_key,upstream_occurrence_id,upstream_run_id,upstream_completed_at,planner_version,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("event-1", "event-key-1", "concept-refresh-planner", "weekly-sync-and-refresh", "occ-1", "run-1", "2026-09-07T08:00:00Z", "concept-refresh-planner.v1", "consumed", "2026-09-07T08:00:00Z", "2026-09-07T08:00:00Z"),
                )
                connection.execute(
                    "INSERT INTO concept_refresh_runs(plan_id,dependency_event_id,upstream_run_id,admission_state,planner_version,source_manifest_path,source_manifest_hash,plan_path,plan_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("plan-1", "event-1", "run-1", "disabled", "concept-refresh-planner.v1", "/tmp/source.json", "sha256:source", "/tmp/plan.json", "sha256:plan", "planned_disabled", "2026-09-07T08:00:00Z"),
                )
                connection.execute(
                    "INSERT INTO concept_refresh_items(plan_id,concept_id,coverage_status,decision,source_count,reason) VALUES(?,?,?,?,?,?)",
                    ("plan-1", "concept-1", "refreshable", "observe_only", 1, "historical"),
                )
            upgraded = PMSystemStore(path)
            with upgraded.connect() as connection:
                run = connection.execute("SELECT status FROM concept_refresh_runs WHERE plan_id='plan-1'").fetchone()
                item = connection.execute("SELECT decision,target_uri,idempotency_key,outbox_item_id FROM concept_refresh_items WHERE plan_id='plan-1'").fetchone()
            self.assertEqual(run[0], "planned_disabled")
            self.assertEqual(tuple(item), ("observe_only", None, None, None))

    def test_dependency_events_and_disabled_plan_records_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            plan_path = root / "plan.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            event = {
                "event_key": "concept-refresh-planner:run-source:sha256:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-source",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": "sha256:manifest",
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": "sha256:evidence",
                "planner_version": "concept-refresh-planner.v2",
                "status": "pending",
            }
            first = store.append_scheduled_dependency_event(event)
            duplicate = store.append_scheduled_dependency_event(event)
            self.assertFalse(first["deduplicated"])
            self.assertTrue(duplicate["deduplicated"])
            self.assertTrue(store.mark_scheduled_dependency_event_blocked(
                first["event_id"], reason="fixture", outcome={"dependency_state": "blocked"}
            ))
            self.assertEqual(store.get_scheduled_dependency_event(first["event_id"])["status"], "blocked_by_upstream")
            plan_path.write_text("{}\n", encoding="utf-8")
            record = store.record_concept_refresh_plan(
                {
                    "plan_id": "plan-001",
                    "dependency_event_id": first["event_id"],
                    "upstream_run_id": "run-source",
                    "admission_state": "disabled",
                    "planner_version": "concept-refresh-planner.v2",
                    "source_manifest_path": str(manifest),
                    "source_manifest_hash": "sha256:manifest",
                    "plan_path": str(plan_path),
                    "plan_hash": "sha256:plan",
                    "status": "planned_disabled",
                },
                items=[{"concept_id": "concept-a", "coverage_status": "refreshable", "decision": "observe_only", "source_count": 1}],
            )
            self.assertFalse(record["deduplicated"])
            replay = store.record_concept_refresh_plan(
                {
                    "plan_id": "plan-001",
                    "dependency_event_id": first["event_id"],
                    "upstream_run_id": "run-source",
                    "admission_state": "disabled",
                    "planner_version": "concept-refresh-planner.v2",
                    "source_manifest_path": str(manifest),
                    "source_manifest_hash": "sha256:manifest",
                    "plan_path": str(plan_path),
                    "plan_hash": "sha256:plan",
                    "status": "planned_disabled",
                },
                items=[{"concept_id": "concept-a", "coverage_status": "refreshable", "decision": "observe_only", "source_count": 1}],
            )
            self.assertTrue(replay["deduplicated"])

    def test_accept_is_one_local_transaction_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            request = {
                "job_type": "sync",
                "loop_id": "daily-radar",
                "idempotency_key": "sync:2026-08-29",
                "profile": "fast-vector",
                "payload": {"source": "fixture"},
            }
            first = store.accept(request)
            second = store.accept(request)
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(first["job_id"], second["job_id"])
            self.assertEqual(first["run_id"], second["run_id"])
            self.assertEqual(store.list_runs(), [store.get_run(first["run_id"])])
            self.assertEqual(len(store.list_events(first["run_id"])), 1)

    def test_concurrent_accepts_share_idempotency_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            barrier = threading.Barrier(8)

            def submit(index: int) -> dict:
                barrier.wait()
                return store.accept(
                    {
                        "job_type": "sync",
                        "loop_id": "concurrent",
                        "idempotency_key": "same-key",
                        "payload": {"worker": index},
                    }
                )

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(submit, range(8)))
            self.assertEqual({item["job_id"] for item in results}, {results[0]["job_id"]})
            self.assertEqual({item["run_id"] for item in results}, {results[0]["run_id"]})
            self.assertEqual(len(store.list_runs()), 1)

    def test_checkpoint_upsert_and_event_sequence_are_transactional(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept({"job_type": "run", "loop_id": "loop"})
            run_id = accepted["run_id"]
            store.upsert_checkpoint(run_id, "source", "snapshot", input_hash="abc", payload={"n": 1})
            store.upsert_checkpoint(run_id, "source", "snapshot", input_hash="def", payload={"n": 2})
            store.append_run_event(run_id, "source/accepted", {"snapshot_id": "s1"})
            with store.connect() as connection:
                checkpoint = connection.execute("SELECT input_hash, payload_json FROM checkpoints WHERE run_id=?", (run_id,)).fetchone()
            self.assertEqual(checkpoint[0], "def")
            self.assertEqual(json.loads(checkpoint[1]), {"n": 2})
            events = store.list_events(run_id)
            self.assertEqual([event["seq"] for event in events], [1, 2])

    def test_corrupt_database_returns_read_only_legacy_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy = RunStore(root / "legacy")
            created = legacy.create({"loop_id": "legacy-loop"})
            db_path = root / "pm-system.db"
            db_path.write_bytes(b"this is not sqlite")
            fallback = open_coordination_store(db_path, root / "legacy")
            self.assertIsInstance(fallback, LegacyRunStoreReadOnlyAdapter)
            assert isinstance(fallback, LegacyRunStoreReadOnlyAdapter)
            self.assertTrue(fallback.read_only)
            self.assertEqual(fallback.state(created["run_id"])["run_id"], created["run_id"])
            with self.assertRaises(ReadOnlyStoreError):
                fallback.accept({"job_type": "run", "loop_id": "blocked"})

    def test_foreign_keys_and_busy_timeout_are_connection_local(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db", busy_timeout_ms=1000)
            values = store.pragmas()
            self.assertEqual(values["foreign_keys"], 1)
            self.assertEqual(values["busy_timeout"], 1000)

    def test_scheduled_occurrence_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            request = {
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-001",
                "occurrence_key": "pm-timeline-daily:20260907T053700Z",
                "scheduled_at": "2026-09-07T05:37:00Z",
                "local_scheduled_at": "2026-09-07T13:37:00+08:00",
                "deadline_at": "2026-09-07T05:52:00Z",
                "registry_hash": "sha256:test",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "trigger_kind": "calendar",
                "payload": {"schedule_key": "pm-timeline-daily"},
            }
            first = store.accept_scheduled_occurrence(request)
            second = store.accept_scheduled_occurrence(request)
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(first["job_id"], second["job_id"])
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM schedule_occurrences").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs WHERE occurrence_id='occ-001'").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM runs WHERE occurrence_id='occ-001'").fetchone()[0], 1)

    def test_scheduled_occurrence_lock_conflict_is_deferred_without_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            with store.transaction() as connection:
                connection.execute("INSERT INTO schedule_leases(lock_key,owner,lease_id,acquired_at,expires_at) VALUES(?,?,?,?,?)", ("pm-timeline-daily", "other", "lease-other", "2026-09-07T05:37:00Z", "2999-01-01T00:00:00Z"))
            result = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-002",
                "occurrence_key": "pm-timeline-daily:20260908T053700Z",
                "scheduled_at": "2026-09-08T05:37:00Z",
                "deadline_at": "2026-09-08T05:52:00Z",
                "registry_hash": "sha256:test",
                "lock_key": "pm-timeline-daily",
                "owner": "scheduler",
                "payload": {},
            })
            self.assertEqual(result["occurrence_state"], "deferred")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)

    def test_ops_alert_and_notification_are_fingerprint_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            first = store.upsert_ops_alert(
                fingerprint="sha256:alert-1",
                severity="P1",
                alert_type="run_failed",
                module="Worker",
                message="fixture failure",
                run_id="run-1",
            )
            second = store.upsert_ops_alert(
                fingerprint="sha256:alert-1",
                severity="P1",
                alert_type="run_failed",
                module="Worker",
                message="fixture failure refreshed",
                run_id="run-1",
            )
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(first["alert_id"], second["alert_id"])
            sent = store.record_notification_delivery(alert_id=first["alert_id"], fingerprint=first["fingerprint"], state="sent", delivered_at="2026-09-01T00:00:00Z")
            duplicate = store.record_notification_delivery(alert_id=first["alert_id"], fingerprint=first["fingerprint"], state="sent")
            self.assertFalse(sent["deduplicated"])
            self.assertTrue(duplicate["deduplicated"])
            self.assertEqual(len(store.list_ops_alerts(state="open")), 1)
            self.assertEqual(len(store.list_notification_deliveries(state="sent")), 1)


if __name__ == "__main__":
    unittest.main()
