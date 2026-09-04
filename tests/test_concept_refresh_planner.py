from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_refresh_planner import PLANNER_VERSION, build_plan  # noqa: E402
from pm_loop_scheduler import PMLoopDispatcher  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_worker import PMSystemWorker  # noqa: E402


def file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


class ConceptRefreshPlannerTests(unittest.TestCase):
    @staticmethod
    def _concept_schema(
        store: PMSystemStore,
        *,
        admission: str = "disabled",
        expires_at: str = "2999-09-07T08:00:00Z",
        renewal_policy: str = "snapshot_ttl",
        pending_count: int = 0,
        soft_limit: int = 2,
        hard_cap: int = 8,
    ) -> None:
        with store.transaction() as connection:
            connection.execute("CREATE TABLE concept_admissions (namespace_epoch TEXT PRIMARY KEY, admission_state TEXT NOT NULL, version INTEGER NOT NULL, expires_at TEXT, renewal_policy TEXT NOT NULL DEFAULT 'snapshot_ttl', updated_at TEXT NOT NULL)")
            connection.execute("CREATE TABLE concept_versions (concept_id TEXT NOT NULL, namespace_epoch TEXT NOT NULL)")
            connection.execute("CREATE TABLE concept_source_map (concept_id TEXT NOT NULL, namespace_epoch TEXT NOT NULL, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE concept_profile_admissions (workload TEXT NOT NULL, profile TEXT NOT NULL, namespace_epoch TEXT NOT NULL, pending_count INTEGER NOT NULL, pending_soft_limit INTEGER NOT NULL, outbox_hard_cap INTEGER NOT NULL, pause_fence TEXT NOT NULL, throttle_until TEXT, policy_hash TEXT, PRIMARY KEY(workload, profile, namespace_epoch))")
            connection.execute("INSERT INTO concept_admissions VALUES(?,?,?,?,?,?)", ("concept-epoch", admission, 7, expires_at, renewal_policy, "2026-09-07T08:00:00Z"))
            connection.execute("INSERT INTO concept_profile_admissions VALUES(?,?,?,?,?,?,?,?,?)", ("concept-semantic", "pm-semantic", "concept-epoch", pending_count, soft_limit, hard_cap, "open", None, "sha256:fixture-profile-policy"))
            connection.executemany("INSERT INTO concept_versions VALUES(?,?)", (("concept-a", "concept-epoch"), ("concept-b", "concept-epoch")))
            connection.executemany("INSERT INTO concept_source_map VALUES(?,?,?)", (("concept-a", "concept-epoch", "mapped"), ("concept-b", "concept-epoch", "quarantined")))

    @staticmethod
    def _coverage(path: Path, manifest: Path, *, statuses: tuple[str, str] = ("refreshable", "refreshable")) -> None:
        path.write_text(json.dumps({
            "schema": "concept-v11.source-coverage-report.v1",
            "status": "PASS",
            "report_hash": "sha256:coverage",
            "closure_hash": "sha256:closure",
            "source_manifest_hash": file_hash(manifest),
            "gate": {"p3_closed": True},
            "concepts": [
                {
                    "concept": "Alpha",
                    "concept_id": "concept-a",
                    "coverage_status": statuses[0],
                    "reference_count": 1,
                    "references": [{"source_uri": "viking://resources/shengsuan/source/alpha.md", "source_map_status": "mapped", "disposition": "mapped", "evidence_set_hash": "sha256:alpha"}],
                },
                {
                    "concept": "Beta",
                    "concept_id": "concept-b",
                    "coverage_status": statuses[1],
                    "reference_count": 1,
                    "references": [{"source_uri": "viking://resources/shengsuan/source/beta.md", "source_map_status": "mapped", "disposition": "mapped", "evidence_set_hash": "sha256:beta"}],
                },
            ],
        }), encoding="utf-8")

    @staticmethod
    def _pages(root: Path) -> Path:
        pages = root / "concepts" / "state" / "pages"
        pages.mkdir(parents=True)
        (pages / "Alpha.md").write_text("# Alpha\n", encoding="utf-8")
        (pages / "Beta.md").write_text("# Beta\n", encoding="utf-8")
        return pages.parents[1]

    class _ProjectionDispatcher:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def enqueue_concept_file(self, **kwargs):
            self.calls.append(kwargs)
            number = len(self.calls)
            return {
                "outbox_id": f"outbox-{number}",
                "idempotency_key": f"fixture:{kwargs['target_uri']}",
            }

    def test_disabled_planner_records_plan_without_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store)
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            manifest.write_text(json.dumps({"schema_version": "concept-source-manifest.v1", "metrics": {"mapped_active_source_count": 1}}), encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            event = store.append_scheduled_dependency_event(
                {
                    "event_key": "concept-refresh-planner:run-001:manifest:concept-refresh-planner.v2",
                    "dependent_schedule_key": "concept-refresh-planner",
                    "upstream_schedule_key": "weekly-sync-and-refresh",
                    "upstream_occurrence_id": "occ-source",
                    "upstream_run_id": "run-001",
                    "upstream_completed_at": "2026-09-07T08:00:00Z",
                    "source_manifest_path": str(manifest),
                    "source_manifest_hash": file_hash(manifest),
                    "handler_evidence_path": str(evidence),
                    "handler_evidence_hash": file_hash(evidence),
                    "planner_version": PLANNER_VERSION,
                    "status": "pending",
                }
            )
            store.mark_scheduled_dependency_event_consumed(
                event["event_id"], occurrence_id="occ-planner", outcome={"dependency_state": "accepted"}
            )
            with store.connect() as connection:
                before = {
                    name: connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                    for name in ("concept_versions", "outbox_items", "semantic_tasks", "generations")
                }
            result = build_plan(db_path=db, event_id=event["event_id"], artifact_dir=root / "run")
            self.assertEqual(result["status"], "planned_disabled")
            self.assertEqual(result["item_count"], 2)
            self.assertTrue((root / "run" / "concept-refresh-plan.v2.json").is_file())
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["side_effects"], {
                "concept_versions": 0,
                "hot_projection": 0,
                "generations": 0,
                "outbox": 0,
                "semantic_tasks": 0,
                "provider_calls": 0,
                "openviking_calls": 0,
            })
            self.assertEqual({item["concept_id"]: item["coverage_status"] for item in plan["items"]}, {
                "concept-a": "refreshable",
                "concept-b": "needs_repair",
            })
            with store.connect() as connection:
                after = {
                    name: connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                    for name in ("concept_versions", "outbox_items", "semantic_tasks", "generations")
                }
                rows = connection.execute("SELECT decision FROM concept_refresh_items ORDER BY concept_id").fetchall()
            self.assertEqual(before, after)
            self.assertEqual([row[0] for row in rows], ["observe_only", "observe_only"])

    def test_non_disabled_admission_remains_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store, admission="canary")
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-002:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-002",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            result = build_plan(db_path=db, event_id=event["event_id"], artifact_dir=root / "run")
            self.assertEqual(result["status"], "blocked")
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["reason"], "source_coverage_manifest_hash_mismatch")
            self.assertEqual(plan["items"][0]["decision"], "blocked")

    def test_matching_coverage_report_becomes_plan_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store)
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-coverage:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-coverage",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            coverage = root / "coverage.json"
            coverage.write_text(json.dumps({
                "schema": "concept-v11.source-coverage-report.v1",
                "status": "PASS",
                "report_hash": "sha256:coverage",
                "source_manifest_hash": file_hash(manifest),
                "closure_hash": "sha256:closure",
                "concepts": [
                    {"concept_id": "concept-a", "coverage_status": "substituted", "reference_count": 3},
                    {"concept_id": "concept-b", "coverage_status": "retired_with_evidence", "reference_count": 1},
                ],
            }), encoding="utf-8")
            result = build_plan(db_path=db, event_id=event["event_id"], artifact_dir=root / "run", coverage_report_path=coverage)
            self.assertEqual(result["status"], "planned_disabled")
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["source_coverage_input"]["status"], "PASS")
            self.assertEqual({row["concept_id"]: row["coverage_status"] for row in plan["items"]}, {
                "concept-a": "substituted",
                "concept-b": "retired_with_evidence",
            })
            self.assertTrue(all(row["evidence_hash"] == "sha256:coverage" for row in plan["items"]))

    def test_canary_queues_at_most_two_isolated_candidate_projections(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store, admission="canary")
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            coverage = root / "coverage.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            self._coverage(coverage, manifest)
            concept_root = self._pages(root)
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-canary:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-canary",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            dispatcher = self._ProjectionDispatcher()
            result = build_plan(
                db_path=db,
                event_id=event["event_id"],
                artifact_dir=root / "run",
                coverage_report_path=coverage,
                concept_root=concept_root,
                dispatcher=dispatcher,
                now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(result["status"], "planned_canary")
            self.assertEqual(len(dispatcher.calls), 2)
            self.assertTrue(all("/concepts/__canary__/concept-epoch/" in call["target_uri"] for call in dispatcher.calls))
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["side_effects"]["outbox"], 2)
            self.assertEqual(plan["side_effects"]["semantic_tasks"], 0)
            self.assertEqual(plan["publication"]["state"], "not_attempted")
            self.assertEqual({row["decision"] for row in plan["items"]}, {"canary_projection"})
            self.assertTrue(all(row["source_manifest_hash"] == file_hash(manifest) for row in plan["items"]))

    def test_canary_default_dispatcher_uses_the_shared_outbox_without_network_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store, admission="canary")
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            coverage = root / "coverage.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            self._coverage(coverage, manifest)
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-outbox:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-outbox",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            result = build_plan(
                db_path=db,
                event_id=event["event_id"],
                artifact_dir=root / "run",
                coverage_report_path=coverage,
                concept_root=self._pages(root),
                now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(result["status"], "planned_canary")
            with store.connect() as connection:
                rows = connection.execute(
                    "SELECT kind,processing_mode,resource_id,status FROM outbox_items ORDER BY resource_id"
                ).fetchall()
                semantic = connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0]
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(tuple(row[:2]) == ("concept", "vectors_only") for row in rows))
            self.assertTrue(all("viking://resources/concepts/__canary__/" in row[2] for row in rows))
            self.assertTrue(all(row[3] == "pending" for row in rows))
            self.assertEqual(semantic, 0)

    def test_incremental_respects_soft_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store, admission="incremental", pending_count=2, soft_limit=2)
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            coverage = root / "coverage.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            self._coverage(coverage, manifest)
            concept_root = self._pages(root)
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-capacity:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-capacity",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            result = build_plan(
                db_path=db,
                event_id=event["event_id"],
                artifact_dir=root / "run",
                coverage_report_path=coverage,
                concept_root=concept_root,
                dispatcher=self._ProjectionDispatcher(),
                now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(result["status"], "blocked")
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["reason"], "profile_capacity_exhausted")
            self.assertEqual(plan["side_effects"]["outbox"], 0)

    def test_canary_rejects_expired_admission_even_with_closed_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store, admission="canary", expires_at="2026-09-07T07:59:59Z")
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            coverage = root / "coverage.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            self._coverage(coverage, manifest)
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-expired:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-expired",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            result = build_plan(
                db_path=db,
                event_id=event["event_id"],
                artifact_dir=root / "run",
                coverage_report_path=coverage,
                concept_root=self._pages(root),
                dispatcher=self._ProjectionDispatcher(),
                now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(result["status"], "blocked")
            plan = json.loads((root / "run" / "concept-refresh-plan.v2.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["reason"], "admission_snapshot_expired")
            self.assertEqual(plan["side_effects"]["outbox"], 0)

    def test_incremental_continuous_admission_remains_live_after_snapshot_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(
                store,
                admission="incremental",
                expires_at="2026-09-07T07:59:59Z",
                renewal_policy="continuous",
            )
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            coverage = root / "coverage.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1"}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            self._coverage(coverage, manifest)
            event = store.append_scheduled_dependency_event({
                "event_key": "concept-refresh-planner:run-continuous:manifest:concept-refresh-planner.v2",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": "run-continuous",
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            store.mark_scheduled_dependency_event_consumed(event["event_id"], occurrence_id="occ-planner", outcome={})
            dispatcher = self._ProjectionDispatcher()
            result = build_plan(
                db_path=db,
                event_id=event["event_id"],
                artifact_dir=root / "run",
                coverage_report_path=coverage,
                concept_root=self._pages(root),
                dispatcher=dispatcher,
                now=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(result["status"], "planned_incremental")
            self.assertEqual(len(dispatcher.calls), 2)

    def test_scheduler_to_worker_disabled_replay_never_enters_production_queues(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            store = PMSystemStore(db)
            self._concept_schema(store)
            manifest = root / "source-manifest.json"
            evidence = root / "handler.json"
            manifest.write_text('{"schema_version":"concept-source-manifest.v1","metrics":{}}\n', encoding="utf-8")
            evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
            upstream = store.accept_scheduled_occurrence({
                "schedule_key": "weekly-sync-and-refresh",
                "occurrence_id": "occ-source",
                "occurrence_key": "weekly-sync-and-refresh:20260907T020500Z",
                "scheduled_at": "2026-09-07T02:05:00Z",
                "deadline_at": "2026-09-07T20:00:00Z",
                "registry_hash": "sha256:fixture",
                "lock_key": "weekly-sync-and-refresh",
                "job_type": "scheduled.weekly_sync",
                "loop_id": "weekly-sync-and-refresh",
                "trigger_kind": "calendar",
                "payload": {"schedule_key": "weekly-sync-and-refresh", "handler": "weekly_sync_and_refresh"},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (upstream["job_id"],))
                connection.execute("UPDATE runs SET status='completed' WHERE run_id=?", (upstream["run_id"],))
            event = store.append_scheduled_dependency_event({
                "event_key": f"concept-refresh-planner:{upstream['run_id']}:{file_hash(manifest)}:{PLANNER_VERSION}",
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": "occ-source",
                "upstream_run_id": upstream["run_id"],
                "upstream_completed_at": "2026-09-07T08:00:00Z",
                "source_manifest_path": str(manifest),
                "source_manifest_hash": file_hash(manifest),
                "handler_evidence_path": str(evidence),
                "handler_evidence_hash": file_hash(evidence),
                "planner_version": PLANNER_VERSION,
                "status": "pending",
            })
            dispatcher = PMLoopDispatcher(db, registry_path=ROOT / "scripts" / "schedule-registry.json", runtime_registry_path=None, lock_path=root / "dispatcher.lock")
            result = dispatcher.tick(now=datetime(2026, 9, 7, 8, 1, tzinfo=timezone.utc), mode="calendar")
            self.assertEqual(result["dependency"]["accepted"], 1)
            # The integration fixture is scoped to the dependency job. The
            # calendar tick may also accept unrelated due tasks; close those
            # fixture jobs so the Worker claims the planner deterministically.
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='completed' WHERE schedule_key != 'concept-refresh-planner'")
                connection.execute("UPDATE runs SET status='completed' WHERE schedule_key != 'concept-refresh-planner'")
            worker = PMSystemWorker(db, artifact_root=root / "runs", max_slots=1)
            self.assertEqual(worker.run_once(), "completed")
            stored = store.get_scheduled_dependency_event(event["event_id"])
            self.assertEqual(stored["status"], "consumed")
            with store.connect() as connection:
                plan = connection.execute("SELECT status FROM concept_refresh_runs").fetchone()
                queues = {
                    name: connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                    for name in ("outbox_items", "semantic_tasks", "generations")
                }
            self.assertEqual(plan[0], "planned_disabled")
            self.assertEqual(queues, {"outbox_items": 0, "semantic_tasks": 0, "generations": 0})


if __name__ == "__main__":
    unittest.main()
