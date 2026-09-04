from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_cockpit import CockpitHTTPServer, CockpitReadModel  # noqa: E402
from http_test_utils import create_loopback_server  # noqa: E402
from pm_system_gateway import SemanticGateway  # noqa: E402
from pm_system_scheduler import Scheduler  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from concept_v11_schema import migrate_schema  # noqa: E402
from concept_v11_schema_v2 import migrate_schema_v2  # noqa: E402
from pm_schedule_registry import load_registry  # noqa: E402


class CockpitTests(unittest.TestCase):
    @staticmethod
    def _coverage_report(*, status: str, needs_repair: int, p3_closed: bool) -> dict[str, object]:
        return {
            "schema": "concept-v11.source-coverage-report.v1",
            "status": status,
            "report_hash": "sha256:coverage",
            "concept_count": 45,
            "concept_status_counts": {"refreshable": 45 - needs_repair, "needs_repair": needs_repair},
            "concepts": [
                {
                    "concept": "退役概念" if index == 0 else f"概念{index}",
                    "coverage_status": "retired_with_evidence" if index == 0 and needs_repair == 0 else "refreshable",
                    "disposition_counts": {"mapped": 0 if index == 0 and needs_repair == 0 else 1},
                }
                for index in range(45)
            ],
            "gate": {"p3_closed": p3_closed},
        }

    def _concept_store(self, root: Path) -> PMSystemStore:
        store = PMSystemStore(root / "pm-system.db")
        first = store.acquire_migration_lease(
            migration_id="test-concept-v1",
            stage_id="C-SCHEMA",
            migration_epoch="test-concept-epoch",
            owner="test-owner",
        )
        migrate_schema(
            store,
            migration_id="test-concept-v1",
            migration_epoch="test-concept-epoch",
            owner="test-owner",
            lease_id=first["lease_id"],
        )
        store.release_migration_lease(lease_id=first["lease_id"])
        second = store.acquire_migration_lease(
            migration_id="test-concept-v2",
            stage_id="C-SCHEMA-V2",
            migration_epoch="test-concept-epoch",
            owner="test-owner",
        )
        migrate_schema_v2(
            store,
            migration_id="test-concept-v2",
            migration_epoch="test-concept-epoch",
            owner="test-owner",
            lease_id=second["lease_id"],
        )
        store.release_migration_lease(lease_id=second["lease_id"])
        return store

    def test_concept_projection_uses_true_latest_rows_and_unbounded_active_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._concept_store(Path(temp))
            epoch = "test-concept-epoch"
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE concept_admissions SET admission_snapshot_id=?,policy_version=?,operator=?,evidence_hash=?,reason=?,updated_at=? WHERE namespace_epoch=?",
                    ("snapshot-current", "policy-current", "operator-current", "sha256:admission-current", "fixture", "2026-09-02T11:00:00Z", epoch),
                )
                for suffix, updated_at in (("old", "2026-09-02T10:00:00Z"), ("new", "2026-09-02T11:00:00Z")):
                    connection.execute(
                        "INSERT INTO concept_candidates(candidate_id,concept_id,namespace_epoch,content,content_hash,policy_decision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        (f"candidate-{suffix}", "DataAgent", epoch, "fixture", f"hash-{suffix}", "hold", "2026-09-02T09:00:00Z", updated_at),
                    )
                    connection.execute(
                        "INSERT INTO concept_source_map(map_id,concept_id,namespace_epoch,source_id,source_uri,identity_method,status,evidence_refs_json,evidence_set_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (f"map-{suffix}", "DataAgent", epoch, f"source-{suffix}", f"viking://resources/{suffix}", "fixture", "quarantined", json.dumps([{"uri": f"viking://evidence/{suffix}", "sha256": f"sha256:{suffix}", "status": "verified"}]), f"sha256:source-{suffix}", "2026-09-02T09:00:00Z", updated_at),
                    )
                # The page's recent-generation sample is intentionally capped.
                # The active generation must be queried separately, rather than
                # inferred from those 20 display rows.
                for index in range(20):
                    connection.execute(
                        "INSERT INTO generations(generation_id,domain,generation_hash,status,created_at,active_at) VALUES(?,?,?,?,?,?)",
                        (f"staged-{index}", "concepts", f"hash-staged-{index}", "staged", "2099-01-01T00:00:00Z", f"2099-01-{index + 1:02d}T00:00:00Z"),
                    )
                connection.execute(
                    "INSERT INTO generations(generation_id,domain,generation_hash,status,created_at,active_at) VALUES(?,?,?,?,?,?)",
                    ("active-outside-sample", "concepts", "hash-active", "active", "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z"),
                )

            concepts = CockpitReadModel(store).snapshot(limit=20)["concepts"]
            self.assertEqual(concepts["candidates"][0]["candidate_id"], "candidate-new")
            self.assertEqual(concepts["source_map"][0]["map_id"], "map-new")
            self.assertEqual(concepts["admission"]["evidence_hash"], "sha256:admission-current")
            self.assertEqual(concepts["source_map"][0]["evidence_refs"], [{"uri": "viking://evidence/new", "sha256": "sha256:new", "status": "verified"}])
            self.assertEqual(concepts["generation"]["active"]["generation_id"], "active-outside-sample")
            self.assertNotIn("active-outside-sample", {item["generation_id"] for item in concepts["generation"]["recent"]})

    def test_concept_coverage_closure_resolves_historical_quarantine_without_hiding_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._concept_store(root)
            epoch = "test-concept-epoch"
            coverage_path = root / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
            coverage_path.parent.mkdir(parents=True)
            coverage_path.write_text(json.dumps(self._coverage_report(status="PASS", needs_repair=0, p3_closed=True)), encoding="utf-8")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO concept_source_map(map_id,concept_id,namespace_epoch,source_id,source_uri,identity_method,status,evidence_refs_json,evidence_set_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("historical-q", "concept-a", epoch, "source-a", "viking://resources/a", "fixture", "quarantined", "[]", "sha256:q", "2026-09-03T00:00:00Z", "2026-09-03T00:00:00Z"),
                )
            concepts = CockpitReadModel(store, runtime_home=root).snapshot()["concepts"]
            self.assertEqual(concepts["quarantine_count"], 1)
            self.assertEqual(concepts["effective_quarantine_count"], 0)
            self.assertEqual(concepts["quarantine_scope"], "historical_exclusion")
            self.assertNotIn("source_map_quarantine", {item["id"] for item in concepts["blockers"]})
            self.assertEqual(concepts["source_coverage"]["no_mapped_concepts"], [])
            self.assertEqual(concepts["source_coverage"]["retired_concepts"], ["退役概念"])

    def test_disabled_admission_is_recovery_gate_not_p0_workflow_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._concept_store(root)
            coverage_path = root / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
            coverage_path.parent.mkdir(parents=True)
            coverage_path.write_text(json.dumps(self._coverage_report(status="PASS", needs_repair=0, p3_closed=True)), encoding="utf-8")

            snapshot = CockpitReadModel(store, runtime_home=root).snapshot()
            blockers = {item["id"]: item for item in snapshot["concepts"]["blockers"]}
            gate = snapshot["gates"]["concept_view_gate"]

            self.assertEqual(blockers["admission_owner_decision"]["severity"], "P2")
            self.assertEqual(blockers["admission_owner_decision"]["status"], "requires_owner_decision")
            self.assertEqual(blockers["admission_owner_decision"]["refresh_trigger"], "pm_scheduler_dependency")
            self.assertNotIn("admission_not_incremental", blockers)
            self.assertEqual(gate["decision"], "recovery_gated")
            self.assertEqual(gate["workflow_status"], "recovery_gated")
            self.assertEqual(gate["refresh_trigger"], "pm_scheduler_dependency")

    def test_concept_coverage_hold_keeps_quarantine_as_p1(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._concept_store(root)
            epoch = "test-concept-epoch"
            coverage_path = root / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
            coverage_path.parent.mkdir(parents=True)
            coverage_path.write_text(json.dumps(self._coverage_report(status="HOLD", needs_repair=1, p3_closed=False)), encoding="utf-8")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO concept_source_map(map_id,concept_id,namespace_epoch,source_id,source_uri,identity_method,status,evidence_refs_json,evidence_set_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("unresolved-q", "concept-a", epoch, "source-a", "viking://resources/a", "fixture", "quarantined", "[]", "sha256:q", "2026-09-03T00:00:00Z", "2026-09-03T00:00:00Z"),
                )
            concepts = CockpitReadModel(store, runtime_home=root).snapshot()["concepts"]
            self.assertEqual(concepts["quarantine_count"], 1)
            self.assertEqual(concepts["effective_quarantine_count"], 1)
            self.assertEqual(concepts["quarantine_scope"], "active_unresolved")
            self.assertIn("source_map_quarantine", {item["id"] for item in concepts["blockers"]})

    def test_list_runs_projects_readable_failure_detail_separately_from_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept({"job_type": "report", "loop_id": "product-docs-gap-report", "idempotency_key": "run-detail:1"})
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE runs SET status='failed',error=? WHERE run_id=?",
                    ("dcdc3f04b150800fbc18", accepted["run_id"]),
                )
            store.append_run_event(
                accepted["run_id"],
                "run/failed",
                {"error": "ValueError: unsupported scheduled schedule_key: product-docs-gap-report", "error_fingerprint": "dcdc3f04b150800fbc18"},
                actor="coordination-worker",
            )
            # A scheduler receipt may append the fingerprint after the worker
            # exception.  The read model must still choose the readable event.
            store.append_run_event(
                accepted["run_id"],
                "run/failed",
                {"error": "dcdc3f04b150800fbc18"},
                actor="scheduler",
            )
            row = CockpitReadModel(store).list_runs()["runs"][0]
            self.assertEqual(row["error"], "dcdc3f04b150800fbc18")
            self.assertEqual(row["error_detail"], "ValueError: unsupported scheduled schedule_key: product-docs-gap-report")
            self.assertEqual(row["failure_reason"], row["error_detail"])
            self.assertEqual(row["last_event"]["payload"]["error"], "ValueError: unsupported scheduled schedule_key: product-docs-gap-report")

            scheduled_only = store.accept({"job_type": "report", "loop_id": "scheduled-only", "idempotency_key": "run-detail:2"})
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE runs SET status='failed',error=? WHERE run_id=?",
                    ("aabbccddeeff00112233", scheduled_only["run_id"]),
                )
            store.append_run_event(
                scheduled_only["run_id"],
                "scheduled/failed",
                {"reason": "handler exited with code 7", "error_fingerprint": "aabbccddeeff00112233"},
                actor="scheduler",
            )
            scheduled_row = next(item for item in CockpitReadModel(store).list_runs()["runs"] if item["run_id"] == scheduled_only["run_id"])
            self.assertEqual(scheduled_row["error_detail"], "handler exited with code 7")

    def test_reviews_classify_controlled_negative_tests_without_suppressing_other_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")

            def make_terminal_run(*, key: str, error: str, actor: str = "coordination-worker") -> str:
                accepted = store.accept({"job_type": "report", "loop_id": key, "idempotency_key": f"review-classification:{key}"})
                run_id = str(accepted["run_id"])
                with store.transaction() as connection:
                    connection.execute("UPDATE runs SET status='dead_letter',error=? WHERE run_id=?", (error, run_id))
                    connection.execute("UPDATE run_events SET actor=? WHERE run_id=? AND event_type='run/accepted'", (actor, run_id))
                return run_id

            controlled_run = make_terminal_run(key="p9-fixture", error="handler_exit_7", actor="pm-p9-dependency-replay")
            controlled_dir = root / ".codex" / "pm-loop" / "runs" / "fixture" / controlled_run
            controlled_dir.mkdir(parents=True)
            controlled_package = controlled_dir / "task-package.v1.json"
            controlled_package.write_text(json.dumps({
                "schema_version": "pm-task-package.v1",
                "execution": {"run_id": controlled_run, "trigger_kind": "manual_replay"},
                "outcome": {"execution_status": "failed", "impact": "handler_exit_7"},
                "business_summary": {"dependency_event": {"status": "blocked_by_upstream"}},
            }), encoding="utf-8")
            (controlled_dir / "request.json").write_text(json.dumps({
                "run_id": controlled_run,
                "replay_fixture": {
                    "stage": "P9.2",
                    "fixture": "fixed_local_upstream_completion",
                    "external_calls": {"oneapi": 0, "openviking": 0},
                },
            }), encoding="utf-8")
            store.upsert_checkpoint(controlled_run, "scheduled", "handler", artifact_uri=str(controlled_package))
            store.append_run_event(controlled_run, "scheduled_dependency_event/appended", {"status": "blocked_by_upstream"}, actor="coordination-worker")
            store.upsert_review_for_run(controlled_run)

            manual_replay = make_terminal_run(key="manual-replay", error="handler_exit_7", actor="pm-p9-dependency-replay")
            manual_dir = root / ".codex" / "pm-loop" / "runs" / "fixture" / manual_replay
            manual_dir.mkdir(parents=True)
            manual_package = manual_dir / "task-package.v1.json"
            manual_package.write_text(json.dumps({
                "schema_version": "pm-task-package.v1",
                "execution": {"run_id": manual_replay, "trigger_kind": "manual_replay"},
                "outcome": {"execution_status": "failed", "impact": "handler_exit_7"},
                "business_summary": {"dependency_event": {"status": "blocked_by_upstream"}},
            }), encoding="utf-8")
            store.upsert_checkpoint(manual_replay, "scheduled", "handler", artifact_uri=str(manual_package))
            store.upsert_review_for_run(manual_replay)

            business_failure = make_terminal_run(key="real-failure", error="8ac95c23d20f7b2a")
            store.append_run_event(
                business_failure,
                "run/failed",
                {"error": "ValueError: runtime registry rejected product-docs-gap-report", "error_fingerprint": "8ac95c23d20f7b2a"},
                actor="coordination-worker",
            )
            store.upsert_review_for_run(business_failure)

            reviews = {item["run_id"]: item for item in CockpitReadModel(store, runtime_home=root).snapshot()["reviews"]["items"]}
            controlled = reviews[controlled_run]["review_diagnosis"]
            self.assertTrue(reviews[controlled_run]["observed_at"])
            self.assertEqual(controlled["classification"], "controlled_negative_test")
            self.assertIn("按预期阻断", controlled["summary"])
            self.assertFalse(controlled["codex_advice"]["actionable"])
            self.assertEqual(controlled["needs_repair"], "no")

            unclassified = reviews[manual_replay]["review_diagnosis"]
            self.assertEqual(unclassified["classification"], "unclassified_failure")
            self.assertTrue(unclassified["codex_advice"]["actionable"])
            self.assertEqual(unclassified["needs_repair"], "confirm")

            operational = reviews[business_failure]["review_diagnosis"]
            self.assertEqual(operational["classification"], "business_failure")
            self.assertIn("runtime registry rejected", operational["summary"])
            self.assertTrue(operational["codex_advice"]["actionable"])
            self.assertIn("不要直接重跑", operational["codex_advice"]["prompt"])

    def test_snapshot_aggregates_modules_queues_provider_and_unknown_watermarks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            scheduler = Scheduler(store, max_slots=2)
            accepted = store.accept({"job_type": "run", "loop_id": "cockpit", "idempotency_key": "cockpit:1"})
            scheduler.claim_next()
            gateway = SemanticGateway(store)
            gateway.enqueue(resource_id="doc", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
            snapshot = CockpitReadModel(store).snapshot()
            self.assertTrue(snapshot["read_only"])
            self.assertEqual(snapshot["summary"]["active_codex_slots"], 1)
            self.assertEqual(snapshot["summary"]["max_codex_slots"], 2)
            self.assertEqual(snapshot["summary"]["queued_runs"], 0)
            self.assertEqual(snapshot["summary"]["outbox_pending"], 1)
            self.assertEqual(snapshot["summary"]["terminal_failed"], 0)
            self.assertIsNone(snapshot["watermarks"]["knowledge"])
            self.assertEqual(snapshot["status"], "unknown")
            self.assertEqual(snapshot["summary"]["execution"], "unknown")
            self.assertEqual(snapshot["summary"]["knowledge"], "unknown")
            modules = {item["module"]: item for item in snapshot["modules"]}
            self.assertEqual(modules["RunStore"]["status"], "healthy")
            # A free slot is capacity evidence, not proof that Scheduler is
            # present and healthy.  The read model must stay fail-closed.
            self.assertEqual(modules["Scheduler"]["status"], "unknown")
            self.assertEqual(modules["Worker"]["status"], "unknown")
            self.assertEqual(modules["Worker"]["freshness"], "stale")
            self.assertEqual(snapshot["run_id"] if "run_id" in snapshot else accepted["run_id"], accepted["run_id"])

    def test_schedule_timing_projects_calendar_and_dependency_without_creating_work(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        now = datetime(2026, 9, 2, 5, 0, tzinfo=timezone.utc)
        daily = CockpitReadModel._schedule_timing(registry.task("pm-timeline-daily"), now=now, timezone_name=registry.timezone_name)
        self.assertEqual(daily["trigger_kind"], "calendar")
        self.assertEqual(daily["next_run_at"], "2026-09-02T05:37:00Z")
        dependency = CockpitReadModel._schedule_timing(registry.task("concept-refresh-planner"), now=now, timezone_name=registry.timezone_name)
        self.assertEqual(dependency["trigger_kind"], "dependency")
        self.assertIsNone(dependency["next_run_at"])
        self.assertIn("weekly-sync-and-refresh", dependency["next_run_reason"])

    def test_knowledge_source_projection_reads_existing_ledgers_and_updates_etag(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = root / ".codex" / "skills" / "shengsuan-sync" / "state" / "ledger.json"
            ledger.parent.mkdir(parents=True)
            ledger.write_text(json.dumps({"doc-1": {"source": "ontology"}}), encoding="utf-8")
            model = CockpitReadModel(PMSystemStore(root / "pm-system.db"), runtime_home=root)
            first = model.snapshot()
            sources = first["schedules"]["knowledge_sources"]["items"]
            internal = next(item for item in sources if item["source_id"] == "shengsuan-internal")
            self.assertEqual(internal["status"], "observed")
            self.assertEqual(internal["record_count"], 1)
            self.assertEqual(internal["source_members"], [{"name": "ontology", "record_count": 1}])
            ledger.write_text(json.dumps({"doc-1": {"source": "ontology"}, "doc-2": {"source": "data-agent"}}), encoding="utf-8")
            self.assertNotEqual(first["source_version"], model.snapshot()["source_version"])

    def test_roles_project_allowlisted_history_and_future_schedule_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            reports = project / "docs" / "产品缺口周报"
            reports.mkdir(parents=True)
            older = reports / "产品缺口与安排建议-20260825.html"
            latest = reports / "产品缺口与安排建议-20260901.html"
            health_report = project / "docs" / "系统健康巡检报告-20260901.html"
            older.write_text("<html>old</html>", encoding="utf-8")
            latest.write_text("<html>latest</html>", encoding="utf-8")
            health_report.write_text("<html>health</html>", encoding="utf-8")
            old_mtime = time.time_ns() - 2_000_000_000
            os.utime(older, ns=(old_mtime, old_mtime))
            store = PMSystemStore(root / "pm-system.db")
            canonical = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
            store.set_schedule_registry_state(
                registry_version=canonical["registry_version"],
                registry_hash="sha256:fixture-registry",
                source_path="fixture://schedule-registry.json",
                canonical_json=json.dumps(canonical),
            )
            model = CockpitReadModel(store, runtime_home=root, project_root=project)
            snapshot = model.snapshot()
            role_outputs = snapshot["roles"]["historical_outputs"]
            self.assertEqual([item["name"] for item in role_outputs], [latest.name, older.name])
            latest_output = role_outputs[0]
            self.assertEqual(latest_output["role_id"], "product-gap-analyst")
            self.assertEqual(model.role_output_path(latest_output["output_id"]), latest.resolve())
            self.assertNotIn(health_report.name, [item["name"] for item in role_outputs])
            self.assertIsNone(model.role_output_path(model._role_output_id(health_report)))
            self.assertIsNone(model.role_output_path("../private-file"))
            future = {item["schedule_key"]: item for item in snapshot["roles"]["future_outputs"]}
            self.assertEqual(future["databuilder-product-gap-report"]["title"], "DataBuilder 产品缺口周报")
            self.assertEqual(future["databuilder-product-gap-report"]["role_id"], "product-gap-analyst")
            self.assertNotEqual(snapshot["source_version"], "sha256:fixture-registry")

    def test_snapshot_is_healthy_only_with_fresh_key_signals_and_watermarks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            Scheduler(store, max_slots=2)
            observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            with store.connect() as connection:
                for module in ("Worker", "OneAPI", "OpenViking", "Source", "Evidence", "Runtime"):
                    connection.execute(
                        "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                        (module, "healthy", observed_at, "{}", "test"),
                    )
                connection.execute(
                    "INSERT INTO source_snapshots(snapshot_id,source_id,source_revision,content_sha256,manifest_json,status,captured_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("snapshot-1", "source", "r1", "sha", "{}", "committed", observed_at, observed_at),
                )
                connection.execute(
                    "INSERT INTO generations(generation_id,domain,generation_hash,status,source_watermark,knowledge_watermark,created_at,active_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("generation-1", "product", "g1", "active", "r1", "k1", observed_at, observed_at),
                )
            snapshot = CockpitReadModel(store).snapshot()
            self.assertEqual(snapshot["status"], "healthy")
            self.assertEqual(snapshot["summary"]["execution"], "unknown")
            self.assertEqual(snapshot["summary"]["knowledge"], "healthy")

    def test_explicit_missing_structured_watermark_remains_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            store.put_watermark(
                source_domain="pm-runtime",
                watermark_name="active_generation",
                captured_at=100,
                value={"status": "missing", "reason": "refresh_disabled"},
                producer="test",
                state="missing",
            )
            snapshot = CockpitReadModel(store).snapshot()
            self.assertIsNone(snapshot["watermarks"]["active_generation"])
            self.assertEqual(snapshot["status"], "unknown")

    def test_snapshot_exposes_semantic_acceptance_states(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = SemanticGateway(store)
            accepted = gateway.enqueue(
                resource_id="doc",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
            )
            dispatch = gateway.dispatch_once(limit=1)[0]
            gateway.ack(accepted["outbox_id"], openviking_task_id="task-1", dispatch_token=dispatch["dispatch_token"], semantic_status="accepted")
            model = CockpitReadModel(store)
            snapshot = model.snapshot()
            self.assertEqual(snapshot["queues"]["semantic"]["accepted"], 1)
            self.assertEqual(snapshot["queues"]["semantic"]["processing"], 0)
            with store.transaction() as connection:
                connection.execute("UPDATE semantic_tasks SET status='processing'")
            snapshot = model.snapshot()
            self.assertEqual(snapshot["queues"]["semantic"]["processing"], 1)
            with store.transaction() as connection:
                connection.execute("UPDATE semantic_tasks SET status='degraded'")
            snapshot = model.snapshot()
            self.assertEqual(snapshot["queues"]["semantic"]["degraded"], 1)

    def test_snapshot_exposes_terminal_failed_and_dead_letter_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,resource_id,revision_id,processing_mode,provider,profile,payload_json,status,attempt,next_attempt_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("outbox-failed", "key-failed", "doc", "r1", "vectors_only", "openviking", "pm", "{}", "failed", 1, None, "2026-08-29T00:00:00Z", "2026-08-29T00:00:01Z"),
                )
                connection.execute(
                    "INSERT INTO semantic_tasks(semantic_task_id,dedupe_key,outbox_id,resource_id,revision_id,processing_mode,provider,status,attempt,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("semantic-failed", "semantic-key", "outbox-failed", "doc", "r1", "vectors_only", "openviking", "failed", 1, "2026-08-29T00:00:00Z", "2026-08-29T00:00:01Z"),
                )
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,resource_id,revision_id,processing_mode,provider,profile,payload_json,status,attempt,next_attempt_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("outbox-dead", "key-dead", "doc2", "r2", "vectors_only", "openviking", "pm", "{}", "dead_letter", 4, None, "2026-08-29T00:00:00Z", "2026-08-29T00:00:01Z"),
                )
                connection.execute(
                    "INSERT INTO semantic_tasks(semantic_task_id,dedupe_key,outbox_id,resource_id,revision_id,processing_mode,provider,status,attempt,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("semantic-dead", "semantic-dead-key", "outbox-dead", "doc2", "r2", "vectors_only", "openviking", "dead_letter", 4, "2026-08-29T00:00:00Z", "2026-08-29T00:00:01Z"),
                )
            snapshot = CockpitReadModel(store).snapshot()
            self.assertEqual(snapshot["summary"]["terminal_failed"], 2)
            self.assertEqual(snapshot["summary"]["dead_letter"], 2)
            self.assertEqual(snapshot["queues"]["outbox"]["failed"], 1)
            self.assertEqual(snapshot["queues"]["outbox"]["dead_letter"], 1)
            self.assertEqual(snapshot["queues"]["semantic"]["failed"], 1)
            self.assertEqual(snapshot["queues"]["semantic"]["dead_letter"], 1)

    def test_fresh_scheduler_tick_overrides_freeze_era_maintenance_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                    ("Scheduler", "maintenance", "2026-08-30T00:56:11Z", '{"migration_freeze":true}', "freeze-era"),
                )
            store.set_schedule_registry_state(registry_version=1, registry_hash="sha256:test", source_path="fixture", canonical_json="{}", state="valid")
            tick_id = store.start_scheduler_tick(scheduler_id="test", mode="calendar", registry_hash="sha256:test", started_at=observed_at)
            store.finish_scheduler_tick(tick_id)
            module = {item["module"]: item for item in CockpitReadModel(store).modules()}["Scheduler"]
            self.assertEqual(module["status"], "healthy")
            self.assertEqual(module["freshness"], "fresh")
            self.assertEqual(module["details"]["scheduler_tick_id"], tick_id)

    def test_fresh_worker_lease_or_event_overrides_freeze_era_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            Scheduler(store, max_slots=1)
            observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)",
                    ("Worker", "maintenance", "2026-08-30T00:56:11Z", '{"migration_freeze":true}', "freeze-era"),
                )
                connection.execute(
                    "UPDATE execution_slots SET status='leased',lease_id='lease-test',run_id=NULL,heartbeat_at=?,leased_at=?,expires_at=?,pid=123 WHERE slot_id='codex-1'",
                    (observed_at, observed_at, "2999-01-01T00:00:00Z"),
                )
            module = {item["module"]: item for item in CockpitReadModel(store).modules()}["Worker"]
            self.assertEqual(module["status"], "healthy")
            self.assertEqual(module["freshness"], "fresh")
            self.assertEqual(module["details"]["evidence_type"], "lease")

    def test_historical_terminal_failures_do_not_become_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,resource_id,revision_id,processing_mode,provider,profile,payload_json,status,attempt,next_attempt_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("old-failed", "old-failed-key", "doc", "r1", "vectors_only", "openviking", "pm", "{}", "failed", 1, None, "2026-08-29T00:00:00Z", "2026-08-29T00:00:01Z"),
                )
            snapshot = CockpitReadModel(store).snapshot()
            self.assertEqual(snapshot["summary"]["historical_terminal_failed"], 1)
            self.assertEqual(snapshot["summary"]["incident_count"], 0)
            self.assertNotIn(snapshot["status"], {"incident", "degraded"})
            self.assertEqual(snapshot["ops_attention_view"]["p0_p1_open"], 0)

    def test_persistent_open_p1_still_marks_summary_as_incident(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            store.upsert_ops_alert(
                fingerprint="sha256:open-p1",
                severity="P1",
                alert_type="run_failed",
                module="Worker",
                message="当前 Worker 失败",
            )
            snapshot = CockpitReadModel(store).snapshot()
            self.assertEqual(snapshot["summary"]["incident_count"], 1)
            self.assertEqual(snapshot["status"], "incident")

    def test_persistent_historical_terminal_alert_is_visible_but_not_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-cockpit-historical-terminal",
                "occurrence_key": "pm-timeline-daily:cockpit-historical-terminal",
                "scheduled_at": "2026-09-02T05:37:00Z",
                "deadline_at": "2026-09-02T06:00:00Z",
                "registry_hash": "sha256:test-cockpit-historical-terminal",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute(
                    "UPDATE schedule_occurrences SET state='failed',updated_at='2026-09-02T06:00:00Z' WHERE occurrence_id=?",
                    (accepted["occurrence_id"],),
                )
                connection.execute(
                    "UPDATE jobs SET status='failed',updated_at='2026-09-02T06:00:00Z' WHERE job_id=?",
                    (accepted["job_id"],),
                )
                connection.execute(
                    "UPDATE runs SET status='failed',updated_at='2026-09-02T06:00:00Z' WHERE run_id=?",
                    (accepted["run_id"],),
                )
            store.upsert_ops_alert(
                fingerprint="sha256:historical-terminal-alert",
                severity="P1",
                alert_type="run_failed",
                module="Worker",
                message="历史 Worker 失败",
                run_id=accepted["run_id"],
            )

            snapshot = CockpitReadModel(store).snapshot()

            self.assertEqual(snapshot["summary"]["incident_count"], 0)
            self.assertNotEqual(snapshot["status"], "incident")
            item = next(item for item in snapshot["ops_attention_view"]["items"] if item["fingerprint"] == "sha256:historical-terminal-alert")
            self.assertEqual(item["state"], "resolved")

    def test_http_get_is_read_only_and_supports_etag_and_required_views(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm-system.db")
            accepted = store.accept({"job_type": "run", "loop_id": "cockpit", "idempotency_key": "cockpit:http"})
            model = CockpitReadModel(store)
            server = create_loopback_server(CockpitHTTPServer, ("127.0.0.1", 0), model)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            before = (root / "pm-system.db").read_bytes()
            try:
                start = time.perf_counter()
                with urlopen(base + "/api/control-plane/v4/summary", timeout=5) as response:
                    body = json.loads(response.read())
                    etag = response.headers["ETag"]
                    self.assertEqual(response.status, 200)
                elapsed_ms = (time.perf_counter() - start) * 1000
                self.assertLess(elapsed_ms, 500)
                self.assertTrue(body["read_only"])
                request = Request(base + "/api/control-plane/v4/summary", headers={"If-None-Match": etag})
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request, timeout=5)
                self.assertEqual(raised.exception.code, 304)
                for suffix in ("modules", "incidents", "queues", "runs", f"runs/{accepted['run_id']}"):
                    with urlopen(base + "/api/control-plane/v4/" + suffix, timeout=5) as response:
                        value = json.loads(response.read())
                    self.assertTrue(value["read_only"])
                request = Request(base + "/api/control-plane/v4/summary", method="POST", data=b"{}")
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request, timeout=5)
                self.assertEqual(raised.exception.code, 405)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)
            self.assertEqual(before, (root / "pm-system.db").read_bytes())


if __name__ == "__main__":
    unittest.main()
