import hashlib
import json
import re
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_schema import migrate_schema, record_model_policy  # noqa: E402
from concept_v11_schema_v2 import migrate_schema_v2, record_model_resolution_append  # noqa: E402
from pm_loop_control_plane_server import ControlPlane
from pm_system_cockpit import CockpitReadModel
from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore, SCHEMA_VERSION, StoreUnavailable


class PMWorkbenchV13Tests(unittest.TestCase):
    def make_store(self, root: Path) -> tuple[Path, PMSystemStore]:
        path = root / "pm-system.db"
        store = PMSystemStore(path)
        return path, store

    def test_control_plane_navigation_pages_are_all_routeable(self) -> None:
        page = Path(__file__).parents[1] / "web" / "pm-loop-control-plane" / "index.html"
        html = page.read_text(encoding="utf-8")
        pages_match = re.search(r"const PAGES=\[(.*?)\];", html)
        self.assertIsNotNone(pages_match)
        pages = set(re.findall(r'"([^"\\]+)"', pages_match.group(1)))
        nav_pages = set(re.findall(r'data-page="([^"]+)"', html))
        self.assertTrue(nav_pages)
        self.assertTrue(nav_pages <= pages, sorted(nav_pages - pages))

    def test_schedule_view_exposes_knowledge_sources_and_next_run_projection(self) -> None:
        page = Path(__file__).parents[1] / "web" / "pm-loop-control-plane" / "index.html"
        html = page.read_text(encoding="utf-8")
        for marker in ("当前知识源", "下次运行", "knowledge_sources", "waiting_dependency"):
            self.assertIn(marker, html)

    def test_v4_concept_view_consumes_live_operational_fields(self) -> None:
        page = Path(__file__).parents[1] / "web" / "pm-loop-control-plane" / "index.html"
        html = page.read_text(encoding="utf-8")
        self.assertIn("function v4Concepts()", html)
        for marker in (
            "summary.active_count",
            "generation.active",
            "summary.source_status_counts",
            "concepts.alignment",
            "concepts.model_resolutions",
            "concepts.admission_events",
            "Source-map 隔离风险",
            "function conceptEvidence",
            "function conceptEvidenceFields",
            "JSON.stringify(value)",
            "source_uri:\"来源 URI\"",
            "sha256:\"SHA-256\"",
            "[\"映射\",{status:item.status,updated_at:item.updated_at,expires_at:item.expires_at}]",
            "item.evidence_set_hash",
            "ledger_projection_outbox_id",
            "item.call_id",
            "item.admission_snapshot_id",
            "source_coverage",
            "P3 来源闭合",
            "候选报告需重算",
        ):
            self.assertIn(marker, html)
        self.assertNotIn('String(item[1]||"").trim()', html)
        self.assertNotIn("[object Object]", html)
        self.assertNotIn("254 条当前缺口", html)

    def test_p3_coverage_rejects_stale_candidate_and_review_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, _store = self.make_store(root)
            coverage_path = root / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
            candidate_path = root / ".codex" / "pm-loop" / "runs" / "concept-v11" / "p3-source-candidates-current-coverage.json"
            package_path = root / "project" / "docs" / "03-产品架构" / "概念自动刷新-P3来源处置决策工作包-20260902.json"
            coverage_path.parent.mkdir(parents=True)
            candidate_path.parent.mkdir(parents=True)
            package_path.parent.mkdir(parents=True)
            coverage = {
                "schema": "concept-v11.source-coverage-report.v1",
                "status": "HOLD",
                "report_hash": "sha256:current-coverage",
                "concept_count": 45,
                "reference_count": 381,
                "concept_status_counts": {"refreshable": 2, "needs_repair": 43},
                "ledger_entry_count": 0,
                "concepts": [{"concept": "无来源概念", "coverage_status": "needs_repair", "disposition_counts": {"mapped": 0}}],
                "gate": {"p3_closed": False},
            }
            candidate = {
                "schema": "concept-v11.source-candidate-discovery.v1",
                "report_hash": "sha256:old-candidates",
                "coverage_report_hash": "sha256:old-coverage",
                "candidate_count": 626,
                "qualified_candidate_count": 460,
            }
            package = {
                "schema": "concept-v11.p3-review-package.v1",
                "package_hash": "sha256:old-package",
                "inputs": {"coverage_report_hash": "sha256:old-coverage", "candidate_report_hash": "sha256:old-candidates"},
                "worksheet_rows": [{"review_decision": ""}],
            }
            coverage_path.write_text(json.dumps(coverage), encoding="utf-8")
            candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
            package_path.write_text(json.dumps(package), encoding="utf-8")

            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            model = CockpitReadModel(reader, runtime_home=root, project_root=root / "project")
            first_version = model.snapshot()["source_version"]
            p3 = model.snapshot()["concepts"]["source_coverage"]
            self.assertEqual(p3["source_status"], "observed")
            self.assertEqual(p3["status"], "hold")
            self.assertEqual(p3["no_mapped_concepts"], ["无来源概念"])
            self.assertEqual(p3["candidate_discovery"]["status"], "stale")
            self.assertEqual(p3["candidate_discovery"]["reason"], "coverage_report_hash_mismatch")
            self.assertIsNone(p3["candidate_discovery"].get("qualified_candidate_count"))
            self.assertEqual(p3["review_package"]["status"], "stale")
            self.assertEqual(p3["review_package"]["review_decision_count"], 0)

            coverage["report_hash"] = "sha256:recomputed-coverage"
            coverage_path.write_text(json.dumps(coverage), encoding="utf-8")
            self.assertNotEqual(first_version, model.snapshot()["source_version"])

    def test_control_plane_reads_p3_package_from_explicit_canonical_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / ".codex"
            state_dir = codex_root / "pm-loop"
            PMSystemStore(state_dir / "state" / "pm-system.db")
            coverage_path = state_dir / "state" / "concept-v11" / "source-coverage-current.json"
            candidate_path = state_dir / "runs" / "concept-v11" / "p3-source-candidates-current-coverage.json"
            package_path = root / "canonical-project" / "docs" / "03-产品架构" / "概念自动刷新-P3来源处置决策工作包-20260902.json"
            coverage_path.parent.mkdir(parents=True)
            candidate_path.parent.mkdir(parents=True)
            package_path.parent.mkdir(parents=True)
            coverage_path.write_text(json.dumps({"schema": "concept-v11.source-coverage-report.v1", "status": "HOLD", "report_hash": "sha256:current", "concept_status_counts": {}, "concepts": [], "gate": {}}), encoding="utf-8")
            candidate_path.write_text(json.dumps({"schema": "concept-v11.source-candidate-discovery.v1", "report_hash": "sha256:old-candidate", "coverage_report_hash": "sha256:old"}), encoding="utf-8")
            package_path.write_text(json.dumps({"schema": "concept-v11.p3-review-package.v1", "package_hash": "sha256:old-package", "inputs": {"coverage_report_hash": "sha256:old", "candidate_report_hash": "sha256:old-candidate"}, "worksheet_rows": []}), encoding="utf-8")

            controller = ControlPlane(
                state_dir,
                root / "adapter.py",
                state_dir / "runtime",
                codex_root,
                root / "web",
                evidence_project_root=root / "canonical-project",
            )
            p3 = controller.v44_cockpit.snapshot()["concepts"]["source_coverage"]
            self.assertEqual(p3["review_package"]["status"], "stale")
            self.assertEqual(Path(p3["review_package"]["path"]).resolve(), package_path.resolve())

    def test_read_only_store_never_enables_wal_or_migrates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            before = path.stat().st_mtime_ns
            with self.assertRaises(ValueError):
                PMSystemStore(path, read_only=True)
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            with reader.connect() as connection:
                self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("CREATE TABLE should_not_exist(id INTEGER)")
            self.assertEqual(store.schema_version(), SCHEMA_VERSION)
            self.assertEqual(path.stat().st_mtime_ns, before)

    def test_snapshot_has_gate_and_target_views_without_fabricating_plans(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, _store = self.make_store(Path(temp))
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            snapshot = CockpitReadModel(reader).snapshot()
            self.assertTrue(snapshot["read_only"])
            self.assertEqual(snapshot["source_status"], "observed")
            self.assertEqual(snapshot["source_cursor"], snapshot["source_version"])
            self.assertIn("runtime_read_model_gate", snapshot["gates"])
            self.assertIn("concept_view_gate", snapshot["gates"])
            # No registry is loaded yet, so the plan view tells the truth
            # without fabricating a plan.  Reviews are a direct terminal-Run
            # projection and are therefore an observed empty collection.
            self.assertEqual(snapshot["plans"]["status"], "not_implemented")
            self.assertEqual(snapshot["reviews"]["status"], "observed")
            self.assertEqual(snapshot["concepts"]["source_status"], "not_implemented")
            self.assertEqual(snapshot["concepts"]["status"], "not_implemented")
            self.assertEqual(len(snapshot["roles"]["items"]), 6)

    def test_registry_plans_terminal_reviews_and_work_item_links_are_observed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            canonical = {
                "timezone": "Asia/Shanghai",
                "tasks": [{
                    "schedule_key": "pm-timeline-daily",
                    "calendar": {"kind": "daily", "hour": 13, "minute": 37},
                    "deadline": "PT15M",
                    "handler": "pm_timeline_daily",
                    "concurrency_key": "pm-timeline",
                    "retry": {"max_attempts": 1, "backoff": "PT5M"},
                }],
            }
            store.set_schedule_registry_state(
                registry_version=1,
                registry_hash="sha256:registry",
                source_path="fixture://schedule-registry.json",
                canonical_json=json.dumps(canonical),
            )
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-plan-1",
                "occurrence_key": "pm-timeline-daily:fixture",
                "scheduled_at": "2026-09-01T05:37:00Z",
                "local_scheduled_at": "2026-09-01T13:37:00+08:00",
                "deadline_at": "2999-09-01T05:52:00Z",
                "registry_hash": "sha256:registry",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {},
            })
            with store.transaction() as connection:
                connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (accepted["job_id"],))
                connection.execute("UPDATE runs SET status='completed' WHERE run_id=?", (accepted["run_id"],))
                connection.execute(
                    "INSERT INTO checkpoints(run_id,stage,checkpoint_key,artifact_uri,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (accepted["run_id"], "scheduled", "handler", "fixture://handler.json", "{}", "2026-09-01T06:00:00Z", "2026-09-01T06:00:00Z"),
                )
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            snapshot = CockpitReadModel(reader, runtime_home=Path(temp)).snapshot()
            self.assertEqual(snapshot["plans"]["status"], "observed")
            self.assertEqual(snapshot["plans"]["items"][0]["plan_id"], "schedule:pm-timeline-daily")
            self.assertEqual(snapshot["work_items"][0]["plan_id"], "schedule:pm-timeline-daily")
            self.assertEqual(snapshot["reviews"]["items"][0]["review_state"], "result_ready")
            self.assertEqual(snapshot["reviews"]["items"][0]["run_id"], accepted["run_id"])

    def test_canonical_activity_operation_and_gate_manifest_are_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            accepted = store.accept_scheduled_occurrence({
                "schedule_key": "pm-timeline-daily",
                "occurrence_id": "occ-ledger-1",
                "occurrence_key": "pm-timeline-daily:ledger-1",
                "scheduled_at": "2026-09-01T05:37:00Z",
                "local_scheduled_at": "2026-09-01T13:37:00+08:00",
                "deadline_at": "2999-09-01T05:52:00Z",
                "registry_hash": "sha256:ledger",
                "lock_key": "pm-timeline-daily",
                "job_type": "scheduled.pm_timeline_daily",
                "loop_id": "pm-timeline-daily",
                "payload": {"handler": "pm_timeline_daily"},
            })
            store.record_activity_event(
                event_type="test/ledger",
                actor="test",
                run_id=accepted["run_id"],
                job_id=accepted["job_id"],
                occurrence_id=accepted["occurrence_id"],
                source_cursor="ledger:1",
                idempotency_key="ledger:test:1",
            )
            store.set_workbench_gate_manifest({
                "gate_id": "runtime_read_model_gate",
                "manifest_version": "test.v1",
                "owner": "test",
                "observed_at": "2026-09-01T06:00:00Z",
                "expires_at": "2999-09-01T06:15:00Z",
                "required_checks": [{"check_id": "smoke", "status": "pass"}],
                "source_hashes": {"smoke": "sha256:smoke"},
                "decision": "enabled",
                "protected_modules": ["schedules"],
                "reason": "fixture",
            })
            scheduler = Scheduler(store, max_slots=1)
            claim = scheduler.claim_next(worker_id="ledger-test")
            self.assertIsNotNone(claim)
            self.assertTrue(scheduler.release(claim["lease_id"], status="completed"))
            with store.connect() as connection:
                operation = connection.execute("SELECT operation_id FROM operations WHERE operation_key=?", ("schedule:pm-timeline-daily",)).fetchone()
            self.assertIsNotNone(operation)
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            snapshot = CockpitReadModel(reader, runtime_home=Path(temp)).snapshot()
            self.assertTrue(any(item["event_type"] == "test/ledger" for item in snapshot["activity"]))
            self.assertEqual(snapshot["reviews"]["items"][0]["run_id"], accepted["run_id"])
            self.assertTrue(any(item.get("operation_key") == "schedule:pm-timeline-daily" for item in snapshot["operations"]))
            self.assertEqual(snapshot["gates"]["runtime_read_model_gate"]["decision"], "enabled")
            self.assertEqual(snapshot["gates"]["runtime_read_model_gate"]["reason"], "fixture")

    def test_snapshot_tolerates_partial_concept_domain_and_without_rowid_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            with store.transaction() as connection:
                connection.execute("CREATE TABLE projection_without_rowid (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID")
                connection.execute("INSERT INTO projection_without_rowid(key,value) VALUES('a','b')")
                connection.execute("CREATE TABLE concept_admissions (namespace_epoch TEXT PRIMARY KEY, admission_state TEXT, version INTEGER, updated_at TEXT)")
                connection.execute("INSERT INTO concept_admissions(namespace_epoch,admission_state,version,updated_at) VALUES('e1','disabled',1,'2026-08-31T00:00:00Z')")
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            snapshot = CockpitReadModel(reader).snapshot()
            self.assertEqual(snapshot["concepts"]["source_status"], "not_implemented")
            self.assertEqual(snapshot["concepts"]["status"], "not_implemented")
            self.assertIsNone(snapshot["concepts"]["admission"])
            self.assertTrue(snapshot["source_version"].startswith("sha256:"))

    def test_concept_projection_uses_global_counts_and_quarantine_sample(self) -> None:
        """List limits must never turn a 25-row concept domain into five facts."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, store = self.make_store(root)
            store.set_migration_freeze(
                migration_id="v45-test",
                migration_epoch="v45-test",
                stage_id="G9",
                owner="test",
                deadline_at="2099-01-01T00:00:00Z",
                state="released",
            )
            first = store.acquire_migration_lease(
                migration_id="concept-v1",
                stage_id="C-SCHEMA",
                migration_epoch="v45-test",
                owner="test",
            )
            migrate_schema(
                store,
                migration_id="concept-v1",
                migration_epoch="v45-test",
                owner="test",
                lease_id=first["lease_id"],
            )
            store.release_migration_lease(lease_id=first["lease_id"])
            second = store.acquire_migration_lease(
                migration_id="concept-v2",
                stage_id="C-SCHEMA-V2",
                migration_epoch="v45-test",
                owner="test",
            )
            migrate_schema_v2(
                store,
                migration_id="concept-v2",
                migration_epoch="v45-test",
                owner="test",
                lease_id=second["lease_id"],
            )
            store.release_migration_lease(lease_id=second["lease_id"])
            policy = record_model_policy(
                store,
                {
                    "policy_version": "policy-auto",
                    "provider": "oneapi",
                    "requested_model": "auto",
                    "allowed_models": [],
                    "status": "active",
                },
            )
            record_model_resolution_append(
                store,
                {
                    "resolution_id": "resolution-1",
                    "run_id": "run-1",
                    "call_id": "call-1",
                    "stage": "provider-shadow",
                    "attempt": 1,
                    "model_requested": "auto",
                    "model_resolved": "test-model",
                    "resolution_status": "resolved",
                    "policy_version": policy["policy_version"],
                    "provider": "oneapi",
                    "resolution_changed": 0,
                    "model_input_hash": "sha256:input",
                    "evidence_hash": "sha256:evidence",
                },
            )
            with store.transaction() as connection:
                for index in range(25):
                    concept_id = f"concept-{index:02d}"
                    version_id = f"version-{index:02d}"
                    generation_id = f"legacy-{index:02d}"
                    timestamp = f"2026-09-01T00:{index:02d}:00Z"
                    connection.execute(
                        "INSERT INTO concept_versions(version_id,concept_id,namespace_epoch,version,generation_id,content,content_hash,compiler_version,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (version_id, concept_id, "v45-test", "v1", generation_id, "body", f"sha256:version-{index}", "legacy-import", "active", timestamp),
                    )
                    connection.execute(
                        "INSERT INTO concept_publish_ledger(publish_id,concept_id,namespace_epoch,version_id,current_generation,desired_hot_generation,projection_state,operator,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (f"publish-{index:02d}", concept_id, "v45-test", version_id, generation_id, generation_id, "legacy_imported", "test", timestamp, timestamp),
                    )
                    connection.execute(
                        "INSERT INTO concept_hot_projection(concept_id,namespace_epoch,generation_id,projection_state,observed_content_hash,updated_at) VALUES(?,?,?,?,?,?)",
                        (concept_id, "v45-test", generation_id, "legacy_imported", f"sha256:version-{index}", timestamp),
                    )
                    source_status = "mapped" if index < 3 else "quarantined"
                    connection.execute(
                        "INSERT INTO concept_source_map(map_id,concept_id,namespace_epoch,source_id,source_uri,identity_method,status,evidence_refs_json,lineage_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (f"map-{index:02d}", concept_id, "v45-test", f"source-{index:02d}", f"viking://resources/source/{index:02d}", "fixture", source_status, "[]", "{}", timestamp, timestamp),
                    )
                connection.execute(
                    "INSERT INTO concept_candidates(candidate_id,concept_id,namespace_epoch,content,content_hash,policy_decision,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    ("candidate-1", "concept-00", "v45-test", "candidate", "sha256:candidate", "hold", "quarantined", "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z"),
                )

            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            concepts = CockpitReadModel(reader, runtime_home=Path(temp)).snapshot(limit=5)["concepts"]
            self.assertEqual(concepts["summary"]["active_count"], 25)
            self.assertEqual(concepts["summary"]["hot_count"], 25)
            self.assertEqual(concepts["summary"]["publish_ledger_count"], 25)
            self.assertEqual(concepts["summary"]["candidate_count"], 1)
            self.assertEqual(concepts["summary"]["source_status_counts"], {"mapped": 3, "quarantined": 22})
            self.assertEqual(concepts["quarantine_count"], 22)
            self.assertEqual(len(concepts["active"]), 5)
            self.assertEqual(len(concepts["alignment"]), 5)
            self.assertTrue(all(item["alignment_status"] == "aligned" for item in concepts["alignment"]))
            self.assertEqual(len(concepts["source_map"]), 5)
            self.assertTrue(all(item["status"] == "quarantined" for item in concepts["source_map"]))
            self.assertEqual(concepts["source_map_sample_scope"], "quarantined")
            self.assertEqual(concepts["admission"]["admission_state"], "disabled")
            self.assertIsNone(concepts["generation"]["active"])
            self.assertEqual(concepts["policy"]["policy_version"], "policy-auto")
            self.assertEqual(concepts["summary"]["model_resolution_count"], 1)
            self.assertEqual(concepts["summary"]["admission_event_count"], 1)
            self.assertEqual({item["id"] for item in concepts["blockers"]}, {"admission_owner_decision", "active_generation_missing", "source_map_quarantine"})

    def test_content_watermark_uses_content_hash_not_capture_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO source_snapshots(snapshot_id,source_id,source_revision,content_sha256,manifest_json,status,captured_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("s1", "source", "r1", "sha256:content", "{}", "committed", "2026-08-31T00:00:00Z", "2026-08-31T00:00:00Z"),
                )
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            value = CockpitReadModel(reader).snapshot()["watermarks"]["content"]
            self.assertEqual(value["value"], "sha256:content")
            self.assertNotEqual(value["value"], value["captured_at"])

    def test_source_version_changes_for_error_and_generation_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            model = CockpitReadModel(reader)
            before = model.snapshot()["source_version"]
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO error_events(occurred_at,fingerprint,severity,module,message) VALUES(?,?,?,?,?)",
                    ("2026-08-31T00:00:00Z", "fp", "P1", "test", "failure"),
                )
            after_error = model.snapshot()["source_version"]
            self.assertNotEqual(before, after_error)
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO generations(generation_id,domain,generation_hash,status,created_at) VALUES(?,?,?,?,?)",
                    ("g1", "concepts", "sha256:g", "staged", "2026-08-31T00:00:01Z"),
                )
            self.assertNotEqual(after_error, model.snapshot()["source_version"])

    def test_run_detail_exposes_checkpoint_artifact_evidence_and_disposition(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path, store = self.make_store(Path(temp))
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO jobs(job_id,idempotency_key,job_type,run_id,status,queued_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    ("j1", "i1", "diagnostic", "r1", "failed", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z"),
                )
                connection.execute(
                    "INSERT INTO runs(run_id,job_id,loop_id,status,created_at,updated_at,terminal_reason) VALUES(?,?,?,?,?,?,?)",
                    ("r1", "j1", "diagnostic", "permanent_failed", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z", "permanent"),
                )
                connection.execute(
                    "INSERT INTO checkpoints(run_id,stage,checkpoint_key,input_hash,artifact_uri,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("r1", "analysis", "a", "sha256:i", "artifact://a", "{}", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z"),
                )
            reader = PMSystemStore(path, auto_migrate=False, read_only=True)
            detail = CockpitReadModel(reader).run_detail("r1")
            self.assertEqual(len(detail["checkpoints"]), 1)
            self.assertEqual(detail["artifacts"][0]["artifact_uri"], "artifact://a")
            self.assertEqual(detail["evidence"]["status"], "not_recorded")
            self.assertEqual(detail["disposition"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
