from __future__ import annotations

import json
import gzip
import multiprocessing
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_control_plane import OpenVikingClient  # noqa: E402
from pm_loop_control_plane_server import ControlPlane, ControlPlaneHTTPServer  # noqa: E402
from pm_loop_action_runner import execute_actions  # noqa: E402
from pm_loop_analysis import build_decision, execute_analysis  # noqa: E402
from pm_loop_runner import parse_last_json, run_once  # noqa: E402
from pm_loop_runtime import RunStore  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from concept_learning import ConceptLearningStore, make_candidate  # noqa: E402
from http_test_utils import create_loopback_server  # noqa: E402


def append_events_in_process(state_dir: str, run_id: str, worker: int, count: int, barrier: object) -> None:
    store = RunStore(Path(state_dir))
    barrier.wait()
    for index in range(count):
        store.append(run_id, "test/concurrent", {"worker": worker, "index": index})


def fixture_snapshot(path: Path) -> Path:
    value = {
        "schema_version": "pm-loop.snapshot.v1",
        "snapshot_id": "snapshot-fixture-001",
        "collected_at": "2026-08-15T00:00:00Z",
        "summary": {"launchd_jobs": 8, "skills": 31, "openviking_status": "healthy", "timeline_events": 8},
        "sources": {
            "launchd": {"status": "healthy", "count": 8},
            "skills": {"status": "healthy", "count": 31},
            "openviking": {"status": "healthy", "count": 8},
            "pm_timeline": {"status": "healthy", "count": 8},
        },
    }
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return path


@contextmanager
def running_http_server(controller: ControlPlane):
    """Run a real loopback server for one test and always release its port."""
    server = create_loopback_server(ControlPlaneHTTPServer, ("127.0.0.1", 0), controller)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def http_json(url: str, method: str = "GET", payload: object = None) -> tuple[int, dict]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"} if body is not None else {}
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


def http_json_with_headers(url: str, method: str = "GET", payload: object = None) -> tuple[int, dict, dict]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"} if body is not None else {}
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read()), dict(response.headers.items())
    except HTTPError as error:
        return error.code, json.loads(error.read()), dict(error.headers.items())


def http_status_headers(url: str, method: str, payload: object = None) -> tuple[int, bytes, dict]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"} if body is not None else {}
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, response.read(), dict(response.headers.items())
    except HTTPError as error:
        return error.code, error.read(), dict(error.headers.items())


def wait_for_run_status(base: str, run_id: str, expected: set[str] | None = None, timeout: float = 8.0) -> dict:
    expected = expected or {"completed", "failed", "cancelled", "rejected"}
    deadline = time.time() + timeout
    state: dict = {}
    while time.time() < deadline:
        status, state = http_json(f"{base}/api/runs/{run_id}")
        if status == 200 and state.get("status") in expected:
            return state
        time.sleep(0.05)
    return state


def isolated_controller(root: Path, snapshot: Path | None = None, adapter: Path | None = None) -> ControlPlane:
    return ControlPlane(
        root / "state",
        adapter or ROOT / "scripts" / "pm_loop_control_plane.py",
        ROOT,
        root / "codex",
        ROOT / "web" / "pm-loop-control-plane",
        snapshot,
    )


class PMLoopRuntimeTests(unittest.TestCase):
    def test_control_plane_snapshot_is_single_flight_cached_for_poll_burst(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            with patch.object(
                controller,
                "_control_plane_snapshot_uncached",
                return_value={"schema_version": "test", "rows": []},
            ) as build:
                first = controller.control_plane_snapshot()
                first["rows"].append("caller mutation")
                second = controller.control_plane_snapshot()

            self.assertEqual(build.call_count, 1)
            self.assertEqual(second["rows"], [])

    def test_read_only_projection_does_not_repair_missing_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = RunStore(Path(temp) / "state")
            request = store.create({"loop_id": "daily-radar"})
            state_path = store.paths(request["run_id"]).state
            state_path.unlink()
            projected = store.state_read_only(request["run_id"])
            self.assertEqual(projected["loop_id"], "daily-radar")
            self.assertFalse(state_path.exists())
            self.assertEqual(store.list_states_read_only()[0]["run_id"], request["run_id"])
            self.assertFalse(state_path.exists())

    def test_v4_cockpit_write_attempt_returns_explicit_405(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True)
            store = PMSystemStore(db_path)
            store.accept({"job_type": "run", "loop_id": "v4-method", "idempotency_key": "v4-method:1"})
            del store
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                for method in ("HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"):
                    status, raw, headers = http_status_headers(
                        f"{base}/api/control-plane/v4/retention",
                        method=method,
                        payload={} if method == "POST" else None,
                    )
                    self.assertEqual(status, 405, method)
                    self.assertEqual(headers.get("Cache-Control"), "no-store", method)
                    self.assertEqual(headers.get("Allow"), "GET" if method == "HEAD" else None, method)
                    if method == "HEAD":
                        self.assertEqual(raw, b"")
                    else:
                        body = json.loads(raw)
                        self.assertTrue(body["read_only"], method)
                        self.assertEqual(body["allow"], ["GET"], method)

    def test_v4_cockpit_missing_run_is_non_retryable_404(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True)
            PMSystemStore(db_path)
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, body = http_json(f"{base}/api/control-plane/v4/runs/missing-run")
            self.assertEqual(status, 404)
            self.assertEqual(body["error"], "run_not_found")
            self.assertEqual(body["run_id"], "missing-run")
            self.assertTrue(body["read_only"])

    def test_concurrent_append_is_linearized_across_store_instances(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp) / "state"
            store = RunStore(state_dir)
            run_id = store.create({"loop_id": "concurrent-thread-test"})["run_id"]
            workers = 12
            barrier = threading.Barrier(workers)

            def append_once(worker: int) -> None:
                independent_store = RunStore(state_dir)
                barrier.wait()
                independent_store.append(run_id, "test/concurrent", {"worker": worker})

            with ThreadPoolExecutor(max_workers=workers) as executor:
                list(executor.map(append_once, range(workers)))

            events = store.events_for(run_id)
            self.assertEqual([event["seq"] for event in events], list(range(1, workers + 2)))
            self.assertEqual(store.state(run_id)["events_count"], workers + 1)

    def test_concurrent_append_is_linearized_across_processes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp) / "state"
            store = RunStore(state_dir)
            run_id = store.create({"loop_id": "concurrent-process-test"})["run_id"]
            context = multiprocessing.get_context("fork")
            workers = 4
            events_per_worker = 8
            barrier = context.Barrier(workers)
            processes = [
                context.Process(
                    target=append_events_in_process,
                    args=(str(state_dir), run_id, worker, events_per_worker, barrier),
                )
                for worker in range(workers)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=15)
                self.assertEqual(process.exitcode, 0)

            expected_count = 1 + workers * events_per_worker
            events = store.events_for(run_id)
            self.assertEqual([event["seq"] for event in events], list(range(1, expected_count + 1)))
            self.assertEqual(store.state(run_id)["events_count"], expected_count)

    def test_openviking_probe_failure_is_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / "ovcli.conf"
            config.write_text(json.dumps({"url": "http://127.0.0.1:1"}), encoding="utf-8")
            result = OpenVikingClient(config).snapshot()
            self.assertEqual(result["status"], "probe_inconclusive")
            self.assertEqual(result["skill_search"], [])

    def test_parse_pretty_printed_adapter_result(self) -> None:
        payload = {"status": "ok", "snapshot_path": "/tmp/snapshot.json", "summary": {"skills": 31}}
        rendered = json.dumps(payload, ensure_ascii=False, indent=2)
        self.assertEqual(parse_last_json(rendered), payload)

    def test_run_lifecycle_and_read_only_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            store = RunStore(root / "state")
            request = store.create({"loop_id": "daily-radar", "permission_mode": "draft"})
            state = run_once(store, request["run_id"], snapshot_path=snapshot)
            self.assertEqual(state["status"], "completed")
            self.assertEqual(state["runtime"], "codex")
            self.assertEqual(state["snapshot_id"], "snapshot-fixture-001")
            self.assertTrue(store.paths(request["run_id"]).draft.is_file())
            event_types = [event["type"] for event in store.events_for(request["run_id"])]
            self.assertEqual(event_types[0], "run/created")
            self.assertIn("source/completed", event_types)
            self.assertIn("verification/completed", event_types)
            self.assertEqual(event_types[-1], "run/completed")
            before = len(event_types)
            self.assertEqual(run_once(store, request["run_id"], snapshot_path=snapshot)["status"], "completed")
            self.assertEqual(len(store.events_for(request["run_id"])), before)
            restarted = RunStore(root / "state")
            restarted.paths(request["run_id"]).state.unlink()
            recovered = restarted.state(request["run_id"])
            self.assertEqual(recovered["status"], "completed")
            self.assertEqual(recovered["snapshot_id"], "snapshot-fixture-001")

    def test_cancel_marker_is_terminal_and_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            store = RunStore(root / "state")
            request = store.create({"loop_id": "daily-radar", "permission_mode": "report"})
            store.paths(request["run_id"]).cancel_marker.write_text("cancel\n", encoding="utf-8")
            state = run_once(store, request["run_id"], snapshot_path=snapshot)
            self.assertEqual(state["status"], "cancelled")
            self.assertEqual(store.events_for(request["run_id"])[-1]["type"], "run/cancelled")

    def test_codex_analysis_writes_verified_artifacts_and_filters_unknown_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            store = RunStore(root / "state")
            request = store.create({"loop_id": "daily-radar", "permission_mode": "draft", "loop_contract": {"analysis_instruction": "排序"}})

            def fake_invoker(prompt: str, timeout: int, codex_root: Path):
                self.assertIn("snapshot-fixture-001", prompt)
                return (0, json.dumps({
                    "answerability": "partial",
                    "confidence": 0.82,
                    "conclusion": {"headline": "部分可判断", "rationale": ["需要继续核验"]},
                    "findings": [{"id": "f-1", "title": "有一个信号", "summary": "事实摘要", "severity": "medium", "evidence_refs": ["source:pm_timeline", "source:does-not-exist"]}],
                    "gaps": ["缺少项目上下文"],
                    "proposed_actions": [{"id": "a-1", "title": "写跟进草稿", "kind": "safe_draft", "requires_gate": True}],
                }, ensure_ascii=False), "", "/fake/codex")

            analysis, decision, report = execute_analysis(store, request["run_id"], json.loads(snapshot.read_text(encoding="utf-8")), root / "codex", fake_invoker)
            self.assertEqual(analysis["schema_version"], "pm-loop.analysis.v2")
            self.assertEqual(analysis["findings"][0]["evidence_refs"], ["source:pm_timeline"])
            self.assertTrue(report.is_file())
            self.assertFalse(decision["gate"]["required"])

    def test_approved_action_binds_token_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            store = RunStore(root / "state")
            request = store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})

            def fake_invoker(prompt: str, timeout: int, codex_root: Path):
                return (0, json.dumps({
                    "answerability": "answerable",
                    "confidence": 0.9,
                    "conclusion": {"headline": "可执行", "rationale": []},
                    "findings": [],
                    "gaps": [],
                    "proposed_actions": [{"id": "a-1", "title": "生成草稿", "kind": "safe_draft", "requires_gate": True}],
                }), "", "/fake/codex")

            analysis, decision, _ = execute_analysis(store, request["run_id"], json.loads(snapshot.read_text(encoding="utf-8")), root / "codex", fake_invoker)
            store.append(request["run_id"], "gate/requested", {"gate_id": request["run_id"], "snapshot_id": snapshot.stem, "actions": [{"action_id": "a-1", "action_hash": decision["proposed_actions"][0]["action_hash"]}]})
            store.append(request["run_id"], "gate/approved", {"gate_token": decision["gate"]["token"], "snapshot_id": decision["snapshot_id"]}, actor="reviewer")
            store.append(request["run_id"], "action/queued", {"gate_token": decision["gate"]["token"]}, actor="control-plane")
            first = execute_actions(store, request["run_id"])
            count = len(store.events_for(request["run_id"]))
            second = execute_actions(store, request["run_id"])
            self.assertEqual(first["status"], "completed")
            self.assertEqual(second["status"], "completed")
            self.assertEqual(len(store.events_for(request["run_id"])), count)
            self.assertTrue((store.paths(request["run_id"]).root / "action" / "a-1.receipt.json").is_file())


class PMLoopHTTPTests(unittest.TestCase):
    def test_candidate_review_projection_enriches_read_apis_without_mutating_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill_root = root / "codex" / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "pages").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps(
                    {
                        "数据搜索": {
                            "status": "active",
                            "category": "数据消费",
                            "sources": ["viking://source/active"],
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (skill_root / "config.yaml").write_text(
                "concepts:\n  - name: 数据搜索\n    aliases: [DataSearch, data search]\n",
                encoding="utf-8",
            )
            (skill_root / "state" / "pages" / "数据搜索.md").write_text("保留行\n旧行\n", encoding="utf-8")
            learning = ConceptLearningStore(skill_root)
            update = learning.save_candidate(
                make_candidate(
                    concept="数据搜索",
                    kind="refresh",
                    before="保留行\n旧行\n",
                    content="保留行\n新行\n增加行\n",
                    source_refs=["viking://source/update"],
                ),
                "保留行\n新行\n增加行\n",
            )
            new_concept = learning.save_candidate(
                make_candidate(
                    concept="Data-Search",
                    kind="new-concept",
                    content="第一行\n第二行\n",
                    source_refs=["viking://source/new"],
                ),
                "第一行\n第二行\n",
            )
            manifest_path = learning.candidate_path(str(new_concept["candidate_id"]))
            manifest_before = manifest_path.read_bytes()
            controller = isolated_controller(root)

            update_projection = controller.candidate_projection(learning.read_candidate(str(update["candidate_id"])))
            self.assertEqual(update_projection["proposal_kind"], "update")
            self.assertEqual(update_projection["proposal_kind_label"], "更新")
            self.assertEqual(update_projection["kindLabel"], "更新")
            self.assertEqual(update_projection["diff_added_lines"], 2)
            self.assertEqual(update_projection["diff_removed_lines"], 1)
            self.assertEqual(update_projection["diff_line_count"], 3)
            self.assertEqual(update_projection["diffStats"], {"added": 2, "removed": 1, "total": 3})
            self.assertFalse(update_projection["suspected_existing"])
            self.assertEqual(update_projection["existing_active_concept"], "数据搜索")

            projected = controller.candidate_projection(learning.read_candidate(str(new_concept["candidate_id"])))
            self.assertEqual(projected["proposal_kind"], "new_concept")
            self.assertEqual(projected["proposalKindLabel"], "新概念")
            self.assertEqual(projected["diffAddedLines"], 2)
            self.assertEqual(projected["diffRemovedLines"], 0)
            self.assertEqual(projected["diffLineCount"], 2)
            self.assertTrue(projected["suspectedExisting"])
            self.assertEqual(projected["suspectedExistingMatches"], ["数据搜索"])
            self.assertEqual(projected["suspectedActiveConcept"], "数据搜索")
            self.assertIn("Active", projected["suspectedExistingReason"])

            with running_http_server(controller) as base:
                status, listing = http_json(base + "/api/candidates")
                self.assertEqual(status, 200)
                listed = next(item for item in listing["candidates"] if item["candidate_id"] == new_concept["candidate_id"])
                self.assertEqual(listed["proposal_kind_label"], "新概念")
                self.assertEqual(listed["suspected_existing_matches"], ["数据搜索"])

                status, detail = http_json(base + f"/api/candidates/{new_concept['candidate_id']}")
                self.assertEqual(status, 200)
                self.assertEqual(detail["diff_line_count"], 2)

                status, concepts = http_json(base + "/api/concepts")
                self.assertEqual(status, 200)
                candidate_only = next(item for item in concepts["concepts"] if item["name"] == "Data-Search")
                self.assertEqual(candidate_only["candidate"]["proposalKindLabel"], "新概念")
                self.assertEqual(candidate_only["candidate"]["suspectedActiveConcept"], "数据搜索")

            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            raw = learning.read_candidate(str(new_concept["candidate_id"]))
            self.assertNotIn("proposal_kind", raw)
            self.assertNotIn("diff_line_count", raw)
            self.assertNotIn("suspected_existing", raw)

    def test_concept_projection_reads_do_not_create_queue_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            lock_path = controller.queue_file_lock_path
            self.assertFalse(lock_path.exists())
            self.assertEqual(controller.queue_status()["items"], [])
            controller.concepts()
            controller.concept_recheck_status()
            self.assertFalse(lock_path.exists())

    def test_candidate_list_is_summary_first_and_supports_status_pagination(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill_root = root / "codex" / "skills" / "shengsuan-concepts"
            (skill_root / "state").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text("{}", encoding="utf-8")
            learning = ConceptLearningStore(skill_root)
            for index in range(3):
                learning.save_candidate(
                    make_candidate(
                        concept=f"候选{index}",
                        kind="new-concept",
                        content="证据正文\n" * 4,
                        source_refs=[f"viking://source/{index}"],
                        evidence=[{"uri": f"viking://source/{index}", "quote": "长证据" * 200}],
                        status="ready_for_review" if index < 2 else "changes_requested",
                    ),
                    "证据正文\n" * 4,
                )
            controller = isolated_controller(root)
            model = controller.candidates_read_model({"status": ["ready_for_review"], "page": ["1"], "page_size": ["1"]})
            self.assertEqual(model["pagination"]["total"], 2)
            self.assertEqual(len(model["candidates"]), 1)
            summary = model["candidates"][0]
            self.assertTrue(summary["details_available"])
            self.assertFalse(summary["details_loaded"])
            self.assertNotIn("evidence", summary)
            detail = controller.candidate_projection(
                learning.read_candidate(summary["candidate_id"]),
                include_details=True,
            )
            self.assertTrue(detail["details_loaded"])
            self.assertIn("evidence", detail)

    def test_control_plane_summary_has_stable_version_and_etag(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            first = controller.control_plane_summary()
            second = controller.control_plane_summary()
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(first["candidate_version"])
            self.assertTrue(first["read_only"])
            self.assertLess(len(json.dumps(first, ensure_ascii=False)), 10000)
            candidate_source = first["sources"]["candidates"]
            if candidate_source is not None:
                self.assertIsInstance(candidate_source, dict)
                self.assertIn("digest", candidate_source)
                self.assertNotIsInstance(candidate_source.get("rows"), list)
            compact = controller._compact_signatures({"rows": [["a", 1, 2]]})
            self.assertEqual(compact["rows"]["count"], 1)
            self.assertIn("digest", compact["rows"])

    def test_name_fingerprint_projection_separates_operational_and_content_baselines(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state"
            (state_root / "full-inventory").mkdir(parents=True)
            (state_root / "weekly-source-revisions.json").write_text(
                json.dumps(
                    {
                        "schema_version": "shengsuan-concepts.weekly-source-revisions.v2",
                        "revision_mode": "name_hash",
                        "revision_kind": "name_hash",
                        "name_hash_rule": "source+path+name:v1",
                        "updated_at": "2026-08-22T00:00:00Z",
                        "revisions": {"viking://docs/a": "sha256:name-a", "viking://docs/b": "sha256:name-b"},
                        "rows": {
                            "viking://docs/a": {"revision": "sha256:name-a"},
                            "viking://docs/b": {"revision": "unknown"},
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (state_root / "source-manifest.json").write_text(
                json.dumps(
                    {
                        "revision_mode": "name_hash",
                        "name_hash_rule": "source+path+name:v1",
                        "metrics": {
                            "document_count": 2,
                            "name_hash_observed": 2,
                            "name_hash_coverage": 1.0,
                            "unmapped_document_count": 1,
                            "conflict_document_count": 1,
                            "unmapped_active_source_count": 1,
                            "conflict_active_source_count": 1,
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (state_root / "content-audit-queue.json").write_text(
                json.dumps(
                    {
                        "status": "pending",
                        "metrics": {"selected_count": 3, "processed_count": 1, "mismatch_count": 0, "failed_count": 0},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            inventory_root = state_root / "full-inventory"
            (inventory_root / "latest-result.json").write_text(
                json.dumps({"run_id": "inventory-1", "status": "completed", "resource_count": 2}),
                encoding="utf-8",
            )
            (inventory_root / "incremental-baseline.json").write_text(
                json.dumps(
                    {
                        "status": "incomplete",
                        "baseline_ready": False,
                        "resource_count": 2,
                        "source_hash_count": 1,
                        "source_hash_coverage": 0.5,
                    }
                ),
                encoding="utf-8",
            )

            projection = isolated_controller(root)._name_fingerprint_status()

            self.assertEqual(projection["mode"], "name_hash")
            self.assertEqual(projection["name_hash_prefix"], "namepath-v1:")
            self.assertEqual(projection["coverage"], {"count": 2, "total": 2, "ratio": 1.0})
            self.assertEqual(projection["coverage_scope"], "name_baseline")
            self.assertEqual(projection["unmapped_document_count"], 1)
            self.assertEqual(projection["conflict_document_count"], 1)
            self.assertEqual(projection["operational_baseline"]["status"], "incomplete")
            self.assertFalse(projection["content_baseline"]["ready"])
            self.assertEqual(projection["content_baseline"]["coverage"], 0.5)
            self.assertEqual(projection["audit_queue"]["pending"], 3)
            self.assertEqual(projection["audit_queue"]["processed"], 1)

    def test_name_fingerprint_projection_does_not_fabricate_missing_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            projection = isolated_controller(Path(temp))._name_fingerprint_status()
            self.assertFalse(projection["available"])
            self.assertIsNone(projection["coverage"]["count"])
            self.assertIsNone(projection["unmapped_count"])
            self.assertIsNone(projection["conflict_count"])
            self.assertEqual(projection["audit_queue"]["status"], "not_recorded")

    def test_name_fingerprint_projection_prefers_sync_ledger_scope_over_inventory_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state"
            state_root.mkdir(parents=True)
            (state_root / "source-manifest.meta.json").write_text(
                json.dumps(
                    {
                        "revision_mode": "name_hash",
                        "name_hash_prefix": "namepath-v1:",
                        "metrics": {
                            "document_count": 7164,
                            "name_hash_observed": 7164,
                            "name_hash_coverage": 1.0,
                            "ledger_document_count": 1429,
                            "ledger_name_hash_count": 1429,
                            "ledger_name_hash_coverage": 1.0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            projection = isolated_controller(root)._name_fingerprint_status()

            self.assertEqual(projection["coverage_scope"], "sync_ledger")
            self.assertEqual(projection["coverage"], {"count": 1429, "total": 1429, "ratio": 1.0})
            self.assertEqual(projection["inventory_coverage"], {"count": 7164, "total": 7164, "ratio": 1.0})

    def test_deep_inventory_status_projects_stage_and_delta_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state" / "full-inventory"
            (state_root / "runs" / "inventory-1").mkdir(parents=True)
            (state_root / "latest-result.json").write_text(
                json.dumps(
                    {
                        "run_id": "inventory-1",
                        "status": "completed",
                        "resource_count": 10,
                        "stage_progress": {
                            "document_read": {
                                "status": "completed",
                                "processed": 10,
                                "total": 10,
                                "elapsed_seconds": 2.5,
                                "cache_hits": 3,
                                "cache_misses": 7,
                                "eta_seconds": 0,
                            }
                        },
                        "changed_documents": {"changed_count": 2, "unchanged_count": 8},
                        "content_dedup": {
                            "document_count": 10,
                            "unique_content_count": 8,
                            "duplicate_document_count": 2,
                            "duplicate_ratio": 0.2,
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            controller = isolated_controller(root)
            status = controller.deep_inventory_status()
            self.assertEqual(status["stage_progress"]["document_read"]["processed"], 10)
            self.assertEqual(status["stage_progress"]["document_read"]["cache_hits"], 3)
            self.assertEqual(status["changed_documents"]["changed_count"], 2)
            self.assertEqual(status["content_dedup"]["duplicate_document_count"], 2)

    def test_deep_inventory_status_derives_legacy_document_progress_without_timing(self) -> None:
        """Legacy aggregate counters are useful, but missing stages stay unknown."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state" / "full-inventory"
            state_root.mkdir(parents=True)
            (state_root / "latest-result.json").write_text(
                json.dumps(
                    {
                        "run_id": "legacy-inventory-1",
                        "status": "completed",
                        "resource_count": 10,
                        "read_count": 10,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            controller = isolated_controller(root)
            status = controller.deep_inventory_status()
            stages = status["stage_progress"]
            self.assertEqual(
                set(stages),
                {"document_read", "term_aggregation", "llm_reduce", "candidate_write"},
            )
            self.assertEqual(status["stage_progress_source"], "legacy_derived")
            self.assertEqual(stages["document_read"]["status"], "completed")
            self.assertEqual(stages["document_read"]["processed"], 10)
            self.assertEqual(stages["document_read"]["total"], 10)
            self.assertEqual(stages["document_read"]["telemetry_source"], "legacy_derived")
            self.assertNotIn("elapsed_seconds", stages["document_read"])
            for name in ("term_aggregation", "llm_reduce", "candidate_write"):
                self.assertEqual(stages[name]["status"], "not_recorded")
                self.assertNotIn("processed", stages[name])
                self.assertNotIn("total", stages[name])
                self.assertNotIn("elapsed_seconds", stages[name])

    def test_deep_inventory_status_derives_all_historical_stage_counters(self) -> None:
        """The shipped pre-telemetry run remains readable in the Control Plane."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state" / "full-inventory"
            run_root = state_root / "runs" / "legacy-inventory-2"
            run_root.mkdir(parents=True)
            (state_root / "latest-result.json").write_text(
                json.dumps(
                    {
                        "run_id": "legacy-inventory-2",
                        "status": "completed",
                        "resource_count": 5735,
                        "read_count": 5735,
                        "term_count": 36334,
                        "decision_count": 160,
                        "candidate_count": 139,
                        "candidate_ids": ["candidate"] * 139,
                        "finished_at": "2026-08-20T12:43:22Z",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (run_root / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_id": "legacy-inventory-2",
                        "status": "completed",
                        "resource_count": 5735,
                        "progress": {"processed": 5735, "read": 5735, "unreadable": 0, "total": 5735},
                        "evidence": {"cache_hits": 181, "cache_misses": 5554},
                        "llm": {"batch_count": 0, "completed_batches": 0},
                        "created_at": "2026-08-20T12:06:58Z",
                        "completed_at": "2026-08-20T12:43:22Z",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            status = isolated_controller(root).deep_inventory_status()
            stages = status["stage_progress"]

            self.assertEqual(status["stage_progress_source"], "legacy_derived")
            self.assertEqual(stages["document_read"]["processed"], 5735)
            self.assertEqual(stages["document_read"]["total"], 5735)
            self.assertEqual(stages["document_read"]["cache_hits"], 181)
            self.assertEqual(stages["term_aggregation"]["processed"], 36334)
            self.assertEqual(stages["term_aggregation"]["total"], 36334)
            self.assertEqual(stages["llm_reduce"]["processed"], 160)
            self.assertEqual(stages["llm_reduce"]["skip_reason"], "legacy_decision_count_fallback")
            self.assertEqual(stages["candidate_write"]["processed"], 139)
            for stage in stages.values():
                self.assertEqual(stage["telemetry_source"], "legacy_derived")
                self.assertNotIn("elapsed_seconds", stage)

    def test_deep_inventory_status_prefers_manifest_when_result_stage_sidecar_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_root = root / "codex" / "skills" / "shengsuan-concepts" / "state" / "full-inventory"
            run_root = state_root / "runs" / "inventory-2"
            run_root.mkdir(parents=True)
            (state_root / "latest-result.json").write_text(
                json.dumps(
                    {
                        "run_id": "inventory-2",
                        "status": "completed",
                        "resource_count": 4,
                        "stage_progress": {},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (run_root / "manifest.json").write_text(
                json.dumps(
                    {
                        "stage_progress": {
                            "document_read": {
                                "status": "completed",
                                "processed": 4,
                                "total": 4,
                            }
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            status = isolated_controller(root).deep_inventory_status()
            self.assertEqual(status["stage_progress"]["document_read"]["status"], "completed")
            self.assertEqual(status["stage_progress"]["document_read"]["processed"], 4)
            self.assertEqual(status["stage_progress"]["term_aggregation"]["status"], "not_recorded")

    def test_http_summary_supports_etag_304_and_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            with running_http_server(controller) as base:
                request = Request(base + "/api/control-plane/summary")
                with urlopen(request, timeout=5) as response:
                    etag = response.headers.get("ETag")
                    self.assertTrue(etag)
                    first = json.loads(response.read())
                self.assertTrue(first["version"])

                cached = Request(base + "/api/control-plane/summary", headers={"If-None-Match": etag})
                with self.assertRaises(HTTPError) as context:
                    urlopen(cached, timeout=5)
                self.assertEqual(context.exception.code, 304)

                compressed = Request(
                    base + "/api/control-plane/summary",
                    headers={"Accept-Encoding": "gzip"},
                )
                with urlopen(compressed, timeout=5) as response:
                    self.assertEqual(response.headers.get("Content-Encoding"), "gzip")
                    payload = gzip.decompress(response.read())
                self.assertEqual(json.loads(payload)["version"], first["version"])

    def test_concepts_indexes_candidates_and_usage_once_per_request(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            with patch.object(controller.learning, "list_candidates", wraps=controller.learning.list_candidates) as list_candidates, patch.object(
                controller.learning, "usage_summary", wraps=controller.learning.usage_summary
            ) as usage_summary, patch.object(
                controller.learning,
                "candidate_for_concept",
                side_effect=AssertionError("concept projection must use the indexed candidates"),
            ):
                controller.concepts()
            self.assertEqual(list_candidates.call_count, 1)
            self.assertEqual(usage_summary.call_count, 1)

    def test_controller_rejects_direct_concept_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            calls = [
                lambda: controller.stage_review("DataAgent", {"action": "approve"}),
                lambda: controller.remove_review("DataAgent"),
                controller.commit_reviews,
                lambda: controller.request_agent_refresh("DataAgent"),
                lambda: controller.request_full_recheck({}),
                lambda: controller.record_manual_seed({"term": "DataAgent"}),
                lambda: controller.retry_publish("run-missing"),
            ]
            for call in calls:
                with self.assertRaises(PermissionError):
                    call()

            request = controller.store.create({"loop_id": "concept-review"})
            for call in [
                lambda: controller.start_action_runner(request["run_id"]),
                lambda: controller.cancel(request["run_id"]),
            ]:
                with self.assertRaises(PermissionError):
                    call()
            controller.store.append(request["run_id"], "gate/requested", {"action": "review"})
            with self.assertRaises(PermissionError):
                controller.gate_decision(request["run_id"], "approve")
            usage = controller.record_usage({"concept": "DataAgent", "event": "concept.used"})
            self.assertEqual(usage["concept"], "DataAgent")
            self.assertEqual(controller.learning.usage_summary()["events"], 1)

    def test_concept_recheck_status_keeps_legacy_requests_history_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            created = controller.store.create({"loop_id": "concept-recheck", "scope": {"mode": "all"}})
            controller.store.append(created["run_id"], "run/started", {"runtime": "codex"})
            with running_http_server(controller) as base:
                status, value = http_json(base + "/api/concept-recheck/status")
                self.assertEqual(status, 200)
                self.assertFalse(value["running"])
                self.assertIsNone(value["active"])
                self.assertEqual(value["status"], "attention")
                self.assertFalse(value["history_only"])
                self.assertTrue(value["legacy_control_plane_write_apis"]["disabled"])
                self.assertEqual(value["latest"]["status"], "history_only")
                self.assertEqual(value["latest"]["raw_status"], "running")
                status, duplicate = http_json(base + "/api/concept-recheck", "POST", {})
                self.assertEqual(status, 405)
                self.assertTrue(duplicate["read_only"])
                self.assertEqual(duplicate["status"], "attention")
                self.assertFalse(duplicate["disabled"])
                self.assertTrue(duplicate["legacy_control_plane_write_apis"]["disabled"])
                self.assertEqual(duplicate["error"], "concept_owned_runner_only")
                self.assertEqual(len([row for row in controller.store.list_states() if row.get("loop_id") == "concept-recheck"]), 1)

    def test_concept_workflow_writes_are_read_only_but_generic_runs_remain_writable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            with running_http_server(controller) as base:
                for path, method, payload in [
                    ("/api/concept-discovery/seeds", "POST", {"term": "DataAgent"}),
                    ("/api/concept-review/commit", "POST", {}),
                    ("/api/concepts/DataAgent/review", "POST", {"action": "approve"}),
                    ("/api/concepts/DataAgent/agent-refresh", "POST", {}),
                ]:
                    status, value = http_json(base + path, method, payload)
                    self.assertEqual(status, 405, path)
                    self.assertTrue(value["read_only"], path)
                    self.assertEqual(value["status"], "attention", path)
                    self.assertFalse(value["disabled"], path)
                    self.assertTrue(value["legacy_control_plane_write_apis"]["disabled"], path)
                status, value = http_json(base + "/api/usage", "POST", {"concept": "DataAgent", "event": "concept.used"})
                self.assertEqual(status, 201)
                self.assertEqual(value["event"]["concept"], "DataAgent")
                status, value = http_json(base + "/api/concepts/DataAgent/review", "DELETE")
                self.assertEqual(status, 405)
                self.assertTrue(value["read_only"])
                status, value = http_json(base + "/api/runs", "POST", {"loop_id": "daily-radar"})
                self.assertEqual(status, 201)
                self.assertEqual(value["loop_id"], "daily-radar")

    def test_concept_workflow_projects_recovery_gate_separately_from_legacy_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            evidence = {
                "expected_concept_count": 45,
                "ready": True,
                "admission_state": "disabled",
                "coverage": {"status": "PASS", "concept_count": 45},
                "content_preflight": {"status": "PASS"},
                "baseline": {"status": "APPLIED", "generation_id": "generation-test"},
                "projection": {"status": "checked", "hot_projection": {"concepts": 45}, "publish_projection": {"concepts": 45}},
                "dependency": {"status": "consumed", "latest_consumed": {"event_id": "dependency-test"}},
                "read_errors": [],
            }
            with patch.object(controller, "_concept_recovery_evidence", return_value=evidence):
                workflow = controller.concept_workflow_status()
                rejected = controller.concept_write_response(endpoint="/api/concepts/Example/review")

            self.assertEqual(workflow["status"], "recovery_gated")
            self.assertFalse(workflow["disabled"])
            self.assertEqual(workflow["execution"], "pm_scheduler_dependency")
            self.assertEqual(workflow["refresh_trigger"], "pm_scheduler_dependency")
            self.assertTrue(workflow["admission"]["blocks_unapproved_publish"])
            self.assertTrue(workflow["admission"]["does_not_mean_workflow_retired"])
            self.assertTrue(workflow["legacy_control_plane_write_apis"]["disabled"])
            self.assertEqual(rejected["status"], "recovery_gated")
            self.assertEqual(rejected["code"], "legacy_concept_control_plane_write_api_disabled")

    def test_full_inventory_request_does_not_start_from_control_plane(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            with patch("pm_loop_control_plane_server.subprocess.Popen") as popen:
                with self.assertRaises(PermissionError):
                    controller.request_full_recheck({"mode": "full_inventory"})
            popen.assert_not_called()

    def test_retired_concept_control_plane_jobs_are_not_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            for payload in [
                {"title": "概念刷新", "instructions": "执行 weekly-sync-and-refresh Step 3"},
                {"title": "全量盘点", "instructions": "run full_inventory"},
                {"title": "需求评估回写", "instructions": "将 requirement-fit 结果写入 Candidate"},
            ]:
                value = controller.create_control_plane_job(payload)
                self.assertEqual(value["status"], "rejected")
                self.assertTrue(value["history_only"])
                self.assertFalse(value["execution_started"])
            self.assertFalse(controller.control_plane_jobs_path.exists())

            generic = controller.create_control_plane_job(
                {"title": "资料同步检查", "instructions": "只读检查同步失败原因"}
            )
            self.assertEqual(generic["status"], "waiting_codex")
            self.assertTrue(controller.control_plane_jobs_path.is_file())

    def test_http_create_sse_status_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            controller = ControlPlane(root / "state", ROOT / "scripts" / "pm_loop_control_plane.py", ROOT, Path.home() / ".codex", ROOT / "web" / "pm-loop-control-plane", snapshot)
            server = create_loopback_server(ControlPlaneHTTPServer, ("127.0.0.1", 0), controller)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                health = json.loads(urlopen(base + "/api/health", timeout=3).read())
                self.assertEqual(health["runtime"], "codex")
                loops = json.loads(urlopen(base + "/api/loops", timeout=3).read())
                self.assertGreaterEqual(len(loops["loops"]), 4)
                demo_html = urlopen(base + "/v2-demo", timeout=3).read().decode("utf-8")
                self.assertIn("PM Loop Control Plane v2", demo_html)
                self.assertIn("开始分析", demo_html)
                request = Request(base + "/api/runs", data=json.dumps({"loop_id": "daily-radar", "permission_mode": "draft"}).encode(), headers={"Content-Type": "application/json"}, method="POST")
                created = json.loads(urlopen(request, timeout=3).read())
                run_id = created["run_id"]
                deadline = time.time() + 8
                state = {}
                while time.time() < deadline:
                    state = json.loads(urlopen(base + f"/api/runs/{run_id}", timeout=3).read())
                    if state.get("status") == "completed":
                        break
                    time.sleep(0.1)
                self.assertEqual(state.get("status"), "completed")
                events = urlopen(base + f"/api/runs/{run_id}/events", timeout=5).read().decode("utf-8")
                self.assertIn("run/completed", events)
                snapshot_response = json.loads(urlopen(base + f"/api/runs/{run_id}/snapshot", timeout=3).read())
                self.assertEqual(snapshot_response["snapshot_id"], "snapshot-fixture-001")
                replay_request = Request(base + f"/api/runs/{run_id}/replay", data=b"{}", method="POST")
                replay = json.loads(urlopen(replay_request, timeout=3).read())
                self.assertTrue(replay["read_only"])
                self.assertEqual(replay["state"]["status"], "completed")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_http_cancel_stops_running_runner_and_closes_sse(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            slow_adapter = root / "slow_adapter.py"
            slow_adapter.write_text(
                "\n".join(
                    [
                        "import argparse, json, time",
                        "from pathlib import Path",
                        "parser = argparse.ArgumentParser()",
                        "parser.add_argument('command')",
                        "parser.add_argument('--out', required=True)",
                        "parser.add_argument('--project-root')",
                        "parser.add_argument('--codex-root')",
                        "args = parser.parse_args()",
                        "time.sleep(5)",
                        "path = Path(args.out) / 'snapshot-slow.json'",
                        "path.parent.mkdir(parents=True, exist_ok=True)",
                        f"path.write_text(Path({str(snapshot)!r}).read_text(), encoding='utf-8')",
                        "print(json.dumps({'status': 'ok', 'snapshot_path': str(path)}))",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            controller = ControlPlane(root / "state", slow_adapter, ROOT, Path.home() / ".codex", ROOT / "web" / "pm-loop-control-plane")
            server = create_loopback_server(ControlPlaneHTTPServer, ("127.0.0.1", 0), controller)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                request = Request(base + "/api/runs", data=json.dumps({"loop_id": "daily-radar", "permission_mode": "draft"}).encode(), headers={"Content-Type": "application/json"}, method="POST")
                created = json.loads(urlopen(request, timeout=3).read())
                run_id = created["run_id"]
                cancel_request = Request(base + f"/api/runs/{run_id}/cancel", data=b"{}", method="POST")
                cancelled = json.loads(urlopen(cancel_request, timeout=3).read())
                self.assertEqual(cancelled["status"], "cancelled")
                events = urlopen(base + f"/api/runs/{run_id}/events", timeout=5).read().decode("utf-8")
                self.assertIn("run/cancelled", events)
                self.assertNotIn("run/completed", events)
                self.assertTrue(controller.store.paths(run_id).cancel_marker.is_file())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_http_v2_health_exposes_controller_fields(self) -> None:
        """Health must expose the resident/control-plane projection, not constants only."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            request = controller.store.create({"loop_id": "daily-radar", "permission_mode": "report"})
            controller.store.append(request["run_id"], "run/started", {"runtime": "codex"}, actor="control-plane")
            latest_event_at = controller.store.state(request["run_id"])["last_event"]["at"]
            with running_http_server(controller) as base:
                status, health = http_json(base + "/api/health")
            self.assertEqual(status, 200)
            self.assertEqual(health["status"], "ok")
            self.assertEqual(health["runtime"], "codex")
            self.assertEqual(health["mode"], "local")
            self.assertEqual(health["service"], "resident-capable")
            self.assertEqual(health["state_dir"], str(controller.store.state_dir))
            self.assertEqual(health["state_root"], str(controller.store.state_dir))
            self.assertIn("last_run_at", health)
            self.assertEqual(health["last_run_at"], latest_event_at)
            queue = controller.queue_status()
            self.assertEqual(health["queue"]["concurrency"], queue["concurrency"])
            self.assertEqual(health["queue"]["worker"], queue["worker"])
            self.assertEqual(health["queue"]["items"], queue["items"])

    def test_http_v2_runs_filters_and_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            store = controller.store
            store.create({"run_id": "run-v2-queued-daily", "loop_id": "daily-radar", "permission_mode": "report"})
            running_daily = store.create({"run_id": "run-v2-running-daily", "loop_id": "daily-radar", "permission_mode": "report"})
            store.append(running_daily["run_id"], "run/started", {}, actor="control-plane")
            running_weekly = store.create({"run_id": "run-v2-running-weekly", "loop_id": "weekly-review", "permission_mode": "draft"})
            store.append(running_weekly["run_id"], "run/started", {}, actor="control-plane")
            completed = store.create({"run_id": "run-v2-completed-daily", "loop_id": "daily-radar", "permission_mode": "report"})
            store.append(completed["run_id"], "run/completed", {}, actor="control-plane")

            with running_http_server(controller) as base:
                status, filtered = http_json(base + "/api/runs?loop_id=daily-radar&status=running&limit=1")
                self.assertEqual(status, 200)
                self.assertEqual(len(filtered["runs"]), 1)
                self.assertEqual(filtered["runs"][0]["loop_id"], "daily-radar")
                self.assertEqual(filtered["runs"][0]["status"], "running")

                status, loop_runs = http_json(base + "/api/runs?loop_id=daily-radar&limit=2")
                self.assertEqual(status, 200)
                self.assertEqual(len(loop_runs["runs"]), 2)
                self.assertTrue(all(item["loop_id"] == "daily-radar" for item in loop_runs["runs"]))

                status, completed_runs = http_json(base + "/api/runs?status=completed&limit=0")
                self.assertEqual(status, 200)
                self.assertEqual(len(completed_runs["runs"]), 1)
                self.assertEqual(completed_runs["runs"][0]["run_id"], "run-v2-completed-daily")

                # The current API clamps malformed limits to its default rather
                # than rejecting the whole listing request.
                status, malformed_limit = http_json(base + "/api/runs?limit=not-a-number")
                self.assertEqual(status, 200)
                self.assertLessEqual(len(malformed_limit["runs"]), 100)

    def test_http_v2_artifacts_distinguish_missing_artifact_from_unknown_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            request = controller.store.create({"loop_id": "daily-radar", "permission_mode": "report"})
            run_id = request["run_id"]
            with running_http_server(controller) as base:
                for artifact in ("analysis", "decision", "log"):
                    status, value = http_json(base + f"/api/runs/{run_id}/{artifact}")
                    self.assertEqual(status, 200, artifact)
                    self.assertFalse(value["available"], artifact)
                    self.assertEqual(value["run_id"], run_id)

                for artifact in ("analysis", "decision", "log"):
                    status, value = http_json(base + f"/api/runs/run-does-not-exist/{artifact}")
                    self.assertEqual(status, 404, artifact)
                    self.assertIn("error", value)

    def test_http_v2_rerun_creates_independent_run_with_real_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = fixture_snapshot(root / "snapshot.json")
            controller = isolated_controller(root, snapshot=snapshot)
            with running_http_server(controller) as base:
                status, original = http_json(
                    base + "/api/runs",
                    "POST",
                    {
                        "loop_id": "daily-radar",
                        "permission_mode": "draft",
                        "scope": {"customer": "fixture-customer"},
                        "record": True,
                    },
                )
                self.assertEqual(status, 201)
                original_id = original["run_id"]
                original_state = wait_for_run_status(base, original_id)
                self.assertEqual(original_state.get("status"), "completed")
                original_request = controller.store.request(original_id)
                original_events = controller.store.events_for(original_id)

                status, rerun = http_json(base + f"/api/runs/{original_id}/rerun", "POST", {})
                self.assertEqual(status, 202)
                rerun_id = rerun["run_id"]
                self.assertNotEqual(rerun_id, original_id)
                rerun_request = controller.store.request(rerun_id)
                self.assertEqual(rerun_request["loop_id"], original_request["loop_id"])
                self.assertEqual(rerun_request["scope"], original_request["scope"])
                self.assertEqual(rerun_request["permission_mode"], original_request["permission_mode"])
                self.assertEqual(rerun_request["record"], original_request["record"])
                self.assertEqual(rerun_request["trigger"]["kind"], "rerun")
                self.assertEqual(rerun_request["trigger"]["rerun_of"], original_id)

                rerun_state = wait_for_run_status(base, rerun_id)
                self.assertEqual(rerun_state.get("status"), "completed")
                rerun_events = controller.store.events_for(rerun_id)
                rerun_types = [event["type"] for event in rerun_events]
                self.assertIn("run/started", rerun_types)
                self.assertIn("source/completed", rerun_types)
                self.assertEqual(rerun_types[-1], "run/completed")
                self.assertEqual(controller.store.request(original_id), original_request)
                self.assertEqual(controller.store.events_for(original_id), original_events)

    def test_http_v2_generic_gate_actions_persist_distinct_events_and_states(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)

            def awaiting_run(action_id: str | None = None) -> str:
                request = controller.store.create(
                    {
                        "loop_id": "daily-radar",
                        "permission_mode": "approved_action",
                        "scope": ({"action_id": action_id} if action_id else {}),
                    }
                )
                controller.store.append(
                    request["run_id"],
                    "gate/requested",
                    {"action": "review", "action_id": action_id},
                    actor="reviewer",
                )
                self.assertEqual(controller.store.state(request["run_id"])["status"], "awaiting_human")
                return request["run_id"]

            with running_http_server(controller) as base:
                approved_id = awaiting_run("fixture-action")
                status, approved = http_json(base + f"/api/gates/{approved_id}/approve", "POST", {"note": "批准执行"})
                self.assertEqual(status, 200)
                self.assertEqual(approved["status"], "completed")
                approved_types = [event["type"] for event in controller.store.events_for(approved_id)]
                self.assertEqual(
                    approved_types,
                    [
                        "run/created",
                        "gate/requested",
                        "gate/approved",
                        "action/queued",
                        "action/started",
                        "action/completed",
                        "run/completed",
                    ],
                )
                self.assertEqual(controller.store.events_for(approved_id)[2]["data"]["note"], "批准执行")

                changes_id = awaiting_run()
                status, changes = http_json(base + f"/api/gates/{changes_id}/changes-requested", "POST", {"note": "补充证据"})
                self.assertEqual(status, 200)
                self.assertEqual(changes["status"], "changes_requested")
                changes_events = controller.store.events_for(changes_id)
                self.assertEqual([event["type"] for event in changes_events], ["run/created", "gate/requested", "gate/changes_requested"])
                self.assertEqual(changes_events[-1]["data"]["note"], "补充证据")

                paused_id = awaiting_run()
                status, paused = http_json(base + f"/api/gates/{paused_id}/pause", "POST", {"note": "暂缓本次审批"})
                self.assertEqual(status, 200)
                self.assertEqual(paused["status"], "paused")
                paused_events = controller.store.events_for(paused_id)
                self.assertEqual([event["type"] for event in paused_events], ["run/created", "gate/requested", "gate/paused"])
                self.assertEqual(paused_events[-1]["data"]["note"], "暂缓本次审批")


if __name__ == "__main__":
    unittest.main()
