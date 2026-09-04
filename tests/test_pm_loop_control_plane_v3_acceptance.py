"""Acceptance checks for the v3 read-only Control Plane projection.

These tests intentionally mutate only isolated fixture files.  They protect the
important v3 contract: the display layer may coalesce a burst of reads, but it
must never keep serving a projection after one of its source files changes.
"""

from __future__ import annotations

import gzip
import json
import os
import plistlib
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_control_plane_server import ControlPlane, ControlPlaneHTTPServer  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from http_test_utils import create_loopback_server  # noqa: E402


@contextmanager
def running_http_server(controller: ControlPlane):
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


def isolated_controller(root: Path) -> ControlPlane:
    return ControlPlane(
        root / "state",
        ROOT / "scripts" / "pm_loop_control_plane.py",
        ROOT,
        root / "codex",
        ROOT / "web" / "pm-loop-control-plane",
    )


def isolated_runtime_controller(root: Path) -> ControlPlane:
    project_root = root / "project"
    (project_root / "docs").mkdir(parents=True, exist_ok=True)
    return ControlPlane(
        root / "state",
        ROOT / "scripts" / "pm_loop_control_plane.py",
        project_root,
        root / "codex",
        ROOT / "web" / "pm-loop-control-plane",
    )


def write_launch_agent(
    root: Path,
    label: str,
    *,
    calendar: dict | None = None,
    interval_seconds: int | None = None,
    run_at_load: bool = False,
    keep_alive: bool = False,
) -> Path:
    path = root / "Library" / "LaunchAgents" / f"{label}.plist"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": label,
        "ProgramArguments": ["/bin/sh", f"/fixture/{label}.sh"],
        "RunAtLoad": run_at_load,
        "KeepAlive": keep_alive,
    }
    if calendar is not None:
        payload["StartCalendarInterval"] = calendar
    if interval_seconds is not None:
        payload["StartInterval"] = interval_seconds
    with path.open("wb") as stream:
        plistlib.dump(payload, stream)
    return path


def write_automation(root: Path, *, name: str = "DataBuilder 产品缺口周度评估") -> Path:
    path = root / "codex" / "automations" / "databuilder" / "automation.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                'id = "databuilder"',
                f'name = "{name}"',
                'kind = "heartbeat"',
                'status = "ACTIVE"',
                'rrule = "FREQ=WEEKLY;BYDAY=TU;BYHOUR=10;BYMINUTE=0"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_health(root: Path, value: dict) -> Path:
    path = root / "codex" / "skills" / "system-health-check" / "state" / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return path


def advance_mtime(path: Path) -> None:
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))


def ledger_path(root: Path) -> Path:
    path = root / "codex" / "skills" / "shengsuan-concepts" / "state" / "concepts-ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_ledger(root: Path, concepts: list[str]) -> Path:
    path = ledger_path(root)
    payload = {
        name: {"status": "active", "sources": [f"viking://fixture/{name}"]}
        for name in concepts
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    # Filesystems with coarse timestamp resolution can otherwise make an
    # mtime-only source signature look unchanged when a fixture is rewritten.
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))
    return path


def snapshot_request(url: str, *, headers: dict[str, str] | None = None) -> tuple[int, dict, dict[str, str]]:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=5) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return response.status, json.loads(body), dict(response.headers.items())
    except HTTPError as error:
        body = error.read()
        if body and error.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        return error.code, (json.loads(body) if body else {}), dict(error.headers.items())


def json_request(
    url: str,
    payload: dict | None = None,
    *,
    method: str = "POST",
) -> tuple[int, dict, dict[str, str]]:
    body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urlopen(request, timeout=5) as response:
            raw = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return response.status, (json.loads(raw) if raw else {}), dict(response.headers.items())
    except HTTPError as error:
        raw = error.read()
        if raw and error.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return error.code, (json.loads(raw) if raw else {}), dict(error.headers.items())


class ControlPlaneV3FreshnessAcceptanceTests(unittest.TestCase):
    def test_snapshot_rebuilds_immediately_when_source_file_changes(self) -> None:
        """A one-second read coalescing window must not hide runner writes."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)

            first = controller.control_plane_snapshot()
            self.assertEqual(first["summary"]["concepts"], 1)
            write_ledger(root, ["Alpha", "Beta"])
            second = controller.control_plane_snapshot()

            self.assertEqual(second["summary"]["concepts"], 2)
            first_version = first.get("source_version") or first.get("version") or first.get("snapshot_id")
            second_version = second.get("source_version") or second.get("version") or second.get("snapshot_id")
            self.assertNotEqual(first_version, second_version)
            self.assertEqual(second.get("read_only"), True)

    def test_snapshot_force_fresh_query_bypasses_any_read_coalescing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, first, _ = snapshot_request(base + "/api/control-plane/snapshot")
                self.assertEqual(status, 200)
                self.assertEqual(first["summary"]["concepts"], 1)

                write_ledger(root, ["Alpha", "Beta", "Gamma"])
                status, second, _ = snapshot_request(base + "/api/control-plane/snapshot?fresh=1")
                self.assertEqual(status, 200)
                self.assertEqual(second["summary"]["concepts"], 3)
                self.assertNotEqual(
                    first.get("source_version") or first.get("snapshot_id"),
                    second.get("source_version") or second.get("snapshot_id"),
                )

    def test_v3_http_aliases_expose_the_same_read_only_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, summary, _ = snapshot_request(base + "/api/control-plane/v3/summary")
                self.assertEqual(status, 200)
                self.assertTrue(str(summary.get("schema_version", "")).endswith("summary.v3"))
                self.assertTrue(summary.get("read_only"))

                status, snapshot, _ = snapshot_request(base + "/api/control-plane/v3/snapshot?fresh=1")
                self.assertEqual(status, 200)
                self.assertTrue(str(snapshot.get("schema_version", "")).endswith("snapshot.v3"))
                self.assertTrue(snapshot.get("read_only"))
                self.assertTrue(snapshot.get("source_version"))

    def test_summary_etag_and_version_follow_source_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, first, headers = snapshot_request(base + "/api/control-plane/summary")
                self.assertEqual(status, 200)
                old_etag = headers.get("ETag")
                self.assertTrue(old_etag)
                old_version = first.get("source_version") or first.get("version")
                self.assertTrue(old_version)

                status, _, not_modified_headers = snapshot_request(
                    base + "/api/control-plane/summary",
                    headers={"If-None-Match": old_etag},
                )
                self.assertEqual(status, 304)
                self.assertEqual(not_modified_headers.get("ETag"), old_etag)

                write_ledger(root, ["Alpha", "Beta"])
                status, second, new_headers = snapshot_request(base + "/api/control-plane/summary")
                self.assertEqual(status, 200)
                self.assertEqual(second["summary"]["active_concepts"], 2)
                self.assertNotEqual(second.get("source_version") or second.get("version"), old_version)
                self.assertNotEqual(new_headers.get("ETag"), old_etag)

    def test_freshness_transitions_from_stale_to_missing_without_serving_old_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = write_ledger(root, ["Alpha"])
            # Concept freshness threshold is 14 days; make the source clearly
            # older than that without relying on wall-clock sleeps.
            old_ns = int((path.stat().st_atime_ns, path.stat().st_mtime_ns)[1] - 45 * 24 * 3600 * 1_000_000_000)
            os.utime(path, ns=(old_ns, old_ns))
            controller = isolated_controller(root)

            stale = controller.control_plane_snapshot()
            concepts = next(item for item in stale["freshness"] if item["id"] == "concepts")
            # The fixture does not satisfy the V11 recovery gate. Preserve the
            # raw freshness signal while surfacing an actionable attention
            # state instead of the legacy disabled workflow label.
            self.assertEqual(concepts["status"], "attention")
            self.assertEqual(concepts.get("raw_status"), "stale")
            self.assertFalse(concepts.get("disabled"))
            self.assertFalse(concepts.get("history_only"))
            self.assertIsNotNone(concepts.get("updated_at"))
            self.assertIn("source_signature", concepts)
            self.assertIn("observed_at", concepts)

            path.unlink()
            missing = controller.control_plane_snapshot()
            concepts = next(item for item in missing["freshness"] if item["id"] == "concepts")
            self.assertEqual(concepts["status"], "attention")
            self.assertEqual(concepts.get("raw_status"), "missing")
            self.assertFalse(concepts.get("disabled"))
            self.assertFalse(concepts.get("history_only"))
            self.assertIsNone(concepts.get("updated_at"))
            missing_signature = concepts.get("source_signature")
            if isinstance(missing_signature, dict):
                self.assertFalse(missing_signature.get("exists"))
            else:
                self.assertIsNone(missing_signature)

    def test_v3_snapshot_exposes_read_consistency_and_source_signatures(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            snapshot = isolated_controller(root).control_plane_snapshot()
            self.assertTrue(str(snapshot.get("schema_version", "")).endswith("v3"))
            self.assertTrue(snapshot.get("source_version"))
            self.assertTrue(snapshot.get("read_at") or snapshot.get("checked_at"))
            self.assertIn(snapshot.get("read_consistency"), {"consistent", "best_effort", "unknown"})
            self.assertIsInstance(snapshot.get("source_signatures"), dict)
            self.assertTrue(snapshot["source_signatures"])

    def test_snapshot_does_not_reuse_cache_when_source_version_is_inconclusive(self) -> None:
        """An unreadable signature must trigger a fresh projection, never stale UI data."""
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_controller(Path(temp))
            builds = [
                {"source_version": "v1", "read_consistency": "consistent", "value": "old"},
                {"source_version": "v2", "read_consistency": "consistent", "value": "fresh"},
            ]
            with patch.object(controller, "_control_plane_snapshot_uncached", side_effect=builds) as build:
                with patch.object(controller, "_source_version", side_effect=["v1", RuntimeError("probe failed")]):
                    first = controller.control_plane_snapshot()
                    second = controller.control_plane_snapshot()
            self.assertEqual(first["value"], "old")
            self.assertEqual(second["value"], "fresh")
            self.assertEqual(build.call_count, 2)

    def test_report_sources_follow_latest_matching_file(self) -> None:
        """New dated reports must replace older artifacts in the read model."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            docs = project / "docs"
            materials = docs / "04-产品设计"
            materials.mkdir(parents=True, exist_ok=True)
            old_gap = docs / "DataBuilder产品缺口与安排建议-20260818.md"
            new_gap = docs / "DataBuilder产品缺口与安排建议-20260823.md"
            old_material = materials / "基本概念-资料评审意见-20260817.md"
            new_material = materials / "基本概念-资料评审意见-20260823.md"
            for path in (old_gap, new_gap, old_material, new_material):
                path.write_text(path.name, encoding="utf-8")
            now_ns = time.time_ns()
            for path in (old_gap, old_material):
                os.utime(path, ns=(now_ns - 2_000_000_000, now_ns - 2_000_000_000))
            for path in (new_gap, new_material):
                os.utime(path, ns=(now_ns, now_ns))

            controller = ControlPlane(
                root / "state",
                ROOT / "scripts" / "pm_loop_control_plane.py",
                project,
                root / "codex",
                ROOT / "web" / "pm-loop-control-plane",
            )
            paths = controller._control_plane_source_paths()
            self.assertEqual(paths["gaps"][0], new_gap)
            self.assertEqual(paths["materials"][0], new_material)

            summary = controller.control_plane_summary()
            self.assertEqual(summary["source_signatures"]["gaps"]["path"], str(new_gap))
            self.assertEqual(summary["source_signatures"]["materials"]["path"], str(new_material))
            snapshot = controller.control_plane_snapshot(force=True)
            freshness = {item["id"]: item for item in snapshot["freshness"]}
            self.assertEqual(freshness["gaps"]["path"], str(new_gap))
            self.assertEqual(freshness["materials"]["path"], str(new_material))
            self.assertEqual(freshness["gaps"]["read_status"], "ok")
            self.assertEqual(freshness["materials"]["read_status"], "ok")

    def test_schedules_project_all_launchagents_and_codex_automations_with_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            reviews = root / "project" / "docs" / "reviews"
            reviews.mkdir(parents=True, exist_ok=True)
            old_review = reviews / "2026-W33-review.html"
            latest_review = reviews / "2026-W34-review.html"
            old_review.write_text("<html>old weekly review</html>", encoding="utf-8")
            latest_review.write_text("<html>latest weekly review</html>", encoding="utf-8")
            now_ns = time.time_ns()
            os.utime(old_review, ns=(now_ns - 2_000_000_000, now_ns - 2_000_000_000))
            os.utime(latest_review, ns=(now_ns, now_ns))
            write_launch_agent(
                root,
                "com.zhujie14.pm-timeline-daily",
                calendar={"Hour": 13, "Minute": 37},
            )
            write_launch_agent(
                root,
                "com.zhujie14.pm-timeline-weekly",
                calendar={"Weekday": 0, "Hour": 19, "Minute": 55},
            )
            write_launch_agent(
                root,
                "com.zhujie14.failing-job",
                interval_seconds=3600,
            )
            write_launch_agent(
                root,
                "com.zhujie14.weekly-sync-and-refresh",
                calendar={"Weekday": 1, "Hour": 9, "Minute": 5},
            )
            write_automation(root)
            health = {
                "run_at": "2026-08-23 12:37:22",
                "checks": {
                    "launchd 作业状态": {
                        "passed": False,
                        "data": {
                            "jobs": [
                                {"label": "com.zhujie14.pm-timeline-daily", "status": "ok"},
                                {"label": "com.zhujie14.pm-timeline-weekly", "status": "ok"},
                                {
                                    "label": "com.zhujie14.failing-job",
                                    "status": "bad_exit_code",
                                    "detail": "上次退出码 9 不在允许集合 [0]",
                                },
                                {"label": "com.zhujie14.weekly-sync-and-refresh", "status": "ok"},
                            ]
                        },
                    },
                    "定时任务执行留痕": {
                        "passed": False,
                        "data": [
                            {
                                "task": "pm-timeline 停滞跟进检查",
                                "status": "ok",
                                "last_output_mtime": "2026-08-22 13:48:46",
                            },
                            {
                                "task": "pm-timeline 周回顾",
                                "status": "not_run",
                                "expected_since": "2026-08-23 19:55",
                                "last_output_mtime": "2026-08-16 19:56:59",
                            },
                        ],
                    },
                },
            }
            write_health(root, health)
            probes = {
                "com.zhujie14.pm-timeline-daily": {
                    "state": "not running",
                    "runs": "4",
                    "last_exit_code": "0",
                },
                "com.zhujie14.pm-timeline-weekly": {
                    "state": "not running",
                    "runs": "0",
                    "last_exit_code": "(never exited)",
                },
                "com.zhujie14.failing-job": {
                    "state": "not running",
                    "runs": "1",
                    "last_exit_code": "9",
                },
                "com.zhujie14.weekly-sync-and-refresh": {
                    "state": "not running",
                    "runs": "1",
                    "last_exit_code": "0",
                },
            }
            controller = isolated_runtime_controller(root)
            refresh = {
                "job": "com.zhujie14.weekly-sync-and-refresh",
                "pipeline": [],
                "last_run": {
                    "finished_at": "2026-08-20T18:40:19+08:00",
                    "status": "partial_failure",
                    "step1_sync": 0,
                    "step2_public_docs": 1,
                    "step3_refresh": 0,
                },
            }

            with patch("pm_loop_control_plane.probe_launchctl", side_effect=lambda label: probes[label]), patch.object(
                controller, "refresh_status", return_value=refresh
            ):
                snapshot = controller.control_plane_snapshot(force=True)

            health_checks = {row["name"]: row for row in snapshot["health"]["checks"]}
            launchd_reason = health_checks["launchd 作业状态"]["reason"]
            self.assertIn("com.zhujie14.failing-job", launchd_reason)
            self.assertIn("上次退出码 9 不在允许集合 [0]", launchd_reason)
            jobs = {row["label"]: row for row in snapshot["schedules"]["jobs"]}
            self.assertEqual(
                set(jobs),
                {
                    "com.zhujie14.pm-timeline-daily",
                    "com.zhujie14.pm-timeline-weekly",
                    "com.zhujie14.failing-job",
                    "com.zhujie14.weekly-sync-and-refresh",
                    "DataBuilder 产品缺口周度评估",
                },
            )
            self.assertEqual(jobs["com.zhujie14.pm-timeline-daily"]["schedule"], "每天 13:37")
            self.assertEqual(jobs["com.zhujie14.pm-timeline-daily"]["status"], "completed")
            self.assertIn("有效产出", jobs["com.zhujie14.pm-timeline-daily"]["reason"])
            self.assertEqual(jobs["com.zhujie14.pm-timeline-weekly"]["schedule"], "周日 19:55")
            self.assertEqual(jobs["com.zhujie14.pm-timeline-weekly"]["status"], "not_run")
            self.assertIn("计划截止 2026-08-23 19:55", jobs["com.zhujie14.pm-timeline-weekly"]["reason"])
            self.assertIn("最近产出 2026-08-16 19:56:59", jobs["com.zhujie14.pm-timeline-weekly"]["reason"])
            timeline_job = jobs["com.zhujie14.pm-timeline-weekly"]
            self.assertEqual(timeline_job["output_url"], "/reports/pm-timeline/latest")
            self.assertEqual(timeline_job["output_path"], str(latest_review))
            self.assertTrue(timeline_job["output_updated_at"])
            self.assertEqual(timeline_job["latest_output"]["name"], latest_review.name)
            self.assertEqual(jobs["com.zhujie14.failing-job"]["schedule"], "每 3600 秒")
            self.assertEqual(jobs["com.zhujie14.failing-job"]["status"], "failed")
            self.assertEqual(jobs["com.zhujie14.failing-job"]["reason"], "上次退出码 9 不在允许集合 [0]")
            weekly = jobs["com.zhujie14.weekly-sync-and-refresh"]
            self.assertEqual(weekly["status"], "partial")
            self.assertEqual(weekly["last"], "2026-08-20T18:40:19+08:00")
            self.assertIn("step2_public_docs", weekly["reason"])
            automation = jobs["DataBuilder 产品缺口周度评估"]
            self.assertEqual(automation["kind"], "automation")
            self.assertEqual(automation["schedule"], "周二 10:00")
            self.assertEqual(automation["status"], "scheduled")
            self.assertIn("已启用", automation["reason"])

    def test_health_report_route_serves_latest_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_runtime_controller(root)
            docs = root / "project" / "docs"
            old_report = docs / "系统健康巡检报告-20260822.html"
            new_report = docs / "系统健康巡检报告-20260823.html"
            old_report.write_text("<html>old report</html>", encoding="utf-8")
            new_report.write_text("<html>latest report</html>", encoding="utf-8")
            now_ns = time.time_ns()
            os.utime(old_report, ns=(now_ns - 2_000_000_000, now_ns - 2_000_000_000))
            os.utime(new_report, ns=(now_ns, now_ns))

            with running_http_server(controller) as base:
                with urlopen(base + "/health-report", timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.headers.get_content_type(), "text/html")
                    self.assertEqual(response.read().decode("utf-8"), "<html>latest report</html>")

            self.assertEqual(controller.health_report_path(), new_report)
            with patch.object(controller, "refresh_status", return_value={"pipeline": []}):
                snapshot = controller.control_plane_snapshot(force=True)
            self.assertEqual(snapshot["health"]["report"]["url"], "/health-report")
            self.assertEqual(snapshot["health"]["report"]["path"], str(new_report))

    def test_v4_summary_uses_explicit_health_report_date_not_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True)
            PMSystemStore(db_path)
            controller = isolated_runtime_controller(root)
            report = root / "project" / "docs" / "系统健康巡检报告-20260904.html"
            report.write_text("<html>health report</html>", encoding="utf-8")
            yesterday = time.time() - 24 * 60 * 60
            os.utime(report, (yesterday, yesterday))

            with running_http_server(controller) as base:
                status, before_touch, _ = snapshot_request(base + "/api/control-plane/v4/summary")
                self.assertEqual(status, 200)
                self.assertEqual(before_touch["health_report"]["url"], "/health-report")
                self.assertTrue(before_touch["health_report"]["generated_today"])
                self.assertEqual(before_touch["health_report"]["status"], "fresh_today")
                self.assertEqual(before_touch["health_report"]["time_basis"], "报告文件名日期")

                now = time.time()
                os.utime(report, (now, now))
                status, after_touch, _ = snapshot_request(base + "/api/control-plane/v4/summary")
                self.assertEqual(status, 200)
                self.assertTrue(after_touch["health_report"]["generated_today"])
                self.assertEqual(after_touch["health_report"]["status"], "fresh_today")
                self.assertEqual(after_touch["health_report"]["generated_date"], "2026-09-04")

    def test_control_plane_snapshot_projects_completed_health_check_without_name_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_runtime_controller(root)
            write_launch_agent(root, "com.zhujie14.system-health-check", interval_seconds=60)
            write_health(
                root,
                {
                    "schema_version": "system-health-check.v1",
                    "checks": {
                        "launchd 作业状态": {"passed": True, "data": {"jobs": []}},
                        "定时任务执行留痕": {"passed": True, "data": []},
                    },
                },
            )
            with patch.object(controller, "refresh_status", return_value={"pipeline": []}):
                snapshot = controller.control_plane_snapshot(force=True)
            jobs = {row["label"]: row for row in snapshot["schedules"]["jobs"]}
            health_job = jobs.get("com.zhujie14.system-health-check")
            self.assertIsNotNone(health_job)
            self.assertEqual(health_job["status"], "completed")

    def test_v3_index_revalidates_the_live_shell(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_runtime_controller(Path(temp))
            with running_http_server(controller) as base:
                with urlopen(base + "/v3", timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(
                        response.headers.get("Cache-Control"),
                        "private, max-age=0, must-revalidate",
                    )
                    self.assertIn("PM Loop Control Plane", response.read().decode("utf-8"))

    def test_health_report_route_returns_404_when_report_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            controller = isolated_runtime_controller(Path(temp))
            with running_http_server(controller) as base:
                status, value, _ = snapshot_request(base + "/health-report")
            self.assertEqual(status, 404)
            self.assertEqual(value["error"], "health_report_not_found")

    def test_pm_timeline_review_route_serves_latest_report_and_404s_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_runtime_controller(root)
            reviews = root / "project" / "docs" / "reviews"
            reviews.mkdir(parents=True, exist_ok=True)
            old_review = reviews / "2026-W33-review.html"
            latest_review = reviews / "2026-W34-review.html"
            old_review.write_text("<html>old</html>", encoding="utf-8")
            latest_review.write_text("<html>latest</html>", encoding="utf-8")
            now_ns = time.time_ns()
            os.utime(old_review, ns=(now_ns - 2_000_000_000, now_ns - 2_000_000_000))
            os.utime(latest_review, ns=(now_ns, now_ns))

            with running_http_server(controller) as base:
                with urlopen(base + "/reports/pm-timeline/latest", timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.headers.get_content_type(), "text/html")
                    self.assertEqual(
                        response.headers.get("Cache-Control"),
                        "private, max-age=0, must-revalidate",
                    )
                    self.assertEqual(response.read().decode("utf-8"), "<html>latest</html>")

            latest_review.unlink()
            old_review.unlink()
            with running_http_server(controller) as base:
                status, value, _ = snapshot_request(base + "/reports/pm-timeline/latest")
            self.assertEqual(status, 404)
            self.assertEqual(value["error"], "pm_timeline_review_not_found")
            self.assertFalse(controller.pm_timeline_review_output()["available"])

    def test_domain_report_routes_serve_latest_html_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            gaps = project / "docs" / "产品缺口周报"
            materials = project / "docs" / "04-产品设计" / "资料缺失周报"
            gaps.mkdir(parents=True, exist_ok=True)
            materials.mkdir(parents=True, exist_ok=True)
            # Legacy Markdown remains present as a source fallback, but is not
            # eligible for a browser-facing report route.
            (project / "docs" / "DataBuilder产品缺口与安排建议-20260818.md").parent.mkdir(parents=True, exist_ok=True)
            (project / "docs" / "DataBuilder产品缺口与安排建议-20260818.md").write_text("# legacy gap markdown", encoding="utf-8")
            (project / "docs" / "04-产品设计" / "基本概念-资料评审意见-20260817.md").write_text("# legacy material markdown", encoding="utf-8")
            gap_html = gaps / "产品缺口与安排建议-20260901.html"
            material_html = materials / "胜算产品资料缺失周报-20260901.html"
            gap_html.write_text("<html><title>DataBuilder 产品缺口与安排建议</title></html>", encoding="utf-8")
            material_html.write_text("<html><title>胜算产品资料缺失周报</title></html>", encoding="utf-8")
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            PMSystemStore(db_path)

            controller = ControlPlane(
                root / "state",
                ROOT / "scripts" / "pm_loop_control_plane.py",
                project,
                root / "codex",
                ROOT / "web" / "pm-loop-control-plane",
            )
            self.assertEqual(controller.domain_report_path("gaps"), gap_html)
            self.assertEqual(controller.domain_report_path("materials"), material_html)
            snapshot = controller.control_plane_snapshot(force=True)
            self.assertTrue(snapshot["domains"]["gaps"]["available"])
            self.assertEqual(snapshot["domains"]["gaps"]["html_path"], str(gap_html))
            self.assertTrue(snapshot["domains"]["materials"]["available"])
            self.assertEqual(snapshot["domains"]["materials"]["html_path"], str(material_html))

            with running_http_server(controller) as base:
                status, summary, _ = snapshot_request(base + "/api/control-plane/v4/summary")
                self.assertEqual(status, 200)
                self.assertTrue(summary["domains"]["gaps"]["available"])
                self.assertEqual(summary["domains"]["materials"]["html_path"], str(material_html))
                self.assertIn(":reports-", summary["source_version"])
                for route, expected_title in (
                    ("/reports/gaps/latest", "DataBuilder 产品缺口与安排建议"),
                    ("/reports/materials/latest", "胜算产品资料缺失周报"),
                ):
                    with urlopen(base + route, timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.headers.get_content_type(), "text/html")
                        self.assertIn(expected_title, response.read().decode("utf-8"))

    def test_project_report_routes_use_the_explicit_canonical_evidence_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            runtime = root / "runtime"
            evidence = root / "evidence"
            (runtime / "docs").mkdir(parents=True)
            (evidence / "docs" / "reviews").mkdir(parents=True)
            (evidence / "docs" / "产品缺口周报").mkdir(parents=True)
            (evidence / "docs" / "04-产品设计" / "资料缺失周报").mkdir(parents=True)
            # A runtime mirror may contain stale report copies. All project
            # report routes must instead read the explicit canonical root.
            (runtime / "docs" / "系统健康巡检报告-20260904.html").write_text("runtime stale", encoding="utf-8")
            health = evidence / "docs" / "系统健康巡检报告-20260904.html"
            timeline = evidence / "docs" / "reviews" / "2026-W36-review.html"
            gap = evidence / "docs" / "产品缺口周报" / "产品缺口与安排建议-20260904.html"
            materials = evidence / "docs" / "04-产品设计" / "资料缺失周报" / "胜算产品资料缺失周报-20260904.html"
            health.write_text("canonical health", encoding="utf-8")
            timeline.write_text("canonical timeline", encoding="utf-8")
            gap.write_text("canonical gap", encoding="utf-8")
            materials.write_text("canonical materials", encoding="utf-8")
            controller = ControlPlane(
                root / "state",
                ROOT / "scripts" / "pm_loop_control_plane.py",
                runtime,
                root / "codex",
                ROOT / "web" / "pm-loop-control-plane",
                evidence_project_root=evidence,
            )

            self.assertEqual(controller.health_report_path(), health)
            self.assertEqual(controller.pm_timeline_review_path(), timeline)
            self.assertEqual(controller.domain_report_path("gaps"), gap)
            self.assertEqual(controller.domain_report_path("materials"), materials)
            with running_http_server(controller) as base:
                for route, expected in (
                    ("/health-report", "canonical health"),
                    ("/reports/pm-timeline/latest", "canonical timeline"),
                    ("/reports/gaps/latest", "canonical gap"),
                    ("/reports/materials/latest", "canonical materials"),
                ):
                    with urlopen(base + route, timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.read().decode("utf-8"), expected)

    def test_role_output_route_serves_only_allowlisted_historical_html(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            reports = project / "docs" / "产品缺口周报"
            reports.mkdir(parents=True)
            report = reports / "产品缺口与安排建议-20260901.html"
            health_report = project / "docs" / "系统健康巡检报告-20260901.html"
            report.write_text("<html>role output</html>", encoding="utf-8")
            health_report.write_text("<html>health output</html>", encoding="utf-8")
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            PMSystemStore(db_path)
            controller = ControlPlane(
                root / "state",
                ROOT / "scripts" / "pm_loop_control_plane.py",
                project,
                root / "codex",
                ROOT / "web" / "pm-loop-control-plane",
            )
            self.assertIsNotNone(controller.v44_cockpit)
            outputs = controller.v44_cockpit._role_output_history()["items"]
            self.assertEqual(len(outputs), 1)
            health_output_id = controller.v44_cockpit._role_output_id(health_report)
            with running_http_server(controller) as base:
                with urlopen(base + outputs[0]["open_url"], timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.headers.get_content_type(), "text/html")
                    self.assertEqual(response.headers.get("Cache-Control"), "private, max-age=0, must-revalidate")
                    self.assertEqual(response.read().decode("utf-8"), "<html>role output</html>")
                status, value, _ = snapshot_request(base + "/artifacts/role-outputs/" + "0" * 64)
            self.assertEqual(status, 404)
            self.assertEqual(value["error"], "role_output_not_found")
            with running_http_server(controller) as base:
                status, value, _ = snapshot_request(base + "/artifacts/role-outputs/" + health_output_id)
            self.assertEqual(status, 404)
            self.assertEqual(value["error"], "role_output_not_found")

    def test_source_version_changes_for_launchagent_automation_and_health_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_runtime_controller(root)
            launch_agent = write_launch_agent(
                root,
                "com.zhujie14.pm-timeline-weekly",
                calendar={"Weekday": 0, "Hour": 19, "Minute": 55},
            )
            automation = write_automation(root)
            report = root / "project" / "docs" / "系统健康巡检报告-20260823.html"
            report.write_text("<html>health v1</html>", encoding="utf-8")
            review = root / "project" / "docs" / "reviews" / "2026-W34-review.html"
            review.parent.mkdir(parents=True, exist_ok=True)
            review.write_text("<html>review v1</html>", encoding="utf-8")

            initial = controller.control_plane_summary()
            launch_agent = write_launch_agent(
                root,
                "com.zhujie14.pm-timeline-weekly",
                calendar={"Weekday": 0, "Hour": 20, "Minute": 5},
            )
            advance_mtime(launch_agent)
            after_launch_agent = controller.control_plane_summary()
            self.assertNotEqual(initial["source_version"], after_launch_agent["source_version"])
            self.assertTrue(after_launch_agent["source_signatures"]["launch_agents"]["exists"])

            automation = write_automation(root, name="DataBuilder 产品缺口周度评估 v2")
            advance_mtime(automation)
            after_automation = controller.control_plane_summary()
            self.assertNotEqual(after_launch_agent["source_version"], after_automation["source_version"])
            self.assertTrue(after_automation["source_signatures"]["automations"]["exists"])

            report.write_text("<html>health v2</html>", encoding="utf-8")
            advance_mtime(report)
            after_report = controller.control_plane_summary()
            self.assertNotEqual(after_automation["source_version"], after_report["source_version"])
            self.assertEqual(after_report["source_signatures"]["health_report"]["path"], str(report))

            review.write_text("<html>review v2</html>", encoding="utf-8")
            advance_mtime(review)
            after_review = controller.control_plane_summary()
            self.assertNotEqual(after_report["source_version"], after_review["source_version"])
            self.assertEqual(after_review["source_signatures"]["timeline_review"]["path"], str(review))

    def test_control_plane_job_records_intent_without_creating_a_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)
            before = controller.control_plane_summary()["source_version"]

            result = controller.create_control_plane_job(
                {
                    "title": "刷新概念证据",
                    "instructions": "请 Codex 检查概念 Alpha 的最新来源并生成草稿。",
                    "scope": {"page": "concepts", "concept": "Alpha"},
                }
            )
            self.assertEqual(result["status"], "waiting_codex")
            self.assertTrue(result["intent_only"])
            self.assertFalse(result["job"]["execution_started"])
            self.assertFalse(result["job"]["active_mutation"])
            self.assertTrue(result["source_version"])
            self.assertNotEqual(result["source_version"], before)
            self.assertFalse(list((root / "state" / "runs").glob("*/request.json")))

            listed = controller.control_plane_jobs(limit=10)
            self.assertEqual(listed["requests"][0]["job_id"], result["job_id"])
            self.assertEqual(listed["requests"][0]["status"], "waiting_codex")
            self.assertEqual(listed["read_consistency"], "consistent")
            self.assertIn("control_plane_jobs", listed["source_signatures"])

    def test_control_plane_jobs_http_post_is_fenced_and_get_remains_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_ledger(root, ["Alpha"])
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, posted, _ = json_request(
                    base + "/api/control-plane/jobs",
                    {"title": "检查健康状态", "instructions": "请 Codex 读取最新 health 产物。", "scope": {"page": "health"}},
                )
                self.assertEqual(status, 405)
                self.assertEqual(posted["error"], "legacy_control_plane_handoff_fenced")
                self.assertTrue(posted["read_only"])
                status, listed, _ = json_request(base + "/api/control-plane/jobs?limit=5", method="GET")
                self.assertEqual(status, 200)
                self.assertEqual(listed["jobs"], [])
                self.assertTrue(listed["source_version"])

    def test_v4_workbench_resources_share_one_read_only_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True)
            store = PMSystemStore(db_path)
            accepted = store.accept({"job_type": "diagnostic", "loop_id": "workbench", "idempotency_key": "workbench:v4"})
            del store
            controller = isolated_controller(root)
            before = db_path.read_bytes()
            with running_http_server(controller) as base:
                for resource in ("activity", "work-items", "plans", "reviews", "operations", "roles", "concepts"):
                    status, value, headers = json_request(base + f"/api/control-plane/v4/{resource}", method="GET")
                    self.assertEqual(status, 200, resource)
                    self.assertTrue(value["read_only"], resource)
                    self.assertTrue(value["source_version"], resource)
                    self.assertTrue(headers.get("ETag"), resource)
                    for field in ("as_of", "source_status", "source_cursor", "metric_source", "freshness", "evidence_status"):
                        self.assertIn(field, value, (resource, field))
                status, detail, _ = json_request(base + f"/api/control-plane/v4/runs/{accepted['run_id']}", method="GET")
                self.assertEqual(status, 200)
                self.assertEqual(detail["run"]["run_id"], accepted["run_id"])
            self.assertEqual(before, db_path.read_bytes())

    def test_v4_write_methods_are_explicitly_fenced(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                for method in ("POST", "PUT", "PATCH", "DELETE"):
                    status, value, _ = json_request(base + "/api/control-plane/v4/summary", method=method, payload={})
                    self.assertEqual(status, 405, method)
                    self.assertTrue(value["read_only"], method)

    def test_v4_runs_etag_is_bound_to_source_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "state" / "state" / "pm-system.db"
            db_path.parent.mkdir(parents=True)
            store = PMSystemStore(db_path)
            accepted = store.accept({"job_type": "diagnostic", "loop_id": "workbench", "idempotency_key": "workbench:etag"})
            controller = isolated_controller(root)
            with running_http_server(controller) as base:
                status, value, headers = json_request(base + "/api/control-plane/v4/runs", method="GET")
                self.assertEqual(status, 200)
                self.assertEqual(value["source_version"], value["source_cursor"])
                request = Request(base + "/api/control-plane/v4/runs", headers={"If-None-Match": headers["ETag"]})
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request, timeout=5)
                self.assertEqual(raised.exception.code, 304)
                status, detail, detail_headers = json_request(base + f"/api/control-plane/v4/runs/{accepted['run_id']}", method="GET")
                self.assertEqual(status, 200)
                request = Request(base + f"/api/control-plane/v4/runs/{accepted['run_id']}", headers={"If-None-Match": detail_headers["ETag"]})
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request, timeout=5)
                self.assertEqual(raised.exception.code, 304)


if __name__ == "__main__":
    unittest.main()
