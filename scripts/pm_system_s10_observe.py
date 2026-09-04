#!/usr/bin/env python3
"""Collect a read-only S10 observation sample for the V4.4 runtime."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

from pm_system_gateway import retry_amplification_from_connection


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
DB_PATH = CODEX_ROOT / "pm-loop/state/pm-system.db"
TIMELINE_STATE = CODEX_ROOT / "skills/pm-timeline/state"
HEALTH_PATH = CODEX_ROOT / "skills/system-health-check/state/latest.json"
LOG_PATHS = {
    "control_plane": CODEX_ROOT / "pm-loop/control-plane.log",
    "worker": CODEX_ROOT / "pm-loop/worker.log",
}
REPORT_DIR = PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告"
LOCAL_ZONE = ZoneInfo("Asia/Shanghai")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _launchctl_getenv(name: str) -> Optional[str]:
    try:
        result = subprocess.run(["launchctl", "getenv", name], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return os.environ.get(name)
    return result.stdout.strip() or os.environ.get(name)


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _seconds_between(start: Any, end: Any = None) -> Optional[float]:
    first = _parse_timestamp(start)
    if first is None:
        return None
    second = _parse_timestamp(end) or datetime.now(timezone.utc)
    value = (second - first).total_seconds()
    return round(max(0.0, value), 4)


def _percentile(values: list[float], fraction: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 4)


def _duration_summary(values: list[float], *, active_count: int = 0) -> dict[str, Any]:
    return {
        "count": len(values),
        "active_count": int(active_count),
        "p50_s": _percentile(values, 0.50),
        "p95_s": _percentile(values, 0.95),
        "max_s": round(max(values), 4) if values else None,
    }


def process_snapshot() -> dict[str, Any]:
    buckets: dict[str, Any] = {"control_plane": [], "worker": [], "codex_exec": [], "forbidden": [], "orphan": [], "rss": {"sampled_at": _now(), "processes": [], "total_mb": 0.0, "peak_mb": 0.0}}
    try:
        result = subprocess.run(["ps", "axo", "pid=,ppid=,rss=,command="], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return {"probe_error": True, **buckets}
    forbidden_patterns = (
        "weekly-sync-and-refresh.sh",
        "product-intelligence-monitor/scripts/sync.py",
        "pm-timeline/scripts/",
        "/.codex/scripts/catchup.py",
    )
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(\d+)\s+(.*)$", line)
        if not match:
            continue
        rss_kb = int(match.group(3))
        item = {"pid": int(match.group(1)), "ppid": int(match.group(2)), "rss_kb": rss_kb, "rss_mb": round(rss_kb / 1024, 4), "command": match.group(4)}
        command = item["command"]
        if "pm_loop_control_plane_server.py" in command:
            buckets["control_plane"].append(item)
            buckets["rss"]["processes"].append({"role": "control_plane", "pid": item["pid"], "rss_mb": item["rss_mb"]})
        elif "pm_system_worker.py" in command:
            buckets["worker"].append(item)
            buckets["rss"]["processes"].append({"role": "worker", "pid": item["pid"], "rss_mb": item["rss_mb"]})
        elif re.search(r"(?:^|/)(?:codex|baidu-cx)(?:\s+exec|$)", command):
            buckets["codex_exec"].append(item)
        elif any(pattern in command for pattern in forbidden_patterns):
            buckets["forbidden"].append(item)
    worker_pids = {item["pid"] for item in buckets["worker"]}
    buckets["orphan"] = [item for item in buckets["codex_exec"] if item.get("ppid") not in worker_pids]
    rss_values = [float(item["rss_mb"]) for item in buckets["rss"]["processes"]]
    buckets["rss"]["total_mb"] = round(sum(rss_values), 4)
    buckets["rss"]["peak_mb"] = round(max(rss_values), 4) if rss_values else 0.0
    return buckets


def _grouped(connection: sqlite3.Connection, table: str, column: str = "status") -> dict[str, int]:
    return {str(key): int(value) for key, value in connection.execute(f"SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}").fetchall()}


def db_snapshot() -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(DB_PATH),
        "exists": DB_PATH.is_file(),
        "tables": {},
        "active": [],
        "slots": {},
        "providers": [],
        "performance": {},
        "watermarks": {},
        "dead_letter": {},
        "integrity_check": None,
        "schema_version": None,
        "latest_mutation_at": None,
        "read_only": True,
    }
    if not DB_PATH.is_file():
        result["error"] = "coordination database missing"
        return result
    try:
        connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            result["integrity_check"] = str(integrity[0]) if integrity else None
            result["schema_version"] = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
            for table in ("jobs", "runs", "outbox_items", "semantic_tasks"):
                result["tables"][table] = _grouped(connection, table)
                for status in ("queued", "running", "retry_wait", "in_flight", "accepted", "processing"):
                    count = connection.execute(f"SELECT COUNT(*) FROM {table} WHERE status=?", (status,)).fetchone()[0]
                    if count:
                        result["active"].append({"table": table, "status": status, "count": int(count)})
            result["dead_letter"] = {
                "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status='dead_letter'").fetchone()[0]),
                "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status='dead_letter'").fetchone()[0]),
            }
            result["dead_letter"]["total"] = result["dead_letter"]["outbox"] + result["dead_letter"]["semantic"]
            mutation_times: list[str] = []
            for table, column in (
                ("jobs", "updated_at"),
                ("runs", "updated_at"),
                ("outbox_items", "updated_at"),
                ("semantic_tasks", "updated_at"),
                ("provider_buckets", "updated_at"),
            ):
                value = connection.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
                if value:
                    mutation_times.append(str(value))
            result["latest_mutation_at"] = max(mutation_times) if mutation_times else None
            result["tables"]["error_events"] = _grouped(connection, "error_events", "severity")
            result["tables"]["error_events_total"] = int(connection.execute("SELECT COUNT(*) FROM error_events").fetchone()[0])
            result["slots"] = _grouped(connection, "execution_slots")
            queue_waits: list[float] = []
            run_durations: list[float] = []
            for queued_at, started_at, status in connection.execute("SELECT queued_at,started_at,status FROM jobs"):
                value = _seconds_between(queued_at, started_at)
                if value is not None:
                    queue_waits.append(value)
                elif status in {"queued", "retry_wait"}:
                    value = _seconds_between(queued_at)
                    if value is not None:
                        queue_waits.append(value)
            for started_at, completed_at, status in connection.execute("SELECT started_at,completed_at,status FROM runs"):
                value = _seconds_between(started_at, completed_at)
                if value is not None and status in {"completed", "degraded", "failed", "cancelled", "interrupted"}:
                    run_durations.append(value)
            model_latencies: list[float] = []
            for started_at, completed_at, status in connection.execute("SELECT started_at,completed_at,status FROM model_calls"):
                value = _seconds_between(started_at, completed_at)
                if value is not None and status in {"completed", "failed", "cancelled"}:
                    model_latencies.append(value)
            active_outbox = connection.execute("SELECT status,created_at FROM outbox_items WHERE status IN ('pending','retry_wait','in_flight')").fetchall()
            outbox_ages = [value for _status, created_at in active_outbox if (value := _seconds_between(created_at)) is not None]
            retry = retry_amplification_from_connection(connection)
            result["performance"] = {
                "queue_wait": _duration_summary(queue_waits, active_count=sum(1 for table in result["active"] if table["table"] == "jobs")),
                "run_duration": _duration_summary(run_durations),
                "model_latency": _duration_summary(model_latencies),
                "outbox": {"active_count": len(active_outbox), "oldest_age_s": round(max(outbox_ages), 4) if outbox_ages else 0.0, "p95_age_s": _percentile(outbox_ages, 0.95)},
                "retry_amplification": retry["amplification"],
                "retry_amplification_breakdown": retry,
                "logical_outbox": retry["outbox"]["logical"],
                "attempts": retry["outbox"]["attempts"],
            }
            def structured_watermark(name: str) -> Any:
                row = connection.execute(
                    "SELECT value,state,captured_at,sequence,value_hash,producer FROM watermarks WHERE watermark_name=? AND state='accepted' ORDER BY captured_at DESC,sequence DESC,rowid DESC LIMIT 1",
                    (name,),
                ).fetchone()
                if row is not None:
                    try:
                        value = json.loads(row[0])
                    except (TypeError, json.JSONDecodeError):
                        value = row[0]
                    return {"value": value, "state": row[1], "captured_at": row[2], "sequence": row[3], "value_hash": row[4], "producer": row[5]}
                return None

            result["watermarks"] = {name: structured_watermark(name) for name in ("source", "content", "knowledge", "active_generation")}
            for row in connection.execute(
                "SELECT provider_key,provider,endpoint,model,throttle_until,circuit_state,consecutive_429,last_retry_after,updated_at "
                "FROM provider_buckets ORDER BY updated_at DESC LIMIT 20"
            ).fetchall():
                provider = dict(zip(("provider_key", "provider", "endpoint", "model", "throttle_until", "circuit_state", "consecutive_429", "last_retry_after", "updated_at"), row))
                provider["throttled"] = bool(provider.get("throttle_until") and provider.get("throttle_until") > _now())
                result["providers"].append(provider)
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


class _ReadOnlyCockpitStore:
    """Minimal facade that keeps the cockpit projection strictly read-only."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=2)
        connection.row_factory = sqlite3.Row
        return connection

    def schema_version(self) -> int:
        with self.connect() as connection:
            return int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])


def cockpit_snapshot() -> dict[str, Any]:
    """Capture the local dashboard projection without HTTP or migrations."""
    result: dict[str, Any] = {
        "available": False,
        "status": "unknown",
        "unknown_key_modules": [],
        "missing_watermarks": [],
        "health_truthful": False,
        "evidence_complete": False,
    }
    if not DB_PATH.is_file():
        result["error"] = "coordination database missing"
        return result
    try:
        from pm_system_cockpit import KEY_SIGNAL_MODULES, CockpitReadModel

        snapshot = CockpitReadModel(_ReadOnlyCockpitStore(DB_PATH)).snapshot()
        modules = {str(item.get("module")): item for item in snapshot.get("modules", [])}
        unknown_key_modules = [
            name for name in KEY_SIGNAL_MODULES
            if modules.get(name, {}).get("status") == "unknown" or modules.get(name, {}).get("freshness") == "stale"
        ]
        missing_watermarks = [
            name for name in ("source", "content", "knowledge", "active_generation")
            if snapshot.get("watermarks", {}).get(name) in (None, "")
        ]
        status = str(snapshot.get("status") or "unknown")
        result.update({
            "available": True,
            "status": status,
            "source_version": snapshot.get("source_version"),
            "read_at": snapshot.get("read_at"),
            "summary": snapshot.get("summary", {}),
            "dead_letter": snapshot.get("summary", {}).get("dead_letter", 0),
            "unknown_key_modules": unknown_key_modules,
            "missing_watermarks": missing_watermarks,
            "health_truthful": status != "healthy" or (not unknown_key_modules and not missing_watermarks),
            "evidence_complete": not unknown_key_modules and not missing_watermarks,
        })
    except (ImportError, OSError, sqlite3.Error, ValueError, KeyError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"exists": False, "path": str(path)}
    try:
        return {"exists": True, "path": str(path), "mtime": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"), "value": json.loads(path.read_text(encoding="utf-8"))}
    except (OSError, json.JSONDecodeError) as exc:
        return {"exists": True, "path": str(path), "error": f"{type(exc).__name__}: {exc}"}


def timeline_snapshot() -> dict[str, Any]:
    values = {name: _read_json(TIMELINE_STATE / filename) for name, filename in (("daily", "daily-latest.json"), ("weekly", "weekly-review-latest.json"))}
    normal: dict[str, bool] = {}
    freshness: dict[str, dict[str, Any]] = {}
    now = datetime.now(timezone.utc)
    for name, item in values.items():
        value = item.get("value") if isinstance(item, dict) else None
        reason = str(value.get("reason", "")) if isinstance(value, dict) else ""
        normal[name] = bool(isinstance(value, dict) and value.get("status") == "ok" and not reason.startswith("maintenance_expected:"))
        finished = _parse_timestamp(value.get("finished_at")) if isinstance(value, dict) else None
        age = round(max(0.0, (now - finished).total_seconds()), 4) if finished else None
        threshold = 36 * 3600 if name == "daily" else 8 * 24 * 3600
        freshness[name] = {"finished_at": value.get("finished_at") if isinstance(value, dict) else None, "age_s": age, "threshold_s": threshold, "fresh": age is not None and age <= threshold}
    return {"markers": values, "normal": normal, "freshness": freshness, "all_normal": all(normal.values())}


def health_snapshot() -> dict[str, Any]:
    item = _read_json(HEALTH_PATH)
    value = item.get("value") if isinstance(item, dict) else None
    checks = value.get("checks", {}) if isinstance(value, dict) else {}
    pending = [name for name, check in checks.items() if not check.get("passed")]
    run_at = value.get("run_at") if isinstance(value, dict) else None
    parsed = _parse_timestamp(run_at)
    age = round(max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds()), 4) if parsed else None
    return {"path": str(HEALTH_PATH), "exists": item.get("exists", False), "run_at": run_at, "age_s": age, "fresh": age is not None and age <= 36 * 3600 if parsed else None, "total": len(checks), "passed": len(checks) - len(pending), "pending": pending, "error": item.get("error")}


def log_snapshot() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, path in LOG_PATHS.items():
        item: dict[str, Any] = {"path": str(path), "exists": path.is_file()}
        if path.is_file():
            try:
                item["mtime"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                text = path.read_text(encoding="utf-8", errors="replace")
                item["line_count"] = len(text.splitlines())
                item["exception_lines"] = sum(1 for line in text.splitlines() if "Exception occurred" in line or "Traceback" in line)
            except OSError as exc:
                item["error"] = f"{type(exc).__name__}: {exc}"
        result[name] = item
    return result


def observation_trend(current: Mapping[str, Any]) -> dict[str, Any]:
    """Read prior sample manifests without writing or treating same-day repeats as workdays."""
    samples: list[dict[str, Any]] = []
    for path in sorted(REPORT_DIR.glob("*-S10-observation-sample-*-manifest.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(item, dict) and item.get("phase_id") == "S10-observation":
            samples.append(item)
    current_at = _parse_timestamp(current.get("observed_at")) or datetime.now(timezone.utc)
    current_date = current_at.astimezone(LOCAL_ZONE).date().isoformat()
    prior = [item for item in samples if item.get("observed_at") != current.get("observed_at")]
    prior.sort(key=lambda item: str(item.get("observed_at", "")))
    previous = prior[-1] if prior else None
    current_perf = current.get("database", {}).get("performance", {}) if isinstance(current.get("database"), Mapping) else {}
    previous_perf = previous.get("database", {}).get("performance", {}) if isinstance(previous, Mapping) else {}
    current_p95 = current_perf.get("queue_wait", {}).get("p95_s")
    previous_p95 = previous_perf.get("queue_wait", {}).get("p95_s")
    return {
        "sample_count_before_current": len(prior),
        "distinct_local_dates_before_current": sorted({(_parse_timestamp(item.get("observed_at")) or current_at).astimezone(LOCAL_ZONE).date().isoformat() for item in prior}),
        "current_local_date": current_date,
        "previous": {"observed_at": previous.get("observed_at"), "status": previous.get("status")} if previous else None,
        "queue_wait_p95_delta_s": round(current_p95 - previous_p95, 4) if isinstance(current_p95, (int, float)) and isinstance(previous_p95, (int, float)) else None,
    }


def observe() -> dict[str, Any]:
    observed_at = _now()
    flags = {"PM_V44_AUTOMATION_FREEZE": _launchctl_getenv("PM_V44_AUTOMATION_FREEZE"), "PM_V44_ADMISSION": _launchctl_getenv("PM_V44_ADMISSION")}
    processes = process_snapshot()
    database = db_snapshot()
    cockpit = cockpit_snapshot()
    timeline = timeline_snapshot()
    health = health_snapshot()
    logs = log_snapshot()
    trend = observation_trend({"observed_at": observed_at, "database": database, "logs": logs})
    hard_issues: list[str] = []
    pending: list[str] = []
    if flags != {"PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_ADMISSION": "on"}:
        hard_issues.append("freeze/admission drifted from off/on")
    if processes.get("probe_error"):
        hard_issues.append("process probe failed")
    if len(processes.get("control_plane", [])) != 1 or len(processes.get("worker", [])) != 1:
        hard_issues.append("Control Plane/Worker process count is not exactly one")
    if processes.get("forbidden"):
        hard_issues.append("business writer process observed")
    if processes.get("orphan"):
        hard_issues.append("orphan codex exec process observed")
    if database.get("error"):
        hard_issues.append("coordination database read failed")
    if database.get("integrity_check") not in (None, "ok"):
        hard_issues.append(f"coordination database integrity check failed: {database.get('integrity_check')}")
    if database.get("active"):
        hard_issues.append("active queue state remains")
    if database.get("slots", {}).get("leased", 0):
        hard_issues.append("execution slot remains leased")
    if database.get("tables", {}).get("error_events_total", 0):
        hard_issues.append("error_events are present")
    for provider in database.get("providers", []):
        # A provider throttle is an active capacity constraint even when the
        # circuit has not crossed its consecutive-429 threshold.  Checking
        # circuit_state alone lets the first 429 pass as healthy.
        if provider.get("throttled") or (provider.get("throttle_until") and provider.get("circuit_state") not in (None, "closed")):
            hard_issues.append(f"provider bucket is open/throttled: {provider.get('provider_key')}")
    if not timeline.get("all_normal"):
        pending.append("daily/weekly normal completion marker not observed")
    freshness = timeline.get("freshness") if isinstance(timeline.get("freshness"), Mapping) else {}
    stale_markers = [name for name, item in freshness.items() if isinstance(item, Mapping) and item.get("fresh") is False]
    if stale_markers:
        pending.append("daily/weekly marker is stale: " + ", ".join(sorted(stale_markers)))
    if health.get("fresh") is False:
        pending.append("system-health-check report is stale")
    if health.get("pending"):
        pending.append("health-check has pending items: " + ", ".join(health["pending"]))
    if cockpit.get("error") or not cockpit.get("available"):
        pending.append("cockpit snapshot unavailable")
    elif not cockpit.get("health_truthful"):
        hard_issues.append("cockpit health semantics are inconsistent")
    status = "PASS" if not hard_issues and not pending else "HOLD_CONTINUE"
    return {
        "schema_version": "pm-system.s10-observation.v2",
        "phase_id": "S10-observation",
        "observed_at": observed_at,
        "status": status,
        "read_only": True,
        "flags": flags,
        "processes": processes,
        "database": database,
        "cockpit": cockpit,
        "timeline": timeline,
        "health": health,
        "logs": logs,
        "trend": trend,
        "hard_issues": hard_issues,
        "pending": pending,
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def write_report(path: Path, value: Mapping[str, Any]) -> None:
    database = value["database"]
    processes = value["processes"]
    timeline = value["timeline"]
    health = value["health"]
    lines = [
        "# V4.4 S10 观察期采样报告",
        "",
        f"> phase_id：`{value['phase_id']}`",
        f"> 采样时间：`{value['observed_at']}`",
        f"> 当前判定：**{value['status']}**",
        "> 运行边界：只读采集；不创建 Run、不 claim、不重试、不调用 OneAPI、不写业务状态。",
        "",
        "## 1. 采样结论",
        "",
        "本样本用于 S10 两个工作日观察趋势，不替代最终验收。",
        f"- Control Plane / Worker：`{len(processes.get('control_plane', []))}` / `{len(processes.get('worker', []))}`",
        f"- 活动队列：`{database.get('active') or []}`",
        f"- leased slot：`{database.get('slots', {}).get('leased', 0)}`",
        f"- error_events：`{database.get('tables', {}).get('error_events_total', 0)}`",
        f"- SQLite integrity/schema：`{database.get('integrity_check')}` / `v{database.get('schema_version')}`",
        f"- 终态 dead-letter（不等于活动队列）：`{database.get('dead_letter', {})}`",
        f"- 最新协调库变更：`{database.get('latest_mutation_at')}`",
        f"- daily/weekly 正常 marker：`{timeline.get('normal')}`",
        f"- 巡检：`{health.get('passed')}/{health.get('total')}` 通过",
        f"- 性能：`{database.get('performance', {})}`",
        f"- watermarks：`{database.get('watermarks', {})}`",
        f"- 驾驶舱实时状态：`{value.get('cockpit', {}).get('status', 'unknown')}`，关键模块缺口：`{value.get('cockpit', {}).get('unknown_key_modules', [])}`",
        f"- RSS：`{processes.get('rss', {})}`",
        "",
        "## 2. 门禁结果",
        "",
        "| 检查 | 结果 |",
        "|---|---|",
        f"| freeze/admission=off/on | {'PASS' if value['flags'] == {'PM_V44_AUTOMATION_FREEZE': 'off', 'PM_V44_ADMISSION': 'on'} else 'FAIL'} |",
        f"| 单 Control Plane / Worker | {'PASS' if len(processes.get('control_plane', [])) == 1 and len(processes.get('worker', [])) == 1 else 'FAIL'} |",
        f"| 无业务 Writer | {'PASS' if not processes.get('forbidden') else 'FAIL'} |",
        f"| 无活动队列和 leased slot | {'PASS' if not database.get('active') and not database.get('slots', {}).get('leased', 0) else 'FAIL'} |",
        f"| 无协调库错误事件 | {'PASS' if not database.get('tables', {}).get('error_events_total', 0) else 'FAIL'} |",
        f"| SQLite integrity_check=ok | {'PASS' if database.get('integrity_check') in (None, 'ok') else 'FAIL'} |",
        f"| 驾驶舱未把 unknown/stale 误报为 healthy | {'PASS' if value.get('cockpit', {}).get('health_truthful') else 'FAIL'} |",
        f"| daily/weekly 正常 marker | {'PASS' if timeline.get('all_normal') else 'PENDING'} |",
        f"| 性能指标已采集 | {'PASS' if database.get('performance') is not None else 'FAIL'} |",
        f"| watermarks 已采集 | {'PASS' if database.get('watermarks') is not None else 'FAIL'} |",
        "",
        "## 3. 待处理和异常",
        "",
    ]
    lines.extend(f"- {item}" for item in value.get("hard_issues", []) or ["无硬故障"]) 
    lines.extend(f"- {item}" for item in value.get("pending", []) or ["无待处理项"])
    lines.extend([
        "",
        "## 4. 下一门禁",
        "",
        "继续保持 2 槽位，待两个工作日趋势和解冻后的 daily/weekly marker 均通过后执行 S10 最终健康检查。",
        "",
        "## 5. 数据安全与副作用",
        "",
        "本次采样使用 SQLite `mode=ro`，未调用 OneAPI/OpenViking 业务写入，未修改协调库、ledger、时间轴或任务状态。",
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_html(path: Path, report_path: Path, value: Mapping[str, Any]) -> None:
    write_report(report_path, value)
    markdown = report_path.read_text(encoding="utf-8")
    body: list[str] = []
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("# "):
            body.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            body.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("> "):
            body.append(f"<aside>{html.escape(line[2:])}</aside>")
        elif line.startswith("- "):
            body.append(f"<p>{html.escape(line[2:])}</p>")
        elif line.startswith("|"):
            body.append(f"<pre>{html.escape(line)}</pre>")
        else:
            body.append(f"<p>{html.escape(line)}</p>")
    document = "<!doctype html><meta charset=\"utf-8\"><title>V4.4 S10 观察期采样报告</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;color:#24302f}h1{font-size:32px;border-bottom:2px solid #24302f;padding-bottom:12px}h2{margin-top:32px}aside{padding:12px 16px;background:#eef3f1;border-left:4px solid #54756b}pre{white-space:pre-wrap;background:#f5f6f4;padding:8px}</style><main>" + "".join(body) + "</main>"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document, encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    args = parser.parse_args(argv)
    value = observe()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_html(args.html, args.report, value)
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "hard_issues": value["hard_issues"], "pending": value["pending"], "manifest": str(args.output), "report": str(args.report), "html": str(args.html)}, ensure_ascii=False))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
