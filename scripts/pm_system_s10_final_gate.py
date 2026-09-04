#!/usr/bin/env python3
"""Read-only final gate before V4.4 leaves the maintenance freeze."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
REPORT_DIR = PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告"
CODEX_ROOT = Path.home() / ".codex"
DB_PATH = CODEX_ROOT / "pm-loop/state/pm-system.db"
AUTOMATION_ROOT = CODEX_ROOT / "automations"
LAUNCH_ROOT = Path.home() / "Library/LaunchAgents"

LABELS = (
    "com.zhujie14.codex-oneapi-env",
    "com.zhujie14.openviking-server",
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.pm-system-worker",
    "com.zhujie14.system-health-check",
    "com.zhujie14.system-health-heartbeat",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.ov-memory-sync",
    "com.zhujie14.catchup",
)
REPORT_PAIRS = (
    ("20260829-S9.3.1-Control-Plane恢复报告.md", "20260829-S9.3.1-Control-Plane恢复报告.html"),
    ("20260829-S9.3.2-scheduler-worker-admission恢复报告.md", "20260829-S9.3.2-scheduler-worker-admission恢复报告.html"),
    ("20260829-S9.3.3-system-health-heartbeat恢复报告.md", "20260829-S9.3.3-system-health-heartbeat恢复报告.html"),
    ("20260829-S9.3.4-pm-timeline恢复报告.md", "20260829-S9.3.4-pm-timeline恢复报告.html"),
    ("20260829-S9.3.5-补跑汇总报告.md", "20260829-S9.3.5-补跑汇总报告.html"),
    ("20260829-S9.3.5-恢复-weekly-sync报告.md", "20260829-S9.3.5-恢复-weekly-sync报告.html"),
    ("20260829-S9.3.5-恢复-product-intelligence报告.md", "20260829-S9.3.5-恢复-product-intelligence报告.html"),
    ("20260829-S9.3.5-恢复-ov-memory-sync报告.md", "20260829-S9.3.5-恢复-ov-memory-sync报告.html"),
    ("20260829-S9.3.5-恢复-catchup报告.md", "20260829-S9.3.5-恢复-catchup报告.html"),
    ("20260829-S9.3.5-恢复-automation-automation报告.md", "20260829-S9.3.5-恢复-automation-automation报告.html"),
    ("20260829-S9.3.5-恢复-automation-databuilder报告.md", "20260829-S9.3.5-恢复-automation-databuilder报告.html"),
)
# The observer is itself a required control for this gate.  If it is paused or
# missing, a future workday sample cannot be trusted to arrive automatically.
AUTOMATION_IDS = ("automation", "databuilder", "v4-4-s10")
OBSERVATION_GLOB = "*-S10-observation-sample-*-manifest.json"
LOCAL_ZONE = ZoneInfo("Asia/Shanghai")
WORKDAY_GATE_ERROR = "S10 observation evidence does not contain two stable distinct-workday PASS samples"


def _launchctl(*args: str) -> tuple[int, str]:
    try:
        result = subprocess.run(["launchctl", *args], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return 125, ""
    return result.returncode, result.stdout.strip()


def launchd_loaded(label: str) -> Optional[bool]:
    code, _ = _launchctl("print", f"gui/{os.getuid()}/{label}")
    return None if code == 125 else code == 0


def launch_flag(name: str) -> Optional[str]:
    code, value = _launchctl("getenv", name)
    if code == 0 and value:
        return value
    return os.environ.get(name)


def report_status(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "exists": False, "pass": False}
    text = path.read_text(encoding="utf-8")
    normalized = html.unescape(re.sub(r"<[^>]+>", " ", text))
    matched = re.findall(r"(?:当前判定|判定)\s*(?:[：:]\s*)?(?:\*\*)?`?\s*PASS(?=\b|`|\*|（|\s)", normalized, flags=re.IGNORECASE)
    return {"path": str(path), "exists": True, "pass": bool(matched), "pass_markers": len(matched)}


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


def _local_date(value: Any) -> Optional[str]:
    parsed = _parse_timestamp(value)
    return parsed.astimezone(LOCAL_ZONE).date().isoformat() if parsed else None


def _numeric(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def observation_samples() -> dict[str, Any]:
    """Load S10 samples and enforce two distinct local workday observations."""
    loaded: list[dict[str, Any]] = []
    invalid: list[str] = []
    for path in sorted(REPORT_DIR.glob(OBSERVATION_GLOB)):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            invalid.append(f"{path.name}: {type(exc).__name__}")
            continue
        if not isinstance(item, dict) or item.get("phase_id") != "S10-observation":
            continue
        item = dict(item)
        item["manifest_path"] = str(path)
        item["local_date"] = _local_date(item.get("observed_at"))
        loaded.append(item)

    # Keep the latest sample for each local date. Repeated same-day probes are
    # useful diagnostics but cannot satisfy the two-workday gate.
    by_date: dict[str, dict[str, Any]] = {}
    for item in sorted(loaded, key=lambda value: str(value.get("observed_at", ""))):
        date = item.get("local_date")
        if date:
            by_date[date] = item
    pass_dates: list[str] = []
    rejected: list[str] = []
    for date, item in by_date.items():
        database = item.get("database") if isinstance(item.get("database"), Mapping) else {}
        performance = database.get("performance") if isinstance(database, Mapping) else None
        processes = item.get("processes") if isinstance(item.get("processes"), Mapping) else {}
        health = item.get("health") if isinstance(item.get("health"), Mapping) else {}
        reasons: list[str] = []
        if item.get("status") != "PASS":
            reasons.append(f"status={item.get('status')}")
        if item.get("hard_issues") or item.get("pending"):
            reasons.append("hard_issues/pending non-empty")
        if item.get("read_only") is not True or item.get("production_state_touched") is not False or item.get("external_provider_calls") != 0:
            reasons.append("sample side-effect boundary failed")
        if not item.get("timeline", {}).get("all_normal"):
            reasons.append("timeline marker not normal")
        if database.get("active") or database.get("slots", {}).get("leased", 0):
            reasons.append("active queue or leased slot")
        if health.get("total", 0) <= 0 or health.get("passed") != health.get("total"):
            reasons.append("health check incomplete")
        if not isinstance(performance, Mapping) or not isinstance(performance.get("queue_wait"), Mapping) or not isinstance(performance.get("outbox"), Mapping):
            reasons.append("performance metrics missing")
        if not isinstance(processes.get("rss"), Mapping):
            reasons.append("RSS metric missing")
        cockpit = item.get("cockpit")
        if cockpit is not None:
            if not isinstance(cockpit, Mapping) or cockpit.get("health_truthful") is not True:
                reasons.append("cockpit health semantics missing or inconsistent")
            elif cockpit.get("status") == "incident":
                reasons.append("cockpit reports incident")
        if reasons:
            rejected.append(f"{date}: " + "; ".join(reasons))
            continue
        retry_amplification = _numeric(performance.get("retry_amplification"), 999.0)
        queue_p95 = performance.get("queue_wait", {}).get("p95_s")
        outbox_age = _numeric(performance.get("outbox", {}).get("oldest_age_s"), 999999.0)
        if retry_amplification > 1.0:
            rejected.append(f"{date}: retry_amplification={retry_amplification} > 1")
            continue
        if queue_p95 is not None and _numeric(queue_p95, 999999.0) > 30.0:
            rejected.append(f"{date}: queue_wait_p95_s={queue_p95} > 30")
            continue
        if outbox_age > 300.0:
            rejected.append(f"{date}: outbox_oldest_age_s={outbox_age} > 300")
            continue
        pass_dates.append(date)

    pass_dates.sort()
    selected_dates = pass_dates[-2:]
    stability: dict[str, Any] = {"checked": False, "pass": False, "reason": "need two distinct PASS workdays"}
    if len(selected_dates) == 2:
        first = by_date[selected_dates[0]]
        second = by_date[selected_dates[1]]
        first_perf = first["database"]["performance"]
        second_perf = second["database"]["performance"]
        first_rss = _numeric(first.get("processes", {}).get("rss", {}).get("total_mb"))
        second_rss = _numeric(second.get("processes", {}).get("rss", {}).get("total_mb"))
        first_queue = _numeric(first_perf.get("queue_wait", {}).get("p95_s"))
        second_queue = _numeric(second_perf.get("queue_wait", {}).get("p95_s"))
        first_outbox = _numeric(first_perf.get("outbox", {}).get("oldest_age_s"))
        second_outbox = _numeric(second_perf.get("outbox", {}).get("oldest_age_s"))
        first_retry = _numeric(first_perf.get("retry_amplification"))
        second_retry = _numeric(second_perf.get("retry_amplification"))
        # With only two workdays, use a bounded-growth check rather than claim
        # statistical confidence. Large one-day jumps keep the gate closed.
        growth_ok = second_queue <= max(first_queue * 2.0, 30.0) and second_outbox <= max(first_outbox * 2.0, 300.0) and second_retry <= max(first_retry * 2.0, 1.0) and second_rss <= max(first_rss + 64.0, 256.0)
        stability = {
            "checked": True,
            "pass": growth_ok,
            "dates": selected_dates,
            "metrics": {
                "queue_wait_p95_s": [first_queue, second_queue],
                "outbox_oldest_age_s": [first_outbox, second_outbox],
                "retry_amplification": [first_retry, second_retry],
                "rss_total_mb": [first_rss, second_rss],
            },
            "reason": "bounded_growth_pass" if growth_ok else "metric_growth_exceeded_bound",
        }
    errors: list[str] = list(invalid)
    if len(selected_dates) < 2:
        errors.append(f"need two distinct local workdays with PASS samples; found {selected_dates}")
    if stability.get("checked") and not stability.get("pass"):
        errors.append("S10 observation metrics are not stable across selected workdays")
    return {
        "sample_count": len(loaded),
        "distinct_local_dates": sorted(by_date),
        "pass_dates": pass_dates,
        "selected_dates": selected_dates,
        "selected_samples": [by_date[date] for date in selected_dates],
        "rejected": rejected,
        "invalid": invalid,
        "errors": errors,
        "stability": stability,
        "pass": len(selected_dates) == 2 and bool(stability.get("pass")),
    }


def process_snapshot() -> dict[str, Any]:
    try:
        result = subprocess.run(["ps", "axo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return {"probe_error": True, "control_plane": [], "worker": [], "memory_sync": [], "forbidden": []}
    buckets = {"control_plane": [], "worker": [], "memory_sync": [], "forbidden": []}
    forbidden_patterns = ("weekly-sync-and-refresh.sh", "product-intelligence-monitor/scripts/sync.py", "pm-timeline/scripts/", "/.codex/scripts/catchup.py")
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if not match:
            continue
        item = {"pid": int(match.group(1)), "ppid": int(match.group(2)), "command": match.group(3)}
        command = item["command"]
        if "pm_loop_control_plane_server.py" in command:
            buckets["control_plane"].append(item)
        elif "pm_system_worker.py" in command:
            buckets["worker"].append(item)
        elif "ov_memory_sync.py" in command and " watch" in command:
            buckets["memory_sync"].append(item)
        elif any(pattern in command for pattern in forbidden_patterns):
            buckets["forbidden"].append(item)
    return buckets


def db_snapshot() -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(DB_PATH),
        "exists": DB_PATH.is_file(),
        "tables": {},
        "active": [],
        "slots": {},
        "terminal_failed": {},
        "dead_letter": {},
        "providers": [],
        "integrity_check": None,
        "schema_version": None,
        "latest_mutation_at": None,
    }
    if not DB_PATH.is_file():
        return result
    try:
        connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            result["integrity_check"] = str(integrity[0]) if integrity else None
            result["schema_version"] = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
            for table in ("jobs", "runs", "outbox_items", "semantic_tasks"):
                rows = connection.execute(f"SELECT status, COUNT(*) FROM {table} GROUP BY status").fetchall()
                result["tables"][table] = {str(status): int(count) for status, count in rows}
            result["tables"]["error_events"] = {
                "total": int(connection.execute("SELECT COUNT(*) FROM error_events").fetchone()[0])
            }
            rows = connection.execute("SELECT status, COUNT(*) FROM execution_slots GROUP BY status").fetchall()
            result["slots"] = {str(status): int(count) for status, count in rows}
            for table in ("jobs", "runs", "outbox_items", "semantic_tasks"):
                for status in ("queued", "running", "retry_wait", "in_flight", "accepted", "processing"):
                    count = connection.execute(f"SELECT COUNT(*) FROM {table} WHERE status=?", (status,)).fetchone()[0]
                    if count:
                        result["active"].append({"table": table, "status": status, "count": int(count)})
            result["dead_letter"] = {
                "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status='dead_letter'").fetchone()[0]),
                "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status='dead_letter'").fetchone()[0]),
            }
            result["dead_letter"]["total"] = result["dead_letter"]["outbox"] + result["dead_letter"]["semantic"]
            result["terminal_failed"] = {
                "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status='failed'").fetchone()[0]),
                "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status='failed'").fetchone()[0]),
            }
            result["terminal_failed"]["total"] = result["terminal_failed"]["outbox"] + result["terminal_failed"]["semantic"]
            # Dead-letter rows are terminal evidence, not active queue work.
            # Keep their count visible so a PASS cannot silently hide them.
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
            for row in connection.execute(
                "SELECT provider_key,provider,endpoint,model,throttle_until,circuit_state,consecutive_429,last_retry_after,updated_at "
                "FROM provider_buckets ORDER BY updated_at DESC LIMIT 20"
            ).fetchall():
                result["providers"].append(dict(zip(
                    ("provider_key", "provider", "endpoint", "model", "throttle_until", "circuit_state", "consecutive_429", "last_retry_after", "updated_at"),
                    row,
                )))
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


class _ReadOnlyCockpitStore:
    """Minimal store facade that prevents CockpitReadModel from enabling WAL."""

    read_only = True

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.db_path = self.path

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=2)
        connection.row_factory = sqlite3.Row
        return connection

    def schema_version(self) -> int:
        with self.connect() as connection:
            return int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])


def cockpit_snapshot() -> dict[str, Any]:
    """Read the same cockpit projection without network probes or DB writes."""
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
            "terminal_failed": snapshot.get("summary", {}).get("terminal_failed", 0),
            "unknown_key_modules": unknown_key_modules,
            "missing_watermarks": missing_watermarks,
            # A healthy response is only truthful when all key signals and
            # watermarks are present and fresh.  Degraded/unknown is an honest
            # state and must be disclosed, not rewritten as green.
            "health_truthful": status != "healthy" or (not unknown_key_modules and not missing_watermarks),
            "evidence_complete": not unknown_key_modules and not missing_watermarks,
        })
        return result
    except (ImportError, OSError, sqlite3.Error, ValueError, KeyError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def automation_statuses() -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for automation_id in AUTOMATION_IDS:
        path = AUTOMATION_ROOT / automation_id / "automation.toml"
        item: dict[str, Any] = {"path": str(path), "exists": path.is_file(), "status": None}
        if path.is_file():
            try:
                value = tomllib.loads(path.read_text(encoding="utf-8"))
                item.update({"id": value.get("id"), "name": value.get("name"), "status": value.get("status"), "rrule": value.get("rrule")})
            except (OSError, tomllib.TOMLDecodeError) as exc:
                item["error"] = f"{type(exc).__name__}: {exc}"
        result[automation_id] = item
    return result


def _workday_gate_waivable(observations: Mapping[str, Any]) -> bool:
    """Allow only a date-count waiver, never a quality or stability waiver."""
    return (
        len(observations.get("selected_dates", [])) < 2
        and len(observations.get("pass_dates", [])) >= 1
        and not observations.get("invalid")
        and not observations.get("rejected")
        and not observations.get("stability", {}).get("checked", False)
    )


def audit(waiver: Optional[Mapping[str, str]] = None) -> dict[str, Any]:
    flags = {"PM_V44_AUTOMATION_FREEZE": launch_flag("PM_V44_AUTOMATION_FREEZE"), "PM_V44_ADMISSION": launch_flag("PM_V44_ADMISSION")}
    launchd = {label: launchd_loaded(label) for label in LABELS}
    reports = [{"markdown": report_status(REPORT_DIR / md), "html": report_status(REPORT_DIR / html_name)} for md, html_name in REPORT_PAIRS]
    processes = process_snapshot()
    database = db_snapshot()
    cockpit = cockpit_snapshot()
    automations = automation_statuses()
    observations = observation_samples()
    errors: list[str] = []
    warnings: list[str] = []
    latest_sample_at: Optional[datetime] = None
    selected_samples = observations.get("selected_samples") if isinstance(observations.get("selected_samples"), list) else []
    if selected_samples:
        latest_sample_at = max(
            (parsed for parsed in (_parse_timestamp(item.get("observed_at")) for item in selected_samples if isinstance(item, Mapping)) if parsed is not None),
            default=None,
        )
    if flags != {"PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_ADMISSION": "on"}:
        errors.append("freeze/admission are not off/on")
    if any(value is not True for value in launchd.values()):
        errors.append("one or more required LaunchAgents are not loaded")
    if any(not item["markdown"]["pass"] or not item["html"]["pass"] for item in reports):
        errors.append("required S9.3 report pair missing or not PASS")
    if processes.get("probe_error"):
        errors.append("process probe failed")
    if len(processes.get("control_plane", [])) != 1 or len(processes.get("worker", [])) != 1:
        errors.append("Control Plane/Worker process count is not exactly one")
    if processes.get("forbidden"):
        errors.append("business writer process observed")
    if database.get("error"):
        errors.append("coordination database read failed")
    if database.get("integrity_check") not in (None, "ok"):
        errors.append(f"coordination database integrity check failed: {database.get('integrity_check')}")
    if database.get("active"):
        errors.append("active queue state remains")
    if database.get("exists") and database.get("slots", {}).get("leased", 0):
        errors.append("execution slot remains leased")
    missing_automations = [automation_id for automation_id in AUTOMATION_IDS if automation_id not in automations]
    if missing_automations:
        errors.append("required Codex Automation is missing: " + ", ".join(missing_automations))
    if any(item.get("status") != "ACTIVE" for item in automations.values()):
        errors.append("Codex Automation is not ACTIVE")
    latest_mutation = _parse_timestamp(database.get("latest_mutation_at"))
    post_sample_state_change = bool(latest_sample_at and latest_mutation and latest_mutation > latest_sample_at)
    if post_sample_state_change:
        warnings.append(
            "coordination state changed after the latest selected S10 sample; current DB/cockpit snapshot is authoritative "
            f"(sample={latest_sample_at.isoformat()}, mutation={latest_mutation.isoformat()})"
        )
    if cockpit.get("error"):
        warnings.append("cockpit snapshot unavailable: " + str(cockpit["error"]))
    elif not cockpit.get("available"):
        warnings.append("cockpit snapshot unavailable")
    else:
        if not cockpit.get("health_truthful"):
            errors.append("cockpit reported healthy while key signals or watermarks are missing/stale")
        if cockpit.get("status") == "incident":
            errors.append("cockpit reports incident")
        if cockpit.get("status") in {"degraded", "unknown"}:
            detail: list[str] = [f"status={cockpit.get('status')}"]
            if cockpit.get("unknown_key_modules"):
                detail.append("unknown/stale modules=" + ",".join(cockpit["unknown_key_modules"]))
            if cockpit.get("missing_watermarks"):
                detail.append("missing watermarks=" + ",".join(cockpit["missing_watermarks"]))
            if cockpit.get("dead_letter"):
                detail.append(f"terminal dead-letter rows={cockpit['dead_letter']}")
            if cockpit.get("terminal_failed"):
                detail.append(f"terminal failed rows={cockpit['terminal_failed']}")
            warnings.append("cockpit is not fully healthy; this state is disclosed: " + "; ".join(detail))
    waiver_record: dict[str, Any] = {
        "requested": bool(waiver),
        "applied": False,
        "gate": "two_distinct_local_workdays",
    }
    if waiver:
        waiver_record.update({
            "id": str(waiver.get("id", "")),
            "waived_by": str(waiver.get("waived_by", "")),
            "reason": str(waiver.get("reason", "")),
        })
    if not observations.get("pass"):
        if waiver and _workday_gate_waivable(observations):
            waiver_record["applied"] = True
            waiver_record["raw_gate_status"] = "HOLD_CONTINUE"
        else:
            errors.append(WORKDAY_GATE_ERROR)
    effective_pass = not errors
    status = "PASS_WITH_WAIVER" if effective_pass and waiver_record["applied"] else "PASS" if effective_pass else "HOLD_CONTINUE"
    return {
        "schema_version": "pm-system.s10-final-gate.v4",
        "phase_id": "S10-final-gate",
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "status": status,
        "read_only": True,
        "flags": flags,
        "launchd": launchd,
        "reports": reports,
        "processes": processes,
        "database": database,
        "cockpit": cockpit,
        "observation_freshness": {
            "latest_selected_sample_at": latest_sample_at.isoformat().replace("+00:00", "Z") if latest_sample_at else None,
            "latest_database_mutation_at": database.get("latest_mutation_at"),
            "post_sample_state_change": post_sample_state_change,
        },
        "automations": automations,
        "observations": observations,
        "errors": errors,
        "warnings": warnings,
        "waiver": waiver_record,
        "effective_pass": effective_pass,
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def write_report(path: Path, value: Mapping[str, Any]) -> None:
    reports = value["reports"]
    automation_summary = ", ".join(f"{key}={item.get('status')}" for key, item in value["automations"].items())
    flags_ok = value["flags"] == {"PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_ADMISSION": "on"}
    launchd_ok = all(item is True for item in value["launchd"].values())
    queues_ok = not value["database"].get("active") and not value["database"].get("slots", {}).get("leased", 0)
    automations_ok = all(item.get("status") == "ACTIVE" for item in value["automations"].values())
    observations = value.get("observations", {})
    waiver = value.get("waiver", {})
    observations_ok = bool(observations.get("pass")) or bool(waiver.get("applied"))
    observations_result = "PASS" if observations.get("pass") else "PASS (人工豁免)" if waiver.get("applied") else "FAIL"
    lines = [
        "# V4.4 S10 最终验收门禁检查报告",
        "",
        f"> phase_id：`{value['phase_id']}`",
        f"> 当前判定：**{value['status']}**",
        "> 运行边界：只读；不创建 Run、不 claim、不调用 OneAPI/OpenViking 业务写入",
        "",
        "## 1. 门禁结论",
        "",
        f"- freeze/admission：`{value['flags'].get('PM_V44_AUTOMATION_FREEZE')}` / `{value['flags'].get('PM_V44_ADMISSION')}`",
        f"- 必需 LaunchAgent：`{sum(item is True for item in value['launchd'].values())}/{len(value['launchd'])}` loaded",
        f"- S9.3 报告对：`{sum(item['markdown']['pass'] and item['html']['pass'] for item in reports)}/{len(reports)}` PASS",
        f"- Control Plane/Worker：`{len(value['processes'].get('control_plane', []))}/{len(value['processes'].get('worker', []))}`",
        f"- active queue state：`{value['database'].get('active') or []}`",
        f"- Automation：`{automation_summary}`",
        f"- S10 观察工作日：`{value.get('observations', {}).get('selected_dates', [])}`",
        "",
        "## 2. 阶段报告",
        "",
        "| Markdown | HTML | 结果 |",
        "|---|---|---|",
    ]
    for item in reports:
        lines.append(f"| `{Path(item['markdown']['path']).name}` | `{Path(item['html']['path']).name}` | {'PASS' if item['markdown']['pass'] and item['html']['pass'] else 'FAIL'} |")
    lines.extend([
        "",
        "## 3. 判定",
        "",
        "| 检查 | 结果 |",
        "|---|---|",
        f"| freeze/admission=off/on | {'PASS' if flags_ok else 'FAIL'} |",
        f"| 全部必需 LaunchAgent loaded | {'PASS' if launchd_ok else 'FAIL'} |",
        f"| 无活动队列或 leased slot | {'PASS' if queues_ok else 'FAIL'} |",
        f"| Automation 全部 ACTIVE | {'PASS' if automations_ok else 'FAIL'} |",
        f"| 两个不同工作日观察样本稳定 | {observations_result} |",
        f"| 无业务 Writer 进程 | {'PASS' if not value['processes'].get('forbidden') else 'FAIL'} |",
        "",
        f"### 判定：`{value['status']}`",
        "",
        "通过后才允许执行最终解冻收口；未通过继续保持当前环境，不自动解冻或回滚。",
        "",
        "## 4. 人工豁免",
        "",
        f"- 请求：`{waiver.get('requested', False)}`；已应用：`{waiver.get('applied', False)}`",
        f"- 豁免编号：`{waiver.get('id', '') or '无'}`",
        f"- 授权方：`{waiver.get('waived_by', '') or '无'}`",
        f"- 理由：{waiver.get('reason', '') or '无'}",
        "- 豁免范围：仅覆盖‘两个不同本地工作日样本’的日期数量要求；原始样本和未豁免门禁结果保持不变。",
        "",
        "## 5. 错误",
        "",
    ])
    lines.extend(f"- {error}" for error in value["errors"] or ["无"])
    lines.extend([
        "",
        "## 6. 驾驶舱实时证据",
        "",
        f"- 可读取：`{value.get('cockpit', {}).get('available', False)}`",
        f"- 实时状态：`{value.get('cockpit', {}).get('status', 'unknown')}`",
        f"- 健康语义未被误报：`{value.get('cockpit', {}).get('health_truthful', False)}`",
        f"- unknown/stale 关键模块：`{value.get('cockpit', {}).get('unknown_key_modules', [])}`",
        f"- 缺失水位：`{value.get('cockpit', {}).get('missing_watermarks', [])}`",
        f"- 终态 dead-letter 总数（不等于活动队列）：`{value.get('database', {}).get('dead_letter', {}).get('total', 0)}`",
        f"- 终态 failed 总数（不等于活动队列）：`{value.get('database', {}).get('terminal_failed', {}).get('total', 0)}`",
        f"- 终态 failed 分项：`{value.get('database', {}).get('terminal_failed', {})}`",
        "",
        "驾驶舱状态与 final gate 分开记录：历史 dead-letter 不作为活动队列，但必须可见；unknown/degraded 不能被报告改写为 healthy。",
        "",
        "## 7. 观察证据",
        "",
    ])
    lines.extend(f"- {warning}" for warning in value.get("warnings", []) or ["无"])
    lines.extend([
        "",
        "## 8. S10 观察证据",
        "",
        f"- 样本总数：`{observations.get('sample_count', 0)}`",
        f"- 不同本地日期：`{observations.get('distinct_local_dates', [])}`",
        f"- 通过日期：`{observations.get('pass_dates', [])}`",
        f"- 选用日期：`{observations.get('selected_dates', [])}`",
        f"- 稳定性：`{observations.get('stability', {})}`",
        "",
        "同一自然日的重复采样只作为诊断证据，不计入两个工作日门禁；样本必须保持只读边界、无活动队列/leased slot、时间轴 marker 正常、健康巡检全通过，并覆盖性能指标。",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_html(path: Path, report_path: Path, value: Mapping[str, Any]) -> None:
    """Render the final gate report without adding another document toolchain."""
    write_report(report_path, value)
    body: list[str] = []
    for raw in report_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("# "):
            body.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            body.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("### "):
            body.append(f"<h3>{html.escape(line[4:])}</h3>")
        elif line.startswith("> "):
            body.append(f"<aside>{html.escape(line[2:])}</aside>")
        elif line.startswith("- "):
            body.append(f"<p>{html.escape(line[2:])}</p>")
        elif line.startswith("|"):
            body.append(f"<pre>{html.escape(line)}</pre>")
        else:
            body.append(f"<p>{html.escape(line)}</p>")
    document = "<!doctype html><meta charset=\"utf-8\"><title>V4.4 S10 最终验收门禁检查报告</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;color:#24302f}h1{font-size:32px;border-bottom:2px solid #24302f;padding-bottom:12px}h2{margin-top:32px}aside{padding:12px 16px;background:#eef3f1;border-left:4px solid #54756b}pre{white-space:pre-wrap;background:#f5f6f4;padding:8px}</style><main>" + "".join(body) + "</main>"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document, encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--html", type=Path)
    parser.add_argument("--waive-workday-gate", action="store_true", help="显式豁免第二个不同本地工作日样本门禁；不豁免其他门禁")
    parser.add_argument("--waiver-id", help="人工豁免编号（与 --waive-workday-gate 一起使用）")
    parser.add_argument("--waived-by", help="人工豁免授权方（与 --waive-workday-gate 一起使用）")
    parser.add_argument("--waiver-reason", help="人工豁免理由（与 --waive-workday-gate 一起使用）")
    args = parser.parse_args(argv)
    if args.waive_workday_gate and not all((args.waiver_id, args.waived_by, args.waiver_reason)):
        parser.error("--waive-workday-gate requires --waiver-id, --waived-by and --waiver-reason")
    if not args.waive_workday_gate and any((args.waiver_id, args.waived_by, args.waiver_reason)):
        parser.error("waiver metadata requires --waive-workday-gate")
    waiver = {
        "id": args.waiver_id,
        "waived_by": args.waived_by,
        "reason": args.waiver_reason,
    } if args.waive_workday_gate else None
    value = audit(waiver)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, value)
    if args.html:
        write_html(args.html, args.report, value)
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "effective_pass": value["effective_pass"], "waiver": value["waiver"], "errors": value["errors"], "warnings": value.get("warnings", []), "cockpit": value.get("cockpit", {}), "manifest": str(args.output), "report": str(args.report)}, ensure_ascii=False))
    return 0 if value["effective_pass"] else 10


if __name__ == "__main__":
    raise SystemExit(main())
