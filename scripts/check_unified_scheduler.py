#!/usr/bin/env python3
"""Read-only health evidence for the PM Loop unified scheduler."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


DEFAULT_DB = Path.home() / ".codex/pm-loop/state/pm-system.db"
DEFAULT_REGISTRY = Path.home() / "Documents/project/scripts/schedule-registry.json"
DEFAULT_RUNTIME_REGISTRY = Path.home() / ".codex/pm-loop/runtime/config/schedule-registry.json"
DEFAULT_SCHEDULER_LABEL = "com.zhujie14.pm-scheduler"
STALE_SECONDS = 5 * 60


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    raw = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _age(value: Any, now: datetime) -> float | None:
    parsed = _parse(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds())


def _canonical_hash(document: Mapping[str, Any]) -> str:
    value = dict(document)
    value.pop("registry_hash", None)
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _planner_version_error(
    *,
    expected: Any,
    actual: Any,
    created_at: Any,
    event_status: Any,
    v12_cutover_at: Any,
) -> str | None:
    """Validate planner versions without rewriting terminal pre-v12 history."""
    expected_text = str(expected or "")
    actual_text = str(actual or "")
    if expected_text == actual_text:
        return None
    event_time = _parse(created_at)
    cutover_time = _parse(v12_cutover_at)
    is_terminal_legacy = str(event_status or "") in {"consumed", "blocked_by_upstream"}
    if (
        expected_text == "concept-refresh-planner.v2"
        and actual_text == "concept-refresh-planner.v1"
        and is_terminal_legacy
        and event_time is not None
        and cutover_time is not None
        and event_time < cutover_time
    ):
        return None
    return "planner_version_mismatch"


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    return (value, None) if isinstance(value, dict) else (None, "registry must be an object")


def _registry_task_summary(document: Mapping[str, Any]) -> tuple[set[str], list[Mapping[str, Any]], list[str]]:
    """Validate task identity without pinning health to a historical task list."""
    raw_tasks = document.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        return set(), [], ["tasks must be a non-empty list"]
    keys: set[str] = set()
    tasks: list[Mapping[str, Any]] = []
    errors: list[str] = []
    for index, raw in enumerate(raw_tasks):
        if not isinstance(raw, Mapping):
            errors.append(f"task[{index}] must be an object")
            continue
        key = raw.get("schedule_key")
        if not isinstance(key, str) or not key.strip():
            errors.append(f"task[{index}].schedule_key is required")
            continue
        normalized = key.strip()
        if normalized in keys:
            errors.append(f"duplicate schedule_key: {normalized}")
            continue
        calendar = raw.get("calendar")
        trigger = raw.get("trigger")
        if isinstance(calendar, Mapping):
            if calendar.get("kind") not in {"daily", "weekly"}:
                errors.append(f"{normalized}.calendar.kind is invalid")
                continue
            if trigger not in (None, {}, {"kind": "calendar"}):
                errors.append(f"{normalized}.trigger conflicts with calendar")
                continue
        elif isinstance(trigger, Mapping) and trigger.get("kind") == "dependency":
            required = ("upstream_schedule_key", "terminal_status", "required_artifact", "planner_version")
            missing = [name for name in required if not str(trigger.get(name) or "").strip()]
            if missing:
                errors.append(f"{normalized}.dependency missing {','.join(missing)}")
                continue
            if str(trigger.get("terminal_status")) != "completed":
                errors.append(f"{normalized}.dependency terminal_status must be completed")
                continue
        else:
            errors.append(f"{normalized} must define calendar or dependency trigger")
            continue
        keys.add(normalized)
        tasks.append(raw)
    return keys, tasks, errors


def _launchd_state(label: str) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"state": "unknown", "detail": f"launchctl probe failed: {exc}"}
    if proc.returncode != 0:
        return {"state": "not_loaded", "detail": "LaunchAgent 未加载"}
    output = proc.stdout or ""
    state = "unknown"
    runs = None
    last_exit_code = None
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("state ="):
            state = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("runs ="):
            runs = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("last exit code ="):
            last_exit_code = stripped.split("=", 1)[1].strip()
    return {"state": state, "runs": runs, "last_exit_code": last_exit_code}


def inspect(*, db_path: Path, registry_path: Path, runtime_registry_path: Path, scheduler_label: str, stale_seconds: int = STALE_SECONDS) -> dict[str, Any]:
    now = _now()
    result: dict[str, Any] = {
        "status": "ok",
        "source_status": "observed",
        "db_path": str(db_path),
        "registry_path": str(registry_path),
        "runtime_registry_path": str(runtime_registry_path),
        "scheduler_label": scheduler_label,
        "checks": {},
    }
    canonical, error = _read_json(registry_path)
    if canonical is None:
        result["checks"]["registry"] = {"status": "failed", "reason": error}
        result["status"] = "failed"
        return result
    expected_hash = _canonical_hash(canonical)
    task_keys, tasks, task_errors = _registry_task_summary(canonical)
    calendars = [item.get("calendar") for item in tasks if isinstance(item.get("calendar"), Mapping)]
    dependency_tasks = [item for item in tasks if isinstance(item.get("trigger"), Mapping) and item["trigger"].get("kind") == "dependency"]
    window_ok = all(
        isinstance(calendar, Mapping)
        and 10 * 60 <= int(calendar.get("hour", -1)) * 60 + int(calendar.get("minute", -1)) <= 18 * 60
        for calendar in calendars
    )
    registry_ok = not task_errors and canonical.get("timezone") == "Asia/Shanghai" and window_ok
    result["checks"]["registry"] = {
        "status": "ok" if registry_ok else "failed",
        "registry_hash": expected_hash,
        "task_keys": sorted(task_keys),
        "task_count": len(task_keys),
        "task_key_source": "canonical_registry",
        "validation_errors": task_errors,
        "timezone": canonical.get("timezone"),
        "business_window": "10:00-18:00",
        "business_window_ok": window_ok,
        "calendar_task_count": len(calendars),
        "dependency_task_count": len(dependency_tasks),
    }
    runtime, runtime_error = _read_json(runtime_registry_path)
    runtime_hash = _canonical_hash(runtime) if runtime is not None else None
    mirror_ok = runtime is not None and runtime_hash == expected_hash
    result["checks"]["registry_mirror"] = {"status": "ok" if mirror_ok else "failed", "canonical_hash": expected_hash, "runtime_hash": runtime_hash, "reason": runtime_error if runtime is None else None}
    if not registry_ok or not mirror_ok:
        result["status"] = "failed"

    if not db_path.is_file():
        result["checks"]["database"] = {"status": "failed", "reason": "pm-system.db 不存在"}
        result["status"] = "failed"
        return result

    try:
        connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
        required_tables = {"schedule_registry_state", "schedule_occurrences", "scheduler_ticks", "jobs", "runs", "delivery_intents"}
        if dependency_tasks:
            required_tables.add("scheduled_dependency_events")
        table_ok = required_tables <= tables
        result["checks"]["database"] = {"status": "ok" if integrity.lower() == "ok" and table_ok else "failed", "integrity": integrity, "schema_tables": sorted(required_tables & tables), "missing_tables": sorted(required_tables - tables)}
        if integrity.lower() != "ok" or not table_ok:
            result["status"] = "failed"
            connection.close()
            return result
        registry_row = connection.execute("SELECT registry_hash,state,loaded_at,updated_at FROM schedule_registry_state WHERE registry_id=1").fetchone()
        db_registry_ok = registry_row is not None and str(registry_row[0]) == expected_hash and str(registry_row[1]) == "valid"
        result["checks"]["registry_state"] = {"status": "ok" if db_registry_ok else "failed", "registry_hash": registry_row[0] if registry_row else None, "state": registry_row[1] if registry_row else None, "loaded_at": registry_row[2] if registry_row else None}
        if not db_registry_ok:
            result["status"] = "failed"
        tick = connection.execute("SELECT * FROM scheduler_ticks ORDER BY started_at DESC,tick_id DESC LIMIT 1").fetchone()
        tick_age = _age((tick["completed_at"] or tick["started_at"]) if tick else None, now)
        tick_ok = tick is not None and str(tick["status"]) == "completed" and str(tick["registry_hash"] or "") == expected_hash and tick_age is not None and tick_age <= max(1, int(stale_seconds))
        result["checks"]["scheduler_tick"] = {"status": "ok" if tick_ok else "failed", "tick_id": tick["tick_id"] if tick else None, "tick_status": tick["status"] if tick else None, "age_seconds": round(tick_age, 1) if tick_age is not None else None, "registry_hash": tick["registry_hash"] if tick else None, "accepted": tick["accepted_count"] if tick else None, "deduplicated": tick["deduplicated_count"] if tick else None, "deferred": tick["deferred_count"] if tick else None, "expired": tick["expired_count"] if tick else None}
        if not tick_ok:
            result["status"] = "failed"
        occurrence_rows = connection.execute("SELECT occurrence_id,schedule_key,state,job_id,run_id,updated_at FROM schedule_occurrences ORDER BY updated_at DESC").fetchall()
        occurrence_counts: dict[str, int] = {}
        orphaned = 0
        for row in occurrence_rows:
            key = f"{row['schedule_key']}:{row['state']}"
            occurrence_counts[key] = occurrence_counts.get(key, 0) + 1
            if row["job_id"] and connection.execute("SELECT 1 FROM jobs WHERE job_id=?", (row["job_id"],)).fetchone() is None:
                orphaned += 1
            if row["run_id"] and connection.execute("SELECT 1 FROM runs WHERE run_id=?", (row["run_id"],)).fetchone() is None:
                orphaned += 1
        occurrence_ok = orphaned == 0
        result["checks"]["occurrences"] = {"status": "ok" if occurrence_ok else "failed", "count": len(occurrence_rows), "by_state": occurrence_counts, "orphan_links": orphaned}
        if not occurrence_ok:
            result["status"] = "failed"
        if dependency_tasks:
            dependency_rows = connection.execute(
                "SELECT * FROM scheduled_dependency_events ORDER BY created_at,event_id"
            ).fetchall()
            v12_cutover = connection.execute(
                "SELECT applied_at FROM schema_migrations WHERE version=12"
            ).fetchone()
            v12_cutover_at = v12_cutover[0] if v12_cutover is not None else None
            dependency_counts: dict[str, int] = {}
            dependency_errors: list[str] = []
            historical_version_events = 0
            for event in dependency_rows:
                value = dict(event)
                event_id = str(value.get("event_id") or "")
                event_status = str(value.get("status") or "")
                dependency_counts[event_status] = dependency_counts.get(event_status, 0) + 1
                dependent = str(value.get("dependent_schedule_key") or "")
                matching = next((task for task in dependency_tasks if str(task.get("schedule_key")) == dependent), None)
                if matching is None:
                    dependency_errors.append(f"{event_id}:dependent_task_not_registered")
                    continue
                trigger = matching.get("trigger") if isinstance(matching.get("trigger"), Mapping) else {}
                if str(trigger.get("upstream_schedule_key") or "") != str(value.get("upstream_schedule_key") or ""):
                    dependency_errors.append(f"{event_id}:upstream_schedule_key_mismatch")
                version_error = _planner_version_error(
                    expected=trigger.get("planner_version"),
                    actual=value.get("planner_version"),
                    created_at=value.get("created_at"),
                    event_status=event_status,
                    v12_cutover_at=v12_cutover_at,
                )
                if version_error:
                    dependency_errors.append(f"{event_id}:{version_error}")
                elif str(trigger.get("planner_version") or "") != str(value.get("planner_version") or ""):
                    historical_version_events += 1
                upstream = connection.execute(
                    "SELECT status,schedule_key,occurrence_id FROM runs WHERE run_id=?",
                    (value.get("upstream_run_id"),),
                ).fetchone()
                if upstream is None:
                    dependency_errors.append(f"{event_id}:upstream_run_missing")
                elif event_status == "consumed" and (
                    str(upstream[0]) != "completed"
                    or str(upstream[1] or "") != str(value.get("upstream_schedule_key") or "")
                    or str(upstream[2] or "") != str(value.get("upstream_occurrence_id") or "")
                ):
                    dependency_errors.append(f"{event_id}:consumed_upstream_contract_invalid")
                if event_status == "consumed":
                    occurrence = connection.execute(
                        "SELECT job_id,run_id,trigger_kind FROM schedule_occurrences WHERE occurrence_id=?",
                        (value.get("occurrence_id"),),
                    ).fetchone()
                    if occurrence is None or not occurrence[0] or not occurrence[1] or str(occurrence[2]) != "dependency":
                        dependency_errors.append(f"{event_id}:consumed_occurrence_missing")
                if event_status in {"pending", "consumed"}:
                    for path_name, hash_name in (("source_manifest_path", "source_manifest_hash"), ("handler_evidence_path", "handler_evidence_hash")):
                        try:
                            path = Path(str(value.get(path_name) or "")).expanduser()
                            if not path.is_file() or _file_hash(path) != str(value.get(hash_name) or ""):
                                dependency_errors.append(f"{event_id}:{path_name}_invalid")
                        except OSError:
                            dependency_errors.append(f"{event_id}:{path_name}_unreadable")
            dependency_ok = not dependency_errors
            result["checks"]["dependencies"] = {
                "status": "ok" if dependency_ok else "failed",
                "count": len(dependency_rows),
                "by_status": dependency_counts,
                "version_cutover_at": v12_cutover_at,
                "historical_version_events": historical_version_events,
                "errors": dependency_errors[:100],
            }
            if not dependency_ok:
                result["status"] = "failed"
        connection.close()
    except (OSError, sqlite3.Error) as exc:
        result["checks"]["database"] = {"status": "failed", "reason": f"{type(exc).__name__}: {exc}"}
        result["status"] = "failed"
    launchd = _launchd_state(scheduler_label)
    # launchctl reports short-lived interval jobs as ``active`` while a tick
    # is executing and ``not running`` between ticks.  Both are healthy when
    # the DB tick evidence is fresh; only an unobservable/missing job is a
    # scheduler presence failure.
    launchd_ok = launchd.get("state") in {"active", "running", "not running"}
    result["checks"]["launchd"] = {**launchd, "status": "ok" if launchd_ok else "failed"}
    if not launchd_ok:
        result["status"] = "failed"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--runtime-registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--scheduler-label", default=DEFAULT_SCHEDULER_LABEL)
    parser.add_argument("--stale-seconds", type=int, default=STALE_SECONDS)
    args = parser.parse_args(argv)
    value = inspect(db_path=args.db_path.expanduser(), registry_path=args.registry.expanduser(), runtime_registry_path=args.runtime_registry.expanduser(), scheduler_label=args.scheduler_label, stale_seconds=args.stale_seconds)
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0 if value.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
