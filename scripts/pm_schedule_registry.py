#!/usr/bin/env python3
"""Canonical calendar registry used by the PM Loop dispatcher.

The registry is deliberately data-only.  This module validates it, computes a
stable hash, and translates local calendar windows to UTC without executing a
job or touching the coordination database.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


REGISTRY_SCHEMA = "pm-loop.schedule-registry.v1"
DEFAULT_REGISTRY_PATH = Path(__file__).with_name("schedule-registry.json")
REQUIRED_HANDLERS = frozenset(
    {"weekly_sync_and_refresh", "concept_refresh_planner", "product_intelligence_weekly", "pm_timeline_daily", "pm_timeline_weekly", "product_docs_gap_report", "databuilder_product_gap_report", "weekly_report_reminder", "competitive_radar_ingest", "competitive_radar_brief", "retention_observer", "retention_reclaimer", "concept_inventory_compaction", "artifact_inventory"}
)
BUSINESS_WINDOW_START_MINUTE = 10 * 60
BUSINESS_WINDOW_END_MINUTE = 18 * 60
_DURATION = re.compile(r"^PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?$")


class RegistryError(ValueError):
    """Raised when a registry is malformed or unsafe to use."""


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value


def canonical_hash(document: Mapping[str, Any]) -> str:
    """Hash registry content while ignoring its optional self-hash field."""
    value = dict(document)
    value.pop("registry_hash", None)
    encoded = json.dumps(_canonical_value(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def parse_duration(value: Any) -> timedelta:
    match = _DURATION.fullmatch(str(value or "").strip().upper())
    if not match:
        raise RegistryError(f"invalid ISO duration: {value!r}")
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    if hours == 0 and minutes == 0:
        raise RegistryError("duration must be positive")
    return timedelta(hours=hours, minutes=minutes)


def _require_string(value: Any, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise RegistryError(f"{name} is required")
    return result


def is_business_window_open(value: datetime, *, timezone_name: str) -> bool:
    """Return whether a local business occurrence may start at ``value``."""
    local = value.astimezone(ZoneInfo(timezone_name))
    minute = local.hour * 60 + local.minute
    return BUSINESS_WINDOW_START_MINUTE <= minute <= BUSINESS_WINDOW_END_MINUTE


@dataclass(frozen=True)
class ScheduleTask:
    schedule_key: str
    calendar: Optional[Dict[str, Any]]
    trigger: Dict[str, Any]
    deadline: timedelta
    lock: Dict[str, Any]
    evidence: Dict[str, Any]
    job_type: str
    handler: str
    profile: str
    concurrency_key: str
    priority: int
    retry: Dict[str, Any]
    execution_window: Optional[Dict[str, Any]] = None
    delivery_policy: Optional[str] = None

    @property
    def deadline_text(self) -> str:
        seconds = int(self.deadline.total_seconds())
        hours, remainder = divmod(seconds, 3600)
        minutes = remainder // 60
        return f"PT{hours}H" if minutes == 0 else f"PT{hours}H{minutes}M"

    @property
    def trigger_kind(self) -> str:
        return str(self.trigger.get("kind") or "calendar")

    @property
    def is_calendar(self) -> bool:
        return self.trigger_kind == "calendar"


@dataclass(frozen=True)
class ScheduleRegistry:
    registry_version: int
    timezone_name: str
    misfire_policy: Dict[str, Any]
    tasks: Tuple[ScheduleTask, ...]
    registry_hash: str
    source_path: Path

    def task(self, schedule_key: str) -> ScheduleTask:
        for item in self.tasks:
            if item.schedule_key == schedule_key:
                return item
        raise KeyError(schedule_key)


def validate_document(document: Mapping[str, Any], *, source_path: Path = DEFAULT_REGISTRY_PATH) -> ScheduleRegistry:
    if not isinstance(document, Mapping):
        raise RegistryError("registry must be an object")
    timezone_name = _require_string(document.get("timezone"), "timezone")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise RegistryError(f"unknown timezone: {timezone_name}") from exc
    version = int(document.get("registry_version", 0))
    if version != 1:
        raise RegistryError(f"unsupported registry_version: {version}")
    misfire = document.get("misfire_policy")
    if not isinstance(misfire, Mapping) or misfire.get("mode") != "coalesce_latest" or int(misfire.get("max_backfill", 0)) != 1:
        raise RegistryError("misfire_policy must be coalesce_latest with max_backfill=1")
    raw_tasks = document.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise RegistryError("tasks must be a non-empty list")
    tasks = []
    seen = set()
    handlers = set()
    for raw in raw_tasks:
        if not isinstance(raw, Mapping):
            raise RegistryError("each task must be an object")
        key = _require_string(raw.get("schedule_key"), "schedule_key")
        if key in seen:
            raise RegistryError(f"duplicate schedule_key: {key}")
        seen.add(key)
        handler = _require_string(raw.get("handler"), f"{key}.handler")
        handlers.add(handler)
        calendar = raw.get("calendar")
        raw_trigger = raw.get("trigger")
        if calendar is not None:
            if not isinstance(calendar, Mapping) or calendar.get("kind") not in {"daily", "weekly"}:
                raise RegistryError(f"invalid calendar for {key}")
            if raw_trigger not in (None, {}, {"kind": "calendar"}):
                raise RegistryError(f"calendar task may not define a non-calendar trigger: {key}")
            trigger = {"kind": "calendar"}
            hour, minute = int(calendar.get("hour", -1)), int(calendar.get("minute", -1))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise RegistryError(f"invalid clock for {key}")
            if not (BUSINESS_WINDOW_START_MINUTE <= hour * 60 + minute <= BUSINESS_WINDOW_END_MINUTE):
                raise RegistryError(f"business schedule for {key} must be within Asia/Shanghai 10:00–18:00")
            if calendar.get("kind") == "weekly" and int(calendar.get("weekday", 0)) not in range(1, 8):
                raise RegistryError(f"weekly weekday must be ISO 1..7 for {key}")
            normalized_calendar: Optional[Dict[str, Any]] = dict(calendar)
        else:
            if not isinstance(raw_trigger, Mapping) or raw_trigger.get("kind") != "dependency":
                raise RegistryError(f"task must define calendar or dependency trigger: {key}")
            trigger = dict(raw_trigger)
            upstream = _require_string(trigger.get("upstream_schedule_key"), f"{key}.trigger.upstream_schedule_key")
            if upstream == key:
                raise RegistryError(f"dependency task cannot depend on itself: {key}")
            if str(trigger.get("terminal_status") or "").strip() != "completed":
                raise RegistryError(f"dependency trigger must require completed upstream: {key}")
            _require_string(trigger.get("required_artifact"), f"{key}.trigger.required_artifact")
            _require_string(trigger.get("planner_version"), f"{key}.trigger.planner_version")
            normalized_calendar = None
        lock = raw.get("lock")
        evidence = raw.get("evidence")
        retry = raw.get("retry")
        for name, value in (("lock", lock), ("evidence", evidence), ("retry", retry)):
            if not isinstance(value, Mapping):
                raise RegistryError(f"{key}.{name} must be an object")
        _require_string(lock.get("key"), f"{key}.lock.key")
        if str(lock.get("mode") or "").strip() != "exclusive":
            raise RegistryError(f"{key}.lock.mode must be exclusive")
        concurrency_key = _require_string(raw.get("concurrency_key"), f"{key}.concurrency_key")
        try:
            max_attempts = int(retry.get("max_attempts"))
        except (TypeError, ValueError) as exc:
            raise RegistryError(f"{key}.retry.max_attempts must be a non-negative integer") from exc
        if max_attempts < 0:
            raise RegistryError(f"{key}.retry.max_attempts must be a non-negative integer")
        parse_duration(retry.get("backoff"))
        normalized_retry = {**dict(retry), "max_attempts": max_attempts, "backoff": str(retry.get("backoff")).upper()}
        delivery_policy = raw.get("delivery_policy")
        if key == "weekly-report-reminder":
            if delivery_policy not in {"dry_run", "scheduled"}:
                raise RegistryError("weekly-report-reminder.delivery_policy must be dry_run or scheduled")
        elif delivery_policy is not None:
            raise RegistryError(f"delivery_policy is only valid for weekly-report-reminder: {key}")
        execution_window = raw.get("execution_window")
        if key == "retention-reclaimer":
            expected_window = {
                "timezone": "Asia/Shanghai", "start": "10:00", "last_batch_start": "16:30", "end": "18:00",
                "no_catchup_outside_window": True, "defer_on_active_p0_p1": True, "max_runtime_minutes": 90,
            }
            if execution_window != expected_window:
                raise RegistryError("retention-reclaimer.execution_window must preserve the fixed maintenance boundary")
        elif execution_window is not None:
            raise RegistryError(f"execution_window is only valid for retention-reclaimer: {key}")
        tasks.append(ScheduleTask(key, normalized_calendar, trigger, parse_duration(raw.get("deadline")), dict(lock), dict(evidence), _require_string(raw.get("job_type"), f"{key}.job_type"), handler, _require_string(raw.get("profile"), f"{key}.profile"), concurrency_key, int(raw.get("priority", 50)), normalized_retry, dict(execution_window) if isinstance(execution_window, Mapping) else None, str(delivery_policy) if delivery_policy is not None else None))
    if handlers != REQUIRED_HANDLERS:
        raise RegistryError(f"handlers must be exactly {sorted(REQUIRED_HANDLERS)}, got {sorted(handlers)}")
    schedule_keys = {task.schedule_key for task in tasks}
    for task in tasks:
        if task.trigger_kind == "dependency":
            upstream = str(task.trigger["upstream_schedule_key"])
            if upstream not in schedule_keys:
                raise RegistryError(f"dependency upstream is not registered: {task.schedule_key}->{upstream}")
            if not next(item for item in tasks if item.schedule_key == upstream).is_calendar:
                raise RegistryError(f"dependency upstream must be a calendar task: {task.schedule_key}->{upstream}")
    return ScheduleRegistry(version, timezone_name, dict(misfire), tuple(tasks), canonical_hash(document), Path(source_path).expanduser().resolve())


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> ScheduleRegistry:
    source = Path(path).expanduser().resolve()
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read registry {source}: {exc}") from exc
    return validate_document(document, source_path=source)


def latest_scheduled_at(task: ScheduleTask, now: datetime, *, timezone_name: str) -> datetime:
    """Return the latest calendar occurrence at or before ``now`` in UTC."""
    if not task.is_calendar or task.calendar is None:
        raise RegistryError(f"latest_scheduled_at requires calendar task: {task.schedule_key}")
    zone = ZoneInfo(timezone_name)
    local_now = now.astimezone(zone)
    candidate = local_now.replace(hour=int(task.calendar["hour"]), minute=int(task.calendar["minute"]), second=0, microsecond=0)
    if task.calendar["kind"] == "weekly":
        target = int(task.calendar["weekday"])
        candidate -= timedelta(days=(candidate.isoweekday() - target) % 7)
    if candidate > local_now:
        candidate -= timedelta(days=1 if task.calendar["kind"] == "daily" else 7)
    return candidate.astimezone(timezone.utc)


def occurrence_key(task: ScheduleTask, scheduled_at: datetime) -> str:
    return f"{task.schedule_key}:{scheduled_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def next_scheduled_at(task: ScheduleTask, scheduled_at: datetime, *, timezone_name: str) -> datetime:
    """Return the next local calendar window after a known occurrence."""
    if not task.is_calendar or task.calendar is None:
        raise RegistryError(f"next_scheduled_at requires calendar task: {task.schedule_key}")
    zone = ZoneInfo(timezone_name)
    local = scheduled_at.astimezone(zone)
    delta = timedelta(days=1 if task.calendar["kind"] == "daily" else 7)
    next_local = (local + delta).replace(
        hour=int(task.calendar["hour"]),
        minute=int(task.calendar["minute"]),
        second=0,
        microsecond=0,
    )
    return next_local.astimezone(timezone.utc)


__all__ = ["BUSINESS_WINDOW_END_MINUTE", "BUSINESS_WINDOW_START_MINUTE", "DEFAULT_REGISTRY_PATH", "REGISTRY_SCHEMA", "RegistryError", "ScheduleRegistry", "ScheduleTask", "canonical_hash", "is_business_window_open", "latest_scheduled_at", "load_registry", "next_scheduled_at", "occurrence_key", "parse_duration", "validate_document"]
