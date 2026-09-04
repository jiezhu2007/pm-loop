#!/usr/bin/env python3
"""Fixed command registry for PM Loop scheduled jobs.

The calendar dispatcher accepts data, while this module owns the executable
surface.  A scheduled payload may select a known ``schedule_key`` only; it
cannot provide a shell command or arbitrary arguments.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from process_utils import run_process_group


CONTENT_RECLAIM_AUTHORIZATION_ROOT = (
    Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1])))
    / "state" / "pm-loop" / "physical-reclaim-authorizations"
)
PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()


@dataclass(frozen=True)
class ScheduledHandler:
    schedule_key: str
    handler: str
    timeout_seconds: int
    command: Callable[[Path], list[str]]


def _weekly_sync(codex_root: Path) -> list[str]:
    return ["/bin/bash", str(codex_root / "scripts" / "weekly-sync-and-refresh.sh")]


def _concept_refresh_planner(_codex_root: Path) -> list[str]:
    # This script lives in the same runtime mirror as this fixed registry.
    # Its inputs are limited to Scheduler-owned environment identity fields.
    return [sys.executable, str(Path(__file__).with_name("concept_refresh_planner.py"))]


def _product_intelligence(codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(codex_root / "skills" / "product-intelligence-monitor" / "scripts" / "sync.py"),
        "weekly",
    ]


def _timeline_daily(codex_root: Path) -> list[str]:
    return ["/bin/bash", str(codex_root / "skills" / "pm-timeline" / "scripts" / "daily.sh")]


def _timeline_weekly(codex_root: Path) -> list[str]:
    return ["/bin/bash", str(codex_root / "skills" / "pm-timeline" / "scripts" / "weekly-review.sh")]


def _product_docs_gap_report(_codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "generate_product_docs_gap_report.py"),
    ]


def _databuilder_product_gap_report(_codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "generate_databuilder_product_gap_report.py"),
    ]


def _weekly_report_reminder(_codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_weekly_report_reminder.py"),
        "--dry-run",
    ]


def _competitive_radar_ingest(_codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "competitive_radar.py"),
        "ingest",
    ]


def _competitive_radar_brief(_codex_root: Path) -> list[str]:
    return [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "competitive_radar.py"),
        "brief",
    ]


def _retention_observer(_codex_root: Path) -> list[str]:
    return [sys.executable, str(Path(__file__).with_name("retention_observer.py"))]


def _retention_reclaimer(_codex_root: Path) -> list[str]:
    return [sys.executable, str(Path(__file__).with_name("retention_reclaimer.py"))]


def _concept_inventory_compaction(_codex_root: Path) -> list[str]:
    # This is deliberately observe-only. Physical replacement requires both
    # explicit CLI flags and a separate authorization decision.
    return [sys.executable, str(Path(__file__).with_name("concept_inventory_compaction.py"))]


def _artifact_inventory(_codex_root: Path) -> list[str]:
    project_root = PROJECT_ROOT
    return [
        sys.executable,
        str(Path(__file__).with_name("artifact_inventory.py")),
        "--root",
        str(project_root),
        "--output-dir",
        str(project_root / "state" / "pm-loop" / "artifact-inventory"),
        "--max-seconds",
        "2700",
    ]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _claim_content_reclaim_authorization(
    *, authorization_root: Path, authorization_id: str, authorization_sha256: str,
    request: Mapping[str, Any],
) -> Path:
    claim = authorization_root / f"{authorization_id}.claimed.json"
    claim.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "schema_version": "pm-loop.physical-reclaim-authorization-claim.v1",
        "authorization_id": authorization_id,
        "authorization_sha256": authorization_sha256,
        "claimed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "run_id": str(request.get("run_id") or ""),
        "job_id": str(request.get("job_id") or ""),
        "occurrence_id": str(request.get("occurrence_id") or ""),
        "schedule_key": "concept-inventory-compaction",
        "target": "content-dedup.json",
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(str(claim), flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        claim.unlink(missing_ok=True)
        raise
    directory_descriptor = os.open(str(claim.parent), os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return claim


def _authorized_content_reclaim_args(request: Mapping[str, Any]) -> list[str]:
    """Return fixed apply flags only for one hash-bound approval envelope."""
    action = request.get("maintenance_action")
    if action is None:
        payload = request.get("payload")
        action = payload.get("maintenance_action") if isinstance(payload, Mapping) else None
    if action is None:
        return []
    if not isinstance(action, Mapping):
        raise ValueError("maintenance_action must be an object")
    if {
        "action": str(action.get("action") or ""),
        "target": str(action.get("target") or ""),
        "approved_by": str(action.get("approved_by") or ""),
    } != {
        "action": "replace_with_verified_gzip",
        "target": "content-dedup.json",
        "approved_by": "zhujie14",
    }:
        raise ValueError("content-dedup reclaim authorization scope mismatch")
    evidence = Path(str(action.get("authorization_evidence") or "")).expanduser().resolve()
    authorization_root = CONTENT_RECLAIM_AUTHORIZATION_ROOT.expanduser().resolve()
    try:
        evidence.relative_to(authorization_root)
    except ValueError as exc:
        raise ValueError("content-dedup reclaim authorization escapes the controlled root") from exc
    if evidence.is_symlink() or not evidence.is_file():
        raise ValueError("content-dedup reclaim authorization evidence is unavailable")
    expected_hash = str(action.get("authorization_sha256") or "")
    if not expected_hash or _sha256_file(evidence) != expected_hash:
        raise ValueError("content-dedup reclaim authorization hash mismatch")
    value = json.loads(evidence.read_text(encoding="utf-8"))
    scope = value.get("scope") if isinstance(value, Mapping) else None
    if not isinstance(scope, Mapping) or {
        "schedule_key": str(scope.get("schedule_key") or ""),
        "action": str(scope.get("action") or ""),
        "target": str(scope.get("target") or ""),
    } != {
        "schedule_key": "concept-inventory-compaction",
        "action": "replace_with_verified_gzip",
        "target": "content-dedup.json",
    }:
        raise ValueError("content-dedup reclaim evidence scope mismatch")
    if str(value.get("status") or "") != "approved" or str(value.get("approved_by") or "") != "zhujie14":
        raise ValueError("content-dedup reclaim evidence is not approved")
    if int(value.get("max_executions") or 0) != 1:
        raise ValueError("content-dedup reclaim authorization must be single-use")
    authorization_id = str(action.get("authorization_id") or "")
    if not authorization_id or str(value.get("authorization_id") or "") != authorization_id:
        raise ValueError("content-dedup reclaim authorization id mismatch")
    expires_at = str(value.get("expires_at") or "")
    if not expires_at:
        raise ValueError("content-dedup reclaim authorization has no expiry")
    try:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("content-dedup reclaim authorization expiry is invalid") from exc
    if expiry.replace(tzinfo=expiry.tzinfo or timezone.utc).astimezone(timezone.utc) <= datetime.now(timezone.utc):
        raise ValueError("content-dedup reclaim authorization expired")
    try:
        _claim_content_reclaim_authorization(
            authorization_root=authorization_root,
            authorization_id=authorization_id,
            authorization_sha256=expected_hash,
            request=request,
        )
    except FileExistsError as exc:
        raise ValueError("content-dedup reclaim authorization already claimed") from exc
    return ["--apply", "--confirm-content-dedup-reclaim"]


HANDLERS: Dict[str, ScheduledHandler] = {
    "weekly-sync-and-refresh": ScheduledHandler("weekly-sync-and-refresh", "weekly_sync_and_refresh", 12 * 3600, _weekly_sync),
    "concept-refresh-planner": ScheduledHandler("concept-refresh-planner", "concept_refresh_planner", 30 * 60, _concept_refresh_planner),
    "product-intelligence-monitor": ScheduledHandler("product-intelligence-monitor", "product_intelligence_weekly", 4 * 3600, _product_intelligence),
    "pm-timeline-daily": ScheduledHandler("pm-timeline-daily", "pm_timeline_daily", 15 * 60, _timeline_daily),
    "pm-timeline-weekly": ScheduledHandler("pm-timeline-weekly", "pm_timeline_weekly", 30 * 60, _timeline_weekly),
    "product-docs-gap-report": ScheduledHandler("product-docs-gap-report", "product_docs_gap_report", 45 * 60, _product_docs_gap_report),
    "databuilder-product-gap-report": ScheduledHandler("databuilder-product-gap-report", "databuilder_product_gap_report", 75 * 60, _databuilder_product_gap_report),
    "weekly-report-reminder": ScheduledHandler("weekly-report-reminder", "weekly_report_reminder", 30 * 60, _weekly_report_reminder),
    "competitive-radar-ingest": ScheduledHandler("competitive-radar-ingest", "competitive_radar_ingest", 90 * 60, _competitive_radar_ingest),
    "competitive-radar-brief": ScheduledHandler("competitive-radar-brief", "competitive_radar_brief", 60 * 60, _competitive_radar_brief),
    "retention-observer": ScheduledHandler("retention-observer", "retention_observer", 60 * 60, _retention_observer),
    "retention-reclaimer": ScheduledHandler("retention-reclaimer", "retention_reclaimer", 90 * 60, _retention_reclaimer),
    "concept-inventory-compaction": ScheduledHandler("concept-inventory-compaction", "concept_inventory_compaction", 30 * 60, _concept_inventory_compaction),
    "artifact-inventory": ScheduledHandler("artifact-inventory", "artifact_inventory", 60 * 60, _artifact_inventory),
}


def resolve_handler(request: Mapping[str, Any], codex_root: Path) -> tuple[ScheduledHandler, list[str]]:
    """Resolve a scheduled command from the immutable key-to-command map."""
    schedule_key = str(request.get("schedule_key") or "").strip()
    handler_name = str(request.get("handler") or "").strip()
    spec = HANDLERS.get(schedule_key)
    if spec is None:
        raise ValueError(f"unsupported scheduled schedule_key: {schedule_key or '<empty>'}")
    if handler_name and handler_name != spec.handler:
        raise ValueError(f"scheduled handler mismatch for {schedule_key}: {handler_name}")
    command = spec.command(Path(codex_root).expanduser().resolve())
    if schedule_key == "retention-reclaimer" and request.get("trigger_kind") == "manual_replay":
        payload = request.get("payload")
        nested = payload if isinstance(payload, Mapping) else {}
        replay_mode = str(request.get("replay_mode") or nested.get("replay_mode") or "")
        physical_authorized = request.get("physical_action_authorized") is True or nested.get("physical_action_authorized") is True
        if replay_mode == "dry_run":
            command.append("--dry-run")
        elif replay_mode == "confirmed" and physical_authorized:
            pass
        else:
            raise ValueError("retention replay mode is not authorized")
    if schedule_key == "concept-inventory-compaction":
        command.extend(_authorized_content_reclaim_args(request))
    if not command or not all(str(item).strip() for item in command):
        raise ValueError(f"invalid command for scheduled handler: {schedule_key}")
    return spec, command


def default_invoker(command: list[str], timeout: int, env: Optional[dict[str, str]] = None):
    return run_process_group(
        command,
        timeout=max(1, int(timeout)),
        env=env,
        stdin=None,
        capture_output=True,
    )


def scheduled_environment(request: Mapping[str, Any]) -> dict[str, str]:
    """Build a minimal child environment with worker-owned identity fields."""
    whitelist = {"PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR", "PYTHONUNBUFFERED"}
    environment = {key: value for key, value in os.environ.items() if key in whitelist}
    for key, expected in (("SANDBOX_USERNAME", "zhujie14"), ("BAIDU_CC_USERNAME", "zhujie14")):
        inherited = os.environ.get(key)
        if inherited and inherited != expected:
            raise ValueError(f"conflicting scheduled identity in environment: {key}")
    for key, env_name in (
        ("occurrence_id", "PM_SCHEDULED_OCCURRENCE_ID"),
        ("schedule_key", "PM_SCHEDULE_KEY"),
        ("registry_hash", "PM_SCHEDULE_REGISTRY_HASH"),
        ("scheduled_at", "PM_SCHEDULED_AT"),
        ("deadline_at", "PM_SCHEDULE_DEADLINE_AT"),
        ("run_id", "PM_SCHEDULE_RUN_ID"),
        ("job_id", "PM_SCHEDULE_JOB_ID"),
        ("run_envelope", "PM_SCHEDULE_RUN_ENVELOPE"),
        ("task_package_candidate", "PM_SCHEDULE_TASK_PACKAGE_CANDIDATE"),
        ("db_path", "PM_SCHEDULE_DB_PATH"),
    ):
        value = str(request.get(key) or "").strip()
        if value:
            environment[env_name] = value
    dependency = request.get("dependency")
    if not isinstance(dependency, Mapping):
        payload = request.get("payload")
        dependency = payload.get("dependency") if isinstance(payload, Mapping) else None
    if isinstance(dependency, Mapping):
        for key, env_name in (
            ("event_id", "PM_CONCEPT_DEPENDENCY_EVENT_ID"),
            ("source_manifest_path", "PM_CONCEPT_SOURCE_MANIFEST_PATH"),
            ("source_manifest_hash", "PM_CONCEPT_SOURCE_MANIFEST_HASH"),
            ("planner_version", "PM_CONCEPT_PLANNER_VERSION"),
        ):
            value = str(dependency.get(key) or "").strip()
            if value:
                environment[env_name] = value
    if request.get("schedule_key") == "weekly-sync-and-refresh":
        occurrence_id = str(request.get("occurrence_id") or "").strip()
        if occurrence_id:
            environment["WEEKLY_RUN_ID"] = occurrence_id
    environment["SANDBOX_USERNAME"] = "zhujie14"
    environment["BAIDU_CC_USERNAME"] = "zhujie14"
    return environment


__all__ = ["HANDLERS", "ScheduledHandler", "default_invoker", "resolve_handler", "scheduled_environment"]
