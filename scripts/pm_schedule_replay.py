#!/usr/bin/env python3
"""Create and wait for one controlled PM Worker scheduled replay.

This is the only supported smoke/replay entry for a known schedule.  It uses
the coordination Store API, never accepts a command or path from the caller,
and keeps reminder delivery in dry-run until a separate operating decision is
recorded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from pm_loop_scheduler import DEFAULT_CANONICAL_REGISTRY, DEFAULT_DB_PATH, DEFAULT_LOCK_PATH, DEFAULT_RUNTIME_REGISTRY, PMLoopDispatcher
from pm_schedule_registry import RegistryError
from pm_system_store import PMSystemStore


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _occurrence_id(registry_hash: str, occurrence_key: str) -> str:
    return "occ-" + hashlib.sha256(f"{registry_hash}:{occurrence_key}".encode("utf-8")).hexdigest()[:32]


def build_request(
    *, schedule: Any, registry_hash: str, reason: str, replay_mode: str = "dry_run",
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    if replay_mode not in {"dry_run", "confirmed"}:
        raise ValueError("unsupported replay_mode")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    replay_id = "replay-" + uuid.uuid4().hex
    occurrence_key = f"{schedule.schedule_key}:manual-replay:{replay_id}"
    scheduled_at = _iso(current)
    deadline_at = _iso(current + schedule.deadline)
    payload: dict[str, Any] = {
        "schedule_key": schedule.schedule_key,
        "handler": schedule.handler,
        "job_type": schedule.job_type,
        "loop_id": schedule.schedule_key,
        "profile": schedule.profile,
        "concurrency_key": schedule.concurrency_key,
        "retry": dict(schedule.retry),
        "evidence": dict(schedule.evidence),
        "permission_mode": "report",
        "replay_id": replay_id,
        "replay_reason": reason,
        "display_role": "supplemental_replay",
        "replay_of_occurrence_id": None,
        "delivery_policy": schedule.delivery_policy,
        "replay_mode": replay_mode,
        "scheduled_at": scheduled_at,
        "local_scheduled_at": scheduled_at,
        "deadline_at": deadline_at,
    }
    if replay_mode == "confirmed":
        payload["physical_action_authorized"] = True
    return {
        "schedule_key": schedule.schedule_key,
        "occurrence_id": _occurrence_id(registry_hash, occurrence_key),
        "occurrence_key": occurrence_key,
        "scheduled_at": scheduled_at,
        "local_scheduled_at": scheduled_at,
        "deadline_at": deadline_at,
        "registry_hash": registry_hash,
        "lock_key": str(schedule.lock.get("key") or schedule.schedule_key),
        "trigger_kind": "manual_replay",
        "owner": "pm-schedule-replay",
        "handler": schedule.handler,
        "job_type": schedule.job_type,
        "loop_id": schedule.schedule_key,
        "profile": schedule.profile,
        "concurrency_key": schedule.concurrency_key,
        "retry": dict(schedule.retry),
        "idempotency_key": f"schedule:{occurrence_key}",
        "payload": payload,
    }


def run_replay(args: argparse.Namespace) -> dict[str, Any]:
    if bool(args.dry_run) == bool(args.confirm):
        raise ValueError("exactly one of --dry-run or --confirm is required")
    if args.confirm and args.schedule_key != "retention-reclaimer":
        raise ValueError("manual_decision_required: --confirm is only supported for retention-reclaimer")
    reason = str(args.reason or "").strip()
    if len(reason) < 8:
        raise ValueError("--reason must contain at least 8 characters")
    dispatcher = PMLoopDispatcher(
        args.db_path,
        registry_path=args.registry,
        runtime_registry_path=args.runtime_registry,
        canonical_registry_path=args.canonical_registry,
        lock_path=args.lock_path,
    )
    registry, _ = dispatcher.load_verified_registry()
    try:
        schedule = registry.task(args.schedule_key)
    except KeyError as exc:
        raise RegistryError(f"unknown schedule_key: {args.schedule_key}") from exc
    if schedule.trigger_kind == "dependency":
        # A dependency occurrence is meaningful only when the Scheduler has
        # verified its completed upstream Run and immutable evidence hashes.
        # The generic manual-replay tool cannot invent that context safely.
        raise RegistryError(
            f"dependency_task_requires_canonical_replay: {schedule.schedule_key}; "
            "use pm_dependency_replay.py"
        )
    replay_mode = "confirmed" if args.confirm else "dry_run"
    request = build_request(
        schedule=schedule, registry_hash=registry.registry_hash, reason=reason,
        replay_mode=replay_mode,
    )
    store = PMSystemStore(args.db_path, auto_migrate=False)
    accepted = store.accept_scheduled_occurrence(request)
    result: dict[str, Any] = {"status": "accepted", "mode": replay_mode, "schedule_key": schedule.schedule_key, **accepted}
    if accepted.get("deduplicated"):
        return result
    run_id = str(accepted.get("run_id") or "")
    wait_until = time.monotonic() + max(1, int(args.wait_seconds))
    while run_id and time.monotonic() < wait_until:
        run = store.get_run(run_id) or {}
        status = str(run.get("status") or "")
        if status in {"completed", "failed", "interrupted", "cancelled", "dead_letter"}:
            checkpoint = store.get_checkpoint(run_id, "scheduled", "handler") or {}
            result.update({"run_status": status, "run": run, "checkpoint": checkpoint})
            return result
        time.sleep(max(0.25, float(args.poll_seconds)))
    result.update({"run_status": "timeout_waiting_for_worker", "run": store.get_run(run_id) if run_id else None})
    return result


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule-key", required=True)
    parser.add_argument("--reason", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--confirm", action="store_true")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--runtime-registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--canonical-registry", type=Path, default=DEFAULT_CANONICAL_REGISTRY)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--wait-seconds", type=int, default=5400)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        result = run_replay(args)
    except Exception as exc:
        print(json.dumps({"status": "rejected", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result.get("run_status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
