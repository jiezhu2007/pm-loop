#!/usr/bin/env python3
"""Run the bounded P9.2 dependency replay without external side effects.

The driver is deliberately narrower than the normal replay entrypoint.  It
creates one synthetic, manual-replay upstream occurrence, executes its
completion through the real Worker code with a fixed local result, lets the
real Scheduler consume the resulting event, and executes the disabled concept
planner through the same Worker code.  It never accepts a command, source URI,
or schedule key from the caller.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from pm_loop_scheduler import DEFAULT_CANONICAL_REGISTRY, DEFAULT_DB_PATH, DEFAULT_LOCK_PATH, DEFAULT_RUNTIME_REGISTRY, PMLoopDispatcher
from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore, SCHEMA_VERSION
from pm_system_worker import PMSystemWorker


CODEX_ROOT = Path.home() / ".codex"
DEFAULT_ARTIFACT_ROOT = CODEX_ROOT / "pm-loop" / "runs" / "concept-v11" / "p9-dependency-replay"
DEFAULT_REPORT = CODEX_ROOT / "pm-loop" / "runs" / "concept-v11" / "p9-disabled-dependency-replay.json"
PLANNER_VERSION = "concept-refresh-planner.v2"
UPSTREAM_KEY = "weekly-sync-and-refresh"
DEPENDENT_KEY = "concept-refresh-planner"
PRODUCTION_TABLES = (
    "concept_versions",
    "concept_hot_projection",
    "generations",
    "outbox_items",
    "semantic_tasks",
    "model_calls",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _table_counts(store: PMSystemStore, names: Iterable[str]) -> dict[str, int]:
    with store.connect() as connection:
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        return {
            name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) if name in tables else 0
            for name in names
        }


def _active_counts(store: PMSystemStore) -> dict[str, int]:
    with store.connect() as connection:
        return {
            "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status IN ('pending','in_flight','retry_wait')").fetchone()[0]),
            "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status IN ('queued','in_flight','accepted','processing','retry_wait')").fetchone()[0]),
        }


def _assert_preflight(store: PMSystemStore) -> dict[str, Any]:
    active = _active_counts(store)
    admission = None
    with store.connect() as connection:
        schema = int(connection.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
        row = connection.execute(
            "SELECT namespace_epoch,admission_state,version FROM concept_admissions ORDER BY version DESC,updated_at DESC LIMIT 1"
        ).fetchone()
        admission = dict(row) if row is not None else None
    if schema != SCHEMA_VERSION:
        raise ValueError(f"schema_version_mismatch:{schema}")
    if admission is None or str(admission.get("admission_state") or "") != "disabled":
        raise ValueError("concept_admission_not_disabled")
    if any(active.values()):
        raise ValueError(f"active_work_present:{active}")
    return {"schema_version": schema, "admission": admission, "active": active}


def _settle_prior_fixture_retries(store: PMSystemStore) -> list[str]:
    """Cancel only an earlier P9.2 fixture retry left by a previous harness.

    This is intentionally narrower than a generic cancellation command: the
    persisted payload must identify this exact replay fixture and the upstream
    task. Normal PM work can never be selected here.
    """
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT j.run_id,j.payload_json FROM jobs AS j JOIN runs AS r ON r.run_id=j.run_id "
            "WHERE j.status='retry_wait' AND r.status='retry_wait' "
            "AND j.schedule_key=? AND r.schedule_key=? AND j.trigger_kind='manual_replay'",
            (UPSTREAM_KEY, UPSTREAM_KEY),
        ).fetchall()
    scheduler = Scheduler(store)
    settled: list[str] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        fixture = payload.get("replay_fixture") if isinstance(payload, dict) else None
        if not isinstance(fixture, dict) or fixture.get("stage") != "P9.2" or fixture.get("fixture") != "fixed_local_upstream_completion":
            continue
        run_id = str(row["run_id"])
        if scheduler.cancel(run_id, reason="p9_fixture_retry_cleanup"):
            settled.append(run_id)
    return settled


def _fixture_request(dispatcher: PMLoopDispatcher, *, replay_id: str, now: datetime) -> tuple[PMSystemStore, dict[str, Any]]:
    store = PMSystemStore(dispatcher.db_path, auto_migrate=False)
    registry, _ = dispatcher.load_verified_registry()
    task = registry.task(UPSTREAM_KEY)
    occurrence_key = f"{UPSTREAM_KEY}:p9-dependency-replay:{replay_id}"
    request = dispatcher._request(
        task,
        registry,
        now,
        now=now,
        trigger_kind="manual_replay",
        occurrence_key_override=occurrence_key,
    )
    request["owner"] = "pm-p9-dependency-replay"
    request["idempotency_key"] = f"p9-dependency-replay:{replay_id}"
    # Replay failure must be terminal. It validates blocked-upstream behavior,
    # not the normal weekly sync retry policy.
    request["retry"] = {"max_attempts": 0, "backoff": "PT0S"}
    request["payload"]["retry"] = dict(request["retry"])
    request["payload"]["replay_fixture"] = {
        "stage": "P9.2",
        "fixture": "fixed_local_upstream_completion",
        "external_calls": {"oneapi": 0, "openviking": 0},
    }
    return store, request


def _event_for_run(store: PMSystemStore, run_id: str) -> dict[str, Any]:
    with store.connect() as connection:
        row = connection.execute(
            "SELECT * FROM scheduled_dependency_events WHERE upstream_run_id=? ORDER BY created_at DESC,event_id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
    if row is None:
        raise RuntimeError("fixture_worker_did_not_append_dependency_event")
    return dict(row)


def _run_fixture_upstream(
    *,
    dispatcher: PMLoopDispatcher,
    artifact_root: Path,
    replay_id: str,
    returncode: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    now = _now()
    store, request = _fixture_request(dispatcher, replay_id=replay_id, now=now)
    accepted = store.accept_scheduled_occurrence(request)
    if accepted.get("deduplicated"):
        raise RuntimeError("replay_occurrence_deduplicated")

    def fixed_invoker(command: list[str], timeout: int, env: Mapping[str, str] | None = None):
        if not command or command[0] != "/bin/bash" or not str(command[-1]).endswith("weekly-sync-and-refresh.sh"):
            raise RuntimeError("unexpected_fixture_command")
        return subprocess.CompletedProcess(command, returncode, "p9 local fixture\n", "" if returncode == 0 else "p9 fixture failure\n")

    worker = PMSystemWorker(
        dispatcher.db_path,
        artifact_root=artifact_root,
        codex_root=CODEX_ROOT,
        max_slots=1,
        scheduled_invoker=fixed_invoker,
    )
    status = worker.run_once()
    expected = "completed" if returncode == 0 else "failed"
    if status != expected:
        raise RuntimeError(f"fixture_worker_status:{status}")
    event = _event_for_run(store, str(accepted["run_id"]))
    return {**accepted, "worker_status": status}, event


def _event_payload(event: Mapping[str, Any], *, event_key: str, source_manifest_hash: str | None = None) -> dict[str, Any]:
    return {
        "event_key": event_key,
        "dependent_schedule_key": str(event["dependent_schedule_key"]),
        "upstream_schedule_key": str(event["upstream_schedule_key"]),
        "upstream_occurrence_id": str(event["upstream_occurrence_id"]),
        "upstream_run_id": str(event["upstream_run_id"]),
        "upstream_completed_at": str(event["upstream_completed_at"]),
        "source_manifest_path": str(event["source_manifest_path"]),
        "source_manifest_hash": source_manifest_hash or str(event["source_manifest_hash"]),
        "handler_evidence_path": str(event["handler_evidence_path"]),
        "handler_evidence_hash": str(event["handler_evidence_hash"]),
        "planner_version": PLANNER_VERSION,
        "status": "pending",
    }


def _planner_failure_detail(store: PMSystemStore, artifact_root: Path) -> dict[str, Any]:
    """Return bounded persisted evidence when the controlled planner fails.

    The replay is an operator-facing proof harness.  A bare worker status loses
    the one diagnostic that distinguishes a handler/environment failure from a
    planner contract failure, while the artifacts still exist at this point.
    """
    with store.connect() as connection:
        row = connection.execute(
            "SELECT run_id,status,error,updated_at FROM runs "
            "WHERE schedule_key=? ORDER BY updated_at DESC,run_id DESC LIMIT 1",
            (DEPENDENT_KEY,),
        ).fetchone()
    if row is None:
        return {"run": None, "events": [], "handler": None, "output": None}
    run = dict(row)
    events = [
        {
            "event_type": event.get("event_type"),
            "payload": event.get("payload", event.get("payload_json")),
        }
        for event in store.list_events(str(run["run_id"]), limit=20)
    ]
    scheduled = artifact_root / str(run["run_id"]) / "scheduled"

    def read_text(path: Path) -> str | None:
        try:
            return path.read_text(encoding="utf-8")[-8000:]
        except OSError:
            return None

    return {
        "run": run,
        "events": events,
        "handler": read_text(scheduled / "handler.json"),
        "output": read_text(scheduled / "output.txt"),
    }


def run_replay(
    *,
    db_path: Path,
    registry_path: Path,
    runtime_registry_path: Path,
    canonical_registry_path: Path,
    lock_path: Path,
    artifact_root: Path,
    replay_id: str,
) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{7,80}", replay_id):
        raise ValueError("replay_id must be 8-81 ASCII alphanumeric, underscore, or hyphen characters")
    dispatcher = PMLoopDispatcher(
        db_path,
        registry_path=registry_path,
        runtime_registry_path=runtime_registry_path,
        canonical_registry_path=canonical_registry_path,
        lock_path=lock_path,
        scheduler_id="pm-p9-dependency-replay",
    )
    store = PMSystemStore(db_path, auto_migrate=False)
    settled_retries = _settle_prior_fixture_retries(store)
    preflight = _assert_preflight(store)
    before = _table_counts(store, PRODUCTION_TABLES)

    success_upstream, success_event = _run_fixture_upstream(
        dispatcher=dispatcher,
        artifact_root=artifact_root,
        replay_id=f"{replay_id}-success",
        returncode=0,
    )
    if success_event["status"] != "pending":
        raise RuntimeError(f"success_event_not_pending:{success_event['status']}")
    duplicate = store.append_scheduled_dependency_event(
        _event_payload(success_event, event_key=str(success_event["event_key"]))
    )
    if not duplicate.get("deduplicated"):
        raise RuntimeError("dependency_event_not_deduplicated")
    success_tick = dispatcher.tick(now=_now(), mode="manual_replay", dependency_only=True)
    consumed = store.get_scheduled_dependency_event(str(success_event["event_id"])) or {}
    if consumed.get("status") != "consumed" or int(success_tick["dependency"]["accepted"]) != 1:
        raise RuntimeError("successful_dependency_not_consumed")
    planner = PMSystemWorker(
        db_path,
        artifact_root=artifact_root,
        codex_root=CODEX_ROOT,
        max_slots=1,
    )
    planner_status = planner.run_once()
    if planner_status != "completed":
        detail = _planner_failure_detail(store, artifact_root)
        raise RuntimeError(
            f"disabled_planner_status:{planner_status}:"
            f"{json.dumps(detail, ensure_ascii=False, sort_keys=True)}"
        )
    with store.connect() as connection:
        plan_row = connection.execute(
            "SELECT plan_id,status,plan_path,plan_hash FROM concept_refresh_runs WHERE dependency_event_id=?",
            (success_event["event_id"],),
        ).fetchone()
    if plan_row is None or str(plan_row["status"]) != "planned_disabled":
        raise RuntimeError("disabled_planner_evidence_missing")

    second_tick = dispatcher.tick(now=_now(), mode="manual_replay", dependency_only=True)
    corrupt = store.append_scheduled_dependency_event(
        _event_payload(
            success_event,
            event_key=f"{success_event['event_key']}:corrupt:{replay_id}",
            source_manifest_hash="sha256:" + "0" * 64,
        )
    )
    planner_jobs_before = _table_counts(store, ("jobs",)).get("jobs", 0)
    corrupt_tick = dispatcher.tick(now=_now(), mode="manual_replay", dependency_only=True)
    corrupt_event = store.get_scheduled_dependency_event(str(corrupt["event_id"])) or {}
    planner_jobs_after = _table_counts(store, ("jobs",)).get("jobs", 0)
    if corrupt_event.get("status") != "blocked_by_upstream" or planner_jobs_after != planner_jobs_before:
        raise RuntimeError("corrupt_manifest_event_not_blocked")

    failed_upstream, failed_event = _run_fixture_upstream(
        dispatcher=dispatcher,
        artifact_root=artifact_root,
        replay_id=f"{replay_id}-failed",
        returncode=7,
    )
    if failed_event.get("status") != "blocked_by_upstream":
        raise RuntimeError("failed_upstream_event_not_blocked")
    failed_tick = dispatcher.tick(now=_now(), mode="manual_replay", dependency_only=True)
    after = _table_counts(store, PRODUCTION_TABLES)
    if after != before:
        raise RuntimeError(f"production_side_effect_detected:{before}->{after}")

    return {
        "schema_version": "pm-loop.p9-disabled-dependency-replay.v1",
        "status": "PASS",
        "replay_id": replay_id,
        "preflight": preflight,
        "settled_prior_fixture_retries": settled_retries,
        "success": {
            "upstream": success_upstream,
            "event_id": success_event["event_id"],
            "event_status": consumed.get("status"),
            "duplicate_event": duplicate,
            "scheduler": success_tick,
            "planner_status": planner_status,
            "plan": dict(plan_row),
        },
        "duplicate_tick": second_tick,
        "hash_mismatch": {
            "event_id": corrupt["event_id"],
            "event_status": corrupt_event.get("status"),
            "reason": corrupt_event.get("reason"),
            "scheduler": corrupt_tick,
            "downstream_jobs_before": planner_jobs_before,
            "downstream_jobs_after": planner_jobs_after,
        },
        "failed_upstream": {
            "upstream": failed_upstream,
            "event_id": failed_event["event_id"],
            "event_status": failed_event.get("status"),
            "reason": failed_event.get("reason"),
            "scheduler": failed_tick,
        },
        "production_table_counts": {"before": before, "after": after},
        "external_calls": {"oneapi": 0, "openviking": 0},
    }


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="run the bounded replay after operator approval")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--runtime-registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--canonical-registry", type=Path, default=DEFAULT_CANONICAL_REGISTRY)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--replay-id", required=True)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.apply:
        print(json.dumps({"status": "HOLD", "reason": "--apply is required; no database writes made"}, ensure_ascii=False))
        return 1
    try:
        result = run_replay(
            db_path=args.db_path.expanduser().resolve(),
            registry_path=args.registry.expanduser().resolve(),
            runtime_registry_path=args.runtime_registry.expanduser().resolve(),
            canonical_registry_path=args.canonical_registry.expanduser().resolve(),
            lock_path=args.lock_path.expanduser().resolve(),
            artifact_root=args.artifact_root.expanduser().resolve(),
            replay_id=str(args.replay_id),
        )
    except Exception as exc:
        result = {"schema_version": "pm-loop.p9-disabled-dependency-replay.v1", "status": "HOLD", "error": f"{type(exc).__name__}: {exc}"}
    _write_json(args.report.expanduser().resolve(), result)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
