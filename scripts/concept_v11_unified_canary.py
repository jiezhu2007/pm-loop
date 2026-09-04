#!/usr/bin/env python3
"""Execute one bounded, vectors-only concept Canary through PM Scheduler.

This is a controlled recovery fixture, not a second scheduling path.  It
creates an auditable manual-replay upstream witness, lets the real Scheduler
consume its dependency event, and runs the fixed planner through the normal
Worker.  The only allowed resource side effect is at most two isolated
``viking://resources/concepts/__canary__/...`` projections.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from concept_v11_canary_readback import run as read_back
from pm_loop_scheduler import (
    DEFAULT_CANONICAL_REGISTRY,
    DEFAULT_DB_PATH,
    DEFAULT_LOCK_PATH,
    DEFAULT_RUNTIME_REGISTRY,
    PMLoopDispatcher,
)
from pm_system_store import PMSystemStore
from pm_system_worker import PMSystemWorker


CODEX_ROOT = Path.home() / ".codex"
DEFAULT_ARTIFACT_ROOT = CODEX_ROOT / "pm-loop" / "runs" / "concept-v11" / "unified-canary"
DEFAULT_COVERAGE = CODEX_ROOT / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_CONTENT_PREFLIGHT = CODEX_ROOT / "pm-loop" / "state" / "concept-v11" / "content-source-preflight-current.json"
DEFAULT_SOURCE_MANIFEST = CODEX_ROOT / "pm-loop" / "runs" / "concept-v11" / "p3-watermark-source-20260903" / "source-manifest.json"
UPSTREAM_KEY = "weekly-sync-and-refresh"
DEPENDENT_KEY = "concept-refresh-planner"
PLANNER_VERSION = "concept-refresh-planner.v2"
CANARY_LIMIT = 2
CONCEPT_FACT_TABLES = ("concept_versions", "concept_hot_projection", "generations")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"invalid JSON object: {path}")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    temporary.write_text(json.dumps(dict(value), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _counts(store: PMSystemStore, names: Iterable[str]) -> dict[str, int]:
    with store.connect() as connection:
        return {name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in names}


def _active_counts(store: PMSystemStore) -> dict[str, int]:
    with store.connect() as connection:
        return {
            "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status IN ('pending','in_flight','retry_wait')").fetchone()[0]),
            "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status IN ('queued','running','retry_wait','in_flight','accepted','processing')").fetchone()[0]),
        }


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise RuntimeError(reason)


def _wait_for(predicate: Any, *, timeout_seconds: float, reason: str) -> Any:
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    value = predicate()
    while value is None and time.monotonic() < deadline:
        time.sleep(0.5)
        value = predicate()
    if value is None:
        raise RuntimeError(reason)
    return value


def preflight(
    *,
    store: PMSystemStore,
    coverage_path: Path,
    content_preflight_path: Path,
    source_manifest_path: Path,
    now: datetime,
) -> dict[str, Any]:
    coverage = _read_json(coverage_path)
    content = _read_json(content_preflight_path)
    manifest = _read_json(source_manifest_path)
    manifest_hash = _file_hash(source_manifest_path)
    _require(str(coverage.get("schema") or "") == "concept-v11.source-coverage-report.v1", "coverage_schema_invalid")
    _require(str(coverage.get("status") or "") == "PASS", "coverage_not_pass")
    _require(str(manifest.get("schema_version") or "") == "concept-source-manifest.v1", "source_manifest_schema_invalid")
    _require((coverage.get("gate") or {}).get("p3_closed") is True, "coverage_gate_not_closed")
    _require(int(coverage.get("concept_count") or 0) == 45, "coverage_concept_count_invalid")
    _require(int((coverage.get("concept_status_counts") or {}).get("needs_repair") or 0) == 0, "coverage_needs_repair")
    _require(str(coverage.get("source_manifest_hash") or "") == manifest_hash, "coverage_source_manifest_hash_mismatch")
    _require(str(content.get("status") or "") == "PASS", "content_source_preflight_not_pass")
    _require(str(content.get("coverage_report_hash") or "") == str(coverage.get("report_hash") or ""), "content_source_coverage_hash_mismatch")
    _require(str(content.get("coverage_source_manifest_hash") or "") == manifest_hash, "content_source_manifest_hash_mismatch")
    _require(int((content.get("summary") or {}).get("ready") or 0) >= 44, "content_source_ready_count_invalid")

    with store.connect() as connection:
        admission = connection.execute(
            "SELECT namespace_epoch,admission_state,expires_at,version FROM concept_admissions ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        model_calls = int(connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0])
    _require(admission is not None, "admission_missing")
    admission_value = dict(admission)
    _require(str(admission_value.get("admission_state") or "") == "canary", "admission_not_canary")
    try:
        expires_at = datetime.fromisoformat(str(admission_value["expires_at"]).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise RuntimeError("admission_expiry_invalid") from None
    _require(expires_at.astimezone(timezone.utc) > now + timedelta(seconds=120), "admission_ttl_too_short")
    active = _active_counts(store)
    _require(not any(active.values()), f"active_work_present:{active}")
    return {
        "coverage_report_hash": coverage.get("report_hash"),
        "source_manifest_hash": manifest_hash,
        "content_preflight_hash": _file_hash(content_preflight_path),
        "admission": admission_value,
        "active": active,
        "model_calls_before": model_calls,
    }


def _run_upstream_witness(
    *,
    dispatcher: PMLoopDispatcher,
    artifact_root: Path,
    canary_id: str,
    source_manifest_path: Path,
    source_manifest_hash: str,
    now: datetime,
) -> dict[str, Any]:
    store = PMSystemStore(dispatcher.db_path, auto_migrate=False)
    registry, _ = dispatcher.load_verified_registry()
    task = registry.task(UPSTREAM_KEY)
    request = dispatcher._request(
        task,
        registry,
        now,
        now=now,
        trigger_kind="manual_replay",
        occurrence_key_override=f"{UPSTREAM_KEY}:unified-canary:{canary_id}",
    )
    request["owner"] = "concept-v11-unified-canary"
    request["idempotency_key"] = f"concept-v11-unified-canary:{canary_id}"
    request["retry"] = {"max_attempts": 0, "backoff": "PT0S"}
    request["payload"]["retry"] = dict(request["retry"])
    request["payload"]["replay_fixture"] = {
        "stage": "P7",
        "fixture": "bounded_vectors_only_unified_canary",
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_hash": source_manifest_hash,
        "external_calls": {"oneapi": 0, "openviking": 0},
    }
    accepted = store.accept_scheduled_occurrence(request)
    _require(not accepted.get("deduplicated"), "canary_witness_occurrence_deduplicated")

    def fixed_invoker(command: list[str], timeout: int, env: Mapping[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        _require(bool(command) and command[0] == "/bin/bash" and str(command[-1]).endswith("weekly-sync-and-refresh.sh"), "unexpected_witness_command")
        return subprocess.CompletedProcess(command, 0, "unified canary upstream witness\n", "")

    worker = PMSystemWorker(
        dispatcher.db_path,
        artifact_root=artifact_root,
        codex_root=CODEX_ROOT,
        max_slots=1,
        scheduled_invoker=fixed_invoker,
    )
    _require(worker.run_once() == "completed", "upstream_witness_worker_failed")
    run_id = str(accepted["run_id"])
    with store.connect() as connection:
        generated = connection.execute(
            "SELECT * FROM scheduled_dependency_events WHERE upstream_run_id=? ORDER BY created_at DESC,event_id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
    _require(generated is not None, "upstream_witness_event_missing")
    generated_event = dict(generated)
    # The Worker correctly built the current live manifest for this synthetic
    # fixture. It is not the coverage-bound immutable manifest, so retain it
    # as an explicitly blocked audit row rather than letting it run a planner.
    if str(generated_event.get("status") or "") == "pending":
        store.mark_scheduled_dependency_event_blocked(
            str(generated_event["event_id"]),
            reason="fixture_live_manifest_not_coverage_bound",
            outcome={"fixture": "bounded_vectors_only_unified_canary", "source_manifest_hash": source_manifest_hash},
        )

    handler_path = artifact_root / run_id / "scheduled" / "handler.json"
    _require(handler_path.is_file(), "upstream_witness_handler_evidence_missing")
    witness_path = artifact_root / run_id / "canary-source-witness.v1.json"
    _write_json(witness_path, {
        "schema_version": "concept-v11.unified-canary-source-witness.v1",
        "canary_id": canary_id,
        "upstream_run_id": run_id,
        "fixture": "bounded_vectors_only_unified_canary",
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_hash": source_manifest_hash,
        "worker_handler_path": str(handler_path),
        "worker_handler_hash": _file_hash(handler_path),
        "external_calls": {"oneapi": 0, "openviking": 0},
    })
    event = store.append_scheduled_dependency_event({
        "event_key": f"{DEPENDENT_KEY}:unified-canary:{canary_id}:{source_manifest_hash}:{PLANNER_VERSION}",
        "dependent_schedule_key": DEPENDENT_KEY,
        "upstream_schedule_key": UPSTREAM_KEY,
        "upstream_occurrence_id": str(accepted["occurrence_id"]),
        "upstream_run_id": run_id,
        "upstream_completed_at": _iso(_now()),
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_hash": source_manifest_hash,
        "handler_evidence_path": str(witness_path),
        "handler_evidence_hash": _file_hash(witness_path),
        "planner_version": PLANNER_VERSION,
        "status": "pending",
    })
    _require(not event.get("deduplicated"), "canary_dependency_event_deduplicated")
    return {"upstream": accepted, "generated_event": generated_event, "event": event, "witness_path": str(witness_path)}


def run_canary(
    *,
    db_path: Path,
    registry_path: Path,
    runtime_registry_path: Path,
    canonical_registry_path: Path,
    lock_path: Path,
    artifact_root: Path,
    coverage_path: Path,
    content_preflight_path: Path,
    source_manifest_path: Path,
    canary_id: str,
) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{7,80}", canary_id):
        raise ValueError("canary_id must be 8-81 ASCII alphanumeric, underscore, or hyphen characters")
    store = PMSystemStore(db_path, auto_migrate=False)
    now = _now()
    check = preflight(
        store=store,
        coverage_path=coverage_path,
        content_preflight_path=content_preflight_path,
        source_manifest_path=source_manifest_path,
        now=now,
    )
    before_facts = _counts(store, CONCEPT_FACT_TABLES)
    dispatcher = PMLoopDispatcher(
        db_path,
        registry_path=registry_path,
        runtime_registry_path=runtime_registry_path,
        canonical_registry_path=canonical_registry_path,
        lock_path=lock_path,
        scheduler_id="concept-v11-unified-canary",
    )
    witness = _run_upstream_witness(
        dispatcher=dispatcher,
        artifact_root=artifact_root,
        canary_id=canary_id,
        source_manifest_path=source_manifest_path,
        source_manifest_hash=str(check["source_manifest_hash"]),
        now=now,
    )
    event = witness["event"]
    resident_scheduler = False
    try:
        tick = dispatcher.tick(now=_now(), mode="manual_replay", dependency_only=True)
    except RuntimeError as exc:
        if str(exc) != "duplicate_scheduler":
            raise
        resident_scheduler = True
        tick = {"status": "delegated_to_resident_scheduler", "reason": "duplicate_scheduler"}

    def consumed_event() -> dict[str, Any] | None:
        candidate = store.get_scheduled_dependency_event(str(event["event_id"])) or {}
        return candidate if str(candidate.get("status") or "") == "consumed" else None

    stored_event = _wait_for(consumed_event, timeout_seconds=60, reason="canary_dependency_not_consumed")
    _require(str(stored_event.get("status") or "") == "consumed", "canary_dependency_not_consumed")
    if not resident_scheduler:
        _require(int((tick.get("dependency") or {}).get("accepted") or 0) == 1, "canary_dependency_not_accepted")

    def plan_row() -> dict[str, Any] | None:
        with store.connect() as connection:
            row = connection.execute(
            "SELECT plan_id,status,plan_path,plan_hash FROM concept_refresh_runs WHERE dependency_event_id=?",
            (event["event_id"],),
            ).fetchone()
        if row is None or str(row["status"] or "") != "planned_canary":
            return None
        return dict(row)

    if not resident_scheduler:
        planner_worker = PMSystemWorker(db_path, artifact_root=artifact_root, codex_root=CODEX_ROOT, max_slots=1)
        _require(planner_worker.run_once() == "completed", "canary_planner_worker_failed")
    plan = _wait_for(plan_row, timeout_seconds=60, reason="canary_plan_missing_or_not_planned")
    plan_data = _read_json(Path(str(plan["plan_path"])))
    selected = [item for item in plan_data.get("items") or [] if item.get("outbox_item_id")]
    _require(len(selected) == CANARY_LIMIT, "canary_selection_count_invalid")
    outbox_ids = [str(item["outbox_item_id"]) for item in selected]
    _require(all("viking://resources/concepts/__canary__/" in str(item.get("target_uri") or "") for item in selected), "canary_target_uri_invalid")

    if not resident_scheduler:
        dispatch_worker = PMSystemWorker(db_path, artifact_root=artifact_root, codex_root=CODEX_ROOT, max_slots=1)
        _require(dispatch_worker.run_once() is None, "canary_dispatch_unexpected_job")

    def completed_outbox() -> list[dict[str, Any]] | None:
        with store.connect() as connection:
            placeholders = ",".join("?" for _ in outbox_ids)
            rows = [dict(row) for row in connection.execute(
                f"SELECT outbox_id,kind,resource_id,processing_mode,status,payload_json FROM outbox_items WHERE outbox_id IN ({placeholders}) ORDER BY outbox_id",
                tuple(outbox_ids),
            ).fetchall()]
        if len(rows) != CANARY_LIMIT or not all(str(row.get("status") or "") == "completed" for row in rows):
            return None
        return rows

    outbox_rows = _wait_for(completed_outbox, timeout_seconds=90, reason="canary_outbox_not_completed")
    readbacks = [read_back(db_path, outbox_id, attempts=3, interval_seconds=1.0) for outbox_id in outbox_ids]
    _require(all(item.get("status") == "PASS" for item in readbacks), "canary_content_readback_failed")
    with store.connect() as connection:
        model_calls_after = int(connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0])
    _require(len(outbox_rows) == CANARY_LIMIT, "canary_outbox_rows_missing")
    _require(all(row["kind"] == "concept" and row["processing_mode"] == "vectors_only" for row in outbox_rows), "canary_processing_mode_invalid")
    _require(all(str(row["status"] or "") == "completed" for row in outbox_rows), "canary_outbox_not_completed")
    _require(all("viking://resources/concepts/__canary__/" in str(row["resource_id"] or "") for row in outbox_rows), "canary_outbox_target_invalid")
    _require(model_calls_after == int(check["model_calls_before"]), "canary_model_call_detected")
    after_facts = _counts(store, CONCEPT_FACT_TABLES)
    _require(after_facts == before_facts, f"concept_fact_table_changed:{before_facts}->{after_facts}")
    active = _active_counts(store)
    _require(not any(active.values()), f"canary_active_work_remaining:{active}")
    return {
        "schema_version": "concept-v11.unified-canary.v1",
        "status": "PASS",
        "canary_id": canary_id,
        "preflight": check,
        "witness": witness,
        "scheduler": tick,
        "plan": dict(plan),
        "outbox_ids": outbox_ids,
        "outbox": outbox_rows,
        "readbacks": readbacks,
        "concept_fact_tables": {"before": before_facts, "after": after_facts},
        "model_calls": {"before": check["model_calls_before"], "after": model_calls_after},
        "active_after": active,
        "external_calls": {"oneapi": 0, "openviking": CANARY_LIMIT},
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="execute the bounded local Canary")
    parser.add_argument("--canary-id", required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--runtime-registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--canonical-registry", type=Path, default=DEFAULT_CANONICAL_REGISTRY)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--content-source-preflight", type=Path, default=DEFAULT_CONTENT_PREFLIGHT)
    parser.add_argument("--source-manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.apply:
        result: Mapping[str, Any] = {"schema_version": "concept-v11.unified-canary.v1", "status": "HOLD", "reason": "--apply is required; no database writes made"}
    else:
        try:
            result = run_canary(
                db_path=args.db_path.expanduser().resolve(),
                registry_path=args.registry.expanduser().resolve(),
                runtime_registry_path=args.runtime_registry.expanduser().resolve(),
                canonical_registry_path=args.canonical_registry.expanduser().resolve(),
                lock_path=args.lock_path.expanduser().resolve(),
                artifact_root=args.artifact_root.expanduser().resolve(),
                coverage_path=args.coverage.expanduser().resolve(),
                content_preflight_path=args.content_source_preflight.expanduser().resolve(),
                source_manifest_path=args.source_manifest.expanduser().resolve(),
                canary_id=str(args.canary_id),
            )
        except Exception as exc:
            result = {"schema_version": "concept-v11.unified-canary.v1", "status": "HOLD", "error": f"{type(exc).__name__}: {exc}"}
    _write_json(args.report.expanduser().resolve(), result)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
