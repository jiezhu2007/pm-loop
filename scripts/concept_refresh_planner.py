#!/usr/bin/env python3
"""Plan and enqueue bounded concept projections for one dependency event.

The fixed handler never compiles a ConceptVersion or publishes a Generation.
When Admission is ``disabled`` it remains observation-only.  Under a fresh
``canary`` or ``incremental`` Admission it may enqueue an existing, source-
closed concept page through the shared PM Outbox for asynchronous OpenViking
content projection.  Canary targets are always isolated and all projections
remain Candidates; no path in this module changes the concept registry.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from pm_loop_runtime import atomic_json_write
from pm_resource_dispatcher import PMResourceDispatcher
from pm_system_store import PMSystemStore
from concept_v11_schema_v2 import admission_is_live


PLAN_SCHEMA = "pm-loop.concept-refresh-plan.v2"
PLANNER_VERSION = "concept-refresh-planner.v2"
COVERAGE_REPORT_SCHEMA = "concept-v11.source-coverage-report.v1"
DEFAULT_COVERAGE_REPORT = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
CANARY_LIMIT = 2


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _not_expired(value: Any, *, now: datetime) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc) > now


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _raw_source_map_coverage(store: PMSystemStore, namespace_epoch: str) -> list[dict[str, Any]]:
    with store.connect() as connection:
        concept_rows = connection.execute(
            "SELECT DISTINCT concept_id FROM concept_versions WHERE namespace_epoch=? ORDER BY concept_id",
            (namespace_epoch,),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in concept_rows:
            concept_id = str(row[0])
            sources = connection.execute(
                "SELECT status,COUNT(*) FROM concept_source_map WHERE namespace_epoch=? AND concept_id=? GROUP BY status ORDER BY status",
                (namespace_epoch, concept_id),
            ).fetchall()
            source_count = sum(int(item[1]) for item in sources)
            statuses = {str(item[0]): int(item[1]) for item in sources}
            if source_count > 0 and set(statuses) == {"mapped"}:
                coverage, reason = "refreshable", "all_source_references_mapped"
            elif source_count == 0:
                coverage, reason = "needs_repair", "source_map_missing"
            else:
                coverage = "needs_repair"
                reason = "source_map_nonmapped:" + ",".join(
                    f"{key}={statuses[key]}" for key in sorted(statuses) if key != "mapped"
                )
            items.append(
                {"concept_id": concept_id, "coverage_status": coverage, "source_count": source_count, "reason": reason}
            )
    return items


def _coverage_report(
    *,
    path: Path,
    manifest_hash: str,
    expected_concept_ids: set[str],
) -> tuple[list[dict[str, Any]] | None, dict[str, Any]]:
    if not path.is_file():
        return None, {"status": "missing", "path": str(path), "reason": "source_coverage_report_missing"}
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_report_invalid"}
    if not isinstance(report, Mapping) or str(report.get("schema_version") or report.get("schema") or "") != COVERAGE_REPORT_SCHEMA:
        return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_report_schema_invalid"}
    if str(report.get("source_manifest_hash") or "") != manifest_hash:
        return None, {"status": "stale", "path": str(path), "reason": "source_coverage_manifest_hash_mismatch", "report_hash": report.get("report_hash")}
    rows = report.get("concepts")
    if not isinstance(rows, list):
        return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_concepts_missing", "report_hash": report.get("report_hash")}
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_concept_invalid", "report_hash": report.get("report_hash")}
        concept_id = str(row.get("concept_id") or "")
        coverage = str(row.get("coverage_status") or "")
        if concept_id not in expected_concept_ids or concept_id in seen or coverage not in {"refreshable", "substituted", "retired_with_evidence", "needs_repair"}:
            return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_catalog_invalid", "report_hash": report.get("report_hash")}
        seen.add(concept_id)
        references = row.get("references")
        current_refs = [
            {
                "source_uri": str(ref.get("source_uri") or ""),
                "evidence_set_hash": str(ref.get("evidence_set_hash") or ""),
                "disposition": str(ref.get("disposition") or ""),
                "source_map_status": str(ref.get("source_map_status") or ""),
            }
            for ref in references
            if isinstance(ref, Mapping)
            and str(ref.get("source_uri") or "")
            and str(ref.get("evidence_set_hash") or "").startswith("sha256:")
            and str(ref.get("disposition") or "") in {"mapped", "substituted"}
            and str(ref.get("source_map_status") or "") == "mapped"
        ] if isinstance(references, list) else []
        items.append(
            {
                "concept_id": concept_id,
                "concept": str(row.get("concept") or ""),
                "coverage_status": coverage,
                "source_count": int(row.get("reference_count") or 0),
                "current_source_refs": sorted(current_refs, key=lambda ref: ref["source_uri"]),
                "reason": "coverage_report:" + coverage,
            }
        )
    if seen != expected_concept_ids:
        return None, {"status": "invalid", "path": str(path), "reason": "source_coverage_catalog_incomplete", "report_hash": report.get("report_hash")}
    return sorted(items, key=lambda row: str(row["concept_id"])), {
        "status": str(report.get("status") or "unknown"),
        "path": str(path),
        "report_hash": str(report.get("report_hash") or ""),
        "closure_hash": str(report.get("closure_hash") or ""),
        "source_manifest_hash": str(report.get("source_manifest_hash") or ""),
        "gate": dict(report.get("gate") or {}) if isinstance(report.get("gate"), Mapping) else {},
    }


def _admission_and_coverage(
    store: PMSystemStore,
    *,
    manifest_hash: str,
    coverage_report_path: Path,
) -> tuple[str, str | None, list[dict[str, Any]], str | None, dict[str, Any]]:
    """Return the current admission state and conservative source coverage."""
    with store.connect() as connection:
        if not _table_exists(connection, "concept_admissions"):
            return "missing", None, [], "concept_admission_schema_missing", {"status": "unavailable"}
        admission = connection.execute(
            "SELECT namespace_epoch,admission_state FROM concept_admissions ORDER BY version DESC,updated_at DESC LIMIT 1"
        ).fetchone()
        if admission is None:
            return "missing", None, [], "concept_admission_missing", {"status": "unavailable"}
        namespace_epoch, admission_state = str(admission[0]), str(admission[1])
        if not _table_exists(connection, "concept_versions") or not _table_exists(connection, "concept_source_map"):
            return admission_state, namespace_epoch, [], "concept_source_schema_missing", {"status": "unavailable"}
    items = _raw_source_map_coverage(store, namespace_epoch)
    if not items:
        return admission_state, namespace_epoch, [], "concept_catalog_empty", {"status": "unavailable"}
    report_items, report_meta = _coverage_report(
        path=coverage_report_path,
        manifest_hash=manifest_hash,
        expected_concept_ids={str(item["concept_id"]) for item in items},
    )
    if report_items is not None:
        return admission_state, namespace_epoch, report_items, None, report_meta
    return admission_state, namespace_epoch, items, str(report_meta.get("reason") or "source_coverage_unavailable"), report_meta


def _admission_runtime(store: PMSystemStore, namespace_epoch: str | None) -> dict[str, Any]:
    """Read the Admission/profile state used only to bound a plan.

    The Gateway repeats these checks transactionally while enqueuing.  Keeping
    this read separate lets the plan explain a rejected admission without
    turning a stale pre-check into authorization.
    """
    if not namespace_epoch:
        return {"admission": {}, "profile": {}}
    with store.connect() as connection:
        admission = connection.execute(
            "SELECT * FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)
        ).fetchone()
        # A disabled observation plan must remain usable against the v11
        # recovery substrate, where the optional profile table is absent.
        # Enabled incremental planning sees an empty profile and fails closed
        # at its pause/capacity gate below.
        profile = None
        if _table_exists(connection, "concept_profile_admissions"):
            profile = connection.execute(
                "SELECT * FROM concept_profile_admissions "
                "WHERE workload='concept-semantic' AND profile='pm-semantic' AND namespace_epoch=?",
                (namespace_epoch,),
            ).fetchone()
    return {
        "admission": dict(admission) if admission is not None else {},
        "profile": dict(profile) if profile is not None else {},
    }


def _page_path(concept_root: Path, concept: str) -> Path | None:
    if not concept:
        return None
    pages_root = (concept_root / "state" / "pages").resolve()
    path = (pages_root / f"{concept}.md").resolve()
    if pages_root not in path.parents or not path.is_file():
        return None
    return path


def _eligible_for_projection(item: Mapping[str, Any], concept_root: Path) -> tuple[bool, str, Path | None]:
    coverage = str(item.get("coverage_status") or "")
    if coverage not in {"refreshable", "substituted"}:
        return False, f"coverage_{coverage or 'unknown'}", None
    refs = item.get("current_source_refs")
    if not isinstance(refs, list) or not refs:
        return False, "current_source_evidence_missing", None
    path = _page_path(concept_root, str(item.get("concept") or ""))
    if path is None:
        return False, "concept_page_missing", None
    return True, "current_source_and_page_verified", path


def _projection_target(*, admission: str, namespace_epoch: str, plan_id: str, concept_id: str) -> str:
    if admission == "canary":
        root = f"viking://resources/concepts/__canary__/{namespace_epoch}/{plan_id}"
    else:
        root = f"viking://resources/concepts/candidates/{namespace_epoch}/{plan_id}"
    return f"{root}/{concept_id}.md"


def _execution_plan(
    *,
    admission: str,
    runtime: Mapping[str, Any],
    coverage_input: Mapping[str, Any],
    coverage_error: str | None,
    coverage: list[dict[str, Any]],
    concept_root: Path,
    plan_id: str,
    now: datetime,
) -> tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    """Select bounded projection candidates and durable per-item decisions."""
    records = [dict(item) for item in coverage]
    if admission == "disabled":
        return "planned_disabled", "admission_disabled_no_production_side_effects", [
            {**item, "decision": "observe_only", "reason": "admission_disabled_no_production_side_effects"}
            for item in records
        ], []
    if coverage_error:
        return "blocked", coverage_error, [
            {**item, "decision": "blocked", "reason": coverage_error} for item in records
        ], []
    if admission not in {"canary", "incremental"}:
        return "blocked", f"admission_not_supported:{admission or 'missing'}", [
            {**item, "decision": "blocked", "reason": f"admission_not_supported:{admission or 'missing'}"}
            for item in records
        ], []
    if str(coverage_input.get("status") or "") != "PASS" or coverage_input.get("gate", {}).get("p3_closed") is not True:
        return "blocked", "source_coverage_not_closed", [
            {**item, "decision": "blocked", "reason": "source_coverage_not_closed"} for item in records
        ], []
    admission_row = runtime.get("admission") if isinstance(runtime.get("admission"), Mapping) else {}
    if not admission_is_live(admission_row, at=now.isoformat(timespec="seconds").replace("+00:00", "Z")):
        renewal_policy = str(admission_row.get("renewal_policy") or "snapshot_ttl")
        rejection = (
            "admission_snapshot_expired"
            if renewal_policy == "snapshot_ttl"
            else "admission_continuous_policy_invalid"
        )
        return "blocked", rejection, [
            {**item, "decision": "blocked", "reason": rejection} for item in records
        ], []

    eligible: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for item in sorted(records, key=lambda row: str(row.get("concept_id") or "")):
        ready, reason, page = _eligible_for_projection(item, concept_root)
        if ready and page is not None:
            eligible.append({**item, "page_path": str(page), "page_hash": _file_hash(page)})
        elif str(item.get("coverage_status") or "") == "retired_with_evidence":
            decisions.append({**item, "decision": "retired_excluded", "reason": "retired_with_evidence_never_executed"})
        else:
            decisions.append({**item, "decision": "blocked", "reason": reason})

    if admission == "canary":
        allowance = CANARY_LIMIT
        status = "planned_canary"
        selected_decision = "canary_projection"
    else:
        profile = runtime.get("profile") if isinstance(runtime.get("profile"), Mapping) else {}
        pending = int(profile.get("pending_count") or 0)
        soft = int(profile.get("pending_soft_limit") or 0)
        hard = int(profile.get("outbox_hard_cap") or 0)
        if str(profile.get("pause_fence") or "") != "open":
            return "blocked", "profile_pause_fence", [
                {**item, "decision": "blocked", "reason": "profile_pause_fence"} for item in records
            ], []
        if profile.get("throttle_until") and _not_expired(profile.get("throttle_until"), now=now):
            return "blocked", "profile_throttled", [
                {**item, "decision": "blocked", "reason": "profile_throttled"} for item in records
            ], []
        allowance = min(max(0, soft - pending), max(0, hard - pending))
        if allowance <= 0:
            return "blocked", "profile_capacity_exhausted", [
                {**item, "decision": "blocked", "reason": "profile_capacity_exhausted"} for item in records
            ], []
        status = "planned_incremental"
        selected_decision = "incremental_projection"

    selected = eligible[:allowance]
    selected_ids = {str(item["concept_id"]) for item in selected}
    for item in eligible:
        if str(item["concept_id"]) in selected_ids:
            decisions.append({**item, "decision": selected_decision, "reason": "projection_queued_pending_outbox"})
        else:
            decisions.append({**item, "decision": "deferred", "reason": "bounded_batch_capacity"})
    return status, "candidate_projection_only_no_active_publish", sorted(decisions, key=lambda row: str(row["concept_id"])), selected


def _artifact_dir() -> Path:
    raw_envelope = str(os.environ.get("PM_SCHEDULE_RUN_ENVELOPE") or "").strip()
    if not raw_envelope:
        raise ValueError("PM_SCHEDULE_RUN_ENVELOPE is required")
    envelope = Path(raw_envelope).expanduser()
    return envelope.resolve().parent


def build_plan(
    *,
    db_path: Path,
    event_id: str,
    artifact_dir: Path,
    coverage_report_path: Path = DEFAULT_COVERAGE_REPORT,
    concept_root: Path = DEFAULT_CONCEPT_ROOT,
    dispatcher: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    store = PMSystemStore(db_path, auto_migrate=False)
    event = store.get_scheduled_dependency_event(event_id)
    if event is None:
        raise ValueError("dependency_event_not_found")
    if str(event.get("status") or "") != "consumed":
        raise ValueError("dependency_event_not_consumed")
    if str(event.get("dependent_schedule_key") or "") != "concept-refresh-planner":
        raise ValueError("dependency_event_wrong_target")
    if str(event.get("planner_version") or "") != PLANNER_VERSION:
        raise ValueError("dependency_event_planner_version_mismatch")
    manifest_path = Path(str(event.get("source_manifest_path") or "")).expanduser().resolve()
    if not manifest_path.is_file():
        raise ValueError("source_manifest_missing")
    if _file_hash(manifest_path) != str(event.get("source_manifest_hash") or ""):
        raise ValueError("source_manifest_hash_mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or str(manifest.get("schema_version") or "") != "concept-source-manifest.v1":
        raise ValueError("source_manifest_invalid")

    admission, namespace_epoch, coverage, coverage_error, coverage_input = _admission_and_coverage(
        store,
        manifest_hash=str(event["source_manifest_hash"]),
        coverage_report_path=coverage_report_path.expanduser().resolve(),
    )
    now = now or _now()
    plan_id = "concept-refresh-plan-" + hashlib.sha256(str(event_id).encode("utf-8")).hexdigest()[:24]
    concept_root = concept_root.expanduser().resolve()
    runtime = _admission_runtime(store, namespace_epoch)
    status, reason, items, selected = _execution_plan(
        admission=admission,
        runtime=runtime,
        coverage_input=coverage_input,
        coverage_error=coverage_error,
        coverage=coverage,
        concept_root=concept_root,
        plan_id=plan_id,
        now=now,
    )
    batch_key = _canonical_hash({
        "plan_id": plan_id,
        "admission": admission,
        "source_manifest_hash": str(event["source_manifest_hash"]),
        "coverage_report_hash": str(coverage_input.get("report_hash") or ""),
    })
    selected_by_id = {str(item["concept_id"]): item for item in selected}
    queue_failures: list[str] = []
    if status in {"planned_canary", "planned_incremental"}:
        active_dispatcher = dispatcher or PMResourceDispatcher(
            store,
            artifact_root=(artifact_dir / "resource-outbox").resolve(),
        )
        updated_items: list[dict[str, Any]] = []
        for item in items:
            concept_id = str(item["concept_id"])
            selected_item = selected_by_id.get(concept_id)
            if selected_item is None:
                updated_items.append(item)
                continue
            target_uri = _projection_target(
                admission=admission,
                namespace_epoch=str(namespace_epoch or ""),
                plan_id=plan_id,
                concept_id=concept_id,
            )
            try:
                accepted = active_dispatcher.enqueue_concept_file(
                    path=Path(str(selected_item["page_path"])),
                    target_uri=target_uri,
                    processing_mode="vectors_only",
                    namespace_epoch=str(namespace_epoch or ""),
                    owner=f"concept-refresh-planner:{plan_id}",
                    wait=False,
                    strict=False,
                )
                outbox_id = str(accepted.get("outbox_id") or "")
                idempotency_key = str(accepted.get("idempotency_key") or "")
                if not outbox_id or not idempotency_key:
                    raise RuntimeError("outbox_acceptance_incomplete")
                updated_items.append({
                    **item,
                    "target_uri": target_uri,
                    "idempotency_key": idempotency_key,
                    "outbox_item_id": outbox_id,
                    "reason": "outbox_accepted_candidate_projection",
                })
            except Exception as exc:
                queue_failures.append(f"{concept_id}:{type(exc).__name__}")
                updated_items.append({
                    **item,
                    "decision": "blocked",
                    "target_uri": target_uri,
                    "reason": f"outbox_enqueue_failed:{type(exc).__name__}",
                })
        items = updated_items
        if queue_failures:
            status = "blocked"
            reason = "outbox_enqueue_failed:" + ",".join(sorted(queue_failures))

    evidence_hash = str(coverage_input.get("report_hash") or event["source_manifest_hash"])
    items = [
        {
            **item,
            "evidence_hash": evidence_hash,
            "source_manifest_hash": str(event["source_manifest_hash"]),
            "coverage_report_hash": str(coverage_input.get("report_hash") or ""),
            "batch_key": batch_key,
            "execution_scope": "candidate_projection_only",
        }
        for item in items
    ]
    body: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "plan_id": plan_id,
        "dependency_event_id": event_id,
        "upstream_run_id": str(event["upstream_run_id"]),
        "upstream_completed_at": str(event["upstream_completed_at"]),
        "namespace_epoch": namespace_epoch,
        "admission_state": admission,
        "planner_version": PLANNER_VERSION,
        "source_manifest_path": str(manifest_path),
        "source_manifest_hash": str(event["source_manifest_hash"]),
        "source_manifest_metrics": dict(manifest.get("metrics") or {}),
        "source_coverage_input": coverage_input,
        "admission_runtime": runtime,
        "batch_key": batch_key,
        "status": status,
        "reason": reason,
        "items": items,
        "publication": {
            "state": "not_attempted",
            "reason": "candidate_projection_only_model_compilation_and_active_publish_are_separate_gates",
            "concept_versions": 0,
            "hot_projection": 0,
            "generations": 0,
        },
        "side_effects": {
            "concept_versions": 0,
            "hot_projection": 0,
            "generations": 0,
            "outbox": sum(1 for item in items if item.get("outbox_item_id")),
            "semantic_tasks": 0,
            "provider_calls": 0,
            "openviking_calls": 0,
        },
    }
    plan_hash = _canonical_hash(body)
    plan = {**body, "plan_hash": plan_hash}
    plan_path = artifact_dir / "concept-refresh-plan.v2.json"
    atomic_json_write(plan_path, plan)
    record = store.record_concept_refresh_plan(
        {
            "plan_id": plan_id,
            "dependency_event_id": event_id,
            "upstream_run_id": str(event["upstream_run_id"]),
            "namespace_epoch": namespace_epoch,
            "admission_state": admission,
            "planner_version": PLANNER_VERSION,
            "source_manifest_path": str(manifest_path),
            "source_manifest_hash": str(event["source_manifest_hash"]),
            "plan_path": str(plan_path),
            "plan_hash": plan_hash,
            "status": status,
            "reason": reason,
        },
        items=items,
    )
    return {
        "status": status,
        "plan_id": plan_id,
        "plan_path": str(plan_path),
        "plan_hash": plan_hash,
        "item_count": len(items),
        "record": record,
        "side_effects": plan["side_effects"],
    }


def main(argv: Iterable[str] | None = None) -> int:
    del argv  # The fixed handler accepts only Scheduler-owned environment fields.
    event_id = str(os.environ.get("PM_CONCEPT_DEPENDENCY_EVENT_ID") or "").strip()
    db_path = Path(str(os.environ.get("PM_SCHEDULE_DB_PATH") or "")).expanduser()
    if not event_id or not str(db_path):
        raise ValueError("controlled dependency event and DB path are required")
    result = build_plan(db_path=db_path.resolve(), event_id=event_id, artifact_dir=_artifact_dir())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
