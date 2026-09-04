#!/usr/bin/env python3
"""Run a bounded production-path concept projection canary.

The normal invocation is a read-only dry run.  ``--apply`` is an explicit
boundary for one controlled canary: it requires a live ``canary`` admission,
fresh capability evidence, a healthy marker, and an authorization id.  The
runner uses the shared PM Outbox/Writer path, targets only an isolated
``__canary__`` namespace, never writes Concept Registry generations, and
never waits on a synchronous OpenViking request.  A semantic task may be
observed to a terminal state, but the local content read-back remains an
independent proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_admission import backup_database  # noqa: E402
from pm_resource_dispatcher import OpenVikingTransport, PMResourceDispatcher  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


SCHEMA = "concept-v11.production-canary.v1"
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
DEFAULT_COVERAGE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_HEALTH = Path.home() / ".codex" / "skills" / "system-health-check" / "state" / "latest.json"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11" / "canary"
DEFAULT_NAMESPACE = "v45-r2-20260830"
DEFAULT_RUNTIME_EPOCH = "pm-loop-scheduler-v11-dependency"
DEFAULT_CONCEPTS = ("DataAgent", "文件管理")
MAX_CONCEPTS = 2
PROCESSING_MODE = "semantic_and_vectors"
POLICY_VERSION = "concept-v11-oneapi-auto-v1"
TERMINAL_OUTBOX = {"completed", "failed", "dead_letter", "quarantine"}
TERMINAL_SEMANTIC = {"completed", "failed", "dead_letter", "quarantine"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_time(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value).strip()).strip("-.")
    return slug or "concept"


def _active_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    status_sets = {
        "jobs": ("queued", "running", "processing", "active", "retry_wait"),
        "runs": ("queued", "running", "processing", "active", "retry_wait"),
        "outbox_items": ("pending", "in_flight", "dispatching", "processing", "active", "retry_wait"),
        "semantic_tasks": ("queued", "in_flight", "accepted", "processing", "active", "retry_wait"),
    }
    result: Dict[str, int] = {}
    for table, statuses in status_sets.items():
        marks = ",".join("?" for _ in statuses)
        result[table] = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE status IN ({marks})", statuses
            ).fetchone()[0]
        )
    result["slots"] = int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status <> 'free'").fetchone()[0])
    result["tokens"] = int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0])
    result["migration_leases"] = int(connection.execute("SELECT COUNT(*) FROM migration_leases WHERE state='active'").fetchone()[0])
    result["dispatch_leases"] = int(connection.execute("SELECT COUNT(*) FROM outbox_dispatch_leases").fetchone()[0])
    result["probe_leases"] = int(connection.execute("SELECT COUNT(*) FROM provider_probe_leases").fetchone()[0])
    return result


def _table_digest(connection: sqlite3.Connection, table: str) -> str:
    rows = connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
    return _sha256_bytes(_canonical([list(row) for row in rows]).encode("utf-8"))


def _registry_snapshot(connection: sqlite3.Connection) -> Dict[str, Any]:
    tables = ("concept_versions", "concept_publish_ledger", "concept_hot_projection")
    return {table: {"count": int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]), "digest": _table_digest(connection, table)} for table in tables}


def _state_snapshot(db_path: Path, namespace_epoch: str) -> Dict[str, Any]:
    uri = f"file:{db_path.resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=3) as connection:
        connection.row_factory = sqlite3.Row
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        admission = connection.execute(
            "SELECT * FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)
        ).fetchone()
        profile = connection.execute(
            "SELECT * FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile='pm-semantic' AND namespace_epoch=?",
            (namespace_epoch,),
        ).fetchone()
        policies = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM concept_model_policies WHERE status='active' ORDER BY policy_version"
            ).fetchall()
        ]
        probes = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM concept_capability_probes WHERE namespace_epoch=? AND profile='pm-semantic' ORDER BY observed_at DESC",
                (namespace_epoch,),
            ).fetchall()
        ]
        active_generation = [
            dict(row)
            for row in connection.execute(
                "SELECT generation_id,generation_hash,status,active_at FROM generations "
                "WHERE domain='concepts' AND status='active' ORDER BY active_at DESC,generation_id"
            ).fetchall()
        ]
        return {
            "integrity": integrity,
            "freeze": dict(connection.execute("SELECT * FROM migration_freeze ORDER BY rowid DESC LIMIT 1").fetchone() or {}),
            "admission": dict(admission) if admission is not None else None,
            "profile": dict(profile) if profile is not None else None,
            "policies": policies,
            "probes": probes,
            "active_generation": active_generation,
            "active": _active_counts(connection),
            "registry": _registry_snapshot(connection),
        }


def _health_errors(path: Path) -> List[str]:
    payload = _read_json(path)
    if not isinstance(payload, Mapping):
        return ["health_marker_missing_or_invalid"]
    checks = payload.get("checks")
    if not isinstance(checks, Mapping):
        return ["health_checks_missing"]
    errors = []
    for name, check in checks.items():
        if not isinstance(check, Mapping) or check.get("passed") is not True or check.get("checker_error") is True:
            errors.append(f"check_not_pass:{name}")
    return errors


def _load_selected(
    concept_root: Path,
    coverage_path: Path,
    concepts: Sequence[str],
) -> tuple[List[Dict[str, Any]], List[str]]:
    ledger = _read_json(concept_root / "state" / "concepts-ledger.json", {})
    coverage = _read_json(coverage_path, {})
    errors: List[str] = []
    if not isinstance(ledger, Mapping):
        return [], ["concept_ledger_missing_or_invalid"]
    if (
        not isinstance(coverage, Mapping)
        or coverage.get("schema") != "concept-v11.source-coverage-report.v1"
        or coverage.get("status") != "PASS"
        or not isinstance(coverage.get("gate"), Mapping)
        or coverage["gate"].get("p3_closed") is not True
    ):
        return [], ["source_coverage_missing_or_not_closed"]
    coverage_by_concept = {
        str(row.get("concept") or ""): row
        for row in coverage.get("concepts", [])
        if isinstance(row, Mapping) and str(row.get("concept") or "")
    }
    selected: List[Dict[str, Any]] = []
    for concept in concepts:
        name = str(concept).strip()
        if not name:
            errors.append("empty_concept_name")
            continue
        record = ledger.get(name)
        if not isinstance(record, Mapping) or str(record.get("status") or "active") != "active":
            errors.append(f"concept_not_active:{name}")
            continue
        page = (concept_root / "state" / "pages" / f"{name}.md").resolve()
        if not page.is_file():
            errors.append(f"concept_page_missing:{name}")
            continue
        coverage_row = coverage_by_concept.get(name)
        if not isinstance(coverage_row, Mapping):
            errors.append(f"concept_coverage_missing:{name}")
            continue
        if str(coverage_row.get("coverage_status") or "") not in {"refreshable", "substituted"}:
            errors.append(f"concept_not_refreshable:{name}:{coverage_row.get('coverage_status') or 'missing'}")
            continue
        refs = [
            row
            for row in coverage_row.get("references", [])
            if isinstance(row, Mapping)
            and str(row.get("disposition") or "") in {"mapped", "substituted"}
            and str(row.get("source_map_status") or "") == "mapped"
            and str(row.get("evidence_set_hash") or "").startswith("sha256:")
        ]
        if not refs:
            errors.append(f"concept_current_source_missing:{name}")
            continue
        selected.append(
            {
                "concept": name,
                "page": page,
                "content_hash": _sha256_file(page),
                "source_refs": [
                    {
                        "source_uri": str(row.get("source_uri") or ""),
                        "map_id": str(row.get("map_id") or ""),
                        "disposition": str(row.get("disposition") or ""),
                        "evidence_set_hash": str(row.get("evidence_set_hash") or ""),
                    }
                    for row in sorted(refs, key=lambda item: str(item.get("source_uri") or ""))
                ],
            }
        )
    return selected, errors


def validate_canary(
    state: Mapping[str, Any],
    *,
    health_path: Path,
    namespace_epoch: str,
    runtime_epoch: str,
    processing_mode: str,
    now: Optional[datetime] = None,
) -> List[str]:
    now = now or _now()
    errors: List[str] = []
    if state.get("integrity") != "ok":
        errors.append("database_integrity_not_ok")
    freeze = state.get("freeze") or {}
    if str(freeze.get("state") or "") != "released":
        errors.append("runtime_fence_not_released")
    if str(freeze.get("migration_epoch") or "") != runtime_epoch:
        errors.append("runtime_epoch_mismatch")
    active = state.get("active") or {}
    if any(int(active.get(key, 0)) != 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")):
        errors.append("active_work_or_lease_present")
    if len(state.get("active_generation") or []) != 1:
        errors.append("concept_active_generation_not_unique")
    admission = state.get("admission") or {}
    if str(admission.get("namespace_epoch") or "") != namespace_epoch:
        errors.append("admission_namespace_mismatch")
    if str(admission.get("admission_state") or "") != "canary":
        errors.append(f"admission_not_canary:{admission.get('admission_state') or 'missing'}")
    expires = _parse_time(admission.get("expires_at"))
    if expires is None:
        errors.append("admission_ttl_missing_or_invalid")
    elif expires <= now:
        errors.append("admission_snapshot_expired")
    if not str(admission.get("admission_snapshot_id") or "") or not str(admission.get("evidence_hash") or ""):
        errors.append("admission_evidence_missing")
    profile = state.get("profile") or {}
    if str(profile.get("namespace_epoch") or "") != namespace_epoch:
        errors.append("profile_namespace_mismatch")
    if str(profile.get("pause_fence") or "") != "open":
        errors.append("profile_pause_fence")
    pending = int(profile.get("pending_count") or 0)
    soft = int(profile.get("pending_soft_limit") or 0)
    hard = int(profile.get("outbox_hard_cap") or 0)
    if soft <= 0 or hard <= 0 or pending >= soft or pending >= hard:
        errors.append("profile_capacity_unavailable")
    policies = list(state.get("policies") or [])
    if len(policies) != 1:
        errors.append("active_policy_not_unique")
        policy: Mapping[str, Any] = {}
    else:
        policy = policies[0]
        if str(policy.get("policy_version") or "") != POLICY_VERSION or str(policy.get("provider") or "") != "oneapi" or str(policy.get("requested_model") or "") != "auto":
            errors.append("active_policy_not_oneapi_auto")
        if str(profile.get("policy_hash") or "") != str(policy.get("policy_hash") or ""):
            errors.append("profile_policy_hash_mismatch")
    required = {"client_accept_probe", "backend_semantic_probe"}
    ready = set()
    for probe in state.get("probes") or []:
        if str(probe.get("probe_type") or "") not in required:
            continue
        if str(probe.get("processing_mode") or "") != processing_mode:
            continue
        if str(probe.get("capability_state") or "") != "ready":
            continue
        if str(probe.get("provider") or "") != str(policy.get("provider") or "") or str(probe.get("model_policy_version") or "") != str(policy.get("policy_version") or ""):
            continue
        probe_expires = _parse_time(probe.get("expires_at"))
        if probe_expires is not None and probe_expires > now:
            ready.add(str(probe.get("probe_type")))
    if not required.issubset(ready):
        errors.append("capability_probe_not_ready")
    health_errors = _health_errors(health_path)
    errors.extend(f"health:{item}" for item in health_errors)
    return sorted(set(errors))


def _status_snapshot(dispatcher: PMResourceDispatcher, outbox_id: str) -> Dict[str, Any]:
    with dispatcher.store.connect() as connection:
        row = connection.execute(
            "SELECT o.status,o.attempt,o.error_fingerprint,s.status,s.openviking_task_id,p.content_state,p.semantic_state "
            "FROM outbox_items o LEFT JOIN semantic_tasks s ON s.outbox_id=o.outbox_id "
            "LEFT JOIN resource_projections p ON p.resource_id=o.resource_id AND p.revision_id=o.revision_id "
            "WHERE o.outbox_id=?",
            (outbox_id,),
        ).fetchone()
    if row is None:
        return {"outbox_id": outbox_id, "missing": True}
    return {
        "outbox_id": outbox_id,
        "outbox_status": row[0],
        "attempt": int(row[1] or 0),
        "error_fingerprint": row[2],
        "semantic_status": row[3],
        "openviking_task_id": row[4],
        "content_state": row[5],
        "semantic_state": row[6],
    }


def _wait_terminal(
    dispatcher: PMResourceDispatcher,
    outbox_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> Dict[str, Any]:
    started = time.monotonic()
    observations: List[Dict[str, Any]] = []
    while True:
        dispatcher.reconcile_content(limit=1, min_age_seconds=0)
        dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
        state = _status_snapshot(dispatcher, outbox_id)
        observations.append({"at": _iso(_now()), **state})
        content_ok = state.get("content_state") == "content_verified"
        semantic_ok = state.get("semantic_status") == "completed" and state.get("semantic_state") == "semantic_completed"
        if content_ok and semantic_ok:
            return {"status": "completed", "elapsed_ms": round((time.monotonic() - started) * 1000, 3), "final": state, "observations": observations}
        if state.get("outbox_status") in TERMINAL_OUTBOX and state.get("semantic_status") in TERMINAL_SEMANTIC:
            return {"status": "failed", "elapsed_ms": round((time.monotonic() - started) * 1000, 3), "final": state, "observations": observations}
        if time.monotonic() - started >= max(0.1, float(timeout_seconds)):
            return {"status": "timeout", "elapsed_ms": round((time.monotonic() - started) * 1000, 3), "final": state, "observations": observations}
        time.sleep(max(0.01, min(float(poll_seconds), max(0.01, float(timeout_seconds) - (time.monotonic() - started)))))


def run_canary(
    db_path: Path,
    *,
    concept_root: Path = DEFAULT_CONCEPT_ROOT,
    coverage_path: Path = DEFAULT_COVERAGE,
    health_path: Path = DEFAULT_HEALTH,
    artifact_root: Optional[Path] = None,
    namespace_epoch: str = DEFAULT_NAMESPACE,
    runtime_epoch: str = DEFAULT_RUNTIME_EPOCH,
    concepts: Sequence[str] = DEFAULT_CONCEPTS,
    processing_mode: str = PROCESSING_MODE,
    canary_id: Optional[str] = None,
    apply: bool = False,
    authorization_id: str = "",
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    observation_seconds: float = 180.0,
    poll_seconds: float = 5.0,
    transport: Optional[Any] = None,
) -> Dict[str, Any]:
    db_path = db_path.expanduser().resolve()
    concept_root = concept_root.expanduser().resolve()
    coverage_path = coverage_path.expanduser().resolve()
    concepts = tuple(str(value).strip() for value in concepts if str(value).strip())
    canary_id = canary_id or ("canary-" + uuid.uuid4().hex[:16])
    selected, selection_errors = _load_selected(concept_root, coverage_path, concepts)
    state = _state_snapshot(db_path, namespace_epoch)
    validation_errors = validate_canary(
        state,
        health_path=health_path.expanduser().resolve(),
        namespace_epoch=namespace_epoch,
        runtime_epoch=runtime_epoch,
        processing_mode=processing_mode,
    )
    errors = sorted(set(selection_errors + validation_errors))
    if len(concepts) == 0 or len(concepts) > MAX_CONCEPTS or len(selected) != len(concepts):
        errors.append(f"concept_count_must_be_1_to_{MAX_CONCEPTS}")
    if processing_mode != PROCESSING_MODE:
        errors.append("production_canary_processing_mode_must_be_semantic_and_vectors")
    if apply and not authorization_id.strip():
        errors.append("authorization_id_required_for_apply")
    base: Dict[str, Any] = {
        "schema": SCHEMA,
        "status": "HOLD" if errors else ("DRY_RUN" if not apply else "HOLD"),
        "apply": bool(apply),
        "authorization_id": authorization_id or None,
        "canary_id": canary_id,
        "namespace_epoch": namespace_epoch,
        "runtime_epoch": runtime_epoch,
        "provider": "oneapi",
        "model_requested": "auto",
        "processing_mode": processing_mode,
        "wait": False,
        "target_namespace": f"viking://resources/concepts/__canary__/{namespace_epoch}/{canary_id}",
        "concepts": [dict(item, page=str(item["page"])) for item in selected],
        "coverage_path": str(coverage_path),
        "health_path": str(health_path),
        "before": state,
        "errors": sorted(set(errors)),
        "external_provider_calls": 0,
        "production_registry_unchanged": True,
    }
    if errors or not apply:
        return base

    backup = backup_database(db_path, backup_root.expanduser().resolve())
    base["backup"] = backup
    dispatcher = PMResourceDispatcher(
        PMSystemStore(db_path),
        transport=transport or OpenVikingTransport(),
        artifact_root=(artifact_root or (Path.home() / ".codex" / "pm-loop" / "resource-outbox" / "concept-canary")).expanduser().resolve(),
        observation_deadline_seconds=max(60, int(observation_seconds)),
    )
    results: List[Dict[str, Any]] = []
    for item in selected:
        target = f"{base['target_namespace']}/{hashlib.sha256(item['concept'].encode('utf-8')).hexdigest()[:16]}.md"
        started = time.monotonic()
        try:
            accepted = dispatcher.enqueue_concept_file(
                path=item["page"],
                target_uri=target,
                processing_mode=processing_mode,
                namespace_epoch=namespace_epoch,
                owner=f"concept-production-canary:{authorization_id}",
                wait=False,
                strict=False,
            )
            outbox_id = str(accepted["outbox_id"])
            dispatched = dispatcher.dispatch_pending(limit=1)
            if not dispatched:
                results.append({"concept": item["concept"], "target_uri": target, "status": "not_dispatched", "accepted": accepted})
                break
            result = _wait_terminal(dispatcher, outbox_id, timeout_seconds=observation_seconds, poll_seconds=poll_seconds)
            results.append(
                {
                    "concept": item["concept"],
                    "target_uri": target,
                    "status": result["status"],
                    "accepted": accepted,
                    "dispatch": dispatched[0],
                    "observation": result,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                }
            )
            if result["status"] != "completed":
                break
        except Exception as exc:  # A canary failure is reported, never retried here.
            results.append(
                {
                    "concept": item["concept"],
                    "target_uri": target,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                }
            )
            break
    after = _state_snapshot(db_path, namespace_epoch)
    registry_unchanged = before_registry = state.get("registry") == after.get("registry")
    active_zero = all(int(after.get("active", {}).get(key, 0)) == 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases"))
    passed = bool(results) and len(results) == len(selected) and all(row.get("status") == "completed" for row in results) and registry_unchanged and active_zero
    base.update(
        {
            "status": "PASS" if passed else "HOLD",
            "results": results,
            "after": after,
            "production_registry_unchanged": registry_unchanged,
            "active_zero_after": active_zero,
            "external_provider_calls": sum(1 for row in results if row.get("dispatch")),
            "errors": [] if passed else ["production_canary_not_fully_completed"],
        }
    )
    return base


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--health", type=Path, default=DEFAULT_HEALTH)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE)
    parser.add_argument("--runtime-epoch", default=DEFAULT_RUNTIME_EPOCH)
    parser.add_argument("--concept", dest="concepts", action="append", help="one or two mapped Active concepts")
    parser.add_argument("--canary-id")
    parser.add_argument("--observation-seconds", type=float, default=180.0)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--authorization-id", default="")
    parser.add_argument("--apply", action="store_true", help="perform the bounded production-path canary")
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    concepts = tuple(args.concepts or DEFAULT_CONCEPTS)
    try:
        result = run_canary(
            args.db_path,
            concept_root=args.concept_root,
            coverage_path=args.coverage,
            health_path=args.health,
            artifact_root=args.artifact_root,
            namespace_epoch=args.namespace_epoch,
            runtime_epoch=args.runtime_epoch,
            concepts=concepts,
            canary_id=args.canary_id,
            apply=args.apply,
            authorization_id=args.authorization_id,
            backup_root=args.backup_root,
            observation_seconds=args.observation_seconds,
            poll_seconds=args.poll_seconds,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        result = {"schema": SCHEMA, "status": "HOLD", "apply": bool(args.apply), "external_provider_calls": 0, "errors": [f"{type(exc).__name__}: {exc}"]}
    report = args.report.expanduser().resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    temporary = report.with_suffix(report.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, report)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
