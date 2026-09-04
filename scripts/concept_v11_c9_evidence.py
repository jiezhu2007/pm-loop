#!/usr/bin/env python3
"""Promote an already verified C6 provider-shadow report into C9 evidence.

This command never calls OneAPI or OpenViking.  It validates a bounded,
isolated C6 report and, when ``--apply`` is supplied, appends only the
concept capability-probe and model-resolution evidence rows in the existing
PM SQLite database.  Replays are idempotent.  A different payload for the
same deterministic evidence identity is reported as ``QUARANTINED`` and is
never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from pm_system_store import PMSystemStore, now_iso  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_POLICY_VERSION = "concept-v11-oneapi-auto-v1"
DEFAULT_TTL_SECONDS = 24 * 60 * 60
ISOLATED_PREFIX = "viking://resources/__pm_v11_provider_shadow__/"
PROBE_TYPES = ("client_accept_probe", "backend_semantic_probe")
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11-evidence"


class EvidenceValidationError(ValueError):
    """Raised when a report cannot be promoted to durable evidence."""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _hash_file_payload(payload: Mapping[str, Any]) -> str:
    return _hash(payload)


def _backup_database(db_path: Path, backup_root: Path) -> Dict[str, Any]:
    """Create and verify a unique recovery point before evidence writes."""
    root = backup_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = root / f"C9-EVIDENCE-{timestamp}-{uuid.uuid4().hex[:12]}.sqlite3"
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex[:8]}.tmp")
    source: Optional[sqlite3.Connection] = None
    target: Optional[sqlite3.Connection] = None
    try:
        source = sqlite3.connect(str(db_path), timeout=10)
        target = sqlite3.connect(str(temporary), timeout=10)
        source.backup(target)
        target.commit()
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()
    try:
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    with sqlite3.connect(str(destination), timeout=10) as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        schema = int(connection.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
    return {
        "path": str(destination),
        "sha256": "sha256:" + hashlib.sha256(destination.read_bytes()).hexdigest(),
        "size_bytes": destination.stat().st_size,
        "integrity_check": integrity,
        "core_schema_version": schema,
        "verified": integrity == "ok",
    }


def _as_hash(value: Any) -> str:
    return str(value or "").strip().lower().removeprefix("sha256:")


def _number(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceValidationError(f"{field} must be numeric") from exc
    if parsed < 0:
        raise EvidenceValidationError(f"{field} must be non-negative")
    return parsed


def _parse_time(value: Any, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise EvidenceValidationError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceValidationError(f"{field} is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise EvidenceValidationError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


def _status(value: Any) -> str:
    if isinstance(value, Mapping):
        raw = value.get("status")
        if isinstance(raw, str) and raw.strip():
            normalized = raw.strip().lower()
            # OpenViking wraps the actual task response in ``status=ok``;
            # continue into ``result.status`` instead of treating the
            # transport envelope as the terminal task state.
            if normalized not in {"ok", "created"}:
                return normalized
        for key, child in value.items():
            if str(key).lower() in {"error", "errors"}:
                continue
            found = _status(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _status(child)
            if found:
                return found
    return ""


def _load_report(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceValidationError(f"cannot read report: {path}") from exc
    if not isinstance(value, dict):
        raise EvidenceValidationError("provider-shadow report must be a JSON object")
    return value


def _validate_report(report: Mapping[str, Any], *, ttl_seconds: int) -> Dict[str, Any]:
    if int(ttl_seconds) <= 0:
        raise EvidenceValidationError("ttl_seconds must be positive")
    if int(ttl_seconds) > 7 * 24 * 60 * 60:
        raise EvidenceValidationError("ttl_seconds exceeds the seven-day evidence bound")
    if str(report.get("schema") or "") != "concept-v11.c6-provider-shadow.v1":
        raise EvidenceValidationError("unsupported provider-shadow report schema")
    if str(report.get("stage_id") or "") != "C6-PROVIDER-SHADOW":
        raise EvidenceValidationError("report stage is not C6-PROVIDER-SHADOW")
    if str(report.get("status") or "") not in {"PASS_WITH_UNKNOWN_MODEL", "PASS"}:
        raise EvidenceValidationError("provider-shadow report is not successful")
    if report.get("read_only_pm_database") is not True:
        raise EvidenceValidationError("report did not prove PM database was read-only")
    if report.get("concept_admission_changed") is not False:
        raise EvidenceValidationError("report did not prove concept admission was unchanged")
    if report.get("namespace_isolated") is not True:
        raise EvidenceValidationError("report did not prove an isolated namespace")
    target_uri = str(report.get("target_uri") or "")
    if not target_uri.startswith(ISOLATED_PREFIX):
        raise EvidenceValidationError("target URI is outside the provider-shadow namespace")
    if report.get("processing_mode") not in {"semantic_only", "semantic_and_vectors"}:
        raise EvidenceValidationError("unsupported processing mode")
    if report.get("wait") is not False or report.get("accepted") is not True:
        raise EvidenceValidationError("report did not prove wait=false accepted/task")
    task_id = str(report.get("task_id") or "").strip()
    if not task_id:
        raise EvidenceValidationError("task_id is required")
    if str(report.get("model_requested") or "") != "auto":
        raise EvidenceValidationError("model_requested must be auto")
    resolution_status = str(report.get("model_resolution_status") or "")
    resolved_model = report.get("model_resolved")
    if resolution_status not in {"unknown", "resolved"}:
        raise EvidenceValidationError("model resolution status is invalid")
    if resolution_status == "resolved" and not str(resolved_model or ""):
        raise EvidenceValidationError("resolved model identity is empty")
    if resolution_status == "unknown" and resolved_model is not None:
        raise EvidenceValidationError("unknown model resolution cannot contain a model identity")
    model_resolution_gate = str(report.get("model_resolution_gate") or "provider_configuration_trusted")
    if model_resolution_gate != "provider_configuration_trusted":
        raise EvidenceValidationError("model resolution gate is not the trusted provider configuration contract")
    if int(report.get("external_provider_calls") or 0) != 1:
        raise EvidenceValidationError("expected exactly one bounded provider-shadow call")
    if report.get("errors") not in ([], None):
        raise EvidenceValidationError("report contains provider-shadow errors")
    if str(report.get("remote_status") or "").lower() not in {"completed", "complete", "success", "succeeded", "done"}:
        raise EvidenceValidationError("remote task is not terminal success")
    if str(report.get("semantic_projection") or "") != "completed":
        raise EvidenceValidationError("semantic projection is not completed")
    if report.get("content_verified") is not True:
        raise EvidenceValidationError("content read-back was not verified")
    source_hash = _as_hash(report.get("source_hash"))
    read_back_hash = _as_hash(report.get("read_back_hash"))
    if not source_hash or source_hash != read_back_hash:
        raise EvidenceValidationError("source/read-back content hashes do not match")

    queue = report.get("queue_status")
    if not isinstance(queue, Mapping):
        raise EvidenceValidationError("queue_status is required")
    semantic = queue.get("Semantic")
    if not isinstance(semantic, Mapping):
        raise EvidenceValidationError("Semantic queue metrics are required")
    if int(semantic.get("processed") or 0) < 1:
        raise EvidenceValidationError("Semantic queue did not process an item")
    if int(semantic.get("requeue_count") or 0) != 0:
        raise EvidenceValidationError("Semantic queue was requeued")
    if int(semantic.get("error_count") or 0) != 0:
        raise EvidenceValidationError("Semantic queue reported an error")
    terminal = report.get("task_terminal_response")
    if _status(terminal) not in {"completed", "complete", "success", "succeeded", "done"}:
        raise EvidenceValidationError("task terminal response is not completed")

    observed_at = _parse_time(report.get("observed_at"), "observed_at")
    expires_at = observed_at + timedelta(seconds=int(ttl_seconds))
    return {
        "task_id": task_id,
        "target_uri": target_uri,
        "source_hash": "sha256:" + source_hash,
        "observed_at": observed_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "accepted_latency_ms": _number(report.get("accepted_latency_ms"), "accepted_latency_ms"),
        "semantic_latency_ms": _number(report.get("semantic_latency_ms"), "semantic_latency_ms"),
        "processing_mode": str(report.get("processing_mode")),
        "model_resolution_gate": model_resolution_gate,
        "model_resolution_status": resolution_status,
        "model_resolved": resolved_model,
        "semantic_metrics": {
            "processed": int(semantic.get("processed") or 0),
            "requeue_count": int(semantic.get("requeue_count") or 0),
            "error_count": int(semantic.get("error_count") or 0),
        },
    }


def _row_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _same_probe(existing: sqlite3.Row, expected: Mapping[str, Any]) -> bool:
    fields = (
        "probe_id", "probe_type", "namespace_epoch", "profile", "processing_mode", "provider",
        "model_policy_version", "capability_state", "accepted_latency_ms", "semantic_latency_ms",
        "task_id", "response_json", "evidence_hash", "observed_at", "expires_at",
    )
    for field in fields:
        actual = existing[field]
        wanted = expected.get(field)
        if field == "response_json":
            if _canonical(_row_json(actual)) != _canonical(_row_json(wanted)):
                return False
        elif field in {"accepted_latency_ms", "semantic_latency_ms"}:
            if (actual is None) != (wanted is None):
                return False
            if actual is not None and abs(float(actual) - float(wanted)) > 1e-9:
                return False
        elif actual != wanted:
            return False
    return True


def _same_resolution(existing: sqlite3.Row, expected: Mapping[str, Any]) -> bool:
    fields = (
        "call_id", "model_requested", "model_resolved", "resolution_status", "policy_version", "provider",
        "resolution_changed", "model_input_hash", "evidence_hash",
    )
    return all(existing[field] == expected.get(field) for field in fields)


def _active_context(store: PMSystemStore) -> Dict[str, Any]:
    with store.connect() as connection:
        policies = connection.execute(
            "SELECT policy_version,provider,requested_model,allowed_models_json,policy_hash,status FROM concept_model_policies "
            "WHERE status='active' AND provider='oneapi' AND requested_model='auto' ORDER BY policy_version"
        ).fetchall()
        if len(policies) != 1:
            raise EvidenceValidationError("expected exactly one active oneapi/auto policy")
        policy = dict(policies[0])
        profiles = connection.execute(
            "SELECT workload,profile,namespace_epoch,pending_count,pending_soft_limit,outbox_hard_cap,pause_fence,policy_hash "
            "FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile='pm-semantic' ORDER BY namespace_epoch"
        ).fetchall()
        if len(profiles) != 1:
            raise EvidenceValidationError("expected exactly one concept-semantic/pm-semantic profile")
        profile = dict(profiles[0])
        admissions = connection.execute(
            "SELECT namespace_epoch,admission_state,version FROM concept_admissions WHERE namespace_epoch=?",
            (profile["namespace_epoch"],),
        ).fetchall()
        if len(admissions) != 1 or str(admissions[0]["admission_state"]) != "disabled":
            raise EvidenceValidationError("concept admission must remain disabled")
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise EvidenceValidationError("database integrity is not ok")
    return {"policy": policy, "profile": profile, "admission": dict(admissions[0])}


def _build_evidence(
    report: Mapping[str, Any],
    *,
    report_hash: str,
    context: Mapping[str, Any],
    validated: Mapping[str, Any],
    ttl_seconds: int,
) -> Dict[str, Any]:
    policy = context["policy"]
    profile = context["profile"]
    namespace_epoch = str(profile["namespace_epoch"])
    policy_version = str(policy["policy_version"])
    task_id = str(validated["task_id"])
    common_response = {
        "evidence_schema": "concept-v11.c9-provider-shadow-evidence.v1",
        "report_hash": report_hash,
        "report_status": report.get("status"),
        "target_uri": validated["target_uri"],
        "task_id": task_id,
        "model_requested": "auto",
        "model_resolved": validated["model_resolved"],
        "model_resolution_status": validated["model_resolution_status"],
        "model_resolution_gate": validated["model_resolution_gate"],
        "model_resolution_gate_status": "not_required",
        "processing_mode": validated["processing_mode"],
        "semantic_metrics": validated["semantic_metrics"],
        "content_verified": True,
        "source_hash": validated["source_hash"],
        "read_back_hash": validated["source_hash"],
        "ttl_seconds": int(ttl_seconds),
    }
    probes = []
    for probe_type in PROBE_TYPES:
        probe_id = "probe-" + hashlib.sha256(
            f"{report_hash}|{namespace_epoch}|pm-semantic|{probe_type}".encode("utf-8")
        ).hexdigest()[:32]
        response = dict(common_response)
        response["probe_type"] = probe_type
        response["accepted_latency_ms"] = validated["accepted_latency_ms"]
        response["semantic_latency_ms"] = validated["semantic_latency_ms"] if probe_type == "backend_semantic_probe" else None
        probes.append(
            {
                "probe_id": probe_id,
                "probe_type": probe_type,
                "namespace_epoch": namespace_epoch,
                "profile": "pm-semantic",
                "processing_mode": validated["processing_mode"],
                "provider": "oneapi",
                "model_policy_version": policy_version,
                "capability_state": "ready",
                "accepted_latency_ms": validated["accepted_latency_ms"],
                "semantic_latency_ms": response["semantic_latency_ms"],
                "task_id": task_id,
                "response_json": _canonical(response),
                "evidence_hash": _hash({"report_hash": report_hash, "probe_type": probe_type}),
                "observed_at": validated["observed_at"],
                "expires_at": validated["expires_at"],
            }
        )
    resolution_identity = f"{report_hash}|{namespace_epoch}|{task_id}|provider-shadow|1"
    resolution = {
        "resolution_id": "resolution-" + hashlib.sha256(resolution_identity.encode("utf-8")).hexdigest()[:32],
        "run_id": "provider-shadow:" + task_id,
        "call_id": "provider-shadow-call:" + task_id,
        "stage": "provider-shadow",
        "attempt": 1,
        "model_requested": "auto",
        "model_resolved": validated["model_resolved"],
        "resolution_status": validated["model_resolution_status"],
        "policy_version": policy_version,
        "provider": "oneapi",
        "resolution_changed": 0,
        "model_input_hash": validated["source_hash"],
        "evidence_hash": report_hash,
        "created_at": validated["observed_at"],
    }
    return {"probes": probes, "resolution": resolution, "namespace_epoch": namespace_epoch, "policy": policy}


def promote(
    db_path: Path,
    report_path: Path,
    *,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    apply: bool = False,
    backup_root: Path = DEFAULT_BACKUP_ROOT,
) -> Dict[str, Any]:
    report = _load_report(report_path)
    validated = _validate_report(report, ttl_seconds=int(ttl_seconds))
    report_hash = _hash_file_payload(report)
    store = PMSystemStore(db_path)
    context = _active_context(store)
    evidence = _build_evidence(report, report_hash=report_hash, context=context, validated=validated, ttl_seconds=int(ttl_seconds))
    probes = evidence["probes"]
    resolution = evidence["resolution"]
    result: Dict[str, Any] = {
        "schema": "concept-v11.c9-evidence-promotion.v1",
        "observed_at": now_iso(),
        "report_path": report_path.name,
        "report_hash": report_hash,
        "ttl_seconds": int(ttl_seconds),
        "external_provider_calls": 0,
        "namespace_epoch": evidence["namespace_epoch"],
        "policy_version": evidence["policy"]["policy_version"],
        "model_requested": "auto",
        "model_resolved": validated["model_resolved"],
        "resolution_status": validated["model_resolution_status"],
        "model_resolution_gate": validated["model_resolution_gate"],
        "model_resolution_gate_status": "not_required",
        "concept_admission": context["admission"],
        "probe_ids": [row["probe_id"] for row in probes],
        "resolution_id": resolution["resolution_id"],
        "db_writes": {"concept_capability_probes": 0, "concept_model_resolutions": 0},
        "status": "DRY_RUN" if not apply else "PASS",
        "conflicts": [],
    }
    if not apply:
        result["next_step"] = "re-run with --apply after an independent SQLite backup"
        return result

    backup = _backup_database(db_path, backup_root)
    result["backup"] = backup
    if not backup.get("verified"):
        result["status"] = "HOLD"
        result["next_step"] = "repair backup before evidence promotion"
        return result

    with store.transaction() as connection:
        for expected in probes:
            existing = connection.execute(
                "SELECT * FROM concept_capability_probes WHERE probe_id=?", (expected["probe_id"],)
            ).fetchone()
            if existing is not None and not _same_probe(existing, expected):
                result["conflicts"].append({"entity": "concept_capability_probes", "id": expected["probe_id"]})
        existing_resolution = connection.execute(
            "SELECT * FROM concept_model_resolutions WHERE run_id IS ? AND stage=? AND attempt=?",
            (resolution["run_id"], resolution["stage"], resolution["attempt"]),
        ).fetchone()
        if existing_resolution is not None and not _same_resolution(existing_resolution, resolution):
            result["conflicts"].append({"entity": "concept_model_resolutions", "id": resolution["resolution_id"], "identity": [resolution["run_id"], resolution["stage"], resolution["attempt"]]})
        if result["conflicts"]:
            result["status"] = "QUARANTINED"
            result["next_step"] = "manual evidence review; existing rows were not overwritten"
            return result
        for expected in probes:
            if connection.execute("SELECT 1 FROM concept_capability_probes WHERE probe_id=?", (expected["probe_id"],)).fetchone() is None:
                connection.execute(
                    "INSERT INTO concept_capability_probes(probe_id,probe_type,namespace_epoch,profile,processing_mode,provider,model_policy_version,capability_state,accepted_latency_ms,semantic_latency_ms,task_id,response_json,evidence_hash,observed_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(expected[field] for field in ("probe_id", "probe_type", "namespace_epoch", "profile", "processing_mode", "provider", "model_policy_version", "capability_state", "accepted_latency_ms", "semantic_latency_ms", "task_id", "response_json", "evidence_hash", "observed_at", "expires_at")),
                )
                result["db_writes"]["concept_capability_probes"] += 1
        if existing_resolution is None:
            connection.execute(
                "INSERT INTO concept_model_resolutions(resolution_id,run_id,call_id,stage,attempt,model_requested,model_resolved,resolution_status,policy_version,provider,resolution_changed,model_input_hash,evidence_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(resolution[field] for field in ("resolution_id", "run_id", "call_id", "stage", "attempt", "model_requested", "model_resolved", "resolution_status", "policy_version", "provider", "resolution_changed", "model_input_hash", "evidence_hash", "created_at")),
            )
            result["db_writes"]["concept_model_resolutions"] = 1
    result["idempotent_replay"] = result["db_writes"]["concept_capability_probes"] == 0 and result["db_writes"]["concept_model_resolutions"] == 0
    result["next_step"] = "run C9 read-only preflight"
    return result


def _write_result_report(path: Path, result: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(result), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--result-report", type=Path, help="write the promotion result as an atomic JSON report")
    parser.add_argument("--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--apply", action="store_true", help="append evidence rows after external backup")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = promote(args.db_path.expanduser().resolve(), args.report.expanduser().resolve(), ttl_seconds=args.ttl_seconds, apply=args.apply, backup_root=args.backup_root.expanduser().resolve())
    except (EvidenceValidationError, RuntimeError, sqlite3.Error) as exc:
        result = {"schema": "concept-v11.c9-evidence-promotion.v1", "status": "HOLD", "errors": [f"{type(exc).__name__}:{exc}"], "external_provider_calls": 0}
        if args.result_report:
            _write_result_report(args.result_report.expanduser().resolve(), result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1
    if args.result_report:
        _write_result_report(args.result_report.expanduser().resolve(), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
