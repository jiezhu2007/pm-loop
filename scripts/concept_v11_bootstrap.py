#!/usr/bin/env python3
"""Create a read-only v1.1 concept bootstrap admission snapshot.

The snapshot is an admission *preflight*, not an admission switch.  It binds
current runtime, queue, health, schema, source-map, Canary, provider-shadow,
model-policy and watermark evidence to one short-lived ID.  This command
never changes ``concept_admission`` and never calls OneAPI/OpenViking.
Known maintenance-only health findings may be explicitly exempted; the raw
finding remains in the snapshot and all other findings stay hard blockers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_migration import foundation_check  # noqa: E402
from concept_v11_schema_v2 import schema_v2_state  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_HEALTH = Path.home() / ".codex" / "skills" / "system-health-check" / "state" / "latest.json"
# The runner is mirrored below ~/.codex at runtime, but admission evidence is
# authored in the canonical project.  Do not derive this path from __file__.
CANONICAL_PROJECT_ROOT = Path(
    os.environ.get("PM_CANONICAL_PROJECT_ROOT", str(Path.home() / "Documents" / "project"))
).expanduser()
DEFAULT_REPORT_DIR = CANONICAL_PROJECT_ROOT / "docs" / "03-产品架构" / "v1.1实施报告"
DEFAULT_CONTENT_SOURCE_PREFLIGHT = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "content-source-preflight-current.json"
DEFAULT_NAMESPACE = "v45-r2-20260830"
DEFAULT_MAX_AGE_SECONDS = 24 * 60 * 60
DEFAULT_TTL_SECONDS = 15 * 60
REQUIRED_REPORTS = {
    # Admission must be bound to the current closure, not the historical
    # August migration artifacts.  Each report is regenerated immediately
    # before a canary or an incremental admission transition.
    "source_map": "source-coverage-report-retirement-20260903.json",
    "canary": "c8-concept-compile-canary-current.json",
    "evidence_promotion": "c9-evidence-promotion-current.json",
}

# A snapshot binds a CAS to the Admission state observed at preflight time.
# The default remains the initial disabled-to-canary path. Recovery targets
# are explicit so an expired Canary can be safely returned to disabled.
BOOTSTRAP_CURRENT_STATES_BY_TARGET = {
    "canary": {"disabled"},
    # This self-transition exists solely for the one-time migration from the
    # former short-TTL incremental Admission to incremental/continuous.  The
    # admission runner rejects a repeat once continuous is already active.
    "incremental": {"canary", "incremental"},
    "disabled": {"canary", "incremental", "hold"},
    "hold": {"disabled", "shadow", "canary", "incremental"},
    "shadow": {"disabled", "hold"},
}

# These are deliberately exact, narrow operational exceptions. They do not
# change the health checker result and cannot bypass schema, queue, provider,
# or runtime-integrity gates.
MAINTENANCE_EXEMPTIONS = {
    "maintenance:v4-4-s10-human-removed": {
        "health_error": "check_not_pass:Codex automation 状态",
        "reason": "v4-4-s10 automation was intentionally removed by the operator; restore is deferred",
    },
    "maintenance:fde-weekly-deferred": {
        "health_error": "check_not_pass:产品情报周度比较门禁",
        "reason": "FDE weekly source is incomplete; repair is deferred and deletion conclusions remain suppressed",
    },
}


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> Optional[str]:
    try:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _apply_maintenance_exemptions(
    health_errors: Iterable[str], requested: Iterable[str]
) -> tuple[list[str], list[Dict[str, Any]], list[str]]:
    """Filter only explicitly named maintenance findings.

    Unknown or unobserved exemption ids remain hard errors so a stale waiver
    cannot silently mask a changed health state.
    """
    requested_ids = list(dict.fromkeys(str(item).strip() for item in requested if str(item).strip()))
    effective: list[str] = []
    applied: list[Dict[str, Any]] = []
    for error in sorted(set(str(item) for item in health_errors)):
        matches = [
            exemption_id
            for exemption_id in requested_ids
            if exemption_id in MAINTENANCE_EXEMPTIONS
            and MAINTENANCE_EXEMPTIONS[exemption_id]["health_error"] == error
        ]
        if not matches:
            effective.append(error)
            continue
        for exemption_id in matches:
            applied.append(
                {
                    "id": exemption_id,
                    "health_error": error,
                    "reason": MAINTENANCE_EXEMPTIONS[exemption_id]["reason"],
                }
            )
    applied_ids = {str(item["id"]) for item in applied}
    for exemption_id in requested_ids:
        if exemption_id not in MAINTENANCE_EXEMPTIONS:
            effective.append(f"maintenance_exemption_unknown:{exemption_id}")
        elif exemption_id not in applied_ids:
            effective.append(f"maintenance_exemption_not_observed:{exemption_id}")
    return sorted(set(effective)), applied, requested_ids


def _parse_time(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _fresh(path: Path, payload: Mapping[str, Any], *, now: datetime, max_age_seconds: int, time_field: str = "observed_at") -> Dict[str, Any]:
    observed = _parse_time(payload.get(time_field))
    if observed is None:
        return {"path": str(path), "fresh": False, "reason": f"{time_field}_missing_or_invalid"}
    age = (now - observed).total_seconds()
    return {"path": str(path), "observed_at": observed.isoformat(timespec="seconds").replace("+00:00", "Z"), "age_seconds": round(age, 3), "fresh": 0 <= age <= max_age_seconds, "reason": "" if 0 <= age <= max_age_seconds else "stale_or_future"}


def _report(path: Path, *, now: datetime, max_age_seconds: int, expected_status: Iterable[str] = ("PASS", "PASS_WITH_QUARANTINE")) -> Dict[str, Any]:
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {"path": str(path), "exists": path.is_file(), "status": None, "fresh": False, "errors": ["report_missing_or_invalid"]}
    timestamp_source = "report.observed_at"
    if not _parse_time(payload.get("observed_at")):
        # C7 coverage produces a generated_at timestamp because it is a
        # deterministic aggregation.  It is still an execution timestamp and
        # is stronger evidence than file mtime.
        if _parse_time(payload.get("generated_at")):
            payload = dict(payload)
            payload["observed_at"] = str(payload["generated_at"])
            timestamp_source = "report.generated_at"
        else:
            # The filesystem mtime is only a compatibility fallback for old
            # reports that predate the structured evidence contract.
            try:
                payload = dict(payload)
                payload["observed_at"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                timestamp_source = "filesystem_mtime"
            except OSError:
                timestamp_source = "missing"
    freshness = _fresh(path, payload, now=now, max_age_seconds=max_age_seconds)
    status = str(payload.get("status") or "")
    errors = [] if status in set(expected_status) else [f"unexpected_status:{status or 'missing'}"]
    if not freshness["fresh"]:
        errors.append(freshness["reason"])
    return {"path": str(path), "exists": True, "status": status, "fresh": freshness["fresh"], "observed_at": freshness.get("observed_at"), "timestamp_source": timestamp_source, "age_seconds": freshness.get("age_seconds"), "sha256": _file_hash(path), "errors": errors, "payload": payload}


def _active_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    statuses = {
        "jobs": ("queued", "running", "processing", "active", "retry_wait"),
        "runs": ("queued", "running", "processing", "active", "retry_wait"),
        "outbox_items": ("pending", "in_flight", "dispatching", "processing", "active", "retry_wait"),
        "semantic_tasks": ("queued", "in_flight", "accepted", "processing", "active", "retry_wait"),
    }
    result: Dict[str, int] = {}
    for table, values in statuses.items():
        placeholders = ",".join("?" for _ in values)
        result[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE status IN ({placeholders})", values).fetchone()[0])
    result["slots"] = int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status <> 'free'").fetchone()[0])
    result["tokens"] = int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0])
    result["migration_leases"] = int(connection.execute("SELECT COUNT(*) FROM migration_leases WHERE state='active'").fetchone()[0])
    result["dispatch_leases"] = int(connection.execute("SELECT COUNT(*) FROM outbox_dispatch_leases").fetchone()[0])
    result["probe_leases"] = int(connection.execute("SELECT COUNT(*) FROM provider_probe_leases").fetchone()[0])
    return result


def _watermarks(connection: sqlite3.Connection) -> Dict[str, Any]:
    rows = connection.execute("SELECT watermark_name,captured_at,value_hash,value,producer,state FROM watermarks WHERE source_domain='pm-runtime' ORDER BY watermark_name").fetchall()
    result = {}
    for row in rows:
        raw_value = row[3]
        try:
            value = json.loads(str(raw_value))
        except (TypeError, json.JSONDecodeError):
            value = raw_value
        result[str(row[0])] = {
            "captured_at": row[1],
            "value_hash": row[2],
            "value": value,
            "producer": row[4],
            "state": row[5],
        }
    return result


def validate_snapshot_inputs(
    *,
    namespace_epoch: str,
    runtime_epoch: str,
    freeze: Mapping[str, Any],
    active: Mapping[str, int],
    schema: Mapping[str, Any],
    admission: Mapping[str, Any],
    profile: Mapping[str, Any],
    policies: list[Mapping[str, Any]],
    probes: list[Mapping[str, Any]],
    resolutions: list[Mapping[str, Any]],
    watermarks: Mapping[str, Any],
    active_generation: list[Mapping[str, Any]],
    health: Mapping[str, Any],
    reports: Mapping[str, Mapping[str, Any]],
    now: datetime,
    transition_target: str = "canary",
) -> list[str]:
    errors: list[str] = []
    if str(freeze.get("state") or "") != "released":
        errors.append("runtime_fence_not_released")
    if not runtime_epoch:
        errors.append("runtime_epoch_missing")
    if not all(int(active.get(key, 0)) == 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")):
        errors.append("active_work_or_lease_present")
    if len(active_generation) != 1:
        errors.append("concept_active_generation_not_unique")
    else:
        active = active_generation[0]
        generation_watermark = watermarks.get("active_generation")
        generation_value = generation_watermark.get("value") if isinstance(generation_watermark, Mapping) else None
        if not isinstance(generation_watermark, Mapping) or str(generation_watermark.get("state") or "") != "accepted":
            errors.append("active_generation_watermark_not_accepted")
        elif not isinstance(generation_value, Mapping) or (
            str(generation_value.get("generation_id") or "") != str(active.get("generation_id") or "")
            or str(generation_value.get("generation_hash") or "") != str(active.get("generation_hash") or "")
        ):
            errors.append("active_generation_watermark_mismatch")
    if int(schema.get("schema_version") or 0) != 2 or not schema.get("hot_projection_composite_key"):
        errors.append("concept_schema_v2_not_ready")
    permitted_admission_states = BOOTSTRAP_CURRENT_STATES_BY_TARGET.get(transition_target)
    if permitted_admission_states is None:
        errors.append("bootstrap_transition_target_invalid")
    elif (
        str(admission.get("namespace_epoch") or "") != namespace_epoch
        or str(admission.get("admission_state") or "") not in permitted_admission_states
    ):
        errors.append(f"concept_admission_not_safe_for_{transition_target}")
    if str(profile.get("namespace_epoch") or "") != namespace_epoch or int(profile.get("pending_count") or 0) != 0:
        errors.append("concept_profile_not_empty_or_wrong_epoch")
    if int(profile.get("pending_soft_limit") or 0) <= 0 or int(profile.get("outbox_hard_cap") or 0) <= 0 or str(profile.get("pause_fence") or "") != "open":
        errors.append("concept_profile_capacity_not_ready")
    if len(policies) != 1 or str(policies[0].get("provider")) != "oneapi" or str(policies[0].get("requested_model")) != "auto":
        errors.append("oneapi_auto_policy_not_unique")
        active_policy: Mapping[str, Any] = {}
    else:
        active_policy = policies[0]
        if not str(active_policy.get("policy_version") or "") or not str(active_policy.get("policy_hash") or ""):
            errors.append("active_policy_incomplete")
        try:
            allowed_models = json.loads(str(active_policy.get("allowed_models_json") or "[]"))
        except json.JSONDecodeError:
            allowed_models = None
        if not isinstance(allowed_models, list):
            errors.append("active_policy_allowlist_invalid")
        if str(profile.get("policy_hash") or "") != str(active_policy.get("policy_hash") or ""):
            errors.append("concept_profile_policy_hash_mismatch")
    required_probe_types = {"client_accept_probe", "backend_semantic_probe"}
    # Probe rows are append-only. Historical or expired probes document the
    # prior state but cannot make a fresh probe fail. Select the latest
    # policy-matching row for each required capability, then validate only
    # that effective current evidence.
    current_probes: Dict[str, Mapping[str, Any]] = {}
    for probe in probes:
        probe_type = str(probe.get("probe_type") or "")
        if probe_type not in required_probe_types:
            continue
        if (
            str(probe.get("namespace_epoch") or "") != namespace_epoch
            or str(probe.get("profile") or "") != str(profile.get("profile") or "")
            or str(probe.get("provider") or "") != str(active_policy.get("provider") or "")
            or str(probe.get("model_policy_version") or "") != str(active_policy.get("policy_version") or "")
        ):
            continue
        observed = _parse_time(probe.get("observed_at"))
        existing = current_probes.get(probe_type)
        existing_observed = _parse_time(existing.get("observed_at")) if existing else None
        if existing is None or (observed is not None and (existing_observed is None or observed > existing_observed)):
            current_probes[probe_type] = probe
    for probe_type in sorted(required_probe_types):
        probe = current_probes.get(probe_type)
        if probe is None:
            errors.append(f"capability_probe_missing:{probe_type}")
            continue
        if str(probe.get("capability_state") or "") != "ready":
            errors.append(f"capability_probe_not_ready:{probe.get('probe_id')}")
        expires = _parse_time(probe.get("expires_at"))
        if expires is None or expires <= now:
            errors.append(f"capability_probe_expired:{probe.get('probe_id')}")
    matching_resolutions = [
        row
        for row in resolutions
        if str(row.get("policy_version") or "") == str(active_policy.get("policy_version") or "")
        and str(row.get("provider") or "") == str(active_policy.get("provider") or "")
    ]
    if not matching_resolutions or any(
        str(row.get("model_requested") or "") != "auto"
        or str(row.get("resolution_status") or "") not in {"unknown", "resolved"}
        or (str(row.get("resolution_status") or "") == "resolved" and not str(row.get("model_resolved") or ""))
        for row in matching_resolutions
    ):
        errors.append("model_resolution_evidence_missing_or_invalid")
    for name, report in reports.items():
        if report.get("errors"):
            errors.extend(f"{name}:{error}" for error in report["errors"])
    effective_health_errors = health.get("effective_errors", health.get("errors", []))
    if effective_health_errors:
        errors.extend(f"health:{error}" for error in effective_health_errors)
    for name in ("source", "content", "knowledge"):
        row = watermarks.get(name)
        if not isinstance(row, Mapping) or str(row.get("state") or "") != "accepted" or not row.get("value_hash"):
            errors.append(f"watermark_not_accepted:{name}")
    return sorted(set(errors))


def build_snapshot(
    db_path: Path,
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    health_path: Path = DEFAULT_HEALTH,
    content_source_preflight_path: Path = DEFAULT_CONTENT_SOURCE_PREFLIGHT,
    namespace_epoch: str = DEFAULT_NAMESPACE,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    maintenance_exemptions: Iterable[str] = (),
    transition_target: str = "canary",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    transition_target = str(transition_target or "").strip().lower()
    if transition_target not in BOOTSTRAP_CURRENT_STATES_BY_TARGET:
        raise ValueError(f"invalid bootstrap transition target: {transition_target}")
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    store = PMSystemStore(db_path)
    freeze = store.migration_freeze() or {}
    runtime_epoch = str(freeze.get("migration_epoch") or "")
    foundation = foundation_check(store, expected_epoch=runtime_epoch)
    with store.connect() as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        active = _active_counts(connection)
        admission_row = connection.execute("SELECT namespace_epoch,admission_state,version FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)).fetchone()
        profile_row = connection.execute("SELECT workload,profile,namespace_epoch,pending_count,pending_soft_limit,pending_high_water,outbox_hard_cap,pause_fence,policy_hash FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile='pm-semantic' AND namespace_epoch=?", (namespace_epoch,)).fetchone()
        policies = [dict(row) for row in connection.execute("SELECT policy_version,provider,requested_model,allowed_models_json,policy_hash,status FROM concept_model_policies WHERE status='active'").fetchall()]
        probes = [dict(row) for row in connection.execute("SELECT * FROM concept_capability_probes WHERE namespace_epoch=? AND profile='pm-semantic' ORDER BY probe_type", (namespace_epoch,)).fetchall()]
        resolutions = [dict(row) for row in connection.execute("SELECT * FROM concept_model_resolutions ORDER BY created_at").fetchall()]
        watermarks = _watermarks(connection)
        active_generation = [
            dict(row)
            for row in connection.execute(
                "SELECT generation_id,generation_hash,status,source_watermark,knowledge_watermark,active_at "
                "FROM generations WHERE domain='concepts' AND status='active' ORDER BY active_at DESC,generation_id"
            ).fetchall()
        ]
        g9 = connection.execute("SELECT migration_id,migration_epoch,stage_id,state,owner FROM migration_leases WHERE migration_id='v45-r2-20260830' AND stage_id='G9' ORDER BY rowid DESC LIMIT 1").fetchone()
    admission = dict(admission_row) if admission_row is not None else {}
    profile = dict(profile_row) if profile_row is not None else {}
    schema = schema_v2_state(store)
    health_payload = _read_json(health_path)
    health_errors: list[str] = []
    if not isinstance(health_payload, dict):
        health_errors.append("marker_missing_or_invalid")
    else:
        checks = health_payload.get("checks")
        if not isinstance(checks, Mapping):
            health_errors.append("checks_missing")
        else:
            for name, check in checks.items():
                if not isinstance(check, Mapping) or check.get("checker_error") is True or check.get("passed") is not True:
                    health_errors.append(f"check_not_pass:{name}")
        marker_mtime = health_path.stat().st_mtime if health_path.is_file() else 0
        age = time.time() - marker_mtime if marker_mtime else float("inf")
        if age < 0 or age > max_age_seconds:
            health_errors.append("marker_stale")
    effective_health_errors, applied_exemptions, requested_exemptions = _apply_maintenance_exemptions(
        health_errors, maintenance_exemptions
    )
    health = {
        "path": str(health_path),
        "sha256": _file_hash(health_path),
        "run_at": health_payload.get("run_at") if isinstance(health_payload, dict) else None,
        "errors": sorted(set(health_errors)),
        "effective_errors": effective_health_errors,
        "maintenance_exemptions_requested": requested_exemptions,
        "maintenance_exemptions_applied": applied_exemptions,
    }
    reports: Dict[str, Dict[str, Any]] = {}
    for name, filename in REQUIRED_REPORTS.items():
        reports[name] = _report(report_dir / filename, now=now, max_age_seconds=max_age_seconds)
    reports["content_source"] = _report(
        content_source_preflight_path,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    release = dict(g9) if g9 is not None else {}
    report_errors = []
    if str(release.get("state") or "") != "released" or str(release.get("migration_epoch") or "") != "v45-r2-20260830":
        report_errors.append("v45_r2_g9_release_evidence_missing")
    if integrity != "ok":
        report_errors.append("database_integrity_not_ok")
    report_errors.extend(validate_snapshot_inputs(namespace_epoch=namespace_epoch, runtime_epoch=runtime_epoch, freeze=freeze, active=active, schema=schema, admission=admission, profile=profile, policies=policies, probes=probes, resolutions=resolutions, watermarks=watermarks, active_generation=active_generation, health=health, reports=reports, now=now, transition_target=transition_target))
    if foundation.get("status") != "PASS":
        report_errors.append("foundation_check_failed")
    report_errors.extend(foundation.get("errors") or [])
    evidence = {
        "runtime_epoch": runtime_epoch,
        "freeze": freeze,
        "release": release,
        "active": active,
        "schema": schema,
        "admission": admission,
        "profile": profile,
        "policies": policies,
        "probes": probes,
        "resolutions": resolutions,
        "watermarks": watermarks,
        "active_generation": active_generation,
        "health": {
            key: value
            for key, value in health.items()
            if key not in {"errors", "effective_errors"}
        },
        "reports": {name: {key: value for key, value in item.items() if key != "payload"} for name, item in reports.items()},
    }
    evidence_hash = _hash(evidence)
    snapshot_id = "bootstrap-" + now.strftime("%Y%m%dT%H%M%SZ") + "-" + hashlib.sha256(evidence_hash.encode("utf-8")).hexdigest()[:16]
    expires_at = now + timedelta(seconds=max(1, int(ttl_seconds)))
    status = "PASS" if not report_errors else "HOLD"
    return {
        "schema": "concept-v11.bootstrap-admission-preflight.v1",
        "status": status,
        "read_only": True,
        "external_provider_calls": 0,
        "production_state_touched": False,
        "concept_admission_changed": False,
        "namespace_epoch": namespace_epoch,
        "transition_target": transition_target,
        "permitted_current_admission_states": sorted(BOOTSTRAP_CURRENT_STATES_BY_TARGET[transition_target]),
        "runtime_epoch": runtime_epoch,
        "admission_snapshot_id": snapshot_id,
        "evidence_hash": evidence_hash,
        "observed_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "current_admission": admission,
        "g9_release": release,
        "foundation": foundation,
        "active": active,
        "schema_v2": schema,
        "profile": profile,
        "policy": policies,
        "capability_probes": [{key: value for key, value in row.items() if key != "response_json"} for row in probes],
        "model_resolutions": resolutions,
        "watermarks": watermarks,
        "active_generation": active_generation,
        "health": health,
        "maintenance_exemptions": {
            "requested": requested_exemptions,
            "applied": applied_exemptions,
            "effective_health_errors": effective_health_errors,
        },
        "reports": {name: {key: value for key, value in item.items() if key != "payload"} for name, item in reports.items()},
        "errors": sorted(set(report_errors)),
        "next_gate": f"由 Codex 以本快照的新短事务 CAS 执行当前状态→{transition_target}；本报告不切换 admission，不执行概念刷新或全量盘点",
    }


def write_report(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--health", type=Path, default=DEFAULT_HEALTH)
    parser.add_argument(
        "--content-source-preflight",
        type=Path,
        default=DEFAULT_CONTENT_SOURCE_PREFLIGHT,
    )
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE)
    parser.add_argument("--max-age-seconds", type=int, default=DEFAULT_MAX_AGE_SECONDS)
    parser.add_argument("--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS)
    parser.add_argument(
        "--transition-target",
        choices=sorted(BOOTSTRAP_CURRENT_STATES_BY_TARGET),
        default="canary",
        help="Admission state this snapshot may authorize; default is disabled-to-canary",
    )
    parser.add_argument(
        "--maintenance-exemption",
        action="append",
        default=[],
        help="explicit maintenance-only exemption id; may be repeated",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        payload = build_snapshot(
            args.db_path.expanduser().resolve(),
            report_dir=args.report_dir.expanduser().resolve(),
            health_path=args.health.expanduser().resolve(),
            content_source_preflight_path=args.content_source_preflight.expanduser().resolve(),
            namespace_epoch=args.namespace_epoch,
            max_age_seconds=args.max_age_seconds,
            ttl_seconds=args.ttl_seconds,
            maintenance_exemptions=args.maintenance_exemption,
            transition_target=args.transition_target,
        )
    except (RuntimeError, sqlite3.Error, OSError) as exc:
        payload = {"schema": "concept-v11.bootstrap-admission-preflight.v1", "status": "HOLD", "read_only": True, "external_provider_calls": 0, "production_state_touched": False, "errors": [f"{type(exc).__name__}:{exc}"]}
    write_report(args.report.expanduser().resolve(), payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
