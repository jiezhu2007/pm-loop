#!/usr/bin/env python3
"""Guarded v1.1 concept admission runner.

The runner is deliberately local and synchronous only for SQLite control
records.  It never calls OneAPI/OpenViking.  A normal invocation is a dry-run:
it creates an independent SQLite backup, performs a restore rehearsal, checks
one short-lived bootstrap snapshot, and reports the exact CAS that would be
attempted.  ``--apply`` is the explicit authorization boundary for one state
transition (for example ``disabled -> canary``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_schema_v2 import set_admission_cas  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11" / "admission"
DEFAULT_NAMESPACE = "v45-r2-20260830"
ALLOWED_STATES = {"disabled", "shadow", "canary", "incremental", "hold"}
ALLOWED_TRANSITIONS = {
    "disabled": {"shadow", "canary", "hold"},
    "shadow": {"canary", "hold", "disabled"},
    "canary": {"incremental", "hold", "disabled"},
    # A same-state transition is permitted only for the audited one-time
    # snapshot-TTL -> continuous policy migration below.
    "incremental": {"incremental", "hold", "disabled"},
    "hold": {"disabled", "shadow", "canary"},
}


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_snapshot(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("admission snapshot must be a JSON object")
    return value


def _read_state(db_path: Path, namespace_epoch: str) -> Dict[str, Any]:
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
        policies = [dict(row) for row in connection.execute("SELECT * FROM concept_model_policies WHERE status='active' ORDER BY policy_version")]
        active: Dict[str, int] = {}
        status_sets = {
            "jobs": ("queued", "running", "processing", "active", "retry_wait"),
            "runs": ("queued", "running", "processing", "active", "retry_wait"),
            "outbox_items": ("pending", "in_flight", "dispatching", "processing", "active", "retry_wait"),
            "semantic_tasks": ("queued", "in_flight", "accepted", "processing", "active", "retry_wait"),
        }
        for table, statuses in status_sets.items():
            marks = ",".join("?" for _ in statuses)
            active[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE status IN ({marks})", statuses).fetchone()[0])
        for name, sql in {
            "slots": "SELECT COUNT(*) FROM execution_slots WHERE status <> 'free'",
            "tokens": "SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL",
            "migration_leases": "SELECT COUNT(*) FROM migration_leases WHERE state='active'",
            "dispatch_leases": "SELECT COUNT(*) FROM outbox_dispatch_leases",
            "probe_leases": "SELECT COUNT(*) FROM provider_probe_leases",
        }.items():
            active[name] = int(connection.execute(sql).fetchone()[0])
    return {
        "integrity": integrity,
        "admission": dict(admission) if admission is not None else None,
        "profile": dict(profile) if profile is not None else None,
        "policies": policies,
        "active": active,
    }


def backup_database(db_path: Path, backup_root: Path) -> Dict[str, Any]:
    """Create a consistent backup and verify it through an independent restore."""
    backup_root = backup_root.expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    os.chmod(backup_root, 0o700)
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    destination = backup_root / f"admission-{stamp}-{uuid.uuid4().hex[:12]}.sqlite3"
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    try:
        with sqlite3.connect(str(db_path.expanduser().resolve()), timeout=10) as source, sqlite3.connect(str(temporary), timeout=10) as target:
            source.backup(target)
            target.commit()
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    with sqlite3.connect(str(destination), timeout=10) as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    restore_root = Path(tempfile.mkdtemp(prefix="concept-v11-admission-restore-", dir="/private/tmp"))
    restore_path = restore_root / "restored.sqlite3"
    shutil.copy2(destination, restore_path)
    try:
        with sqlite3.connect(str(restore_path), timeout=10) as connection:
            restore_integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        shutil.rmtree(restore_root, ignore_errors=True)
    return {
        "path": str(destination),
        "sha256": _sha256(destination),
        "integrity_check": integrity,
        "restore_integrity_check": restore_integrity,
        "verified": integrity == "ok" and restore_integrity == "ok",
    }


def validate_snapshot(
    snapshot: Mapping[str, Any],
    state: Mapping[str, Any],
    *,
    namespace_epoch: str,
    target_state: str = "canary",
    now: Optional[datetime] = None,
) -> list[str]:
    now = now or _now()
    errors: list[str] = []
    if str(snapshot.get("status") or "") != "PASS":
        errors.append("snapshot_not_pass")
    if snapshot.get("read_only") is not True or snapshot.get("concept_admission_changed") is not False:
        errors.append("snapshot_not_read_only")
    if str(snapshot.get("namespace_epoch") or "") != namespace_epoch:
        errors.append("snapshot_namespace_mismatch")
    if not str(snapshot.get("evidence_hash") or ""):
        errors.append("snapshot_evidence_hash_missing")
    observed = _parse_time(snapshot.get("observed_at"))
    expires = _parse_time(snapshot.get("expires_at"))
    if observed is None or expires is None:
        errors.append("snapshot_ttl_missing_or_invalid")
    else:
        if observed > now:
            errors.append("snapshot_observed_in_future")
        if expires <= now:
            errors.append("snapshot_expired")
        if expires <= observed:
            errors.append("snapshot_ttl_invalid")
    admission = state.get("admission") or {}
    snapshot_admission = snapshot.get("current_admission") or {}
    if str(admission.get("namespace_epoch") or "") != namespace_epoch:
        errors.append("admission_namespace_mismatch")
    if str(admission.get("admission_state") or "") != str(snapshot_admission.get("admission_state") or ""):
        errors.append("admission_snapshot_state_mismatch")
    if int(admission.get("version") or 0) != int(snapshot_admission.get("version") or 0):
        errors.append("admission_snapshot_version_mismatch")
    permitted_origins = {
        "canary": {"disabled", "hold"},
        "incremental": {"canary", "incremental"},
    }
    if target_state in permitted_origins and str(admission.get("admission_state") or "") not in permitted_origins[target_state]:
        errors.append("admission_not_safe_for_bootstrap")
    profile = state.get("profile") or {}
    policies = list(state.get("policies") or [])
    if len(policies) != 1:
        errors.append("active_policy_not_unique")
    else:
        policy = policies[0]
        if str(policy.get("provider")) != "oneapi" or str(policy.get("requested_model")) != "auto":
            errors.append("active_policy_not_oneapi_auto")
        if str(profile.get("policy_hash") or "") != str(policy.get("policy_hash") or ""):
            errors.append("profile_policy_hash_mismatch")
        if str(admission.get("policy_version") or "") not in {"", str(policy.get("policy_version") or "")}: 
            errors.append("admission_policy_mismatch")
    if state.get("integrity") != "ok":
        errors.append("database_integrity_not_ok")
    active = state.get("active") or {}
    if any(int(active.get(key, 0)) != 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")):
        errors.append("active_work_or_lease_present")
    return sorted(set(errors))


def run_admission(
    db_path: Path,
    snapshot_path: Path,
    *,
    namespace_epoch: str = DEFAULT_NAMESPACE,
    target_state: str = "canary",
    operator: str = "codex-admission",
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    apply: bool = False,
    now: Optional[datetime] = None,
    renewal_policy: Optional[str] = None,
) -> Dict[str, Any]:
    target_state = str(target_state or "").strip().lower()
    if target_state not in ALLOWED_STATES:
        raise ValueError(f"invalid target state: {target_state}")
    snapshot = _read_snapshot(snapshot_path.expanduser().resolve())
    before = _read_state(db_path.expanduser().resolve(), namespace_epoch)
    backup = backup_database(db_path, backup_root)
    errors = validate_snapshot(snapshot, before, namespace_epoch=namespace_epoch, target_state=target_state, now=now)
    current_state = str((before.get("admission") or {}).get("admission_state") or "")
    current_version = int((before.get("admission") or {}).get("version") or 0)
    if target_state not in ALLOWED_TRANSITIONS.get(current_state, set()):
        errors.append(f"invalid_transition:{current_state}->{target_state}")
    if not operator.strip():
        errors.append("operator_missing")
    effective_renewal_policy = str(
        renewal_policy or ("continuous" if target_state == "incremental" else "snapshot_ttl")
    ).strip().lower()
    if effective_renewal_policy not in {"snapshot_ttl", "continuous"}:
        errors.append("renewal_policy_invalid")
    elif effective_renewal_policy == "continuous" and target_state != "incremental":
        errors.append("renewal_policy_continuous_requires_incremental")
    if current_state == target_state == "incremental":
        current_policy = str((before.get("admission") or {}).get("renewal_policy") or "snapshot_ttl")
        if current_policy == "continuous":
            errors.append("incremental_continuous_already_active")
        if effective_renewal_policy != "continuous":
            errors.append("incremental_renewal_requires_continuous_policy")
    if apply and errors:
        status = "HOLD"
    elif not apply:
        status = "DRY_RUN"
    else:
        policy = (before.get("policies") or [{}])[0]
        snapshot_expires = _parse_time(snapshot.get("expires_at")) or (_now() + timedelta(seconds=900))
        ttl_seconds = max(1, int((snapshot_expires - _now()).total_seconds()))
        try:
            changed = set_admission_cas(
                PMSystemStore(db_path.expanduser().resolve()),
                namespace_epoch=namespace_epoch,
                expected_state=current_state,
                expected_version=current_version,
                state=target_state,
                snapshot_id=str(snapshot["admission_snapshot_id"]),
                policy_version=str(policy.get("policy_version") or snapshot.get("policy_version") or ""),
                operator=operator,
                evidence_hash=str(snapshot["evidence_hash"]),
                reason=(
                    f"v1.1 admission runner {current_state}->{target_state}; "
                    f"renewal_policy={effective_renewal_policy}"
                ),
                ttl_seconds=ttl_seconds,
                renewal_policy=effective_renewal_policy,
            )
        except Exception as exc:  # CAS errors are a durable HOLD, never a retry loop.
            errors.append(f"apply_failed:{type(exc).__name__}:{exc}")
            changed = None
            status = "HOLD"
        else:
            after = _read_state(db_path.expanduser().resolve(), namespace_epoch)
            row = after.get("admission") or {}
            event_ok = int(row.get("version") or 0) == current_version + 1 and str(row.get("admission_state") or "") == target_state
            if not event_ok:
                errors.append("post_readback_mismatch")
            status = "PASS" if not errors else "HOLD"
    result: Dict[str, Any] = {
        "schema": "concept-v11.admission-runner.v1",
        "status": status,
        "apply": bool(apply),
        "external_provider_calls": 0,
        "namespace_epoch": namespace_epoch,
        "target_state": target_state,
        "renewal_policy": effective_renewal_policy,
        "snapshot": {
            "path": str(snapshot_path),
            "sha256": _sha256(snapshot_path.expanduser().resolve()),
            "admission_snapshot_id": snapshot.get("admission_snapshot_id"),
            "evidence_hash": snapshot.get("evidence_hash"),
            "observed_at": snapshot.get("observed_at"),
            "expires_at": snapshot.get("expires_at"),
        },
        "before": before,
        "backup": backup,
        "errors": sorted(set(errors)),
        "rollback": "使用 backup.path 恢复 SQLite；或以同一快照和 CAS 显式切换到 hold/disabled，不自动回滚",
    }
    if apply and "changed" in locals() and changed is not None:
        result["changed"] = changed
        result["after"] = _read_state(db_path.expanduser().resolve(), namespace_epoch)
    else:
        result["would_apply"] = {
            "expected_state": current_state,
            "expected_version": current_version,
            "target_state": target_state,
            "snapshot_id": snapshot.get("admission_snapshot_id"),
        }
    return result


def write_report(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE)
    parser.add_argument("--target-state", choices=sorted(ALLOWED_STATES), default="canary")
    parser.add_argument("--operator", default="codex-admission")
    parser.add_argument(
        "--renewal-policy",
        choices=("snapshot_ttl", "continuous"),
        help="incremental defaults to continuous; canary is always snapshot_ttl",
    )
    parser.add_argument("--apply", action="store_true", help="explicitly apply exactly one CAS transition")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = run_admission(
            args.db_path,
            args.snapshot,
            namespace_epoch=args.namespace_epoch,
            target_state=args.target_state,
            operator=args.operator,
            backup_root=args.backup_root,
            apply=args.apply,
            renewal_policy=args.renewal_policy,
        )
    except (OSError, sqlite3.Error, ValueError, KeyError) as exc:
        result = {
            "schema": "concept-v11.admission-runner.v1",
            "status": "HOLD",
            "apply": bool(args.apply),
            "external_provider_calls": 0,
            "errors": [f"{type(exc).__name__}:{exc}"],
        }
    write_report(args.report, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
