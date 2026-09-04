#!/usr/bin/env python3
"""Create a read-only snapshot for resetting a concept Canary.

This is a safety preflight only.  It never changes admission state and never
calls OneAPI or OpenViking.  The snapshot is intentionally narrower than the
normal bootstrap snapshot: it proves that a live or expired ``canary`` can be
returned to ``disabled`` without active work in flight.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_admission import _parse_time, _read_state  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_HEALTH = Path.home() / ".codex" / "skills" / "system-health-check" / "state" / "latest.json"
DEFAULT_NAMESPACE = "v45-r2-20260830"
DEFAULT_TTL_SECONDS = 600


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _health(path: Path) -> Dict[str, Any]:
    errors: list[str] = []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = None
    if not isinstance(payload, Mapping):
        errors.append("health_marker_missing_or_invalid")
    else:
        checks = payload.get("checks")
        if not isinstance(checks, Mapping):
            errors.append("health_checks_missing")
        else:
            for name, check in checks.items():
                if not isinstance(check, Mapping) or check.get("passed") is not True or check.get("checker_error") is True:
                    errors.append(f"health_check_not_pass:{name}")
    return {
        "path": str(path),
        "status": "PASS" if not errors else "HOLD",
        "run_at": payload.get("run_at") if isinstance(payload, Mapping) else None,
        "checks_total": len(payload.get("checks", {})) if isinstance(payload, Mapping) and isinstance(payload.get("checks"), Mapping) else 0,
        "checks_passed": sum(1 for item in (payload.get("checks", {}).values() if isinstance(payload, Mapping) and isinstance(payload.get("checks"), Mapping) else []) if isinstance(item, Mapping) and item.get("passed") is True),
        "errors": sorted(set(errors)),
    }


def build_snapshot(
    db_path: Path,
    *,
    health_path: Path = DEFAULT_HEALTH,
    namespace_epoch: str = DEFAULT_NAMESPACE,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = (now or _now()).astimezone(timezone.utc)
    db_path = db_path.expanduser().resolve()
    state = _read_state(db_path, namespace_epoch)
    store = PMSystemStore(db_path)
    freeze = store.migration_freeze() or {}
    health = _health(health_path.expanduser().resolve())
    admission = state.get("admission") or {}
    expires = _parse_time(admission.get("expires_at"))
    errors: list[str] = []
    if state.get("integrity") != "ok":
        errors.append("database_integrity_not_ok")
    if str(admission.get("namespace_epoch") or "") != namespace_epoch:
        errors.append("admission_namespace_mismatch")
    if str(admission.get("admission_state") or "") != "canary":
        errors.append(f"admission_not_canary:{admission.get('admission_state') or 'missing'}")
    if expires is None:
        errors.append("admission_ttl_missing_or_invalid")
    active = state.get("active") or {}
    if any(int(active.get(key, 0)) != 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")):
        errors.append("active_work_or_lease_present")
    # Closing admission is a fail-safe action. Unrelated health degradation is
    # preserved in the snapshot but must not keep a no-work canary open.
    reset_mode = "expired_fail_safe" if expires is not None and expires <= now else "manual_abort"
    evidence = {"state": state, "freeze": freeze, "health": health, "purpose": f"{reset_mode}_canary_to_disabled"}
    evidence_hash = _hash(evidence)
    snapshot_id = "reset-" + now.strftime("%Y%m%dT%H%M%SZ") + "-" + hashlib.sha256(evidence_hash.encode("utf-8")).hexdigest()[:16]
    return {
        "schema": "concept-v11.admission-reset-preflight.v1",
        "status": "PASS" if not errors else "HOLD",
        "read_only": True,
        "external_provider_calls": 0,
        "concept_admission_changed": False,
        "namespace_epoch": namespace_epoch,
        "runtime_epoch": str(freeze.get("migration_epoch") or ""),
        "admission_snapshot_id": snapshot_id,
        "evidence_hash": evidence_hash,
        "observed_at": _iso(now),
        "expires_at": _iso(now + timedelta(seconds=max(1, int(ttl_seconds)))),
        "current_admission": admission,
        "active": active,
        "freeze": freeze,
        "health": health,
        "reset_mode": reset_mode,
        "reason": "canary reset to disabled with no concept refresh",
        "errors": sorted(set(errors)),
    }


def write_report(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--health", type=Path, default=DEFAULT_HEALTH)
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE)
    parser.add_argument("--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = build_snapshot(args.db_path, health_path=args.health, namespace_epoch=args.namespace_epoch, ttl_seconds=args.ttl_seconds)
    except (OSError, sqlite3.Error, ValueError) as exc:
        result = {"schema": "concept-v11.admission-reset-preflight.v1", "status": "HOLD", "read_only": True, "external_provider_calls": 0, "concept_admission_changed": False, "errors": [f"{type(exc).__name__}:{exc}"]}
    write_report(args.report, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
