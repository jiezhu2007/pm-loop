#!/usr/bin/env python3
"""Read-only gate for the C6 provider shadow.

This command intentionally does not probe OneAPI or OpenViking.  It reports
whether the local policy and capability evidence are sufficient to request a
separate, explicitly authorized provider-shadow run.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
MODEL_RESOLUTION_GATE = "provider_configuration_trusted"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def evaluate_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate the provider/model contract without making a provider call.

    OneAPI owns model selection when the request is ``auto``.  Therefore an
    empty allowlist is valid for that contract; it means no local model
    pinning is requested, not that the policy is incomplete.  A fixed model
    still requires an explicit allowlist entry.
    """
    provider = str(policy.get("provider") or "")
    requested_model = str(policy.get("requested_model") or "")
    allowed = json.loads(policy.get("allowed_models_json") or "[]")
    if not isinstance(allowed, list):
        allowed = []
    errors: list[str] = []
    if provider != "oneapi" or requested_model != "auto":
        errors.append(f"policy_contract_mismatch:{policy.get('policy_version')}")
    auto_resolution = provider == "oneapi" and requested_model == "auto"
    if not auto_resolution and not allowed:
        errors.append(f"allowlist_empty:{policy.get('policy_version')}")
    return {
        "policy_version": policy.get("policy_version"),
        "provider": provider,
        "requested_model": requested_model,
        "allowlist_count": len(allowed),
        "allowlist_required": not auto_resolution,
        "allowlist_mode": (
            "oneapi_auto" if auto_resolution and not allowed
            else "oneapi_auto_constrained" if auto_resolution
            else "explicit"
        ),
        "auto_provider_resolution": "delegated_to_oneapi" if auto_resolution else "not_applicable",
        "model_resolution_gate": MODEL_RESOLUTION_GATE if auto_resolution else "not_applicable",
        "model_resolution_required": False if auto_resolution else True,
        "model_resolution_gate_status": "not_required" if auto_resolution else "required",
        "status": "PASS" if not errors else "HOLD",
        "errors": errors,
    }


def preflight(db_path: Path) -> Dict[str, Any]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=3)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        policies = [dict(row) for row in connection.execute("SELECT * FROM concept_model_policies ORDER BY policy_version").fetchall()]
        probes = [dict(row) for row in connection.execute("SELECT * FROM concept_capability_probes ORDER BY observed_at").fetchall()]
        admissions = [dict(row) for row in connection.execute("SELECT namespace_epoch,admission_state,version FROM concept_admissions ORDER BY namespace_epoch").fetchall()]
        active = [dict(row) for row in connection.execute(
            "SELECT namespace_epoch,profile,pending_count,pending_soft_limit,outbox_hard_cap,pause_fence,policy_hash FROM concept_profile_admissions"
        ).fetchall()]
    finally:
        connection.close()
    errors = []
    policy_checks = []
    if integrity != "ok":
        errors.append("database_integrity_not_ok")
    if not admissions or any(row.get("admission_state") != "disabled" for row in admissions):
        errors.append("concept_admission_not_disabled")
    if not policies:
        errors.append("model_policy_missing")
    else:
        for policy in policies:
            check = evaluate_policy(policy)
            policy_checks.append(check)
            errors.extend(check["errors"])
    if not probes:
        errors.append("capability_probe_missing")
    return {
        "schema": "concept-v11.c6-provider-shadow-preflight.v1",
        "stage_id": "C6-PROVIDER-SHADOW",
        "observed_at": _now(),
        "status": "PASS" if not errors else "HOLD",
        "external_provider_calls": 0,
        "read_only": True,
        "errors": errors,
        "policy_status": "PASS" if policies and not any(check["errors"] for check in policy_checks) else "HOLD",
        "policy_checks": policy_checks,
        "policies": policies,
        "capability_probes": probes,
        "concept_admissions": admissions,
        "profiles": active,
        "next_gate": "capability probes + accepted/task read-back + isolated namespace/budget + active oneapi/auto policy binding; actual model identity is optional diagnostics",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    payload = preflight(args.db_path.expanduser().resolve())
    report = args.report.expanduser().resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
