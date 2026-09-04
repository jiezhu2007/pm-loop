#!/usr/bin/env python3
"""V4.5 R2 G7 host performance gate.

The gate is intentionally evidence-first.  It may run the isolated SQLite
capacity harness as a supplemental regression, but it cannot pass without a
separately recorded host OpenViking shadow manifest that meets the absolute
sample and duration requirements in the V4.5 design.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Sequence


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
DEFAULT_DB = CODEX_ROOT / "pm-loop/state/pm-system.db"
DEFAULT_MANIFEST = PROJECT_ROOT / "docs/03-产品架构/v4.5实施报告/g7-performance-manifest.json"
DEFAULT_SHADOW_MANIFEST = CODEX_ROOT / "pm-loop/state/v45-r2-g7/shadow-manifest.json"
DEFAULT_MODEL_SHADOW_MANIFEST = CODEX_ROOT / "pm-loop/state/v45-r2-g7/model-shadow-manifest.json"
DEFAULT_BASELINE_MANIFEST = PROJECT_ROOT / "docs/03-产品架构/v4.5实施报告/g7.1-性能归因与阈值校准-manifest.json"
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
OV_REST = CODEX_ROOT / "skills/openviking-rest/scripts/ov_rest.py"
BASELINE_AUTHORIZATION_ID = "v45-g7-baseline-20260830-01"

MIN_TASKS = 1000
MIN_DURATION_SECONDS = 30 * 60
MIN_RESOURCES = 100
MIN_RESOURCE_BYTES = 10 * 1024
MINIMUM_POLICY = "all"
_UNAVAILABLE_SOURCES = {
    "",
    "unknown",
    "unavailable",
    "not_collected",
    "not_exposed",
    "not_exposed_by_openviking_api",
    "estimated",
    "inferred",
    "task_state_poll_resolution",
}
THRESHOLDS = {
    "accepted_p95_ms": 100.0,
    "accepted_p99_ms": 250.0,
    "queue_wait_p95_s": 2.0,
    "queue_wait_p99_s": 10.0,
    "queue_wait_max_s": 30.0,
    "content_verified_p95_s": 120.0,
    "content_verified_p99_s": 300.0,
    "memory_link_lag_p95_s": 300.0,
    "memory_link_lag_p99_s": 900.0,
    "lock_wait_p95_ms": 250.0,
    "lock_wait_max_ms": 2000.0,
    "rss_growth_ratio": 0.30,
    "wal_peak_ratio": 2.0,
    "retry_amplification": 1.2,
}

# The user-visible resource path keeps the base thresholds above.  Semantic
# model inference is a separate background projection: its provider latency
# may be substantially longer than local acceptance or content persistence,
# but it still needs a bounded deadline.  Keep this map explicit so a slow
# model cannot silently relax queue, lock, memory, or retry gates.
PROFILE_THRESHOLDS = {
    "pm-semantic": {
        "semantic_model_latency_p95_s": 120.0,
        "semantic_model_latency_p99_s": 600.0,
        "semantic_model_latency_max_s": 900.0,
    },
}


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _read_freeze(db_path: Path) -> dict[str, Any] | None:
    import sqlite3

    try:
        connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=3)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute("SELECT * FROM migration_freeze WHERE freeze_id=1").fetchone()
            return dict(row) if row else None
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return None


def _health(*, execute: bool) -> dict[str, Any]:
    if not execute:
        return {"status": "dry_run", "healthy": None}
    if not OV_REST.is_file():
        return {"status": "missing_wrapper", "healthy": False}
    started = time.perf_counter()
    try:
        result = subprocess.run([CANONICAL_PYTHON, str(OV_REST), "health"], capture_output=True, text=True, timeout=20)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError):
            payload = {"raw": result.stdout[-500:]}
        payload.update({"returncode": result.returncode, "elapsed_ms": elapsed_ms, "status": "healthy" if result.returncode == 0 and payload.get("healthy") else "unhealthy"})
        if result.stderr:
            payload["stderr"] = result.stderr[-500:]
        return payload
    except (OSError, subprocess.SubprocessError) as exc:
        return {"status": "probe_failed", "healthy": False, "error": f"{type(exc).__name__}: {exc}"}


def _isolated_capacity() -> dict[str, Any]:
    # Import lazily so a check-only invocation does not touch production state.
    from pm_system_capacity import run_capacity

    with tempfile.TemporaryDirectory(prefix="v45-r2-g7-capacity-") as temporary:
        result = run_capacity(Path(temporary), levels=(2, 4, 8))
        return {
            "status": result.get("status"),
            "levels": result.get("levels"),
            "elapsed_ms": result.get("elapsed_ms"),
            "production_state_touched": result.get("production_state_touched"),
            "external_provider_calls": result.get("external_provider_calls"),
            "evidence_role": "supplemental_only",
        }


def _number(item: dict[str, Any], key: str, *, integer: bool = False) -> tuple[float | int | None, str | None]:
    """Parse a manifest number without allowing null/NaN to become zero."""
    if key not in item or item.get(key) is None or item.get(key) == "":
        return None, f"missing:{key}"
    value = item.get(key)
    if isinstance(value, bool):
        return None, f"invalid:{key}"
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None, f"invalid:{key}"
    if not math.isfinite(parsed) or (integer and not parsed.is_integer()):
        return None, f"invalid:{key}"
    return (int(parsed) if integer else parsed), None


def _source_issue(sources: Any, key: str, *, queue: bool = False) -> str | None:
    if not isinstance(sources, dict) or key not in sources:
        return f"missing:{key}_source"
    source = sources.get(key)
    if not isinstance(source, str) or not source.strip():
        return f"invalid:{key}_source"
    normalized = source.strip().lower()
    if normalized in _UNAVAILABLE_SOURCES or any(token in normalized for token in ("unavailable", "not_collected", "not_exposed", "estimated", "inferred")):
        return f"invalid:{key}_source"
    if queue and (
        normalized == "mixed_explicit_telemetry"
        or not normalized.startswith(("telemetry.", "shadow_worker.", "queue_db."))
    ):
        return f"invalid:{key}_source"
    return None


def _shadow_checks(shadow: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    profiles = shadow.get("profiles") if isinstance(shadow.get("profiles"), dict) else {}
    minimums = shadow.get("minimums")
    if not isinstance(minimums, dict):
        checks.append({"name": "shadow manifest minimums", "status": "HOLD", "detail": "missing:minimums"})
    elif minimums.get("policy") != MINIMUM_POLICY:
        checks.append({"name": "shadow manifest minimums", "status": "HOLD", "detail": f"invalid:minimums.policy={minimums.get('policy')!r}"})
    else:
        checks.append({"name": "shadow manifest minimums", "status": "PASS", "detail": json.dumps(minimums, ensure_ascii=False)})

    for profile in ("fast-vector", "pm-semantic", "memory-link", "codex-model"):
        item = profiles.get(profile) if isinstance(profiles, dict) else None
        if not isinstance(item, dict):
            checks.append({"name": f"shadow profile {profile}", "status": "SKIPPED/HOLD" if profile == "memory-link" else "HOLD", "detail": "G4 已确认无独立 MemoryLink API；不伪造 linking 样本" if profile == "memory-link" else "缺少宿主机 OpenViking shadow manifest"})
            continue
        if profile == "memory-link" and item.get("skipped") is True:
            checks.append({"name": f"shadow profile {profile}", "status": "SKIPPED/HOLD", "detail": "G4 已确认无独立 MemoryLink API；不伪造 linking 样本"})
            continue

        violations: list[str] = []
        not_applicable: list[str] = []
        values: dict[str, float | int] = {}
        for key in ("sample_count", "resource_count", "min_resource_bytes"):
            value, issue = _number(item, key, integer=True)
            if issue:
                violations.append(issue)
            elif value is not None:
                values[key] = value
        duration, issue = _number(item, "duration_seconds")
        if issue:
            violations.append(issue)
        elif duration is not None:
            values["duration_seconds"] = duration
        samples = int(values.get("sample_count", 0))
        duration_value = float(values.get("duration_seconds", 0))
        resources = int(values.get("resource_count", 0))
        min_bytes = int(values.get("min_resource_bytes", 0))
        valid_samples = (
            samples >= MIN_TASKS
            and duration_value >= MIN_DURATION_SECONDS
            and resources >= MIN_RESOURCES
            and min_bytes >= MIN_RESOURCE_BYTES
        ) and not any(issue.startswith(("missing:sample_count", "invalid:sample_count", "missing:duration_seconds", "invalid:duration_seconds", "missing:resource_count", "invalid:resource_count", "missing:min_resource_bytes", "invalid:min_resource_bytes")) for issue in violations)

        processing_mode = item.get("processing_mode")
        if processing_mode not in {"vectors_only", "semantic_and_vectors"}:
            violations.append("missing:processing_mode" if processing_mode in (None, "") else "invalid:processing_mode")
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        metric_item = {**item, **metrics}
        applicable_thresholds = dict(THRESHOLDS)
        applicable_thresholds.update(PROFILE_THRESHOLDS.get(profile, {}))
        for key, limit in applicable_thresholds.items():
            if profile != "memory-link" and key.startswith("memory_link_lag_"):
                not_applicable.append(key)
                continue
            value, issue = _number(metric_item, key)
            if issue:
                violations.append(issue)
                continue
            assert value is not None
            if key.endswith("growth_ratio") or key.endswith("wal_peak_ratio") or key == "retry_amplification":
                exceeded = value > limit
            elif "max" in key:
                exceeded = value > limit
            else:
                exceeded = value > limit
            if exceeded:
                violations.append(f"{key}={value}>{limit}")

        sources = item.get("metric_sources")
        for key in ("accepted", "content_verified", "lock_wait", "rss", "wal"):
            source_issue = _source_issue(sources, key)
            if source_issue:
                violations.append(source_issue)
        if profile == "pm-semantic":
            source_issue = _source_issue(sources, "semantic_model_latency")
            if source_issue:
                violations.append(source_issue)
        else:
            not_applicable.append("semantic_model_latency")
        queue_source_issue = _source_issue(sources, "queue_wait", queue=True)
        if queue_source_issue:
            violations.append(queue_source_issue)
        missing_count, count_issue = _number({"queue_wait_missing_count": item.get("queue_wait_missing_count", 0)}, "queue_wait_missing_count", integer=True)
        if count_issue:
            violations.append(count_issue)
        elif missing_count:
            violations.append("missing:queue_wait_samples")
        queue_source = sources.get("queue_wait") if isinstance(sources, dict) else None
        content_source = sources.get("content_verified") if isinstance(sources, dict) else None
        if queue_source and content_source and queue_source == content_source:
            violations.append("invalid:content_verified_source_not_independent")
        provider_verified = item.get("provider_calls_verified")
        if provider_verified is not True:
            violations.append("invalid:provider_calls_verified")
        status = "PASS" if valid_samples and not violations else "HOLD"
        checks.append({"name": f"shadow profile {profile}", "status": status, "detail": json.dumps({"sample_count": samples, "duration_seconds": duration_value, "resource_count": resources, "min_resource_bytes": min_bytes, "violations": violations, "not_applicable": not_applicable, "metric_sources": sources, "provider_calls_verified": provider_verified}, ensure_ascii=False)})
    return checks


def _model_shadow_check(path: Path) -> dict[str, Any]:
    """Require an independent Run/model-call ledger for ``codex-model``.

    An OpenViking ``semantic_and_vectors`` task proves resource processing, but
    it is not evidence that multiple Codex Runs exercised the OneAPI model
    admission path.  The companion model shadow may use a deterministic
    OneAPI fixture; it must still expose the same durable attempt/token
    invariants and remain isolated from production state.
    """
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"name": "codex-model OneAPI model-call shadow", "status": "HOLD", "detail": f"missing:model_shadow_manifest={path}"}
    if not isinstance(manifest, dict):
        return {"name": "codex-model OneAPI model-call shadow", "status": "HOLD", "detail": "invalid:model_shadow_manifest"}
    violations: list[str] = []
    required = (
        "sample_count",
        "run_count",
        "model_call_count",
        "duration_seconds",
        "provider_calls_verified",
        "external_provider_calls",
        "production_state_touched",
        "metric_sources",
        "metrics",
        "model_calls_ledger",
        "model_calls_ledger_sha256",
    )
    for key in required:
        if key not in manifest or manifest.get(key) is None:
            violations.append(f"missing:model_shadow.{key}")
    for key, minimum in (("sample_count", MIN_TASKS), ("run_count", MIN_TASKS)):
        value, issue = _number(manifest, key, integer=True)
        if issue:
            violations.append(f"model_shadow.{issue}")
        elif value is not None and value < minimum:
            violations.append(f"model_shadow.{key}={value}<{minimum}")
    duration, issue = _number(manifest, "duration_seconds")
    if issue:
        violations.append(f"model_shadow.{issue}")
    elif duration is not None and duration < MIN_DURATION_SECONDS:
        violations.append(f"model_shadow.duration_seconds={duration}<{MIN_DURATION_SECONDS}")
    model_calls, issue = _number(manifest, "model_call_count", integer=True)
    runs, _ = _number(manifest, "run_count", integer=True)
    if issue:
        violations.append(f"model_shadow.{issue}")
    elif model_calls is not None and runs is not None and model_calls < runs:
        violations.append("model_shadow.model_call_count_less_than_run_count")
    if manifest.get("provider_calls_verified") is not True:
        violations.append("invalid:model_shadow.provider_calls_verified")
    if manifest.get("external_provider_calls") != 0:
        violations.append("invalid:model_shadow.external_provider_calls")
    if manifest.get("production_state_touched") is not False:
        violations.append("invalid:model_shadow.production_state_touched")
    sources = manifest.get("metric_sources")
    source = sources.get("model_calls") if isinstance(sources, dict) else None
    if not isinstance(source, str) or not source.strip() or "model_calls" not in source:
        violations.append("missing:model_shadow.model_calls_source")
    metrics = manifest.get("metrics") if isinstance(manifest.get("metrics"), dict) else {}
    retry, retry_issue = _number(metrics, "retry_amplification")
    if retry_issue:
        violations.append(f"model_shadow.{retry_issue}")
    elif retry is not None and retry > THRESHOLDS["retry_amplification"]:
        violations.append(f"model_shadow.retry_amplification={retry}>{THRESHOLDS['retry_amplification']}")
    unknown, unknown_issue = _number(manifest, "response_unknown_count", integer=True)
    attempt_two, attempt_issue = _number(manifest, "attempt_two_count", integer=True)
    if unknown_issue or attempt_issue:
        violations.append("missing:model_shadow.attempt_two_evidence")
    elif unknown != attempt_two:
        violations.append(f"model_shadow.attempt_two_count={attempt_two}!={unknown}")

    ledger = manifest.get("model_calls_ledger")
    if not isinstance(ledger, list):
        violations.append("invalid:model_shadow.model_calls_ledger")
    else:
        canonical_ledger: list[dict[str, Any]] = []
        run_stages: dict[tuple[str, str], list[int]] = {}
        ledger_unknown = 0
        ledger_attempt_two = 0
        run_ids: set[str] = set()
        for index, row in enumerate(ledger):
            if not isinstance(row, dict):
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}]")
                continue
            run_id = row.get("run_id")
            stage = row.get("stage")
            status = row.get("status")
            model_input_hash = row.get("model_input_hash")
            attempt_value, attempt_value_issue = _number(row, "attempt", integer=True)
            if not isinstance(run_id, str) or not run_id.strip():
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}].run_id")
                continue
            if not isinstance(stage, str) or not stage.strip():
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}].stage")
                continue
            if not isinstance(status, str) or not status.strip():
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}].status")
                continue
            if not isinstance(model_input_hash, str) or not model_input_hash.strip():
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}].model_input_hash")
                continue
            if attempt_value_issue or attempt_value is None or int(attempt_value) < 1:
                violations.append(f"invalid:model_shadow.model_calls_ledger[{index}].attempt")
                continue
            attempt_int = int(attempt_value)
            run_id = run_id.strip()
            stage = stage.strip()
            status = status.strip()
            model_input_hash = model_input_hash.strip()
            run_ids.add(run_id)
            run_stages.setdefault((run_id, stage), []).append(attempt_int)
            if status == "result_unknown":
                ledger_unknown += 1
            if attempt_int == 2:
                ledger_attempt_two += 1
            canonical_ledger.append(
                {
                    "run_id": run_id,
                    "stage": stage,
                    "attempt": attempt_int,
                    "status": status,
                    "model_input_hash": model_input_hash,
                    "prompt_version": row.get("prompt_version"),
                    "provider": row.get("provider"),
                }
            )
        if model_calls is not None and len(ledger) != int(model_calls):
            violations.append(f"model_shadow.model_calls_ledger_count={len(ledger)}!={int(model_calls)}")
        if runs is not None and len(run_ids) != int(runs):
            violations.append(f"model_shadow.model_calls_run_count={len(run_ids)}!={int(runs)}")
        if unknown is not None and ledger_unknown != int(unknown):
            violations.append(f"model_shadow.ledger_unknown_count={ledger_unknown}!={int(unknown)}")
        if attempt_two is not None and ledger_attempt_two != int(attempt_two):
            violations.append(f"model_shadow.ledger_attempt_two_count={ledger_attempt_two}!={int(attempt_two)}")
        for key, attempts in run_stages.items():
            ordered = sorted(attempts)
            expected = list(range(1, len(ordered) + 1))
            if ordered != expected:
                violations.append(f"invalid:model_shadow.attempt_sequence={key[0]}/{key[1]}:{ordered}")
        digest = manifest.get("model_calls_ledger_sha256")
        if digest != _hash(canonical_ledger):
            violations.append("invalid:model_shadow.model_calls_ledger_sha256")
    for key in ("active_slots_after_release", "active_provider_tokens_after_release"):
        value, value_issue = _number(manifest, key, integer=True)
        if value_issue or value != 0:
            violations.append(f"invalid:model_shadow.{key}" if value_issue is None else f"model_shadow.{value_issue}")
    return {
        "name": "codex-model OneAPI model-call shadow",
        "status": "PASS" if not violations else "HOLD",
        "detail": json.dumps({"path": str(path), "violations": violations, "evidence_role": manifest.get("evidence_role"), "transport": manifest.get("transport")}, ensure_ascii=False),
    }


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read a JSON object for evidence reporting without turning bad evidence into a crash."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _file_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _baseline_acceptance(*, path: Path, shadow_manifest: Path, strict_checks: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Validate the explicit user-approved performance baseline exception.

    The exception can defer profile performance optimization, but it cannot
    waive the migration freeze, host health, or production-isolation checks.
    Every strict HOLD must be named so new failures cannot silently inherit an
    old baseline decision.
    """
    manifest = _read_json_object(path)
    violations: list[str] = []
    required_values = {
        "schema_version": "pm-system.v45-r4-g7-baseline-acceptance.v1",
        "authorization_id": BASELINE_AUTHORIZATION_ID,
        "decision": "ACCEPTED_BASELINE",
        "baseline_complete": True,
        "performance_issues_recorded": True,
        "optimization_deferred": True,
        "allow_next_stage": True,
        "production_state_touched": False,
        "external_provider_calls": 0,
        "does_not_authorize_concurrency_increase": True,
    }
    for key, expected in required_values.items():
        if manifest.get(key) != expected:
            violations.append(f"invalid:baseline.{key}")

    expected_shadow_hash = _file_sha256(shadow_manifest)
    if not expected_shadow_hash or manifest.get("source_shadow_sha256") != expected_shadow_hash:
        violations.append("invalid:baseline.source_shadow_sha256")

    non_waivable = {
        "persistent G7 freeze",
        "host OpenViking health",
        "isolated SQLite capacity supplemental",
        "shadow manifest minimums",
    }
    by_name = {str(item.get("name")): str(item.get("status")) for item in strict_checks}
    for name in non_waivable:
        if by_name.get(name) != "PASS":
            violations.append(f"non_waivable:{name}")

    baseline_eligible = {
        "shadow profile fast-vector",
        "shadow profile pm-semantic",
        "shadow profile codex-model",
        "codex-model OneAPI model-call shadow",
    }
    strict_holds = {name for name, status in by_name.items() if status == "HOLD"}
    unexpected_holds = strict_holds - baseline_eligible
    if unexpected_holds:
        violations.append("non_waivable_holds:" + ",".join(sorted(unexpected_holds)))
    accepted_names = manifest.get("accepted_check_names")
    if not isinstance(accepted_names, list) or set(map(str, accepted_names)) != strict_holds:
        violations.append("invalid:baseline.accepted_check_names")
    deviations = manifest.get("accepted_deviations")
    if not isinstance(deviations, list) or not deviations or not all(isinstance(item, dict) and item.get("issue") and item.get("follow_up") for item in deviations):
        violations.append("invalid:baseline.accepted_deviations")

    return {
        "status": "PASS" if not violations else "HOLD",
        "path": str(path),
        "authorization_id": manifest.get("authorization_id"),
        "source_shadow_sha256": expected_shadow_hash,
        "accepted_check_names": sorted(strict_holds),
        "violations": violations,
        "scope": "G7 progression only; performance optimization deferred",
    }


def run_g7(*, db_path: Path = DEFAULT_DB, shadow_manifest: Path = DEFAULT_SHADOW_MANIFEST, model_shadow_manifest: Path = DEFAULT_MODEL_SHADOW_MANIFEST, baseline_manifest: Path | None = None, manifest_path: Path = DEFAULT_MANIFEST, execute: bool = True) -> dict[str, Any]:
    freeze = _read_freeze(db_path)
    checks: list[dict[str, Any]] = []
    freeze_ok = bool(freeze and freeze.get("migration_id") == "v45-r2-20260830" and freeze.get("migration_epoch") == "v45-r2-20260830" and freeze.get("stage_id") == "G7" and freeze.get("state") == "freeze")
    checks.append({"name": "persistent G7 freeze", "status": "PASS" if freeze_ok else "HOLD", "detail": json.dumps(freeze or {}, ensure_ascii=False)})
    health = _health(execute=execute)
    checks.append({"name": "host OpenViking health", "status": "PASS" if health.get("healthy") is True else ("HOLD" if execute else "SKIPPED"), "detail": json.dumps(health, ensure_ascii=False)})
    isolated = _isolated_capacity() if execute else {"status": "not_run", "evidence_role": "supplemental_only"}
    checks.append({"name": "isolated SQLite capacity supplemental", "status": "PASS" if isolated.get("status") == "pass" and not isolated.get("production_state_touched") and isolated.get("external_provider_calls") == 0 else ("HOLD" if execute else "SKIPPED"), "detail": "不能替代真实 OpenViking shadow"})
    shadow = _read_json_object(shadow_manifest)
    model_shadow = _read_json_object(model_shadow_manifest.expanduser().resolve())
    checks.extend(_shadow_checks(shadow))
    checks.append(_model_shadow_check(model_shadow_manifest.expanduser().resolve()))
    strict_decision = "PASS" if checks and all(item["status"] == "PASS" or (item["name"] == "shadow profile memory-link" and item["status"] == "SKIPPED/HOLD") for item in checks) else "HOLD"
    baseline_acceptance = (
        _baseline_acceptance(path=baseline_manifest.expanduser().resolve(), shadow_manifest=shadow_manifest.expanduser().resolve(), strict_checks=checks)
        if baseline_manifest is not None
        else {"status": "NOT_REQUESTED", "violations": []}
    )
    decision = "PASS" if strict_decision == "PASS" else ("PASS_WITH_BASELINE" if baseline_acceptance.get("status") == "PASS" else "HOLD")
    result = {
        "schema_version": "pm-system.v45-r4-g7-performance-manifest.v2",
        "stage_id": "G7",
        "migration_id": "v45-r2-20260830",
        "migration_epoch": "v45-r2-20260830",
        "freeze": freeze,
        "health": health,
        "isolated_capacity": isolated,
        "shadow_manifest": str(shadow_manifest),
        "shadow_manifest_hash": _hash(shadow) if shadow else None,
        "model_shadow_manifest": str(model_shadow_manifest),
        "model_shadow_manifest_hash": _hash(model_shadow) if model_shadow else None,
        "baseline_manifest": str(baseline_manifest) if baseline_manifest is not None else None,
        "strict_decision": strict_decision,
        "baseline_acceptance": baseline_acceptance,
        "thresholds": THRESHOLDS,
        "profile_thresholds": PROFILE_THRESHOLDS,
        "minimums": {"policy": MINIMUM_POLICY, "tasks": MIN_TASKS, "duration_seconds": MIN_DURATION_SECONDS, "resources": MIN_RESOURCES, "resource_bytes": MIN_RESOURCE_BYTES},
        "checks": checks,
        "production_state_touched": False,
        "external_provider_calls": 0,
        "memory_link": {"status": "SKIPPED/HOLD", "reason": "OpenViking 0.4.16 未提供独立 MemoryLink API"},
        "decision": decision,
    }
    manifest_path = manifest_path.expanduser().resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--shadow-manifest", type=Path, default=DEFAULT_SHADOW_MANIFEST)
    parser.add_argument("--model-shadow-manifest", type=Path, default=DEFAULT_MODEL_SHADOW_MANIFEST)
    parser.add_argument("--baseline-manifest", type=Path, help="explicit accepted performance baseline; does not waive health or freeze checks")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_g7(
        db_path=args.db_path,
        shadow_manifest=args.shadow_manifest,
        model_shadow_manifest=args.model_shadow_manifest,
        baseline_manifest=args.baseline_manifest,
        manifest_path=args.manifest,
        execute=not args.check_only,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["decision"] in {"PASS", "PASS_WITH_BASELINE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
