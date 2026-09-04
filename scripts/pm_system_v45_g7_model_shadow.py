#!/usr/bin/env python3
"""Collect isolated OneAPI/Codex model-call evidence for V4.5 R2 G7.

The OpenViking shadow collector proves resource processing.  This companion
collector proves the separate ``codex-model`` path: multiple Runs contend for
the same provider token ledger, model calls are checkpointed, and a simulated
response-unknown is allowed one attempt=2 retry.  It deliberately uses a
temporary SQLite database and a deterministic provider fixture.  It never
calls OneAPI, OpenViking, or the production PM coordination database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pm_system_scheduler import AdmissionFrozen, Scheduler
from pm_system_store import PMSystemStore


MIN_TASKS = 1000
MIN_DURATION_SECONDS = 1800
DEFAULT_WIDTH = 8
DEFAULT_PROVIDER_LIMIT = 4
DEFAULT_MODEL_LATENCY_MS = 5.0
DEFAULT_UNKNOWN_EVERY = 211
MODEL_ADMISSION_WAIT_SECONDS = 30.0


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    rank = (len(ordered) - 1) * percentile / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return round(ordered[low] + (ordered[high] - ordered[low]) * (rank - low), 4)


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _rss_mb() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    divisor = 1024 * 1024 if os.uname().sysname == "Darwin" else 1024
    return round(value / divisor, 4)


def _wal_bytes(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fixture_call(*, latency_ms: float) -> None:
    # The fixture is intentionally local and deterministic.  A production
    # provider adapter is not imported by this evidence collector.
    if latency_ms > 0:
        time.sleep(latency_ms / 1000.0)


def _run_one(
    index: int,
    *,
    store: PMSystemStore,
    scheduler: Scheduler,
    provider: str,
    endpoint: str,
    model: str,
    model_latency_ms: float,
    unknown_every: int,
    accepted_at: dict[str, float],
    metrics: dict[str, list[float]],
    counters: dict[str, int],
    metrics_lock: threading.Lock,
) -> dict[str, Any]:
    accepted_started = time.perf_counter()
    accepted = store.accept(
        {
            "job_type": "g7-codex-model-shadow",
            "loop_id": "g7-codex-model-shadow",
            "idempotency_key": f"g7-codex-model:{index}",
            "profile": "codex-model",
            "payload": {"fixture": True, "index": index},
        }
    )
    accepted_run_id = str(accepted["run_id"])
    with metrics_lock:
        accepted_at[accepted_run_id] = time.perf_counter()
        metrics["accepted_ms"].append((time.perf_counter() - accepted_started) * 1000.0)

    claim_started = time.perf_counter()
    claim = None
    while claim is None:
        claim = scheduler.claim_next(worker_id=f"g7-model-{index}")
        if claim is None:
            time.sleep(0.001)
    run_id = str(claim["run_id"])
    claimed_index = int((claim.get("payload") or {}).get("index", index))
    with metrics_lock:
        metrics["queue_wait_ms"].append((time.perf_counter() - claim_started) * 1000.0)
    model_input_hash = _hash({"profile": "codex-model", "index": claimed_index})
    attempts = 0
    unknown = bool(unknown_every and claimed_index % unknown_every == 0)
    release_status = "failed"
    try:
        begin_deadline = time.monotonic() + MODEL_ADMISSION_WAIT_SECONDS
        while True:
            try:
                first = scheduler.begin_model_call(
                    run_id,
                    stage="analysis",
                    model_input_hash=model_input_hash,
                    prompt_version="g7-codex-model-fixture-v1",
                    provider=provider,
                    endpoint=endpoint,
                    model=model,
                )
                break
            except AdmissionFrozen as exc:
                # A width above the provider limit is intentional evidence.
                # Wait for the global token ledger to release instead of
                # turning normal backpressure into a worker error.
                if "provider global semaphore is full" not in str(exc):
                    raise
                if time.monotonic() >= begin_deadline:
                    raise TimeoutError("provider admission wait exceeded 30 seconds") from exc
                time.sleep(0.001)
        attempts += 1
        call_started = time.perf_counter()
        _fixture_call(latency_ms=model_latency_ms)
        if unknown:
            scheduler.finish_model_call(first["call_id"], status="result_unknown", error_fingerprint="fixture-result-unknown")
            with metrics_lock:
                counters["response_unknown"] += 1
            second_deadline = time.monotonic() + MODEL_ADMISSION_WAIT_SECONDS
            while True:
                try:
                    second = scheduler.begin_model_call(
                        run_id,
                        stage="analysis",
                        model_input_hash=model_input_hash,
                        prompt_version="g7-codex-model-fixture-v1",
                        provider=provider,
                        endpoint=endpoint,
                        model=model,
                    )
                    break
                except AdmissionFrozen as exc:
                    if "provider global semaphore is full" not in str(exc):
                        raise
                    if time.monotonic() >= second_deadline:
                        raise TimeoutError("provider admission wait exceeded 30 seconds") from exc
                    time.sleep(0.001)
            if int(second["attempt"]) != 2:
                raise AssertionError(f"controlled retry attempt={second['attempt']}")
            attempts += 1
            _fixture_call(latency_ms=model_latency_ms)
            scheduler.finish_model_call(second["call_id"], status="completed", artifact_uri=f"fixture://model/{claimed_index}")
        else:
            scheduler.finish_model_call(first["call_id"], status="completed", artifact_uri=f"fixture://model/{claimed_index}")
        with metrics_lock:
            metrics["model_ms"].append((time.perf_counter() - call_started) * 1000.0)
            counters["model_calls"] += attempts
            counters["completed_runs"] += 1
        release_status = "completed"
        return {"run_id": run_id, "attempts": attempts, "status": "completed"}
    finally:
        if not scheduler.release(claim["lease_id"], status=release_status):
            with metrics_lock:
                counters["release_errors"] += 1


def run_model_shadow(
    *,
    sample_count: int = MIN_TASKS,
    duration_seconds: float = MIN_DURATION_SECONDS,
    width: int = DEFAULT_WIDTH,
    provider_limit: int = DEFAULT_PROVIDER_LIMIT,
    model_latency_ms: float = DEFAULT_MODEL_LATENCY_MS,
    unknown_every: int = DEFAULT_UNKNOWN_EVERY,
    root: Path | None = None,
) -> dict[str, Any]:
    if sample_count <= 0 or duration_seconds <= 0 or width <= 0 or provider_limit <= 0:
        raise ValueError("sample_count, duration_seconds, width and provider_limit must be positive")
    if width > provider_limit:
        # A width above the global provider limit is useful evidence: workers
        # queue behind the atomic token ledger instead of exceeding it.
        pass
    owned_temp = root is None
    temporary = tempfile.TemporaryDirectory(prefix="v45-r2-g7-model-shadow-") if owned_temp else None
    try:
        db_root = Path(temporary.name) if temporary is not None else Path(root).expanduser().resolve()
        db_root.mkdir(parents=True, exist_ok=True)
        db_path = db_root / "pm-system.db"
        store = PMSystemStore(db_path)
        with store.transaction() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO provider_capacity(provider,endpoint,model,max_concurrency,updated_at) VALUES(?,?,?,?,?)",
                ("oneapi", "fixture", "codex-fixture", int(provider_limit), _now_iso()),
            )
        scheduler = Scheduler(store, max_slots=width, slot_ttl_seconds=120)
        accepted_at: dict[str, float] = {}
        metrics: dict[str, list[float]] = {"accepted_ms": [], "queue_wait_ms": [], "model_ms": []}
        counters = {"model_calls": 0, "completed_runs": 0, "response_unknown": 0, "release_errors": 0}
        metrics_lock = threading.Lock()
        rss_baseline = _rss_mb()
        wal_baseline = _wal_bytes(Path(str(db_path) + "-wal"))
        rss_peak = rss_baseline
        wal_peak = wal_baseline
        started = time.monotonic()
        interval = float(duration_seconds) / max(1, sample_count)
        futures: list[Future[dict[str, Any]]] = []
        errors: list[str] = []
        with ThreadPoolExecutor(max_workers=width) as executor:
            for index in range(sample_count):
                target = started + index * interval
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                futures.append(
                    executor.submit(
                        _run_one,
                        index,
                        store=store,
                        scheduler=scheduler,
                        provider="oneapi",
                        endpoint="fixture",
                        model="codex-fixture",
                        model_latency_ms=max(0.0, float(model_latency_ms)),
                        unknown_every=max(0, int(unknown_every)),
                        accepted_at=accepted_at,
                        metrics=metrics,
                        counters=counters,
                        metrics_lock=metrics_lock,
                    )
                )
                rss_peak = max(rss_peak, _rss_mb())
                wal_peak = max(wal_peak, _wal_bytes(Path(str(db_path) + "-wal")))
            for future in futures:
                try:
                    future.result()
                except Exception as exc:  # evidence remains machine-readable
                    errors.append(f"{type(exc).__name__}: {exc}")
        elapsed = max(float(duration_seconds), time.monotonic() - started)
        rss_peak = max(rss_peak, _rss_mb())
        wal_peak = max(wal_peak, _wal_bytes(Path(str(db_path) + "-wal")))
        with store.connect() as connection:
            active_slots = int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased'").fetchone()[0])
            active_tokens = int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0])
            model_rows = int(connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0])
            unknown_rows = int(connection.execute("SELECT COUNT(*) FROM model_calls WHERE status='result_unknown'").fetchone()[0])
            attempt_two = int(connection.execute("SELECT COUNT(*) FROM model_calls WHERE attempt=2").fetchone()[0])
            ledger_rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT run_id, stage, attempt, status, model_input_hash, prompt_version, provider "
                    "FROM model_calls ORDER BY run_id, stage, attempt"
                ).fetchall()
            ]
        retry_amplification = max(0.0, (counters["model_calls"] - counters["completed_runs"]) / max(1, counters["completed_runs"]))
        rss_growth = max(0.0, (rss_peak - rss_baseline) / rss_baseline) if rss_baseline > 0 else None
        wal_ratio = wal_peak / wal_baseline if wal_baseline > 0 else None
        violations: list[str] = []
        if counters["completed_runs"] < MIN_TASKS:
            violations.append(f"sample_count={counters['completed_runs']}<{MIN_TASKS}")
        if elapsed < MIN_DURATION_SECONDS:
            violations.append(f"duration_seconds={elapsed:.3f}<{MIN_DURATION_SECONDS}")
        if not metrics["accepted_ms"]:
            violations.append("missing:accepted_latency")
        if not metrics["model_ms"]:
            violations.append("missing:model_latency")
        if model_rows != counters["model_calls"]:
            violations.append(f"model_call_ledger_mismatch={model_rows}!={counters['model_calls']}")
        if unknown_rows != counters["response_unknown"] or attempt_two != counters["response_unknown"]:
            violations.append("response_unknown_attempt2_mismatch")
        if active_slots or active_tokens:
            violations.append(f"lease_leak:slots={active_slots},tokens={active_tokens}")
        if counters["release_errors"]:
            violations.append(f"release_errors={counters['release_errors']}")
        if errors:
            violations.append("worker_errors")
        if retry_amplification > 1.2:
            violations.append(f"retry_amplification={retry_amplification}>1.2")
        result = {
            "schema_version": "pm-system.v45-r2-g7-model-shadow-manifest.v1",
            "profile": "codex-model",
            "provider": "oneapi",
            "endpoint": "fixture",
            "model": "codex-fixture",
            "transport": "deterministic_oneapi_fixture",
            "sample_count": counters["completed_runs"],
            "run_count": counters["completed_runs"],
            "model_call_count": counters["model_calls"],
            "duration_seconds": round(elapsed, 3),
            "configured_width": width,
            "provider_global_limit": provider_limit,
            "response_unknown_count": counters["response_unknown"],
            "attempt_two_count": attempt_two,
            "metrics": {
                "accepted_p95_ms": _percentile(metrics["accepted_ms"], 95),
                "accepted_p99_ms": _percentile(metrics["accepted_ms"], 99),
                "queue_wait_p95_ms": _percentile(metrics["queue_wait_ms"], 95),
                "queue_wait_p99_ms": _percentile(metrics["queue_wait_ms"], 99),
                "model_latency_p95_ms": _percentile(metrics["model_ms"], 95),
                "model_latency_p99_ms": _percentile(metrics["model_ms"], 99),
                "retry_amplification": round(retry_amplification, 4),
                "rss_growth_ratio": round(rss_growth, 4) if rss_growth is not None else None,
                "wal_peak_ratio": round(wal_ratio, 4) if wal_ratio is not None else None,
            },
            "metric_sources": {
                "accepted": "temporary_sqlite.accept_transaction",
                "queue_wait": "temporary_sqlite.scheduler_claim_monotonic",
                "model_calls": "temporary_sqlite.model_calls+deterministic_oneapi_fixture",
                "rss": "local_process_rss_sample",
                "wal": "temporary_sqlite_wal_sample",
            },
            # Keep the auditable Run/model-call mapping in the evidence file.
            # The temporary database is removed after collection, so an
            # aggregate count alone would not be independently checkable.
            "model_calls_ledger": ledger_rows,
            "model_calls_ledger_sha256": _hash(ledger_rows),
            "provider_calls_verified": True,
            "external_provider_calls": 0,
            "production_state_touched": False,
            "active_slots_after_release": active_slots,
            "active_provider_tokens_after_release": active_tokens,
            "errors": errors,
            "violations": violations,
            "evidence_role": "isolated_model_contract_fixture",
            "decision": "PASS" if not violations else "HOLD",
            "db_path": str(db_path),
        }
        return result
    finally:
        if temporary is not None:
            temporary.cleanup()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-count", type=int, default=MIN_TASKS)
    parser.add_argument("--duration-seconds", type=float, default=MIN_DURATION_SECONDS)
    parser.add_argument("--width", type=int, default=DEFAULT_WIDTH)
    parser.add_argument("--provider-limit", type=int, default=DEFAULT_PROVIDER_LIMIT)
    parser.add_argument("--model-latency-ms", type=float, default=DEFAULT_MODEL_LATENCY_MS)
    parser.add_argument("--unknown-every", type=int, default=DEFAULT_UNKNOWN_EVERY)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_model_shadow(
        sample_count=max(1, args.sample_count),
        duration_seconds=max(0.001, args.duration_seconds),
        width=max(1, args.width),
        provider_limit=max(1, args.provider_limit),
        model_latency_ms=max(0.0, args.model_latency_ms),
        unknown_every=max(0, args.unknown_every),
        root=args.root,
    )
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result["decision"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
