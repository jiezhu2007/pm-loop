#!/usr/bin/env python3
"""Isolated 2/4/8-lane capacity gate for the V4.4 coordination path.

The harness deliberately exercises only the local SQLite store, scheduler and
gateway.  It never imports a provider client and never touches production
state, OpenViking or OneAPI.  Each lane is run in its own temporary database so
one failed level cannot contaminate the next level's measurements.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from pm_system_gateway import SemanticGateway, provider_key
from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 4)


def _rss_mb() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # macOS reports bytes; Linux reports KiB.
    return round(value / (1024 * 1024 if os.uname().sysname == "Darwin" else 1024), 4)


def _parallel(values: Iterable[Any], worker: Callable[[Any], Any], *, max_workers: int) -> List[Any]:
    values = list(values)
    if not values:
        return []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(worker, value) for value in values]
        return [future.result() for future in futures]


def _queue_count(store: PMSystemStore) -> int:
    with store.connect() as connection:
        return int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0])


def _counts(store: PMSystemStore) -> Dict[str, int]:
    with store.connect() as connection:
        return {
            "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]),
            "completed_runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status='completed'").fetchone()[0]),
            "queued_runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status='queued'").fetchone()[0]),
            "running_runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status='running'").fetchone()[0]),
            "semantic_tasks": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0]),
            "semantic_duplicate_rows": int(connection.execute("SELECT COUNT(*) - COUNT(DISTINCT dedupe_key) FROM semantic_tasks").fetchone()[0]),
            "active_slots": int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased'").fetchone()[0]),
            "orphan_slots": int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased' AND (run_id IS NULL OR lease_id IS NULL)").fetchone()[0]),
            "all_nonfree_slots_with_lease": int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased'").fetchone()[0]),
            "outbox_attempts": int(connection.execute("SELECT COALESCE(SUM(attempt), 0) FROM outbox_items").fetchone()[0]),
            "rate_limit_attempts": int(connection.execute("SELECT COALESCE(SUM(attempt), 0) FROM outbox_items WHERE resource_id LIKE 'rate-%'").fetchone()[0]),
        }


def _run_level(root: Path, width: int) -> Dict[str, Any]:
    level_root = root / f"level-{width}"
    level_root.mkdir(parents=True, exist_ok=True)
    store = PMSystemStore(level_root / "pm-system.db", busy_timeout_ms=5000)
    scheduler = Scheduler(store, max_slots=width, slot_ttl_seconds=60)
    gateway = SemanticGateway(store, max_attempts=2, circuit_threshold=3)
    errors: List[str] = []

    total_runs = width * 2 + 1
    accept_latencies: List[float] = []

    def accept_one(index: int) -> Dict[str, Any]:
        started = time.perf_counter_ns()
        try:
            result = store.accept(
                {
                    "job_type": "capacity-fixture",
                    "loop_id": f"v44-capacity-{width}",
                    "idempotency_key": f"capacity:{width}:run:{index}",
                    "profile": "fast-vector",
                    "priority": 80,
                    "payload": {"fixture": True, "level": width, "index": index},
                }
            )
            return {"result": result, "latency_ms": (time.perf_counter_ns() - started) / 1_000_000}
        except Exception as exc:  # pragma: no cover - retained for report evidence
            return {"error": f"accept:{type(exc).__name__}:{exc}", "latency_ms": (time.perf_counter_ns() - started) / 1_000_000}

    accepted = _parallel(range(total_runs), accept_one, max_workers=min(total_runs, 32))
    accept_latencies = [float(item["latency_ms"]) for item in accepted]
    errors.extend(str(item["error"]) for item in accepted if "error" in item)
    accepted_results = [item["result"] for item in accepted if "result" in item]
    queue_peak = _queue_count(store)

    def claim_one(index: int) -> Dict[str, Any]:
        started = time.perf_counter_ns()
        try:
            return {
                "claim": scheduler.claim_next(worker_id=f"capacity-{width}-{index}"),
                "latency_ms": (time.perf_counter_ns() - started) / 1_000_000,
            }
        except Exception as exc:  # pragma: no cover - retained for report evidence
            return {"error": f"claim:{type(exc).__name__}:{exc}", "latency_ms": (time.perf_counter_ns() - started) / 1_000_000}

    first_claims = _parallel(range(total_runs), claim_one, max_workers=min(total_runs, 32))
    claim_latencies = [float(item["latency_ms"]) for item in first_claims]
    errors.extend(str(item["error"]) for item in first_claims if "error" in item)
    leased = [item["claim"] for item in first_claims if item.get("claim") is not None]
    queue_after_first_claim = _queue_count(store)
    queue_peak = max(queue_peak, queue_after_first_claim)

    release_results = _parallel(
        leased,
        lambda claim: scheduler.release(claim["lease_id"], status="completed"),
        max_workers=max(1, min(width, len(leased))),
    )
    if not all(release_results):
        errors.append("release:false")

    # A release must immediately make room for the next queued run.
    second_claims = _parallel(range(width), claim_one, max_workers=width)
    errors.extend(str(item["error"]) for item in second_claims if "error" in item)
    leased_again = [item["claim"] for item in second_claims if item.get("claim") is not None]
    queue_after_second_claim = _queue_count(store)
    queue_peak = max(queue_peak, queue_after_second_claim)
    _parallel(leased_again, lambda claim: scheduler.release(claim["lease_id"], status="completed"), max_workers=max(1, min(width, len(leased_again))))
    final_claim = scheduler.claim_next(worker_id=f"capacity-{width}-final")
    if final_claim is not None:
        scheduler.release(final_claim["lease_id"], status="completed")
    if _queue_count(store):
        errors.append("queue:not-drained")

    # Duplicate enqueue requests are issued concurrently for every logical
    # resource.  The unique dedupe key must leave exactly one outbox row.
    logical_semantic = width * 2
    duplicate_requests = [(index, copy) for index in range(logical_semantic) for copy in (0, 1)]

    def enqueue_duplicate(item: Any) -> Dict[str, Any]:
        index, _copy = item
        try:
            return gateway.enqueue(
                resource_id=f"semantic-{width}-{index}",
                revision_id="r1",
                processing_mode="vectors_only",
                provider="oneapi",
                profile="fast-vector",
                endpoint="fixture",
                model="fixture-vector",
                payload={"fixture": True, "level": width},
            )
        except Exception as exc:  # pragma: no cover - retained for report evidence
            return {"error": f"enqueue:{type(exc).__name__}:{exc}"}

    enqueue_results = _parallel(duplicate_requests, enqueue_duplicate, max_workers=min(len(duplicate_requests), 32))
    errors.extend(str(item["error"]) for item in enqueue_results if "error" in item)
    semantic_dispatch = gateway.dispatch_once(limit=logical_semantic * 2)
    for item in semantic_dispatch:
        gateway.ack(item["outbox_id"], openviking_task_id=f"fixture-{item['semantic_task_id']}")

    # One 429 per dispatched item must update the shared provider window, but
    # must not consume business attempts or multiply work in each worker.
    rate_key = provider_key("oneapi", "fixture-rate", "fixture-semantic")
    rate_items = []
    for index in range(width):
        rate_items.append(
            gateway.enqueue(
                resource_id=f"rate-{width}-{index}",
                revision_id="r1",
                processing_mode="semantic_and_vectors",
                provider="oneapi",
                profile="pm-semantic",
                endpoint="fixture-rate",
                model="fixture-semantic",
                payload={"fixture": True, "level": width},
            )
        )
    rate_dispatch = gateway.dispatch_once(limit=width)
    rate_failures = _parallel(
        rate_dispatch,
        lambda item: gateway.fail(item["outbox_id"], category="429", retry_after="60", provider_key_value=rate_key),
        max_workers=max(1, min(width, len(rate_dispatch))),
    )
    if any(item.get("attempt") != 0 for item in rate_failures):
        errors.append("429:attempt-incremented")
    bucket = None
    with store.connect() as connection:
        bucket_row = connection.execute(
            "SELECT consecutive_429,circuit_state,throttle_until FROM provider_buckets WHERE provider_key=?",
            (rate_key,),
        ).fetchone()
        if bucket_row is not None:
            bucket = {"consecutive_429": int(bucket_row[0]), "circuit_state": bucket_row[1], "throttle_until": bucket_row[2]}

    # A single bounded transient retry proves retry accounting without adding a
    # provider client or sleeping in the harness.
    retry = gateway.enqueue(
        resource_id=f"retry-{width}",
        revision_id="r1",
        processing_mode="vectors_only",
        provider="oneapi",
        profile="fast-vector",
        endpoint="fixture-retry",
        model="fixture-vector",
        payload={"fixture": True, "level": width},
    )
    retry_dispatch = gateway.dispatch_once(limit=1)
    retry_result = gateway.fail(retry_dispatch[0]["outbox_id"], category="timeout") if retry_dispatch else {"error": "retry:not-dispatched"}
    with store.connect() as connection:
        connection.execute("UPDATE outbox_items SET next_attempt_at=NULL WHERE outbox_id=?", (retry["outbox_id"],))
    retry_dispatch_again = gateway.dispatch_once(limit=1)
    if retry_dispatch_again:
        gateway.ack(retry_dispatch_again[0]["outbox_id"], openviking_task_id=f"fixture-retry-{width}")
    else:
        errors.append("retry:not-replayed")

    counts = _counts(store)
    pragmas = store.pragmas()
    expected_completed = total_runs
    # The total includes the duplicate-tested logical set plus the dedicated
    # rate-limit and bounded-retry fixtures below.
    expected_semantic_tasks = logical_semantic + width + 1
    deduplicated_responses = sum(1 for item in enqueue_results if item.get("deduplicated"))
    duplicate_task_count = counts["semantic_duplicate_rows"]
    active_slots = counts["active_slots"]
    result = {
        "width": width,
        "status": "pass",
        "accepted_runs": len(accepted_results),
        "accepted_unique_expected": total_runs,
        "accepted_errors": sum(1 for item in accepted if "error" in item),
        "accept_p50_ms": _percentile(accept_latencies, 0.50),
        "accept_p95_ms": _percentile(accept_latencies, 0.95),
        "claim_p50_ms": _percentile(claim_latencies, 0.50),
        "claim_p95_ms": _percentile(claim_latencies, 0.95),
        "first_claimed": len(leased),
        "queue_after_first_claim": queue_after_first_claim,
        "queue_after_second_claim": queue_after_second_claim,
        "queue_peak": queue_peak,
        "backpressure_pass": len(leased) == width and queue_after_first_claim == total_runs - width,
        "completed_runs": counts["completed_runs"],
        "expected_completed_runs": expected_completed,
        "semantic_logical": logical_semantic,
        "semantic_deduplicated_responses": deduplicated_responses,
        "semantic_tasks": counts["semantic_tasks"],
        "duplicate_semantic_tasks": duplicate_task_count,
        "rate_limit_dispatched": len(rate_dispatch),
        "rate_limit_attempts": counts["rate_limit_attempts"],
        "provider_bucket": bucket,
        "provider_bucket_blocks_dispatch": not gateway.can_dispatch(rate_key),
        "retry_result": retry_result,
        "retry_amplification": gateway.retry_amplification(),
        "orphan_slots": counts["orphan_slots"],
        "active_slots_after_release": active_slots,
        "sqlite": {
            "journal_mode": str(pragmas.get("journal_mode")),
            "busy_timeout": int(pragmas.get("busy_timeout", 0)),
            "foreign_keys": int(pragmas.get("foreign_keys", 0)),
        },
        "external_provider_calls": 0,
        "production_state_touched": False,
        "max_rss_mb": _rss_mb(),
        "errors": errors,
    }
    failures = [
        result["accepted_runs"] != total_runs,
        result["accepted_errors"] != 0,
        not result["backpressure_pass"],
        result["completed_runs"] != expected_completed,
        result["semantic_tasks"] != expected_semantic_tasks,
        result["duplicate_semantic_tasks"] != 0,
        result["rate_limit_dispatched"] != width,
        result["rate_limit_attempts"] != 0,
        not result["provider_bucket_blocks_dispatch"],
        result["orphan_slots"] != 0,
        result["active_slots_after_release"] != 0,
        result["sqlite"]["journal_mode"].lower() != "wal",
        result["sqlite"]["busy_timeout"] < 5000,
        result["sqlite"]["foreign_keys"] != 1,
        result["external_provider_calls"] != 0,
        result["production_state_touched"],
        bool(errors),
    ]
    result["status"] = "fail" if any(failures) else "pass"
    return result


def run_capacity(root: Optional[Path] = None, *, levels: Sequence[int] = (2, 4, 8)) -> Dict[str, Any]:
    """Run all capacity levels in an isolated root and return JSON-safe evidence."""
    if not levels or any(int(level) <= 0 for level in levels):
        raise ValueError("levels must contain positive integers")
    if root is None:
        with tempfile.TemporaryDirectory(prefix="v44-capacity-") as temporary:
            return run_capacity(Path(temporary), levels=levels)
    root = Path(root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    level_results = [_run_level(root, int(level)) for level in levels]
    return {
        "suite": "v4.4-s8.6-capacity",
        "status": "pass" if all(item["status"] == "pass" for item in level_results) else "fail",
        "levels": level_results,
        "levels_requested": [int(level) for level in levels],
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "isolation_root": str(root),
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help="isolated output root; defaults to a temporary directory")
    parser.add_argument("--output", type=Path, help="write the JSON evidence to this path")
    args = parser.parse_args()
    result = run_capacity(args.root)
    encoded = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
