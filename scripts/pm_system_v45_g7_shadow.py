#!/usr/bin/env python3
"""Collect real OpenViking G7 shadow evidence.

The collector writes only to an explicitly supplied non-production namespace.
It records submission, task-state and completion observations but never writes
the PM coordination database or uses the PM Resource Outbox.  A profile is
not considered complete unless its requested sample count and wall-clock
window are both reached; missing service-level metrics remain explicit so the
G7 runner can keep the gate on HOLD instead of inventing values.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import sqlite3
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
OV_REST = CODEX_ROOT / "skills/openviking-rest/scripts/ov_rest.py"
DEFAULT_MANIFEST = CODEX_ROOT / "pm-loop/state/v45-r2-g7/shadow-manifest.json"
DEFAULT_NAMESPACE = "viking://resources/v45-r2-g7-shadow"
DEFAULT_QUEUE_DB = Path.home() / ".openviking/data/_system/queue/queue.db"
MIN_TASKS = 1000
MIN_DURATION_SECONDS = 1800
MIN_RESOURCES = 100
MIN_RESOURCE_BYTES = 10 * 1024
POLL_SECONDS = 1.0
QUEUE_SAMPLE_SECONDS = 0.25
HOST_SAMPLE_SECONDS = 5.0
SUBMIT_WORKERS = 8
MAX_DRAIN_SECONDS = 300


def _load_ov_rest() -> Any:
    spec = importlib.util.spec_from_file_location("v45_g7_ov_rest", OV_REST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load OpenViking REST wrapper: {OV_REST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    rank = (len(ordered) - 1) * percentile / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    value = ordered[low] + (ordered[high] - ordered[low]) * (rank - low)
    return round(value, 4)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _timestamp_from_payload(payload: Any) -> tuple[float | None, str | None]:
    """Return an explicit dequeue/worker-start timestamp if the API exposes one.

    OpenViking 0.4.16 normally exposes only created/updated timestamps and task
    state.  Those fields are deliberately excluded here: an update or a poll
    observation is not evidence that a worker dequeued the task.
    """
    if not isinstance(payload, dict):
        return None, None
    result = payload.get("result")
    candidates: list[tuple[str, Any]] = []
    for container in (payload, result if isinstance(result, dict) else None):
        if not isinstance(container, dict):
            continue
        for key in (
            "dequeued_at",
            "dequeue_at",
            "worker_started_at",
            "worker_start_at",
            "processing_started_at",
        ):
            if key in container:
                candidates.append((key, container[key]))
        telemetry = container.get("telemetry")
        if isinstance(telemetry, dict):
            for key in (
                "dequeued_at",
                "dequeue_at",
                "worker_started_at",
                "worker_start_at",
                "processing_started_at",
            ):
                if key in telemetry:
                    candidates.append((f"telemetry.{key}", telemetry[key]))
    for source, value in candidates:
        try:
            if isinstance(value, str):
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                timestamp = parsed.timestamp()
            else:
                timestamp = float(value)
            if timestamp > 0:
                return timestamp, source
        except (TypeError, ValueError, OverflowError):
            continue
    return None, None


def _timestamp_value(value: Any) -> float | None:
    """Parse QueueFS timestamps without treating malformed values as evidence."""
    try:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            timestamp = parsed.timestamp()
        else:
            timestamp = float(value)
        # QueueFS currently stores epoch seconds; accept milliseconds for a
        # forward-compatible read path without changing the stored evidence.
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return timestamp if timestamp > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _mapping(value: Any) -> dict[str, Any]:
    """Return a JSON object or an empty mapping for partial task responses."""
    return value if isinstance(value, dict) else {}


def _counter(value: Any) -> int | None:
    """Parse a non-negative queue counter without failing the task probe."""
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _extract_task_payloads(value: Any) -> list[dict[str, Any]]:
    """Find payload dictionaries containing task IDs in a QueueFS row."""
    if isinstance(value, str):
        try:
            return _extract_task_payloads(json.loads(value))
        except (TypeError, ValueError):
            return []
    if isinstance(value, dict):
        payloads: list[dict[str, Any]] = []
        if value.get("task_id"):
            payloads.append(value)
        for child in value.values():
            payloads.extend(_extract_task_payloads(child))
        return payloads
    if isinstance(value, (list, tuple)):
        if value and all(isinstance(item, int) and 0 <= item <= 255 for item in value):
            try:
                return _extract_task_payloads(bytes(value).decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                pass
        payloads: list[dict[str, Any]] = []
        for child in value:
            payloads.extend(_extract_task_payloads(child))
        return payloads
    return []


def _read_queue_processing_starts(
    queue_db: Path = DEFAULT_QUEUE_DB,
    *,
    namespace: str | None = None,
    profile: str | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str], int, int]:
    """Read active QueueFS processing starts without acquiring a write lock.

    Completed rows may already have been ACKed and removed by QueueFS. Such a
    task is intentionally absent from the returned mapping; callers must keep
    its queue wait as ``unknown`` instead of deriving it from completion time.
    """
    starts: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    row_count = 0
    scope_unmatched_count = 0
    path = queue_db.expanduser().resolve()
    if not path.is_file():
        return starts, ["queue_db_missing"], row_count, scope_unmatched_count
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True, timeout=0.2) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT queue_name, message_id, data, processing_started_at, status, created_at "
                "FROM queue_messages WHERE processing_started_at IS NOT NULL"
            ).fetchall()
            row_count = len(rows)
            for row in rows:
                started_at = _timestamp_value(row["processing_started_at"])
                if started_at is None:
                    continue
                for payload in _extract_task_payloads(row["data"]):
                    task_id = str(payload.get("task_id") or "")
                    scope_text = " ".join(
                        str(payload.get(key) or "")
                        for key in ("root_uri", "uri", "target_uri", "source_name", "path")
                    )
                    if namespace and namespace.rstrip("/") not in scope_text:
                        scope_unmatched_count += 1
                        continue
                    if profile and profile not in scope_text:
                        scope_unmatched_count += 1
                        continue
                    candidate = {
                        "queue_name": str(row["queue_name"]),
                        "message_id": str(row["message_id"]),
                        "processing_started_at": started_at,
                        "status": str(row["status"]),
                        "created_at": row["created_at"],
                        "namespace": namespace,
                        "profile": profile,
                        "source": "queue_db.processing_started_at",
                    }
                    previous = starts.get(task_id)
                    if previous is None or started_at < float(previous["processing_started_at"]):
                        starts[task_id] = candidate
    except (OSError, sqlite3.Error) as exc:
        errors.append(f"queue_db:{type(exc).__name__}")
    return starts, errors, row_count, scope_unmatched_count


def _host_snapshot(queue_db: Path = DEFAULT_QUEUE_DB) -> dict[str, Any]:
    """Collect read-only host RSS and queue WAL samples when observable."""
    snapshot: dict[str, Any] = {"rss_mb": None, "wal_bytes": None, "errors": []}
    try:
        result = subprocess.run(
            ["ps", "axo", "rss=,command="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            rss_kb = 0
            matched = 0
            for line in result.stdout.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) != 2 or "openviking-server" not in parts[1]:
                    continue
                try:
                    rss_kb += int(parts[0])
                    matched += 1
                except ValueError:
                    continue
            if matched:
                snapshot["rss_mb"] = round(rss_kb / 1024.0, 4)
        else:
            snapshot["errors"].append(f"ps_exit={result.returncode}")
    except (OSError, subprocess.SubprocessError) as exc:
        snapshot["errors"].append(f"rss:{type(exc).__name__}")
    try:
        wal_path = Path(str(queue_db) + "-wal")
        snapshot["wal_bytes"] = wal_path.stat().st_size
    except OSError as exc:
        snapshot["errors"].append(f"wal:{type(exc).__name__}")
    return snapshot


def _submit_one(ov: Any, source: Path, target: str, processing_mode: str, index: int) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        uploaded = ov.upload_file(source, timeout=120)
        uploaded_at = time.perf_counter()
        temp_id = (uploaded.get("result") or {}).get("temp_file_id") or uploaded.get("temp_file_id")
        if not temp_id:
            raise RuntimeError(f"temp_upload_missing_id: {uploaded}")
        # ``accepted`` is the resource submission hot path.  File upload is a
        # preparation step and must not be mixed into its latency percentile.
        accepted_started = time.perf_counter()
        response = ov.request(
            "POST",
            "/api/v1/resources",
            {
                "temp_file_id": temp_id,
                "to": target,
                "create_parent": True,
                "wait": False,
                "processing_mode": processing_mode,
            },
            timeout=60,
        )
        result = response.get("result") if isinstance(response, dict) else None
        task_id = (result or {}).get("task_id") or response.get("task_id") if isinstance(response, dict) else None
        return {
            "index": index,
            "target_uri": target,
            "task_id": str(task_id) if task_id else None,
            "accepted_at": time.time(),
            "upload_latency_ms": round((uploaded_at - started) * 1000, 4),
            "accepted_latency_ms": round((time.perf_counter() - accepted_started) * 1000, 4),
            "status": "accepted" if task_id else "unknown",
            "error": None if task_id else f"missing_task_id: {response}",
            "queue_start_at": None,
            "queue_start_source": None,
        }
    except Exception as exc:  # evidence records must survive one bad task
        return {
            "index": index,
            "target_uri": target,
            "task_id": None,
            "accepted_at": time.time(),
            "accepted_latency_ms": round((time.perf_counter() - started) * 1000, 4),
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "queue_start_at": None,
            "queue_start_source": None,
        }


def collect_profile(
    *,
    profile: str,
    processing_mode: str,
    namespace: str,
    sample_count: int,
    duration_seconds: int,
    source_size: int,
    submit_workers: int,
    queue_db: Path = DEFAULT_QUEUE_DB,
) -> dict[str, Any]:
    if not namespace.startswith("viking://resources/v45-r2-g7-shadow/") and namespace != DEFAULT_NAMESPACE:
        raise ValueError("namespace must be under the dedicated v45-r2-g7-shadow prefix")
    ov = _load_ov_rest()
    with tempfile.TemporaryDirectory(prefix="v45-r2-g7-shadow-source-") as temp:
        source = Path(temp) / f"{profile}.md"
        body = (f"V45 R2 G7 synthetic shadow profile={profile}\n" + ("x" * max(0, source_size - 40))).encode("utf-8")
        source.write_bytes(body)
        actual_size = source.stat().st_size
        started_wall = time.time()
        deadline = started_wall + max(1, duration_seconds)
        host_baseline = _host_snapshot()
        host_peak = dict(host_baseline)
        next_host_sample_at = started_wall + HOST_SAMPLE_SECONDS
        submissions: list[dict[str, Any]] = []
        accepted: list[dict[str, Any]] = []
        task_by_id: dict[str, dict[str, Any]] = {}
        completion_latencies: list[float] = []
        queue_waits: list[float] = []
        queue_totals_by_task: dict[str, dict[str, int]] = {}
        provider_usage_task_ids: set[str] = set()
        terminal: dict[str, int] = {}
        last_status: dict[str, str] = {}
        queue_start_sources: set[str] = set()
        queue_wait_missing = 0
        queue_observation_errors: set[str] = set()
        queue_rows_seen_peak = 0
        queue_scope_unmatched_peak = 0
        pending_count_at_deadline: int | None = None
        submit_futures: dict[concurrent.futures.Future[dict[str, Any]], int] = {}
        interval = max(0.0, float(duration_seconds) / max(1, sample_count))
        next_submit_at = started_wall
        next_queue_sample_at = started_wall
        next_index = 0
        max_pending_submissions = max(1, submit_workers) * 2
        drain_deadline = deadline + MAX_DRAIN_SECONDS

        # Submit and poll in the same bounded loop. Waiting until all samples
        # are submitted before polling turns completion latency into the test
        # duration, which is not a valid content or queue metric.
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, submit_workers)) as executor:
            while True:
                now = time.time()
                while next_index < max(0, sample_count) and now >= next_submit_at and len(submit_futures) < max_pending_submissions:
                    target = f"{namespace.rstrip('/')}/{profile}/{uuid.uuid4().hex}-{next_index}"
                    future = executor.submit(_submit_one, ov, source, target, processing_mode, next_index)
                    submit_futures[future] = next_index
                    next_index += 1
                    next_submit_at += interval
                    now = time.time()

                done_futures = [future for future in submit_futures if future.done()]
                for future in done_futures:
                    submit_futures.pop(future, None)
                    item = future.result()
                    submissions.append(item)
                    task_id = item.get("task_id")
                    if task_id:
                        accepted.append(item)
                        task_by_id[str(task_id)] = item

                now = time.time()
                if now >= next_host_sample_at:
                    host_sample = _host_snapshot()
                    for key in ("rss_mb", "wal_bytes"):
                        value = host_sample.get(key)
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            previous = host_peak.get(key)
                            if not isinstance(previous, (int, float)) or value > previous:
                                host_peak[key] = value
                    next_host_sample_at = now + HOST_SAMPLE_SECONDS

                # QueueFS removes a row on ACK, so sample the durable table
                # while work is in flight. A missing row is left unknown.
                if now >= next_queue_sample_at:
                    queue_starts, sample_errors, row_count, scope_unmatched_count = _read_queue_processing_starts(
                        queue_db,
                        namespace=namespace,
                        profile=profile,
                    )
                    queue_observation_errors.update(sample_errors)
                    queue_rows_seen_peak = max(queue_rows_seen_peak, row_count)
                    queue_scope_unmatched_peak = max(queue_scope_unmatched_peak, scope_unmatched_count)
                    for task_id, item in task_by_id.items():
                        observation = queue_starts.get(task_id)
                        if observation is not None and item.get("queue_start_at") is None:
                            item["queue_start_at"] = observation["processing_started_at"]
                            item["queue_start_source"] = observation["source"]
                            item["queue_observation"] = observation
                            queue_start_sources.add(str(observation["source"]))
                    next_queue_sample_at = now + QUEUE_SAMPLE_SECONDS

                for task_id, item in list(task_by_id.items()):
                    try:
                        payload = ov.request("GET", f"/api/v1/tasks/{task_id}", timeout=30)
                        payload_map = _mapping(payload)
                        result = _mapping(payload_map.get("result"))
                        status = str(result.get("status") or payload_map.get("status") or "unknown")
                        stage = str(result.get("stage") or payload_map.get("stage") or "")
                        last_status[task_id] = f"{status}:{stage}"
                        queue_start_at, queue_start_source = _timestamp_from_payload(payload)
                        if queue_start_at is not None and item.get("queue_start_at") is None:
                            item["queue_start_at"] = queue_start_at
                            item["queue_start_source"] = queue_start_source
                            queue_start_sources.add(str(queue_start_source))
                        nested = _mapping(result.get("result"))
                        queue_status_raw = nested.get("queue_status")
                        if queue_status_raw is None:
                            queue_status: dict[str, Any] = {}
                        elif isinstance(queue_status_raw, dict):
                            queue_status = queue_status_raw
                        else:
                            queue_status = {}
                            queue_observation_errors.add("invalid_queue_status")
                        usage = _mapping(nested.get("usage"))
                        tokens = _mapping(usage.get("tokens"))
                        llm = _mapping(tokens.get("llm"))
                        try:
                            llm_total = float(llm.get("total") or 0)
                        except (TypeError, ValueError, OverflowError):
                            llm_total = 0.0
                            queue_observation_errors.add("invalid_llm_usage")
                        if llm_total > 0:
                            provider_usage_task_ids.add(task_id)
                        totals = queue_totals_by_task.setdefault(task_id, {"requeue_count": 0, "error_count": 0})
                        for queue_item in queue_status.values():
                            if not isinstance(queue_item, dict):
                                queue_observation_errors.add("invalid_queue_status_item")
                                continue
                            requeue_count = _counter(queue_item.get("requeue_count"))
                            error_count = _counter(queue_item.get("error_count"))
                            if requeue_count is None or error_count is None:
                                queue_observation_errors.add("invalid_queue_counter")
                                continue
                            totals["requeue_count"] = max(totals["requeue_count"], requeue_count)
                            totals["error_count"] = max(totals["error_count"], error_count)
                        if status in {"completed", "failed", "cancelled", "dead_letter", "quarantine"}:
                            terminal[status] = terminal.get(status, 0) + 1
                            completion_latencies.append(max(0.0, now - float(item["accepted_at"])))
                            if item.get("queue_start_at") is not None:
                                queue_waits.append(max(0.0, float(item["queue_start_at"]) - float(item["accepted_at"])))
                            else:
                                queue_wait_missing += 1
                            task_by_id.pop(task_id, None)
                    except Exception as exc:
                        last_status[task_id] = f"probe_error:{type(exc).__name__}"

                if pending_count_at_deadline is None and now >= deadline:
                    pending_count_at_deadline = len(task_by_id)
                all_submissions_done = next_index >= max(0, sample_count) and not submit_futures
                if now >= deadline and all_submissions_done and not task_by_id:
                    break
                if now >= drain_deadline:
                    if pending_count_at_deadline is None:
                        pending_count_at_deadline = len(task_by_id)
                    break
                sleep_for = POLL_SECONDS
                if next_index < max(0, sample_count) and len(submit_futures) < max_pending_submissions:
                    sleep_for = min(sleep_for, max(0.01, next_submit_at - time.time()))
                time.sleep(max(0.01, sleep_for))

        if pending_count_at_deadline is None:
            pending_count_at_deadline = len(task_by_id)
        requeues = sum(item["requeue_count"] for item in queue_totals_by_task.values())
        provider_errors = sum(item["error_count"] for item in queue_totals_by_task.values())
        provider_usage_tasks = len(provider_usage_task_ids)
        final_host_sample = _host_snapshot()
        for key in ("rss_mb", "wal_bytes"):
            value = final_host_sample.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                previous = host_peak.get(key)
                if not isinstance(previous, (int, float)) or value > previous:
                    host_peak[key] = value
        elapsed = max(duration_seconds, time.time() - started_wall)
        accepted_latencies = [float(item["accepted_latency_ms"]) for item in submissions if item.get("status") == "accepted"]
        completed = terminal.get("completed", 0)
        queue_wait_source = next(iter(queue_start_sources)) if len(queue_start_sources) == 1 else (
            "mixed_explicit_telemetry" if queue_start_sources else "unavailable"
        )
        if "queue_db.processing_started_at" in queue_start_sources and len(queue_start_sources) > 1:
            queue_wait_source = ",".join(sorted(queue_start_sources))
        baseline_rss = host_baseline.get("rss_mb")
        peak_rss = host_peak.get("rss_mb")
        baseline_wal = host_baseline.get("wal_bytes")
        peak_wal = host_peak.get("wal_bytes")
        rss_growth_ratio = (
            round(max(0.0, (float(peak_rss) - float(baseline_rss)) / float(baseline_rss)), 4)
            if isinstance(baseline_rss, (int, float)) and isinstance(peak_rss, (int, float)) and float(baseline_rss) > 0
            else None
        )
        wal_peak_ratio = (
            round(float(peak_wal) / float(baseline_wal), 4)
            if isinstance(baseline_wal, (int, float)) and isinstance(peak_wal, (int, float)) and float(baseline_wal) > 0
            else None
        )
        profile_result = {
            "profile": profile,
            "processing_mode": processing_mode,
            "namespace": namespace,
            "sample_count": completed,
            "submitted_count": len(submissions),
            "accepted_count": len(accepted),
            "duration_seconds": round(elapsed, 3),
            "resource_count": len({str(item["target_uri"]) for item in accepted}),
            "min_resource_bytes": actual_size,
            "terminal_counts": terminal,
            "pending_count_at_deadline": pending_count_at_deadline,
            "queue_wait_observed_count": len(queue_waits),
            "queue_wait_missing_count": queue_wait_missing,
            "submission_errors": [item for item in submissions if item.get("status") != "accepted"],
            "last_status_sample": dict(list(last_status.items())[:20]),
            "requeue_count": requeues,
            "provider_error_count": provider_errors,
            "provider_calls_verified": processing_mode == "vectors_only" or provider_usage_tasks > 0,
            "provider_usage_tasks": provider_usage_tasks,
            "metrics": {
                "accepted_p95_ms": _percentile(accepted_latencies, 95),
                "accepted_p99_ms": _percentile(accepted_latencies, 99),
                "queue_wait_p95_s": _percentile(queue_waits, 95) if not queue_wait_missing else None,
                "queue_wait_p99_s": _percentile(queue_waits, 99) if not queue_wait_missing else None,
                "queue_wait_max_s": round(max(queue_waits), 4) if queue_waits and not queue_wait_missing else None,
                "content_verified_p95_s": _percentile(completion_latencies, 95),
                "content_verified_p99_s": _percentile(completion_latencies, 99),
                "memory_link_lag_p95_s": None,
                "memory_link_lag_p99_s": None,
                "lock_wait_p95_ms": None,
                "lock_wait_max_ms": None,
                "rss_growth_ratio": rss_growth_ratio,
                "wal_peak_ratio": wal_peak_ratio,
                "retry_amplification": round((requeues + provider_errors) / max(1, completed), 4),
            },
            "metric_sources": {
                "accepted": "client_monotonic_submission_latency",
                "queue_wait": queue_wait_source,
                "content_verified": "task_state_poll_completion",
                "lock_wait": "unavailable",
                "rss": "host_process_rss_sample" if rss_growth_ratio is not None else "unavailable",
                "wal": "host_queue_db_wal_sample" if wal_peak_ratio is not None else "unavailable",
            },
            "queue_wait_unknown_reason": (
                (
                    "no matching queue_messages row with processing_started_at was observed"
                    if queue_wait_source == "unavailable"
                    else f"{queue_wait_missing} terminal task(s) had no durable processing_started_at match"
                )
                if queue_wait_missing or queue_wait_source == "unavailable"
                else None
            ),
            "queue_observation": {
                "db_path": str(queue_db.expanduser().resolve()),
                "rows_seen_peak": queue_rows_seen_peak,
                "scope_unmatched_peak": queue_scope_unmatched_peak,
                "matched_task_count": sum(1 for item in accepted if item.get("queue_start_at") is not None),
                "errors": sorted(queue_observation_errors),
            },
            "host_observation": {
                "baseline": host_baseline,
                "peak": host_peak,
                "errors": sorted(set(host_baseline.get("errors", []) + final_host_sample.get("errors", []))),
            },
            "evidence_role": "host_openviking_shadow",
            "captured_at": _now(),
        }
        return profile_result


def update_manifest(path: Path, profile_result: dict[str, Any], *, namespace: str) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    except (OSError, ValueError):
        manifest = {}
    if not isinstance(manifest, dict):
        manifest = {}
    manifest.setdefault("schema_version", "pm-system.v45-r2-g7-shadow-manifest.v1")
    manifest.setdefault("migration_id", "v45-r2-20260830")
    manifest.setdefault("migration_epoch", "v45-r2-20260830")
    manifest.setdefault(
        "minimums",
        {
            "policy": "all",
            "tasks": MIN_TASKS,
            "duration_seconds": MIN_DURATION_SECONDS,
            "resources": MIN_RESOURCES,
            "resource_bytes": MIN_RESOURCE_BYTES,
        },
    )
    manifest["namespace"] = namespace
    manifest["captured_at"] = _now()
    profiles = manifest.setdefault("profiles", {})
    profiles[str(profile_result["profile"])] = profile_result
    manifest["manifest_sha256"] = "sha256:" + hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("fast-vector", "pm-semantic", "codex-model"), required=True)
    parser.add_argument("--processing-mode", choices=("vectors_only", "semantic_and_vectors"), required=True)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--sample-count", type=int, default=MIN_TASKS)
    parser.add_argument("--duration-seconds", type=int, default=MIN_DURATION_SECONDS)
    parser.add_argument("--source-size", type=int, default=MIN_RESOURCE_BYTES)
    parser.add_argument("--submit-workers", type=int, default=SUBMIT_WORKERS)
    parser.add_argument("--queue-db", type=Path, default=DEFAULT_QUEUE_DB)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = collect_profile(
        profile=args.profile,
        processing_mode=args.processing_mode,
        namespace=args.namespace,
        sample_count=max(0, args.sample_count),
        duration_seconds=max(1, args.duration_seconds),
        source_size=max(MIN_RESOURCE_BYTES, args.source_size),
        submit_workers=max(1, args.submit_workers),
        queue_db=args.queue_db.expanduser().resolve(),
    )
    update_manifest(args.manifest.expanduser().resolve(), result, namespace=args.namespace)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("pending_count_at_deadline") == 0 and result.get("sample_count", 0) >= MIN_TASKS else 1


if __name__ == "__main__":
    raise SystemExit(main())
