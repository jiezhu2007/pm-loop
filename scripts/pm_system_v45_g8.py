#!/usr/bin/env python3
"""V4.5 R2 G8 isolated fault/recovery and catch-up gate.

Every scenario uses a temporary SQLite store and deterministic transports.  No
production database, OpenViking namespace, provider, scheduler, or LaunchAgent
is touched.  The gate is deliberately evidence-first: all scenarios must pass
before G8 can be considered complete.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pm_resource_dispatcher import DispatchTransportError, PMResourceDispatcher
from pm_system_cockpit import CockpitReadModel
from pm_system_gateway import SemanticGateway, provider_key
from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore
from pm_system_worker import PMSystemWorker


MIGRATION_ID = "v45-r2-20260830"
MIGRATION_EPOCH = "v45-r2-20260830"


def _scenario(name: str, fn: Any) -> dict[str, Any]:
    try:
        detail = fn()
        return {"name": name, "status": "PASS", "detail": detail or {}}
    except Exception as exc:
        return {"name": name, "status": "HOLD", "detail": {"error": f"{type(exc).__name__}: {exc}"}}


def disconnect_model() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-disconnect-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        scheduler = Scheduler(store, max_slots=1)
        accepted = store.accept({"job_type": "assessment", "loop_id": "g8-disconnect", "idempotency_key": "g8:disconnect"})
        claim = scheduler.claim_next(worker_id="g8-disconnect-worker")
        if claim is None:
            raise AssertionError("model disconnect fixture was not claimed")
        first = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="g8-input", prompt_version="v1", provider="oneapi")
        if scheduler.finish_model_call(first["call_id"], status="result_unknown") != "result_unknown":
            raise AssertionError("first response-unknown was not recorded")
        second = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="g8-input", prompt_version="v1", provider="oneapi")
        if second["attempt"] != 2 or second["model_input_hash"] != first["model_input_hash"]:
            raise AssertionError("controlled model retry did not use attempt=2 and same input hash")
        scheduler.finish_model_call(second["call_id"], status="completed", artifact_uri="artifact://g8-model-result")
        scheduler.release(claim["lease_id"], status="completed")
        with store.connect() as connection:
            calls = connection.execute("SELECT attempt,status FROM model_calls WHERE run_id=? ORDER BY attempt", (accepted["run_id"],)).fetchall()
            active_tokens = connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]
        run_status = store.get_run(accepted["run_id"])["status"]
        slot_status = scheduler.slot_snapshot()[0]["status"]
        if [tuple(row) for row in calls] != [(1, "result_unknown"), (2, "completed")] or run_status != "completed" or slot_status != "free" or active_tokens != 0:
            raise AssertionError(f"calls={calls} run={run_status} slot={slot_status} tokens={active_tokens}")
        return {"model_calls": [tuple(row) for row in calls], "run_status": run_status, "slot_status": slot_status, "active_provider_tokens": active_tokens}


def resource_504_isolation() -> dict[str, Any]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-504-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        gateway = SemanticGateway(store, max_attempts=2)
        semantic = gateway.enqueue(resource_id="g8-semantic", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic")
        fast = gateway.enqueue(resource_id="g8-fast", revision_id="r1", processing_mode="vectors_only", provider="oneapi", profile="fast-vector")
        dispatched = gateway.dispatch_once(limit=2)
        by_id = {item["outbox_id"]: item for item in dispatched}
        gateway.ack(fast["outbox_id"], dispatch_token=by_id[fast["outbox_id"]]["dispatch_token"], semantic_status="completed")
        semantic_result = gateway.fail(semantic["outbox_id"], category="504", dispatch_token=by_id[semantic["outbox_id"]]["dispatch_token"])
        failure_count = 1
        while semantic_result["status"] == "retry_wait" and failure_count < 5:
            with store.transaction() as connection:
                connection.execute("UPDATE outbox_items SET next_attempt_at=NULL WHERE outbox_id=?", (semantic["outbox_id"],))
            retry = gateway.dispatch_once(limit=1)
            if len(retry) != 1:
                raise AssertionError(f"504 retry was not dispatchable: {retry}")
            semantic_result = gateway.fail(semantic["outbox_id"], category="504", dispatch_token=retry[0]["dispatch_token"])
            failure_count += 1
        with store.connect() as connection:
            fast_status = connection.execute("SELECT status FROM outbox_items WHERE outbox_id=?", (fast["outbox_id"],)).fetchone()[0]
            active_leases = connection.execute("SELECT COUNT(*) FROM outbox_dispatch_leases").fetchone()[0]
        if semantic_result["status"] != "dead_letter" or fast_status != "completed" or active_leases != 0 or gateway.dispatch_once(limit=1):
            raise AssertionError(f"semantic={semantic_result['status']} fast={fast_status}")
        return {"semantic_status": semantic_result["status"], "fast_status": fast_status, "failure_count": failure_count, "active_dispatch_leases": active_leases, "wall_clock_seconds": round(time.monotonic() - started, 6)}


def duplicate_revision() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-duplicate-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        gateway = SemanticGateway(store)
        values = [gateway.enqueue(resource_id="g8-doc", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic") for _ in range(5)]
        # ``semantic_tasks`` is a dispatch projection, so materialize it before
        # checking the duplicate-revision invariant.  Admission itself must
        # still remain enqueue-only and idempotent.
        dispatched = gateway.dispatch_once(limit=1)
        with store.connect() as connection:
            outbox = connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0]
            semantic = connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0]
        if outbox != 1 or semantic != 1 or len(dispatched) != 1 or sum(not item["deduplicated"] for item in values) != 1:
            raise AssertionError(f"outbox={outbox} semantic={semantic} dispatched={dispatched} responses={values}")
        return {"outbox_rows": outbox, "semantic_rows": semantic, "dispatch_rows": len(dispatched)}


def terminal_cancel_late_callback() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-cancel-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        scheduler = Scheduler(store, max_slots=1)
        accepted = store.accept({"job_type": "run", "loop_id": "g8-cancel", "idempotency_key": "g8:cancel"})
        scheduler.claim_next(worker_id="g8-cancel-worker")
        call = scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="g8-race", prompt_version="v1", provider="oneapi")
        if not scheduler.cancel(accepted["run_id"], reason="g8-cancel"):
            raise AssertionError("cancel did not transition run")
        result = scheduler.finish_model_call(call["call_id"], status="response_received", artifact_uri="artifact://late")
        detail = CockpitReadModel(store).run_detail(accepted["run_id"])
        if result != "cancelled" or detail["run"]["status"] != "cancelled" or detail["model_calls"][0]["status"] != "cancelled":
            raise AssertionError(f"late callback resurrected terminal state: {result}, {detail}")
        return {"run_status": detail["run"]["status"], "model_status": detail["model_calls"][0]["status"]}


def restart_lease_reconcile() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-restart-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        scheduler = Scheduler(store, max_slots=1)
        accepted = store.accept({"job_type": "run", "loop_id": "g8-restart", "idempotency_key": "g8:restart"})
        scheduler.claim_next(worker_id="g8-restart-worker")
        with store.transaction() as connection:
            connection.execute("UPDATE execution_slots SET expires_at=?", ("2000-01-01T00:00:00Z",))
        result = scheduler.startup_reconcile(active_lease_ids=[])
        if result["interrupted_runs"] != 1 or store.get_run(accepted["run_id"])["status"] != "interrupted" or scheduler.slot_snapshot()[0]["status"] != "free":
            raise AssertionError(f"reconcile={result}")
        return result


def provider_429_window() -> dict[str, Any]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-429-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        gateway = SemanticGateway(store, circuit_threshold=10)
        key = provider_key("oneapi", "endpoint", "model")
        accepted = gateway.enqueue(resource_id="g8-429", revision_id="r1", processing_mode="semantic_and_vectors", provider="oneapi", profile="pm-semantic", endpoint="endpoint", model="model")
        dispatched = gateway.dispatch_once(limit=1)
        if len(dispatched) != 1:
            raise AssertionError(f"resource 429 fixture was not dispatched: {dispatched}")
        with store.transaction() as connection:
            connection.execute("UPDATE outbox_items SET retry_deadline_at='2000-01-01T00:00:00Z' WHERE outbox_id=?", (accepted["outbox_id"],))
        terminal = gateway.fail(
            accepted["outbox_id"],
            category="429",
            retry_after="60",
            provider_key_value=key,
            dispatch_token=dispatched[0]["dispatch_token"],
        )
        with store.connect() as connection:
            row = connection.execute("SELECT status,attempt,next_attempt_at FROM outbox_items WHERE outbox_id=?", (accepted["outbox_id"],)).fetchone()
            rate_events = connection.execute("SELECT COUNT(*) FROM provider_rate_limit_events WHERE provider_key=?", (key,)).fetchone()[0]
        if tuple(row) != ("dead_letter", 0, None) or terminal["status"] != "dead_letter" or rate_events != 1 or gateway.dispatch_once(limit=1):
            raise AssertionError(f"terminal={terminal} row={tuple(row)} events={rate_events}")
        return {"status": row[0], "attempt": row[1], "provider_key": key, "rate_limit_events": rate_events, "wall_clock_seconds": round(time.monotonic() - started, 6)}


def _model_snapshot(path: Path) -> Path:
    path.write_text(json.dumps({
        "schema_version": "pm-loop.snapshot.v1",
        "snapshot_id": "g8-model-429-snapshot",
        "collected_at": "2026-08-30T00:00:00Z",
        "summary": {"launchd_jobs": 0, "skills": 0, "openviking_status": "fixture", "timeline_events": 0},
        "sources": {"fixture": {"status": "healthy"}},
    }), encoding="utf-8")
    return path


def model_429_retry_after_and_deadline() -> dict[str, Any]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-model-429-success-") as temp:
        root = Path(temp)
        store = PMSystemStore(root / "pm.db")
        snapshot = _model_snapshot(root / "snapshot.json")
        accepted = store.accept({
            "job_type": "pm-loop",
            "loop_id": "g8-model-429",
            "idempotency_key": "g8:model-429:success",
            "profile": "interactive",
            "payload": {"loop_id": "g8-model-429", "analysis_mode": "codex", "snapshot_path": str(snapshot), "provider": "oneapi", "provider_endpoint": "chat", "model": "g8-model"},
        })
        provider_calls = []

        def rate_limited_then_success(prompt: str, timeout: int, codex_root: Path) -> tuple[int, str, str, str]:
            provider_calls.append(1)
            if len(provider_calls) == 1:
                return 1, "", "HTTP 429 Too Many Requests\nRetry-After: 0", "g8-fixture"
            return 0, json.dumps({"answerability": "partial", "confidence": 0.5, "conclusion": {"headline": "recovered"}}), "", "g8-fixture"

        worker = PMSystemWorker(root / "pm.db", artifact_root=root / "runs", max_slots=1, invoker=rate_limited_then_success)
        first_status = worker.run_once()
        second_status = worker.run_once()
        with store.connect() as connection:
            job_attempt = connection.execute("SELECT attempt FROM jobs WHERE run_id=?", (accepted["run_id"],)).fetchone()[0]
            calls = [tuple(row) for row in connection.execute("SELECT attempt,status FROM model_calls WHERE run_id=? ORDER BY attempt", (accepted["run_id"],)).fetchall()]
            rate_events = connection.execute("SELECT COUNT(*) FROM provider_rate_limit_events").fetchone()[0]
            active_tokens = connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]
        if first_status != "retry_wait" or second_status != "completed" or job_attempt != 0 or calls != [(1, "retry_wait"), (2, "completed")] or rate_events != 1 or active_tokens != 0:
            raise AssertionError(f"statuses={first_status}/{second_status} job_attempt={job_attempt} calls={calls} events={rate_events} tokens={active_tokens}")

    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-model-429-deadline-") as temp:
        root = Path(temp)
        store = PMSystemStore(root / "pm.db")
        snapshot = _model_snapshot(root / "snapshot.json")
        accepted = store.accept({
            "job_type": "pm-loop",
            "loop_id": "g8-model-429-deadline",
            "idempotency_key": "g8:model-429:deadline",
            "profile": "interactive",
            "payload": {"loop_id": "g8-model-429-deadline", "analysis_mode": "codex", "snapshot_path": str(snapshot)},
        })
        deadline_calls = []

        def rate_limited(prompt: str, timeout: int, codex_root: Path) -> tuple[int, str, str, str]:
            deadline_calls.append(1)
            return 1, "", "HTTP 429 Too Many Requests\nRetry-After: 60", "g8-fixture"

        worker = PMSystemWorker(root / "pm.db", artifact_root=root / "runs", max_slots=1, invoker=rate_limited)
        if worker.run_once() != "retry_wait":
            raise AssertionError("deadline fixture did not enter retry_wait")
        with store.transaction() as connection:
            connection.execute("UPDATE model_calls SET retry_deadline_at='2000-01-01T00:00:00Z' WHERE run_id=?", (accepted["run_id"],))
            connection.execute("UPDATE provider_buckets SET throttle_until='2000-01-01T00:00:00Z'")
            connection.execute("UPDATE jobs SET next_attempt_at=NULL WHERE run_id=?", (accepted["run_id"],))
        terminal_status = worker.run_once()
        with store.connect() as connection:
            active_tokens = connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]
        run_status = store.get_run(accepted["run_id"])["status"]
        slot_status = worker.scheduler.slot_snapshot()[0]["status"]
        if terminal_status != "failed" or run_status != "failed" or len(deadline_calls) != 1 or active_tokens != 0 or slot_status != "free":
            raise AssertionError(f"terminal={terminal_status}/{run_status} provider_calls={len(deadline_calls)} tokens={active_tokens} slot={slot_status}")
    return {"retry_after_recovery": "completed", "deadline_terminal": run_status, "provider_calls_after_deadline": 0, "active_provider_tokens": active_tokens, "slot_status": slot_status, "wall_clock_seconds": round(time.monotonic() - started, 6)}


class _UnknownThenSuccessTransport:
    url = "http://g8-fake-openviking"

    def __init__(self) -> None:
        self.add_calls = 0

    def upload_file(self, path: Path, *, timeout: float | None = None) -> dict[str, Any]:
        return {"result": {"temp_file_id": "g8-temp"}}

    def add_resource(self, body: dict[str, Any], *, timeout: float | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        self.add_calls += 1
        if self.add_calls == 1:
            raise DispatchTransportError("simulated response unknown")
        return {"status": "accepted", "task_id": "g8-task"}

    def get_task(self, task_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        return {"status": "completed", "task_id": task_id}


def response_unknown_once() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-unknown-") as temp:
        root = Path(temp)
        source = root / "g8.md"
        source.write_text("g8 synthetic resource", encoding="utf-8")
        store = PMSystemStore(root / "pm.db")
        transport = _UnknownThenSuccessTransport()
        dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts", observation_backoff_seconds=0)
        dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/g8-shadow")
        first = dispatcher.dispatch_pending(limit=1)
        if not first or first[0].get("status") != "retry_wait":
            raise AssertionError(f"first attempt={first}")
        with store.transaction() as connection:
            connection.execute("UPDATE outbox_items SET next_attempt_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE status='retry_wait'")
        second = dispatcher.dispatch_pending(limit=1)
        if not second or second[0].get("status") != "completed":
            raise AssertionError(f"second attempt={second}")
        with store.connect() as connection:
            rows = connection.execute("SELECT operation_type,response_state,attempt,request_hash,namespace_epoch FROM operation_ledger WHERE operation_type='add_resource' ORDER BY attempt").fetchall()
        values = [tuple(row) for row in rows]
        if values != [("add_resource", "unknown", 1, values[0][3], values[0][4]), ("add_resource", "completed", 2, values[1][3], values[1][4])]:
            raise AssertionError(f"operation ledger={values}")
        return {"transport_add_calls": transport.add_calls, "operation_ledger": [{"operation_type": r[0], "response_state": r[1], "attempt": r[2], "request_hash_present": bool(r[3]), "epoch": r[4]} for r in rows]}


def catchup_idempotency() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v45-r2-g8-catchup-") as temp:
        store = PMSystemStore(Path(temp) / "pm.db")
        payload = {"job_type": "daily", "loop_id": "g8-catchup", "idempotency_key": "g8:catchup:2026-08-29", "profile": "fast-vector"}
        first = store.accept(payload)
        second = store.accept(payload)
        if first["job_id"] != second["job_id"] or not second.get("deduplicated"):
            raise AssertionError(f"catchup duplicate was not idempotent: {first}, {second}")
        with store.connect() as connection:
            count = connection.execute("SELECT COUNT(*) FROM jobs WHERE idempotency_key=?", (payload["idempotency_key"],)).fetchone()[0]
        if count != 1:
            raise AssertionError(f"catchup rows={count}")
        return {"job_rows": count, "deduplicated": bool(second.get("deduplicated"))}


def run_g8() -> dict[str, Any]:
    scenarios = [
        _scenario("model disconnect and bounded retry", disconnect_model),
        _scenario("OpenViking 504 profile isolation", resource_504_isolation),
        _scenario("duplicate revision idempotency", duplicate_revision),
        _scenario("cancel and late callback terminal fence", terminal_cancel_late_callback),
        _scenario("restart lease reconciliation", restart_lease_reconcile),
        _scenario("Resource 429 total-wall-clock terminal", provider_429_window),
        _scenario("model 429 Retry-After and deadline", model_429_retry_after_and_deadline),
        _scenario("response-unknown one controlled resend", response_unknown_once),
        _scenario("missed-period catch-up idempotency", catchup_idempotency),
    ]
    decision = "PASS" if all(item["status"] == "PASS" for item in scenarios) else "HOLD"
    return {
        "schema_version": "pm-system.v45-r2-g8-recovery-manifest.v1",
        "stage_id": "G8",
        "migration_id": MIGRATION_ID,
        "migration_epoch": MIGRATION_EPOCH,
        "decision": decision,
        "scenarios": scenarios,
        "scenario_counts": {"total": len(scenarios), "passed": sum(item["status"] == "PASS" for item in scenarios), "hold": sum(item["status"] != "PASS" for item in scenarios)},
        "production_state_touched": False,
        "external_provider_calls": 0,
        "notes": ["所有场景使用临时 SQLite 与 deterministic transport", "Resource/Model 429 不增加业务 attempt，并受持久总墙钟约束", "response-unknown 最多一次受控重发", "每个 retry 场景必须证明终态、slot/token/lease 释放和有界墙钟", "MemoryLink 无独立 API 不在 G8 伪造通过"],
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_g8()
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["decision"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
