#!/usr/bin/env python3
"""Small single-process scheduler/admission layer for V4.4 S2.

The scheduler owns capacity, not business execution.  Workers claim a durable
job and receive one slot lease; the actual Codex process is started by the
worker in its own process group.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from pm_system_gateway import provider_key
from pm_schedule_registry import is_business_window_open, parse_duration
from pm_system_store import MigrationFrozen, PMSystemStore, now_iso


DEFAULT_SLOT_TTL_SECONDS = 60
DEFAULT_MAX_CODEX_SLOTS = 2
ADMISSION_ENABLED = frozenset({"on", "canary", "enabled", "true", "1"})
ADMISSION_FROZEN = frozenset({"freeze", "frozen", "off", "disabled", "false", "0"})
PROFILE_WEIGHT = {
    "interactive": 4,
    "report": 4,
    "fast-vector": 3,
    "memory-skill": 2,
    "pm-semantic": 1,
    "public-semantic": 1,
}


def _future_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _scheduled_policy(payload_json: Any) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    """Read immutable scheduled execution policy from a persisted Job payload."""
    try:
        payload = json.loads(str(payload_json or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, None
    if not isinstance(payload, Mapping):
        return None, None
    key = str(payload.get("concurrency_key") or "").strip() or None
    retry = payload.get("retry")
    return key, dict(retry) if isinstance(retry, Mapping) else None


def _retry_policy(retry: Optional[Mapping[str, Any]]) -> tuple[int, int]:
    """Return retry-count budget and fixed backoff seconds for a Job."""
    if retry is None:
        return 0, 0
    try:
        max_attempts = max(0, int(retry.get("max_attempts", 0)))
        backoff_seconds = max(0, int(parse_duration(retry.get("backoff")).total_seconds()))
    except (TypeError, ValueError):
        # Registry validation should prevent this, but a historic payload must
        # fail terminally rather than accidentally create an unbounded retry.
        return 0, 0
    return max_attempts, backoff_seconds


def _lease_id() -> str:
    return f"lease-{uuid.uuid4().hex}"


def _call_id() -> str:
    return f"call-{uuid.uuid4().hex}"


class AdmissionFrozen(RuntimeError):
    pass


class ModelRetryDeadlineExceeded(AdmissionFrozen):
    pass


class ProviderThrottled(AdmissionFrozen):
    def __init__(self, *, provider_key_value: str, throttle_until: str, retry_after_seconds: int) -> None:
        self.provider_key = provider_key_value
        self.throttle_until = throttle_until
        self.retry_after_seconds = max(0, int(retry_after_seconds))
        super().__init__(f"provider bucket {provider_key_value} is throttled until {throttle_until}")


def admission_from_env(*, default: str = "on") -> str:
    """Read the shared admission switch without accepting malformed values."""
    value = str(os.environ.get("PM_V44_ADMISSION", default) or default).strip().lower()
    if value in ADMISSION_ENABLED:
        return "on"
    if value in ADMISSION_FROZEN:
        return "freeze"
    # Fail closed during a deployment if launchd passes an unexpected value.
    return "freeze"


def max_slots_from_env(*, default: int = DEFAULT_MAX_CODEX_SLOTS) -> int:
    """Read the bounded global Codex slot count from the environment."""
    raw = str(os.environ.get("PM_V44_MAX_CODEX_SLOTS", default) or default).strip()
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(0, value)


class Scheduler:
    """Global slot admission and model-stage checkpoint coordinator."""

    def __init__(self, store: PMSystemStore, *, max_slots: Optional[int] = None, slot_ttl_seconds: int = DEFAULT_SLOT_TTL_SECONDS, admission: Optional[str] = None) -> None:
        configured_slots = max_slots_from_env() if max_slots is None else int(max_slots)
        if configured_slots < 0:
            raise ValueError("max_slots must be non-negative")
        if slot_ttl_seconds <= 0:
            raise ValueError("slot_ttl_seconds must be positive")
        self.store = store
        self.max_slots = configured_slots
        self.admission = "on" if str(admission or admission_from_env()).strip().lower() in ADMISSION_ENABLED else "freeze"
        self.slot_ttl_seconds = int(slot_ttl_seconds)
        self.ensure_slots()

    def ensure_slots(self) -> None:
        with self.store.transaction() as connection:
            for index in range(self.max_slots):
                connection.execute(
                    "INSERT OR IGNORE INTO execution_slots(slot_id,status) VALUES(?, 'free')",
                    (f"codex-{index + 1}",),
                )

    def set_admission(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        self.admission = "on" if normalized in ADMISSION_ENABLED else "freeze"
        return self.admission

    def admission_snapshot(self) -> Dict[str, Any]:
        return {"admission": self.admission, "max_slots": self.max_slots, "claim_enabled": self.admission == "on" and self.max_slots > 0}

    @staticmethod
    def _terminate_orphan(pid: Optional[int], process_group_id: Optional[int]) -> None:
        """Best-effort cleanup for a lease whose owner stopped heartbeating.

        Never signal the current worker's own process group.  A stale lease can
        otherwise make a new worker kill itself while reconciling a test or a
        same-process thread.
        """
        current_pid = os.getpid()
        current_group = os.getpgrp()
        try:
            group = int(process_group_id or 0)
        except (TypeError, ValueError):
            group = 0
        try:
            process = int(pid or 0)
        except (TypeError, ValueError):
            process = 0
        if group > 1 and group != current_group:
            try:
                os.killpg(group, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                return
            return
        if process > 1 and process != current_pid:
            try:
                os.kill(process, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass

    def _release_expired_unlocked(self, connection: sqlite3.Connection, at: str) -> int:
        rows = connection.execute(
            "SELECT slot_id, lease_id, run_id, pid, process_group_id FROM execution_slots WHERE status='leased' AND expires_at IS NOT NULL AND expires_at <= ?",
            (at,),
        ).fetchall()
        for row in rows:
            connection.execute(
                "UPDATE execution_slots SET status='free', lease_id=NULL, run_id=NULL, profile=NULL, leased_at=NULL, heartbeat_at=NULL, expires_at=NULL, pid=NULL, process_group_id=NULL WHERE slot_id=?",
                (row[0],),
            )
            if row[2]:
                connection.execute(
                    "UPDATE jobs SET status='interrupted', updated_at=? WHERE run_id=? AND status='running'",
                    (at, row[2]),
                )
                connection.execute(
                    "UPDATE runs SET status='interrupted', updated_at=? WHERE run_id=? AND status='running'",
                    (at, row[2]),
                )
                occurrence = connection.execute(
                    "SELECT occurrence_id FROM runs WHERE run_id=?",
                    (row[2],),
                ).fetchone()
                if occurrence is not None and occurrence[0]:
                    self.store._update_schedule_occurrence_unlocked(
                        connection,
                        str(occurrence[0]),
                        state="failed",
                        reason="lease_expired",
                    )
            self._terminate_orphan(row[3], row[4])
        return len(rows)

    def claim_next(self, *, worker_id: str = "worker", pid: Optional[int] = None, process_group_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Claim one queued job, or return None when all slots are occupied."""
        if self.admission != "on" or self.max_slots <= 0:
            return None
        at = now_iso()
        with self.store.transaction() as connection:
            freeze = self.store._freeze_blocks(connection)
            if freeze is not None:
                return None
            connection.execute("UPDATE provider_tokens SET released_at=? WHERE released_at IS NULL AND expires_at<=?", (at, at))
            self._release_expired_unlocked(connection, at)
            expired_jobs = connection.execute(
                "SELECT job_id,run_id,occurrence_id FROM jobs WHERE status IN ('queued','retry_wait') AND deadline_at IS NOT NULL AND deadline_at<=?",
                (at,),
            ).fetchall()
            for job_id, run_id, occurrence_id in expired_jobs:
                connection.execute(
                    "UPDATE jobs SET status='failed',completed_at=?,updated_at=?,terminal_reason='deadline_exceeded',error_fingerprint='deadline_exceeded' WHERE job_id=?",
                    (at, at, job_id),
                )
                connection.execute(
                    "UPDATE runs SET status='failed',completed_at=?,updated_at=?,error='deadline_exceeded' WHERE run_id=? AND status IN ('queued','retry_wait')",
                    (at, at, run_id),
                )
                seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]) + 1
                connection.execute(
                    "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                    (run_id, seq, "run/failed", "scheduler", json.dumps({"reason": "deadline_exceeded"}, separators=(",", ":")), at),
                )
                if occurrence_id:
                    self.store._update_schedule_occurrence_unlocked(
                        connection,
                        str(occurrence_id),
                        state="failed",
                        reason="deadline_exceeded",
                    )
            slot = connection.execute(
                "SELECT slot_id FROM execution_slots WHERE status='free' ORDER BY slot_id LIMIT 1"
            ).fetchone()
            if slot is None:
                return None
            active_concurrency_keys = {
                key
                for payload_json, in connection.execute(
                    "SELECT payload_json FROM jobs WHERE status='running' AND occurrence_id IS NOT NULL"
                ).fetchall()
                for key, _retry in (_scheduled_policy(payload_json),)
                if key
            }
            # Priority remains primary; profile weight is a deterministic
            # tie-breaker applied before LIMIT so a semantic backlog cannot
            # hide interactive work behind the first candidates. A scheduled
            # Job may only be claimed when no running Job shares its durable
            # ``concurrency_key``; independent task locks stay in the
            # handler, so this is a group constraint rather than a lock
            # alias.
            candidates = connection.execute(
                """SELECT job_id, run_id, job_type, profile, priority, queued_at, payload_json,
                          occurrence_id, schedule_key, trigger_kind, registry_hash, lock_key, deadline_at
                          , status
                   FROM jobs
                   WHERE status IN ('queued', 'retry_wait')
                     AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                     AND (deadline_at IS NULL OR deadline_at > ?)
                   ORDER BY priority DESC,
                            CASE profile
                                WHEN 'interactive' THEN 4
                                WHEN 'report' THEN 4
                                WHEN 'fast-vector' THEN 3
                                WHEN 'memory-skill' THEN 2
                                WHEN 'pm-semantic' THEN 1
                                WHEN 'public-semantic' THEN 1
                                ELSE 0
                            END DESC,
                            queued_at ASC, rowid ASC
                   LIMIT 256""",
                (at, at),
            ).fetchall()
            job = None
            for candidate in candidates:
                # Calendar-created retries must not start outside the global
                # business window. Initial queued jobs are admitted by the
                # calendar tick; this guard specifically closes the retry
                # path, which can otherwise wake at an arbitrary backoff time.
                if candidate[13] == "retry_wait" and candidate[8] and not is_business_window_open(_parse_iso(at) or datetime.now(timezone.utc), timezone_name="Asia/Shanghai"):
                    continue
                concurrency_key, _retry = _scheduled_policy(candidate[6])
                if concurrency_key and concurrency_key in active_concurrency_keys:
                    continue
                job = candidate
                break
            if job is None:
                return None
            lease = _lease_id()
            expires = _future_iso(self.slot_ttl_seconds)
            slot_update = connection.execute(
                "UPDATE execution_slots SET status='leased', lease_id=?, run_id=?, profile=?, leased_at=?, heartbeat_at=?, expires_at=?, pid=?, process_group_id=? WHERE slot_id=? AND status='free'",
                (lease, job[1], job[3], at, at, expires, pid, process_group_id, slot[0]),
            )
            if slot_update.rowcount != 1:
                return None
            job_update = connection.execute(
                "UPDATE jobs SET status='running', started_at=COALESCE(started_at, ?), updated_at=? WHERE job_id=? AND status IN ('queued', 'retry_wait')",
                (at, at, job[0]),
            )
            if job_update.rowcount != 1:
                connection.execute("UPDATE execution_slots SET status='free', lease_id=NULL, run_id=NULL, profile=NULL, leased_at=NULL, heartbeat_at=NULL, expires_at=NULL, pid=NULL, process_group_id=NULL WHERE slot_id=?", (slot[0],))
                return None
            connection.execute("UPDATE runs SET status='running', started_at=COALESCE(started_at, ?), updated_at=? WHERE run_id=?", (at, at, job[1]))
            if job[7]:
                self.store._update_schedule_occurrence_unlocked(
                    connection,
                    str(job[7]),
                    state="running",
                    reason=None,
                )
            seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (job[1],)).fetchone()[0]) + 1
            connection.execute(
                "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                (job[1], seq, "run/claimed", worker_id, json.dumps({"lease_id": lease, "slot_id": slot[0]}, separators=(",", ":")), at),
            )
            self.store._record_activity_unlocked(
                connection,
                event_type="run/claimed",
                actor=worker_id,
                payload={"lease_id": lease, "slot_id": slot[0], "run_id": job[1], "job_id": job[0], "occurrence_id": job[7]},
                run_id=job[1],
                job_id=job[0],
                occurrence_id=job[7],
                source_cursor=f"{job[1]}:{seq}",
                idempotency_key=f"run:{job[1]}:{seq}",
                occurred_at=at,
            )
            if job[8]:
                self.store._upsert_operation_unlocked(
                    connection,
                    operation_key=f"schedule:{job[8]}",
                    module_id="Scheduler",
                    schedule_key=str(job[8]),
                    process="pm-system-worker",
                    heartbeat_at=at,
                    lease_id=lease,
                    current_run=str(job[1]),
                    status="running",
                    freshness="fresh",
                    reconcile_state="clear",
                    evidence_refs=[f"run_events:{job[1]}:{seq}"],
                    source_version=job[10],
                    observed_at=at,
                    updated_at=at,
                )
            return {
                "job_id": job[0],
                "run_id": job[1],
                "job_type": job[2],
                "profile": job[3],
                "priority": job[4],
                "payload": json.loads(job[6] or "{}"),
                "occurrence_id": job[7],
                "schedule_key": job[8],
                "trigger_kind": job[9],
                "registry_hash": job[10],
                "lock_key": job[11],
                "deadline_at": job[12],
                "slot_id": slot[0],
                "lease_id": lease,
                "expires_at": expires,
            }

    def heartbeat(self, lease_id: str, *, ttl_seconds: Optional[int] = None) -> bool:
        at = now_iso()
        expires = _future_iso(ttl_seconds or self.slot_ttl_seconds)
        with self.store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE execution_slots SET heartbeat_at=?, expires_at=? WHERE lease_id=? AND status='leased'",
                (at, expires, lease_id),
            )
            if cursor.rowcount != 1:
                return False
            connection.execute("UPDATE runs SET updated_at=? WHERE run_id=(SELECT run_id FROM execution_slots WHERE lease_id=?)", (at, lease_id))
            return True

    def release(
        self,
        lease_id: str,
        *,
        status: str = "completed",
        error: Optional[str] = None,
        retry_after_seconds: Optional[int] = None,
        increment_attempt: bool = True,
    ) -> bool:
        if status not in {"completed", "degraded", "failed", "dead_letter", "retry_wait", "interrupted", "cancelled"}:
            raise ValueError("invalid release status")
        at = now_iso()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT slot_id, run_id FROM execution_slots WHERE lease_id=? AND status='leased'", (lease_id,)).fetchone()
            if row is None:
                return False
            run_id = row[1]
            connection.execute("UPDATE execution_slots SET status='free', lease_id=NULL, run_id=NULL, profile=NULL, leased_at=NULL, heartbeat_at=NULL, expires_at=NULL, pid=NULL, process_group_id=NULL WHERE lease_id=?", (lease_id,))
            job = connection.execute(
                "SELECT attempt, occurrence_id, deadline_at, payload_json, job_id, schedule_key, registry_hash FROM jobs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            attempt = int(job[0]) if job is not None else 0
            next_attempt_at = None
            next_attempt = attempt
            effective_status = status
            effective_error = error
            if status == "failed" and job is not None and job[1]:
                _concurrency_key, retry = _scheduled_policy(job[3])
                # A missing retry policy is legacy behavior: retain ``failed``
                # rather than silently changing manually accepted fixtures.
                no_retry_after_effect = str(error or "") in {"no_retry_after_effect", "delivery_uncertain"} or "no_retry_after_effect" in str(error or "")
                if retry is not None and not no_retry_after_effect:
                    max_attempts, backoff_seconds = _retry_policy(retry)
                    deadline = _parse_iso(job[2])
                    retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
                    if attempt < max_attempts and (deadline is None or retry_at <= deadline):
                        effective_status = "retry_wait"
                        next_attempt = attempt + 1
                        next_attempt_at = retry_at.isoformat(timespec="seconds").replace("+00:00", "Z")
                    else:
                        effective_status = "dead_letter"
                        effective_error = effective_error or (
                            "retry_deadline_exceeded" if deadline is not None and retry_at > deadline else "retry_exhausted"
                        )
            if effective_status == "retry_wait" and next_attempt_at is None:
                next_attempt = attempt + 1 if increment_attempt else attempt
                delay = max(0, int(retry_after_seconds)) if retry_after_seconds is not None else min(120, 2 ** max(next_attempt - 1, 0) * 2)
                next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat(timespec="seconds").replace("+00:00", "Z")
            connection.execute(
                "UPDATE jobs SET status=?, attempt=?, completed_at=CASE WHEN ? IN ('completed','failed','dead_letter','degraded','cancelled') THEN ? ELSE completed_at END, next_attempt_at=?, updated_at=?, error_fingerprint=COALESCE(?,error_fingerprint), terminal_reason=CASE WHEN ? IN ('failed','dead_letter') THEN COALESCE(?,terminal_reason) ELSE terminal_reason END WHERE run_id=?",
                (effective_status, next_attempt, effective_status, at, next_attempt_at, at, effective_error, effective_status, effective_error, run_id),
            )
            connection.execute(
                "UPDATE runs SET status=?, completed_at=CASE WHEN ? IN ('completed','failed','dead_letter','degraded','cancelled') THEN ? ELSE completed_at END, updated_at=?, error=COALESCE(?,error) WHERE run_id=?",
                (effective_status, effective_status, at, at, effective_error, run_id),
            )
            if job is not None and job[1]:
                occurrence_state = "accepted" if effective_status == "retry_wait" else (
                    "completed" if effective_status == "completed" else "dead_letter" if effective_status == "dead_letter" else "failed"
                )
                occurrence_reason = effective_error
                if effective_status in {"interrupted", "cancelled", "degraded"} and not occurrence_reason:
                    occurrence_reason = effective_status
                self.store._update_schedule_occurrence_unlocked(
                    connection,
                    str(job[1]),
                    state=occurrence_state,
                    reason=occurrence_reason,
                    next_retry_at=next_attempt_at,
                )
            seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]) + 1
            connection.execute("INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)", (run_id, seq, f"run/{effective_status}", "scheduler", json.dumps({"lease_id": lease_id, "error": effective_error, "attempt": next_attempt, "next_attempt_at": next_attempt_at}, separators=(",", ":")), at))
            self.store._record_activity_unlocked(
                connection,
                event_type=f"run/{effective_status}",
                actor="scheduler",
                payload={"lease_id": lease_id, "error": effective_error, "attempt": next_attempt, "next_attempt_at": next_attempt_at, "run_id": run_id},
                run_id=run_id,
                job_id=job[4] if job is not None else None,
                occurrence_id=job[1] if job is not None else None,
                source_cursor=f"{run_id}:{seq}",
                idempotency_key=f"run:{run_id}:{seq}",
                occurred_at=at,
            )
            if job is not None and job[1]:
                schedule_key = str(job[5] or "") or None
                terminal = effective_status in {"completed", "failed", "dead_letter", "cancelled", "degraded"}
                self.store._upsert_operation_unlocked(
                    connection,
                    operation_key=f"schedule:{schedule_key}" if schedule_key else f"run:{run_id}",
                    module_id="Scheduler",
                    schedule_key=schedule_key,
                    process="pm-system-worker",
                    heartbeat_at=at,
                    current_run=None if terminal else run_id,
                    status="healthy" if effective_status == "completed" else "incident" if effective_status in {"failed", "dead_letter"} else "unknown" if not terminal else "degraded",
                    freshness="fresh",
                    reconcile_state="clear" if effective_status == "completed" else "required" if effective_status in {"failed", "dead_letter"} else "unknown",
                    last_exit_code=0 if effective_status == "completed" else None,
                    evidence_refs=[f"run_events:{run_id}:{seq}"],
                    source_version=job[6],
                    observed_at=at,
                    updated_at=at,
                )
                if terminal:
                    self.store._upsert_review_unlocked(connection, str(run_id), updated_at=at)
            return True

    def cancel(self, run_id: str, *, reason: str = "cancelled") -> bool:
        at = now_iso()
        process_targets: list[tuple[Optional[int], Optional[int]]] = []
        with self.store.transaction() as connection:
            row = connection.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row[0] in {"completed", "failed", "cancelled", "degraded"}:
                return False
            for process_row in connection.execute("SELECT pid,process_group_id FROM execution_slots WHERE run_id=? AND status='leased'", (run_id,)).fetchall():
                process_targets.append((process_row[0], process_row[1]))
            connection.execute(
                "INSERT INTO cancellation_intents(run_id,requested_at,reason,actor) VALUES(?,?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET requested_at=excluded.requested_at,reason=excluded.reason,actor=excluded.actor",
                (run_id, at, reason, "scheduler"),
            )
            connection.execute("UPDATE jobs SET status='cancelled', completed_at=?, updated_at=? WHERE run_id=?", (at, at, run_id))
            connection.execute("UPDATE runs SET status='cancelled', completed_at=?, updated_at=?, error=? WHERE run_id=?", (at, at, reason, run_id))
            occurrence = connection.execute("SELECT occurrence_id FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if occurrence is not None and occurrence[0]:
                self.store._update_schedule_occurrence_unlocked(
                    connection,
                    str(occurrence[0]),
                    state="failed",
                    reason=reason or "cancelled",
                )
            connection.execute("UPDATE execution_slots SET status='free', lease_id=NULL, run_id=NULL, profile=NULL, leased_at=NULL, heartbeat_at=NULL, expires_at=NULL, pid=NULL, process_group_id=NULL WHERE run_id=?", (run_id,))
            seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]) + 1
            connection.execute("INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)", (run_id, seq, "run/cancelled", "scheduler", json.dumps({"reason": reason}, separators=(",", ":")), at))
            self.store._record_activity_unlocked(
                connection,
                event_type="run/cancelled",
                actor="scheduler",
                payload={"reason": reason, "run_id": run_id},
                run_id=run_id,
                occurrence_id=occurrence[0] if occurrence is not None else None,
                source_cursor=f"{run_id}:{seq}",
                idempotency_key=f"run:{run_id}:{seq}",
                occurred_at=at,
            )
            self.store._upsert_review_unlocked(connection, str(run_id), updated_at=at)
        for pid, process_group_id in process_targets:
            self._terminate_orphan(pid, process_group_id)
        return True

    def begin_model_call(
        self,
        run_id: str,
        *,
        stage: str,
        model_input_hash: str,
        prompt_version: str,
        provider: str,
        endpoint: str = "default",
        model: str = "default",
    ) -> Dict[str, Any]:
        if not model_input_hash or not prompt_version or not provider:
            raise ValueError("model_input_hash, prompt_version and provider are required")
        at = now_iso()
        call_id = _call_id()
        with self.store.transaction() as connection:
            freeze = self.store._freeze_blocks(connection)
            if freeze is not None:
                raise MigrationFrozen(f"model admission blocked by migration {freeze[1]}")
            state = connection.execute("SELECT status,deadline_at,profile FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if state is None:
                raise KeyError(run_id)
            if state[0] in {"cancelled", "completed", "failed", "degraded"}:
                raise AdmissionFrozen(f"run {run_id} is {state[0]}")
            current = datetime.now(timezone.utc)
            run_deadline = _parse_iso(state[1])
            if run_deadline is not None and run_deadline <= current:
                raise ModelRetryDeadlineExceeded(f"run deadline exceeded for {run_id}")
            profile = str(state[2] or "interactive")
            attempt = int(connection.execute("SELECT COALESCE(MAX(attempt),0)+1 FROM model_calls WHERE run_id=? AND stage=?", (run_id, stage)).fetchone()[0])
            unknown_count = int(connection.execute("SELECT COUNT(*) FROM model_calls WHERE run_id=? AND stage=? AND status='result_unknown'", (run_id, stage)).fetchone()[0])
            # The initial provider response may be unknown.  Permit exactly
            # one fenced retry (attempt 2); if that retry is also unknown,
            # quarantine/terminate instead of opening an unbounded loop.
            if unknown_count >= 2:
                raise AdmissionFrozen(f"result_unknown retry budget exhausted for {run_id}/{stage}")
            prior_deadline = connection.execute(
                "SELECT MIN(retry_deadline_at) FROM model_calls WHERE run_id=? AND stage=? AND retry_deadline_at IS NOT NULL",
                (run_id, stage),
            ).fetchone()[0]
            retry_deadline_value = _parse_iso(prior_deadline) if prior_deadline else None
            if retry_deadline_value is None:
                retry_deadline_value = current + timedelta(seconds=max(60, int(os.environ.get("PM_V45_MODEL_RETRY_DEADLINE", "900") or "900")))
            if run_deadline is not None:
                retry_deadline_value = min(retry_deadline_value, run_deadline)
            if retry_deadline_value <= current:
                raise ModelRetryDeadlineExceeded(f"model retry deadline exceeded for {run_id}/{stage}")
            retry_deadline = retry_deadline_value.isoformat(timespec="seconds").replace("+00:00", "Z")
            bucket_key = provider_key(provider, endpoint, model, operation="model", profile=profile)
            bucket = connection.execute(
                "SELECT throttle_until FROM provider_buckets WHERE provider_key=?",
                (bucket_key,),
            ).fetchone()
            throttle_until = str(bucket[0]) if bucket and bucket[0] else ""
            throttle_deadline = _parse_iso(throttle_until)
            if throttle_deadline is not None and throttle_deadline > current:
                retry_after_seconds = max(1, int((min(throttle_deadline, retry_deadline_value) - current).total_seconds() + 0.999))
                raise ProviderThrottled(
                    provider_key_value=bucket_key,
                    throttle_until=throttle_until,
                    retry_after_seconds=retry_after_seconds,
                )
            configured_limit = max(1, int(os.environ.get("PM_V45_PROVIDER_MAX_CONCURRENCY", "4") or "4"))
            capacity = connection.execute(
                "SELECT max_concurrency FROM provider_capacity WHERE provider=? AND endpoint=? AND model=?",
                (provider, endpoint, model),
            ).fetchone()
            provider_limit = max(1, int(capacity[0])) if capacity is not None else configured_limit
            connection.execute("UPDATE provider_tokens SET released_at=? WHERE released_at IS NULL AND expires_at<=?", (at, at))
            active_tokens = int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE provider=? AND endpoint=? AND model=? AND released_at IS NULL", (provider, endpoint, model)).fetchone()[0])
            if active_tokens >= provider_limit:
                raise AdmissionFrozen(f"provider global semaphore is full for {provider}")
            token_id = f"provider-token-{uuid.uuid4().hex}"
            token_expires = _future_iso(self.slot_ttl_seconds)
            connection.execute("INSERT INTO provider_tokens(token_id,provider,endpoint,model,owner,acquired_at,expires_at) VALUES(?,?,?,?,?,?,?)", (token_id, provider, endpoint, model, call_id, at, token_expires))
            connection.execute("INSERT INTO model_calls(call_id,run_id,stage,attempt,status,response_state,model_input_hash,prompt_version,provider,provider_token_id,started_at,retry_deadline_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (call_id, run_id, stage, attempt, "running", "waiting", model_input_hash, prompt_version, provider, token_id, at, retry_deadline))
            connection.execute("UPDATE runs SET model_input_hash=?, updated_at=? WHERE run_id=?", (model_input_hash, at, run_id))
            connection.execute("INSERT INTO checkpoints(run_id,stage,checkpoint_key,input_hash,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(run_id,stage,checkpoint_key) DO UPDATE SET input_hash=excluded.input_hash,payload_json=excluded.payload_json,updated_at=excluded.updated_at", (run_id, stage, "model_call", model_input_hash, json.dumps({"call_id": call_id, "attempt": attempt, "prompt_version": prompt_version, "provider": provider, "status": "running"}, separators=(",", ":")), at, at))
            return {"call_id": call_id, "run_id": run_id, "stage": stage, "attempt": attempt, "status": "running", "model_input_hash": model_input_hash, "prompt_version": prompt_version, "provider": provider, "endpoint": endpoint, "model": model, "profile": profile, "provider_bucket_key": bucket_key, "provider_token_id": token_id, "retry_deadline_at": retry_deadline, "prior_result_unknown_count": unknown_count}

    def finish_model_call(self, call_id: str, *, status: str, artifact_uri: Optional[str] = None, error_fingerprint: Optional[str] = None) -> str:
        if status not in {"response_received", "completed", "result_unknown", "retry_wait", "failed", "cancelled"}:
            raise ValueError("invalid model call status")
        at = now_iso()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT run_id, stage, model_input_hash, status, provider_token_id FROM model_calls WHERE call_id=?", (call_id,)).fetchone()
            if row is None:
                raise KeyError(call_id)
            run_id, stage, input_hash, previous_status, provider_token_id = row[0], row[1], row[2], row[3], row[4]
            if previous_status in {"response_received", "completed", "result_unknown", "retry_wait", "failed", "cancelled"}:
                # A late duplicate callback must be observationally idempotent.
                return str(previous_status)
            run_state = connection.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()[0]
            effective = status
            if run_state == "cancelled" and status in {"response_received", "completed"}:
                # Preserve the diagnostic artifact but never advance a
                # cancelled run or its checkpoint.
                effective = "cancelled"
            connection.execute("UPDATE model_calls SET status=?, response_state=?, completed_at=?, artifact_uri=COALESCE(?,artifact_uri), error_fingerprint=COALESCE(?,error_fingerprint) WHERE call_id=?", (effective, effective, at, artifact_uri, error_fingerprint, call_id))
            if provider_token_id:
                connection.execute("UPDATE provider_tokens SET released_at=? WHERE token_id=? AND released_at IS NULL", (at, provider_token_id))
            checkpoint = connection.execute("SELECT payload_json FROM checkpoints WHERE run_id=? AND stage=? AND checkpoint_key='model_call'", (run_id, stage)).fetchone()
            checkpoint_payload: Dict[str, Any] = {}
            if checkpoint:
                try:
                    checkpoint_payload = json.loads(checkpoint[0] or "{}")
                except json.JSONDecodeError:
                    checkpoint_payload = {}
            # Only the current fenced attempt may advance the model checkpoint.
            # An older attempt can still retain its diagnostic artifact above.
            if checkpoint_payload.get("call_id") in {None, call_id}:
                connection.execute("UPDATE checkpoints SET artifact_uri=COALESCE(?,artifact_uri), payload_json=?, updated_at=? WHERE run_id=? AND stage=? AND checkpoint_key='model_call'", (artifact_uri, json.dumps({"call_id": call_id, "model_input_hash": input_hash, "status": effective, "artifact_uri": artifact_uri}, separators=(",", ":")), at, run_id, stage))
            return effective

    @staticmethod
    def _scheduled_completion_evidence(connection: sqlite3.Connection, run_id: str) -> Optional[Dict[str, Any]]:
        """Return a completed scheduled-handler event only when it is authoritative.

        A worker can be stopped after the fixed command has finished but before
        the scheduler release transaction.  The event and artifact are then
        durable, while the run is still marked interrupted.  Recovery is safe
        only when the completion event is the latest event, reports rc=0, and
        its evidence file still exists.
        """
        row = connection.execute(
            "SELECT e.event_type, e.payload_json "
            "FROM run_events AS e JOIN runs AS r ON r.run_id=e.run_id "
            "WHERE e.run_id=? AND r.schedule_key IS NOT NULL "
            "ORDER BY e.seq DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None or row[0] != "scheduled/completed":
            return None
        try:
            payload = json.loads(row[1] or "{}")
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict) or payload.get("failure_reason"):
            return None
        try:
            returncode = int(payload.get("returncode", 1))
        except (TypeError, ValueError):
            return None
        if returncode != 0:
            return None
        artifact = Path(str(payload.get("artifact") or "")).expanduser()
        if not artifact.is_file():
            return None
        return payload

    def startup_reconcile(self, *, active_lease_ids: Optional[Iterable[str]] = None) -> Dict[str, int]:
        """Resolve stale leases/runs without starting new model calls."""
        active = set(active_lease_ids or ())
        at = now_iso()
        counts = {"expired_slots": 0, "interrupted_runs": 0, "completed_from_checkpoint": 0, "completed_from_scheduled_evidence": 0}
        with self.store.transaction() as connection:
            expired = connection.execute("SELECT slot_id, lease_id, run_id, pid, process_group_id FROM execution_slots WHERE status='leased' AND (expires_at IS NULL OR expires_at <= ? OR lease_id NOT IN ({0}))".format(",".join("?" for _ in active) if active else "'__none__'"), (at, *active)).fetchall()
            for slot_id, lease_id, run_id, pid, process_group_id in expired:
                if not run_id:
                    connection.execute("UPDATE execution_slots SET status='free',lease_id=NULL,run_id=NULL,profile=NULL,leased_at=NULL,heartbeat_at=NULL,expires_at=NULL,pid=NULL,process_group_id=NULL WHERE slot_id=?", (slot_id,))
                    self._terminate_orphan(pid, process_group_id)
                    continue
                checkpoint = connection.execute("SELECT payload_json, artifact_uri FROM checkpoints WHERE run_id=? AND stage=(SELECT stage FROM model_calls WHERE run_id=? ORDER BY rowid DESC LIMIT 1) AND checkpoint_key='model_call'", (run_id, run_id)).fetchone()
                payload = {}
                if checkpoint:
                    try:
                        payload = json.loads(checkpoint[0] or "{}")
                    except json.JSONDecodeError:
                        payload = {}
                scheduled_evidence = self._scheduled_completion_evidence(connection, str(run_id))
                if scheduled_evidence is not None:
                    new_status = "completed"
                    counts["completed_from_scheduled_evidence"] += 1
                elif checkpoint and checkpoint[1] and payload.get("status") in {"response_received", "completed"}:
                    new_status = "completed"
                    counts["completed_from_checkpoint"] += 1
                else:
                    new_status = "interrupted"
                    counts["interrupted_runs"] += 1
                connection.execute("UPDATE execution_slots SET status='free',lease_id=NULL,run_id=NULL,profile=NULL,leased_at=NULL,heartbeat_at=NULL,expires_at=NULL,pid=NULL,process_group_id=NULL WHERE slot_id=?", (slot_id,))
                connection.execute("UPDATE jobs SET status=?,updated_at=? WHERE run_id=? AND status='running'", (new_status, at, run_id))
                connection.execute("UPDATE runs SET status=?,updated_at=?,completed_at=CASE WHEN ?='completed' THEN ? ELSE completed_at END WHERE run_id=? AND status='running'", (new_status, at, new_status, at, run_id))
                occurrence = connection.execute("SELECT occurrence_id FROM runs WHERE run_id=?", (run_id,)).fetchone()
                if occurrence is not None and occurrence[0]:
                    self.store._update_schedule_occurrence_unlocked(
                        connection,
                        str(occurrence[0]),
                        state="completed" if new_status == "completed" else "failed",
                        reason=None if new_status == "completed" else "worker_interrupted",
                    )
                if new_status == "completed" and scheduled_evidence is not None:
                    seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]) + 1
                    job_id_row = connection.execute("SELECT job_id FROM jobs WHERE run_id=?", (run_id,)).fetchone()
                    connection.execute(
                        "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                        (run_id, seq, "run/reconciled_completed", "scheduler", json.dumps({"reason": "scheduled_completion_evidence", "job_id": job_id_row[0] if job_id_row else None, "occurrence_id": occurrence[0] if occurrence is not None else None}, separators=(",", ":")), at),
                    )
                connection.execute(
                    "UPDATE provider_tokens SET released_at=? WHERE owner IN (SELECT call_id FROM model_calls WHERE run_id=?) AND released_at IS NULL",
                    (at, run_id),
                )
                self._terminate_orphan(pid, process_group_id)
            counts["expired_slots"] = len(expired)
            # Repair a narrow crash window where the scheduled command wrote a
            # durable success event/artifact but the final release was skipped.
            # This also repairs historical interrupted rows discovered after a
            # worker restart; the appended event preserves the original trail.
            stale_scheduled = connection.execute(
                "SELECT run_id, job_id, occurrence_id FROM runs "
                "WHERE schedule_key IS NOT NULL AND status='interrupted'"
            ).fetchall()
            for run_id, job_id, occurrence_id in stale_scheduled:
                if self._scheduled_completion_evidence(connection, str(run_id)) is None:
                    continue
                connection.execute("UPDATE jobs SET status='completed',completed_at=COALESCE(completed_at,?),updated_at=?,error_fingerprint=NULL,terminal_reason=NULL WHERE job_id=? AND status IN ('interrupted','failed','dead_letter')", (at, at, job_id))
                connection.execute("UPDATE runs SET status='completed',completed_at=COALESCE(completed_at,?),updated_at=?,error=NULL,terminal_reason=NULL WHERE run_id=? AND status='interrupted'", (at, at, run_id))
                if occurrence_id:
                    self.store._update_schedule_occurrence_unlocked(connection, str(occurrence_id), state="completed", reason=None)
                seq = int(connection.execute("SELECT COALESCE(MAX(seq),0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]) + 1
                connection.execute(
                    "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                    (run_id, seq, "run/reconciled_completed", "scheduler", json.dumps({"reason": "scheduled_completion_evidence", "job_id": job_id, "occurrence_id": occurrence_id}, separators=(",", ":")), at),
                )
                self.store._record_activity_unlocked(
                    connection,
                    event_type="run/reconciled_completed",
                    actor="scheduler",
                    payload={"reason": "scheduled_completion_evidence", "run_id": run_id, "job_id": job_id, "occurrence_id": occurrence_id},
                    run_id=run_id,
                    job_id=job_id,
                    occurrence_id=occurrence_id,
                    source_cursor=f"{run_id}:{seq}",
                )
                counts["completed_from_scheduled_evidence"] += 1
        return counts

    def slot_snapshot(self) -> List[Dict[str, Any]]:
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM execution_slots ORDER BY slot_id").fetchall()]


__all__ = ["AdmissionFrozen", "ModelRetryDeadlineExceeded", "ProviderThrottled", "DEFAULT_MAX_CODEX_SLOTS", "DEFAULT_SLOT_TTL_SECONDS", "Scheduler", "admission_from_env", "max_slots_from_env"]
