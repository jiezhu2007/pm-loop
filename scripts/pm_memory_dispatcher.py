#!/usr/bin/env python3
"""Dedicated dispatcher for local Memory Markdown file mirrors.

The watcher owns local file detection and durable Outbox admission.  This
module owns only the ``memory-skill`` lane and talks to OpenViking's
``/content/write`` API; Resource ingestion and Conversation Extraction remain
separate features.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from pm_resource_dispatcher import (
    DispatchHTTPError,
    DispatchProtocolError,
    DispatchTimeoutError,
    DispatchTransportError,
    OpenVikingTransport,
    OBSERVATION_ACTIVE,
    OBSERVATION_TERMINAL_FAILURE,
    OBSERVATION_TERMINAL_SUCCESS,
    _content_hash_from_response,
    _task_id,
    _walk_status,
)
from pm_system_store import PMSystemStore, now_iso


MEMORY_ROOT = "viking://resources/memory"
TRANSIENT_HTTP_STATUSES = {408, 425, 500, 502, 503, 504}
RETRYABLE = {"connection", "timeout", "transient", "429"}


class MemoryQuarantineError(RuntimeError):
    """The remote state cannot be proven safe to replay."""


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _parse_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class MemorySkillWriter:
    """Single-writer state machine for local Markdown -> OpenViking content."""

    def __init__(
        self,
        store: PMSystemStore,
        *,
        transport: Optional[OpenVikingTransport] = None,
        max_attempts: int = 3,
        observation_max_attempts: int = 3,
        observation_deadline_seconds: int = 3600,
        observation_backoff_seconds: int = 30,
        dispatch_lease_seconds: int = 300,
    ) -> None:
        self.store = store
        self.transport = transport or OpenVikingTransport()
        self.max_attempts = max(1, int(max_attempts))
        self.observation_max_attempts = max(1, int(observation_max_attempts))
        self.observation_deadline_seconds = max(1, int(observation_deadline_seconds))
        self.observation_backoff_seconds = max(0, int(observation_backoff_seconds))
        self.dispatch_lease_seconds = max(1, int(dispatch_lease_seconds))

    def submit_file(
        self,
        *,
        path: Path,
        target_uri: Optional[str] = None,
        namespace_epoch: str = "v4",
    ) -> Dict[str, Any]:
        source = Path(path).expanduser().resolve()
        content = source.read_text(encoding="utf-8")
        name = source.name
        target = target_uri or f"{MEMORY_ROOT}/{name}/{name}"
        if not target.startswith(MEMORY_ROOT + "/"):
            raise ValueError("MemorySkillWriter requires a viking://resources/memory target")
        return self.store.enqueue_memory_change(
            name=name,
            mtime=source.stat().st_mtime_ns,
            content_hash=_sha256_text(content),
            snapshot_uri=target,
            file_path=str(source),
            namespace_epoch=namespace_epoch,
        )

    @staticmethod
    def _logical_key(item: Mapping[str, Any]) -> str:
        return str(item.get("idempotency_key") or item.get("outbox_id") or "")

    @staticmethod
    def _payload(item: Mapping[str, Any]) -> Dict[str, Any]:
        raw = item.get("payload_json")
        if isinstance(raw, Mapping):
            return dict(raw)
        try:
            value = json.loads(str(raw or "{}"))
        except json.JSONDecodeError:
            value = {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _fingerprint(category: str, detail: Any = "") -> str:
        return hashlib.sha256(f"memory:{category}:{detail}".encode("utf-8")).hexdigest()[:20]

    def _claim(self, outbox_ids: Optional[Sequence[str]] = None) -> Optional[Dict[str, Any]]:
        at = now_iso()
        with self.store.transaction() as connection:
            # A crashed writer must not strand a Memory item in flight.
            expired = connection.execute(
                "SELECT outbox_id FROM outbox_dispatch_leases WHERE expires_at<=?",
                (at,),
            ).fetchall()
            for row in expired:
                connection.execute(
                    "UPDATE outbox_items SET status='retry_wait',next_attempt_at=?,updated_at=? "
                    "WHERE outbox_id=? AND kind='memory' AND status IN ('in_flight','writing','awaiting_task','readback')",
                    (at, at, row[0]),
                )
                connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (row[0],))
            id_clause = ""
            id_params: tuple[str, ...] = ()
            if outbox_ids is not None:
                selected_ids = [str(value) for value in outbox_ids if str(value)]
                if not selected_ids:
                    return None
                id_clause = " AND outbox_id IN (" + ",".join("?" for _ in selected_ids) + ")"
                id_params = tuple(selected_ids)
            row = connection.execute(
                """
                SELECT outbox_id,idempotency_key,kind,resource_id,revision_id,processing_mode,
                       provider,profile,owner,namespace_epoch,payload_json,attempt,retry_deadline_at,created_at
                FROM outbox_items
                WHERE kind='memory' AND profile='memory-skill'
                  AND status IN ('pending','retry_wait')
                  AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                """ + id_clause + " ORDER BY rowid LIMIT 1",
                (at,) + id_params,
            ).fetchone()
            if row is None:
                return None
            token = f"memory-dispatch-{os.getpid()}-{hashlib.sha256(f'{row[0]}:{at}'.encode()).hexdigest()[:16]}"
            expires = _timestamp(datetime.now(timezone.utc) + timedelta(seconds=self.dispatch_lease_seconds))
            updated = connection.execute(
                "UPDATE outbox_items SET status='in_flight',updated_at=? WHERE outbox_id=? AND status IN ('pending','retry_wait')",
                (at, row[0]),
            )
            if updated.rowcount != 1:
                return None
            connection.execute(
                "INSERT OR REPLACE INTO outbox_dispatch_leases(outbox_id,dispatch_token,owner,leased_at,expires_at) VALUES(?,?,?,?,?)",
                (row[0], token, f"memory-writer-{os.getpid()}", at, expires),
            )
            item = dict(row)
            item["dispatch_token"] = token
            item["dispatch_expires_at"] = expires
            return item

    def _upsert_projection(self, item: Mapping[str, Any], payload: Mapping[str, Any], **changes: Any) -> None:
        at = now_iso()
        values = {
            "resource_id": str(item.get("resource_id") or ""),
            "revision_id": str(item.get("revision_id") or ""),
            "target_uri": str(payload.get("target_uri") or item.get("resource_id") or ""),
            "local_hash": str(payload.get("content_hash") or item.get("revision_id") or ""),
            "content_state": "content_pending",
            "remote_hash": None,
            "openviking_task_id": None,
            "operation_id": None,
            "observation_attempt": 0,
            "next_observation_at": None,
            "observation_deadline_at": None,
            "terminal_reason": None,
            "verified_at": None,
        }
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM memory_projections WHERE resource_id=? AND revision_id=?",
                (values["resource_id"], values["revision_id"]),
            ).fetchone()
            if existing is not None:
                for key in values:
                    if key in existing.keys() and existing[key] is not None:
                        values[key] = existing[key]
            values.update(changes)
            connection.execute(
                """INSERT INTO memory_projections(
                    resource_id,revision_id,target_uri,content_state,local_hash,remote_hash,
                    openviking_task_id,operation_id,observation_attempt,next_observation_at,
                    observation_deadline_at,terminal_reason,verified_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(resource_id,revision_id) DO UPDATE SET
                    target_uri=excluded.target_uri,content_state=excluded.content_state,
                    local_hash=excluded.local_hash,remote_hash=excluded.remote_hash,
                    openviking_task_id=excluded.openviking_task_id,operation_id=excluded.operation_id,
                    observation_attempt=excluded.observation_attempt,next_observation_at=excluded.next_observation_at,
                    observation_deadline_at=excluded.observation_deadline_at,terminal_reason=excluded.terminal_reason,
                    verified_at=excluded.verified_at,updated_at=excluded.updated_at""",
                tuple(values[key] for key in (
                    "resource_id", "revision_id", "target_uri", "content_state", "local_hash", "remote_hash",
                    "openviking_task_id", "operation_id", "observation_attempt", "next_observation_at",
                    "observation_deadline_at", "terminal_reason", "verified_at",
                )) + (at,),
            )

    def _read_back(self, item: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("target_uri") or item.get("resource_id") or "")
        expected = str(payload.get("content_hash") or item.get("revision_id") or "").lower().removeprefix("sha256:")
        try:
            response = self.transport.read_content(target, timeout=min(float(payload.get("timeout") or 30), 10.0))
        except DispatchHTTPError as exc:
            if exc.status_code == 404:
                return {"verified": False, "status": "not_found", "target_uri": target}
            return {"verified": False, "status": "unknown", "error": str(exc), "target_uri": target}
        except (DispatchTransportError, OSError) as exc:
            return {"verified": False, "status": "unknown", "error": str(exc), "target_uri": target}
        actual = _content_hash_from_response(response)
        verified = bool(actual and actual.lower() == expected)
        return {"verified": verified, "status": "verified" if verified else "mismatch", "expected_hash": expected, "actual_hash": actual, "target_uri": target}

    @staticmethod
    def _parent_uri(target_uri: str) -> str:
        normalized = str(target_uri or "").rstrip("/")
        if "/" not in normalized:
            return ""
        return normalized.rsplit("/", 1)[0]

    def _prepare_missing_target(self, target_uri: str, *, timeout: float) -> Optional[str]:
        """Return ``create`` for a missing file, creating its parent once."""
        parent = self._parent_uri(target_uri)
        if not parent:
            return None

        # fs/stat distinguishes a missing URI from an existing empty directory;
        # fs/ls returns 200 + [] for both on OpenViking 0.4.x.
        if hasattr(self.transport, "stat_uri"):
            try:
                self.transport.stat_uri(target_uri, timeout=min(timeout, 10.0))
                return None
            except DispatchHTTPError as exc:
                if exc.status_code != 404:
                    return None
            except (DispatchTransportError, OSError):
                return None
            if hasattr(self.transport, "stat_uri"):
                try:
                    self.transport.stat_uri(parent, timeout=min(timeout, 10.0))
                    return "create"
                except DispatchHTTPError as exc:
                    if exc.status_code != 404:
                        return None
                    if not hasattr(self.transport, "mkdir"):
                        return None
                    try:
                        self.transport.mkdir(parent, timeout=min(timeout, 10.0))
                    except DispatchHTTPError as mkdir_exc:
                        # Another writer may have created the directory
                        # between stat and mkdir; that race is harmless.
                        if mkdir_exc.status_code != 409:
                            return None
                    return "create"
                except (DispatchTransportError, OSError):
                    return None

        # Backward-compatible fake/older transports without fs/stat.
        if not hasattr(self.transport, "list_uri") or not hasattr(self.transport, "mkdir"):
            return None
        try:
            self.transport.list_uri(parent, timeout=min(timeout, 10.0))
        except DispatchHTTPError as exc:
            if exc.status_code != 404:
                return None
            try:
                self.transport.mkdir(parent, timeout=min(timeout, 10.0))
            except DispatchHTTPError as mkdir_exc:
                if mkdir_exc.status_code != 409:
                    return None
            return "create"
        except (DispatchTransportError, OSError):
            return None
        return None

    def _superseded(self, item: Mapping[str, Any]) -> bool:
        """Prevent an older edit from overwriting a newer admitted revision."""
        with self.store.connect() as connection:
            current = connection.execute(
                "SELECT rowid FROM outbox_items WHERE outbox_id=?",
                (str(item.get("outbox_id") or ""),),
            ).fetchone()
            if current is None:
                return False
            newer = connection.execute(
                "SELECT 1 FROM outbox_items WHERE kind='memory' AND profile='memory-skill' "
                "AND resource_id=? AND outbox_id<>? AND rowid>? "
                "AND revision_id<>? LIMIT 1",
                (str(item.get("resource_id") or ""), str(item.get("outbox_id") or ""), int(current[0]), str(item.get("revision_id") or "")),
            ).fetchone()
            return newer is not None

    def _complete(self, item: Mapping[str, Any], payload: Mapping[str, Any], read_back: Mapping[str, Any]) -> Dict[str, Any]:
        if self._superseded(item):
            return self._fail(item, payload, "superseded_revision", "newer revision admitted")
        at = now_iso()
        operation_id = None
        with self.store.transaction() as connection:
            projection = connection.execute(
                "SELECT operation_id FROM memory_projections WHERE resource_id=? AND revision_id=?",
                (str(item.get("resource_id") or ""), str(item.get("revision_id") or "")),
            ).fetchone()
            operation_id = projection[0] if projection is not None else None
            connection.execute(
                "UPDATE outbox_items SET status='completed',next_attempt_at=NULL,terminal_reason=NULL,updated_at=? WHERE outbox_id=? AND status IN ('in_flight','awaiting_task','readback')",
                (at, item["outbox_id"]),
            )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (item["outbox_id"],))
            connection.execute(
                "UPDATE memory_change_events SET state='consumed',consumed_at=? WHERE event_id=? AND consumed_at IS NULL",
                (at, payload.get("event_id")),
            )
        if operation_id:
            self.store.finish_operation(str(operation_id), response_state="completed", response={"content_read_back": dict(read_back)})
        self._upsert_projection(item, payload, content_state="completed", remote_hash=read_back.get("actual_hash"), verified_at=at, terminal_reason=None, next_observation_at=None)
        return {"status": "completed", "content_read_back": dict(read_back)}

    def _fail(self, item: Mapping[str, Any], payload: Mapping[str, Any], category: str, detail: Any = "") -> Dict[str, Any]:
        category = str(category or "unknown").lower()
        at_dt = datetime.now(timezone.utc)
        at = _timestamp(at_dt)
        attempt = int(item.get("attempt") or 0)
        deadline = _parse_timestamp(item.get("retry_deadline_at"))
        if deadline is not None and deadline <= at_dt and category in RETRYABLE:
            status, next_at, new_attempt = "dead_letter", None, attempt
            category = "retry_deadline_exceeded"
        elif category == "429":
            status, next_at, new_attempt = "retry_wait", _timestamp(at_dt + timedelta(seconds=30)), attempt
        elif category in RETRYABLE:
            new_attempt = attempt + 1
            if new_attempt > self.max_attempts:
                status, next_at = "dead_letter", None
            else:
                status, next_at = "retry_wait", _timestamp(at_dt + timedelta(seconds=min(120, 2 ** (new_attempt - 1) * 2)))
        elif category in {"quarantine", "task_not_found", "readback_unknown", "superseded_revision"}:
            status, next_at, new_attempt = "quarantine", None, attempt
        else:
            status, next_at, new_attempt = "failed", None, attempt
        fingerprint = self._fingerprint(category, detail)
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE outbox_items SET status=?,attempt=?,next_attempt_at=?,error_fingerprint=?,terminal_reason=?,updated_at=? WHERE outbox_id=? AND status IN ('in_flight','awaiting_task','readback')",
                (status, new_attempt, next_at, fingerprint, category, at, item["outbox_id"]),
            )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (item["outbox_id"],))
            if status in {"failed", "dead_letter", "quarantine"}:
                connection.execute(
                    "UPDATE memory_change_events SET state='quarantine',consumed_at=? WHERE event_id=? AND consumed_at IS NULL",
                    (at, payload.get("event_id")),
                )
        self._upsert_projection(item, payload, content_state=status, terminal_reason=category, next_observation_at=None)
        return {"status": status, "attempt": new_attempt, "error_fingerprint": fingerprint, "terminal_reason": category, "next_attempt_at": next_at}

    def _write(self, item: Mapping[str, Any]) -> Dict[str, Any]:
        payload = self._payload(item)
        path = Path(str(payload.get("file_path") or ""))
        if not path.is_file():
            return self._fail(item, payload, "permanent", "memory source file is missing")
        content = path.read_text(encoding="utf-8")
        expected = _sha256_text(content)
        if self._superseded(item):
            return self._fail(item, payload, "superseded_revision", "newer revision admitted")
        if expected != str(item.get("revision_id") or payload.get("content_hash") or ""):
            return self._fail(item, payload, "quarantine", "memory source hash changed after admission")
        logical = self._logical_key(item)
        attempt = max(1, int(item.get("attempt") or 0) + 1)
        ledger = self.store.begin_operation(
            operation_type="content_write",
            idempotency_key=logical,
            target_uri=str(payload.get("target_uri") or item.get("resource_id") or ""),
            request_hash=expected,
            namespace_epoch=str(item.get("namespace_epoch") or "v4"),
            attempt=attempt,
        )
        if ledger.get("response_state") in {"accepted", "completed"}:
            try:
                response = json.loads(str(ledger.get("response_json") or "{}"))
            except json.JSONDecodeError:
                response = {}
        else:
            target_uri = str(payload.get("target_uri") or item.get("resource_id") or "")
            write_retried_after_mkdir = False
            write_mode = "replace"
            try:
                while True:
                    try:
                        response = self.transport.write_content(
                            target_uri,
                            content,
                            mode=write_mode,
                            processing_mode="vectors_only",
                            wait=False,
                            timeout=float(payload.get("timeout") or 30),
                            idempotency_key=logical,
                        )
                        break
                    except DispatchHTTPError as exc:
                        if (
                            exc.status_code == 404
                            and not write_retried_after_mkdir
                            and (missing_mode := self._prepare_missing_target(target_uri, timeout=float(payload.get("timeout") or 30)))
                        ):
                            write_retried_after_mkdir = True
                            write_mode = missing_mode
                            continue
                        self.store.finish_operation(str(ledger["operation_id"]), response_state="unknown", response={"error": str(exc)})
                        if exc.status_code == 429:
                            return self._fail(item, payload, "429", exc.body)
                        if exc.status_code in TRANSIENT_HTTP_STATUSES:
                            read_back = self._read_back(item, payload)
                            if read_back.get("verified"):
                                self.store.finish_operation(str(ledger["operation_id"]), response_state="completed", response={"recovered_by": "read_back", "content_read_back": read_back})
                                return self._complete(item, payload, read_back)
                            with self.store.connect() as connection:
                                unknown_count = int(connection.execute(
                                    "SELECT COUNT(*) FROM operation_ledger WHERE operation_type='content_write' AND idempotency_key=? AND response_state='unknown'",
                                    (logical,),
                                ).fetchone()[0])
                            if unknown_count > 1:
                                return self._fail(item, payload, "quarantine", "content/write response unknown after controlled resend")
                            return self._fail(item, payload, "transient", exc.body)
                        return self._fail(item, payload, "permanent", exc.body)
            except (DispatchTimeoutError, DispatchTransportError, OSError) as exc:
                self.store.finish_operation(str(ledger["operation_id"]), response_state="unknown", response={"error": str(exc)})
                read_back = self._read_back(item, payload)
                if read_back.get("verified"):
                    self.store.finish_operation(str(ledger["operation_id"]), response_state="completed", response={"recovered_by": "read_back", "content_read_back": read_back})
                    return self._complete(item, payload, read_back)
                with self.store.connect() as connection:
                    unknown_count = int(connection.execute(
                        "SELECT COUNT(*) FROM operation_ledger WHERE operation_type='content_write' AND idempotency_key=? AND response_state='unknown'",
                        (logical,),
                    ).fetchone()[0])
                if unknown_count > 1:
                    return self._fail(item, payload, "quarantine", "content/write response unknown after controlled resend")
                return self._fail(item, payload, "timeout" if isinstance(exc, DispatchTimeoutError) else "connection", str(exc))
            self.store.finish_operation(str(ledger["operation_id"]), response_state="accepted", response=response)
        status, task = _walk_status(response)
        task_id = task or _task_id(response)
        if status in OBSERVATION_ACTIVE:
            if not task_id:
                return self._fail(item, payload, "quarantine", "accepted content/write response has no task id")
            now = datetime.now(timezone.utc)
            deadline = _timestamp(now + timedelta(seconds=self.observation_deadline_seconds))
            self._upsert_projection(item, payload, content_state="awaiting_task", openviking_task_id=task_id, operation_id=ledger.get("operation_id"), observation_attempt=0, next_observation_at=now_iso(), observation_deadline_at=deadline)
            with self.store.transaction() as connection:
                connection.execute("UPDATE outbox_items SET status='awaiting_task',updated_at=? WHERE outbox_id=?", (now_iso(), item["outbox_id"]))
                connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (item["outbox_id"],))
            return {"status": "awaiting_task", "openviking_task_id": task_id}
        if status not in OBSERVATION_TERMINAL_SUCCESS and status not in {"", "ok", "created", "success"}:
            return self._fail(item, payload, "permanent", f"content/write status={status or 'missing'}")
        read_back = self._read_back(item, payload)
        if read_back.get("verified"):
            self.store.finish_operation(str(ledger["operation_id"]), response_state="completed", response=response)
            return self._complete(item, payload, read_back)
        return self._fail(item, payload, "readback_unknown", read_back.get("status"))

    def dispatch_pending(self, *, limit: int = 20, outbox_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for _ in range(max(0, int(limit))):
            item = self._claim(outbox_ids=outbox_ids)
            if item is None:
                break
            try:
                result = self._write(item)
            except Exception as exc:  # keep one item from stopping the lane
                result = self._fail(item, self._payload(item), "connection", str(exc))
            results.append({**item, **result})
        return results

    def reconcile_tasks(self, *, limit: int = 20, outbox_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        selected_ids = [str(value) for value in (outbox_ids or ()) if str(value)]
        if outbox_ids is not None and not selected_ids:
            return []
        id_clause = ""
        id_params: tuple[str, ...] = ()
        if outbox_ids is not None:
            id_clause = " AND o.outbox_id IN (" + ",".join("?" for _ in selected_ids) + ")"
            id_params = tuple(selected_ids)
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT o.outbox_id,o.idempotency_key,o.resource_id,o.revision_id,o.namespace_epoch,
                          o.payload_json,o.attempt,p.openviking_task_id,p.observation_attempt,
                          p.next_observation_at,p.observation_deadline_at
                   FROM outbox_items o JOIN memory_projections p
                     ON p.resource_id=o.resource_id AND p.revision_id=o.revision_id
                  WHERE o.kind='memory' AND o.profile='memory-skill' AND o.status='awaiting_task'
                    AND (p.next_observation_at IS NULL OR p.next_observation_at<=?)
                  """ + id_clause + " ORDER BY o.updated_at LIMIT ?""",
                (now_iso(),) + id_params + (max(0, int(limit)),),
            ).fetchall()
        observed: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload = self._payload(item)
            task_id = str(row[7] or "")
            attempt = int(row[8] or 0) + 1
            deadline = _parse_timestamp(row[10])
            if deadline is not None and deadline <= datetime.now(timezone.utc):
                observed.append({**item, **self._fail(item, payload, "quarantine", "task observation deadline exceeded")})
                continue
            query_error = False
            try:
                response = self.transport.get_task(task_id, timeout=min(float(payload.get("timeout") or 30), 10.0))
                status, _ = _walk_status(response)
            except DispatchHTTPError as exc:
                if exc.status_code == 404:
                    result = self._fail(item, payload, "task_not_found", "OpenViking task not found")
                else:
                    status = "unknown"
                    query_error = True
                    result = None
            except (DispatchTransportError, OSError) as exc:
                status = "unknown"
                query_error = True
                result = None
            else:
                result = None
            if query_error:
                if attempt >= self.observation_max_attempts:
                    result = self._fail(item, payload, "quarantine", "task status unknown after observation budget")
                else:
                    next_at = _timestamp(datetime.now(timezone.utc) + timedelta(seconds=self.observation_backoff_seconds))
                    self._upsert_projection(item, payload, content_state="awaiting_task", observation_attempt=attempt, next_observation_at=next_at, terminal_reason="task_status_unknown")
                    result = {"status": "awaiting_task", "observation_attempt": attempt, "next_observation_at": next_at}
            elif result is not None and result.get("status") == "quarantine":
                pass
            elif status in OBSERVATION_TERMINAL_FAILURE:
                result = self._fail(item, payload, "quarantine", f"task status={status}")
            elif status in OBSERVATION_TERMINAL_SUCCESS:
                read_back = self._read_back(item, payload)
                if read_back.get("verified"):
                    result = self._complete(item, payload, read_back)
                else:
                    result = self._fail(item, payload, "readback_unknown", read_back.get("status"))
            else:
                next_at = _timestamp(datetime.now(timezone.utc) + timedelta(seconds=self.observation_backoff_seconds))
                if attempt >= self.observation_max_attempts:
                    result = self._fail(item, payload, "quarantine", "task status unknown after observation budget")
                else:
                    self._upsert_projection(item, payload, content_state="awaiting_task", observation_attempt=attempt, next_observation_at=next_at, terminal_reason="task_status_unknown")
                    result = {"status": "awaiting_task", "observation_attempt": attempt, "next_observation_at": next_at}
            observed.append({**item, **result})
        return observed


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path.home() / ".codex/pm-loop/state/pm-system.db")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    writer = MemorySkillWriter(PMSystemStore(args.db_path))
    result = writer.dispatch_pending(limit=args.limit)
    result.extend(writer.reconcile_tasks(limit=args.limit))
    print(json.dumps({"status": "ok", "processed": len(result), "items": result}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
