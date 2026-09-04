#!/usr/bin/env python3
"""Core run/event persistence for the local Codex-only PM Loop control plane."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from uuid import uuid4


RUN_REQUEST_SCHEMA = "pm-loop.run-request.v1"
EVENT_SCHEMA = "pm-loop.event.v1"
STATE_SCHEMA = "pm-loop.run-state.v1"
TERMINAL_STATES = {"completed", "failed", "cancelled", "rejected"}


_RUN_THREAD_LOCKS: Dict[str, threading.RLock] = {}
_RUN_THREAD_LOCKS_GUARD = threading.Lock()


def _run_thread_lock(path: Path) -> threading.RLock:
    """Share one in-process lock across every RunStore for the same Run."""
    key = str(path.resolve())
    with _RUN_THREAD_LOCKS_GUARD:
        return _RUN_THREAD_LOCKS.setdefault(key, threading.RLock())

# The event names are the durable protocol; this projection is deliberately
# explicit so a newly appended event cannot silently leave the UI in an old
# or misleading state.  ``reasoning`` remains for v1 source events while the
# v2 analysis/action stages use their more precise names.
STATUS_BY_EVENT = {
    "run/created": "queued",
    "run/started": "running",
    "source/started": "collecting",
    "source/completed": "reasoning",
    "analysis/started": "analyzing",
    "analysis/tool_called": "analyzing",
    "analysis/tool_call": "analyzing",
    "tool/call": "analyzing",
    "tool/result": "analyzing",
    "analysis/completed": "verifying",
    "assistant/draft": "verifying",
    "artifact/written": "verifying",
    "gate/requested": "awaiting_human",
    "gate/paused": "paused",
    "gate/changes_requested": "changes_requested",
    # Approval hands the run to the single-use action queue.  The following
    # action/queued event normally follows immediately, but mapping both keeps
    # replay/SSE correct if the process is interrupted between the two writes.
    "gate/approved": "action_queued",
    "gate/rejected": "rejected",
    "action/queued": "action_queued",
    "action/started": "executing",
    "action/completed": "verifying",
    "action/failed": "failed",
    "agent/requested": "running",
    "agent/completed": "verifying",
    "agent/failed": "failed",
    "candidate/created": "awaiting_human",
    "verification/completed": "verifying",
    "verification/failed": "failed",
    "run/retrying": "retrying",
    "retry/requested": "retrying",
    "run/completed": "completed",
    "run/failed": "failed",
    "run/cancelled": "cancelled",
    "run/rejected": "rejected",
}

# Only these event types close a run.  Keep this separate from
# ``TERMINAL_STATES`` (the public projected values).  Most terminal events
# carry the ``run/`` namespace; an explicit gate rejection is terminal too.
TERMINAL_EVENT_TYPES = {
    "run/completed",
    "run/failed",
    "run/cancelled",
    "run/rejected",
    "gate/rejected",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_run_id(loop_id: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{stamp}-{uuid4().hex[:8]}-{loop_id}"


def atomic_json_write(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


@dataclass(frozen=True)
class RunPaths:
    state_dir: Path
    run_id: str

    @property
    def root(self) -> Path:
        return self.state_dir / "runs" / self.run_id

    @property
    def request(self) -> Path:
        return self.root / "request.json"

    @property
    def events(self) -> Path:
        return self.root / "events.jsonl"

    @property
    def state(self) -> Path:
        return self.root / "state.json"

    @property
    def snapshot(self) -> Path:
        return self.root / "snapshot.json"

    @property
    def draft(self) -> Path:
        return self.root / "draft" / "report.md"

    @property
    def cancel_marker(self) -> Path:
        return self.root / "CANCEL"

    @property
    def runner_log(self) -> Path:
        return self.root / "runner.log"


class RunStore:
    """Append-only events plus a rebuildable current-state projection."""

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir.expanduser().resolve()
        self.runs_dir = self.state_dir / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def paths(self, run_id: str) -> RunPaths:
        return RunPaths(self.state_dir, run_id)

    @contextmanager
    def _run_transaction(self, run_id: str) -> Iterator[None]:
        """Serialize one Run's event log and projection across processes."""
        paths = self.paths(run_id)
        paths.root.mkdir(parents=True, exist_ok=True)
        lock_path = paths.root / ".events.lock"
        with _run_thread_lock(lock_path):
            with lock_path.open("a+", encoding="utf-8") as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def exists(self, run_id: str) -> bool:
        return self.paths(run_id).request.is_file()

    def create(self, request: Dict[str, Any]) -> Dict[str, Any]:
        loop_id = str(request.get("loop_id") or "")
        if not loop_id:
            raise ValueError("loop_id is required")
        run_id = str(request.get("run_id") or new_run_id(loop_id))
        paths = self.paths(run_id)
        if paths.request.exists():
            raise FileExistsError(f"run already exists: {run_id}")
        normalized = {
            "schema_version": RUN_REQUEST_SCHEMA,
            "run_id": run_id,
            "loop_id": loop_id,
            "trigger": request.get("trigger") or {"kind": "manual", "actor": "local"},
            "scope": request.get("scope") or {},
            "runtime": {"kind": "codex", **(request.get("runtime") or {})},
            "permission_mode": request.get("permission_mode") or "report",
            "loop_contract": request.get("loop_contract") or {},
            "input_hash": request.get("input_hash") or (request.get("runtime") or {}).get("input_hash"),
            "budget": request.get("budget") or {"max_seconds": 900, "max_tool_calls": 30},
            "record": bool(request.get("record", False)),
            "created_at": now_iso(),
        }
        if normalized["runtime"].get("kind") != "codex":
            raise ValueError("the current runner runtime is codex only")
        if normalized["permission_mode"] not in {"report", "draft", "approved_action"}:
            raise ValueError("permission_mode must be report, draft, or approved_action")
        paths.root.mkdir(parents=True, exist_ok=True)
        atomic_json_write(paths.request, normalized)
        self.append(run_id, "run/created", {"loop_id": loop_id, "permission_mode": normalized["permission_mode"]})
        return normalized

    def request(self, run_id: str) -> Dict[str, Any]:
        paths = self.paths(run_id)
        if not paths.request.is_file():
            raise FileNotFoundError(f"unknown run: {run_id}")
        return read_json(paths.request)

    def events_for(self, run_id: str) -> List[Dict[str, Any]]:
        path = self.paths(run_id).events
        if not path.is_file():
            return []
        events: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                events.append(value)
        return events

    def append(self, run_id: str, event_type: str, data: Optional[Dict[str, Any]] = None, actor: str = "codex-runner") -> Dict[str, Any]:
        paths = self.paths(run_id)
        with self._run_transaction(run_id):
            events = self.events_for(run_id)
            event = {
                "schema_version": EVENT_SCHEMA,
                "run_id": run_id,
                "seq": len(events) + 1,
                "at": now_iso(),
                "type": event_type,
                "actor": actor,
                "data": data or {},
                "visibility": "user",
            }
            with paths.events.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(event, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._write_state_unlocked(run_id)
            return event

    def write_state(self, run_id: str) -> Dict[str, Any]:
        with self._run_transaction(run_id):
            return self._write_state_unlocked(run_id)

    def _write_state_unlocked(self, run_id: str) -> Dict[str, Any]:
        state = project_state(self.request(run_id), self.events_for(run_id))
        atomic_json_write(self.paths(run_id).state, state)
        return state

    def state(self, run_id: str) -> Dict[str, Any]:
        paths = self.paths(run_id)
        if paths.state.is_file():
            return read_json(paths.state)
        return self.write_state(run_id)

    def state_read_only(self, run_id: str) -> Dict[str, Any]:
        """Return a current projection without repairing or creating files."""
        paths = self.paths(run_id)
        if paths.state.is_file():
            return read_json(paths.state)
        return project_state(self.request(run_id), self.events_for(run_id))

    def list_states(self) -> List[Dict[str, Any]]:
        states = []
        for request_path in sorted(self.runs_dir.glob("*/request.json"), reverse=True):
            run_id = request_path.parent.name
            try:
                states.append(self.state(run_id))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return states

    def list_states_read_only(self) -> List[Dict[str, Any]]:
        states = []
        for request_path in sorted(self.runs_dir.glob("*/request.json"), reverse=True):
            try:
                states.append(self.state_read_only(request_path.parent.name))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return states


def project_state(request: Dict[str, Any], events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    events_list = list(events)
    state: Dict[str, Any] = {
        "schema_version": STATE_SCHEMA,
        "run_id": request.get("run_id"),
        "loop_id": request.get("loop_id"),
        "permission_mode": request.get("permission_mode"),
        "scope": request.get("scope") or {},
        "trigger": request.get("trigger") or {},
        "runtime": request.get("runtime", {}).get("kind", "codex"),
        "status": "unknown",
        "events_count": len(events_list),
        "last_event": None,
        "snapshot_id": None,
        "draft_path": None,
        "error": None,
        "created_at": request.get("created_at"),
        "started_at": None,
        "completed_at": None,
    }
    for event in events_list:
        event_type = event.get("type")
        state["last_event"] = {"seq": event.get("seq"), "type": event_type, "at": event.get("at")}
        if event_type in STATUS_BY_EVENT:
            state["status"] = STATUS_BY_EVENT[event_type]
        data = event.get("data") or {}
        if event_type == "run/started":
            state["started_at"] = event.get("at")
        if event_type == "source/completed":
            state["snapshot_id"] = data.get("snapshot_id") or state["snapshot_id"]
        if event_type == "assistant/draft":
            state["draft_path"] = data.get("path")
        if event_type == "run/failed":
            state["error"] = data.get("error")
        if event_type in {"run/retrying", "retry/requested"}:
            # A retry starts a new attempt.  Do not expose the previous
            # attempt's terminal timestamp/error as if they belonged to the
            # currently running attempt.
            state["completed_at"] = None
            state["error"] = None
        if event_type in TERMINAL_EVENT_TYPES:
            state["completed_at"] = event.get("at")
    return state
