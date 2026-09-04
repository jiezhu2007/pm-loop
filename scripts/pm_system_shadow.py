#!/usr/bin/env python3
"""Read-only legacy RunStore projection used by the S4 shadow gate."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

from pm_loop_runtime import RunStore
from pm_system_store import PMSystemStore, now_iso


TERMINAL = {"completed", "failed", "cancelled", "rejected", "degraded", "dead_letter"}


def _safe_status(value: Any) -> str:
    status = str(value or "unknown")
    return status if status in TERMINAL | {"queued", "running", "retry_wait", "interrupted", "unknown"} else "unknown"


def legacy_digest(legacy_state_dir: Path) -> str:
    """Hash legacy request/event/state bytes without modifying any file."""
    root = Path(legacy_state_dir).expanduser().resolve()
    digest = hashlib.sha256()
    for path in sorted((root / "runs").glob("**/*")):
        if not path.is_file() or path.name == ".events.lock":
            continue
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def import_legacy_shadow(store: PMSystemStore, legacy_state_dir: Path) -> Dict[str, int]:
    """Project legacy states once, marked as shadow and never executable."""
    legacy = RunStore(Path(legacy_state_dir))
    states = legacy.list_states_read_only()
    created = 0
    existing = 0
    at = now_iso()
    with store.transaction() as connection:
        for state in states:
            run_id = str(state.get("run_id") or "")
            if not run_id:
                continue
            key = f"legacy-shadow:{run_id}"
            row = connection.execute("SELECT job_id FROM jobs WHERE idempotency_key=?", (key,)).fetchone()
            if row is not None:
                existing += 1
                continue
            job_id = f"shadow-job-{hashlib.sha256(run_id.encode('utf-8')).hexdigest()[:24]}"
            status = _safe_status(state.get("status"))
            payload = {"shadow": True, "legacy_state": state}
            connection.execute(
                "INSERT INTO jobs(job_id,idempotency_key,job_type,run_id,status,priority,profile,payload_json,queued_at,updated_at,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, key, "legacy-shadow", run_id, status, 0, "legacy-shadow", json.dumps(payload, ensure_ascii=False, separators=(",", ":")), str(state.get("created_at") or at), at, str(state.get("completed_at") or "") or None),
            )
            connection.execute(
                "INSERT INTO runs(run_id,job_id,loop_id,status,profile,created_at,updated_at,started_at,completed_at,snapshot_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (run_id, job_id, str(state.get("loop_id") or "legacy"), status, "legacy-shadow", str(state.get("created_at") or at), at, state.get("started_at"), state.get("completed_at"), state.get("snapshot_id")),
            )
            connection.execute(
                "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                (run_id, 1, "legacy/state", "shadow-import", json.dumps({"shadow": True, "status": status, "events_count": state.get("events_count", 0)}, separators=(",", ":")), at),
            )
            created += 1
    return {"legacy_states": len(states), "created": created, "existing": existing}


def compare_shadow(store: PMSystemStore, legacy_state_dir: Path) -> Dict[str, Any]:
    legacy = RunStore(Path(legacy_state_dir))
    old_states = legacy.list_states_read_only()
    old_counts: Dict[str, int] = {}
    for state in old_states:
        status = _safe_status(state.get("status"))
        old_counts[status] = old_counts.get(status, 0) + 1
    with store.connect() as connection:
        rows = connection.execute("SELECT status,COUNT(*) FROM runs WHERE profile='legacy-shadow' GROUP BY status").fetchall()
    new_counts = {str(row[0]): int(row[1]) for row in rows}
    return {
        "legacy_count": len(old_states),
        "shadow_count": sum(new_counts.values()),
        "legacy_status_counts": old_counts,
        "shadow_status_counts": new_counts,
        "status_counts_equal": old_counts == new_counts,
    }


__all__ = ["compare_shadow", "import_legacy_shadow", "legacy_digest"]

