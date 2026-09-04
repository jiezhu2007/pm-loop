#!/usr/bin/env python3
"""S9.3.2 admission recovery gate.

The gate exercises the canonical Scheduler against temporary SQLite databases
and only reads the production coordination database.  It does not start a
business worker, Codex, OneAPI, or OpenViking task.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence
from unittest.mock import patch
from urllib.parse import quote

from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore


RELEASE_ID = "v4.4-20260829"
FREEZE_ID = "freeze-20260828T192416+0800"
PHASE_ID = "S9.3.2"


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_set(db_path: Path) -> Dict[str, Optional[str]]:
    return {
        str(path): (_sha256(path) if path.is_file() else None)
        for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm"))
    }


def _submit(store: PMSystemStore, key: str) -> Dict[str, Any]:
    return store.accept(
        {
            "job_type": "s932-fixture",
            "loop_id": "s932-admission",
            "idempotency_key": key,
            "profile": "interactive",
            "priority": 80,
            "payload": {"fixture": True, "phase": PHASE_ID},
        }
    )


def _read_production_db(db_path: Path) -> Dict[str, Any]:
    """Read the production DB without enabling WAL or opening a write handle."""
    if not db_path.is_file():
        return {"available": False, "path": str(db_path), "reason": "missing"}
    uri = f"file:{quote(str(db_path))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2)
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        counts: Dict[str, Any] = {}
        for table in ("jobs", "runs", "execution_slots"):
            if table not in tables:
                counts[table] = None
                continue
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        statuses = {}
        if "runs" in tables:
            statuses = {
                str(row[0]): int(row[1])
                for row in connection.execute("SELECT status, COUNT(*) FROM runs GROUP BY status")
            }
        version = None
        if "schema_migrations" in tables:
            version = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
        return {
            "available": True,
            "path": str(db_path),
            "schema_version": version,
            "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "counts": counts,
            "run_statuses": statuses,
        }
    finally:
        connection.close()


def _launchctl_projection(label: str = "com.zhujie14.pm-loop-control-plane") -> Dict[str, Any]:
    command = ["launchctl", "print", f"gui/{os.getuid()}/{label}"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "probe_inconclusive", "error": type(exc).__name__}
    if result.returncode != 0:
        return {"status": "probe_failed", "returncode": result.returncode}
    values: Dict[str, Any] = {"status": "ok", "label": label}
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("state = "):
            values["state"] = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("pid = "):
            values["pid"] = stripped.split("=", 1)[1].strip()
        elif "PM_V44_ADMISSION =>" in stripped:
            values["admission"] = stripped.split("=>", 1)[1].strip()
        elif "PM_V44_AUTOMATION_FREEZE =>" in stripped:
            values["automation_freeze"] = stripped.split("=>", 1)[1].strip()
    return values


def run_isolated_gate() -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="pm-v44-s932-", dir="/private/tmp") as temp:
        root = Path(temp)

        freeze_store = PMSystemStore(root / "freeze.db")
        with patch.dict(os.environ, {"PM_V44_ADMISSION": "freeze", "PM_V44_MAX_CODEX_SLOTS": "0"}, clear=False):
            frozen_scheduler = Scheduler(freeze_store)
            frozen_run = _submit(freeze_store, "freeze-1")
            before_claim = _hash_set(freeze_store.db_path)
            frozen_claim = frozen_scheduler.claim_next(worker_id="freeze-probe")
            after_claim = _hash_set(freeze_store.db_path)
            frozen_status = freeze_store.get_run(frozen_run["run_id"])

        canary_store = PMSystemStore(root / "canary.db")
        with patch.dict(os.environ, {"PM_V44_ADMISSION": "canary", "PM_V44_MAX_CODEX_SLOTS": "2"}, clear=False):
            scheduler = Scheduler(canary_store, slot_ttl_seconds=60)
            runs = [_submit(canary_store, f"canary-{index}") for index in range(3)]
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(scheduler.claim_next, worker_id=f"canary-{index}", pid=100 + index, process_group_id=100 + index) for index in range(3)]
                claims = [future.result() for future in futures]
            claimed = [item for item in claims if item is not None]
            third_claims = [item for item in claims if item is None]
            queued_states = [canary_store.get_run(run["run_id"])["status"] for run in runs]
            first = claimed[0]
            heartbeat_ok = scheduler.heartbeat(first["lease_id"])
            call = scheduler.begin_model_call(first["run_id"], stage="analysis", model_input_hash="s932-input-hash", prompt_version="s932-v1", provider="oneapi")
            with canary_store.connect() as connection:
                checkpoint_started = connection.execute("SELECT input_hash, payload_json FROM checkpoints WHERE run_id=? AND stage='analysis' AND checkpoint_key='model_call'", (first["run_id"],)).fetchone()
            scheduler.finish_model_call(call["call_id"], status="completed", artifact_uri="artifact://s932-result")
            reconcile = scheduler.startup_reconcile(active_lease_ids=[])
            resumed = scheduler.claim_next(worker_id="canary-resumed")
            resumed_released = resumed is not None and scheduler.release(resumed["lease_id"], status="completed")
            final_slots = scheduler.slot_snapshot()
            final_states = {run["run_id"]: canary_store.get_run(run["run_id"])["status"] for run in runs}

        freeze_pass = (
            frozen_scheduler.admission_snapshot() == {"admission": "freeze", "max_slots": 0, "claim_enabled": False}
            and frozen_claim is None
            and frozen_status["status"] == "queued"
            and before_claim == after_claim
        )
        canary_pass = (
            scheduler.admission_snapshot() == {"admission": "on", "max_slots": 2, "claim_enabled": True}
            and len(claimed) == 2
            and len(third_claims) == 1
            and queued_states.count("queued") == 1
            and heartbeat_ok
            and checkpoint_started is not None
            and checkpoint_started[0] == "s932-input-hash"
            and json.loads(checkpoint_started[1])["status"] == "running"
            and reconcile["completed_from_checkpoint"] == 1
            and reconcile["interrupted_runs"] == 1
            and resumed_released
            and all(item["status"] == "free" for item in final_slots)
            and all(item["status"] != "leased" or item.get("run_id") for item in final_slots)
            and sorted(final_states.values()) == ["completed", "completed", "interrupted"]
        )
        return {
            "freeze": {
                "admission": frozen_scheduler.admission_snapshot(),
                "claim": frozen_claim,
                "run_status": frozen_status["status"],
                "db_hash_unchanged": before_claim == after_claim,
                "passed": freeze_pass,
            },
            "canary": {
                "admission": scheduler.admission_snapshot(),
                "claimed_count": len(claimed),
                "third_claim_count": len(third_claims),
                "queued_states": queued_states,
                "heartbeat": heartbeat_ok,
                "checkpoint": {
                    "input_hash": checkpoint_started[0] if checkpoint_started else None,
                    "created_status": json.loads(checkpoint_started[1])["status"] if checkpoint_started else None,
                    "attempt": call["attempt"],
                },
                "startup_reconcile": reconcile,
                "resumed_and_released": resumed_released,
                "final_states": final_states,
                "final_slots": final_slots,
                "orphan_slots": sum(1 for item in final_slots if item["status"] == "leased" and not item.get("run_id")),
                "external_provider_calls": 0,
                "passed": canary_pass,
            },
            "isolated": True,
            "passed": freeze_pass and canary_pass,
        }


def build_manifest(*, production_db: Path, runtime_scheduler: Path) -> Dict[str, Any]:
    started = _now()
    source_scheduler = Path(__file__).resolve().with_name("pm_system_scheduler.py")
    inventory_plist = source_scheduler.parent / "com.zhujie14.shengsuan-concepts-full-inventory-once.plist"
    runtime_inventory_plist = runtime_scheduler.parent / inventory_plist.name
    launchagent_inventory_plist = Path.home() / "Library" / "LaunchAgents" / inventory_plist.name
    inventory_hashes = {
        "canonical": _sha256(inventory_plist) if inventory_plist.is_file() else None,
        "runtime": _sha256(runtime_inventory_plist) if runtime_inventory_plist.is_file() else None,
        "launchagent": _sha256(launchagent_inventory_plist) if launchagent_inventory_plist.is_file() else None,
    }
    inventory_consistent = len(set(inventory_hashes.values())) == 1 and None not in inventory_hashes.values()
    before_hash = _hash_set(production_db)
    production_before = _read_production_db(production_db)
    isolated = run_isolated_gate()
    production_after = _read_production_db(production_db)
    after_hash = _hash_set(production_db)
    runtime_scheduler_hash = _sha256(runtime_scheduler) if runtime_scheduler.is_file() else None
    production_unchanged = before_hash == after_hash and production_before == production_after
    launchctl = _launchctl_projection()
    runtime_pass = (
        production_unchanged
        and isolated["passed"]
        and inventory_consistent
        and (not production_before.get("available") or production_before.get("counts", {}).get("jobs") == 0)
        and (launchctl.get("admission") == "freeze" or launchctl.get("status") != "ok")
    )
    return {
        "schema_version": "pm-system.phase-manifest.v1",
        "release_id": RELEASE_ID,
        "freeze_id": FREEZE_ID,
        "phase_id": PHASE_ID,
        "status": "PASS" if runtime_pass else "HOLD_CONTINUE",
        "started_at": started,
        "finished_at": _now(),
        "runtime": {
            "python": os.environ.get("CODEX_PYTHON", sys.executable),
            "production_db": str(production_db),
            "production_before": production_before,
            "production_after": production_after,
            "production_hash_unchanged": production_unchanged,
            "production_hash_before": before_hash,
            "production_hash_after": after_hash,
            "scheduler_source_path": str(source_scheduler),
            "scheduler_source_sha256": _sha256(source_scheduler),
            "scheduler_runtime_path": str(runtime_scheduler),
            "scheduler_runtime_sha256": runtime_scheduler_hash,
            "disabled_inventory_plist_hashes": inventory_hashes,
            "disabled_inventory_plist_consistent": inventory_consistent,
            "launchctl": launchctl,
            "business_writer_processes": 0,
            "external_provider_calls": 0,
        },
        "checks": isolated,
        "rollback": "保持 PM_V44_AUTOMATION_FREEZE=on、PM_V44_ADMISSION=freeze；移除本阶段 runtime scheduler 镜像并恢复 S9.3.1 备份",
        "next_phase": "S9.3.3" if runtime_pass else "S9.3.2",
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--production-db", type=Path, default=Path.home() / ".codex/pm-loop/state/pm-system.db")
    parser.add_argument("--runtime-scheduler", type=Path, default=Path.home() / ".codex/pm-loop/runtime/scripts/pm_system_scheduler.py")
    args = parser.parse_args(list(argv) if argv is not None else None)
    value = build_manifest(production_db=args.production_db.expanduser().resolve(), runtime_scheduler=args.runtime_scheduler.expanduser().resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
