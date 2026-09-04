#!/usr/bin/env python3
"""C2 shared-runtime migration for the V1.1 concept projection contract.

The runner changes runtime code only. It uses a new V4.5 migration/epoch,
keeps concept admission disabled, creates independently addressable database
and runtime backups, and never calls OneAPI or OpenViking.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from pm_system_store import PMSystemStore, SCHEMA_VERSION, now_iso  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_RUNTIME_ROOT = Path.home() / ".codex" / "pm-loop" / "runtime"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11-shared-runtime"
DEFAULT_REPORT = PROJECT_ROOT / "docs" / "03-产品架构" / "v1.1实施报告" / "c2-shared-runtime-20260831.json"
DEFAULT_MIGRATION_ID = "v45-concept-runtime-20260831"
DEFAULT_MIGRATION_EPOCH = "v45-concept-runtime-20260831"
DEFAULT_OWNER = f"codex-concept-runtime:{os.getpid()}"
STAGE_ID = "C2-SHARED-RUNTIME"
MIN_SUPPORTED_CORE_SCHEMA_VERSION = 10
RUNTIME_FILES = (
    "pm_system_gateway.py",
    "pm_resource_dispatcher.py",
    "pm_system_worker.py",
)
SERVICE_LABELS = (
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.pm-system-worker",
    "com.zhujie14.ov-memory-sync",
)
PROCESS_MARKERS = {
    "control_plane": "pm_loop_control_plane_server.py",
    "worker": "pm_system_worker.py",
    "memory_watcher": "ov_memory_sync.py",
}
TEST_MODULES = (
    "tests.test_pm_v11_shared_runtime",
    "tests.test_pm_system_gateway",
    "tests.test_pm_resource_dispatcher",
)


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _future(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(seconds)))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(source.read_bytes())
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, source.stat().st_mode & 0o777)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _database_backup(db_path: Path, root: Path, *, migration_id: str, epoch: str) -> Dict[str, Any]:
    stage_root = root.expanduser().resolve() / migration_id
    stage_root.mkdir(parents=True, exist_ok=True)
    destination = stage_root / f"{STAGE_ID}-{_timestamp()}-{uuid.uuid4().hex[:12]}.sqlite3"
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with sqlite3.connect(str(db_path), timeout=10) as source_connection, sqlite3.connect(str(temporary), timeout=10) as target_connection:
            source_connection.backup(target_connection)
    except Exception:
        # A failed backup must not leave a misleading partial SQLite file that
        # a later operator could mistake for a recovery point.
        temporary.unlink(missing_ok=True)
        raise
    try:
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    with sqlite3.connect(str(destination), timeout=10) as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        core_schema = int(connection.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
    rehearsal = stage_root / f".{destination.name}.restore-rehearsal-{uuid.uuid4().hex[:8]}.sqlite3"
    shutil.copy2(destination, rehearsal)
    try:
        with sqlite3.connect(str(rehearsal), timeout=10) as connection:
            restore_integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        rehearsal.unlink(missing_ok=True)
    return {
        "path": str(destination),
        "migration_id": migration_id,
        "migration_epoch": epoch,
        "stage_id": STAGE_ID,
        "sha256": _sha256(destination),
        "size_bytes": destination.stat().st_size,
        "integrity_check": integrity,
        "restore_rehearsal_integrity": restore_integrity,
        "core_schema_version": core_schema,
        "verified": integrity == "ok" and restore_integrity == "ok",
    }


def _runtime_backup(canonical_root: Path, runtime_root: Path, backup_root: Path, *, migration_id: str) -> Dict[str, Any]:
    destination = backup_root.expanduser().resolve() / migration_id / f"runtime-{STAGE_ID}-{_timestamp()}-{uuid.uuid4().hex[:12]}"
    destination.mkdir(parents=True, exist_ok=False)
    entries: Dict[str, Any] = {}
    try:
        for name in RUNTIME_FILES:
            canonical = canonical_root / "scripts" / name
            runtime = runtime_root / "scripts" / name
            if not canonical.is_file() or not runtime.is_file():
                raise FileNotFoundError(f"runtime migration input missing: {name}")
            backup = destination / name
            shutil.copy2(runtime, backup)
            entries[name] = {
                "canonical_before": _sha256(canonical),
                "runtime_before": _sha256(runtime),
                "backup": str(backup),
                "backup_sha256": _sha256(backup),
            }
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    return {"path": str(destination), "files": entries, "verified": all(item["runtime_before"] == item["backup_sha256"] for item in entries.values())}


def _sync_runtime(canonical_root: Path, runtime_root: Path) -> Dict[str, Any]:
    files: Dict[str, Any] = {}
    for name in RUNTIME_FILES:
        source = canonical_root / "scripts" / name
        destination = runtime_root / "scripts" / name
        _atomic_copy(source, destination)
        source_hash = _sha256(source)
        runtime_hash = _sha256(destination)
        files[name] = {"canonical": str(source), "runtime": str(destination), "canonical_sha256": source_hash, "runtime_sha256": runtime_hash, "match": source_hash == runtime_hash}
    return {"files": files, "verified": all(item["match"] for item in files.values())}


def _restore_runtime(runtime_root: Path, backup: Mapping[str, Any]) -> Dict[str, Any]:
    restored: Dict[str, Any] = {}
    for name, entry in dict(backup.get("files") or {}).items():
        source = Path(str(entry["backup"]))
        destination = runtime_root / "scripts" / str(name)
        _atomic_copy(source, destination)
        restored[str(name)] = {"sha256": _sha256(destination), "expected": str(entry["backup_sha256"])}
    return {"files": restored, "verified": all(item["sha256"] == item["expected"] for item in restored.values())}


def _process_snapshot() -> Dict[str, Any]:
    completed = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True, check=False)
    rows = completed.stdout.splitlines() if completed.returncode == 0 else []
    result: Dict[str, Any] = {}
    for name, marker in PROCESS_MARKERS.items():
        matching = [line.strip() for line in rows if marker in line and str(os.getpid()) not in line.split(None, 1)[:1]]
        result[name] = {
            "count": len(matching) if completed.returncode == 0 else -1,
            "commands": matching[:4],
            "probe_error": None if completed.returncode == 0 else (completed.stderr or "ps failed").strip()[-500:],
        }
    return result


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    if not _table_exists(connection, table):
        return 0
    query = f"SELECT COUNT(*) FROM {table}"
    if where:
        query += f" WHERE {where}"
    return int(connection.execute(query, params).fetchone()[0])


def _restore_freeze_snapshot(store: PMSystemStore, snapshot: Optional[Mapping[str, Any]]) -> None:
    """Restore the single durable freeze row after a pre-lease failure."""
    with store.transaction() as connection:
        if snapshot is None:
            connection.execute("DELETE FROM migration_freeze WHERE freeze_id=1")
            return
        connection.execute(
            """UPDATE migration_freeze SET migration_id=?,migration_epoch=?,stage_id=?,owner=?,state=?,deadline_at=?,created_at=?,updated_at=?
               WHERE freeze_id=1""",
            (
                snapshot.get("migration_id"),
                snapshot.get("migration_epoch"),
                snapshot.get("stage_id"),
                snapshot.get("owner"),
                snapshot.get("state"),
                snapshot.get("deadline_at"),
                snapshot.get("created_at"),
                now_iso(),
            ),
        )


def _run_tests() -> Dict[str, Any]:
    command = [sys.executable, "-m", "unittest", "-v", *TEST_MODULES]
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=180, check=False)
    combined = (completed.stdout or "") + "\n" + (completed.stderr or "")
    summary = [line for line in combined.splitlines() if line.startswith("Ran ") or line.strip() in {"OK", "FAILED"} or line.startswith("FAILED ")]
    return {"command": command, "returncode": completed.returncode, "status": "PASS" if completed.returncode == 0 else "HOLD", "summary": summary[-6:], "output_tail": combined[-4000:]}


def _core_schema_error(version: int) -> Optional[str]:
    """Keep this runtime-only migration compatible with the live PM schema."""
    if version < MIN_SUPPORTED_CORE_SCHEMA_VERSION:
        return f"core_schema_too_old:{version}"
    if version > SCHEMA_VERSION:
        return f"core_schema_future:{version}"
    return None


def _snapshot(store: PMSystemStore) -> Dict[str, Any]:
    with store.connect() as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        active = {
            "jobs": _count_rows(connection, "jobs", "status IN ('queued','running','retry_wait')"),
            "runs": _count_rows(connection, "runs", "status IN ('queued','running','retry_wait')"),
            "outbox": _count_rows(connection, "outbox_items", "status IN ('pending','in_flight','retry_wait')"),
            "semantic": _count_rows(connection, "semantic_tasks", "status IN ('queued','in_flight','accepted','processing','retry_wait')"),
            "dispatch_leases": _count_rows(connection, "outbox_dispatch_leases"),
            "probe_leases": _count_rows(connection, "provider_probe_leases"),
            "provider_tokens": _count_rows(connection, "provider_tokens", "released_at IS NULL"),
            "execution_slots": _count_rows(connection, "execution_slots", "status='leased'"),
        }
        concept = connection.execute("SELECT schema_version,schema_id,migration_id,migration_epoch,ddl_sha256 FROM concept_schema_meta ORDER BY schema_version DESC LIMIT 1").fetchone() if _table_exists(connection, "concept_schema_meta") else None
        admission = connection.execute("SELECT namespace_epoch,admission_state,version,reason FROM concept_admissions ORDER BY namespace_epoch LIMIT 1").fetchone() if _table_exists(connection, "concept_admissions") else None
        lease_count = _count_rows(connection, "migration_leases", "state='active'")
    return {
        "observed_at": now_iso(),
        "integrity_check": integrity,
        "core_schema_version": store.schema_version(),
        "freeze": store.migration_freeze(),
        "active": active,
        "active_migration_leases": lease_count,
        "concept_schema": dict(concept) if concept is not None else None,
        "concept_admission": dict(admission) if admission is not None else None,
        "processes": _process_snapshot(),
    }


def _service_command(action: str, label: str) -> Dict[str, Any]:
    domain = f"gui/{os.getuid()}"
    plist = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
    if action == "stop":
        command = ["launchctl", "bootout", f"{domain}/{label}"]
    elif action == "start":
        command = ["launchctl", "bootstrap", domain, str(plist)]
    else:
        raise ValueError(action)
    completed = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
    acceptable = completed.returncode == 0 or (action == "stop" and "Could not find service" in (completed.stderr or "")) or (action == "start" and "service already loaded" in (completed.stderr or "").lower())
    return {"label": label, "action": action, "command": command, "returncode": completed.returncode, "status": "ok" if acceptable else "error", "stderr": (completed.stderr or "").strip()[-500:]}


def _set_services(action: str) -> list[Dict[str, Any]]:
    return [_service_command(action, label) for label in SERVICE_LABELS]


def _wait_process_counts(expected: int, *, timeout: float = 30.0) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    snapshot = _process_snapshot()
    while time.monotonic() < deadline:
        snapshot = _process_snapshot()
        if all(int(item["count"]) == expected for item in snapshot.values()):
            return {"status": "PASS", "expected": expected, "processes": snapshot}
        time.sleep(0.25)
    return {"status": "HOLD", "expected": expected, "processes": snapshot}


def _write_report(path: Path, value: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def run(
    *,
    db_path: Path,
    runtime_root: Path,
    backup_root: Path,
    report_path: Path,
    migration_id: str,
    migration_epoch: str,
    owner: str,
    apply: bool,
    manage_services: bool,
    run_tests: bool,
) -> Dict[str, Any]:
    # Preflight is observational. The v10->v11 scheduler migration owns DDL.
    store = PMSystemStore(db_path, auto_migrate=False)
    before = _snapshot(store)
    tests = _run_tests() if run_tests else {"status": "SKIPPED", "returncode": 0}
    errors: list[str] = []
    if before["integrity_check"] != "ok":
        errors.append("foundation_integrity_failed")
    core_schema_error = _core_schema_error(int(before["core_schema_version"]))
    if core_schema_error:
        errors.append(core_schema_error)
    if not before.get("concept_schema") or int(before["concept_schema"].get("schema_version") or 0) != 2:
        errors.append("concept_schema_v2_missing")
    if not before.get("concept_admission") or before["concept_admission"].get("admission_state") != "disabled":
        errors.append("concept_admission_not_disabled")
    if str((before.get("freeze") or {}).get("state")) != "released":
        errors.append("runtime_not_released")
    if before["active_migration_leases"]:
        errors.append("active_migration_lease")
    if any(int(value) for value in before["active"].values()):
        errors.append("active_runtime_work")
    if tests.get("status") != "PASS" and run_tests:
        errors.append("regression_failed")
    if not apply or errors:
        result = {
            "schema": "concept-v11.shared-runtime-migration.v1",
            "status": "HOLD" if errors else "PASS",
            "mode": "preflight",
            "migration_id": migration_id,
            "migration_epoch": migration_epoch,
            "stage_id": STAGE_ID,
            "before": before,
            "tests": tests,
            "errors": errors,
        }
        _write_report(report_path, result)
        return result

    deadline = _future(1800)
    previous_freeze = before.get("freeze")
    freeze_set = False
    lease: Optional[Dict[str, Any]] = None
    try:
        store.set_migration_freeze(migration_id=migration_id, migration_epoch=migration_epoch, stage_id=STAGE_ID, owner=owner, deadline_at=deadline, state="freeze")
        freeze_set = True
        lease = store.acquire_migration_lease(migration_id=migration_id, stage_id=STAGE_ID, migration_epoch=migration_epoch, owner=owner, lease_seconds=1800)
    except Exception:
        if freeze_set:
            _restore_freeze_snapshot(store, previous_freeze)
        raise
    service_stop: list[Dict[str, Any]] = []
    service_start: list[Dict[str, Any]] = []
    db_backup: Dict[str, Any] = {}
    runtime_backup: Dict[str, Any] = {}
    runtime_sync: Dict[str, Any] = {}
    rollback: Optional[Dict[str, Any]] = None
    after: Dict[str, Any] = {}
    try:
        if manage_services:
            service_stop = _set_services("stop")
            drained = _wait_process_counts(0)
            if any(item["status"] != "ok" for item in service_stop) or drained["status"] != "PASS":
                raise RuntimeError("failed to stop shared runtime services")
        db_backup = _database_backup(db_path, backup_root, migration_id=migration_id, epoch=migration_epoch)
        runtime_backup = _runtime_backup(PROJECT_ROOT, runtime_root, backup_root, migration_id=migration_id)
        if not db_backup.get("verified") or not runtime_backup.get("verified"):
            raise RuntimeError("backup verification failed")
        runtime_sync = _sync_runtime(PROJECT_ROOT, runtime_root)
        if not runtime_sync.get("verified"):
            raise RuntimeError("runtime hash convergence failed")
        compile_result = subprocess.run(
            [sys.executable, "-m", "py_compile", *[str(runtime_root / "scripts" / name) for name in RUNTIME_FILES]],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        runtime_sync["compile"] = {"returncode": compile_result.returncode, "stderr": (compile_result.stderr or "")[-2000:]}
        if compile_result.returncode != 0:
            raise RuntimeError("runtime compile failed")
        if manage_services:
            service_start = _set_services("start")
            started = _wait_process_counts(1)
            if any(item["status"] != "ok" for item in service_start) or started["status"] != "PASS":
                raise RuntimeError("failed to start shared runtime services")
        if not store.release_migration_lease(lease_id=str(lease["lease_id"])):
            raise RuntimeError("stage lease release failed")
        lease["state"] = "released"
        if not store.update_migration_freeze(migration_id=migration_id, state="released"):
            raise RuntimeError("migration release failed")
        after = _snapshot(store)
        if after["integrity_check"] != "ok":
            raise RuntimeError("post-migration integrity failed")
        if int(after["core_schema_version"]) != int(before["core_schema_version"]):
            raise RuntimeError("shared runtime migration changed core schema")
        if _core_schema_error(int(after["core_schema_version"])):
            raise RuntimeError("post-migration core schema unsupported")
        if after["concept_admission"].get("admission_state") != "disabled":
            raise RuntimeError("concept admission changed unexpectedly")
        if any(int(value) for value in after["active"].values()) or after["active_migration_leases"]:
            raise RuntimeError("post-migration runtime is not drained")
        if str((after.get("freeze") or {}).get("state")) != "released":
            raise RuntimeError("post-migration freeze is not released")
        status = "PASS"
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        if runtime_backup:
            try:
                rollback = _restore_runtime(runtime_root, runtime_backup)
            except Exception as rollback_exc:
                rollback = {"verified": False, "error": f"{type(rollback_exc).__name__}: {rollback_exc}"}
        if manage_services:
            try:
                service_start.extend(_set_services("start"))
            except Exception as service_exc:
                errors.append(f"service_restore:{type(service_exc).__name__}:{service_exc}")
        if lease is not None:
            store.release_migration_lease(lease_id=str(lease["lease_id"]), state="failed")
        store.update_migration_freeze(migration_id=migration_id, state="maintenance")
        after = _snapshot(store)
        status = "HOLD"
    result = {
        "schema": "concept-v11.shared-runtime-migration.v1",
        "status": status,
        "mode": "apply",
        "migration_id": migration_id,
        "migration_epoch": migration_epoch,
        "stage_id": STAGE_ID,
        "owner": owner,
        "tests": tests,
        "before": before,
        "stage_lease": lease,
        "database_backup": db_backup,
        "runtime_backup": runtime_backup,
        "runtime_sync": runtime_sync,
        "service_stop": service_stop,
        "service_start": service_start,
        "rollback": rollback,
        "after": after,
        "external_calls": {"oneapi": 0, "openviking": 0, "concept_refresh": 0},
        "errors": errors,
    }
    _write_report(report_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--migration-id", default=DEFAULT_MIGRATION_ID)
    parser.add_argument("--migration-epoch", default=DEFAULT_MIGRATION_EPOCH)
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--no-manage-services", action="store_true", help="test-only: do not call launchctl")
    parser.add_argument("--skip-tests", action="store_true")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    result = run(
        db_path=args.db_path.expanduser().resolve(),
        runtime_root=args.runtime_root.expanduser().resolve(),
        backup_root=args.backup_root.expanduser().resolve(),
        report_path=args.report.expanduser().resolve(),
        migration_id=str(args.migration_id),
        migration_epoch=str(args.migration_epoch),
        owner=str(args.owner),
        apply=bool(args.apply),
        manage_services=not bool(args.no_manage_services),
        run_tests=not bool(args.skip_tests),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
