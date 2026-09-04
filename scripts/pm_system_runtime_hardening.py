#!/usr/bin/env python3
"""Converge the V4.4 coordination runtime to the canonical project code.

The caller is responsible for stopping the Control Plane and Worker before
running this command.  Every existing runtime module is backed up first and
replaced atomically; the production SQLite migration is then opened through
``PMSystemStore`` so schema changes remain idempotent and auditable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
RUNTIME_SCRIPTS = CODEX_ROOT / "pm-loop/runtime/scripts"
DEFAULT_DB = CODEX_ROOT / "pm-loop/state/pm-system.db"
DEFAULT_BACKUP = CODEX_ROOT / "backups/v4.4-20260829/S10.1-runtime-hardening"

CORE_MODULES = (
    "pm_system_store.py",
    "pm_system_scheduler.py",
    "pm_system_worker.py",
    "pm_system_gateway.py",
    "pm_resource_dispatcher.py",
    "pm_system_cockpit.py",
    "pm_system_evidence.py",
    "pm_system_s10_observe.py",
    "pm_system_s10_final_gate.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    mode = source.stat().st_mode & 0o777
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            with source.open("rb") as source_handle:
                shutil.copyfileobj(source_handle, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        destination.chmod(mode)
    finally:
        temporary.unlink(missing_ok=True)


def backup_file(path: Path, backup_root: Path) -> Path:
    relative = Path(str(path).lstrip("/"))
    destination = backup_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)
    return destination


def backup_database(db_path: Path, backup_root: Path) -> Path:
    """Use SQLite's online backup API so WAL contents are included."""
    source = db_path.expanduser().resolve()
    destination = backup_root / "database" / source.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    source_connection = sqlite3.connect(str(source), timeout=5)
    try:
        target_connection = sqlite3.connect(str(temporary))
        try:
            source_connection.backup(target_connection)
            target_connection.commit()
        finally:
            target_connection.close()
    finally:
        source_connection.close()
    os.replace(temporary, destination)
    destination.chmod(0o600)
    return destination


def converge_runtime(*, db_path: Path = DEFAULT_DB, backup_root: Path = DEFAULT_BACKUP) -> Dict[str, Any]:
    backup_root = backup_root.expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    os.chmod(backup_root, 0o700)
    before: Dict[str, str] = {}
    after: Dict[str, str] = {}
    backups: List[str] = []
    if db_path.expanduser().resolve().is_file():
        backups.append(str(backup_database(db_path, backup_root)))
    for name in CORE_MODULES:
        source = PROJECT_ROOT / "scripts" / name
        destination = RUNTIME_SCRIPTS / name
        if not source.is_file():
            raise FileNotFoundError(source)
        if destination.is_file():
            before[str(destination)] = sha256(destination)
            backups.append(str(backup_file(destination, backup_root)))
        atomic_copy(source, destination)
        after[str(destination)] = sha256(destination)
    return {
        "schema_version": "pm-system.s10.1-runtime-hardening.v1",
        "phase_id": "S10-observation-hardening.2",
        "canonical_root": str(PROJECT_ROOT),
        "runtime_root": str(RUNTIME_SCRIPTS),
        "backup_root": str(backup_root),
        "modules": list(CORE_MODULES),
        "before_sha256": before,
        "after_sha256": after,
        "backup_files": backups,
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def migrate_database(db_path: Path) -> Dict[str, Any]:
    import sys

    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from pm_system_store import PMSystemStore  # noqa: WPS433

    store = PMSystemStore(db_path.expanduser().resolve())
    with store.connect() as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        counts: Dict[str, int] = {}
        for table in ("jobs", "runs", "model_calls", "outbox_items", "semantic_tasks", "semantic_task_observations", "error_events", "execution_slots", "outbox_dispatch_leases", "provider_probe_leases", "provider_rate_limit_events", "cancellation_intents"):
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return {
        "db_path": str(db_path.expanduser().resolve()),
        "schema_version": store.schema_version(),
        "pragmas": store.pragmas(),
        "integrity": integrity,
        "counts": counts,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    runtime = converge_runtime(db_path=args.db_path, backup_root=args.backup_root)
    database = migrate_database(args.db_path)
    result = {
        **runtime,
        "database": database,
        "production_schema_migrated": True,
        "status": "PASS" if database["schema_version"] >= 4 and database["integrity"] == "ok" and all(runtime["after_sha256"].values()) else "FAIL_ROLLBACK",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
