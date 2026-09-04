#!/usr/bin/env python3
"""Safely migrate the PM Loop scheduler to dependency-event schema v11.

The migration is intentionally separate from Scheduler/Worker startup. It
backs up the live SQLite database, proves the backup is readable, holds the
existing migration fence while v11 DDL is applied, and only releases that
fence after the new tables and widened occurrence trigger check are verified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from pm_system_store import PMSystemStore, SCHEMA_VERSION, StoreUnavailable


CODEX_ROOT = Path.home() / ".codex"
DEFAULT_DB_PATH = CODEX_ROOT / "pm-loop" / "state" / "pm-system.db"
DEFAULT_BACKUP_ROOT = CODEX_ROOT / "pm-loop" / "scheduler-migration" / "db-backups"
DEFAULT_MANIFEST = CODEX_ROOT / "pm-loop" / "scheduler-migration" / "S2-v11-dependency-migration.json"
MIGRATION_ID = "pm-loop-scheduler-v11-dependency"
MIGRATION_EPOCH = MIGRATION_ID
STAGE_ID = "S2-SCHEMA-V11-DEPENDENCY"
OWNER = "pm-loop-dependency-migration"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _integrity(db_path: Path) -> str:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=10)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _active_work(db_path: Path) -> dict[str, int]:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=10)
    try:
        return {
            "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status IN ('pending','in_flight','retry_wait')").fetchone()[0]),
            "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status IN ('queued','in_flight','accepted','processing','retry_wait')").fetchone()[0]),
        }
    finally:
        connection.close()


def _backup(db_path: Path, destination: Path) -> dict[str, Any]:
    destination.mkdir(parents=True, exist_ok=False)
    target_path = destination / "pm-system.db"
    source = sqlite3.connect(str(db_path), timeout=20)
    target = sqlite3.connect(str(target_path), timeout=20)
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
        source.close()
    restore_probe = destination / "restore-probe.db"
    probe_source = sqlite3.connect(str(target_path), timeout=10)
    probe_target = sqlite3.connect(str(restore_probe), timeout=10)
    try:
        probe_source.backup(probe_target)
        probe_target.commit()
    finally:
        probe_target.close()
        probe_source.close()
    try:
        restore_integrity = _integrity(restore_probe)
    finally:
        restore_probe.unlink(missing_ok=True)
    return {
        "path": str(target_path),
        "sha256": _sha256(target_path),
        "integrity_check": _integrity(target_path),
        "restore_rehearsal_integrity": restore_integrity,
    }


def _verify_v11(db_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=10)
    try:
        schema = int(connection.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
        occurrence_sql = str(connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='schedule_occurrences'").fetchone()[0])
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        event_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(scheduled_dependency_events)")}
    finally:
        connection.close()
    valid = (
        schema >= SCHEMA_VERSION
        and "'dependency'" in occurrence_sql
        and {"scheduled_dependency_events", "concept_refresh_runs", "concept_refresh_items"} <= tables
        and {"upstream_completed_at", "handler_evidence_path"} <= event_columns
    )
    return {
        "schema_version": schema,
        "dependency_trigger_enabled": "'dependency'" in occurrence_sql,
        "tables": sorted(tables & {"scheduled_dependency_events", "concept_refresh_runs", "concept_refresh_items"}),
        "event_columns": sorted(event_columns),
        "valid": valid,
    }


def migrate(
    *,
    db_path: Path = DEFAULT_DB_PATH,
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    manifest_path: Path = DEFAULT_MANIFEST,
    lease_seconds: int = 900,
) -> dict[str, Any]:
    db_path = Path(db_path).expanduser().resolve()
    backup_root = Path(backup_root).expanduser().resolve()
    manifest_path = Path(manifest_path).expanduser().resolve()
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    store = PMSystemStore(db_path, auto_migrate=False)
    before_schema = store.schema_version()
    if before_schema > SCHEMA_VERSION:
        raise StoreUnavailable(f"unsupported future schema version: {before_schema}")
    integrity_before = _integrity(db_path)
    active = _active_work(db_path)
    existing_freeze = store.migration_freeze()
    if integrity_before.lower() != "ok":
        raise StoreUnavailable(f"SQLite integrity check failed: {integrity_before}")
    if any(active.values()):
        raise StoreUnavailable(f"active PM work must drain before v11 migration: {active}")
    if existing_freeze and str(existing_freeze.get("state") or "") not in {"", "released"}:
        raise StoreUnavailable(f"migration freeze already active: {existing_freeze.get('migration_id')}")

    manifest: dict[str, Any] = {
        "schema_version": "pm-loop.dependency-migration.v1",
        "migration_id": MIGRATION_ID,
        "migration_epoch": MIGRATION_EPOCH,
        "stage_id": STAGE_ID,
        "db_path": str(db_path),
        "before_schema": before_schema,
        "integrity_before": integrity_before,
        "active_before": active,
        "started_at": _iso(_now()),
        "status": "running",
    }
    lease: Optional[dict[str, Any]] = None
    try:
        deadline = _iso(_now() + timedelta(seconds=max(60, int(lease_seconds))))
        store.set_migration_freeze(
            migration_id=MIGRATION_ID,
            migration_epoch=MIGRATION_EPOCH,
            stage_id=STAGE_ID,
            owner=OWNER,
            deadline_at=deadline,
            state="freeze",
        )
        lease = store.acquire_migration_lease(
            migration_id=MIGRATION_ID,
            stage_id=STAGE_ID,
            migration_epoch=MIGRATION_EPOCH,
            owner=OWNER,
            lease_seconds=max(60, int(lease_seconds)),
        )
        backup_id = _now().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        backup = _backup(db_path, backup_root / backup_id)
        if backup["integrity_check"].lower() != "ok" or backup["restore_rehearsal_integrity"].lower() != "ok":
            raise StoreUnavailable("backup verification failed")
        manifest["backup"] = backup
        after_schema = store.migrate()
        verification = _verify_v11(db_path)
        if after_schema < SCHEMA_VERSION or not verification["valid"] or _integrity(db_path).lower() != "ok":
            raise StoreUnavailable(f"v11 verification failed: {verification}")
        store.update_migration_freeze(migration_id=MIGRATION_ID, state="released")
        store.release_migration_lease(lease_id=str(lease["lease_id"]), state="released")
        manifest.update({
            "after_schema": after_schema,
            "integrity_after": _integrity(db_path),
            "verification": verification,
            "lease": lease,
            "status": "completed",
            "completed_at": _iso(_now()),
        })
    except Exception as exc:
        manifest.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}", "lease": lease, "failed_at": _iso(_now())})
        if lease:
            try:
                store.release_migration_lease(lease_id=str(lease["lease_id"]), state="failed")
            except Exception:
                pass
        # Preserve the freeze for operator inspection and recovery.
        raise
    finally:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--lease-seconds", type=int, default=900)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = migrate(db_path=args.db_path, backup_root=args.backup_root, manifest_path=args.manifest, lease_seconds=args.lease_seconds)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
