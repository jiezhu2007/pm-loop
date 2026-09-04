#!/usr/bin/env python3
"""Safely migrate the PM Loop coordination database to scheduler schema v8.

The migration is intentionally separate from Scheduler/Worker startup.  It
creates a consistent SQLite backup, acquires the existing durable migration
freeze/lease, applies the idempotent v8 DDL, and writes a machine-readable
manifest.  A failed migration keeps the freeze in place so an incomplete
schema cannot be used by a worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from pm_system_store import PMSystemStore, StoreUnavailable


CODEX_ROOT = Path.home() / ".codex"
DEFAULT_DB_PATH = CODEX_ROOT / "pm-loop" / "state" / "pm-system.db"
DEFAULT_BACKUP_ROOT = CODEX_ROOT / "pm-loop" / "scheduler-migration" / "db-backups"
DEFAULT_MANIFEST = CODEX_ROOT / "pm-loop" / "scheduler-migration" / "S1-v8-migration.json"
MIGRATION_ID = "pm-loop-scheduler-v8"
MIGRATION_EPOCH = "pm-loop-scheduler-v8"
STAGE_ID = "S1-SCHEMA-V8"
OWNER = "pm-loop-scheduler-migration"


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


def _schema_version(db_path: Path) -> int:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5)
    try:
        return int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
    finally:
        connection.close()


def _integrity(db_path: Path) -> str:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _active_jobs(db_path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT job_id,run_id,status FROM jobs WHERE status IN ('queued','running','retry_wait') ORDER BY job_id"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def _consistent_backup(db_path: Path, destination: Path) -> dict[str, Any]:
    """Use SQLite's online backup API, then preserve sidecar evidence if any."""
    destination.mkdir(parents=True, exist_ok=False)
    backup_db = destination / "pm-system.db"
    source = sqlite3.connect(str(db_path), timeout=10)
    target = sqlite3.connect(str(backup_db))
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
        source.close()
    sidecars: list[str] = []
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.is_file():
            copied = destination / f"pm-system.db{suffix}"
            shutil.copy2(sidecar, copied)
            sidecars.append(str(copied))
    return {
        "path": str(backup_db),
        "sha256": _sha256(backup_db),
        "sidecars": sidecars,
        "source": str(db_path),
    }


def migrate(
    *,
    db_path: Path = DEFAULT_DB_PATH,
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    manifest_path: Path = DEFAULT_MANIFEST,
    lease_seconds: int = 900,
    allow_active: bool = False,
) -> dict[str, Any]:
    db_path = Path(db_path).expanduser().resolve()
    backup_root = Path(backup_root).expanduser().resolve()
    manifest_path = Path(manifest_path).expanduser().resolve()
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    before_schema = _schema_version(db_path)
    if before_schema > 8:
        raise StoreUnavailable(f"unsupported schema version {before_schema}")
    integrity_before = _integrity(db_path)
    if integrity_before.lower() != "ok":
        raise StoreUnavailable(f"SQLite integrity check failed: {integrity_before}")
    active = _active_jobs(db_path)
    if active and not allow_active:
        raise StoreUnavailable(f"active jobs must drain before migration: {len(active)}")

    backup_root.mkdir(parents=True, exist_ok=True)
    backup_id = _now().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    backup_path = backup_root / backup_id
    store = PMSystemStore(db_path, auto_migrate=False)
    existing_freeze = store.migration_freeze()
    if existing_freeze and str(existing_freeze.get("state")) not in {"released", ""}:
        raise StoreUnavailable(f"migration freeze already active: {existing_freeze.get('migration_id')}")

    freeze_deadline = _iso(_now() + timedelta(seconds=lease_seconds))
    lease: Optional[dict[str, Any]] = None
    backup: Optional[dict[str, Any]] = None
    manifest: Dict[str, Any] = {
        "schema_version": "pm-loop.scheduler-migration.v1",
        "migration_id": MIGRATION_ID,
        "migration_epoch": MIGRATION_EPOCH,
        "stage_id": STAGE_ID,
        "owner": OWNER,
        "db_path": str(db_path),
        "started_at": _iso(_now()),
        "before_schema": before_schema,
        "integrity_before": integrity_before,
        "active_jobs_before": active,
        "status": "running",
    }
    try:
        store.set_migration_freeze(
            migration_id=MIGRATION_ID,
            migration_epoch=MIGRATION_EPOCH,
            stage_id=STAGE_ID,
            owner=OWNER,
            deadline_at=freeze_deadline,
            state="freeze",
        )
        lease = store.acquire_migration_lease(
            migration_id=MIGRATION_ID,
            stage_id=STAGE_ID,
            migration_epoch=MIGRATION_EPOCH,
            owner=OWNER,
            lease_seconds=lease_seconds,
        )
        backup = _consistent_backup(db_path, backup_path)
        manifest["backup"] = backup
        migrated_store = PMSystemStore(db_path, auto_migrate=False)
        after_schema = migrated_store.migrate(max_schema_version=8)
        integrity_after = _integrity(db_path)
        if after_schema != 8 or integrity_after.lower() != "ok":
            raise StoreUnavailable(f"schema/integrity check failed after migration: schema={after_schema}, integrity={integrity_after}")
        manifest.update({"after_schema": after_schema, "integrity_after": integrity_after, "status": "completed", "completed_at": _iso(_now()), "lease": lease})
        store.update_migration_freeze(migration_id=MIGRATION_ID, state="released")
    except Exception as exc:
        manifest.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}", "failed_at": _iso(_now()), "lease": lease, "backup": backup})
        # Keep the durable freeze in place.  A human can inspect the manifest,
        # restore the backup, or rerun the idempotent migration before release.
        if lease:
            try:
                store.release_migration_lease(lease_id=str(lease["lease_id"]), state="failed")
            except Exception:
                pass
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise
    finally:
        if lease and manifest.get("status") == "completed":
            store.release_migration_lease(lease_id=str(lease["lease_id"]), state="released")

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--allow-active", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = migrate(db_path=args.db_path, backup_root=args.backup_root, manifest_path=args.manifest, lease_seconds=args.lease_seconds, allow_active=args.allow_active)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
