#!/usr/bin/env python3
"""Synchronize unified scheduler code into a runtime mirror atomically."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path(os.environ.get("CODEX_ROOT", str(Path.home() / ".codex"))).expanduser().resolve()
DEFAULT_RUNTIME_ROOT = CODEX_ROOT / "pm-loop" / "runtime"
DEFAULT_BACKUP_ROOT = CODEX_ROOT / "pm-loop" / "scheduler-migration" / "runtime-backups"
DEFAULT_CHECKER_PATH = CODEX_ROOT / "skills" / "system-health-check" / "scripts" / "check_unified_scheduler.py"
FILES = (
    "pm_system_store.py",
    "pm_system_scheduler.py",
    "pm_schedule_registry.py",
    "process_utils.py",
    "pm_loop_scheduler.py",
    "pm_scheduled_handlers.py",
    "concept_v11_schema.py",
    "concept_v11_schema_v2.py",
    "concept_v11_admission.py",
    "concept_v11_bootstrap.py",
    "concept_v11_migration.py",
    "concept_refresh_planner.py",
    "pm_system_gateway.py",
    "pm_resource_dispatcher.py",
    "pm_loop_catchup.py",
    "pm_system_worker.py",
    "pm_ops_attention.py",
    "pm_system_cockpit.py",
    "pm_schedule_replay.py",
    "competitive_radar.py",
    "competitive_radar_read_model.py",
    "retention_registry.py",
    "retention_observer.py",
    "retention_reclaimer.py",
    "retention_read_model.py",
    "concept_inventory_compaction.py",
    "artifact_inventory.py",
    "artifact_manifest.py",
    "artifact_registry_read_model.py",
)
CONFIG_FILES = (
    "schedule-registry.json",
    "retention-source-registry.json",
    "retention-policy.v3.json",
    "retention-deletion-capabilities.json",
)


def config_source_path(name: str) -> Path:
    """Return the canonical source for a runtime configuration file."""
    if name == "schedule-registry.json":
        return PROJECT_ROOT / "scripts" / name
    return PROJECT_ROOT / "config" / name


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    mode = source.stat().st_mode & 0o777
    fd, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as target, source.open("rb") as origin:
            shutil.copyfileobj(origin, target)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, destination)
        destination.chmod(mode)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json_write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_snapshot_manifest(backup_run: Path, *, backup_id: str) -> Optional[dict]:
    """Commit a backup only after every scheduler runtime member is present."""
    relative_paths = [Path("scripts") / name for name in FILES] + [Path("config") / name for name in CONFIG_FILES]
    files = []
    for relative in relative_paths:
        path = backup_run / relative
        if not path.is_file() or path.is_symlink():
            return None
        files.append({"relative_path": relative.as_posix(), "sha256": sha256(path), "bytes": path.stat().st_size})
    manifest = {
        "schema_version": "pm-loop.runtime-backup-manifest.v1",
        "snapshot_id": backup_id,
        "status": "completed",
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "files": files,
    }
    path = backup_run / "snapshot-manifest.json"
    atomic_json_write(path, manifest)
    return {"path": str(path), "sha256": sha256(path), "file_count": len(files)}


def sync(
    *,
    runtime_root: Path = DEFAULT_RUNTIME_ROOT,
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    compat_root: Optional[Path] = None,
    checker_path: Optional[Path] = None,
) -> dict:
    runtime_root = Path(runtime_root).expanduser().resolve()
    backup_root = Path(backup_root).expanduser().resolve()
    backup_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_run = backup_root / backup_id
    backup_run.mkdir(parents=True, exist_ok=False)
    before: dict[str, Optional[str]] = {}
    after: dict[str, str] = {}
    sources: dict[str, str] = {}
    destinations = []
    try:
        for name in FILES:
            source = PROJECT_ROOT / "scripts" / name
            destination = runtime_root / "scripts" / name
            if not source.is_file():
                raise FileNotFoundError(source)
            sources[str(source)] = sha256(source)
            before[str(destination)] = sha256(destination) if destination.is_file() else None
            if destination.is_file():
                backup_target = backup_run / "scripts" / name
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, backup_target)
            atomic_copy(source, destination)
            after[str(destination)] = sha256(destination)
            if after[str(destination)] != sources[str(source)]:
                raise RuntimeError(f"hash mismatch after copy: {destination}")
            destinations.append(str(destination))
        for name in CONFIG_FILES:
            config_source = config_source_path(name)
            config_destination = runtime_root / "config" / name
            if not config_source.is_file():
                raise FileNotFoundError(config_source)
            sources[str(config_source)] = sha256(config_source)
            before[str(config_destination)] = sha256(config_destination) if config_destination.is_file() else None
            if config_destination.is_file():
                backup_target = backup_run / "config" / name
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(config_destination, backup_target)
            atomic_copy(config_source, config_destination)
            after[str(config_destination)] = sha256(config_destination)
            if after[str(config_destination)] != sources[str(config_source)]:
                raise RuntimeError(f"hash mismatch after copy: {config_destination}")
            destinations.append(str(config_destination))
        if compat_root is not None:
            compat_root = Path(compat_root).expanduser().resolve()
            compat_source = PROJECT_ROOT / "scripts" / "catchup.py"
            compat_destination = compat_root / "catchup.py"
            sources[str(compat_source)] = sha256(compat_source)
            before[str(compat_destination)] = sha256(compat_destination) if compat_destination.is_file() else None
            if compat_destination.is_file():
                backup_target = backup_run / "compat" / "catchup.py"
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(compat_destination, backup_target)
            atomic_copy(compat_source, compat_destination)
            after[str(compat_destination)] = sha256(compat_destination)
            if after[str(compat_destination)] != sources[str(compat_source)]:
                raise RuntimeError(f"hash mismatch after copy: {compat_destination}")
            destinations.append(str(compat_destination))
        if checker_path is not None:
            checker_path = Path(checker_path).expanduser().resolve()
            checker_source = PROJECT_ROOT / "scripts" / "check_unified_scheduler.py"
            if not checker_source.is_file():
                raise FileNotFoundError(checker_source)
            sources[str(checker_source)] = sha256(checker_source)
            before[str(checker_path)] = sha256(checker_path) if checker_path.is_file() else None
            if checker_path.is_file():
                backup_target = backup_run / "checker" / checker_path.name
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(checker_path, backup_target)
            atomic_copy(checker_source, checker_path)
            after[str(checker_path)] = sha256(checker_path)
            if after[str(checker_path)] != sources[str(checker_source)]:
                raise RuntimeError(f"hash mismatch after copy: {checker_path}")
            destinations.append(str(checker_path))
    except Exception:
        # Individual replacements are backed up; leave the backup manifest for
        # a human/operator to decide whether to restore after a partial copy.
        raise
    snapshot_manifest = write_snapshot_manifest(backup_run, backup_id=backup_id)
    return {
        "schema_version": "pm-loop.scheduler-runtime-sync.v1",
        "runtime_root": str(runtime_root),
        "backup_root": str(backup_run),
        "sources": sources,
        "before": before,
        "after": after,
        "destinations": destinations,
        "snapshot_manifest": snapshot_manifest,
        "verified": all(digest in set(sources.values()) for digest in after.values()),
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compat-root", type=Path)
    parser.add_argument("--checker-path", type=Path, default=DEFAULT_CHECKER_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = sync(
        runtime_root=args.runtime_root,
        backup_root=args.backup_root,
        compat_root=args.compat_root,
        checker_path=args.checker_path,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
