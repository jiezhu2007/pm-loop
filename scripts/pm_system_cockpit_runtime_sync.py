#!/usr/bin/env python3
"""Atomically deploy the read-only cockpit fix to the Codex runtime mirror.

This command deliberately touches only the cockpit projection and its static
page. It does not migrate the coordination database, change admission, or
start/stop the Worker; the caller may reload only the Control Plane after the
files and hashes have been verified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
RUNTIME_ROOT = CODEX_ROOT / "pm-loop/runtime"
DEFAULT_BACKUP_ROOT = CODEX_ROOT / "backups/v4.4-20260829/S10.3-cockpit-remediation-before"
TARGETS = (
    (PROJECT_ROOT / "scripts/pm_system_store.py", RUNTIME_ROOT / "scripts/pm_system_store.py"),
    (PROJECT_ROOT / "scripts/pm_system_cockpit.py", RUNTIME_ROOT / "scripts/pm_system_cockpit.py"),
    (PROJECT_ROOT / "scripts/pm_loop_control_plane_server.py", RUNTIME_ROOT / "scripts/pm_loop_control_plane_server.py"),
    (PROJECT_ROOT / "scripts/competitive_radar_read_model.py", RUNTIME_ROOT / "scripts/competitive_radar_read_model.py"),
    (PROJECT_ROOT / "scripts/retention_read_model.py", RUNTIME_ROOT / "scripts/retention_read_model.py"),
    (PROJECT_ROOT / "scripts/retention_registry.py", RUNTIME_ROOT / "scripts/retention_registry.py"),
    (PROJECT_ROOT / "scripts/artifact_registry_read_model.py", RUNTIME_ROOT / "scripts/artifact_registry_read_model.py"),
    (PROJECT_ROOT / "web/pm-loop-control-plane/index.html", RUNTIME_ROOT / "web/pm-loop-control-plane/index.html"),
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


def backup_path(path: Path, backup_root: Path) -> Path:
    relative = Path(str(path).lstrip("/"))
    destination = backup_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)
    return destination


def sync(*, backup_root: Path = DEFAULT_BACKUP_ROOT) -> dict:
    backup_root = backup_root.expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_root.chmod(0o700)
    backup_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    backup_run_root = backup_root / backup_id
    backup_run_root.mkdir(parents=True, exist_ok=False)
    backup_run_root.chmod(0o700)
    before: dict[str, str | None] = {}
    after: dict[str, str] = {}
    backups: list[str] = []
    for source, destination in TARGETS:
        if not source.is_file():
            raise FileNotFoundError(source)
        before[str(destination)] = sha256(destination) if destination.is_file() else None
        if destination.is_file():
            backups.append(str(backup_path(destination, backup_run_root)))
        atomic_copy(source, destination)
        after[str(destination)] = sha256(destination)
        if after[str(destination)] != sha256(source):
            raise RuntimeError(f"hash mismatch after atomic copy: {destination}")
    return {
        "schema_version": "pm-system.s10.3-cockpit-runtime-sync.v1",
        "phase_id": "S10.3-cockpit-remediation",
        "targets": [str(destination) for _, destination in TARGETS],
        "source_sha256": {str(source): sha256(source) for source, _ in TARGETS},
        "before_sha256": before,
        "after_sha256": after,
        "backups": backups,
        "backup_id": backup_id,
        "production_state_touched": False,
        "external_provider_calls": 0,
        "worker_restarted": False,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    value = sync(backup_root=args.backup_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
