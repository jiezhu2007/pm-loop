#!/usr/bin/env python3
"""Atomically deploy the Memory watcher to the Codex runtime mirror.

The active LaunchAgent normally points at the canonical project script.  The
runtime mirror is kept identical for recovery and audit tooling; this command
backs up the previous mirror, replaces it atomically, and never touches the
coordination database or starts a service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
SOURCE = PROJECT_ROOT / "scripts" / "ov_memory_sync.py"
DESTINATION = CODEX_ROOT / "pm-loop" / "runtime" / "scripts" / "ov_memory_sync.py"
DEFAULT_BACKUP = CODEX_ROOT / "backups" / "v4.4-20260829" / "S10.12-watcher-runtime-before"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup(path: Path, root: Path) -> Path:
    destination = root / Path(str(path).lstrip("/"))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)
    return destination


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    mode = source.stat().st_mode & 0o777
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as source_stream:
            shutil.copyfileobj(source_stream, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        destination.chmod(mode)
    finally:
        temporary.unlink(missing_ok=True)


def sync(*, backup_root: Path = DEFAULT_BACKUP) -> dict:
    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)
    backup_root = backup_root.expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_root.chmod(0o700)
    before = sha256(DESTINATION) if DESTINATION.is_file() else None
    backups = [str(backup(DESTINATION, backup_root))] if DESTINATION.is_file() else []
    atomic_copy(SOURCE, DESTINATION)
    source_hash = sha256(SOURCE)
    destination_hash = sha256(DESTINATION)
    if source_hash != destination_hash:
        raise RuntimeError("watcher runtime hash mismatch after atomic copy")
    return {
        "schema_version": "pm-system.s10.12-watcher-runtime-sync.v1",
        "phase_id": "S10.12-watcher-bounded-retry",
        "source": str(SOURCE),
        "destination": str(DESTINATION),
        "source_sha256": source_hash,
        "before_sha256": before,
        "after_sha256": destination_hash,
        "backups": backups,
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = sync(backup_root=args.backup_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
