#!/usr/bin/env python3
"""Atomically synchronize V11 provider and admission-evidence scripts into runtime."""

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
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_ROOT = Path.home() / ".codex" / "pm-loop" / "runtime"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "runtime-backups"
RUNTIME_FILES = (
    "concept_v11_c6_provider_preflight.py",
    "concept_v11_c6_provider_shadow.py",
    "concept_v11_c9_evidence.py",
    "concept_v11_bootstrap.py",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            with source.open("rb") as stream:
                shutil.copyfileobj(stream, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, source.stat().st_mode & 0o777)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sync(
    *,
    runtime_root: Path,
    backup_root: Path,
    apply: bool,
) -> Mapping[str, Any]:
    runtime_root = runtime_root.expanduser().resolve()
    backup_root = backup_root.expanduser().resolve()
    before: dict[str, Any] = {}
    for name in RUNTIME_FILES:
        canonical = PROJECT_ROOT / "scripts" / name
        runtime = runtime_root / "scripts" / name
        if not canonical.is_file() or not runtime.is_file():
            raise FileNotFoundError(f"runtime sync input missing: {name}")
        before[name] = {
            "canonical": str(canonical),
            "runtime": str(runtime),
            "canonical_sha256": _sha256(canonical),
            "runtime_sha256": _sha256(runtime),
        }
    if not apply:
        return {
            "schema": "concept-v11.provider-runtime-sync.v1",
            "status": "DRY_RUN",
            "files": before,
            "would_change": [name for name, row in before.items() if row["canonical_sha256"] != row["runtime_sha256"]],
        }

    destination = backup_root / f"{_timestamp()}-c6-auto-provider-policy-{uuid.uuid4().hex[:8]}"
    destination.mkdir(parents=True, exist_ok=False)
    backups: dict[str, str] = {}
    try:
        for name, row in before.items():
            backup = destination / name
            _atomic_copy(Path(str(row["runtime"])), backup)
            if _sha256(backup) != str(row["runtime_sha256"]):
                raise RuntimeError(f"runtime backup hash mismatch: {name}")
            backups[name] = str(backup)
        for name, row in before.items():
            _atomic_copy(Path(str(row["canonical"])), Path(str(row["runtime"])))
            if _sha256(Path(str(row["runtime"]))) != str(row["canonical_sha256"]):
                raise RuntimeError(f"runtime sync hash mismatch: {name}")
    except Exception:
        for name, backup in backups.items():
            _atomic_copy(Path(backup), Path(str(before[name]["runtime"])))
        raise

    after = {
        name: {
            **row,
            "runtime_sha256_after": _sha256(Path(str(row["runtime"]))),
            "backup": backups[name],
        }
        for name, row in before.items()
    }
    return {
        "schema": "concept-v11.provider-runtime-sync.v1",
        "status": "PASS",
        "backup": str(destination),
        "files": after,
        "verified": all(row["canonical_sha256"] == row["runtime_sha256_after"] for row in after.values()),
    }


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = sync(runtime_root=args.runtime_root, backup_root=args.backup_root, apply=args.apply)
    except (OSError, RuntimeError) as exc:
        report = {"schema": "concept-v11.provider-runtime-sync.v1", "status": "HOLD", "errors": [f"{type(exc).__name__}:{exc}"]}
    _write_report(args.report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("status") in {"PASS", "DRY_RUN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
