#!/usr/bin/env python3
"""Run non-destructive restore drills for the first Retention R1 candidates."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


DEFAULT_STATE_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "state" / "full-inventory"
DEFAULT_DEEP_RUN = DEFAULT_STATE_ROOT / "runs" / "deep-inventory-20260820T120658Z-6257c2"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def file_identity(path: Path) -> dict[str, Any]:
    # Path.stat(follow_symlinks=...) is unavailable in the system Python 3.9
    # used by legacy restore entrypoints. lstat keeps the intended no-follow
    # invariant on both supported runtimes.
    stat = os.lstat(path)
    return {
        "st_dev": stat.st_dev,
        "st_ino": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": sha256(path),
    }


def tree_manifest(root: Path) -> list[dict[str, Any]]:
    root = root.resolve(strict=True)
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"symlink is not allowed in restore drill: {path.relative_to(root)}")
        if not path.is_file():
            continue
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        rows.append({
            "relative_path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": sha256(path),
        })
    return rows


def safe_extract_regular_files(bundle: tarfile.TarFile, destination: Path) -> None:
    """Extract only regular, source-rooted files on Python 3.9 through 3.12."""
    root = destination.resolve(strict=True)
    for member in bundle.getmembers():
        name = member.name
        parts = PurePosixPath(name).parts
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or any(part in {"", ".", ".."} for part in parts)
            or not member.isfile()
        ):
            raise ValueError(f"unsafe archive member: {name!r}")
        target = destination.joinpath(*parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        resolved_parent = target.parent.resolve(strict=True)
        if resolved_parent != root and root not in resolved_parent.parents:
            raise ValueError(f"archive member escapes destination: {name!r}")
        source = bundle.extractfile(member)
        if source is None:
            raise ValueError(f"unable to read archive member: {name!r}")
        with source, target.open("xb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)


def content_dedup_drill(source: Path, compressed: Path, work: Path) -> dict[str, Any]:
    started = time.monotonic()
    work.mkdir(parents=True, exist_ok=False)
    before = file_identity(source)
    compressed_before = file_identity(compressed)
    restored = work / "content-dedup.json"
    with gzip.open(compressed, "rb") as origin, restored.open("wb") as target:
        shutil.copyfileobj(origin, target, length=1024 * 1024)
        target.flush()
        os.fsync(target.fileno())
    restored_hash = sha256(restored)
    after = file_identity(source)
    compressed_after = file_identity(compressed)
    passed = before == after and compressed_before == compressed_after and restored_hash == before["sha256"]
    if not passed:
        raise RuntimeError("content-dedup restore/hash or source identity verification failed")
    return {
        "status": "passed",
        "source_bytes": before["size"],
        "compressed_bytes": compressed_before["size"],
        "restored_bytes": restored.stat().st_size,
        "source_sha256": before["sha256"],
        "restored_sha256": restored_hash,
        "source_identity_unchanged": before == after,
        "compressed_identity_unchanged": compressed_before == compressed_after,
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def deep_inventory_drill(source: Path, work: Path) -> dict[str, Any]:
    started = time.monotonic()
    work.mkdir(parents=True, exist_ok=False)
    source = source.resolve(strict=True)
    before = tree_manifest(source)
    archive = work / "deep-inventory.tar.gz"
    with tarfile.open(archive, "w:gz", compresslevel=1) as bundle:
        for item in before:
            bundle.add(source / item["relative_path"], arcname=f"deep-inventory/{item['relative_path']}", recursive=False)
    restored_root = work / "restored"
    restored_root.mkdir()
    with tarfile.open(archive, "r:gz") as bundle:
        safe_extract_regular_files(bundle, restored_root)
    restored = restored_root / "deep-inventory"
    after_restore = tree_manifest(restored)
    after_source = tree_manifest(source)
    if before != after_source:
        raise RuntimeError("deep-inventory source changed during restore drill")
    if before != after_restore:
        raise RuntimeError("deep-inventory restored manifest does not match source")
    manifest = json.loads((restored / "manifest.json").read_text(encoding="utf-8"))
    progress = manifest.get("progress") if isinstance(manifest.get("progress"), dict) else {}
    smoke = {
        "schema_version": manifest.get("schema_version"),
        "status": manifest.get("status"),
        "resource_count": manifest.get("resource_count"),
        "processed": progress.get("processed"),
        "read": progress.get("read"),
        "unreadable": progress.get("unreadable"),
        "resources_artifact_present": (restored / str(manifest.get("resources_artifact") or "")).is_file(),
        "taxonomy_artifact_present": (restored / str(manifest.get("taxonomy_artifact") or "")).is_file(),
    }
    if not (
        smoke["status"] == "completed"
        and smoke["resource_count"] == 5735
        and smoke["processed"] == 5735
        and smoke["read"] == 5735
        and smoke["unreadable"] == 0
        and smoke["resources_artifact_present"]
        and smoke["taxonomy_artifact_present"]
    ):
        raise RuntimeError("deep-inventory restored consumer smoke failed")
    return {
        "status": "passed",
        "source_file_count": len(before),
        "source_bytes": sum(item["size"] for item in before),
        "archive_bytes": archive.stat().st_size,
        "restored_file_count": len(after_restore),
        "manifest_match": before == after_restore,
        "source_unchanged": before == after_source,
        "consumer_smoke": smoke,
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def run(*, state_root: Path, deep_run: Path, output: Path, temp_root: Path) -> dict[str, Any]:
    output = output.expanduser().resolve()
    temp_root = temp_root.expanduser().resolve()
    temp_root.mkdir(parents=True, exist_ok=True)
    started_at = now_iso()
    with tempfile.TemporaryDirectory(prefix="pm-retention-restore-drill-", dir=temp_root) as temporary:
        work = Path(temporary)
        content = content_dedup_drill(state_root / "content-dedup.json", state_root / "content-dedup.json.gz", work / "content")
        deep = deep_inventory_drill(deep_run, work / "deep")
    result = {
        "schema_version": "pm-loop.retention-restore-drill.v1",
        "status": "passed",
        "started_at": started_at,
        "completed_at": now_iso(),
        "mode": "non_destructive_temporary_restore",
        "originals_modified": False,
        "temporary_artifacts_retained": False,
        "content_dedup": content,
        "deep_inventory": deep,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--deep-run", type=Path, default=DEFAULT_DEEP_RUN)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--temp-root", type=Path, default=Path("/private/tmp"))
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = run(state_root=args.state_root, deep_run=args.deep_run, output=args.output, temp_root=args.temp_root)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError, tarfile.TarError) as exc:
        print(json.dumps({"schema_version": "pm-loop.retention-restore-drill.v1", "status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
