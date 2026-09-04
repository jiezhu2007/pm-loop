#!/usr/bin/env python3
"""Verify the OpenViking version/commit and patch manifest shipped with PM Loop."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(root: Path) -> dict[str, object]:
    manifest_path = root / "config" / "openviking-dependency.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    patch = root / str(manifest["patch"]["file"])
    actual_hash = sha256(patch)
    expected_hash = str(manifest["patch"]["sha256"])
    if actual_hash != expected_hash:
        raise RuntimeError(f"OpenViking patch hash mismatch: expected {expected_hash}, got {actual_hash}")
    changed_files = list(manifest["patch"]["changed_files"])
    if len(changed_files) != 5 or len(set(changed_files)) != len(changed_files):
        raise RuntimeError("OpenViking changed_files must contain five unique paths")
    if not str(manifest["fork"]["commit"]).isalnum() or len(str(manifest["fork"]["commit"])) != 40:
        raise RuntimeError("OpenViking fork commit must be a full 40-character SHA")
    submodule_path = root / str(manifest["fork"].get("submodule_path") or "vendor/openviking/source")
    submodule_initialized = False
    try:
        checked_out = subprocess.run(
            ["git", "-C", str(submodule_path), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        submodule_initialized = (
            checked_out.returncode == 0
            and checked_out.stdout.strip() == str(manifest["fork"]["commit"])
            and (submodule_path / "pyproject.toml").is_file()
        )
    except (OSError, subprocess.SubprocessError):
        submodule_initialized = False
    return {
        "ok": True,
        "package": manifest["package"],
        "compatible_version": manifest["compatible_version"],
        "fork_commit": manifest["fork"]["commit"],
        "patch_sha256": actual_hash,
        "changed_files": changed_files,
        "submodule_path": str(submodule_path.relative_to(root)),
        "submodule_initialized": submodule_initialized,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    print(json.dumps(verify(args.root.expanduser().resolve()), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
