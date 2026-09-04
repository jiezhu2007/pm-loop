#!/usr/bin/env python3
"""Disable legacy business LaunchAgents while preserving rollback evidence."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional


LEGACY_LABELS = (
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.catchup",
)

Runner = Callable[..., subprocess.CompletedProcess[str]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_plist(path: Path) -> dict[str, Any]:
    try:
        value = plistlib.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        return {"parse_error": f"{type(exc).__name__}: {exc}"}
    return value if isinstance(value, dict) else {"parse_error": "plist is not an object"}


def _run(command: list[str], *, runner: Runner) -> dict[str, Any]:
    try:
        completed = runner(command, capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": command, "returncode": None, "error": f"{type(exc).__name__}: {exc}"}
    return {
        "command": command,
        "returncode": int(completed.returncode),
        "stderr": (completed.stderr or "").strip()[-500:],
    }


def _disable_plist(path: Path) -> None:
    """Persist the rollback-only state without deleting the original file."""
    value = _read_plist(path)
    if value.get("parse_error"):
        raise ValueError(f"cannot update {path}: {value['parse_error']}")
    value["Disabled"] = True
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            plistlib.dump(value, stream, fmt=plistlib.FMT_XML, sort_keys=False)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def cutover(
    *,
    launch_agents: Path,
    backup_root: Path,
    domain: str,
    apply: bool,
    runner: Runner = subprocess.run,
    labels: Iterable[str] = LEGACY_LABELS,
) -> dict[str, Any]:
    """Create a rollback backup, then disable and boot out legacy labels."""
    launch_agents = Path(launch_agents).expanduser().resolve()
    backup_root = Path(backup_root).expanduser().resolve()
    selected = tuple(str(label) for label in labels)
    backup_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = backup_root / backup_id
    entries: list[dict[str, Any]] = []

    for label in selected:
        path = launch_agents / f"{label}.plist"
        config = _read_plist(path) if path.is_file() else None
        entries.append(
            {
                "label": label,
                "path": str(path),
                "exists": path.is_file(),
                "plist_label": config.get("Label") if isinstance(config, dict) else None,
                "disabled_in_plist": bool(config.get("Disabled")) if isinstance(config, dict) else None,
            }
        )

    result: dict[str, Any] = {
        "schema_version": "pm-loop.scheduler-cutover.v1",
        "created_at": _now(),
        "mode": "apply" if apply else "plan",
        "domain": domain,
        "backup_dir": str(backup_dir) if apply else None,
        "legacy": entries,
        "status": "planned" if not apply else "pending",
    }
    if not apply:
        return result

    backup_dir.mkdir(parents=True, exist_ok=False)
    for entry in entries:
        source = Path(str(entry["path"]))
        if source.is_file():
            shutil.copy2(source, backup_dir / source.name)
    (backup_dir / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    errors: list[str] = []
    for entry in entries:
        label = str(entry["label"])
        target = f"{domain}/{label}"
        path = Path(str(entry["path"]))
        try:
            if path.is_file():
                _disable_plist(path)
            entry["plist_disabled"] = bool(_read_plist(path).get("Disabled")) if path.is_file() else False
        except (OSError, ValueError) as exc:
            entry["plist_disabled"] = False
            entry["plist_error"] = f"{type(exc).__name__}: {exc}"
            errors.append(f"plist:{label}")
            continue
        disable = _run(["launchctl", "disable", target], runner=runner)
        entry["disable"] = disable
        if disable["returncode"] != 0:
            errors.append(f"disable:{label}")
            continue
        bootout = _run(["launchctl", "bootout", target], runner=runner)
        entry["bootout"] = bootout
        # A missing service is the desired end state if it had already been
        # unloaded before this cutover runner was invoked.
        if bootout["returncode"] not in {0, 3}:
            errors.append(f"bootout:{label}")
    result["errors"] = errors
    result["status"] = "applied" if not errors else "failed"
    (backup_dir / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-agents", type=Path, default=Path.home() / "Library" / "LaunchAgents")
    parser.add_argument("--backup-root", type=Path, default=Path.home() / ".codex" / "pm-loop" / "scheduler-migration" / "cutover-backups")
    parser.add_argument("--domain", default=f"gui/{os.getuid()}")
    parser.add_argument("--apply", action="store_true", help="Disable and unload legacy labels after creating backups")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = cutover(launch_agents=args.launch_agents, backup_root=args.backup_root, domain=args.domain, apply=args.apply)
    args.output.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    args.output.expanduser().resolve().write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in {"planned", "applied"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
