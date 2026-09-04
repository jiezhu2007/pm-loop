#!/usr/bin/env python3
"""Apply S9.1 runtime configuration atomically without loading jobs.

Only the explicitly listed Codex runtime files are touched.  Every existing
target is copied to a timestamped backup before replacement; no LaunchAgent is
bootstrapped by this command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path(os.environ.get("CODEX_ROOT", str(Path.home() / ".codex"))).expanduser().resolve()
LAUNCH_ROOT = Path(
    os.environ.get("PM_LOOP_LAUNCH_ROOT", str(Path.home() / "Library/LaunchAgents"))
).expanduser().resolve()
RUNTIME_SCRIPTS = CODEX_ROOT / "pm-loop/runtime/scripts"
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
BACKUP_ROOT = CODEX_ROOT / "backups/v4.4-20260829/S9.1-runtime-before"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def backup(path: Path) -> None:
    if not path.exists():
        return
    relative = Path(str(path).lstrip("/"))
    destination = BACKUP_ROOT / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)


def copy_atomic(source: Path, destination: Path) -> None:
    data = source.read_bytes()
    mode = source.stat().st_mode & 0o777
    atomic_bytes(destination, data)
    destination.chmod(mode)


def update_plist(path: Path, updates: Mapping[str, Any]) -> None:
    value = plistlib.loads(path.read_bytes())
    for key, item in updates.items():
        value[key] = item
    atomic_bytes(path, plistlib.dumps(value, fmt=plistlib.FMT_XML, sort_keys=False))


def add_codex_python(path: Path) -> None:
    value = plistlib.loads(path.read_bytes())
    environment = dict(value.get("EnvironmentVariables") or {})
    environment["CODEX_PYTHON"] = CANONICAL_PYTHON
    value["EnvironmentVariables"] = environment
    atomic_bytes(path, plistlib.dumps(value, fmt=plistlib.FMT_XML, sort_keys=False))


def replace_python_in_shell(path: Path) -> None:
    original = path.read_text(encoding="utf-8")
    updated = original.replace(
        "${CLAUDE_PYTHON:-/usr/bin/python3}",
        "${CLAUDE_PYTHON:-python3}",
    )
    updated = updated.replace(
        "/usr/bin/python3 \"$RUNNER\"",
        '"${CODEX_PYTHON:-python3}" "$RUNNER"',
    )
    updated = updated.replace(
        "/usr/bin/python3 run_all_checks.py",
        '"${CODEX_PYTHON:-python3}" run_all_checks.py',
    )
    updated = updated.replace(
        "/usr/bin/python3 - \"$PRODUCT_MARKER\"",
        '"${CODEX_PYTHON:-python3}" - "$PRODUCT_MARKER"',
    )
    if updated == original:
        raise RuntimeError(f"no expected Python invocation found in {path}")
    atomic_bytes(path, updated.encode("utf-8"))


def apply_runtime() -> Dict[str, Any]:
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    source_plists = {
        "com.zhujie14.pm-loop-control-plane.plist": PROJECT_ROOT / "scripts/com.zhujie14.pm-loop-control-plane.plist",
        "com.zhujie14.pm-system-worker.plist": PROJECT_ROOT / "scripts/com.zhujie14.pm-system-worker.plist",
        "com.zhujie14.ov-memory-sync.plist": PROJECT_ROOT / "scripts/com.zhujie14.ov-memory-sync.plist",
        "com.zhujie14.product-intelligence-monitor.plist": PROJECT_ROOT / "scripts/com.zhujie14.product-intelligence-monitor.plist",
    }
    destinations: List[Path] = []
    before: Dict[str, str] = {}
    for name, source in source_plists.items():
        if not source.exists():
            raise FileNotFoundError(source)
        for destination in (LAUNCH_ROOT / name, RUNTIME_SCRIPTS / name):
            if destination.exists():
                backup(destination)
                before[str(destination)] = sha256(destination)
            copy_atomic(source, destination)
            destinations.append(destination)

    # Generate and install the product monitor runtime contract from the
    # canonical project source; this also updates the PM Loop mirror config.
    import importlib.util

    spec = importlib.util.spec_from_file_location("product_schedule_generator", PROJECT_ROOT / "scripts/generate-product-intelligence-schedule.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load schedule generator")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    generated_fd, generated_name = tempfile.mkstemp(prefix="v44-s91-product-", suffix=".plist", dir="/private/tmp")
    os.close(generated_fd)
    generated_plist = Path(generated_name)
    runtime_config = CODEX_ROOT / "skills/product-intelligence-monitor/schedule.json"
    generator.generate(
        PROJECT_ROOT / "scripts/product-intelligence-schedule.json",
        generated_plist,
        runtime_config,
        project_root=PROJECT_ROOT,
        runtime_skill=CODEX_ROOT / "skills/product-intelligence-monitor",
    )
    for destination in (LAUNCH_ROOT / "com.zhujie14.product-intelligence-monitor.plist", RUNTIME_SCRIPTS / "com.zhujie14.product-intelligence-monitor.plist"):
        if destination.exists():
            backup(destination)
            before[str(destination)] = sha256(destination)
        copy_atomic(generated_plist, destination)
        destinations.append(destination)
    generated_plist.unlink(missing_ok=True)
    for source, destination in (
        (PROJECT_ROOT / "scripts/product-intelligence-schedule.json", RUNTIME_SCRIPTS / "product-intelligence-schedule.json"),
        (PROJECT_ROOT / "scripts/generate-product-intelligence-schedule.py", RUNTIME_SCRIPTS / "generate-product-intelligence-schedule.py"),
        (PROJECT_ROOT / "scripts/verify-product-intelligence-schedule.py", RUNTIME_SCRIPTS / "verify-product-intelligence-schedule.py"),
    ):
        if destination.exists():
            backup(destination)
            before[str(destination)] = sha256(destination)
        copy_atomic(source, destination)
        destinations.append(destination)

    # Existing source plists without a project generator receive only the
    # explicit interpreter/env correction; their schedule and paths remain
    # unchanged.
    for name in ("com.zhujie14.catchup.plist",):
        path = LAUNCH_ROOT / name
        backup(path)
        before[str(path)] = sha256(path)
        value = plistlib.loads(path.read_bytes())
        args = list(value.get("ProgramArguments") or [])
        if args and args[0] == "/usr/bin/python3":
            args[0] = CANONICAL_PYTHON
        value["ProgramArguments"] = args
        atomic_bytes(path, plistlib.dumps(value, fmt=plistlib.FMT_XML, sort_keys=False))
        destinations.append(path)

    for name in (
        "com.zhujie14.weekly-sync-and-refresh.plist",
        "com.zhujie14.system-health-check.plist",
        "com.zhujie14.system-health-heartbeat.plist",
        "com.zhujie14.pm-timeline-daily.plist",
        "com.zhujie14.pm-timeline-weekly.plist",
    ):
        path = LAUNCH_ROOT / name
        backup(path)
        before[str(path)] = sha256(path)
        add_codex_python(path)
        destinations.append(path)

    # The health wrappers invoke Python directly and therefore need a guarded
    # fallback even when launched manually outside launchd.
    health_scripts = (
        CODEX_ROOT / "skills/system-health-check/scripts/cron_run.sh",
        CODEX_ROOT / "skills/system-health-check/scripts/heartbeat_check.sh",
    )
    for path in health_scripts:
        backup(path)
        before[str(path)] = sha256(path)
        replace_python_in_shell(path)
        destinations.append(path)

    after = {str(path): sha256(path) for path in destinations if path.exists()}
    return {
        "schema_version": "pm-system.s9.1-runtime-apply.v1",
        "phase_id": "S9.1",
        "backup_root": str(BACKUP_ROOT),
        "canonical_python": CANONICAL_PYTHON,
        "targets": sorted(after),
        "before_sha256": before,
        "after_sha256": after,
        "launchctl_loaded": False,
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = apply_runtime()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
