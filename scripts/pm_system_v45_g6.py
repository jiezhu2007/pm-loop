#!/usr/bin/env python3
"""V4.5 R2 G6 runtime and LaunchAgent convergence.

G6 is a controlled service restore under the durable migration freeze.  It
syncs only the canonical runtime modules and three maintenance LaunchAgents,
backs up every existing target, then verifies that the Control Plane and
Worker are alive while admission/claim remain blocked by the persistent fence.
Business schedules and Codex Automations stay paused until the final GO.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
RUNTIME_ROOT = CODEX_ROOT / "pm-loop" / "runtime"
RUNTIME_SCRIPTS = RUNTIME_ROOT / "scripts"
LAUNCH_ROOT = Path.home() / "Library" / "LaunchAgents"
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
DEFAULT_DB = CODEX_ROOT / "pm-loop" / "state" / "pm-system.db"
DEFAULT_BACKUP = CODEX_ROOT / "backups" / "v4.5-r2" / "G6-runtime-before"
DEFAULT_MANIFEST = PROJECT_ROOT / "docs/03-产品架构/v4.5实施报告/g6-runtime-manifest.json"

RUNTIME_MODULES = (
    "pm_system_store.py",
    "pm_system_scheduler.py",
    "pm_system_worker.py",
    "pm_system_gateway.py",
    "pm_resource_dispatcher.py",
    "pm_system_cockpit.py",
    "pm_system_evidence.py",
    "pm_system_s10_observe.py",
    "pm_system_s10_final_gate.py",
    "pm_loop_control_plane_server.py",
    "pm_loop_control_plane.py",
    "ov_memory_sync.py",
    "pm_system_s9_writer_preflight.py",
    "pm_system_s9_3_3_health_restore.py",
    "pm_system_s9_timeline_dry_run.py",
)

MAINTENANCE_PLISTS = (
    "com.zhujie14.pm-loop-control-plane.plist",
    "com.zhujie14.pm-system-worker.plist",
    "com.zhujie14.ov-memory-sync.plist",
)

LABELS = (
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.pm-system-worker",
    "com.zhujie14.ov-memory-sync",
)

PROCESS_MARKERS = {
    "com.zhujie14.pm-loop-control-plane": "pm_loop_control_plane_server.py",
    "com.zhujie14.pm-system-worker": "pm_system_worker.py",
    "com.zhujie14.ov-memory-sync": "ov_memory_sync.py",
}

LEGACY_MARKERS = (str(Path.home() / ".claude"), "/opt/ducc", ".comate/baidu-cc")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    mode = source.stat().st_mode & 0o777
    fd, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as input_stream:
            shutil.copyfileobj(input_stream, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        destination.chmod(mode)
    finally:
        temporary.unlink(missing_ok=True)


def backup(path: Path, backup_root: Path) -> str | None:
    if not path.is_file():
        return None
    destination = backup_root / Path(str(path).lstrip("/"))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)
    return str(destination)


def _plist(path: Path) -> dict[str, Any]:
    return dict(plistlib.loads(path.read_bytes()))


def validate_plist(path: Path, *, expected_label: str, runtime_scripts: Path | None = None, mirror: Path | None = None) -> list[str]:
    issues: list[str] = []
    try:
        value = _plist(path)
    except (OSError, plistlib.InvalidFileException, ValueError) as exc:
        return [f"plist unreadable: {type(exc).__name__}: {exc}"]
    if value.get("Label") != expected_label:
        issues.append(f"label={value.get('Label')!r}")
    args = [str(item) for item in (value.get("ProgramArguments") or [])]
    if not args or args[0] != CANONICAL_PYTHON:
        issues.append(f"interpreter={args[0] if args else ''!r}")
    text = "\n".join(args) + "\n" + json.dumps(value.get("EnvironmentVariables") or {}, ensure_ascii=False)
    for marker in LEGACY_MARKERS:
        if marker in text:
            issues.append(f"legacy path: {marker}")
    env = {str(k): str(v) for k, v in (value.get("EnvironmentVariables") or {}).items()}
    runtime_scripts = runtime_scripts or RUNTIME_SCRIPTS
    mirror = mirror or PROJECT_ROOT / "memory" / "openviking"
    if expected_label == "com.zhujie14.ov-memory-sync":
        if "--durable-events" not in args:
            issues.append("watcher missing --durable-events")
        if "--mirror" not in args or "watch" not in args or args.index("--mirror") > args.index("watch"):
            issues.append("watcher global --mirror must precede watch subcommand")
        if env.get("PM_V45_MEMORY_EVENT_MODE") != "outbox":
            issues.append("watcher missing PM_V45_MEMORY_EVENT_MODE=outbox")
        if env.get("PM_V45_NAMESPACE_EPOCH") != "v45-r2-20260830":
            issues.append("watcher namespace epoch mismatch")
        if str(runtime_scripts / "ov_memory_sync.py") not in args:
            issues.append("watcher does not use Codex runtime mirror")
        if str(mirror) not in args and str(mirror.resolve()) not in args:
            issues.append("watcher mirror is not the project memory source")
    elif expected_label == "com.zhujie14.pm-loop-control-plane":
        if str(runtime_scripts / "pm_loop_control_plane_server.py") not in args:
            issues.append("control plane does not use Codex runtime mirror")
    elif expected_label == "com.zhujie14.pm-system-worker":
        if str(runtime_scripts / "pm_system_worker.py") not in args:
            issues.append("worker does not use Codex runtime mirror")
    return issues


def _matching_processes(*, ps_output: str | None = None) -> list[dict[str, Any]]:
    if ps_output is None:
        try:
            ps_output = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,pgid=,stat=,command="],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            return []
    result: list[dict[str, Any]] = []
    for line in ps_output.splitlines():
        fields = line.strip().split(None, 4)
        if len(fields) < 5:
            continue
        command = fields[4]
        label = next((name for name, marker in PROCESS_MARKERS.items() if marker in command), None)
        if not label:
            continue
        try:
            pid = int(fields[0])
        except ValueError:
            continue
        result.append({"label": label, "pid": pid, "ppid": fields[1], "pgid": fields[2], "stat": fields[3], "command": command})
    return result


def _launchctl(args: list[str], *, execute: bool) -> dict[str, Any]:
    if not execute:
        return {"args": args, "returncode": 0, "stdout": "dry-run", "stderr": ""}
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=30)
        return {"args": args, "returncode": result.returncode, "stdout": result.stdout.strip()[-500:], "stderr": result.stderr.strip()[-500:]}
    except (OSError, subprocess.SubprocessError) as exc:
        return {"args": args, "returncode": 127, "stdout": "", "stderr": f"{type(exc).__name__}: {exc}"}


def _read_db_freeze(db_path: Path) -> dict[str, Any] | None:
    import sqlite3

    try:
        connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=3)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute("SELECT * FROM migration_freeze WHERE freeze_id=1").fetchone()
            return dict(row) if row else None
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return None


def _post_freeze_check(url: str, *, execute: bool) -> dict[str, Any]:
    if not execute:
        return {"status": "dry_run", "http_status": None}
    request = urllib.request.Request(url, data=b"{}", method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return {"status": "unexpected_success", "http_status": int(response.status), "body": response.read(300).decode("utf-8", "replace")}
    except urllib.error.HTTPError as exc:
        body = exc.read(500).decode("utf-8", "replace")
        return {"status": "pass" if exc.code == 405 else "unexpected_http", "http_status": int(exc.code), "body": body}
    except (OSError, urllib.error.URLError) as exc:
        return {"status": "unreachable", "http_status": None, "error": f"{type(exc).__name__}: {exc}"}


def apply_g6(
    *,
    project_root: Path = PROJECT_ROOT,
    codex_root: Path = CODEX_ROOT,
    launch_root: Path = LAUNCH_ROOT,
    db_path: Path = DEFAULT_DB,
    backup_root: Path = DEFAULT_BACKUP,
    manifest_path: Path = DEFAULT_MANIFEST,
    execute_launchd: bool = True,
    wait_seconds: int = 20,
) -> dict[str, Any]:
    runtime_scripts = codex_root / "pm-loop" / "runtime" / "scripts"
    launch_root = launch_root.expanduser().resolve()
    backup_root = backup_root.expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_root.chmod(0o700)
    before: dict[str, str | None] = {}
    after: dict[str, str] = {}
    backups: list[str] = []
    issues: list[str] = []

    freeze = _read_db_freeze(db_path)
    freeze_valid = bool(
        freeze
        and str(freeze.get("migration_id")) == "v45-r2-20260830"
        and str(freeze.get("migration_epoch")) == "v45-r2-20260830"
        and str(freeze.get("stage_id")) == "G6"
        and str(freeze.get("state")) == "freeze"
    )
    if not freeze_valid:
        # Never alter runtime files or launch services when the durable fence
        # does not identify this exact stage and epoch.
        result = {
            "schema_version": "pm-system.v45-r2-g6-runtime-manifest.v1",
            "stage_id": "G6",
            "migration_id": "v45-r2-20260830",
            "migration_epoch": "v45-r2-20260830",
            "canonical_python": CANONICAL_PYTHON,
            "runtime_modules": list(RUNTIME_MODULES),
            "maintenance_plists": list(MAINTENANCE_PLISTS),
            "before_sha256": {},
            "after_sha256": {},
            "backups": [],
            "legacy_scan": {"files_scanned": 0, "issues": []},
            "launch_actions": [],
            "processes": [],
            "process_cardinality": {},
            "control_plane_post_freeze": {"status": "not_run"},
            "freeze": freeze,
            "automations": {},
            "production_state_touched": False,
            "external_provider_calls": 0,
            "decision": "HOLD",
            "issues": [f"persistent freeze is not G6: {freeze or {}}"],
        }
        manifest_path = manifest_path.expanduser().resolve()
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result

    for name in RUNTIME_MODULES:
        source = project_root / "scripts" / name
        destination = runtime_scripts / name
        if not source.is_file():
            issues.append(f"missing canonical module: {source}")
            continue
        before[str(destination)] = sha256(destination) if destination.is_file() else None
        old_backup = backup(destination, backup_root)
        if old_backup:
            backups.append(old_backup)
        atomic_copy(source, destination)
        after[str(destination)] = sha256(destination)
        if after[str(destination)] != sha256(source):
            issues.append(f"hash mismatch: {destination}")

    for name in MAINTENANCE_PLISTS:
        source = project_root / "scripts" / name
        if not source.is_file():
            issues.append(f"missing canonical plist: {source}")
            continue
        for destination in (runtime_scripts / name, launch_root / name):
            before[str(destination)] = sha256(destination) if destination.is_file() else None
            old_backup = backup(destination, backup_root)
            if old_backup:
                backups.append(old_backup)
            atomic_copy(source, destination)
            after[str(destination)] = sha256(destination)
            if after[str(destination)] != sha256(source):
                issues.append(f"plist hash mismatch: {destination}")
            issues.extend(validate_plist(destination, expected_label=name.removesuffix(".plist"), runtime_scripts=runtime_scripts, mirror=project_root / "memory" / "openviking"))

    # Old executable/deployment roots are prohibited in the active runtime.
    scanned = 0
    for root in (runtime_scripts, launch_root):
        if not root.is_dir():
            continue
        for path in root.glob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sh", ".plist", ".json"}:
                continue
            scanned += 1
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except OSError as exc:
                issues.append(f"unreadable active file {path}: {exc}")
                continue
            for marker in LEGACY_MARKERS:
                if marker in content:
                    issues.append(f"legacy path {marker} in {path}")

    launch_actions: list[dict[str, Any]] = []
    domain = f"gui/{os.getuid()}"
    for label in LABELS:
        launch_actions.append(_launchctl(["launchctl", "bootout", f"{domain}/{label}"], execute=execute_launchd))
    for name in MAINTENANCE_PLISTS:
        bootstrap_action = _launchctl(["launchctl", "bootstrap", domain, str(launch_root / name)], execute=execute_launchd)
        # launchd may still be draining a KeepAlive job immediately after the
        # bootout. Retry only this registration a few times; never retry a
        # business operation or a provider request.
        if execute_launchd and bootstrap_action.get("returncode") != 0:
            for _ in range(4):
                time.sleep(0.75)
                bootstrap_action = _launchctl(["launchctl", "bootstrap", domain, str(launch_root / name)], execute=True)
                if bootstrap_action.get("returncode") == 0:
                    break
        launch_actions.append(bootstrap_action)

    deadline = time.monotonic() + max(1, int(wait_seconds))
    process_snapshot: list[dict[str, Any]] = []
    if execute_launchd:
        while time.monotonic() < deadline:
            process_snapshot = _matching_processes()
            by_label = {label: sum(1 for item in process_snapshot if item["label"] == label) for label in LABELS}
            if all(value == 1 for value in by_label.values()):
                break
            time.sleep(0.5)
        process_snapshot = _matching_processes()
    by_label = {label: sum(1 for item in process_snapshot if item["label"] == label) for label in LABELS}
    if execute_launchd and any(value != 1 for value in by_label.values()):
        issues.append(f"maintenance process cardinality={by_label}")

    post_freeze = {"status": "dry_run", "http_status": None}
    if execute_launchd:
        readiness_deadline = time.monotonic() + max(1, int(wait_seconds))
        while time.monotonic() < readiness_deadline:
            post_freeze = _post_freeze_check("http://127.0.0.1:8876/api/runs", execute=True)
            if post_freeze.get("status") == "pass":
                break
            time.sleep(0.5)
    if execute_launchd and post_freeze.get("status") != "pass":
        issues.append(f"Control Plane POST freeze check failed: {post_freeze}")
    automations = {}
    for automation_id in ("databuilder", "automation", "v4-4-s10"):
        path = codex_root / "automations" / automation_id / "automation.toml"
        try:
            status_line = next((line for line in path.read_text(encoding="utf-8").splitlines() if line.startswith("status =")), "")
            automations[automation_id] = status_line.split("=", 1)[1].strip().strip('"') if "=" in status_line else "missing"
        except OSError:
            automations[automation_id] = "missing"
    if any(value != "PAUSED" for value in automations.values()):
        issues.append(f"Codex Automations not paused: {automations}")

    result = {
        "schema_version": "pm-system.v45-r2-g6-runtime-manifest.v1",
        "stage_id": "G6",
        "migration_id": "v45-r2-20260830",
        "migration_epoch": "v45-r2-20260830",
        "canonical_python": CANONICAL_PYTHON,
        "runtime_modules": list(RUNTIME_MODULES),
        "maintenance_plists": list(MAINTENANCE_PLISTS),
        "before_sha256": before,
        "after_sha256": after,
        "backups": backups,
        "legacy_scan": {"files_scanned": scanned, "issues": [item for item in issues if "legacy path" in item]},
        "launch_actions": launch_actions,
        "processes": process_snapshot,
        "process_cardinality": by_label,
        "control_plane_post_freeze": post_freeze,
        "freeze": freeze,
        "automations": automations,
        "production_state_touched": False,
        "external_provider_calls": 0,
        "decision": "PASS" if not issues else "HOLD",
        "issues": issues,
    }
    manifest_path = manifest_path.expanduser().resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--no-launchd", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = apply_g6(db_path=args.db_path, backup_root=args.backup_root, manifest_path=args.manifest, execute_launchd=not args.check_only and not args.no_launchd)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["decision"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
