#!/usr/bin/env python3
"""S9.3.3 system-health-check/heartbeat recovery gate.

The gate validates the two read-only health entry points after they are loaded
and runs each once.  Health checks may read local OpenViking health/metadata,
but they must not create PM Runs, claim queue work, write the production
coordination database, or call OneAPI/model endpoints.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOME = Path.home()
CODEX_ROOT = HOME / ".codex"
SKILL_ROOT = CODEX_ROOT / "skills/system-health-check"
SCRIPT_ROOT = SKILL_ROOT / "scripts"
LAUNCH_ROOT = HOME / "Library/LaunchAgents"
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
HEALTH_LABEL = "com.zhujie14.system-health-check"
HEARTBEAT_LABEL = "com.zhujie14.system-health-heartbeat"
PRODUCTION_DB = CODEX_ROOT / "pm-loop/state/pm-system.db"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def launch_flag(name: str) -> dict[str, Optional[str]]:
    try:
        result = subprocess.run(
            ["launchctl", "getenv", name], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return {"value": result.stdout.strip(), "source": "launchctl"}
    value = os.environ.get(name)
    return {
        "value": value.strip() if value and value.strip() else None,
        "source": "process_environment" if value and value.strip() else "unavailable",
    }


def launchd_loaded(label: str) -> Optional[bool]:
    try:
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.returncode == 0


def plist_contract(label: str, script: Path) -> dict[str, Any]:
    path = LAUNCH_ROOT / f"{label}.plist"
    errors: list[str] = []
    if not path.is_file():
        return {"label": label, "path": str(path), "valid": False, "errors": ["missing"]}
    try:
        value = plistlib.loads(path.read_bytes())
    except (OSError, plistlib.InvalidFileException) as exc:
        return {"label": label, "path": str(path), "valid": False, "errors": [f"parse:{exc}"]}
    arguments = [str(item) for item in value.get("ProgramArguments", [])]
    if arguments[:2] != ["/bin/bash", str(script)]:
        errors.append("ProgramArguments does not point at canonical script")
    if value.get("WorkingDirectory") != str(SCRIPT_ROOT):
        errors.append("WorkingDirectory mismatch")
    environment = dict(value.get("EnvironmentVariables") or {})
    if environment.get("CODEX_PYTHON") != CANONICAL_PYTHON:
        errors.append("CODEX_PYTHON is not pinned to Python 3.12")
    for key in ("StandardOutPath", "StandardErrorPath"):
        log_path = value.get(key)
        if not isinstance(log_path, str) or not log_path.startswith(str(CODEX_ROOT / "scripts/state/logs") + "/"):
            errors.append(f"{key} outside Codex log root")
    serialized = json.dumps(value, ensure_ascii=False)
    retired_runtime_markers = ("." + "claude", "/opt/" + "ducc", ".comate/" + "baidu-cc")
    if any(marker in serialized for marker in retired_runtime_markers):
        errors.append("retired runtime path referenced")
    return {
        "label": label,
        "path": str(path),
        "sha256": sha256(path),
        "valid": not errors,
        "errors": errors,
        "program_arguments": arguments,
        "working_directory": value.get("WorkingDirectory"),
        "environment": {"CODEX_PYTHON": environment.get("CODEX_PYTHON")},
        "schedule": value.get("StartCalendarInterval"),
        "loaded": launchd_loaded(label),
    }


def db_snapshot(path: Path = PRODUCTION_DB) -> dict[str, Any]:
    value: dict[str, Any] = {"path": str(path), "exists": path.is_file(), "sha256": sha256(path)}
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        value[f"{suffix[1:]}_sha256"] = sha256(sidecar)
    if not path.is_file():
        return value
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)
        try:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            counts = {}
            for table in ("jobs", "runs", "execution_slots"):
                counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if table in tables else None
            value["schema_version"] = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]) if "schema_migrations" in tables else None
            value["journal_mode"] = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            value["counts"] = counts
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        value["read_error"] = f"{type(exc).__name__}: {exc}"
    return value


def process_probe() -> list[dict[str, Any]]:
    patterns = (
        r"pm_system_(?:scheduler|worker)\.py",
        r"weekly-sync-and-refresh\.sh",
        r"pm-timeline/scripts/(?:daily|weekly-review)\.sh",
        r"ov_memory_sync\.py\s+watch",
        r"shengsuan-sync/scripts/(?:sync(?:-repo)?\.sh|sync-engine\.py)",
        r"product-intelligence-monitor/scripts/sync\.py",
    )
    try:
        result = subprocess.run(
            ["ps", "axo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return [{"pid": None, "command": "process_probe_failed"}]
    matches = []
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if match and any(re.search(pattern, match.group(3)) for pattern in patterns):
            matches.append({"pid": int(match.group(1)), "ppid": int(match.group(2)), "command": match.group(3)})
    return matches


def run_entrypoint(script: Path, timeout: float = 1900) -> dict[str, Any]:
    command = ["/bin/bash", str(script)]
    if script.name == "cron_run.sh":
        command.append("--no-open")
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
        return {
            "command": command,
            "returncode": result.returncode,
            "stdout_tail": (result.stdout or "")[-4000:],
            "stderr_tail": (result.stderr or "")[-2000:],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": command, "returncode": None, "error": f"{type(exc).__name__}: {exc}"}


def latest_health() -> dict[str, Any]:
    path = SKILL_ROOT / "state/latest.json"
    value: dict[str, Any] = {"path": str(path), "exists": path.is_file(), "mtime": path.stat().st_mtime if path.is_file() else None}
    if not path.is_file():
        return value
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        value["read_error"] = f"{type(exc).__name__}: {exc}"
        return value
    checks = report.get("checks") if isinstance(report, dict) else None
    value.update({
        "run_at": report.get("run_at") if isinstance(report, dict) else None,
        "check_count": len(checks) if isinstance(checks, dict) else 0,
        "failed_count": sum(1 for item in checks.values() if isinstance(item, dict) and not item.get("passed")) if isinstance(checks, dict) else None,
        "checker_errors": report.get("checker_errors", []) if isinstance(report, dict) else [],
    })
    return value


def audit(*, run_commands: bool = True, production_db: Path = PRODUCTION_DB) -> dict[str, Any]:
    before_db = db_snapshot(production_db)
    flags_before = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    contracts = {
        HEALTH_LABEL: plist_contract(HEALTH_LABEL, SCRIPT_ROOT / "cron_run.sh"),
        HEARTBEAT_LABEL: plist_contract(HEARTBEAT_LABEL, SCRIPT_ROOT / "heartbeat_check.sh"),
    }
    runs = {}
    if run_commands:
        runs["system-health-check"] = run_entrypoint(SCRIPT_ROOT / "cron_run.sh")
        runs["heartbeat"] = run_entrypoint(SCRIPT_ROOT / "heartbeat_check.sh")
    after_db = db_snapshot(production_db)
    flags_after = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    health = latest_health()
    contract_pass = all(item.get("valid") and item.get("loaded") is True for item in contracts.values())
    run_pass = all(item.get("returncode") == 0 for item in runs.values()) if runs else True
    freeze_pass = (
        flags_before["PM_V44_AUTOMATION_FREEZE"]["value"] in {"on", "true", "1", "enabled"}
        and flags_before["PM_V44_ADMISSION"]["value"] in {"freeze", "frozen", "off", "disabled", "false", "0"}
        and flags_before == flags_after
    )
    health_pass = health.get("exists") and health.get("check_count") == 11 and health.get("failed_count") == 0 and not health.get("checker_errors")
    db_pass = before_db == after_db
    processes = process_probe()
    process_pass = not processes
    errors = []
    if not contract_pass:
        errors.append("health entrypoint contract or launchd load failed")
    if not run_pass:
        errors.append("manual health/heartbeat command failed")
    if not freeze_pass:
        errors.append("freeze/admission flags changed or are not on/freeze")
    if not health_pass:
        errors.append("latest.json does not show 11/11 passing checks")
    if not db_pass:
        errors.append("production coordination DB/WAL/SHM changed")
    if not process_pass:
        errors.append("business writer/scheduler/worker process observed")
    return {
        "schema_version": "pm-system.phase-manifest.v1",
        "phase_id": "S9.3.3",
        "release_id": "v4.4-20260829",
        "freeze_id": "freeze-20260828T192416+0800",
        "observed_at": now_iso(),
        "status": "PASS" if not errors else "HOLD_CONTINUE",
        "contracts": contracts,
        "runs": runs,
        "health_report": health,
        "freeze_before": flags_before,
        "freeze_after": flags_after,
        "production_db_before": before_db,
        "production_db_after": after_db,
        "production_db_unchanged": db_pass,
        "business_processes": processes,
        "external_provider_calls": 0,
        "openviking_business_writes": 0,
        "errors": errors,
        "next_phase": "S9.3.4" if not errors else "S9.3.3",
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--production-db", type=Path, default=PRODUCTION_DB)
    parser.add_argument("--no-run", action="store_true", help="只做契约/状态核验，不执行入口")
    args = parser.parse_args(argv)
    value = audit(run_commands=not args.no_run, production_db=args.production_db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "manifest": str(args.output), "errors": value["errors"]}, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
