#!/usr/bin/env python3
"""Read-only S9.2.6 preflight for timeline and sync writers.

The checker validates the installed Codex entry points, launchd contracts,
freeze gates, process/lock state, and the side-effect boundary of
``shengsuan-sync plan``.  It never bootstraps a job or calls a write mode.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import plistlib
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path(os.environ.get("CODEX_ROOT", str(Path.home() / ".codex"))).expanduser().resolve()
LAUNCH_ROOT = Path(
    os.environ.get("PM_LOOP_LAUNCH_ROOT", str(Path.home() / "Library/LaunchAgents"))
).expanduser().resolve()
# This is a deployment contract, not the interpreter that happens to run a
# source-checkout test. Deployments on a different host must set CODEX_PYTHON.
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", "/opt/homebrew/opt/python@3.12/bin/python3.12")
SYNC_ENTRY = CODEX_ROOT / "skills/shengsuan-sync/scripts/sync.sh"
TASK_DIR = Path(
    os.environ.get(
        "OPENVIKING_TASK_DIR",
        str(Path.home() / ".openviking/data/viking/default/_system/tasks/default"),
    )
).expanduser().resolve()
SOURCES = (
    "databuilder-internal",
    "feature-list",
    "ontology",
    "data-agent",
    "datasearch",
    "pipeline-logic-fde",
    "product-management",
)

WRITER_JOBS: Mapping[str, Mapping[str, Any]] = {
    "com.zhujie14.pm-timeline-daily": {
        "program": ["/bin/bash", str(CODEX_ROOT / "skills/pm-timeline/scripts/daily.sh")],
        "working_directory": str(CODEX_ROOT / "skills/pm-timeline"),
        "log_root": str(CODEX_ROOT / "skills/pm-timeline/state/logs"),
        "env": {"CODEX_PYTHON": CANONICAL_PYTHON},
    },
    "com.zhujie14.pm-timeline-weekly": {
        "program": ["/bin/bash", str(CODEX_ROOT / "skills/pm-timeline/scripts/weekly-review.sh")],
        "working_directory": str(CODEX_ROOT / "skills/pm-timeline"),
        "log_root": str(CODEX_ROOT / "skills/pm-timeline/state/logs"),
        "env": {"CODEX_PYTHON": CANONICAL_PYTHON},
    },
    "com.zhujie14.weekly-sync-and-refresh": {
        "program": ["/bin/bash", str(CODEX_ROOT / "scripts/weekly-sync-and-refresh.sh")],
        "working_directory": str(CODEX_ROOT / "pm-loop/runtime"),
        "log_root": str(CODEX_ROOT / "scripts/state/logs"),
        "env": {
            "CODEX_PYTHON": CANONICAL_PYTHON,
            "CONCEPT_PROJECT_ROOT": str(CODEX_ROOT / "pm-loop/runtime"),
        },
    },
    "com.zhujie14.product-intelligence-monitor": {
        "program": [CANONICAL_PYTHON, str(CODEX_ROOT / "scripts/run_with_timeout.py")],
        "working_directory": str(PROJECT_ROOT),
        "log_root": str(CODEX_ROOT / "scripts/state/logs"),
        "python_arguments": [0, 5],
    },
    "com.zhujie14.ov-memory-sync": {
        "program": [CANONICAL_PYTHON, str(CODEX_ROOT / "pm-loop/runtime/scripts/ov_memory_sync.py")],
        "working_directory": str(PROJECT_ROOT),
        "log_root": str(Path.home() / ".openviking/logs"),
        "python_arguments": [0],
    },
    "com.zhujie14.catchup": {
        "program": [CANONICAL_PYTHON, str(CODEX_ROOT / "scripts/catchup.py")],
        "working_directory": str(CODEX_ROOT / "scripts"),
        "log_root": str(CODEX_ROOT / "scripts/state/logs"),
        "python_arguments": [0],
    },
}

ACTIVE_SCRIPTS = (
    CODEX_ROOT / "scripts/weekly-sync-and-refresh.sh",
    CODEX_ROOT / "skills/pm-timeline/scripts/daily.sh",
    CODEX_ROOT / "skills/pm-timeline/scripts/weekly-review.sh",
    CODEX_ROOT / "skills/shengsuan-sync/scripts/sync.sh",
    CODEX_ROOT / "skills/shengsuan-sync/scripts/sync-repo.sh",
    CODEX_ROOT / "skills/databuilder-public-docs/scripts/crawl.sh",
    CODEX_ROOT / "skills/databuilder-public-docs/scripts/weekly-sync.sh",
)

LOCKS = (
    CODEX_ROOT / "scripts/state/catchup.lock",
    CODEX_ROOT / "scripts/state/ov-memory-sync.lock",
    CODEX_ROOT / "scripts/state/weekly-sync-and-refresh.lock",
    CODEX_ROOT / "skills/pm-timeline/state/.daily.lock",
    CODEX_ROOT / "skills/pm-timeline/state/.weekly-review.lock",
    CODEX_ROOT / "skills/product-intelligence-monitor/state/run.lock",
    CODEX_ROOT / "skills/product-intelligence-monitor/state/watchdog.lock",
    CODEX_ROOT / "skills/databuilder-public-docs/state/weekly-sync.lock",
)

RUNNING_MARKERS = (
    CODEX_ROOT / "scripts/state/weekly-sync-and-refresh.running",
    CODEX_ROOT / "scripts/state/weekly-sync-and-refresh.resume.json",
    CODEX_ROOT / "skills/shengsuan-sync/state/sync-running.json",
)

WRITER_PROCESS_PATTERNS = (
    r"weekly-sync-and-refresh\.sh",
    r"product-intelligence-monitor/scripts/sync\.py",
    r"ov_memory_sync\.py\s+watch",
    r"pm-timeline/scripts/(?:daily|weekly-review)\.sh",
    r"\.codex/scripts/catchup\.py",
    r"shengsuan-sync/scripts/(?:sync(?:-repo)?\.sh|sync-engine\.py)",
    r"databuilder-public-docs/scripts/(?:weekly-sync|crawl|sync)\.(?:sh|py)",
    r"pm_system_(?:scheduler|worker)\.py",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_fingerprint(root: Path, pattern: str = "*") -> dict[str, Any]:
    files = sorted(path for path in root.rglob(pattern) if path.is_file()) if root.is_dir() else []
    digest = hashlib.sha256()
    for path in files:
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return {"path": str(root), "files": len(files), "sha256": digest.hexdigest()}


def read_launch_flag(name: str) -> dict[str, Optional[str]]:
    try:
        result = subprocess.run(
            ["launchctl", "getenv", name], capture_output=True, text=True, timeout=3, check=False
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


def validate_plist(path: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if not path.is_file():
        return {"path": str(path), "errors": ["missing"], "valid": False}
    value = plistlib.loads(path.read_bytes())
    arguments = list(value.get("ProgramArguments") or [])
    expected = list(contract.get("program") or [])
    if arguments[: len(expected)] != expected:
        errors.append(f"ProgramArguments prefix mismatch: {arguments[:len(expected)]!r}")
    if value.get("WorkingDirectory") != contract.get("working_directory"):
        errors.append(f"WorkingDirectory mismatch: {value.get('WorkingDirectory')!r}")
    environment = dict(value.get("EnvironmentVariables") or {})
    for key, expected_value in dict(contract.get("env") or {}).items():
        if environment.get(key) != expected_value:
            errors.append(f"EnvironmentVariables[{key}] mismatch")
    for index in contract.get("python_arguments") or []:
        if index >= len(arguments) or arguments[index] != CANONICAL_PYTHON:
            errors.append(f"ProgramArguments[{index}] is not canonical Python")
    for key in ("StandardOutPath", "StandardErrorPath"):
        log_path = value.get(key)
        if not isinstance(log_path, str) or not log_path.startswith(str(contract["log_root"]) + "/"):
            errors.append(f"{key} is outside expected log root")
    serialized = json.dumps(value, ensure_ascii=False)
    retired_runtime_markers = ("." + "claude", "/opt/" + "ducc", ".comate/" + "baidu-cc")
    if any(marker in serialized for marker in retired_runtime_markers):
        errors.append("retired runtime path referenced")
    return {
        "path": str(path),
        "valid": not errors,
        "errors": errors,
        "program_arguments": arguments,
        "working_directory": value.get("WorkingDirectory"),
        "environment_keys": sorted(environment),
        "stdout": value.get("StandardOutPath"),
        "stderr": value.get("StandardErrorPath"),
    }


def validate_script(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    if not path.is_file():
        return {"path": str(path), "valid": False, "errors": ["missing"]}
    source = path.read_text(encoding="utf-8")
    expected = f'PY="${{CODEX_PYTHON:-{CANONICAL_PYTHON}}}"'
    if expected not in source:
        errors.append("canonical CODEX_PYTHON default missing")
    if "CLAUDE_PYTHON" in source:
        errors.append("CLAUDE_PYTHON compatibility fallback remains")
    retired_runtime_markers = ("/opt/" + "ducc", ".comate/" + "baidu-cc")
    if any(marker in source for marker in retired_runtime_markers):
        errors.append("retired runtime path referenced")
    return {"path": str(path), "sha256": sha256(path), "valid": not errors, "errors": errors}


def launchd_loaded(label: str) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            capture_output=True,
            text=True,
            timeout=4,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"loaded": None, "error": f"{type(exc).__name__}: {exc}"}
    return {"loaded": result.returncode == 0, "exit_code": result.returncode}


def lock_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "held": False}
    try:
        with path.open("r+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return {"path": str(path), "exists": True, "held": False}
    except BlockingIOError:
        return {"path": str(path), "exists": True, "held": True}
    except OSError as exc:
        return {"path": str(path), "exists": True, "held": None, "error": str(exc)}


def writer_processes() -> list[dict[str, Any]]:
    try:
        result = subprocess.run(
            ["ps", "axo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return [{"pid": None, "command": "process_probe_failed"}]
    matches: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if not match:
            continue
        command = match.group(3)
        if any(re.search(pattern, command) for pattern in WRITER_PROCESS_PATTERNS):
            matches.append({"pid": int(match.group(1)), "ppid": int(match.group(2)), "command": command})
    return matches


def extract_totals(text: str) -> Mapping[str, Any]:
    decoder = json.JSONDecoder()
    values: list[Mapping[str, Any]] = []
    for position, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[position:])
        except ValueError:
            continue
        if isinstance(value, Mapping) and "discovered" in value and "kept" in value:
            values.append(value)
    return dict(values[-1]) if values else {}


def run_plan(source: str, timeout: int) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["/bin/bash", str(SYNC_ENTRY), "plan", source],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=max(1, timeout),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"source": source, "exit_code": None, "error": f"{type(exc).__name__}: {exc}"}
    output = (result.stdout or "") + (result.stderr or "")
    return {"source": source, "exit_code": result.returncode, "totals": extract_totals(output)}


def disabled_inventory_job() -> dict[str, Any]:
    path = LAUNCH_ROOT / "com.zhujie14.shengsuan-concepts-full-inventory-once.plist"
    if not path.is_file():
        return {"path": str(path), "present": False, "blocking": False}
    value = plistlib.loads(path.read_bytes())
    arguments = list(value.get("ProgramArguments") or [])
    return {
        "path": str(path),
        "present": True,
        "disabled": bool(value.get("Disabled")),
        "uses_noncanonical_python": "/usr/bin/python3" in arguments,
        "restore_scope": False,
        "blocking": not bool(value.get("Disabled")),
    }


def core_state() -> dict[str, Any]:
    return {
        "sync_state": tree_fingerprint(CODEX_ROOT / "skills/shengsuan-sync/state"),
        "weekly_state": tree_fingerprint(CODEX_ROOT / "scripts/state"),
        "timeline_state": tree_fingerprint(CODEX_ROOT / "skills/pm-timeline/state"),
        "openviking_tasks": tree_fingerprint(TASK_DIR, "*.json"),
    }


def audit(skip_plans: bool, plan_timeout: int) -> dict[str, Any]:
    plists = {
        label: validate_plist(LAUNCH_ROOT / f"{label}.plist", contract)
        for label, contract in WRITER_JOBS.items()
    }
    scripts = [validate_script(path) for path in ACTIVE_SCRIPTS]
    launchd = {label: launchd_loaded(label) for label in WRITER_JOBS}
    locks = [lock_state(path) for path in LOCKS]
    markers = [{"path": str(path), "exists": path.exists()} for path in RUNNING_MARKERS]
    processes = writer_processes()
    freeze = {
        "PM_V44_AUTOMATION_FREEZE": read_launch_flag("PM_V44_AUTOMATION_FREEZE"),
        "PM_V44_ADMISSION": read_launch_flag("PM_V44_ADMISSION"),
    }
    before = core_state()
    plans = [] if skip_plans else [run_plan(source, plan_timeout) for source in SOURCES]
    after = core_state()
    unchanged = before == after
    disabled = disabled_inventory_job()
    failures = {
        "plists": [label for label, item in plists.items() if not item["valid"]],
        "scripts": [item["path"] for item in scripts if not item["valid"]],
        "loaded_jobs": [label for label, item in launchd.items() if item.get("loaded") is not False],
        "held_or_unknown_locks": [item["path"] for item in locks if item.get("held") is not False],
        "running_markers": [item["path"] for item in markers if item["exists"]],
        "writer_processes": [item.get("pid") for item in processes],
        "plans": [item["source"] for item in plans if item.get("exit_code") != 0],
    }
    freeze_ok = (
        freeze["PM_V44_AUTOMATION_FREEZE"]["value"] == "on"
        and freeze["PM_V44_ADMISSION"]["value"] == "freeze"
    )
    status = "PASS" if not any(failures.values()) and unchanged and freeze_ok and not disabled["blocking"] and not skip_plans else "HOLD_CONTINUE"
    return {
        "schema_version": "pm-system.s9.2.6-writer-preflight.v1",
        "phase_id": "S9.2.6",
        "observed_at": now_iso(),
        "status": status,
        "read_only": True,
        "canonical_python": CANONICAL_PYTHON,
        "freeze": freeze,
        "plists": plists,
        "scripts": scripts,
        "launchd": launchd,
        "locks": locks,
        "running_markers": markers,
        "writer_processes": processes,
        "plans": plans,
        "state_before": before,
        "state_after": after,
        "state_unchanged": unchanged,
        "disabled_inventory_job": disabled,
        "failures": failures,
        "production_writer_started": False,
        "external_provider_calls": 0,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument("--plan-timeout", type=int, default=900)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    value = audit(args.skip_plans, args.plan_timeout)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": value["status"],
        "freeze": value["freeze"],
        "failures": value["failures"],
        "state_unchanged": value["state_unchanged"],
        "plans": [{"source": item["source"], "exit_code": item.get("exit_code"), "totals": item.get("totals", {})} for item in value["plans"]],
    }, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
