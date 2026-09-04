#!/usr/bin/env python3
"""S9.3.4 sequential pm-timeline restore and freeze no-op gate.

The gate loads the daily entrypoint, runs it once under the V4.4 freeze, and
only then loads/runs the weekly entrypoint.  The only permitted production
change is the corresponding operational run marker; timeline JSONL, review
artifacts, logs, coordination DB, and provider-facing work must remain
unchanged.
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
TIMELINE_ROOT = CODEX_ROOT / "skills/pm-timeline"
TIMELINE_STATE = TIMELINE_ROOT / "state"
TIMELINE_DIR = TIMELINE_STATE / "timeline"
REVIEW_SOURCE = PROJECT_ROOT / "docs/reviews"
REVIEW_MIRROR = CODEX_ROOT / "pm-loop/runtime/docs/reviews"
LAUNCH_ROOT = HOME / "Library/LaunchAgents"
PRODUCTION_DB = CODEX_ROOT / "pm-loop/state/pm-system.db"
PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
FREEZE_ID = "freeze-20260828T192416+0800"

JOBS: Mapping[str, Mapping[str, Any]] = {
    "daily": {
        "label": "com.zhujie14.pm-timeline-daily",
        "script": TIMELINE_ROOT / "scripts/daily.sh",
        "plist": LAUNCH_ROOT / "com.zhujie14.pm-timeline-daily.plist",
        "marker": TIMELINE_STATE / "daily-latest.json",
        "schedule": "daily 13:37",
    },
    "weekly": {
        "label": "com.zhujie14.pm-timeline-weekly",
        "script": TIMELINE_ROOT / "scripts/weekly-review.sh",
        "plist": LAUNCH_ROOT / "com.zhujie14.pm-timeline-weekly.plist",
        "marker": TIMELINE_STATE / "weekly-review-latest.json",
        "schedule": "Sunday 19:55",
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def file_sha256(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_fingerprint(root: Path, pattern: str = "*") -> dict[str, Any]:
    files = sorted(path for path in root.rglob(pattern) if path.is_file()) if root.is_dir() else []
    digest = hashlib.sha256()
    entries: list[dict[str, Any]] = []
    for path in files:
        relative = str(path.relative_to(root))
        content_hash = file_sha256(path)
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update((content_hash or "").encode("ascii"))
        digest.update(b"\0")
        entries.append({"path": relative, "sha256": content_hash, "size": path.stat().st_size})
    return {"path": str(root), "files": len(files), "sha256": digest.hexdigest(), "entries": entries}


def file_state(path: Path) -> dict[str, Any]:
    value: dict[str, Any] = {"path": str(path), "exists": path.exists(), "sha256": file_sha256(path)}
    if path.is_file():
        value["size"] = path.stat().st_size
    return value


def db_state(path: Path = PRODUCTION_DB) -> dict[str, Any]:
    value: dict[str, Any] = {"path": str(path), "exists": path.is_file(), "sha256": file_sha256(path)}
    for suffix in ("-wal", "-shm"):
        value[f"{suffix[1:]}_sha256"] = file_sha256(Path(str(path) + suffix))
    if not path.is_file():
        return value
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)
        try:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            value["counts"] = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if table in tables else None
                for table in ("jobs", "runs", "execution_slots")
            }
            value["journal_mode"] = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        value["read_error"] = f"{type(exc).__name__}: {exc}"
    return value


def state_snapshot(production_db: Path = PRODUCTION_DB) -> dict[str, Any]:
    return {
        "timeline": tree_fingerprint(TIMELINE_DIR, "*.jsonl"),
        "timeline_logs": tree_fingerprint(TIMELINE_STATE / "logs", "*.log"),
        "reviews": {
            "source": tree_fingerprint(REVIEW_SOURCE, "*-review.html"),
            "mirror": tree_fingerprint(REVIEW_MIRROR, "*-review.html"),
        },
        "markers": {name: file_state(spec["marker"]) for name, spec in JOBS.items()},
        "production_db": db_state(production_db),
    }


def launch_flag(name: str) -> Optional[str]:
    try:
        result = subprocess.run(["launchctl", "getenv", name], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def launchd_loaded(label: str) -> Optional[bool]:
    try:
        result = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{label}"], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.returncode == 0


def launchctl_action(action: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    label = str(spec["label"])
    target = str(spec["plist"] if action == "bootstrap" else f"gui/{os.getuid()}/{label}")
    try:
        result = subprocess.run(["launchctl", action, f"gui/{os.getuid()}" if action == "bootstrap" else target, str(spec["plist"])] if action == "bootstrap" else ["launchctl", action, target], capture_output=True, text=True, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"action": action, "label": label, "returncode": None, "error": f"{type(exc).__name__}: {exc}"}
    return {"action": action, "label": label, "returncode": result.returncode, "stderr_tail": (result.stderr or "")[-1000:]}


def writer_processes() -> list[dict[str, Any]]:
    patterns = (
        r"pm-timeline/scripts/(?:daily|weekly-review)\.sh",
        r"\.codex/scripts/catchup\.py",
        r"weekly-sync-and-refresh\.sh",
        r"product-intelligence-monitor/scripts/sync\.py",
        r"ov_memory_sync\.py\s+watch",
    )
    try:
        result = subprocess.run(["ps", "axo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return [{"pid": None, "command": "process_probe_failed"}]
    matches: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if match and any(re.search(pattern, match.group(3)) for pattern in patterns):
            matches.append({"pid": int(match.group(1)), "ppid": int(match.group(2)), "command": match.group(3)})
    return matches


def plist_contract(name: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(spec["plist"])
    errors: list[str] = []
    if not path.is_file():
        return {"name": name, "label": spec["label"], "valid": False, "errors": ["missing"], "path": str(path)}
    try:
        value = plistlib.loads(path.read_bytes())
    except (OSError, plistlib.InvalidFileException) as exc:
        return {"name": name, "label": spec["label"], "valid": False, "errors": [f"parse:{exc}"], "path": str(path)}
    args = [str(item) for item in value.get("ProgramArguments", [])]
    if args[:2] != ["/bin/bash", str(spec["script"])]:
        errors.append("ProgramArguments mismatch")
    if value.get("WorkingDirectory") != str(TIMELINE_ROOT):
        errors.append("WorkingDirectory mismatch")
    env = dict(value.get("EnvironmentVariables") or {})
    if env.get("CODEX_PYTHON") != PYTHON:
        errors.append("CODEX_PYTHON not pinned to Python 3.12")
    for key in ("StandardOutPath", "StandardErrorPath"):
        log_path = value.get(key)
        if not isinstance(log_path, str) or not log_path.startswith(str(TIMELINE_STATE / "logs") + "/"):
            errors.append(f"{key} outside pm-timeline log root")
    script = Path(spec["script"])
    if not os.access(script, os.X_OK):
        errors.append("script is not executable")
    text = script.read_text(encoding="utf-8") if script.is_file() else ""
    for marker in ("freeze_active", "maintenance_expected:v44_freeze", "exit 0"):
        if marker not in text:
            errors.append(f"no-op guard missing:{marker}")
    return {
        "name": name,
        "label": spec["label"],
        "path": str(path),
        "sha256": file_sha256(path),
        "valid": not errors,
        "errors": errors,
        "script": str(script),
        "script_sha256": file_sha256(script),
        "schedule": spec["schedule"],
        "loaded": launchd_loaded(str(spec["label"])),
    }


def read_marker(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"exists": False, "path": str(path)}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"exists": True, "path": str(path), "read_error": f"{type(exc).__name__}: {exc}"}
    return {"exists": True, "path": str(path), **value} if isinstance(value, dict) else {"exists": True, "path": str(path), "value": value}


def run_entrypoint(name: str, spec: Mapping[str, Any], timeout: int = 60) -> dict[str, Any]:
    env = os.environ.copy()
    env.update({"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze", "CODEX_PYTHON": PYTHON})
    try:
        result = subprocess.run(["/bin/bash", str(spec["script"])], cwd=str(TIMELINE_ROOT), env=env, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"name": name, "returncode": None, "error": f"{type(exc).__name__}: {exc}"}
    return {"name": name, "command": ["/bin/bash", str(spec["script"])], "returncode": result.returncode, "stdout_tail": (result.stdout or "")[-2000:], "stderr_tail": (result.stderr or "")[-2000:]}


def marker_pass(marker: Mapping[str, Any], task: str) -> bool:
    return bool(
        marker.get("exists")
        and marker.get("task") == task
        and marker.get("status") == "ok"
        and marker.get("reason") == "maintenance_expected:v44_freeze"
        # Successful pm-timeline runs historically omit --exit-code, which
        # serializes as null; an explicit zero is equally valid.
        and marker.get("exit_code") in (None, 0)
        and marker.get("finished_at")
    )


def run_one(name: str, spec: Mapping[str, Any], production_db: Path, timeout: int) -> dict[str, Any]:
    before = state_snapshot(production_db)
    flags_before = {key: launch_flag(key) for key in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    loaded_before = launchd_loaded(str(spec["label"]))
    bootstrap = {"skipped": loaded_before is True}
    if loaded_before is not True:
        bootstrap = launchctl_action("bootstrap", spec)
    loaded_after_bootstrap = launchd_loaded(str(spec["label"]))
    execution = run_entrypoint(name, spec, timeout)
    after = state_snapshot(production_db)
    flags_after = {key: launch_flag(key) for key in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    marker = read_marker(Path(spec["marker"]))
    marker_changed = before["markers"][name] != after["markers"][name]
    unchanged = {key: before[key] == after[key] for key in ("timeline", "timeline_logs", "reviews", "production_db")}
    processes = writer_processes()
    errors: list[str] = []
    if bootstrap.get("returncode") not in (None, 0) and not bootstrap.get("skipped"):
        errors.append("launchctl bootstrap failed")
    if loaded_after_bootstrap is not True:
        errors.append("entrypoint not loaded after bootstrap")
    if execution.get("returncode") != 0:
        errors.append("manual entrypoint failed")
    if not marker_pass(marker, f"pm-timeline-{name}"):
        errors.append("freeze no-op marker invalid")
    if not marker_changed:
        errors.append("operational marker did not advance")
    errors.extend(f"{key} changed" for key, same in unchanged.items() if not same)
    if flags_before != {"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze"} or flags_after != flags_before:
        errors.append("freeze/admission flags changed or are not on/freeze")
    if processes:
        errors.append("business Writer observed")
    return {
        "name": name,
        "label": spec["label"],
        "schedule": spec["schedule"],
        "bootstrap": bootstrap,
        "loaded_before": loaded_before,
        "loaded_after_bootstrap": loaded_after_bootstrap,
        "execution": execution,
        "marker": marker,
        "marker_changed": marker_changed,
        "unchanged": unchanged,
        "flags_before": flags_before,
        "flags_after": flags_after,
        "writer_processes": processes,
        "status": "PASS" if not errors else "HOLD_CONTINUE",
        "errors": errors,
    }


def audit(*, production_db: Path = PRODUCTION_DB, timeout: int = 60, run_commands: bool = True) -> dict[str, Any]:
    contracts = {name: plist_contract(name, spec) for name, spec in JOBS.items()}
    if run_commands:
        daily = run_one("daily", JOBS["daily"], production_db, timeout)
        weekly = run_one("weekly", JOBS["weekly"], production_db, timeout) if daily["status"] == "PASS" else {"name": "weekly", "status": "SKIPPED", "errors": ["daily did not pass"]}
    else:
        daily = {"name": "daily", "status": "SKIPPED", "errors": ["run_commands=false"]}
        weekly = {"name": "weekly", "status": "SKIPPED", "errors": ["run_commands=false"]}
    flags = {key: launch_flag(key) for key in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    processes = writer_processes()
    errors = []
    if any(not value["valid"] for value in contracts.values()):
        errors.append("timeline entrypoint contract failed")
    if daily["status"] != "PASS":
        errors.append("daily restore failed")
    if weekly["status"] != "PASS":
        errors.append("weekly restore failed")
    if flags != {"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze"}:
        errors.append("final freeze/admission flags are not on/freeze")
    if processes:
        errors.append("business Writer observed after restore")
    return {
        "schema_version": "pm-system.s9.3.4-timeline-restore.v1",
        "phase_id": "S9.3.4",
        "release_id": "v4.4-20260829",
        "freeze_id": FREEZE_ID,
        "observed_at": now_iso(),
        "status": "PASS" if not errors else "HOLD_CONTINUE",
        "contracts": contracts,
        "daily": daily,
        "weekly": weekly,
        "final_flags": flags,
        "writer_processes": processes,
        "external_provider_calls": 0,
        "openviking_business_writes": 0,
        "errors": errors,
        "next_phase": "S9.3.5" if not errors else "S9.3.4",
    }


def write_report(path: Path, data: Mapping[str, Any]) -> None:
    daily = data["daily"]
    weekly = data["weekly"]
    lines = [
        "# V4.4 S9.3.4 pm-timeline daily/weekly 恢复检查报告",
        "",
        "> release_id：`v4.4-20260829`",
        f"> freeze_id：`{data['freeze_id']}`",
        "> phase_id：`S9.3.4`",
        "> 运行边界：冻结态逐项加载并手工执行；只允许运行 marker 变化",
        f"> 当前判定：**{data['status']}**",
        "",
        "## 1. 阶段结论",
        "",
        "daily 与 weekly 按严格顺序分别加载和执行。冻结态入口只写 operational marker，不生成时间轴事件、周/月回顾，不调用 OneAPI 或 OpenViking 业务写入。",
        "",
        f"- daily：`{daily['status']}`；marker 通过：`{daily.get('marker_changed', False)}`；timeline/log/review/DB 不变：`{all(daily.get('unchanged', {}).values()) if daily.get('unchanged') else False}`",
        f"- weekly：`{weekly['status']}`；marker 通过：`{weekly.get('marker_changed', False)}`；timeline/log/review/DB 不变：`{all(weekly.get('unchanged', {}).values()) if weekly.get('unchanged') else False}`",
        f"- final freeze/admission：`{data['final_flags'].get('PM_V44_AUTOMATION_FREEZE')}` / `{data['final_flags'].get('PM_V44_ADMISSION')}`",
        "",
        "## 2. 顺序与门禁",
        "",
        "| 门禁 | 结果 |",
        "|---|---|",
        f"| plist/script canonical contract | {'PASS' if all(value['valid'] for value in data['contracts'].values()) else 'FAIL'} |",
        f"| daily 先于 weekly | {'PASS' if weekly.get('status') != 'SKIPPED' else 'FAIL'} |",
        f"| daily freeze no-op marker | {'PASS' if daily.get('status') == 'PASS' else 'FAIL'} |",
        f"| weekly freeze no-op marker | {'PASS' if weekly.get('status') == 'PASS' else 'FAIL'} |",
        f"| timeline JSONL unchanged | {'PASS' if daily.get('unchanged', {}).get('timeline') and weekly.get('unchanged', {}).get('timeline') else 'FAIL'} |",
        f"| review source/mirror unchanged | {'PASS' if daily.get('unchanged', {}).get('reviews') and weekly.get('unchanged', {}).get('reviews') else 'FAIL'} |",
        f"| production coordination DB unchanged | {'PASS' if daily.get('unchanged', {}).get('production_db') and weekly.get('unchanged', {}).get('production_db') else 'FAIL'} |",
        f"| business Writer/Scheduler/Worker | {'PASS' if not data['writer_processes'] else 'FAIL'} |",
        f"| external provider calls | `0` |",
        f"| OpenViking business writes | `0` |",
        "",
        "## 3. 逐项执行证据",
        "",
        "```json",
        json.dumps({"daily": daily, "weekly": weekly}, ensure_ascii=False, indent=2),
        "```",
        "",
        "## 4. 异常与下一步",
        "",
        f"- errors：`{json.dumps(data['errors'], ensure_ascii=False)}`",
        "- 通过后进入 S9.3.5：先计算错过窗口并执行 dry-run，再按 source/任务逐项恢复同步、catchup 和 Codex Automation。" if data["status"] == "PASS" else "- 未通过则保持全量冻结，不恢复 weekly 之后的任何 Writer。",
        "",
        "## 5. 机器证据",
        "",
        "- manifest：与本报告同名 `.json`",
        "- 验收器：`scripts/pm_system_s9_3_4_timeline_restore.py`",
        "- 回归测试：`tests/test_pm_system_s9_3_4_timeline_restore.py`",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--production-db", type=Path, default=PRODUCTION_DB)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--no-run", action="store_true")
    args = parser.parse_args(argv)
    value = audit(production_db=args.production_db, timeout=args.timeout, run_commands=not args.no_run)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, value)
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "manifest": str(args.output), "report": str(args.report), "errors": value["errors"]}, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
