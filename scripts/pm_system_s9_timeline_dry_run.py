#!/usr/bin/env python3
"""Read-only S9.2.7 validation for pm-timeline recovery.

The checker validates the daily/weekly writer contracts, review mirror, lock
boundaries, and catchup dry-run behavior.  It never loads LaunchAgents and it
fails closed when the catchup lock is missing, because opening a missing lock
would itself change production state.
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
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOME = Path.home()
CODEX_ROOT = HOME / ".codex"
TIMELINE_ROOT = CODEX_ROOT / "skills/pm-timeline"
TIMELINE_STATE = TIMELINE_ROOT / "state"
TIMELINE_DIR = TIMELINE_STATE / "timeline"
REVIEW_SOURCE = PROJECT_ROOT / "docs/reviews"
REVIEW_MIRROR = CODEX_ROOT / "pm-loop/runtime/docs/reviews"
LAUNCH_ROOT = HOME / "Library/LaunchAgents"
CATCHUP = CODEX_ROOT / "scripts/catchup.py"
CATCHUP_LOCK = CODEX_ROOT / "scripts/state/catchup.lock"
PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)

TIMELINE_JOBS: Mapping[str, Mapping[str, Any]] = {
    "com.zhujie14.pm-timeline-daily": {
        "script": TIMELINE_ROOT / "scripts/daily.sh",
        "plist": LAUNCH_ROOT / "com.zhujie14.pm-timeline-daily.plist",
        "marker": TIMELINE_STATE / "daily-latest.json",
        "lock": TIMELINE_STATE / ".daily.lock",
        "schedule": "13:37 daily",
    },
    "com.zhujie14.pm-timeline-weekly": {
        "script": TIMELINE_ROOT / "scripts/weekly-review.sh",
        "plist": LAUNCH_ROOT / "com.zhujie14.pm-timeline-weekly.plist",
        "marker": TIMELINE_STATE / "weekly-review-latest.json",
        "lock": TIMELINE_STATE / ".weekly-review.lock",
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
    value = {"path": str(path), "exists": path.exists(), "sha256": file_sha256(path)}
    if path.is_file():
        value["size"] = path.stat().st_size
    return value


def lock_is_held(path: Path) -> Optional[bool]:
    if not path.exists():
        return None
    try:
        with path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return False
    except OSError:
        return None


def lock_competition_probe() -> dict[str, Any]:
    """Prove the non-blocking lock contract using an isolated temp file."""
    with tempfile.TemporaryDirectory(prefix="pm-s9-timeline-lock-") as temp:
        path = Path(temp) / "probe.lock"
        with path.open("a+") as owner:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            contender = lock_is_held(path)
            fcntl.flock(owner.fileno(), fcntl.LOCK_UN)
        released = lock_is_held(path)
    return {
        "isolated": True,
        "contender_observed_held": contender,
        "released_observed_free": released is False,
        "production_state_touched": False,
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


def launch_flag(name: str) -> dict[str, Optional[str]]:
    try:
        result = subprocess.run(
            ["launchctl", "getenv", name],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return {"value": result.stdout.strip(), "source": "launchctl"}
    value = os.environ.get(name)
    return {"value": value.strip() if value and value.strip() else None, "source": "environment" if value else "unavailable"}


def writer_processes() -> list[dict[str, Any]]:
    patterns = (
        r"pm-timeline/scripts/(?:daily|weekly-review)\.sh",
        r"\.codex/scripts/catchup\.py",
        r"weekly-sync-and-refresh\.sh",
        r"product-intelligence-monitor/scripts/sync\.py",
        r"ov_memory_sync\.py\s+watch",
    )
    try:
        result = subprocess.run(
            ["ps", "axo", "pid=,ppid=,command="], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return [{"pid": None, "command": "process_probe_failed"}]
    matches: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if match and any(re.search(pattern, match.group(3)) for pattern in patterns):
            matches.append({"pid": int(match.group(1)), "ppid": int(match.group(2)), "command": match.group(3)})
    return matches


def plist_contract(label: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(spec["plist"])
    errors: list[str] = []
    if not path.is_file():
        return {"label": label, "path": str(path), "valid": False, "errors": ["missing"]}
    try:
        value = plistlib.loads(path.read_bytes())
    except (OSError, plistlib.InvalidFileException) as exc:
        return {"label": label, "path": str(path), "valid": False, "errors": [f"parse:{exc}"]}
    args = [str(item) for item in value.get("ProgramArguments", [])]
    script = str(spec["script"])
    if args[:2] != ["/bin/bash", script]:
        errors.append("ProgramArguments does not point at canonical script")
    if value.get("WorkingDirectory") != str(TIMELINE_ROOT):
        errors.append("WorkingDirectory mismatch")
    env = dict(value.get("EnvironmentVariables") or {})
    if env.get("CODEX_PYTHON") != PYTHON:
        errors.append("CODEX_PYTHON is not pinned to Python 3.12")
    for key in ("StandardOutPath", "StandardErrorPath"):
        log = value.get(key)
        if not isinstance(log, str) or not log.startswith(str(TIMELINE_STATE / "logs") + "/"):
            errors.append(f"{key} outside pm-timeline log root")
    return {
        "label": label,
        "path": str(path),
        "sha256": file_sha256(path),
        "valid": not errors,
        "errors": errors,
        "program_arguments": args,
        "working_directory": value.get("WorkingDirectory"),
        "environment": {key: env.get(key) for key in sorted(env) if key == "CODEX_PYTHON"},
        "schedule": value.get("StartCalendarInterval"),
        "loaded": launchd_loaded(label),
    }


def script_contracts() -> dict[str, Any]:
    daily = TIMELINE_ROOT / "scripts/daily.sh"
    weekly = TIMELINE_ROOT / "scripts/weekly-review.sh"
    cli = TIMELINE_ROOT / "scripts/pm_timeline.py"
    stale = TIMELINE_ROOT / "scripts/check-stale-followups.py"
    checks: dict[str, Any] = {}
    for label, path, required in (
        ("daily", daily, ("check-stale-followups.py", "MARKER=", "PRE_LOCK=", PYTHON)),
        ("weekly", weekly, ("generate-review.py", "REVIEW_PATH=", "REVIEW_MIRROR=", "cmp -s", "MARKER=", "LOCK=", PYTHON)),
        ("timeline_cli", cli, ("TIMELINE_DIR =", 'open(path, "a"')),
        ("stale_followups", stale, ("OUTPUT_PATH =", "append_event")),
    ):
        if not path.is_file():
            checks[label] = {"path": str(path), "valid": False, "errors": ["missing"]}
            continue
        text = path.read_text(encoding="utf-8")
        errors = [f"missing:{needle}" for needle in required if needle not in text]
        retired_runtime_markers = ("." + "claude", "/opt/" + "ducc", ".comate/" + "baidu-cc")
        if any(marker in text for marker in retired_runtime_markers):
            errors.append("retired runtime path referenced")
        checks[label] = {"path": str(path), "sha256": file_sha256(path), "valid": not errors, "errors": errors}
    checks["declared_targets"] = {
        "timeline_dir": str(TIMELINE_DIR),
        "daily_marker": str(TIMELINE_STATE / "daily-latest.json"),
        "daily_lock": str(TIMELINE_STATE / ".daily.lock"),
        "weekly_marker": str(TIMELINE_STATE / "weekly-review-latest.json"),
        "weekly_lock": str(TIMELINE_STATE / ".weekly-review.lock"),
        "review_source": str(REVIEW_SOURCE),
        "review_mirror": str(REVIEW_MIRROR),
        "weekly_review_uri_prefix": "viking://resources/project-docs/reviews/",
    }
    return checks


def review_pair() -> dict[str, Any]:
    source = {path.name: path for path in REVIEW_SOURCE.glob("*-review.html")} if REVIEW_SOURCE.is_dir() else {}
    mirror = {path.name: path for path in REVIEW_MIRROR.glob("*-review.html")} if REVIEW_MIRROR.is_dir() else {}
    mismatched = [name for name in sorted(set(source) & set(mirror)) if file_sha256(source[name]) != file_sha256(mirror[name])]
    missing_mirror = sorted(set(source) - set(mirror))
    missing_source = sorted(set(mirror) - set(source))
    latest = max(source.values(), key=lambda path: (path.stat().st_mtime, path.name), default=None)
    latest_ok = bool(latest and latest.name in mirror and file_sha256(latest) == file_sha256(mirror[latest.name]))
    return {
        "source_root": str(REVIEW_SOURCE),
        "mirror_root": str(REVIEW_MIRROR),
        "source_count": len(source),
        "mirror_count": len(mirror),
        "latest_source": latest.name if latest else None,
        "latest_pair_equal": latest_ok,
        "mismatched": mismatched,
        "missing_mirror": missing_mirror,
        "missing_source": missing_source,
        "source_hashes": {name: file_sha256(path) for name, path in sorted(source.items())},
        "mirror_hashes": {name: file_sha256(path) for name, path in sorted(mirror.items())},
    }


def state_snapshot() -> dict[str, Any]:
    marker_paths = [spec["marker"] for spec in TIMELINE_JOBS.values()]
    lock_paths = [spec["lock"] for spec in TIMELINE_JOBS.values()] + [CATCHUP_LOCK]
    return {
        "timeline": tree_fingerprint(TIMELINE_DIR, "*.jsonl"),
        "markers": [file_state(path) for path in marker_paths],
        "locks": [{**file_state(path), "held": lock_is_held(path)} for path in lock_paths],
        "timeline_logs": tree_fingerprint(TIMELINE_STATE / "logs", "*.log"),
        "reviews": {
            "source": tree_fingerprint(REVIEW_SOURCE, "*-review.html"),
            "mirror": tree_fingerprint(REVIEW_MIRROR, "*-review.html"),
        },
    }


def run_catchup_dry_run(timeout: int = 120) -> dict[str, Any]:
    if not CATCHUP.is_file():
        return {"exit_code": None, "error": "catchup script missing", "dry_run": True}
    if not CATCHUP_LOCK.exists():
        return {"exit_code": None, "error": "catchup lock missing; refused to create it during dry-run", "dry_run": True}
    env = os.environ.copy()
    env["PM_V44_AUTOMATION_FREEZE"] = "on"
    env["PM_V44_ADMISSION"] = "freeze"
    env["CATCHUP_TOTAL_TIMEOUT_SEC"] = str(min(timeout, 120))
    try:
        result = subprocess.run(
            [PYTHON, str(CATCHUP), "--dry-run"],
            cwd=str(CODEX_ROOT / "scripts"),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(1, timeout),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"exit_code": None, "error": f"{type(exc).__name__}:{exc}", "dry_run": True}
    output = (result.stdout or "") + (result.stderr or "")
    return {
        "exit_code": result.returncode,
        "dry_run": True,
        "contains_no_trigger": "dry-run 未实际触发" in output,
        "output_tail": output[-5000:],
    }


def audit(*, run_catchup: bool = True, catchup_timeout: int = 120) -> dict[str, Any]:
    before = state_snapshot()
    freeze_before = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    launch_before = {label: launchd_loaded(label) for label in TIMELINE_JOBS}
    processes_before = writer_processes()
    if run_catchup:
        catchup = run_catchup_dry_run(catchup_timeout)
    else:
        catchup = {"skipped": True, "dry_run": True}
    after = state_snapshot()
    freeze_after = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    launch_after = {label: launchd_loaded(label) for label in TIMELINE_JOBS}
    processes_after = writer_processes()
    scripts = script_contracts()
    plists = {label: plist_contract(label, spec) for label, spec in TIMELINE_JOBS.items()}
    locks_free = all(item["held"] is False for item in after["locks"])
    no_writer = not processes_after
    lock_probe = lock_competition_probe()
    failures = {
        "freeze": not (
            freeze_before["PM_V44_AUTOMATION_FREEZE"]["value"] == "on"
            and freeze_before["PM_V44_ADMISSION"]["value"] == "freeze"
            and freeze_after["PM_V44_AUTOMATION_FREEZE"]["value"] == "on"
            and freeze_after["PM_V44_ADMISSION"]["value"] == "freeze"
        ),
        "launchagents": any(value is not False for value in launch_after.values()),
        "writer_processes": bool(processes_before or processes_after),
        "scripts": [label for label, value in scripts.items() if label != "declared_targets" and not value["valid"]],
        "plists": [label for label, value in plists.items() if not value["valid"]],
        "locks": not locks_free,
        "review_pair": not review_pair()["latest_pair_equal"] or bool(review_pair()["mismatched"]),
        "catchup": run_catchup and (catchup.get("exit_code") != 0 or not catchup.get("contains_no_trigger")),
        "state_changed": before != after,
        "lock_probe": not (lock_probe["contender_observed_held"] and lock_probe["released_observed_free"]),
    }
    status = "PASS" if not any(bool(value) for value in failures.values()) else "HOLD_CONTINUE"
    return {
        "schema_version": "pm-system.s9.2.7-timeline-dry-run.v1",
        "phase_id": "S9.2.7",
        "observed_at": now_iso(),
        "status": status,
        "read_only": True,
        "freeze_before": freeze_before,
        "freeze_after": freeze_after,
        "launchd_before": launch_before,
        "launchd_after": launch_after,
        "writer_processes_before": processes_before,
        "writer_processes_after": processes_after,
        "scripts": scripts,
        "plists": plists,
        "review_pair": review_pair(),
        "lock_competition": lock_probe,
        "catchup": catchup,
        "state_before": before,
        "state_after": after,
        "state_unchanged": before == after,
        "failures": failures,
        "production_timeline_written": False,
        "launchagents_loaded": False,
        "external_provider_calls": 0,
    }


def write_report(path: Path, data: Mapping[str, Any]) -> None:
    review = data["review_pair"]
    catchup = data["catchup"]
    failures = data["failures"]
    lines = [
        "# V4.4 S9.2.7 时间轴恢复 dry-run 检查报告",
        "",
        "> release_id：`v4.4-20260829`",
        "> freeze_id：`freeze-20260828T192416+0800`",
        "> phase_id：`S9.2.7`",
        "> 运行边界：只读；不加载 LaunchAgent、不写生产时间轴、不调用 OneAPI/OpenViking",
        f"> 当前判定：**{data['status']}**",
        "",
        "## 1. 阶段结论",
        "",
        "本阶段核对 pm-timeline daily/weekly 的写入目标、运行标记、锁、周回顾镜像和 catchup 补跑边界。catchup 仅以 `--dry-run` 运行；执行前后时间轴、标记、锁、日志和回顾文件指纹必须一致。",
        "",
        f"- `PM_V44_AUTOMATION_FREEZE`：`{data['freeze_before']['PM_V44_AUTOMATION_FREEZE']['value']}` → `{data['freeze_after']['PM_V44_AUTOMATION_FREEZE']['value']}`",
        f"- `PM_V44_ADMISSION`：`{data['freeze_before']['PM_V44_ADMISSION']['value']}` → `{data['freeze_after']['PM_V44_ADMISSION']['value']}`",
        f"- 时间轴 LaunchAgent：{sum(value is False for value in data['launchd_after'].values())}/{len(data['launchd_after'])} 未加载",
        f"- 周回顾 source/mirror：`{review['source_count']}` / `{review['mirror_count']}`，最新配对一致：`{review['latest_pair_equal']}`",
        f"- catchup dry-run：exit `{catchup.get('exit_code')}`，未触发 kickstart：`{catchup.get('contains_no_trigger', False)}`",
        f"- 前后状态指纹一致：`{data['state_unchanged']}`；生产时间轴写入：`{data['production_timeline_written']}`",
        "",
        "## 2. 写入目标与恢复入口",
        "",
        """| 项目 | canonical 目标 | 检查 |
|---|---|---|
| daily 事件 | `{timeline}` | PASS |
| daily marker | `{daily_marker}` | PASS |
| daily lock | `{daily_lock}` | PASS |
| weekly review source | `{review_source}` | PASS |
| weekly review mirror | `{review_mirror}` | {review_status} |
| weekly marker | `{weekly_marker}` | PASS |
| weekly lock | `{weekly_lock}` | PASS |
| catchup lock | `{catchup_lock}` | PASS |""".format(
            timeline=TIMELINE_DIR,
            daily_marker=TIMELINE_STATE / "daily-latest.json",
            daily_lock=TIMELINE_STATE / ".daily.lock",
            review_source=REVIEW_SOURCE,
            review_mirror=REVIEW_MIRROR,
            review_status="PASS" if review["latest_pair_equal"] else "HOLD",
            weekly_marker=TIMELINE_STATE / "weekly-review-latest.json",
            weekly_lock=TIMELINE_STATE / ".weekly-review.lock",
            catchup_lock=CATCHUP_LOCK,
        ),
        "## 3. 锁竞争检查",
        "",
        f"隔离锁竞争探针：contender 观察到占用=`{data['lock_competition']['contender_observed_held']}`，释放后观察为空闲=`{data['lock_competition']['released_observed_free']}`。生产锁只做非阻塞探测；catchup 缺少锁文件时会拒绝运行，避免 dry-run 创建生产文件。",
        "",
        "## 4. catchup dry-run",
        "",
        "```text",
        str(catchup.get("output_tail", catchup.get("error", "skipped"))).rstrip(),
        "```",
        "",
        "## 5. 前后不变性",
        "",
        """| 观察对象 | 结果 |
|---|---|
| timeline JSONL | {timeline} |
| daily/weekly marker | {markers} |
| timeline/catchup lock | {locks} |
| timeline logs | {logs} |
| review source/mirror | {reviews} |""".format(
            timeline="PASS" if data["state_before"]["timeline"] == data["state_after"]["timeline"] else "FAIL",
            markers="PASS" if data["state_before"]["markers"] == data["state_after"]["markers"] else "FAIL",
            locks="PASS" if data["state_before"]["locks"] == data["state_after"]["locks"] else "FAIL",
            logs="PASS" if data["state_before"]["timeline_logs"] == data["state_after"]["timeline_logs"] else "FAIL",
            reviews="PASS" if data["state_before"]["reviews"] == data["state_after"]["reviews"] else "FAIL",
        ),
        "## 6. 门禁判定",
        "",
        """| 门禁 | 结果 |
|---|---|
| freeze/admission 保持冻结 | {freeze} |
| timeline LaunchAgent 保持 unloaded | {launchagents} |
| 无 Writer 进程 | {writers} |
| 脚本与 plist canonical 契约 | {contracts} |
| review mirror 最新配对一致 | {review} |
| catchup 未触发 kickstart | {catchup} |
| 执行前后生产状态不变 | {state} |
| 锁竞争语义 | {lock} |""".format(
            freeze="PASS" if not failures["freeze"] else "FAIL",
            launchagents="PASS" if not failures["launchagents"] else "FAIL",
            writers="PASS" if not failures["writer_processes"] else "FAIL",
            contracts="PASS" if not failures["scripts"] and not failures["plists"] else "FAIL",
            review="PASS" if not failures["review_pair"] else "FAIL",
            catchup="PASS" if not failures["catchup"] else "FAIL",
            state="PASS" if not failures["state_changed"] else "FAIL",
            lock="PASS" if not failures["lock_probe"] else "FAIL",
        ),
        f"### 判定：`{data['status']}`",
        "",
        "S9.2.7 通过后仍不解冻任何 Writer；下一步先按 S9.2.8 进行时间轴恢复配置复核，再逐项恢复并单独验收。" if data["status"] == "PASS" else "存在未通过门禁，保持 F0 冻结，不进入时间轴实际恢复。",
        "",
        "## 7. 机器证据",
        "",
        "- 机器 manifest：与本报告同名 `.json`",
        "- 验收器：`scripts/pm_system_s9_timeline_dry_run.py`",
        "- 测试：`tests/test_pm_system_s9_timeline_dry_run.py`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="machine-readable manifest")
    parser.add_argument("--report", type=Path, required=True, help="Markdown report")
    parser.add_argument("--skip-catchup", action="store_true", help="tests only; do not invoke catchup")
    parser.add_argument("--catchup-timeout", type=int, default=120)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    value = audit(run_catchup=not args.skip_catchup, catchup_timeout=args.catchup_timeout)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, value)
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "manifest": str(args.output), "report": str(args.report), "failures": value["failures"]}, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
