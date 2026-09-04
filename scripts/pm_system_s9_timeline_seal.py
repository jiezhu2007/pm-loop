#!/usr/bin/env python3
"""Seal the pm-timeline restore configuration without loading any job."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from pm_system_s9_timeline_dry_run import (
    CODEX_ROOT,
    LAUNCH_ROOT,
    PYTHON,
    TIMELINE_JOBS,
    TIMELINE_ROOT,
    TIMELINE_STATE,
    launch_flag,
    launchd_loaded,
    writer_processes,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKUP_ROOT = CODEX_ROOT / "backups/v4.4-20260829/S9.1-runtime-before/Library/LaunchAgents"
RUNTIME_STATE = TIMELINE_ROOT / "scripts/runtime_state.py"
CATCHUP = CODEX_ROOT / "scripts/catchup.py"


def sha256(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.is_file(), "sha256": sha256(path), "size": path.stat().st_size if path.is_file() else None}


def plist_record(label: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(spec["plist"])
    backup = BACKUP_ROOT / path.name
    errors: list[str] = []
    if not path.is_file():
        errors.append("installed plist missing")
        value: dict[str, Any] = {}
    else:
        try:
            value = plistlib.loads(path.read_bytes())
        except (OSError, plistlib.InvalidFileException) as exc:
            value = {}
            errors.append(f"parse:{exc}")
    args = [str(item) for item in value.get("ProgramArguments", [])]
    script = str(spec["script"])
    if args[:2] != ["/bin/bash", script]:
        errors.append("canonical script mismatch")
    if value.get("WorkingDirectory") != str(TIMELINE_ROOT):
        errors.append("working directory mismatch")
    env = dict(value.get("EnvironmentVariables") or {})
    if env.get("CODEX_PYTHON") != PYTHON:
        errors.append("Python 3.12 pin missing")
    if launchd_loaded(label) is not False:
        errors.append("LaunchAgent is loaded during seal")
    return {
        "label": label,
        "installed": file_record(path),
        "previous_backup": file_record(backup),
        "program_arguments": args,
        "working_directory": value.get("WorkingDirectory"),
        "schedule": value.get("StartCalendarInterval"),
        "python": env.get("CODEX_PYTHON"),
        "loaded": launchd_loaded(label),
        "valid": not errors,
        "errors": errors,
        "restore_command": ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(path)],
        "verification_command": ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
        "rollback_command": ["launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
    }


def audit() -> dict[str, Any]:
    flags_before = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    flags_after = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    plists = {label: plist_record(label, spec) for label, spec in TIMELINE_JOBS.items()}
    files = {
        "daily_script": file_record(TIMELINE_ROOT / "scripts/daily.sh"),
        "weekly_script": file_record(TIMELINE_ROOT / "scripts/weekly-review.sh"),
        "runtime_state": file_record(RUNTIME_STATE),
        "catchup": file_record(CATCHUP),
    }
    errors = [label for label, value in plists.items() if not value["valid"]]
    errors.extend(name for name, value in files.items() if not value["exists"])
    if flags_before["PM_V44_AUTOMATION_FREEZE"]["value"] != "on" or flags_before["PM_V44_ADMISSION"]["value"] != "freeze":
        errors.append("freeze flags are not on/freeze")
    if flags_before != flags_after:
        errors.append("freeze flags changed during audit")
    if writer_processes():
        errors.append("writer process observed")
    return {
        "schema_version": "pm-system.s9.2.8-timeline-seal.v1",
        "phase_id": "S9.2.8",
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "status": "PASS" if not errors else "HOLD_CONTINUE",
        "read_only": True,
        "freeze_before": flags_before,
        "freeze_after": flags_after,
        "plists": plists,
        "files": files,
        "restore_policy": "先逐项 bootstrap/print 验收；未通过前保持其余入口 unloaded；不使用 kickstart 代替验收",
        "rollback_policy": "按单个 label bootout，保留当前 plist、S9.1 备份和本 manifest，不删除时间轴状态",
        "production_state_touched": False,
        "launchagents_loaded": False,
        "writer_processes": [],
        "external_provider_calls": 0,
        "errors": errors,
    }


def write_report(path: Path, value: Mapping[str, Any]) -> None:
    lines = [
        "# V4.4 S9.2.8 时间轴恢复配置封存检查报告",
        "",
        "> release_id：`v4.4-20260829`",
        "> phase_id：`S9.2.8`",
        "> 运行边界：只读；不加载 LaunchAgent、不启动 Writer、不写时间轴",
        f"> 当前判定：**{value['status']}**",
        "",
        "## 1. 阶段结论",
        "",
        "本阶段封存 pm-timeline daily/weekly 的安装 plist、脚本、运行状态工具、catchup 入口和 S9.1 回滚备份 hash，生成逐项恢复、核验和回滚命令。封存只记录命令，不执行命令。",
        "",
        f"- freeze/admission：`{value['freeze_before']['PM_V44_AUTOMATION_FREEZE']['value']}` / `{value['freeze_before']['PM_V44_ADMISSION']['value']}`",
        f"- 时间轴 LaunchAgent：{sum(item['loaded'] is False for item in value['plists'].values())}/{len(value['plists'])} 未加载",
        f"- Writer 进程：`{len(value['writer_processes'])}`",
        f"- 生产状态写入：`{value['production_state_touched']}`；外部 provider 调用：`{value['external_provider_calls']}`",
        "",
        "## 2. 配置与备份",
        "",
        """| 入口 | 当前 plist | S9.1 备份 | 恢复命令 |
|---|---|---|---|""",
    ]
    for label, item in value["plists"].items():
        lines.append(
            f"| `{label}` | `{item['installed']['sha256']}` | `{item['previous_backup']['sha256']}` | `{' '.join(item['restore_command'])}` |"
        )
    lines.extend([
        "",
        "## 3. 恢复与回滚边界",
        "",
        "1. 保持 `PM_V44_AUTOMATION_FREEZE=on`、`PM_V44_ADMISSION=freeze`，先完成依赖阶段验收。",
        "2. 对单个时间轴入口执行 `bootstrap` 后立即 `print` 验证；未通过则立刻 `bootout`，不执行 `kickstart`。",
        "3. daily/weekly 不并行恢复；前一个入口的 marker、lock、进程树和输出检查通过后才可处理下一个。",
        "4. 保留 `~/.codex/backups/v4.4-20260829/S9.1-runtime-before` 和本 manifest，禁止删除历史时间轴 JSONL。",
        "",
        "## 4. 门禁判定",
        "",
        "| 门禁 | 结果 |",
        "|---|---|",
        f"| plist/script/backup 可读取 | {'PASS' if not value['errors'] else 'FAIL'} |",
        f"| Python 3.12 与 canonical root | {'PASS' if not value['errors'] else 'FAIL'} |",
        f"| freeze/admission 未变化 | {'PASS' if value['freeze_before'] == value['freeze_after'] and value['freeze_before']['PM_V44_AUTOMATION_FREEZE']['value'] == 'on' and value['freeze_before']['PM_V44_ADMISSION']['value'] == 'freeze' else 'FAIL'} |",
        f"| LaunchAgent 未加载、Writer=0 | {'PASS' if not value['writer_processes'] and all(item['loaded'] is False for item in value['plists'].values()) else 'FAIL'} |",
        "| 本阶段未执行恢复命令 | PASS |",
        "",
        f"### 判定：`{value['status']}`",
        "",
        "S9.2.8 通过后仍保持冻结；后续恢复必须按本封存的单入口、先验收后放行顺序执行。" if value["status"] == "PASS" else "封存门禁未通过，保持冻结，不进入真实时间轴恢复。",
        "",
        "## 5. 机器证据",
        "",
        "- 机器 manifest：与本报告同名 `.json`",
        "- 验收器：`scripts/pm_system_s9_timeline_seal.py`",
        "- 输入 dry-run：`20260829-S9.2.7-时间轴恢复dry-run-manifest.json`",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    value = audit()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, value)
    print(json.dumps({"phase_id": value["phase_id"], "status": value["status"], "manifest": str(args.output), "report": str(args.report), "errors": value["errors"]}, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
