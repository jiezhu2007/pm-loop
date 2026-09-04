#!/usr/bin/env python3
"""Read-only S9.2.9 gate before restoring any business environment.

The freeze gate deliberately distinguishes services that are safe to keep
loaded in maintenance mode from business writers that must remain unloaded.
Loading a service is not, by itself, evidence that it is allowed to write.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from pm_system_s9_timeline_dry_run import (
    CATCHUP_LOCK,
    TIMELINE_JOBS,
    TIMELINE_ROOT,
    launch_flag,
    launchd_loaded,
    lock_is_held,
    writer_processes,
)


# The checker is copied into ``~/.codex/pm-loop/runtime/scripts`` for launchd
# compatibility, but the authoritative phase reports stay in the canonical
# project tree.  Prefer an explicit override, then the known canonical root,
# and only fall back to the script parent for isolated tests.
_CANONICAL_PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser()
PROJECT_ROOT = _CANONICAL_PROJECT_ROOT if (_CANONICAL_PROJECT_ROOT / "docs/03-产品架构").is_dir() else Path(__file__).resolve().parents[1]
REPORT_DIR = PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告"
REQUIRED_REPORTS = (
    "20260829-S9.2.1-openviking环境核验报告.md",
    "20260829-S9.2.2-oneapi环境核验报告.md",
    "20260829-S9.2.3-control-plane只读核验报告.md",
    "20260829-S9.2.4-scheduler-admission核验报告.md",
    "20260829-S9.2.5-system-health-check核验报告.md",
    "20260829-S9.2.6-时间轴与同步Writer恢复前置检查报告.md",
    "20260829-S9.2.7-时间轴恢复dry-run报告.md",
    "20260829-S9.2.8-时间轴恢复配置封存报告.md",
)
REQUIRED_PAIRS = tuple((name, name[:-3] + ".html") for name in REQUIRED_REPORTS)
ALL_KNOWN_LABELS = (
    "com.zhujie14.codex-oneapi-env",
    "com.zhujie14.openviking-server",
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.pm-system-worker",
    "com.zhujie14.system-health-check",
    "com.zhujie14.system-health-heartbeat",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.ov-memory-sync",
    "com.zhujie14.catchup",
)
OPENVIKING_LABEL = "com.zhujie14.openviking-server"
ONEAPI_LABEL = "com.zhujie14.codex-oneapi-env"
CONTROL_PLANE_LABEL = "com.zhujie14.pm-loop-control-plane"
WORKER_LABEL = "com.zhujie14.pm-system-worker"
FROZEN_ALLOWED_LABELS = (
    CONTROL_PLANE_LABEL,
    WORKER_LABEL,
    "com.zhujie14.system-health-check",
    "com.zhujie14.system-health-heartbeat",
    # daily/weekly are freeze-aware: they record a maintenance no-op marker
    # and do not append production timeline events while admission is frozen.
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
)
# These are real business writers or dispatchers that must remain unloaded
# until S9.3.5 restores them one by one.  A freeze-aware script is not a
# substitute for the restore order because launchd can start an old/stale
# entrypoint.
EXPECTED_UNLOADED_LABELS = (
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.ov-memory-sync",
    "com.zhujie14.catchup",
)
EXPECTED_LOADED_LABELS = (ONEAPI_LABEL, OPENVIKING_LABEL)
# Backwards-compatible name used by older tests and reports.
ALL_WRITER_LABELS = ALL_KNOWN_LABELS
LOCKS = tuple(spec["lock"] for spec in TIMELINE_JOBS.values()) + (CATCHUP_LOCK,)


def report_status(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "exists": False, "pass": False, "reason": "missing"}
    text = path.read_text(encoding="utf-8")
    # Reports use either 当前判定: **PASS** or an HTML metadata cell where
    # tags sit between 当前判定 and PASS.
    normalized = html.unescape(re.sub(r"<[^>]+>", " ", text))
    matched = re.findall(r"当前判定\s*(?:[：:]\s*)?(?:\*\*)?`?\s*(PASS)(?=\b|`|\*|（|\s)", normalized, flags=re.IGNORECASE)
    if not matched:
        matched = re.findall(r"判定\s*[：:]\s*(?:\*\*)?`?\s*(PASS)(?=\b|`|\*|（|\s)", normalized, flags=re.IGNORECASE)
    return {"path": str(path), "exists": True, "pass": bool(matched), "pass_markers": len(matched), "size": path.stat().st_size}


def audit() -> dict[str, Any]:
    report_pairs = [
        {"markdown": report_status(REPORT_DIR / markdown), "html": report_status(REPORT_DIR / html)}
        for markdown, html in REQUIRED_PAIRS
    ]
    flags = {name: launch_flag(name) for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION")}
    labels = {label: launchd_loaded(label) for label in ALL_WRITER_LABELS + (OPENVIKING_LABEL,)}
    locks = [{"path": str(path), "held": lock_is_held(path), "exists": path.exists()} for path in LOCKS]
    processes = writer_processes()
    errors: list[str] = []
    missing_required_labels = [label for label in EXPECTED_LOADED_LABELS if labels[label] is not True]
    unexpected_loaded_labels = [label for label in EXPECTED_UNLOADED_LABELS if labels[label] is not False]
    frozen_allowed_states = {label: labels[label] for label in FROZEN_ALLOWED_LABELS}
    if flags["PM_V44_AUTOMATION_FREEZE"]["value"] != "on" or flags["PM_V44_ADMISSION"]["value"] != "freeze":
        errors.append("freeze/admission not on/freeze")
    if any(not pair["markdown"]["pass"] or not pair["html"]["pass"] for pair in report_pairs):
        errors.append("required S9.2 report pair missing or not PASS")
    if unexpected_loaded_labels:
        errors.append("business LaunchAgent not fully unloaded")
    if missing_required_labels:
        errors.append("required environment service is not loaded")
    if processes:
        errors.append("writer process observed")
    if any(item["held"] is not False for item in locks):
        errors.append("timeline/catchup lock held or unknown")
    return {
        "schema_version": "pm-system.s9.2.9-restore-gate.v2",
        "phase_id": "S9.2.9",
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "status": "PASS" if not errors else "HOLD_CONTINUE",
        "read_only": True,
        "required_report_pairs": report_pairs,
        "freeze": flags,
        "launchd": labels,
        "frozen_allowed_labels": frozen_allowed_states,
        "expected_unloaded_labels": list(EXPECTED_UNLOADED_LABELS),
        "unexpected_loaded_labels": unexpected_loaded_labels,
        "expected_loaded_labels": list(EXPECTED_LOADED_LABELS),
        "missing_required_labels": missing_required_labels,
        "locks": locks,
        "writer_processes": processes,
        "restore_order": [
            "OpenViking server/config (already loaded; verify only)",
            "OneAPI environment initialization",
            "Control Plane maintenance/read-only",
            "Scheduler/Worker admission at 0 then 2 slots",
            "system-health-check and heartbeat",
            "pm-timeline daily/weekly one by one",
            "sync LaunchAgents",
            "catchup dry-run then controlled restore",
            "Codex Automation one by one",
        ],
        "rollback_boundary": "任一项失败即 bootout 当前入口、保持其余入口冻结，保留 S0/S9.1 备份、WAL、manifest、日志和 marker",
        "production_state_touched": False,
        "external_provider_calls": 0,
        "errors": errors,
    }


def write_report(path: Path, value: Mapping[str, Any]) -> None:
    pairs = value["required_report_pairs"]
    lines = [
        "# V4.4 S9.2.9 环境恢复总门禁检查报告",
        "",
        "> release_id：`v4.4-20260829`",
        "> phase_id：`S9.2.9`",
        "> 运行边界：只读；不加载 LaunchAgent、不启动 Writer、不写业务状态",
        f"> 当前判定：**{value['status']}**",
        "",
        "## 1. 阶段结论",
        "",
        "本阶段汇总 S9.2.1～S9.2.8 的报告对和当前宿主机状态，确认恢复顺序、冻结边界和回滚点。总门禁只决定是否具备逐项恢复条件，不直接执行恢复。",
        "",
        f"- 前置报告对：`{sum(item['markdown']['pass'] and item['html']['pass'] for item in pairs)}/{len(pairs)}` PASS",
        f"- freeze/admission：`{value['freeze']['PM_V44_AUTOMATION_FREEZE']['value']}` / `{value['freeze']['PM_V44_ADMISSION']['value']}`",
        f"- 必须卸载的业务入口：`{sum(value['launchd'][label] is False for label in EXPECTED_UNLOADED_LABELS)}/{len(EXPECTED_UNLOADED_LABELS)}` unloaded",
        f"- 冻结态允许加载：`{', '.join(label for label, loaded in value['frozen_allowed_labels'].items() if loaded is True) or 'none'}`",
        f"- OneAPI 环境初始化/OpenViking：`{value['launchd'][ONEAPI_LABEL]}` / `{value['launchd'][OPENVIKING_LABEL]}` loaded",
        f"- Writer 进程：`{len(value['writer_processes'])}`；生产写入：`{value['production_state_touched']}`",
        "",
        "## 2. 前置报告对",
        "",
        "| 阶段 | Markdown | HTML |",
        "|---|---|---|",
    ]
    for pair in pairs:
        phase = next((part for part in Path(pair['markdown']['path']).name.split("-") if part.startswith("S9.2.")), Path(pair['markdown']['path']).stem)
        lines.append(f"| `{phase}` | {'PASS' if pair['markdown']['pass'] else 'FAIL'} | {'PASS' if pair['html']['pass'] else 'FAIL'} |")
    lines.extend([
        "",
        "## 3. 恢复顺序",
        "",
    ])
    lines.extend(f"{index}. {step}" for index, step in enumerate(value["restore_order"], 1))
    lines.extend([
        "",
        "## 4. 回滚边界",
        "",
        value["rollback_boundary"],
        "",
        "## 5. 门禁判定",
        "",
        "| 门禁 | 结果 |",
        "|---|---|",
        f"| S9.2.1～S9.2.8 报告对齐 | {'PASS' if all(item['markdown']['pass'] and item['html']['pass'] for item in pairs) else 'FAIL'} |",
        f"| freeze/admission 保持冻结 | {'PASS' if value['freeze']['PM_V44_AUTOMATION_FREEZE']['value'] == 'on' and value['freeze']['PM_V44_ADMISSION']['value'] == 'freeze' else 'FAIL'} |",
        f"| 必须卸载的业务入口全部 unloaded | {'PASS' if not value['unexpected_loaded_labels'] else 'FAIL'} |",
        f"| 冻结态允许加载入口分类明确 | {'PASS' if all(value['launchd'][label] in (True, False) for label in FROZEN_ALLOWED_LABELS) else 'FAIL'} |",
        f"| OneAPI 环境初始化/OpenViking 保持 loaded | {'PASS' if all(value['launchd'][label] is True for label in EXPECTED_LOADED_LABELS) else 'FAIL'} |",
        f"| Writer=0、锁均未占用 | {'PASS' if not value['writer_processes'] and all(item['held'] is False for item in value['locks']) else 'FAIL'} |",
        "| 本阶段未执行恢复命令 | PASS |",
        "",
        f"### 判定：`{value['status']}`",
        "",
        "总门禁通过后，才允许按恢复顺序逐项执行；每项恢复后必须生成独立报告，任何失败立即回到冻结态。" if value["status"] == "PASS" else "总门禁未通过，继续保持全量冻结。",
        "",
        "## 6. 机器证据",
        "",
        "- 机器 manifest：与本报告同名 `.json`",
        "- 验收器：`scripts/pm_system_s9_restore_gate.py`",
        "- 输入阶段：S9.2.1～S9.2.8 报告及 S9.2.7/S9.2.8 manifest",
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
