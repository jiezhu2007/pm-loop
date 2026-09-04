#!/usr/bin/env python3
"""Read-only S9 environment-recovery and missed-task dry-run.

This command inventories production configuration without loading jobs,
starting workers, modifying LaunchAgents, or calling OneAPI/OpenViking.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import plistlib
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping


CODEX_ROOT = Path.home() / ".codex"
PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
REPORT_DIR = PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告"
LAUNCH_AGENTS = Path.home() / "Library/LaunchAgents"
LEDGER = CODEX_ROOT / "skills/shengsuan-sync/state/ledger.json"
PENDING = CODEX_ROOT / "skills/shengsuan-sync/state/pending-uploads.json"
CHECKPOINT = CODEX_ROOT / "skills/shengsuan-sync/state/hash-only-checkpoint.json"
HEALTH = CODEX_ROOT / "skills/system-health-check/state/latest.json"
PYTHON312 = os.environ.get("CODEX_PYTHON", sys.executable)

LABELS = (
    "com.zhujie14.openviking-server",
    "com.zhujie14.codex-oneapi-env",
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.system-health-check",
    "com.zhujie14.system-health-heartbeat",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.ov-memory-sync",
    "com.zhujie14.catchup",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _launchctl_state(label: str) -> Dict[str, Any]:
    try:
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "unknown", "error": f"{type(exc).__name__}:{exc}"}
    if result.returncode == 0:
        lines = result.stdout.splitlines()
        state = next((line.split("=", 1)[1].strip() for line in lines if line.strip().startswith("state =")), "loaded")
        return {"status": "loaded", "state": state}
    return {"status": "not_loaded", "stderr": result.stderr.strip()[:300]}


def _plist_snapshot(label: str) -> Dict[str, Any]:
    path = LAUNCH_AGENTS / f"{label}.plist"
    if not path.exists():
        return {"label": label, "exists": False}
    try:
        data = plistlib.loads(path.read_bytes())
    except (OSError, plistlib.InvalidFileException) as exc:
        return {"label": label, "exists": True, "parse_error": f"{type(exc).__name__}:{exc}"}
    arguments = [str(item) for item in data.get("ProgramArguments", [])]
    unpinned = [item for item in arguments if item in {"/usr/bin/python3", "python3"}]
    return {
        "label": label,
        "path": str(path),
        "exists": True,
        "sha256": _sha256(path),
        "program_arguments": arguments,
        "working_directory": data.get("WorkingDirectory"),
        "environment": data.get("EnvironmentVariables", {}),
        "keep_alive": data.get("KeepAlive"),
        "start_calendar_interval": data.get("StartCalendarInterval"),
        "interpreter_unpinned": unpinned,
        "launchctl": _launchctl_state(label),
    }


def _ledger_snapshot() -> Dict[str, Any]:
    if not LEDGER.exists():
        return {"exists": False}
    data = json.loads(LEDGER.read_text(encoding="utf-8"))
    values = list(data.values()) if isinstance(data, dict) else []
    sources = collections.Counter(str(item.get("source") or "unknown") for item in values if isinstance(item, dict))
    with_hash = sum(1 for item in values if isinstance(item, dict) and item.get("sha256"))
    return {
        "exists": True,
        "path": str(LEDGER),
        "sha256": _sha256(LEDGER),
        "rows": len(values),
        "sources": dict(sorted(sources.items())),
        "rows_with_sha256": with_hash,
        "rows_without_sha256": len(values) - with_hash,
    }


def _pending_snapshot() -> Dict[str, Any]:
    if not PENDING.exists():
        return {"exists": False}
    data = json.loads(PENDING.read_text(encoding="utf-8"))
    values = data.get("items", []) if isinstance(data, dict) else []
    status = collections.Counter(str(item.get("status") or "unknown") for item in values if isinstance(item, dict))
    task_status = collections.Counter(str(item.get("task_status") or "unknown") for item in values if isinstance(item, dict))
    sources = collections.Counter((str(item.get("uri") or "").split("/")[4] if len(str(item.get("uri") or "").split("/")) > 4 else "unknown") for item in values if isinstance(item, dict))
    queued_unknown = [
        {"doc_guid": item.get("docGuid"), "uri": item.get("uri"), "task_id": item.get("task_id"), "idempotency_key": f"{item.get('uri')}|revision:unknown|vectors_only|oneapi"}
        for item in values
        if isinstance(item, dict) and item.get("status") == "queued" and item.get("task_status") in {None, "unknown"}
    ]
    return {
        "exists": True,
        "path": str(PENDING),
        "sha256": _sha256(PENDING),
        "rows": len(values),
        "by_status": dict(sorted(status.items())),
        "by_task_status": dict(sorted(task_status.items())),
        "by_source": dict(sorted(sources.items())),
        "queued_unknown_count": len(queued_unknown),
        "queued_unknown_sample": queued_unknown[:10],
        "replay_policy": "按 source + revision/hash + processing_mode + provider 幂等键逐项 dry-run；不按时间批量重试",
    }


def _checkpoint_snapshot() -> Dict[str, Any]:
    if not CHECKPOINT.exists():
        return {"exists": False}
    data = json.loads(CHECKPOINT.read_text(encoding="utf-8"))
    sources = data.get("sources", {}) if isinstance(data, dict) else {}
    return {
        "exists": True,
        "path": str(CHECKPOINT),
        "sha256": _sha256(CHECKPOINT),
        "status": data.get("status"),
        "mode": data.get("mode"),
        "run_id": data.get("run_id"),
        "source_count": len(sources),
        "source_statuses": {key: value.get("status") for key, value in sources.items() if isinstance(value, dict)},
        "action": "保留原文件作为证据；不把历史 running/test fixture checkpoint 当成可恢复生产任务",
    }


def _health_snapshot() -> Dict[str, Any]:
    if not HEALTH.exists():
        return {"exists": False}
    data = json.loads(HEALTH.read_text(encoding="utf-8"))
    checks = data.get("checks", {}) if isinstance(data, dict) else {}
    return {
        "exists": True,
        "run_at": data.get("run_at"),
        "check_count": len(checks),
        "failed_checks": [key for key, value in checks.items() if isinstance(value, dict) and not value.get("passed", True)],
        "freeze_relevant": {
            "automation": checks.get("Codex automation 状态", {}).get("data", {}).get("automations", []),
            "launchd": checks.get("launchd 作业状态", {}).get("data", {}).get("jobs", []),
        },
    }


def build_dry_run() -> Dict[str, Any]:
    plists = [_plist_snapshot(label) for label in LABELS if label != "com.zhujie14.openviking-server"]
    unpinned = [item["label"] for item in plists if item.get("interpreter_unpinned")]
    not_loaded = [item["label"] for item in plists if item.get("launchctl", {}).get("status") == "not_loaded"]
    return {
        "schema_version": "pm-system.s9-dry-run.v1",
        "phase_id": "S9-preflight",
        "read_only": True,
        "python312": PYTHON312,
        "python312_exists": Path(PYTHON312).exists(),
        "launchagents": plists,
        "ledger": _ledger_snapshot(),
        "pending_uploads": _pending_snapshot(),
        "hash_only_checkpoint": _checkpoint_snapshot(),
        "health": _health_snapshot(),
        "restore_order": [
            "OpenViking server/config",
            "OneAPI environment",
            "Control Plane maintenance/read-only",
            "Scheduler/admission at 0 then 2 slots",
            "system-health-check and heartbeat",
            "pm-timeline daily/weekly",
            "weekly-sync/product-intelligence/ov-memory-sync",
            "catchup dry-run",
            "Codex Automation one by one",
        ],
        "dry_run_actions": [
            "先修正并核对所有 plist 的 canonical Python 3.12 解释器、WorkingDirectory、日志和锁路径",
            "对 pending uploads 生成 source/revision/hash 幂等键清单，先处理 queued unknown，failed 单项复核",
            "hash-only running checkpoint 只保留证据，不恢复或重试 test-source",
            "自动任务恢复前保持 PM_V44_AUTOMATION_FREEZE=on、PM_V44_ADMISSION=freeze",
        ],
        "findings": {
            "unpinned_python_labels": unpinned,
            "not_loaded_labels": not_loaded,
            "pending_queued_unknown": _pending_snapshot().get("queued_unknown_count", 0),
            "known_checkpoint_status": _checkpoint_snapshot().get("status"),
        },
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def write_report(path: Path, data: Mapping[str, Any]) -> None:
    pending = data["pending_uploads"]
    findings = data["findings"]
    lines = [
        "# V4.4 S9 环境恢复与错过任务只读 dry-run",
        "",
        "> phase_id：`S9-preflight`",
        "> 运行边界：只读；不加载任务、不修改 plist、不调用 OneAPI/OpenViking",
        "> 当前判定：**PASS（前置清单生成）**",
        "",
        "## 1. 结论",
        "",
        "S8 已收口通过，本次生成 S9 环境恢复顺序、LaunchAgent 配置快照、ledger/pending/checkpoint 现状和冻结期补跑清单。该 dry-run 只证明清单可生成，不等于已恢复环境或已补跑任务。",
        "",
        "## 2. 当前发现",
        "",
        f"- LaunchAgent 未加载：`{len(findings['not_loaded_labels'])}` 个（冻结期预期）。",
        f"- 仍使用未固定 Python 解释器的 plist：`{len(findings['unpinned_python_labels'])}` 个；恢复前必须统一到 `{data['python312']}`。",
        f"- pending uploads：`{pending.get('rows', 0)}` 条，其中 queued/unknown：`{findings.get('pending_queued_unknown', 0)}`；不得按时间批量重试。",
        f"- hash-only checkpoint：状态 `{data['hash_only_checkpoint'].get('status')}`，按证据保留，不视为可恢复生产任务。",
        "",
        "## 3. pending source 分布",
        "",
        "| source | 数量 |",
        "|---|---:|",
    ]
    lines.extend(f"| `{key}` | {value} |" for key, value in sorted(pending.get("by_source", {}).items()))
    lines.extend(
        [
            "",
            "## 4. 恢复顺序",
            "",
        ]
    )
    lines.extend(f"{index}. {step}" for index, step in enumerate(data["restore_order"], 1))
    lines.extend(
        [
            "",
            "## 5. 门禁",
            "",
            "| 检查 | 结果 |",
            "|---|---|",
            f"| Python 3.12 存在 | {'PASS' if data['python312_exists'] else 'FAIL'} |",
            "| 生产状态写入 | PASS（0） |",
            "| 外部 provider 调用 | PASS（0） |",
            "| 补跑使用幂等键 | PASS（source/revision/hash/mode/provider） |",
            "| 自动恢复已执行 | HOLD（必须完成逐项恢复验收） |",
            "",
            "## 6. 下一步",
            "",
            "先在 S9.1 修正并核对 plist 的解释器和路径，再按恢复顺序逐项加载并生成独立报告；环境恢复全部通过后，按本清单执行 S9.2 补跑，最后才恢复正式 schedule。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    data = build_dry_run()
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, data)
    print(json.dumps({"phase_id": data["phase_id"], "status": "PASS", "manifest": str(args.manifest), "report": str(args.report)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
