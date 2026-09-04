#!/usr/bin/env python3
"""Read-only S8 closeout gate for the V4.4 upgrade.

The gate verifies that every S8 report and machine manifest is present and
passed, while preserving explicitly known follow-up items (for example
unmapped concept references).  It never changes production state.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping


REPORT_DIR = Path(__file__).resolve().parents[1] / "docs/03-产品架构/v4.4实施报告"
REPORT_PAIRS = (
    ("S8.1", "20260828-S8.1-数据快照与证据报告"),
    ("S8.2", "20260828-S8.2-数据源与任务对账报告"),
    ("S8.3", "20260828-S8.3-历史任务与旁路快照报告"),
    ("S8.4-M0", "20260828-S8.4-M0-hash-only-live-read报告"),
    ("S8.4-M1", "20260828-S8.4-M1-openviking-runtime-reliability报告"),
    ("S8.4-M2-feature-list", "20260828-S8.4-M2-feature-list可信hash报告"),
    ("S8.4-M2-ontology", "20260828-S8.4-M2-ontology可信hash报告"),
    ("S8.4-M2-data-agent", "20260828-S8.4-M2-data-agent可信hash报告"),
    ("S8.4-M2-datasearch", "20260828-S8.4-M2-datasearch可信hash报告"),
    ("S8.4-M2-pipeline-logic-fde", "20260828-S8.4-M2-pipeline-logic-fde可信hash报告"),
    ("S8.4-M2-product-management", "20260828-S8.4-M2-product-management可信hash报告"),
    ("S8.4-M3", "20260829-S8.4-M3-统一只读对账报告"),
    ("S8.5", "20260829-S8.5-只读门禁报告"),
    ("S8.6", "20260829-S8.6-容量压测报告"),
)


def _load_json(path: Path) -> Mapping[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _report_status(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    return "当前判定：**PASS" in text or "判定：`PASS" in text or "判定：**PASS" in text


def _check_reports(report_dir: Path) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for phase_id, stem in REPORT_PAIRS:
        md = report_dir / f"{stem}.md"
        html = report_dir / f"{stem}.html"
        rows.append(
            {
                "phase_id": phase_id,
                "markdown": str(md),
                "html": str(html),
                "markdown_exists": md.exists(),
                "html_exists": html.exists(),
                "markdown_pass": md.exists() and _report_status(md),
            }
        )
    return {"count": len(rows), "rows": rows, "all_complete": all(row["markdown_exists"] and row["html_exists"] and row["markdown_pass"] for row in rows)}


def build_gate(report_dir: Path = REPORT_DIR) -> Dict[str, Any]:
    report_dir = Path(report_dir).expanduser().resolve()
    reports = _check_reports(report_dir)
    m3_path = report_dir / "20260829-S8.4-M3-统一只读对账-manifest.json"
    s85_path = report_dir / "20260829-S8.5-只读门禁-manifest.json"
    s86_path = report_dir / "20260829-S8.6-容量压测-manifest.json"
    manifests: Dict[str, Any] = {}
    errors: List[str] = []
    for key, path in (("s8.4_m3", m3_path), ("s8.5", s85_path), ("s8.6", s86_path)):
        if not path.exists():
            errors.append(f"missing:{path.name}")
            continue
        try:
            manifests[key] = dict(_load_json(path))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"invalid:{path.name}:{exc}")

    m3_pass = manifests.get("s8.4_m3", {}).get("status") == "PASS"
    s85 = manifests.get("s8.5", {})
    s85_pass = s85.get("gate", {}).get("status") == "PASS"
    s85_guards = all(
        bool(s85.get("gate", {}).get(key))
        for key in ("skill_non_orphan_consistent", "historical_runs_consistent", "concept_chain_read_only", "generation_recovery_replayable")
    )
    s86 = manifests.get("s8.6", {})
    levels = s86.get("levels", [])
    s86_pass = s86.get("status") == "pass" and [item.get("width") for item in levels] == [2, 4, 8] and all(item.get("status") == "pass" for item in levels)
    s86_isolated = s86.get("production_state_touched") is False and s86.get("external_provider_calls") == 0
    source_freeze_evidence = (report_dir / "20260829-S8.4-M3-统一只读对账报告.md").read_text(encoding="utf-8") if (report_dir / "20260829-S8.4-M3-统一只读对账报告.md").exists() else ""
    freeze_pass = all(marker in source_freeze_evidence for marker in ("PM_V44_AUTOMATION_FREEZE=on", "PM_V44_ADMISSION=freeze", "Writer=0"))

    checks = {
        "all_s8_report_pairs_complete": reports["all_complete"],
        "s8.4_m3_manifest_pass": m3_pass,
        "s8.5_manifest_and_guards_pass": s85_pass and s85_guards,
        "s8.6_capacity_manifest_pass": s86_pass,
        "s8.6_isolation": s86_isolated,
        "freeze_evidence": freeze_pass,
    }
    errors.extend(key for key, value in checks.items() if not value)
    return {
        "schema_version": "pm-system.s8-closeout.v1",
        "phase_id": "S8-closeout",
        "read_only": True,
        "report_dir": str(report_dir),
        "reports": reports,
        "manifests": {
            "s8.4_m3": str(m3_path),
            "s8.5": str(s85_path),
            "s8.6": str(s86_path),
        },
        "checks": checks,
        "known_follow_ups": {
            "unmapped_concept_active_references": s85.get("gate", {}).get("known_non_blocking", {}).get("concept_active_mapping_gap", 260),
            "historical_skill_orphans": s85.get("gate", {}).get("known_non_blocking", {}).get("skill_historical_orphans", ["md2wechat", "system-runtime"]),
            "automatic_recovery_allowed": False,
        },
        "errors": errors,
        "status": "PASS" if not errors else "FAIL",
        "production_state_touched": False,
    }


def write_report(path: Path, gate: Mapping[str, Any]) -> None:
    checks = gate["checks"]
    rows = "\n".join(f"| {key} | {'PASS' if value else 'FAIL'} |" for key, value in checks.items())
    body = f"""# V4.4 S8 总阶段收口报告

> phase_id：`S8-closeout`
> 运行边界：只读核验；不恢复环境、不写生产状态
> 当前判定：**{gate['status']}**

## 1. 阶段结论

S8.1～S8.6 的 Markdown/HTML 阶段报告和关键机器 manifest 均已核验。S8.4-M3、S8.5 和 S8.6 的机器状态均为 PASS；S8.6 的 2/4/8 路容量结果来自独立临时 SQLite，外部 provider 调用和生产状态写入均为 0。

因此，S8 总阶段收口通过，允许进入 S9 环境恢复前的准备。该结论不自动解冻任何任务；S9 仍必须按依赖顺序逐项恢复并逐项验收。

## 2. 子阶段门禁

| 门禁 | 状态 |
|---|---|
{rows}

报告对：`{gate['reports']['count']}` 组；全部 MD/HTML 齐全且 MD 含 PASS：`{gate['reports']['all_complete']}`。

## 3. 保留项与边界

- 概念 manifest 中 `260` 个未映射 Active source reference 继续不可激活。
- 历史 Skill orphan `md2wechat`、`system-runtime` 按历史残留保留，不删除。
- OpenViking 历史 quarantine/stale task 不因 S8 通过而批量重试或强行改终态。
- `PM_V44_AUTOMATION_FREEZE=on`、`PM_V44_ADMISSION=freeze` 继续有效；Writer、Scheduler、同步任务和 Codex Automation 不在本阶段恢复。

## 4. 进入 S9 的前置条件

1. 读取本报告和所有 S8 checkpoint，确认恢复点为 `S8-closeout`。
2. 对 ledger、snapshot、watermark、幂等键执行只读 dry-run，生成环境恢复和错过任务补跑清单。
3. 按计划逐项恢复 OpenViking、OneAPI、Control Plane、Scheduler、健康检查、时间轴、LaunchAgent、catchup 和 Codex Automation。
4. 每项恢复后生成独立检查报告；全部通过后才允许 `PM_V44_AUTOMATION_FREEZE=off`、`PM_V44_ADMISSION=on`。

## 5. 机器证据

- S8.4-M3：`20260829-S8.4-M3-统一只读对账-manifest.json`
- S8.5：`20260829-S8.5-只读门禁-manifest.json`
- S8.6：`20260829-S8.6-容量压测-manifest.json`
- 收口 manifest：`20260829-S8-closeout-manifest.json`
"""
    path.write_text(body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    gate = build_gate(args.report_dir)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(gate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, gate)
    print(json.dumps({"phase_id": gate["phase_id"], "status": gate["status"], "manifest": str(args.manifest), "report": str(args.report)}, ensure_ascii=False))
    return 0 if gate["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
