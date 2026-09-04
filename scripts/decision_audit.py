#!/usr/bin/env python3
"""decision_audit.py — 决策记录兜底审计（heartbeat 调用）

机制补充：决策落盘是"会话触发"的——由对话里的 agent 主动 append/落盘，没有
后台进程自动捕捉。文件层能做的兜底是有限的：脚本看不到对话，只能扫已经写下的
痕迹。因此本脚本只做两类机械核查，只读、只报告、不写任何东西：

  1) 游离决策：memory 当天文件里出现决策措辞，却不在任何"决策区块"内
     （区块 = 标题含 决策/决定/ADR 的 ## 小节，或 `- **决策**` 条目）。
     这类是"提了一嘴但没正式记"的疑点 → 退出码 2。
  2) ADR 缺口（建议级，不改退出码）：决策区块提到产品/技术/架构线索
     （docs/adr、架构、方案选型 等），但当天 docs/adr 无对应 ADR。

真正覆盖"对话里聊到但完全没写下来"的漏网，靠 HEARTBEAT.md 让当轮 agent
回看对话+memory 做人工对账，这个脚本是它的机械辅助。

用法：
    python3 scripts/decision_audit.py            # 审计今天（北京时间）
    python3 scripts/decision_audit.py --date 2026-08-15
    python3 scripts/decision_audit.py --days 2   # 今天+回看 N-1 天

退出码：0=无游离决策，2=有游离决策需人工复核。
"""
import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
MEMORY_DIR = PROJECT / "memory"
ADR_DIR = PROJECT / "docs" / "adr"
TIMELINE_DIR = Path.home() / ".codex" / "skills" / "pm-timeline" / "state" / "timeline"
BJ = timezone(timedelta(hours=8))

# 决策类措辞：命中即视为"可能是决策"的候选行
DECISION_HINTS = [
    "决策", "决定", "选型", "选择了", "拍板", "定了", "方案选", "采用",
    "改用", "确定用", "最终用", "取舍", "否决", "废弃", "架构变更", "定方向",
]
# 明显不是决策的噪声（纯执行/查询/元信息），命中则跳过
NOISE_HINTS = ["修 bug", "修bug", "跑测试", "改脚本", "查询", "只读", "备注", "待办"]
# 决策区块的标记
SECTION_MARKERS = ["决策", "决定", "ADR"]
# 产品/技术/架构线索（用于 ADR 缺口建议）
ADR_CUES = ["docs/adr", "架构", "方案选型", "选型", "概念定义", "技术选型"]


def load_timeline_events(dates: set) -> list:
    events = []
    if not TIMELINE_DIR.exists():
        return events
    for f in sorted(TIMELINE_DIR.glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = e.get("ts", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                bj = dt.astimezone(BJ).strftime("%Y-%m-%d")
            except ValueError:
                bj = ""
            if bj in dates:
                events.append(e)
    return events


def scan_memory(dates: list):
    """返回 (stray, documented)：
    stray     = 决策措辞但落在决策区块之外的行 (d, lineno, text)
    documented= 决策区块内、命中决策措辞的行 (d, lineno, text)
    """
    stray, documented = [], []
    for d in dates:
        f = MEMORY_DIR / f"{d}.md"
        if not f.exists():
            continue
        in_section = False
        for i, raw in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                # 标题：决定它开启的是不是"决策区块"
                in_section = any(m in line for m in SECTION_MARKERS)
                continue
            if any(n in line for n in NOISE_HINTS):
                continue
            is_decision_bullet = line.startswith("- **决策**") or line.startswith("-**决策**")
            if not (any(h in line for h in DECISION_HINTS)):
                continue
            if in_section or is_decision_bullet:
                documented.append((d, i, line))
            else:
                stray.append((d, i, line))
    return stray, documented


def adr_files_touched(dates: set) -> list:
    if not ADR_DIR.exists():
        return []
    out = []
    for f in ADR_DIR.glob("ADR-*.md"):
        head = f.read_text(encoding="utf-8")[:400]
        m = re.search(r"日期[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})", head)
        if m and m.group(1) in dates:
            out.append(f.name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="审计日期 YYYY-MM-DD（默认今天，北京时间）")
    ap.add_argument("--days", type=int, default=1, help="回看天数（含当天）")
    args = ap.parse_args()

    end = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now(BJ)
    dates = [(end - timedelta(days=k)).strftime("%Y-%m-%d") for k in range(args.days)]
    dateset = set(dates)

    stray, documented = scan_memory(dates)
    events = load_timeline_events(dateset)
    decision_events = [e for e in events if e.get("type") in ("adr", "strategy")]
    adr_touched = adr_files_touched(dateset)

    print(f"决策兜底审计 · 范围 {dates[-1]}~{dates[0]}（北京时间）")
    print(f"  memory 已入区块的决策: {len(documented)}")
    print(f"  memory 游离决策措辞:   {len(stray)}")
    print(f"  pm-timeline 决策事件(adr/strategy): {len(decision_events)}")
    print(f"  docs/adr 当期新增/更新: {len(adr_touched)} {adr_touched}")

    # 建议级：决策区块提到产品/技术/架构线索，但当天没有 ADR
    adr_gap = (
        any(any(c in t for c in ADR_CUES) for _, _, t in documented)
        and not adr_touched
    )
    if adr_gap:
        print("\nℹ️ 建议：当天决策提到架构/选型等线索，但 docs/adr 无对应 ADR；"
              "若确属产品/技术决策，考虑补一份 ADR。")

    if stray:
        print("\n⚠️ 游离决策：memory 里有决策措辞但未收进任何决策区块，疑似只提了一嘴没正式记：")
        for d, ln, text in stray[:12]:
            print(f"  - {d}:{ln}  {text[:80]}")
        print("\n→ 请 agent 复核是否为真决策；是则按 default-record-all-decisions 规则补记。")
        return 2

    if not documented and not decision_events and not adr_touched:
        print("\n本期无决策痕迹，无需记录。")
        return 0

    print("\n✅ 决策均已入区块/时间轴，无游离漏记。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
