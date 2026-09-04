#!/usr/bin/env python3
"""Render a self-contained Chinese HTML view from a PM Loop snapshot."""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_DIR = PROJECT_ROOT / "output" / "pm-loop-control-plane"
DEFAULT_OUTPUT = DEFAULT_SNAPSHOT_DIR / "live.html"


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def status_label(value: Any) -> str:
    labels = {
        "running": "运行中",
        "not running": "未运行",
        "loaded": "已加载",
        "not_loaded": "未加载",
        "healthy": "正常",
        "probe_inconclusive": "探测不确定",
        "unknown": "未知",
    }
    return labels.get(str(value), str(value or "未知"))


def status_class(value: Any) -> str:
    value = str(value or "unknown")
    if value == "healthy" or value == "running" or value == "loaded":
        return "good"
    if value in {"probe_inconclusive", "not running", "unknown"}:
        return "warn"
    return "quiet"


EVENT_TYPE_LABELS = {
    "adr": "架构决策",
    "assessment": "需求评估",
    "concept-refresh": "概念刷新",
    "strategy": "战略决策",
    "follow-up": "跟进事项",
}


def localize_schedule(value: Any) -> str:
    """Translate launchd's compact schedule notation for the UI only."""
    text = str(value or "按事件加载")
    weekday_names = {"0": "周日", "1": "周一", "2": "周二", "3": "周三", "4": "周四", "5": "周五", "6": "周六"}
    text = re.sub(r"weekday=(\d)", lambda match: weekday_names.get(match.group(1), match.group(0)), text)
    return "日历触发" if text == "calendar" else text


def latest_snapshot(snapshot_dir: Path, explicit: Optional[Path]) -> Path:
    if explicit:
        return explicit.expanduser().resolve()
    candidates = sorted(snapshot_dir.glob("snapshot-*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No snapshot JSON found under {snapshot_dir}")
    return candidates[0]


def render_jobs(jobs: List[Dict[str, Any]]) -> str:
    rows = []
    for job in jobs:
        launchctl = job.get("launchctl") or {}
        state = launchctl.get("state", "unknown")
        program = job.get("program") or []
        command = " ".join(program[:3])
        if len(command) > 86:
            command = command[:83] + "..."
        schedule = localize_schedule(job.get("schedule") or (f"每 {job['interval_seconds']} 秒" if job.get("interval_seconds") else "按事件加载"))
        rows.append(
            "<tr data-search=\"{search}\"><td><strong>{label}</strong><small>{plist}</small></td>"
            "<td>{schedule}</td><td><span class=\"badge {klass}\">{state}</span></td>"
            "<td><code>{command}</code></td></tr>".format(
                search=esc(" ".join([str(job.get("label", "")), str(schedule), command])),
                label=esc(job.get("label")),
                plist=esc(Path(str(job.get("plist", ""))).name),
                schedule=esc(schedule),
                klass=status_class(state),
                state=esc(status_label(state)),
                command=esc(command),
            )
        )
    return "".join(rows) or '<tr><td colspan="4" class="empty">没有读取到 LaunchAgent</td></tr>'


def render_skills(skills: List[Dict[str, Any]]) -> str:
    rows = []
    for skill in skills[:14]:
        scripts = "含脚本" if skill.get("has_scripts") else "纯说明"
        rows.append(
            "<li><div><strong>{name}</strong><small>{description}</small></div>"
            "<span class=\"skill-meta\">{scripts}<br>{modified}</span></li>".format(
                name=esc(skill.get("name")),
                description=esc(skill.get("description") or "没有 front matter 描述"),
                scripts=scripts,
                modified=esc(skill.get("modified_at") or "更新时间未知"),
            )
        )
    return "".join(rows) or '<li class="empty">没有读取到 Skill</li>'


def render_hits(hits: List[Dict[str, Any]]) -> str:
    rows = []
    for hit in hits[:8]:
        abstract = str(hit.get("abstract") or "没有摘要")
        if len(abstract) > 180:
            abstract = abstract[:177] + "..."
        rows.append(
            "<li><div class=\"hit-score\">{score}</div><div><strong>{uri}</strong><small>{abstract}</small></div></li>".format(
                score=esc(f"{float(hit.get('score') or 0):.2f}"),
                uri=esc(hit.get("uri")),
                abstract=esc(abstract),
            )
        )
    return "".join(rows) or '<li class="empty">没有命中</li>'


def render_timeline(events: List[Dict[str, Any]]) -> str:
    rows = []
    for event in reversed(events):
        conclusion = str(event.get("conclusion") or event.get("topic") or "没有结论")
        if len(conclusion) > 130:
            conclusion = conclusion[:127] + "..."
        topic = str(event.get("topic") or event.get("customer") or "未标注主题")
        if re.search(r"deepseek|harness|\bdsh\b", f"{topic} {conclusion}", flags=re.IGNORECASE):
            topic = f"历史 · {topic}"
        rows.append(
            "<li><span class=\"timeline-dot\"></span><div><strong>{type}</strong><small>{ts} · {topic}</small><p>{conclusion}</p></div></li>".format(
                type=esc(EVENT_TYPE_LABELS.get(str(event.get("type") or ""), event.get("type") or "事件")),
                ts=esc(event.get("ts") or "时间未知"),
                topic=esc(topic),
                conclusion=esc(conclusion),
            )
        )
    return "".join(rows) or '<li class="empty">没有最近时间轴事件</li>'


def render(snapshot: Dict[str, Any], snapshot_path: Path) -> str:
    summary = snapshot.get("summary") or {}
    sources = snapshot.get("sources") or {}
    jobs = (sources.get("launchd") or {}).get("jobs") or []
    skills = (sources.get("skills") or {}).get("skills") or []
    ov = sources.get("openviking") or {}
    timeline = sources.get("pm_timeline") or {}
    ov_status = ov.get("status", "unknown")
    runner_copy = {
        "eyebrow": "CODEX 运行器 / 本地来源适配器",
        "title": "PM 工作闭环 · Codex 运行器实时快照",
        "sub": "展示 Codex runner 消费本地快照的形态；本次数据由来源适配器采集",
        "label": "Codex 运行器",
        "detail": "Codex runner 按次运行 · 适配器命令行已验证",
        "model_label": "Codex 运行入口已就绪",
        "model_class": "good",
    }
    return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>PM 工作闭环 · 本机真实快照</title>
  <style>
    :root {{ --page:#edf1f2; --surface:#fff; --soft:#f7f9fa; --ink:#14232c; --muted:#687983; --line:#d9e2e6; --nav:#17242b; --blue:#2366d1; --blue-soft:#eaf1ff; --green:#168765; --green-soft:#e5f5ee; --amber:#a76a10; --amber-soft:#fff3dc; --shadow:0 3px 14px rgba(29,49,61,.06); --radius:8px; font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; color:var(--ink); }}
    * {{ box-sizing:border-box; }} body {{ margin:0; background:var(--page); font-size:14px; }} button,input {{ font:inherit; }}
    .shell {{ min-height:100vh; display:grid; grid-template-columns:224px minmax(0,1fr); }}
    .sidebar {{ display:flex; flex-direction:column; min-height:100vh; padding:24px 16px; color:#edf5f5; background:var(--nav); }}
    .brand {{ display:flex; gap:10px; align-items:center; padding:0 7px; }} .mark {{ display:grid; place-items:center; width:31px; height:31px; border-radius:7px; color:white; background:var(--blue); font-weight:600; }} .brand strong {{ font-size:15px; font-weight:500; }} .brand small {{ display:block; margin-top:3px; color:#9aabb3; font-size:11px; }}
    .side-title {{ margin:34px 10px 8px; color:#71848d; font-size:10px; letter-spacing:.12em; text-transform:uppercase; }} .side-list {{ display:grid; gap:5px; margin:0; padding:0; list-style:none; }} .side-list li {{ display:flex; justify-content:space-between; gap:8px; padding:10px; border-radius:6px; color:#c6d1d6; }} .side-list li.active {{ color:#fff; background:rgba(35,102,209,.34); }} .side-list span:last-child {{ color:#91a2aa; font-size:11px; }} .side-footer {{ margin-top:auto; padding:13px; border:1px solid rgba(255,255,255,.12); border-radius:7px; color:#a7b6bb; font-size:11px; }} .side-footer strong {{ display:block; margin-top:7px; color:#e5edef; font-size:12px; font-weight:500; }}
    .main {{ min-width:0; }} .topbar {{ display:flex; justify-content:space-between; align-items:center; min-height:68px; padding:0 34px; border-bottom:1px solid var(--line); background:rgba(255,255,255,.72); }} .crumb {{ color:var(--muted); font-size:12px; }} .crumb strong {{ color:var(--ink); font-weight:500; }} .top-actions {{ display:flex; gap:8px; align-items:center; }} .top-actions a {{ color:var(--blue); font-size:12px; text-decoration:none; }} .avatar {{ display:grid; place-items:center; width:31px; height:31px; margin-left:9px; border-radius:50%; color:#fff; background:#465b66; font-size:11px; }}
    .content {{ width:min(1440px,100%); margin:auto; padding:32px 34px 45px; }} .heading {{ display:flex; justify-content:space-between; align-items:flex-end; gap:20px; margin-bottom:24px; }} .eyebrow {{ margin-bottom:8px; color:var(--blue); font-size:10px; font-weight:500; letter-spacing:.13em; }} h1,h2,h3,p {{ margin:0; }} h1 {{ font-size:27px; font-weight:500; }} .sub {{ margin-top:8px; color:var(--muted); font-size:12px; }} .snapshot-badge {{ padding:9px 12px; border:1px solid var(--line); border-radius:6px; color:var(--muted); background:var(--surface); font-size:11px; text-align:right; }} .snapshot-badge strong {{ display:block; margin-top:4px; color:var(--ink); font-weight:500; }}
    .metrics {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); margin-bottom:24px; border:1px solid var(--line); border-radius:var(--radius); background:var(--surface); box-shadow:var(--shadow); }} .metric {{ min-height:82px; padding:16px 18px; border-right:1px solid var(--line); }} .metric:last-child {{ border-right:0; }} .metric-label {{ color:var(--muted); font-size:11px; }} .metric-value {{ margin-top:8px; font-size:23px; font-weight:500; }} .metric-note {{ margin-top:5px; color:var(--muted); font-size:10px; }}
    .grid {{ display:grid; grid-template-columns:minmax(0,1.12fr) minmax(310px,.88fr); gap:20px; align-items:start; }} .panel {{ border:1px solid var(--line); border-radius:var(--radius); background:var(--surface); box-shadow:var(--shadow); }} .panel + .panel {{ margin-top:20px; }} .panel-head {{ display:flex; justify-content:space-between; align-items:flex-end; gap:12px; padding:17px 18px 13px; border-bottom:1px solid var(--line); }} .panel-title {{ font-size:14px; font-weight:500; }} .panel-meta {{ margin-top:4px; color:var(--muted); font-size:10px; }} .panel-body {{ padding:15px 18px 18px; }}
    .toolbar {{ display:flex; justify-content:space-between; gap:10px; align-items:center; margin-bottom:11px; }} .search {{ width:210px; min-height:32px; padding:0 10px; border:1px solid var(--line); border-radius:6px; color:var(--ink); background:var(--soft); font-size:11px; }} .tiny {{ color:var(--muted); font-size:10px; }} table {{ width:100%; border-collapse:collapse; }} th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ color:var(--muted); background:var(--soft); font-size:10px; font-weight:500; }} td {{ font-size:11px; }} tbody tr:last-child td {{ border-bottom:0; }} td strong {{ display:block; font-weight:500; }} td small {{ display:block; margin-top:3px; color:var(--muted); font-size:10px; }} code {{ color:#415560; font-size:10px; word-break:break-word; }} .badge {{ display:inline-flex; padding:4px 7px; border-radius:4px; color:var(--muted); background:#eef2f3; font-size:10px; white-space:nowrap; }} .badge.good {{ color:var(--green); background:var(--green-soft); }} .badge.warn {{ color:var(--amber); background:var(--amber-soft); }}
    .skill-list,.hit-list,.timeline {{ display:grid; gap:0; margin:0; padding:0; list-style:none; }} .skill-list li {{ display:flex; justify-content:space-between; gap:12px; min-width:0; padding:10px 0; border-bottom:1px solid var(--line); }} .skill-list li:last-child,.hit-list li:last-child,.timeline li:last-child {{ border-bottom:0; }} .skill-list li > div,.hit-list li > div:last-child {{ min-width:0; }} .skill-list strong,.hit-list strong {{ display:block; font-size:11px; font-weight:500; }} .skill-list small,.hit-list small {{ display:block; max-width:100%; margin-top:3px; overflow:hidden; color:var(--muted); font-size:10px; line-height:1.45; text-overflow:ellipsis; white-space:nowrap; }} .skill-meta {{ flex:0 0 auto; color:var(--muted); font-size:10px; line-height:1.45; text-align:right; white-space:nowrap; }} .hit-list li {{ display:grid; grid-template-columns:40px minmax(0,1fr); gap:10px; padding:10px 0; border-bottom:1px solid var(--line); }} .hit-score {{ color:var(--blue); font-variant-numeric:tabular-nums; font-size:12px; font-weight:500; }}
    .timeline li {{ display:grid; grid-template-columns:12px minmax(0,1fr); gap:10px; padding:10px 0; border-bottom:1px solid var(--line); }} .timeline-dot {{ width:7px; height:7px; margin-top:4px; border-radius:50%; background:var(--blue); }} .timeline strong {{ font-size:11px; font-weight:500; }} .timeline small {{ display:block; margin-top:3px; color:var(--muted); font-size:10px; }} .timeline p {{ margin-top:5px; color:#3e5059; font-size:11px; line-height:1.5; }} .empty {{ padding:18px 0; color:var(--muted); text-align:center; }}
    .harness {{ display:grid; gap:10px; }} .harness-row {{ display:flex; justify-content:space-between; align-items:flex-start; gap:14px; padding:10px 0; border-bottom:1px solid var(--line); }} .harness-row:last-child {{ border-bottom:0; }} .harness-row span {{ color:var(--muted); font-size:11px; }} .harness-row strong {{ max-width:70%; color:var(--ink); font-size:11px; font-weight:500; text-align:right; word-break:break-word; }} .evidence {{ margin-top:12px; padding:10px; border-radius:6px; color:#52636b; background:var(--soft); font-size:10px; line-height:1.5; }} .evidence code {{ display:block; margin-top:4px; }}
    @media (max-width:1080px) {{ .shell {{ grid-template-columns:200px minmax(0,1fr); }} .topbar,.content {{ padding-right:24px; padding-left:24px; }} .grid {{ grid-template-columns:minmax(0,1fr); }} }}
    @media (max-width:760px) {{ .shell {{ display:block; }} .sidebar {{ min-height:auto; padding:14px 15px; }} .side-title,.side-list,.side-footer {{ display:none; }} .topbar {{ min-height:56px; padding:0 16px; }} .content {{ padding:24px 16px 34px; }} .heading {{ display:block; }} .snapshot-badge {{ display:inline-block; margin-top:15px; text-align:left; }} .metrics {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .metric:nth-child(2) {{ border-right:0; }} .metric:nth-child(-n+2) {{ border-bottom:1px solid var(--line); }} .panel-body {{ padding-right:13px; padding-left:13px; }} .search {{ width:160px; }} table {{ min-width:680px; }} .panel-body.table-wrap {{ overflow-x:auto; }} }}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar"><div class="brand"><div class="mark">P</div><div><strong>PM 工作闭环</strong><small>本机真实数据快照</small></div></div><div class="side-title">数据源</div><ul class="side-list"><li class="active"><span>本机总览</span><span>{jobs_count}</span></li><li><span>定时任务</span><span>{jobs_count}</span></li><li><span>Skill 目录</span><span>{skills_count}</span></li><li><span>OpenViking</span><span>{ov_hits}</span></li><li><span>时间轴</span><span>{timeline_count}</span></li></ul><div class="side-footer">当前运行方式<strong>{runner_label}</strong><small>{runner_detail}</small></div></aside>
    <main class="main">
      <header class="topbar"><div class="crumb">个人工作区　/　<strong>本机真实快照</strong></div><div class="top-actions"><a href="../../docs/10-Demo与工具/pm-loop-control-plane-demo.html">打开交互演示 ↗</a><div class="avatar">ZJ</div></div></header>
      <div class="content">
        <section class="heading"><div><div class="eyebrow">{runner_eyebrow}</div><h1>{runner_title}</h1><p class="sub">{runner_sub}</p></div><div class="snapshot-badge">快照编号<strong>{snapshot_id}</strong><span>{collected_at}</span></div></section>
        <section class="metrics"><div class="metric"><div class="metric-label">可读定时任务</div><div class="metric-value">{jobs_count}</div><div class="metric-note">LaunchAgent plist + launchctl 状态</div></div><div class="metric"><div class="metric-label">本地 Skill</div><div class="metric-value">{skills_count}</div><div class="metric-note">含描述、更新时间和脚本标记</div></div><div class="metric"><div class="metric-label">OpenViking</div><div class="metric-value"><span class="badge {ov_class}">{ov_label}</span></div><div class="metric-note">{ov_url}</div></div><div class="metric"><div class="metric-label">最近时间轴事件</div><div class="metric-value">{timeline_count}</div><div class="metric-note">来自最新季度 JSONL</div></div></section>
        <div class="grid">
          <div><section class="panel"><div class="panel-head"><div><h2 class="panel-title">定时任务</h2><div class="panel-meta">真实 launchd 任务 · 只读</div></div><span class="badge good">已读取</span></div><div class="panel-body table-wrap"><div class="toolbar"><span class="tiny">{jobs_count} 个 `com.zhujie14.*` 任务</span><input class="search" id="job-search" type="search" placeholder="筛选任务名称或命令"></div><table><thead><tr><th>任务</th><th>调度</th><th>状态</th><th>入口</th></tr></thead><tbody id="job-rows">{job_rows}</tbody></table></div></section>
          <section class="panel"><div class="panel-head"><div><h2 class="panel-title">最近时间轴</h2><div class="panel-meta">pm-timeline 最新事件</div></div><span class="tiny">{timeline_file}</span></div><div class="panel-body"><ul class="timeline">{timeline_rows}</ul></div></section></div>
          <div><section class="panel"><div class="panel-head"><div><h2 class="panel-title">Skill 目录</h2><div class="panel-meta">本地 `~/.codex/skills` · 展示前 14 项</div></div><span class="badge good">{skills_count} 个</span></div><div class="panel-body"><ul class="skill-list">{skill_rows}</ul></div></section>
          <section class="panel"><div class="panel-head"><div><h2 class="panel-title">OpenViking</h2><div class="panel-meta">本地私有知识服务 · Skill 命名空间检索</div></div><span class="badge {ov_class}">{ov_label}</span></div><div class="panel-body"><div class="harness-row"><span>服务地址</span><strong>{ov_url}</strong></div><div class="harness-row"><span>检索命中</span><strong>{ov_hits} 条</strong></div><ul class="hit-list">{hit_rows}</ul></div></section>
          <section class="panel"><div class="panel-head"><div><h2 class="panel-title">运行时接入状态</h2><div class="panel-meta">{runner_label} · Codex runner + `pm_loop_snapshot` 工具</div></div><span class="badge {model_class}">{model_label}</span></div><div class="panel-body"><div class="harness"><div class="harness-row"><span>当前方式</span><strong>{runner_detail}</strong></div><div class="harness-row"><span>运行时</span><strong>Codex canonical runtime</strong></div><div class="harness-row"><span>事实入口</span><strong>scripts/pm_loop_control_plane.py</strong></div><div class="harness-row"><span>状态持久化</span><strong>run events JSONL + snapshot</strong></div></div><div class="evidence">当前页面只展示 Codex runner 方案：本地来源适配器读取四类真实来源，快照可重放，外部写入仍由人工闸门控制。<code>{snapshot_path}</code></div></div></section></div>
        </div>
      </div>
    </main>
  </div>
  <script>
    const input = document.getElementById('job-search');
    input.addEventListener('input', () => {{ const query = input.value.trim().toLowerCase(); document.querySelectorAll('#job-rows tr[data-search]').forEach(row => {{ row.hidden = query && !row.dataset.search.toLowerCase().includes(query); }}); }});
  </script>
</body>
</html>""".format(
        runner_eyebrow=esc(runner_copy["eyebrow"]),
        runner_title=esc(runner_copy["title"]),
        runner_sub=esc(runner_copy["sub"]),
        runner_label=esc(runner_copy["label"]),
        runner_detail=esc(runner_copy["detail"]),
        model_label=esc(runner_copy["model_label"]),
        model_class=runner_copy["model_class"],
        jobs_count=esc(summary.get("launchd_jobs", 0)),
        skills_count=esc(summary.get("skills", 0)),
        ov_hits=esc(summary.get("openviking_skill_hits", 0)),
        timeline_count=esc(summary.get("timeline_events", 0)),
        snapshot_id=esc(snapshot.get("snapshot_id")),
        collected_at=esc(snapshot.get("collected_at")),
        ov_class=status_class(ov_status),
        ov_label=esc(status_label(ov_status)),
        ov_url=esc(ov.get("base_url", "127.0.0.1:1933")),
        timeline_file=esc(Path(str(timeline.get("file") or "")).name or "未找到文件"),
        job_rows=render_jobs(jobs),
        skill_rows=render_skills(skills),
        hit_rows=render_hits(ov.get("skill_search") or []),
        timeline_rows=render_timeline(timeline.get("events") or []),
        snapshot_path=esc(snapshot_path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a Chinese PM Loop live snapshot HTML")
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--snapshot-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    snapshot_path = latest_snapshot(args.snapshot_dir.expanduser().resolve(), args.snapshot)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    output = args.out.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render(snapshot, snapshot_path), encoding="utf-8")
    print(json.dumps({"status": "ok", "runtime": "codex", "html": str(output), "snapshot": str(snapshot_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
