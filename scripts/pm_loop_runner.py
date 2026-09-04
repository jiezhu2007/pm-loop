#!/usr/bin/env python3
"""Run one Codex-only PM Loop attempt and persist a replayable result."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from pm_loop_runtime import RunStore, now_iso
from pm_loop_analysis import execute_analysis


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ADAPTER = PROJECT_ROOT / "scripts" / "pm_loop_control_plane.py"


def parse_last_json(output: str) -> Dict[str, Any]:
    # The source adapter emits pretty-printed JSON. Prefer parsing the complete
    # payload, then fall back to the last JSON line for compatible adapters that
    # also write diagnostic lines to stdout.
    try:
        value = json.loads(output.strip())
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    for line in reversed(output.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ValueError("adapter did not return a JSON result")


def check_cancel(store: RunStore, run_id: str) -> bool:
    paths = store.paths(run_id)
    if not paths.cancel_marker.is_file():
        return False
    state = store.state(run_id)
    if state.get("status") not in {"cancelled", "completed", "failed", "rejected"}:
        store.append(run_id, "run/cancelled", {"reason": "cancel_marker"})
    return True


def validate_snapshot(snapshot_path: Path) -> Dict[str, Any]:
    with snapshot_path.open("r", encoding="utf-8") as stream:
        snapshot = json.load(stream)
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != "pm-loop.snapshot.v1":
        raise ValueError("snapshot schema must be pm-loop.snapshot.v1")
    for key in ("summary", "sources", "snapshot_id"):
        if key not in snapshot:
            raise ValueError(f"snapshot missing {key}")
    return snapshot


def write_draft(store: RunStore, run_id: str, snapshot: Dict[str, Any]) -> Path:
    request = store.request(run_id)
    summary = snapshot.get("summary") or {}
    sources = snapshot.get("sources") or {}
    lines = [
        f"# {request.get('loop_id', 'PM Loop')} 运行草稿",
        "",
        f"- run_id：`{run_id}`",
        f"- snapshot_id：`{snapshot.get('snapshot_id')}`",
        f"- 采集时间：`{snapshot.get('collected_at')}`",
        f"- 权限模式：`{request.get('permission_mode')}`",
        "",
        "## 本次快照",
        "",
        f"- LaunchAgent：{summary.get('launchd_jobs', 0)} 个",
        f"- Skill：{summary.get('skills', 0)} 个",
        f"- OpenViking：`{summary.get('openviking_status', 'unknown')}`",
        f"- 时间轴事件：{summary.get('timeline_events', 0)} 条",
        "",
        "## 来源状态",
        "",
    ]
    for source_id, source in sources.items():
        if not isinstance(source, dict):
            continue
        status = source.get("status") or ("available" if source_id in {"launchd", "skills", "pm_timeline"} else "unknown")
        lines.append(f"- `{source_id}`：`{status}`")
    lines.extend(
        [
            "",
            "## 下一步",
            "",
            "这是只读/草稿运行。任何时间轴写入、文档修改、外部消息或发布动作都必须先创建人工闸门。",
        ]
    )
    path = store.paths(run_id).draft
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def collect_snapshot(
    store: RunStore,
    run_id: str,
    adapter_script: Path,
    project_root: Optional[Path],
    codex_root: Optional[Path],
) -> Dict[str, Any]:
    source_dir = store.paths(run_id).root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, str(adapter_script), "snapshot", "--out", str(source_dir)]
    if project_root:
        command.extend(["--project-root", str(project_root)])
    if codex_root:
        command.extend(["--codex-root", str(codex_root)])
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=900)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "adapter failed").strip()[-2000:]
        raise RuntimeError(detail)
    adapter_result = parse_last_json(result.stdout)
    snapshot_path = Path(str(adapter_result.get("snapshot_path") or ""))
    if not snapshot_path.is_file():
        raise FileNotFoundError(f"adapter snapshot missing: {snapshot_path}")
    destination = store.paths(run_id).snapshot
    shutil.copy2(snapshot_path, destination)
    return validate_snapshot(destination)


def run_once(
    store: RunStore,
    run_id: str,
    snapshot_path: Optional[Path] = None,
    adapter_script: Path = ADAPTER,
    project_root: Optional[Path] = None,
    codex_root: Optional[Path] = None,
    analysis_mode: str = "snapshot-only",
    analysis_invoker: Optional[Any] = None,
) -> Dict[str, Any]:
    request = store.request(run_id)
    if request.get("runtime", {}).get("kind") != "codex":
        raise ValueError("only codex runtime is supported")
    if store.state(run_id).get("status") in {"completed", "failed", "cancelled", "rejected"}:
        return store.state(run_id)
    if check_cancel(store, run_id):
        return store.state(run_id)
    store.append(run_id, "run/started", {"runtime": "codex", "at": now_iso()})
    try:
        if check_cancel(store, run_id):
            return store.state(run_id)
        store.append(run_id, "source/started", {"source": "local-pm-loop-snapshot"})
        if snapshot_path:
            destination = store.paths(run_id).snapshot
            shutil.copy2(snapshot_path.expanduser().resolve(), destination)
            snapshot = validate_snapshot(destination)
        else:
            snapshot = collect_snapshot(store, run_id, adapter_script, project_root, codex_root)
        summary = snapshot.get("summary") or {}
        store.append(run_id, "source/completed", {"source": "local-pm-loop-snapshot", "snapshot_id": snapshot.get("snapshot_id"), "summary": summary})
        if check_cancel(store, run_id):
            return store.state(run_id)
        if analysis_mode == "codex":
            store.append(run_id, "analysis/started", {"executor": "codex", "snapshot_id": snapshot.get("snapshot_id")})
            analysis, decision, draft_path = execute_analysis(
                store,
                run_id,
                snapshot,
                (codex_root or Path.home() / ".codex").expanduser(),
                invoker=analysis_invoker,
            )
            store.append(
                run_id,
                "analysis/completed",
                {
                    "answerability": analysis.get("answerability"),
                    "confidence": analysis.get("confidence"),
                    "findings": len(analysis.get("findings") or []),
                    "proposed_actions": len(decision.get("proposed_actions") or []),
                },
            )
            store.append(run_id, "assistant/draft", {"path": str(draft_path), "snapshot_id": snapshot.get("snapshot_id"), "mode": request.get("permission_mode")})
            store.append(run_id, "verification/completed", {"checks": ["snapshot_schema", "analysis_schema", "evidence_refs", "decision_schema", "draft_written"], "ok": True})
            if (decision.get("gate") or {}).get("required"):
                store.append(
                    run_id,
                    "gate/requested",
                    {
                        "gate_id": (decision.get("gate") or {}).get("gate_id"),
                        "snapshot_id": snapshot.get("snapshot_id"),
                        "actions": [
                            {"action_id": item.get("id"), "action_hash": item.get("action_hash")}
                            for item in decision.get("proposed_actions") or []
                            if item.get("requires_gate")
                        ],
                    },
                )
            else:
                store.append(run_id, "run/completed", {"draft_path": str(draft_path), "snapshot_id": snapshot.get("snapshot_id"), "analysis": True})
        else:
            draft_path = write_draft(store, run_id, snapshot)
            store.append(run_id, "assistant/draft", {"path": str(draft_path), "snapshot_id": snapshot.get("snapshot_id"), "mode": request.get("permission_mode")})
            store.append(run_id, "verification/completed", {"checks": ["snapshot_schema", "source_summary", "draft_written"], "ok": True})
            store.append(run_id, "run/completed", {"draft_path": str(draft_path), "snapshot_id": snapshot.get("snapshot_id"), "analysis": False})
    except Exception as exc:  # persist failure before returning to the supervisor
        store.append(run_id, "run/failed", {"error": f"{type(exc).__name__}: {exc}"})
    return store.state(run_id)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one Codex-only PM Loop")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create", help="create a queued run")
    create.add_argument("--loop-id", default="daily-radar")
    create.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    create.add_argument("--permission-mode", choices=["report", "draft", "approved_action"], default="report")
    create.add_argument("--record", action="store_true")
    create.add_argument("--json", action="store_true")
    run = sub.add_parser("run", help="run an existing run")
    run.add_argument("--run-id", required=True)
    run.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    run.add_argument("--snapshot", type=Path)
    run.add_argument("--adapter", type=Path, default=ADAPTER)
    run.add_argument("--project-root", type=Path)
    run.add_argument("--codex-root", type=Path)
    run.add_argument("--analysis-mode", choices=["codex", "snapshot-only"], default="codex")
    cancel = sub.add_parser("cancel", help="cancel a queued or running run")
    cancel.add_argument("--run-id", required=True)
    cancel.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    for name in ("status", "replay"):
        command = sub.add_parser(name)
        command.add_argument("--run-id", required=True)
        command.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    listing = sub.add_parser("list")
    listing.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    store = RunStore(args.state_dir)
    if args.command == "create":
        value = store.create({"loop_id": args.loop_id, "permission_mode": args.permission_mode, "record": args.record})
        print(json.dumps(value, ensure_ascii=False, indent=2))
        return 0
    if args.command == "list":
        print(json.dumps(store.list_states(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "status":
        print(json.dumps(store.state(args.run_id), ensure_ascii=False, indent=2))
        return 0
    if args.command == "cancel":
        paths = store.paths(args.run_id)
        store.request(args.run_id)
        paths.root.mkdir(parents=True, exist_ok=True)
        paths.cancel_marker.write_text("cancel requested\n", encoding="utf-8")
        if store.state(args.run_id).get("status") not in {"completed", "failed", "cancelled", "rejected"}:
            store.append(args.run_id, "run/cancelled", {"reason": "cli_cancel"}, actor="codex-cli")
        print(json.dumps(store.state(args.run_id), ensure_ascii=False, indent=2))
        return 0
    if args.command == "replay":
        print(json.dumps({"state": store.state(args.run_id), "events": store.events_for(args.run_id), "read_only": True}, ensure_ascii=False, indent=2))
        return 0
    state = run_once(store, args.run_id, args.snapshot, args.adapter, args.project_root, args.codex_root, args.analysis_mode)
    print(json.dumps(state, ensure_ascii=False, indent=2))
    return 0 if state.get("status") in {"completed", "cancelled"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
