#!/usr/bin/env python3
"""One-shot executor for approved local PM Loop draft actions."""

from __future__ import annotations

import argparse
import fcntl
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from pm_loop_analysis import canonical_hash
from pm_loop_runtime import RunStore, atomic_json_write, now_iso


def _read_object(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _validate_gate(store: RunStore, run_id: str, decision: Dict[str, Any]) -> Dict[str, Any]:
    gate = decision.get("gate") if isinstance(decision.get("gate"), dict) else {}
    if not gate.get("required") or gate.get("gate_id") != run_id:
        raise ValueError("decision does not contain an executable Gate")
    expires_at = str(gate.get("expires_at") or "")
    if not expires_at:
        raise ValueError("Gate has no expiry")
    expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expiry:
        raise ValueError("Gate token expired")
    approvals = [event for event in store.events_for(run_id) if event.get("type") == "gate/approved"]
    if not approvals:
        raise ValueError("Gate has not been approved")
    approved = approvals[-1].get("data") or {}
    if approved.get("gate_token") != gate.get("token"):
        raise ValueError("approved Gate token does not match decision")
    return gate


def execute_actions(store: RunStore, run_id: str) -> Dict[str, Any]:
    paths = store.paths(run_id)
    lock_path = paths.root / ".action.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_stream:
        fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
        try:
            if store.state(run_id).get("status") == "completed":
                return store.state(run_id)
            decision_path = paths.root / "decision" / "decision.json"
            decision = _read_object(decision_path)
            gate = _validate_gate(store, run_id, decision)
            snapshot_id = decision.get("snapshot_id")
            request = store.request(run_id)
            completed_ids = {
                str((event.get("data") or {}).get("action_id"))
                for event in store.events_for(run_id)
                if event.get("type") == "action/completed"
            }
            gated = [item for item in decision.get("proposed_actions") or [] if isinstance(item, dict) and item.get("requires_gate")]
            if not gated:
                raise ValueError("Gate has no approved actions")
            for action in gated:
                action_id = str(action.get("id") or "")
                if not action_id or action_id in completed_ids:
                    continue
                expected_hash = canonical_hash({"run_id": run_id, "snapshot_id": snapshot_id, "action": {key: value for key, value in action.items() if key not in {"action_hash", "status"}}})
                if action.get("action_hash") != expected_hash:
                    raise ValueError(f"action hash mismatch: {action_id}")
                store.append(run_id, "action/started", {"action_id": action_id, "action_hash": expected_hash, "mode": "safe-draft"}, actor="action-runner")
                draft_path = paths.root / "draft" / f"action-{action_id}.md"
                draft_path.parent.mkdir(parents=True, exist_ok=True)
                draft_path.write_text(
                    "\n".join(
                        [
                            f"# {action.get('title') or action_id}",
                            "",
                            f"- run_id: `{run_id}`",
                            f"- snapshot_id: `{snapshot_id}`",
                            f"- action_hash: `{expected_hash}`",
                            "- reviewer: `zhujie14`",
                            "- effect: local draft only",
                            "",
                            "本动作不会发送消息、修改真源、发布内容或变更权限。",
                            "",
                        ]
                    ),
                    encoding="utf-8",
                )
                receipt_path = paths.root / "action" / f"{action_id}.receipt.json"
                atomic_json_write(
                    receipt_path,
                    {
                        "schema_version": "pm-loop.action-receipt.v1",
                        "run_id": run_id,
                        "action_id": action_id,
                        "action_hash": expected_hash,
                        "gate_token": gate.get("token"),
                        "completed_at": now_iso(),
                        "writes": [str(draft_path)],
                        "external_writes": [],
                    },
                )
                store.append(
                    run_id,
                    "action/completed",
                    {"action_id": action_id, "action_hash": expected_hash, "path": str(draft_path), "receipt": str(receipt_path), "writes": [str(draft_path)], "external_writes": []},
                    actor="action-runner",
                )
            store.append(run_id, "run/completed", {"gate": "approved", "action_count": len(gated), "executor": "one-shot-action-runner"}, actor="action-runner")
            return store.state(run_id)
        finally:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="执行已批准的本地安全草稿动作")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    store = RunStore(args.state_dir)
    try:
        state = execute_actions(store, args.run_id)
    except Exception as exc:
        store.append(args.run_id, "action/failed", {"error": f"{type(exc).__name__}: {exc}"}, actor="action-runner")
        store.append(args.run_id, "run/failed", {"error": f"action runner failed: {type(exc).__name__}: {exc}"}, actor="action-runner")
        raise
    print(json.dumps(state, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
