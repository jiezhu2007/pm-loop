#!/usr/bin/env python3
"""Advance the durable V4.5 migration freeze between already-gated stages.

This is deliberately narrower than the stage runner: it changes only the
``migration_freeze.stage_id`` fence and writes an auditable continuation
report.  It does not touch jobs, runs, Outbox rows, providers, or services.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

from pm_system_store import PMSystemStore, now_iso
from pm_system_v45_migration import STAGES, _matching_processes, automation_statuses, snapshot


def _future_iso(seconds: int) -> str:
    return (
        (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(seconds))))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _report_path(report_dir: Path, target_stage: str) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"freeze-continuation-{target_stage.lower()}-检查报告.json"


def _write_report(report_dir: Path, report: Dict[str, Any]) -> Path:
    path = _report_path(report_dir, str(report["to_stage"]))
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown = path.with_suffix(".md")
    checks = "\n".join(
        f"- {item['name']}：`{item['status']}`，{item.get('detail', '')}"
        for item in report.get("checks", [])
    )
    markdown.write_text(
        "# V4.5 R2 Freeze Continuation "
        + str(report["from_stage"])
        + " → "
        + str(report["to_stage"])
        + " 检查报告\n\n"
        + f"- 判定：`{report['decision']}`\n"
        + f"- migration_id：`{report['migration_id']}`\n"
        + f"- migration_epoch：`{report['migration_epoch']}`\n"
        + f"- owner：`{report['owner']}`\n"
        + f"- 采集时间：`{report['finished_at']}`\n\n"
        + "## 检查项\n\n"
        + checks
        + "\n\n## 快照\n\n```json\n"
        + json.dumps({"before": report.get("before"), "after": report.get("after")}, ensure_ascii=False, indent=2)
        + "\n```\n",
        encoding="utf-8",
    )
    html = markdown.with_suffix(".html")
    converter = Path(__file__).with_name("markdown_to_architecture_html.py")
    try:
        subprocess.run([os.environ.get("CODEX_PYTHON", sys.executable), str(converter), str(markdown), str(html)], check=True, capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError):
        pass
    return path


def continue_freeze(
    *,
    db_path: Path,
    report_dir: Path,
    migration_id: str,
    epoch: str,
    owner: str,
    from_stage: str,
    to_stage: str,
    lease_seconds: int = 900,
) -> Dict[str, Any]:
    source = str(from_stage).upper()
    target = str(to_stage).upper()
    if source not in STAGES or target not in STAGES or STAGES.index(target) != STAGES.index(source) + 1:
        raise ValueError(f"continuation must advance one stage: {source} -> {target}")

    store = PMSystemStore(db_path, auto_migrate=False)
    before = snapshot(store)
    freeze = store.migration_freeze()
    checks = []
    if freeze is None:
        checks.append({"name": "persistent migration freeze", "status": "HOLD", "detail": "freeze row missing"})
    else:
        checks.append({
            "name": "persistent migration freeze",
            "status": "PASS" if freeze.get("migration_id") == migration_id and freeze.get("migration_epoch") == epoch and freeze.get("state") == "freeze" else "HOLD",
            "detail": json.dumps({"migration_id": freeze.get("migration_id"), "migration_epoch": freeze.get("migration_epoch"), "stage_id": freeze.get("stage_id"), "state": freeze.get("state")}, ensure_ascii=False),
        })
        checks.append({
            "name": "source stage matches fence",
            "status": "PASS" if freeze.get("stage_id") == source else "HOLD",
            "detail": f"fence_stage={freeze.get('stage_id')}; expected={source}",
        })

    with store.connect() as connection:
        active = [dict(row) for row in connection.execute(
            "SELECT migration_id,stage_id,owner,lease_id,lease_expires_at FROM migration_leases WHERE migration_id=? AND state='active'",
            (migration_id,),
        ).fetchall()]
    checks.append({"name": "no active stage lease", "status": "PASS" if not active else "HOLD", "detail": json.dumps(active, ensure_ascii=False)})

    # G0 through the G6 entry must stay in maintenance with business writers
    # stopped.  LaunchAgents are KeepAlive-managed and can restart between
    # stages, so the drain is checked at every continuation rather than only
    # once during G0.
    if STAGES.index(target) <= STAGES.index("G6"):
        residual = _matching_processes()
        automations = automation_statuses()
        checks.append({"name": "business services drained", "status": "PASS" if not residual else "HOLD", "detail": json.dumps(residual, ensure_ascii=False)})
        checks.append({"name": "Codex Automations paused", "status": "PASS" if automations and all(value == "PAUSED" for value in automations.values()) else "HOLD", "detail": json.dumps(automations, ensure_ascii=False)})

    decision = "HOLD"
    if all(item["status"] == "PASS" for item in checks):
        current_deadline = _parse_time(str(freeze.get("deadline_at"))) if freeze else None
        proposed_deadline = _parse_time(_future_iso(lease_seconds))
        deadline = max(current_deadline, proposed_deadline) if current_deadline and proposed_deadline else proposed_deadline
        deadline_text = deadline.isoformat(timespec="seconds").replace("+00:00", "Z") if deadline else _future_iso(lease_seconds)
        with store.transaction() as connection:
            connection.execute(
                "UPDATE migration_freeze SET stage_id=?,owner=?,deadline_at=?,updated_at=? WHERE freeze_id=1 AND migration_id=? AND migration_epoch=? AND state='freeze'",
                (target, owner, deadline_text, now_iso(), migration_id, epoch),
            )
        decision = "PASS"

    after = snapshot(store)
    report = {
        "schema": "pm-system.v45-r2-freeze-continuation-report.v1",
        "migration_id": migration_id,
        "migration_epoch": epoch,
        "owner": owner,
        "from_stage": source,
        "to_stage": target,
        "started_at": now_iso(),
        "finished_at": now_iso(),
        "decision": decision,
        "checks": checks,
        "before": {"freeze": freeze, "snapshot": before},
        "after": {"freeze": store.migration_freeze(), "snapshot": after},
        "business_state_mutated": False,
    }
    report["manifest_hash"] = _hash(report)
    report_path = _write_report(report_dir, report)
    report["report_path"] = str(report_path)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--from-stage", required=True)
    parser.add_argument("--to-stage", required=True)
    parser.add_argument("--lease-seconds", type=int, default=900)
    args = parser.parse_args()
    report = continue_freeze(**vars(args))
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0 if report["decision"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
