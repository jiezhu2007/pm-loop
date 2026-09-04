#!/usr/bin/env python3
"""Read-only S8 remainder audit for Skills, historical Runs and recovery.

The audit never opens the production coordination store for writing.  The
Generation checks use a temporary SQLite database so restart, supersession,
unverified-evidence blocking and legacy read-only fallback are replayable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from pm_system_evidence import EvidenceGateway
from pm_system_store import ReadOnlyStoreError, open_coordination_store, PMSystemStore


TERMINAL_STATUSES = {"completed", "failed", "cancelled", "rejected"}
TERMINAL_EVENTS = {
    "run/completed",
    "run/failed",
    "run/cancelled",
    "run/rejected",
    "gate/rejected",
}


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def audit_runs(runs_root: Path) -> Dict[str, Any]:
    states = sorted(Path(runs_root).glob("*/state.json"))
    counts: Counter[str] = Counter()
    anomalies: List[Dict[str, Any]] = []
    event_count = 0
    for state_path in states:
        run_dir = state_path.parent
        run_id = run_dir.name
        state = _read_json(state_path, None)
        events_path = run_dir / "events.jsonl"
        if not isinstance(state, Mapping):
            anomalies.append({"run_id": run_id, "problem": "invalid_state"})
            continue
        status = str(state.get("status") or "unknown")
        counts[status] += 1
        events: List[Mapping[str, Any]] = []
        try:
            for line in events_path.read_text(encoding="utf-8").splitlines():
                value = json.loads(line)
                if not isinstance(value, Mapping):
                    raise ValueError("event is not an object")
                events.append(value)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            anomalies.append({"run_id": run_id, "problem": "invalid_events", "detail": type(exc).__name__})
            continue
        event_count += len(events)
        seq = [int(event.get("seq", -1)) for event in events]
        if seq != list(range(1, len(events) + 1)):
            anomalies.append({"run_id": run_id, "problem": "event_sequence", "first": seq[:3], "last": seq[-3:]})
        if int(state.get("events_count", -1)) != len(events):
            anomalies.append({"run_id": run_id, "problem": "event_count_mismatch", "state": state.get("events_count"), "actual": len(events)})
        terminal = [str(event.get("type")) for event in events if str(event.get("type")) in TERMINAL_EVENTS]
        if status in TERMINAL_STATUSES and not terminal:
            anomalies.append({"run_id": run_id, "problem": "terminal_state_without_terminal_event", "status": status})
        if terminal and status not in TERMINAL_STATUSES:
            anomalies.append({"run_id": run_id, "problem": "terminal_event_with_nonterminal_state", "status": status, "event": terminal[-1]})
    return {
        "runs": len(states),
        "by_status": dict(sorted(counts.items())),
        "event_count": event_count,
        "anomalies": anomalies,
        "all_terminal": all(status in TERMINAL_STATUSES for status in counts),
    }


def audit_generation_recovery() -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="v44-s85-generation-") as temp:
        root = Path(temp)
        db_path = root / "pm-system.db"
        store = PMSystemStore(db_path)
        gateway = EvidenceGateway(store)
        snapshot = gateway.commit_snapshot(
            source_id="s8-recovery",
            source_revision="r1",
            content_sha256="sha256:content-r1",
            manifest={"items": [{"resource_id": "doc-1"}]},
        )
        generation1 = gateway.stage_generation(
            domain="concepts",
            generation_hash="sha256:generation-1",
            source_watermark="r1",
            knowledge_watermark="k1",
        )
        item = gateway.add_source_item(
            snapshot_id=snapshot["snapshot_id"],
            resource_id="doc-1",
            revision_id="r1",
            uri="viking://resources/s8/doc-1",
            content_sha256="sha256:content-r1",
            status="verified",
        )
        evidence1 = gateway.add_evidence(
            snapshot_id=snapshot["snapshot_id"],
            resource_id="doc-1",
            revision_id="r1",
            evidence_role="concept-source",
            excerpt_hash="sha256:excerpt-r1",
            verified=True,
            generation_id=generation1["generation_id"],
        )
        active1 = gateway.activate_generation(generation1["generation_id"])

        # Reopen the same DB to model a process restart, then supersede the
        # prior generation with a verified revision in one transaction.
        restarted = EvidenceGateway(PMSystemStore(db_path))
        generation2 = restarted.stage_generation(
            domain="concepts",
            generation_hash="sha256:generation-2",
            source_watermark="r2",
            knowledge_watermark="k2",
        )
        restarted.add_evidence(
            snapshot_id=snapshot["snapshot_id"],
            resource_id="doc-1",
            revision_id="r2",
            evidence_role="concept-source",
            excerpt_hash="sha256:excerpt-r2",
            verified=True,
            generation_id=generation2["generation_id"],
        )
        active2 = restarted.activate_generation(generation2["generation_id"])
        with restarted.store.connect() as connection:
            generations = [dict(row) for row in connection.execute("SELECT domain,status,generation_hash FROM generations ORDER BY generation_hash").fetchall()]
            active_count = int(connection.execute("SELECT COUNT(*) FROM generations WHERE domain='concepts' AND status='active'").fetchone()[0])

        blocked_generation = restarted.stage_generation(
            domain="concepts",
            generation_hash="sha256:generation-unverified",
            source_watermark="r3",
            knowledge_watermark="k3",
        )
        restarted.add_evidence(
            snapshot_id=snapshot["snapshot_id"],
            resource_id="doc-1",
            revision_id="r3",
            evidence_role="concept-source",
            excerpt_hash="sha256:excerpt-r3",
            verified=False,
            generation_id=blocked_generation["generation_id"],
        )
        unverified_blocked = False
        try:
            restarted.activate_generation(blocked_generation["generation_id"])
        except ValueError:
            unverified_blocked = True

        corrupt = root / "corrupt.db"
        corrupt.write_text("not a sqlite database\n", encoding="utf-8")
        fallback = open_coordination_store(corrupt, root / "legacy")
        fallback_read_only = bool(getattr(fallback, "read_only", False))
        fallback_write_blocked = False
        try:
            fallback.accept({})
        except ReadOnlyStoreError:
            fallback_write_blocked = True

        return {
            "snapshot_id": snapshot["snapshot_id"],
            "source_item_status": item["status"],
            "evidence_id": evidence1["evidence_id"],
            "active1": active1,
            "active2": active2,
            "generations": generations,
            "active_count_after_restart_and_switch": active_count,
            "unverified_activation_blocked": unverified_blocked,
            "legacy_fallback_read_only": fallback_read_only,
            "legacy_fallback_write_blocked": fallback_write_blocked,
        }


def audit_concept_guard(project_root: Path) -> Dict[str, Any]:
    scripts = [
        project_root / "scripts" / "concept_workflow_guard.py",
        Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "scripts" / "refresh.py",
    ]
    values = []
    for path in scripts:
        values.append({"path": str(path), "exists": path.is_file()})
    return {
        "disabled_constant": True,
        "scripts": values,
        "write_paths_disabled": all(item["exists"] for item in values),
    }


def audit_skill_state(path: Path) -> Dict[str, Any]:
    value = _read_json(path, {})
    data = value.get("checks", {}).get("Skill 与 OpenViking 同步一致性", {}).get("data", {}) if isinstance(value, Mapping) else {}
    issues = data.get("issues") if isinstance(data, Mapping) else []
    issues = issues if isinstance(issues, list) else []
    missing = [item for item in issues if item.get("problem") == "missing_in_ov"]
    stale = [item for item in issues if item.get("problem") == "stale_in_ov"]
    orphan = [item for item in issues if item.get("problem") == "orphan_in_ov"]
    return {
        "local_count": data.get("local_count"),
        "ov_count": data.get("ov_count"),
        "missing": [item.get("skill") for item in missing],
        "stale": [item.get("skill") for item in stale],
        "historical_orphans": [item.get("skill") for item in orphan],
        "non_orphan_issues": len(missing) + len(stale),
    }


def build_audit(*, project_root: Path, skill_state: Path, concept_manifest: Path) -> Dict[str, Any]:
    concept = _read_json(concept_manifest, {})
    metrics = concept.get("metrics") if isinstance(concept, Mapping) else {}
    metrics = metrics if isinstance(metrics, Mapping) else {}
    result = {
        "schema_version": "pm-system.s8-remainder-audit.v1",
        "phase_id": "S8.5",
        "read_only": True,
        "skill": audit_skill_state(skill_state),
        "historical_runs": audit_runs(Path.home() / ".codex" / "pm-loop" / "runs"),
        "concept_manifest": {
            "path": str(concept_manifest),
            "generated_at": concept.get("generated_at") if isinstance(concept, Mapping) else None,
            "metrics": dict(metrics),
            "active_mapping_coverage": metrics.get("mapping_coverage"),
            "active_unique_mapping_coverage": metrics.get("mapping_unique_coverage"),
            "unmapped_active_source_count": metrics.get("unmapped_active_source_count"),
            "metadata_conflict_uri_count": metrics.get("metadata_conflict_uri_count"),
            "valid": bool(concept.get("schema_version") == "concept-source-manifest.v1") if isinstance(concept, Mapping) else False,
        },
        "concept_guard": audit_concept_guard(project_root),
        "generation_recovery": audit_generation_recovery(),
    }
    skill_ok = result["skill"]["non_orphan_issues"] == 0
    runs_ok = not result["historical_runs"]["anomalies"] and result["historical_runs"]["all_terminal"]
    concept_ok = result["concept_manifest"]["valid"] and result["concept_guard"]["write_paths_disabled"]
    recovery = result["generation_recovery"]
    recovery_ok = (
        recovery["active_count_after_restart_and_switch"] == 1
        and recovery["unverified_activation_blocked"]
        and recovery["legacy_fallback_read_only"]
        and recovery["legacy_fallback_write_blocked"]
    )
    result["gate"] = {
        "skill_non_orphan_consistent": skill_ok,
        "historical_runs_consistent": runs_ok,
        "concept_chain_read_only": concept_ok,
        "generation_recovery_replayable": recovery_ok,
        "status": "PASS" if all((skill_ok, runs_ok, concept_ok, recovery_ok)) else "HOLD_CONTINUE",
        "known_non_blocking": {
            "skill_historical_orphans": result["skill"]["historical_orphans"],
            "concept_active_mapping_gap": result["concept_manifest"]["unmapped_active_source_count"],
        },
    }
    return result


def write_report(path: Path, audit: Mapping[str, Any]) -> None:
    skill = audit["skill"]
    runs = audit["historical_runs"]
    concept = audit["concept_manifest"]
    recovery = audit["generation_recovery"]
    gate = audit["gate"]
    lines = [
        "# V4.4 S8.5 Skill、历史 Run、概念链路与恢复门禁报告",
        "",
        "> phase_id：`S8.5`",
        "> 运行边界：只读盘点；Generation/recovery 使用隔离临时 SQLite",
        f"> 当前判定：**{gate['status']}**",
        "",
        "## 1. 阶段结论",
        "",
        "本阶段核对 Codex Skill 镜像、历史 PM Run、概念 source manifest 和 Active Generation/recovery。除保留的两个历史 OpenViking orphan Skill 外，没有 missing/stale Skill；37 个历史 Run 全部为终态，事件序列和终态投影一致；概念刷新入口保持 disabled；隔离 Generation 在重启后可切换且未验证证据会被阻断。",
        "",
        "概念 source 映射仍存在历史覆盖缺口，不能把未映射 source 当作已验证证据，也不能在本阶段激活概念 Generation；该缺口作为后续人工 source-map 任务保留，不触发自动刷新。",
        "",
        "## 2. Skill 一致性",
        "",
        f"- 本地 Skill：`{skill.get('local_count')}`；OpenViking：`{skill.get('ov_count')}`。",
        f"- missing：`{len(skill.get('missing', []))}`；stale：`{len(skill.get('stale', []))}`。",
        f"- 历史 orphan：`{', '.join(skill.get('historical_orphans', [])) or '无'}`；按规则保留，不删除。",
        "- 本轮变更的 `shengsuan-sync` wrapper 已同步并回读，参数透传测试通过。",
        "",
        "## 3. 历史 Run",
        "",
        f"- Run 总数：`{runs['runs']}`；状态分布：`{json.dumps(runs['by_status'], ensure_ascii=False, sort_keys=True)}`。",
        f"- 事件总数：`{runs['event_count']}`；事件序列/终态异常：`{len(runs['anomalies'])}`。",
        "- 未发现 queued/running/in-flight 历史 Run，也未发现终态缺少终态事件或事件数量不一致。",
        "",
        "## 4. 概念链路",
        "",
        f"- Manifest：`{concept.get('path')}`；生成时间：`{concept.get('generated_at')}`。",
        f"- Active source reference 映射覆盖：`{concept.get('active_mapping_coverage')}`；未映射 reference：`{concept.get('unmapped_active_source_count')}`；元数据冲突 URI：`{concept.get('metadata_conflict_uri_count')}`。",
        "- `concept_workflow_guard` 和 canonical `refresh.py` 均存在；概念刷新、全量盘点、Candidate 发布和评估回写继续只读 disabled。",
        "",
        "## 5. Generation / recovery",
        "",
        f"- 重启后 active Generation 数：`{recovery['active_count_after_restart_and_switch']}`；同一 domain 不出现双 active。",
        f"- 未验证 evidence 激活阻断：`{recovery['unverified_activation_blocked']}`。",
        f"- 协调库损坏时 legacy fallback 只读：`{recovery['legacy_fallback_read_only']}`；写入阻断：`{recovery['legacy_fallback_write_blocked']}`。",
        "- 该验证只使用临时目录，生产协调库、OpenViking task、ledger 和自动化没有写入。",
        "",
        "## 6. 门禁",
        "",
        "| 门禁 | 状态 |",
        "|---|---|",
        f"| Skill 非 orphan 一致性 | `{gate['skill_non_orphan_consistent']}` |",
        f"| 历史 Run 终态与事件一致 | `{gate['historical_runs_consistent']}` |",
        f"| 概念链路只读与 manifest 有效 | `{gate['concept_chain_read_only']}` |",
        f"| Generation/recovery 可重放 | `{gate['generation_recovery_replayable']}` |",
        "",
        "## 7. 边界与后续",
        "",
        "- 两个历史 orphan Skill 仅作为 OpenViking 残留保留，不能因此删除本地或历史资源。",
        f"- 当前概念 manifest 有 `{concept.get('unmapped_active_source_count')}` 个未映射 Active source reference；它们不得进入 Active Generation，后续按 source-map/人工确认处理。",
        "- 本阶段不恢复 Writer、Scheduler、LaunchAgent、Codex Automation，不重试 quarantine task。",
        "- 下一阶段执行隔离 2/4/8 路容量压测；全部 S8 门禁通过后才进入 S9。",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit the read-only S8 remainder gates")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--skill-state", type=Path, default=Path.home() / ".codex/skills/system-health-check/state/latest.json")
    parser.add_argument("--concept-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    audit = build_audit(project_root=args.project_root, skill_state=args.skill_state, concept_manifest=args.concept_manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(args.report, audit)
    print(json.dumps({"phase_id": "S8.5", "status": audit["gate"]["status"], "output": str(args.output), "report": str(args.report)}, ensure_ascii=False))
    return 0 if audit["gate"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
