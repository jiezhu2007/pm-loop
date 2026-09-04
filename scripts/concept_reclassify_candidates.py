#!/usr/bin/env python3
"""Reclassify actionable duplicate new-concept Candidates against Active concepts.

The command is dry-run by default.  ``--apply`` preserves the Candidate and
its evidence/content, but marks an unambiguous Active match as an alias/merge
and moves it to the terminal ``superseded`` state so it leaves the review
queue.  Candidate writes use a whole-file hash compare-and-swap to avoid
overwriting concurrent review work.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from concept_deep_inventory import (
    _active_concept_index,
    _active_match_for_term,
    _active_taxonomy_rows,
    _normalize_term,
)
from concept_full_inventory import taxonomy
from concept_learning import ConceptLearningStore, now_iso
from concept_refresh_adapter import append_agent_audit
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


DEFAULT_SKILL_ROOT = Path("~/.codex/skills/shengsuan-concepts")
ACTIONABLE_STATUSES = {
    "ready_for_review",
    "paused",
    "changes_requested",
    "stale",
    "failed",
}
RULE_VERSION = "active-match-v1"
MIGRATION_VERSION = "candidate-reclassification-v1"
RECLASSIFICATION_SCHEMA = "concept-learning.candidate-reclassification.v1"


def _file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _plan_candidate(
    store: ConceptLearningStore,
    candidate: Mapping[str, Any],
    active_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if str(candidate.get("kind") or "") != "new_concept":
        return None
    status = str(candidate.get("status") or "")
    if status not in ACTIONABLE_STATUSES:
        return None
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    concept = str(candidate.get("concept") or "").strip()
    if not candidate_id or not concept:
        return None
    match = _active_match_for_term(concept, active_index)
    if not match:
        return None
    path = store.candidate_path(candidate_id)
    if not path.is_file():
        return None
    match_type = "exact" if match.get("match_type") == "exact" else "controlled_fuzzy"
    matched_surface = str(match.get("matched_surface") or match["target"])
    if match_type == "exact":
        reason = f"规范化后与 Active 名称/别名「{matched_surface}」完全一致"
    elif "pipline" in concept.casefold():
        reason = f"历史拼写 Pipline 已归一化为 Pipeline，并命中 Active 别名「{matched_surface}」"
    elif str(match.get("common") or "") == "资源":
        reason = f"命中受控资源子能力词形，并归入 Active「{match['target']}」"
    elif str(match.get("common") or "").endswith("权限"):
        reason = f"命中受控行/列权限词形，并归入 Active「{match['target']}」"
    elif _normalize_term(matched_surface) in _normalize_term(concept):
        reason = f"候选名称包含 Active 名称/别名「{matched_surface}」，属于更具体子能力"
    else:
        reason = f"受控词形命中 Active 名称/别名「{matched_surface}」"
    return {
        "candidate_id": candidate_id,
        "concept": concept,
        "from_kind": "new_concept",
        "from_status": status,
        "to_kind": str(match["decision"]),
        "to_status": "superseded",
        "target": str(match["target"]),
        "matched_surface": matched_surface,
        "match_type": match_type,
        "reason": reason,
        "score": float(match.get("score") or 0),
        "common": str(match.get("common") or ""),
        "candidate_file_sha256": _file_sha256(path),
    }


def build_plan(store: ConceptLearningStore) -> Dict[str, Any]:
    candidates = store.list_candidates()
    active_rows = _active_taxonomy_rows(taxonomy(store))
    active_index = _active_concept_index(active_rows)
    plans: List[Dict[str, Any]] = []
    new_concept_statuses: Counter[str] = Counter()
    actionable_count = 0

    for candidate in candidates:
        if str(candidate.get("kind") or "") != "new_concept":
            continue
        status = str(candidate.get("status") or "")
        new_concept_statuses[status] += 1
        if status in ACTIONABLE_STATUSES:
            actionable_count += 1
        item = _plan_candidate(store, candidate, active_index)
        if item:
            plans.append(item)

    plans.sort(key=lambda item: (item["target"], item["concept"], item["candidate_id"]))
    decision_counts = Counter(item["to_kind"] for item in plans)
    return {
        "schema_version": "concept-learning.candidate-reclassification-plan.v1",
        "rule_version": RULE_VERSION,
        "active_concept_count": len(active_rows),
        "candidate_count": len(candidates),
        "new_concept_count": sum(new_concept_statuses.values()),
        "new_concept_statuses": dict(sorted(new_concept_statuses.items())),
        "actionable_new_concept_count": actionable_count,
        "matched_count": len(plans),
        "matched_decisions": dict(sorted(decision_counts.items())),
        "items": plans,
    }


def _reclassification(plan: Mapping[str, Any], actor: str, at: str) -> Dict[str, Any]:
    value = {
        "schema_version": RECLASSIFICATION_SCHEMA,
        "from_kind": str(plan["from_kind"]),
        "from_status": str(plan["from_status"]),
        "decision": str(plan["to_kind"]),
        "target": str(plan["target"]),
        "matched_surface": str(plan["matched_surface"]),
        "match_type": str(plan["match_type"]),
        "score": float(plan["score"]),
        "reason": str(plan["reason"]),
        "rule_version": RULE_VERSION,
        "matcher_version": RULE_VERSION,
        "migration_version": MIGRATION_VERSION,
        "actor": actor,
        "at": at,
    }
    if plan.get("common"):
        value["common"] = str(plan["common"])
    return value


def apply_plan(
    store: ConceptLearningStore,
    plan: Mapping[str, Any],
    *,
    actor: str = "codex",
) -> Dict[str, Any]:
    applied: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    audit_failures: List[Dict[str, Any]] = []

    for planned in list(plan.get("items") or []):
        candidate_id = str(planned.get("candidate_id") or "")
        path = store.candidate_path(candidate_id)
        try:
            if planned.get("match_type") not in {"exact", "controlled_fuzzy"}:
                raise ValueError("match is not deterministic enough for automatic apply")
            with store.candidate_lock(candidate_id):
                if not path.is_file():
                    raise FileNotFoundError("candidate file disappeared")
                current_sha256 = _file_sha256(path)
                if current_sha256 != planned.get("candidate_file_sha256"):
                    raise ValueError("candidate changed after planning")
                current = json.loads(path.read_text(encoding="utf-8"))
                if str(current.get("kind") or "") != planned.get("from_kind"):
                    raise ValueError("candidate kind changed after planning")
                if str(current.get("status") or "") != planned.get("from_status"):
                    raise ValueError("candidate status changed after planning")
                if str(current.get("concept") or "") != planned.get("concept"):
                    raise ValueError("candidate concept changed after planning")

                at = now_iso()
                reclassification = _reclassification(planned, actor, at)
                current.update(
                    {
                        "kind": str(planned["to_kind"]),
                        "status": "superseded",
                        "triage_decision": str(planned["to_kind"]),
                        "merge_target": str(planned["target"]),
                        "matcher_version": RULE_VERSION,
                        "migration_version": MIGRATION_VERSION,
                        "reclassification": reclassification,
                        "superseded_reason": "reclassified_to_active_concept",
                        "superseded_at": at,
                        "reclassified_at": at,
                        "updated_at": at,
                    }
                )
                store.save_candidate(current)
        except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError) as exc:
            conflicts.append(
                {
                    "candidate_id": candidate_id,
                    "concept": str(planned.get("concept") or ""),
                    "reason": str(exc),
                }
            )
            continue

        audit_payload = {
            "candidate_id": candidate_id,
            "concept": str(planned["concept"]),
            "from_kind": str(planned["from_kind"]),
            "from_status": str(planned["from_status"]),
            "to_kind": str(planned["to_kind"]),
            "to_status": "superseded",
            "target": str(planned["target"]),
            "match_type": str(planned["match_type"]),
            "score": float(planned["score"]),
            "reason": str(planned["reason"]),
            "rule_version": RULE_VERSION,
            "matcher_version": RULE_VERSION,
            "migration_version": MIGRATION_VERSION,
            "actor": actor,
        }
        try:
            append_agent_audit(store.skill_root, "candidate.reclassified", audit_payload)
        except OSError as exc:
            audit_failures.append({"candidate_id": candidate_id, "reason": str(exc)})
        applied.append({**audit_payload, "audit_written": not audit_failures or audit_failures[-1].get("candidate_id") != candidate_id})

    return {
        "schema_version": "concept-learning.candidate-reclassification-result.v1",
        "rule_version": RULE_VERSION,
        "planned_count": len(list(plan.get("items") or [])),
        "applied_count": len(applied),
        "conflict_count": len(conflicts),
        "audit_failure_count": len(audit_failures),
        "applied": applied,
        "conflicts": conflicts,
        "audit_failures": audit_failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="将明确命中 Active 概念的历史 new_concept 候选重分类并移出待审队列"
    )
    parser.add_argument("--skill-root", type=Path, default=DEFAULT_SKILL_ROOT)
    parser.add_argument("--apply", action="store_true", help="实际写入；默认仅输出 dry-run 计划")
    parser.add_argument("--actor", default="codex")
    args = parser.parse_args()

    # Planning is read-only and remains useful for historical audit.  Applying
    # a reclassification mutates Candidate state, so the retired workflow
    # rejects it before opening the skill state directory for writes.
    if CONCEPT_REFRESH_DISABLED and args.apply:
        return emit_disabled("candidate-reclassify")

    store = ConceptLearningStore(args.skill_root)
    plan = build_plan(store)
    if not args.apply:
        result: Dict[str, Any] = {"mode": "dry_run", **plan}
    else:
        result = {
            "mode": "apply",
            "plan_summary": {key: value for key, value in plan.items() if key != "items"},
            **apply_plan(store, plan, actor=str(args.actor)),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not args.apply or not result.get("conflict_count") else 2


if __name__ == "__main__":
    raise SystemExit(main())
