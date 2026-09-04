#!/usr/bin/env python3
"""Concept-owned administrative transitions with an auditable safety gate.

This module is intentionally narrower than the Control Plane.  It performs
two explicit, human-directed operations that belong to the concept runner:

* create a new-concept proposal from a historical terminal Candidate without
  reviving that terminal row; and
* mark a guarded set of refresh proposals as not required after a human
  decision, preserving their evidence and content.

The command is dry-run by default for the batch operation.  Applying a batch
requires an expected count and uses a whole-file hash CAS for every row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:  # project checkout (`scripts` namespace) and runtime mirror both work
    from scripts.concept_learning import ConceptLearningStore, content_hash, make_candidate, now_iso  # noqa: E402
    from scripts.concept_refresh_adapter import append_agent_audit  # noqa: E402
    from scripts.concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled  # noqa: E402
except ModuleNotFoundError:  # pragma: no cover - exercised by runtime mirror
    from concept_learning import ConceptLearningStore, content_hash, make_candidate, now_iso  # type: ignore # noqa: E402
    from concept_refresh_adapter import append_agent_audit  # type: ignore # noqa: E402
    from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled  # type: ignore # noqa: E402


ADMIN_SCHEMA = "concept-learning.candidate-admin.v1"
SUPERSEDE_DECISION = "user_confirmed_duplicate_refresh"
SUPERSEDE_REASON = "refresh_not_required_user_confirmed"
TERMINAL_STATUSES = {"published", "rejected", "superseded"}


def _file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate_path(store: ConceptLearningStore, candidate: Mapping[str, Any]) -> Path:
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    if not candidate_id:
        raise ValueError("candidate has no candidate_id")
    path = store.candidate_path(candidate_id)
    if not path.is_file():
        raise FileNotFoundError(f"candidate manifest missing: {path}")
    return path


def build_refresh_supersede_plan(
    store: ConceptLearningStore,
    *,
    statuses: Iterable[str] = ("ready_for_review",),
    expected_count: Optional[int] = None,
) -> Dict[str, Any]:
    """Build a deterministic plan for actionable refresh rows.

    ``expected_count`` is a deliberate guard: a future run must not silently
    reinterpret a broader or smaller queue as the same user decision.
    """

    allowed = {str(value) for value in statuses}
    rows = [
        candidate
        for candidate in store.list_candidates()
        if str(candidate.get("kind") or "") == "refresh"
        and str(candidate.get("status") or "") in allowed
    ]
    rows.sort(key=lambda value: (str(value.get("concept") or ""), str(value.get("candidate_id") or "")))
    if expected_count is not None and len(rows) != int(expected_count):
        raise ValueError(
            f"refresh candidate count changed: expected {int(expected_count)}, found {len(rows)}"
        )
    items: List[Dict[str, Any]] = []
    for candidate in rows:
        path = _candidate_path(store, candidate)
        items.append(
            {
                "candidate_id": str(candidate["candidate_id"]),
                "concept": str(candidate.get("concept") or ""),
                "status": str(candidate.get("status") or ""),
                "kind": str(candidate.get("kind") or ""),
                "candidate_file_sha256": _file_sha256(path),
                "content_hash": str(candidate.get("content_hash") or ""),
                "base_page_sha256": candidate.get("base_page_sha256"),
                "created_at": candidate.get("created_at"),
            }
        )
    return {
        "schema_version": f"{ADMIN_SCHEMA}.refresh-supersede-plan",
        "decision": SUPERSEDE_DECISION,
        "reason": SUPERSEDE_REASON,
        "statuses": sorted(allowed),
        "count": len(items),
        "items": items,
    }


def apply_refresh_supersede_plan(
    store: ConceptLearningStore,
    plan: Mapping[str, Any],
    *,
    actor: str = "zhujie14",
    reason: str = SUPERSEDE_REASON,
) -> Dict[str, Any]:
    """Apply a planned batch without overwriting a concurrent review edit."""

    applied: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    for item in list(plan.get("items") or []):
        candidate_id = str(item.get("candidate_id") or "")
        if not candidate_id:
            conflicts.append({"candidate_id": candidate_id, "error": "missing candidate_id"})
            continue
        path = store.candidate_path(candidate_id)
        try:
            if not path.is_file():
                raise FileNotFoundError("candidate manifest missing")
            expected_status = str(item.get("status") or "")
            current = store.read_candidate(candidate_id)
            if str(current.get("kind") or "") != "refresh":
                raise ValueError("candidate kind changed after planning")
            at = now_iso()
            transition = {
                "schema_version": ADMIN_SCHEMA,
                "decision": SUPERSEDE_DECISION,
                "reason": reason,
                "from_kind": "refresh",
                "from_status": expected_status,
                "to_status": "superseded",
                "actor": actor,
                "at": at,
            }
            updated = store.update_candidate_cas(
                candidate_id,
                expected_file_sha256=str(item.get("candidate_file_sha256") or ""),
                expected_statuses={expected_status},
                status="superseded",
                superseded_at=at,
                superseded_reason=reason,
                review_decision="not_required",
                duplicate_decision=SUPERSEDE_DECISION,
                duplicate_of=f"active:{current.get('concept') or item.get('concept')}",
                duplicate_active_page_sha256=current.get("base_page_sha256"),
                candidate_admin_transition=transition,
            )
            applied.append(
                {
                    "candidate_id": candidate_id,
                    "concept": updated.get("concept"),
                    "from_status": expected_status,
                    "to_status": "superseded",
                }
            )
            append_agent_audit(
                store.skill_root,
                "candidate.superseded",
                {
                    "candidate_id": candidate_id,
                    "concept": updated.get("concept"),
                    "kind": "refresh",
                    "from_status": expected_status,
                    "reason": reason,
                    "decision": SUPERSEDE_DECISION,
                    "actor": actor,
                    "content_hash": updated.get("content_hash"),
                },
            )
        except Exception as exc:  # keep applying independent rows
            conflicts.append({"candidate_id": candidate_id, "error": f"{type(exc).__name__}: {exc}"})
    return {
        "schema_version": f"{ADMIN_SCHEMA}.refresh-supersede-result",
        "decision": SUPERSEDE_DECISION,
        "reason": reason,
        "applied_count": len(applied),
        "conflict_count": len(conflicts),
        "applied": applied,
        "conflicts": conflicts,
    }


def restore_new_concept_candidate(
    store: ConceptLearningStore,
    source_candidate_id: str,
    content_path: Path,
    *,
    actor: str = "zhujie14",
    note: str = "",
) -> Dict[str, Any]:
    """Create a fresh proposal from a terminal Candidate's evidence lineage."""

    source = store.read_candidate(source_candidate_id)
    source_status = str(source.get("status") or "")
    if source_status not in TERMINAL_STATUSES:
        raise ValueError(f"source candidate must be terminal, got: {source_status}")
    concept = str(source.get("concept") or "").strip()
    if not concept:
        raise ValueError("source candidate has no concept")
    ledger = store.load_ledger()
    record = ledger.get(concept)
    if isinstance(record, dict) and str(record.get("status") or "active") == "active":
        raise ValueError(f"concept is already Active: {concept}")
    page_path = store.skill_root / "state" / "pages" / f"{concept}.md"
    if page_path.is_file():
        raise ValueError(f"concept page already exists: {page_path}")
    reviewed_path = content_path.expanduser().resolve()
    if not reviewed_path.is_file():
        raise FileNotFoundError(f"reviewed content missing: {reviewed_path}")
    content = reviewed_path.read_text(encoding="utf-8").strip() + "\n"
    if not content.startswith("---"):
        raise ValueError("reviewed content must start with YAML frontmatter")
    # The frontmatter validator is intentionally lightweight here; publish's
    # normal hash/evidence/base checks remain the authoritative gate.
    if f"concept: {concept}" not in content and f'"concept": "{concept}"' not in content:
        raise ValueError("reviewed content concept does not match source candidate")
    source_refs = list(dict.fromkeys(str(uri).strip() for uri in (source.get("source_refs") or []) if str(uri).strip()))
    if not source_refs:
        raise ValueError("source candidate has no evidence sources")
    with store.concept_lock(concept):
        # Idempotency: do not create a second lineage if a previous invocation
        # already made a reviewable/published restoration candidate.
        for existing in store.list_candidates(concept=concept):
            if existing.get("restored_from_candidate_id") == source_candidate_id:
                return existing
        at = now_iso()
        candidate = make_candidate(
            concept=concept,
            kind="new-concept",
            content=content,
            before="",
            base_version="new",
            source_refs=source_refs,
            evidence=list(source.get("evidence") or []),
            reason=["user_explicitly_promoted_from_superseded_candidate"],
            confidence=source.get("confidence"),
            status="ready_for_review",
            restored_from_candidate_id=source_candidate_id,
            restored_from_content_hash=source.get("content_hash") or content_hash(content),
            restored_by=actor,
            restored_at=at,
            approval_note=note,
            source_strategy="replace",
            proposed_by=actor,
        )
        saved = store.save_candidate(candidate, content)
        append_agent_audit(
            store.skill_root,
            "candidate.restored_for_active",
            {
                "candidate_id": saved["candidate_id"],
                "concept": concept,
                "restored_from_candidate_id": source_candidate_id,
                "source_status": source_status,
                "actor": actor,
                "content_hash": saved.get("content_hash"),
                "source_refs": source_refs,
            },
        )
        return saved


def _main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Concept runner candidate administration")
    parser.add_argument("--skill-root", type=Path, required=True)
    parser.add_argument("--actor", default="zhujie14")
    sub = parser.add_subparsers(dest="command", required=True)

    supersede = sub.add_parser("supersede-refresh", help="mark a guarded refresh queue as not required")
    supersede.add_argument("--expected-count", type=int, required=True)
    supersede.add_argument("--apply", action="store_true")
    supersede.add_argument("--reason", default=SUPERSEDE_REASON)

    restore = sub.add_parser("restore-new-concept", help="create a new proposal from a terminal Candidate")
    restore.add_argument("--source-candidate", required=True)
    restore.add_argument("--content", type=Path, required=True)
    restore.add_argument("--note", default="")

    args = parser.parse_args(argv)
    # Keep dry-run planning available for historical inspection, but block all
    # operations that create/transition Candidates once the workflow is retired.
    if CONCEPT_REFRESH_DISABLED and (
        args.command == "restore-new-concept"
        or (args.command == "supersede-refresh" and args.apply)
    ):
        return emit_disabled(f"candidate-admin:{args.command}")
    skill_root = args.skill_root.expanduser().resolve()
    store = ConceptLearningStore(skill_root)
    if args.command == "supersede-refresh":
        plan = build_refresh_supersede_plan(store, expected_count=args.expected_count)
        if not args.apply:
            print(json.dumps(plan, ensure_ascii=False, indent=2))
            return 0
        result = apply_refresh_supersede_plan(store, plan, actor=args.actor, reason=args.reason)
        print(json.dumps({**result, "plan_count": plan["count"]}, ensure_ascii=False, indent=2))
        return 0 if result["conflict_count"] == 0 and result["applied_count"] == plan["count"] else 1
    if args.command == "restore-new-concept":
        saved = restore_new_concept_candidate(
            store,
            args.source_candidate,
            args.content,
            actor=args.actor,
            note=args.note,
        )
        print(json.dumps({"status": "ok", "candidate_id": saved.get("candidate_id"), "concept": saved.get("concept"), "state": saved.get("status")}, ensure_ascii=False))
        return 0
    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
