#!/usr/bin/env python3
"""Collect private PM usage signals that may justify a new concept."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List
import argparse

from concept_learning import ConceptLearningStore, discover_from_uris
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


def _jsonl(path: Path, limit: int) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _known_terms(store: ConceptLearningStore) -> set[str]:
    terms = {str(name).casefold() for name in store.load_ledger()}
    for candidate in store.list_candidates():
        terms.add(str(candidate.get("concept") or "").casefold())
        for alias in candidate.get("aliases") or []:
            terms.add(str(alias).casefold())
    return {term for term in terms if term}


def _ref(source: str, term: str, context: str) -> str:
    digest = hashlib.sha256(f"{source}\0{term}\0{context}".encode("utf-8")).hexdigest()[:20]
    return f"signal://{source}/{digest}"


def collect_signals(codex_root: Path, store: ConceptLearningStore, manual_path: Path | None = None) -> List[Dict[str, Any]]:
    known = _known_terms(store)
    items: List[Dict[str, Any]] = []
    timeline_paths = sorted((codex_root / "skills" / "pm-timeline" / "state" / "timeline").glob("*.jsonl"))
    for row in _jsonl(timeline_paths[-1], 300) if timeline_paths else []:
        context = "；".join(filter(None, [str(row.get("topic") or ""), str(row.get("conclusion") or "")]))[:1800]
        for term in row.get("concepts") or []:
            name = str(term).strip()
            if not name or name.casefold() in known:
                continue
            ref = _ref("pm-timeline", name, context)
            items.append({"uri": ref, "term": name, "source": "pm_timeline", "text": f"候选术语：{name}\n上下文：{context}\n原始文档：{row.get('doc') or '—'}"})

    assessment_path = codex_root / "skills" / "requirement-fit-assessment" / "state" / "index.jsonl"
    assessment_rows = _jsonl(assessment_path, 2000)
    counts = Counter(str(row.get("category") or "").strip() for row in assessment_rows if row.get("category"))
    for term, count in counts.most_common(30):
        if count < 3 or term.casefold() in known:
            continue
        examples = [str(row.get("capability") or "")[:400] for row in assessment_rows if str(row.get("category") or "").strip() == term][:3]
        context = f"近 {len(assessment_rows)} 条评估中出现 {count} 次；示例：" + "；".join(examples)
        ref = _ref("requirement-fit", term, context)
        items.append({"uri": ref, "term": term, "source": "requirement_term", "text": f"候选能力分类：{term}\n{context}"})

    if manual_path:
        for row in _jsonl(manual_path, 200):
            term = str(row.get("term") or "").strip()
            if not term or term.casefold() in known:
                continue
            context = str(row.get("context") or "")[:2000]
            ref = _ref("manual", term, context)
            items.append({"uri": ref, "term": term, "source": "manual_seed", "text": f"本人提议：{term}\n上下文：{context}", "source_refs": row.get("source_refs") or []})

    unique: Dict[str, Dict[str, Any]] = {}
    for item in items:
        unique.setdefault(str(item["uri"]), item)
    return list(unique.values())


def register_signals(store: ConceptLearningStore, items: Iterable[Dict[str, Any]]) -> Dict[str, Any] | None:
    rows = [dict(item) for item in items if item.get("uri") and item.get("text")]
    if not rows:
        return None
    revisions = {str(item["uri"]): hashlib.sha256(str(item["text"]).encode("utf-8")).hexdigest() for item in rows}
    run = discover_from_uris(store, revisions.keys(), source="pm_usage_signals", evidence_revisions=revisions)
    if not run.get("run_id"):
        return run
    selected = [item for item in rows if str(item["uri"]) in set(run.get("unmatched_uris") or [])]
    if not selected and run.get("evidence_items"):
        return run
    return store.update_discovery_run(str(run["run_id"]), evidence_items=selected, signal_sources=sorted({str(item.get("source")) for item in selected}))


def main() -> int:
    parser = argparse.ArgumentParser(description="登记 PM timeline、需求评估和本人手工信号")
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--manual-path", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    # Signal registration creates discovery runs; it is part of the retired
    # refresh path even though signal collection itself is read-only.
    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("signal-discovery")
    root = args.codex_root.expanduser()
    store = ConceptLearningStore(root / "skills" / "shengsuan-concepts")
    items = collect_signals(root, store, args.manual_path.expanduser() if args.manual_path else None)
    result = register_signals(store, items) or {"status": "no_signal", "signal_count": 0}
    result = dict(result)
    result["signal_count"] = len(items)
    print(json.dumps(result, ensure_ascii=False, indent=2) if args.json else f"signals={len(items)} run={result.get('run_id', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
