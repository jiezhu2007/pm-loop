#!/usr/bin/env python3
"""One-shot full Concept Learning Loop recheck.

It intentionally exits after proposing candidates.  Publishing is a separate
approved-action worker so a temporary Agent process can never bypass the Gate.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from concept_learning import ConceptLearningStore, discover_from_uris, now_iso
from concept_discovery_triage import propose as triage_discovery
from concept_signal_discovery import collect_signals, register_signals
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


def _read(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def collect_delta_evidence(codex_root: Path) -> Dict[str, str]:
    """Read URI revisions from both real sync-ledger schemas."""
    paths = [
        codex_root / "skills" / "shengsuan-sync" / "state" / "ledger.json",
        codex_root / "skills" / "databuilder-public-docs" / "state" / "ledger.json",
    ]
    evidence: Dict[str, str] = {}
    for path in paths:
        ledger = _read(path)
        for item in ledger.values():
            if not isinstance(item, dict):
                continue
            uri = item.get("target_uri") or item.get("viking_uri")
            if uri:
                revision = item.get("sha256") or item.get("publishTime") or item.get("fetched_at") or "unknown"
                evidence[str(uri)] = str(revision)
    return evidence


def collect_delta_uris(codex_root: Path) -> List[str]:
    """Backward-compatible URI-only view used by older callers."""
    return list(collect_delta_evidence(codex_root))


def configured_concepts(skill_root: Path) -> List[str]:
    try:
        import yaml

        config = yaml.safe_load((skill_root / "config.yaml").read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    names = [str(item.get("name")) for item in config.get("concepts", []) if isinstance(item, dict) and item.get("name")]
    ledger = _read(skill_root / "state" / "concepts-ledger.json")
    names.extend(
        str(name)
        for name, record in ledger.items()
        if isinstance(record, dict) and str(record.get("status") or "active") == "active"
    )
    return list(dict.fromkeys(names))


def run_refresh(skill_root: Path, concepts: Iterable[str], python: str) -> Dict[str, Any]:
    refresh = skill_root / "scripts" / "refresh.py"
    if not refresh.is_file():
        return {"status": "failed", "error": f"missing refresh script: {refresh}", "proposed": []}
    proposed: List[str] = []
    unchanged: List[str] = []
    failed: List[Dict[str, Any]] = []
    for name in concepts:
        result = subprocess.run([python, str(refresh), "--propose", name], cwd=str(skill_root), text=True, capture_output=True, check=False, timeout=900)
        output = (result.stderr or "") + "\n" + (result.stdout or "")
        if result.returncode == 0:
            proposed.append(name)
        elif "无命中" in output or "无法拉取文档" in output:
            unchanged.append(name)
        else:
            failed.append({"concept": name, "returncode": result.returncode, "error": output[-1000:] or "refresh failed"})
    return {"status": "ok" if not failed else "partial_failure", "proposed": proposed, "unchanged": unchanged, "failed": failed}


def _triage_should_stop(row: Mapping[str, Any]) -> bool:
    """Stop a bounded page loop once the current discovery run is terminal."""
    status = str(row.get("status") or "")
    triage_status = str(row.get("triage_status") or "")
    if status in {"triaged", "triage_no_candidate", "triage_blocked", "triage_failed"}:
        return True
    if triage_status in {"complete", "complete_with_unavailable", "blocked", "failed"}:
        return True
    remaining = row.get("triage_remaining")
    if remaining is None:
        return False
    try:
        return int(remaining) <= 0
    except (TypeError, ValueError):
        return False


def _triage_is_unsuccessful(row: Mapping[str, Any]) -> bool:
    """Return whether the final triage state must fail the recheck gate."""
    status = str(row.get("status") or "")
    triage_status = str(row.get("triage_status") or "")
    if status in {"triage_partial", "triage_blocked", "triage_failed"}:
        return True
    if triage_status in {"in_progress", "complete_with_unavailable", "blocked", "failed"}:
        return True
    remaining = row.get("triage_remaining")
    if remaining is not None:
        try:
            if int(remaining) > 0:
                return True
        except (TypeError, ValueError):
            return True
    return False


def _triage_gate_failed(rows: Iterable[Mapping[str, Any]]) -> bool:
    """Evaluate only the final page for each discovery run."""
    final: Dict[str, Mapping[str, Any]] = {}
    for index, row in enumerate(rows):
        key = str(row.get("run_id") or f"row-{index}")
        final[key] = row
    return any(_triage_is_unsuccessful(row) for row in final.values())


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="全量重检概念并发现未归类证据")
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    parser.add_argument("--concepts", nargs="*", help="只重检指定概念；省略则全量")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--result-path", type=Path, help="persist the machine-readable result for Control Plane verification")
    parser.add_argument("--skip-triage", action="store_true", help="only refresh existing concepts and register discovery inbox")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("concept-recheck")

    skill_root = args.codex_root.expanduser() / "skills" / "shengsuan-concepts"
    store = ConceptLearningStore(skill_root)
    concepts = args.concepts or configured_concepts(skill_root)
    refresh_result = run_refresh(skill_root, concepts, args.python)
    evidence = collect_delta_evidence(args.codex_root)
    discovery = discover_from_uris(
        store,
        evidence,
        source="full_recheck",
        evidence_revisions=evidence,
    )
    signal_items = collect_signals(args.codex_root.expanduser(), store, args.state_dir.expanduser() / "concept-review" / "manual-seeds.jsonl")
    signal_discovery = register_signals(store, signal_items)
    discoveries = [item for item in (discovery, signal_discovery) if isinstance(item, dict) and item.get("run_id")]
    triage_runs = []
    if not args.skip_triage:
        for item in discoveries:
            current = item
            # Triage is paged and resumable. Each batch persists processed_uris;
            # a retry therefore advances the same discovery run instead of
            # repeatedly asking the Agent about the first page.
            for _ in range(10):
                current = triage_discovery(store, current, args.codex_root.expanduser(), max_items=20)
                triage_runs.append(current)
                if _triage_should_stop(current):
                    break
    usage = store.usage_summary()
    result = {
        "schema_version": "concept-learning.recheck.v1",
        "run_id": "recheck-" + now_iso().replace(":", "").replace("-", ""),
        "finished_at": now_iso(),
        "concept_count": len(concepts),
        "refresh": refresh_result,
        "discovery_run_id": discovery.get("run_id"),
        "discovery_run_ids": [item.get("run_id") for item in discoveries],
        "unmatched_uri_count": len(discovery.get("unmatched_uris", [])),
        "signal_count": len(signal_items),
        "usage_event_count": usage.get("events", 0),
        "triage": triage_runs or [{"status": "skipped"}],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.result_path:
        args.result_path.parent.mkdir(parents=True, exist_ok=True)
        args.result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    # A run may contribute several intermediate pages.  Gate on the final
    # state per discovery run so an earlier ``in_progress`` page does not make
    # a later successful completion fail, while a bounded/partial final page
    # cannot be mistaken for success.
    triage_failed = _triage_gate_failed(triage_runs)
    return 0 if refresh_result.get("status") == "ok" and not triage_failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
