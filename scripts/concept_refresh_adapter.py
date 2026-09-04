#!/usr/bin/env python3
"""Safe proposal/publish adapter for the local shengsuan-concepts skill.

The historical ``refresh.py`` implementation combines evidence collection and
publication.  This adapter keeps those phases separate while reusing the
skill's real OpenViking search, prompt, and page helpers.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from concept_learning import ConceptLearningStore, make_candidate, now_iso  # noqa: E402
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled  # noqa: E402


PROPOSAL_REUSE_STATUSES = {"ready_for_review", "paused", "approved", "publishing", "queued", "publish_failed"}
PROPOSAL_PROTECTED_STATUSES = {"approved", "publishing", "queued", "publish_failed"}
PROPOSAL_SUPERSEDE_STATUSES = {"ready_for_review", "paused", "changes_requested", "stale", "failed"}
PROPOSAL_TERMINAL_STATUSES = {"published", "rejected", "superseded"}
AUDIT_LOG_NAME = "concept-agent-audit.jsonl"


def _env_positive_int(name: str, default: int, *, maximum: Optional[int] = None) -> int:
    """Read a worker setting defensively; malformed launchd env must not abort startup."""
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    value = max(1, value)
    return min(value, maximum) if maximum is not None else value


def _env_positive_float(
    name: str,
    default: float,
    *,
    minimum: float = 0.1,
    maximum: Optional[float] = None,
) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    value = max(minimum, value)
    return min(value, maximum) if maximum is not None else value


def append_agent_audit(skill_root: Path, event: str, payload: Dict[str, Any]) -> None:
    """Append a concept-owned audit event independent of Control Plane."""
    log_dir = skill_root / "state" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    row = {"at": now_iso(), "event": event, "runtime": "codex", **payload}
    with (log_dir / AUDIT_LOG_NAME).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _skill_modules(skill_root: Path):
    scripts = str(skill_root / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    import lib_frontmatter as fm  # type: ignore
    import lib_pages  # type: ignore
    import ov_search  # type: ignore
    from llm_runner import run_prompt  # type: ignore

    return fm, lib_pages, ov_search, run_prompt


def load_config(skill_root: Path) -> Dict[str, Any]:
    import yaml

    value = yaml.safe_load((skill_root / "config.yaml").read_text(encoding="utf-8")) or {}
    return value if isinstance(value, dict) else {}


def load_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def proposal_fingerprint(
    *,
    concept: str,
    kind: str,
    base_version: str,
    base_page_sha256: Optional[str],
    base_ledger_last_updated: Optional[str],
    source_snapshot: Iterable[Dict[str, Any]],
) -> str:
    sources = sorted(
        {
            (str(item.get("uri") or ""), str(item.get("sha256") or ""))
            for item in source_snapshot
            if isinstance(item, dict) and item.get("uri") and item.get("sha256")
        }
    )
    payload = {
        "concept": concept,
        "kind": kind,
        "base_version": base_version,
        "base_page_sha256": base_page_sha256,
        "base_ledger_last_updated": base_ledger_last_updated,
        "sources": sources,
    }
    return sha256_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def candidate_fingerprint(candidate: Dict[str, Any]) -> Optional[str]:
    stored = str(candidate.get("proposal_fingerprint") or "").strip()
    if stored:
        return stored
    snapshot = candidate.get("source_snapshot")
    if not isinstance(snapshot, list) or not snapshot:
        return None
    return proposal_fingerprint(
        concept=str(candidate.get("concept") or ""),
        kind=str(candidate.get("kind") or ""),
        base_version=str(candidate.get("base_version") or "unversioned"),
        base_page_sha256=candidate.get("base_page_sha256"),
        base_ledger_last_updated=candidate.get("base_ledger_last_updated"),
        source_snapshot=snapshot,
    )


def current_proposal_decision(
    store: ConceptLearningStore,
    concept: str,
    fingerprint: str,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    candidates = [
        candidate
        for candidate in store.list_candidates(concept=concept)
        if str(candidate.get("status") or "") not in PROPOSAL_TERMINAL_STATUSES
    ]
    # Any approved/in-flight proposal wins over file recency. A legacy race may
    # have left a newer ready row beside an older approved row; generating again
    # in that shape would create two competing publication paths.
    for candidate in candidates:
        if str(candidate.get("status") or "") in PROPOSAL_PROTECTED_STATUSES:
            return "defer", candidate
    for candidate in candidates:
        if (
            str(candidate.get("status") or "") in PROPOSAL_REUSE_STATUSES
            and candidate_fingerprint(candidate) == fingerprint
        ):
            return "reuse", candidate
    return "generate", candidates[0] if candidates else None


def supersede_older_proposals(
    store: ConceptLearningStore,
    concept: str,
    keep_candidate_id: str,
) -> None:
    for candidate in store.list_candidates(concept=concept):
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id or candidate_id == keep_candidate_id:
            continue
        current_status = str(candidate.get("status") or "")
        if current_status not in PROPOSAL_SUPERSEDE_STATUSES:
            continue
        try:
            store.update_candidate(
                candidate_id,
                expected_statuses={current_status},
                status="superseded",
                superseded_by=keep_candidate_id,
                superseded_at=now_iso(),
            )
        except ValueError:
            continue


def reuse_proposal(
    store: ConceptLearningStore,
    candidate: Dict[str, Any],
    *,
    decision: str,
) -> Dict[str, Any]:
    saved = dict(candidate)
    saved["deduplicated"] = decision == "reuse"
    saved["deferred"] = decision == "defer"
    if decision == "reuse":
        supersede_older_proposals(store, str(saved.get("concept") or ""), str(saved["candidate_id"]))
    return saved


def concept_config(config: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for item in config.get("concepts", []):
        if isinstance(item, dict) and item.get("name") == name:
            return item
    return None


def candidate_concept_config(store: ConceptLearningStore, name: str) -> Optional[Dict[str, Any]]:
    """Use an existing Candidate as the identity seed for another evidence pass."""
    candidates = store.list_candidates(concept=name)
    if not candidates:
        return None
    candidate = candidates[0]
    aliases = candidate.get("aliases") if isinstance(candidate.get("aliases"), list) else []
    keywords = candidate.get("search_keywords") if isinstance(candidate.get("search_keywords"), list) else []
    return {
        "name": name,
        "category": candidate.get("category") or "未分类",
        "aliases": aliases,
        "search_keywords": list(dict.fromkeys([name, *aliases, *keywords])),
    }


def fetch_documents(
    ov_search: Any,
    hits: List[Dict[str, Any]],
    max_chars: int,
    *,
    fetch_jobs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    outcomes = fetch_document_outcomes(
        ov_search,
        hits,
        max_chars,
        fetch_jobs=fetch_jobs,
    )
    return [item["document"] for item in outcomes if item.get("status") == "available"]


def fetch_document_outcomes(
    ov_search: Any,
    hits: List[Dict[str, Any]],
    max_chars: int,
    *,
    fetch_jobs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Read evidence bodies with bounded concurrency while preserving hit order.

    ``read_content`` already applies the per-request network timeout. The
    batch deadline is an independent safety net. Every attempted URI gets an
    explicit outcome so timed-out or failed reads remain auditable even though
    only available documents are sent to the proposal model.
    """
    if fetch_jobs is None:
        fetch_jobs = _env_positive_int("CONCEPTS_DOC_FETCH_JOBS", 4, maximum=8)
    fetch_jobs = max(1, fetch_jobs)

    def read_one(hit: Dict[str, Any]) -> Dict[str, Any]:
        uri = str(hit.get("uri") or "")
        try:
            content = ov_search.read_content(uri)
        except Exception as exc:
            return {
                "uri": uri,
                "status": "unavailable",
                "error": type(exc).__name__,
            }
        if not content:
            return {
                "uri": uri,
                "status": "unavailable",
                "error": "unreadable_or_empty",
            }
        truncated = len(content) > max_chars
        if truncated:
            content = content[:max_chars] + "\n\n[... 截断 ...]"
        return {
            "uri": uri,
            "status": "available",
            "document": {
                "uri": uri,
                "source": hit.get("source"),
                "score": hit.get("score"),
                "query": hit.get("query"),
                "content": content,
                "truncated": truncated,
                "content_sha256": sha256_text(content),
            },
        }

    if not hits:
        return []

    # Consume whichever evidence read finishes first. ``executor.map`` would
    # add head-of-line blocking. Keep the returned outcomes in hit order.
    pool = ThreadPoolExecutor(
        max_workers=min(fetch_jobs, len(hits)),
        thread_name_prefix="concept-evidence",
    )
    futures = {pool.submit(read_one, hit): index for index, hit in enumerate(hits)}
    pending = set(futures)
    results_by_index: Dict[int, Dict[str, Any]] = {}
    deadline = time.monotonic() + _env_positive_float(
        "CONCEPTS_DOC_FETCH_BATCH_TIMEOUT", 120
    )
    try:
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            for future in done:
                index = futures[future]
                try:
                    results_by_index[index] = future.result()
                except Exception as exc:
                    results_by_index[index] = {
                        "uri": str(hits[index].get("uri") or ""),
                        "status": "unavailable",
                        "error": type(exc).__name__,
                    }
            if not done:
                break
    finally:
        for future in pending:
            future.cancel()
            index = futures[future]
            results_by_index[index] = {
                "uri": str(hits[index].get("uri") or ""),
                "status": "unavailable",
                "error": "batch_timeout",
            }
        # A foreign worker can ignore its request timeout. Waiting here would
        # silently turn the batch deadline back into an unbounded join.
        pool.shutdown(wait=False, cancel_futures=True)
    return [results_by_index[index] for index in range(len(hits))]


def _compact_text(value: str, limit: int, *, marker: str = "evidence") -> str:
    """Keep both ends of text while bounding retry input."""
    text = str(value or "")
    if len(text) <= limit:
        return text
    if limit <= 80:
        return text[:limit]
    marker = f"\n\n[... {marker} compacted for retry ...]\n\n"
    available = max(1, limit - len(marker))
    head = max(1, int(available * 0.7))
    tail = max(1, available - head)
    return text[:head] + marker + text[-tail:]


def _compact_existing_page(value: str, limit: int) -> str:
    """Compact an Active page while retaining frontmatter and section headings."""
    text = str(value or "")
    if len(text) <= limit:
        return text
    if limit <= 160:
        return _compact_text(text, limit, marker="existing page")

    # Keep YAML frontmatter intact whenever possible: it carries source and
    # version metadata that the model needs to make a safe incremental edit.
    frontmatter = ""
    body = text
    if text.startswith("---\n"):
        end = text.find("\n---", 4)
        if end >= 0:
            end += len("\n---")
            frontmatter = text[:end]
            body = text[end:]

    body_budget = max(80, limit - len(frontmatter) - 64)
    lines = body.splitlines()
    headings = [index for index, line in enumerate(lines) if line.lstrip().startswith("#")]
    if not headings:
        compacted = _compact_text(body, body_budget, marker="existing page")
        return (frontmatter + compacted)[:limit]

    # Preserve every section heading and distribute the remaining budget so a
    # large page cannot crowd all useful context out with its first section.
    heading_budget = min(body_budget, max(0, len(headings) * 24))
    content_budget = max(40, body_budget - heading_budget)
    per_section = max(40, content_budget // len(headings))
    chunks: List[str] = []
    for position, start in enumerate(headings):
        stop = headings[position + 1] if position + 1 < len(headings) else len(lines)
        section = "\n".join(lines[start:stop]).strip()
        chunks.append(_compact_text(section, per_section, marker="existing section"))
    compacted_body = "\n\n".join(chunks)
    result = frontmatter + "\n\n[... existing page compacted for retry ...]\n\n" + compacted_body
    if len(result) > limit:
        # The fallback still keeps the frontmatter and section headings at the
        # front of the bounded representation.
        result = frontmatter + "\n\n" + _compact_text(compacted_body, max(40, limit - len(frontmatter) - 2), marker="existing page")
    return result[:limit]


def _render_incremental_prompt(
    template: str,
    name: str,
    existing_page: str,
    docs: List[Dict[str, Any]],
    *,
    evidence_chars: Optional[int] = None,
    prompt_chars: Optional[int] = None,
) -> str:
    """Build an incremental prompt with optional total-character budgeting.

    The retry budget applies to the complete prompt, including the existing
    Active page and all evidence labels/bodies.  This matters because an old
    page can be much larger than the newly fetched evidence.
    """
    fixed = template.replace("{concept_name}", name).replace("{existing_page}", "").replace("{new_documents}", "")
    bounded_existing = str(existing_page or "")
    per_document = evidence_chars
    if prompt_chars is not None:
        target = max(512, int(prompt_chars))
        available = max(256, target - len(fixed))
        existing_limit = min(len(bounded_existing), max(512, int(available * 0.45)))
        if len(bounded_existing) > existing_limit:
            bounded_existing = _compact_existing_page(bounded_existing, existing_limit)
        evidence_budget = max(160, available - len(bounded_existing))
        per_document = min(
            evidence_chars if evidence_chars is not None else evidence_budget,
            max(80, evidence_budget // max(1, len(docs))),
        )
    prompt = template.replace("{concept_name}", name).replace("{existing_page}", bounded_existing)
    sections = []
    for index, doc in enumerate(docs, 1):
        content = str(doc.get("content") or "")
        if per_document is not None:
            content = _compact_text(content, per_document)
        sections.append(
            f"\n### 文档 {index} (来源: {doc.get('source')}, URI: {doc['uri']})\n\n"
            f"{content}\n\n---\n"
        )
    result = prompt.replace("{new_documents}", "".join(sections))
    if prompt_chars is not None and len(result) > int(prompt_chars):
        # Labels and frontmatter are more valuable than long evidence tails;
        # shrink the evidence once more to make the total bound deterministic.
        overflow = len(result) - int(prompt_chars)
        current = per_document or 80
        reduced = max(80, current - (overflow // max(1, len(docs)) + 1))
        if reduced < current:
            return _render_incremental_prompt(
                template,
                name,
                bounded_existing,
                docs,
                evidence_chars=reduced,
                prompt_chars=prompt_chars,
            )
        # An unusually large template/frontmatter cannot be reduced safely;
        # keep the metadata and return the smallest truthful representation.
        result = result[: int(prompt_chars)]
    return result


def _run_incremental_prompt(
    prompt: str,
    *,
    template: str,
    name: str,
    existing_page: str,
    docs: List[Dict[str, Any]],
    run_prompt: Any,
) -> Tuple[Any, str]:
    """Run a full prompt once, then one bounded compact retry on timeout."""
    full_timeout = _env_positive_int("CONCEPTS_LLM_TIMEOUT", 600)
    result, output = run_prompt(prompt, full_timeout)
    if result.returncode != 124:
        return result, output

    retry_timeout = _env_positive_float(
        "CONCEPTS_LLM_RETRY_TIMEOUT",
        60,
        minimum=1,
        maximum=60,
    )
    retry_evidence_chars = _env_positive_int(
        "CONCEPTS_LLM_RETRY_DOC_CHARS",
        1800,
        maximum=4000,
    )
    retry_prompt_chars = _env_positive_int(
        "CONCEPTS_LLM_RETRY_PROMPT_CHARS",
        12000,
        maximum=24000,
    )
    retry_prompt = _render_incremental_prompt(
        template,
        name,
        existing_page,
        docs,
        evidence_chars=retry_evidence_chars,
        prompt_chars=retry_prompt_chars,
    )
    print(
        "   ⏱️ 完整 prompt 超时，使用压缩证据重试 "
        f"(timeout={retry_timeout:g}s, prompt_chars={len(retry_prompt)}, "
        f"evidence_chars={retry_evidence_chars})",
        file=sys.stderr,
    )
    return run_prompt(
        retry_prompt,
        retry_timeout,
        reasoning_effort="low",
    )


def compile_content(
    *,
    skill_root: Path,
    config: Dict[str, Any],
    concept: Dict[str, Any],
    existing_page: str,
    docs: List[Dict[str, Any]],
    fm: Any,
    run_prompt: Any,
) -> Tuple[str, str]:
    """Return (content, prompt_mode) without any Active writes."""
    name = str(concept["name"])
    if existing_page:
        template = load_prompt(skill_root / "prompts" / "incremental-update.md")
        mode = "incremental"
        prompt = _render_incremental_prompt(template, name, existing_page, docs)
    else:
        # bootstrap's compiler is safe to reuse here because compile_with_llm
        # only writes its cache; upload_concept/compile_one are intentionally
        # never called in proposal mode.
        from bootstrap import compile_with_llm, load_prompt_template  # type: ignore

        mode = "bootstrap"
        prompt = load_prompt_template()
        return compile_with_llm(concept, docs, prompt), mode
    result, output = _run_incremental_prompt(
        prompt,
        template=template,
        name=name,
        existing_page=existing_page,
        docs=docs,
        run_prompt=run_prompt,
    )
    if result.returncode != 0:
        raise RuntimeError(f"LLM exited with code {result.returncode}: {(result.stderr or result.stdout or '')[-500:]}")
    return fm.sanitize_llm_output(output.strip()), mode


def propose_one(skill_root: Path, name: str, run_id: Optional[str] = None) -> Dict[str, Any]:
    store = ConceptLearningStore(skill_root)
    with store.concept_lock(name):
        return _propose_one_locked(skill_root, name, run_id=run_id, store=store)


def propose_file(
    skill_root: Path,
    name: str,
    content_file: Path,
    *,
    actor: str = "zhujie14",
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create an auditable correction Candidate from a reviewed Markdown file.

    This path is intentionally proposal-only. It captures the current Active
    page hash and replaces the evidence list only after the same human approval
    and publication checks used by generated Candidates.
    """
    store = ConceptLearningStore(skill_root)
    with store.concept_lock(name):
        ledger = store.load_ledger()
        record = ledger.get(name) if isinstance(ledger.get(name), dict) else None
        if not record or str(record.get("status") or "active") != "active":
            raise ValueError(f"manual correction requires an Active concept: {name}")
        page_path = skill_root / "state" / "pages" / f"{name}.md"
        if not page_path.is_file():
            raise FileNotFoundError(f"active concept page missing: {page_path}")
        source_path = content_file.expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"correction content missing: {source_path}")
        content = source_path.read_text(encoding="utf-8").strip() + "\n"
        if not content.startswith("---\n"):
            raise ValueError("correction content must start with YAML frontmatter")

        import yaml

        parts = content.split("---", 2)
        if len(parts) != 3:
            raise ValueError("correction content has invalid YAML frontmatter")
        metadata = yaml.safe_load(parts[1]) or {}
        if not isinstance(metadata, dict) or str(metadata.get("concept") or "") != name:
            raise ValueError("correction frontmatter concept does not match the requested concept")
        source_refs = metadata.get("sources")
        if not isinstance(source_refs, list) or not source_refs:
            raise ValueError("correction content requires at least one evidence source")
        normalized_sources = list(
            dict.fromkeys(str(uri).strip() for uri in source_refs if str(uri).strip())
        )
        if not normalized_sources or any(not uri.startswith("viking://") for uri in normalized_sources):
            raise ValueError("correction sources must be non-empty viking:// URIs")
        required_sections = [
            "## 定义",
            "## 能力边界（能做什么）",
            "## 已知限制（不能做什么/需定制）",
            "## 版本演进",
            "## 关联概念",
            "## 出现过的客户/评估",
        ]
        missing_sections = [section for section in required_sections if section not in content]
        if missing_sections:
            raise ValueError(f"correction content missing sections: {', '.join(missing_sections)}")

        old_content = page_path.read_text(encoding="utf-8")
        if content == old_content:
            raise ValueError("correction content is identical to the Active page")
        base_version = str(record.get("current_version") or record.get("latest_version") or "v0")
        base_page_sha256 = sha256_text(old_content)
        fingerprint = sha256_text(
            json.dumps(
                {
                    "concept": name,
                    "kind": "correction",
                    "base_version": base_version,
                    "base_page_sha256": base_page_sha256,
                    "content_hash": sha256_text(content),
                    "sources": normalized_sources,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        decision, current = current_proposal_decision(store, name, fingerprint)
        if current is not None and decision in {"reuse", "defer"}:
            return reuse_proposal(store, current, decision=decision)

        candidate = make_candidate(
            concept=name,
            kind="correction",
            content=content,
            before=old_content,
            base_version=base_version,
            source_refs=normalized_sources,
            evidence=[{"uri": uri, "source": "reviewed-correction"} for uri in normalized_sources],
            reason=["remove_unsupported_or_stale_content"],
            confidence=1.0,
            run_id=run_id,
            base_page_sha256=base_page_sha256,
            base_ledger_last_updated=record.get("last_updated"),
            proposal_fingerprint=fingerprint,
            prompt_mode="reviewed-file",
            source_strategy="replace",
            proposed_by=actor,
            proposed_from=str(source_path),
            status="ready_for_review",
        )
        saved = store.save_candidate(candidate, content)
        supersede_older_proposals(store, name, str(saved["candidate_id"]))
        append_agent_audit(
            skill_root,
            "candidate.created",
            {
                "candidate_id": saved["candidate_id"],
                "concept": name,
                "kind": saved["kind"],
                "content_hash": saved.get("content_hash"),
                "source_refs": normalized_sources,
                "source_strategy": "replace",
                "proposed_by": actor,
                "run_id": run_id,
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "candidate_id": saved["candidate_id"],
                    "concept": name,
                    "kind": saved["kind"],
                    "confidence": saved.get("confidence"),
                },
                ensure_ascii=False,
            )
        )
        return saved


def _propose_one_locked(
    skill_root: Path,
    name: str,
    *,
    run_id: Optional[str],
    store: ConceptLearningStore,
) -> Dict[str, Any]:
    config = load_config(skill_root)
    ledger_path = skill_root / "state" / "concepts-ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8")) if ledger_path.is_file() else {}
    ledger = ledger if isinstance(ledger, dict) else {}
    concept = concept_config(config, name)
    if not concept and isinstance(ledger.get(name), dict):
        record = ledger[name]
        concept = {
            "name": name,
            "category": record.get("category") or "未分类",
            "aliases": [],
            "search_keywords": [name],
        }
    if not concept:
        concept = candidate_concept_config(store, name)
    if not concept:
        raise ValueError(f"concept not found in config.yaml, Active ledger, or Candidate store: {name}")
    record = ledger.get(name) if isinstance(ledger.get(name), dict) else {}
    fm, lib_pages, ov_search, run_prompt = _skill_modules(skill_root)
    settings = config.get("settings") or {}
    keywords = concept.get("search_keywords", [name])
    hits = ov_search.search_concept(
        keywords=keywords,
        targets=[target for target in settings.get("search_targets", []) if "concepts" not in target],
        limit_per_query=5,
        threshold=settings.get("score_threshold", 0.55),
        max_docs=settings.get("max_docs_per_concept", 10),
        exclude_concepts=True,
    )
    if not hits:
        raise RuntimeError("no evidence hits")
    fetch_outcomes = fetch_document_outcomes(
        ov_search,
        hits,
        int(settings.get("max_chars_per_doc", 8000)),
    )
    docs = [item["document"] for item in fetch_outcomes if item.get("status") == "available"]
    unavailable_uris = [
        str(item.get("uri") or "")
        for item in fetch_outcomes
        if item.get("status") != "available" and item.get("uri")
    ]
    fetch_audit = [
        {
            key: item[key]
            for key in ("uri", "status", "error")
            if key in item
        }
        for item in fetch_outcomes
    ]
    append_agent_audit(
        skill_root,
        "evidence.fetch.completed",
        {
            "concept": name,
            "run_id": run_id,
            "attempted": len(fetch_outcomes),
            "available": len(docs),
            "unavailable_uris": unavailable_uris,
            "outcomes": fetch_audit,
        },
    )
    if not docs:
        raise RuntimeError("evidence documents could not be read")
    existing_page = lib_pages.read_page(name, str(record.get("viking_uri") or "")) if record else ""
    current_version = record.get("current_version") or record.get("latest_version") or "v0"
    kind = "refresh" if existing_page else "new-concept"
    base_page_sha256 = sha256_text(existing_page) if existing_page else None
    source_snapshot = [
        {"uri": doc["uri"], "sha256": doc["content_sha256"], "fetched_at": now_iso()}
        for doc in docs
    ]
    fingerprint = proposal_fingerprint(
        concept=name,
        kind=kind,
        base_version=current_version,
        base_page_sha256=base_page_sha256,
        base_ledger_last_updated=record.get("last_updated"),
        source_snapshot=source_snapshot,
    )
    decision, current = current_proposal_decision(store, name, fingerprint)
    if current is not None and decision in {"reuse", "defer"}:
        return reuse_proposal(store, current, decision=decision)

    content, mode = compile_content(skill_root=skill_root, config=config, concept=concept, existing_page=existing_page, docs=docs, fm=fm, run_prompt=run_prompt)
    if not content or not content.startswith("---"):
        raise RuntimeError("LLM output is not a concept page with frontmatter")
    # A manual refresh can race the weekly worker. Recheck after the expensive
    # LLM call so identical input still persists only one Candidate.
    decision, current = current_proposal_decision(store, name, fingerprint)
    if current is not None and decision in {"reuse", "defer"}:
        return reuse_proposal(store, current, decision=decision)

    candidate = make_candidate(
        concept=name,
        kind=kind,
        content=content,
        before=existing_page,
        base_version=current_version,
        source_refs=[hit["uri"] for hit in hits],
        evidence=[{"uri": doc["uri"], "source": doc.get("source"), "score": doc.get("score"), "quote": doc["content"][:500], "content_sha256": doc["content_sha256"], "truncated": doc.get("truncated", False)} for doc in docs],
        reason=["source_changed" if existing_page else "new_concept_evidence"],
        confidence=round(sum(float(hit.get("score") or 0) for hit in hits) / len(hits), 4) if hits else None,
        run_id=run_id,
        base_page_sha256=base_page_sha256,
        base_ledger_last_updated=record.get("last_updated"),
        source_snapshot=source_snapshot,
        evidence_fetch=fetch_audit,
        unavailable_uris=unavailable_uris,
        proposal_fingerprint=fingerprint,
        prompt_mode=mode,
        status="ready_for_review",
    )
    saved = store.save_candidate(candidate, content)
    supersede_older_proposals(store, name, str(saved["candidate_id"]))
    append_agent_audit(
        skill_root,
        "candidate.created",
        {
            "candidate_id": saved["candidate_id"],
            "concept": name,
            "kind": saved["kind"],
            "content_hash": saved.get("content_hash"),
            "source_refs": saved.get("source_refs") or [],
            "run_id": run_id,
        },
    )
    return saved


def _atomic_json(path: Path, value: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def approve_one(
    skill_root: Path,
    candidate_id: str,
    *,
    actor: str = "zhujie14",
    note: str = "",
) -> Dict[str, Any]:
    """Record an explicit human approval in the concept-owned Candidate store."""
    store = ConceptLearningStore(skill_root)
    candidate = store.read_candidate(candidate_id)
    concept = str(candidate.get("concept") or "")
    if not concept:
        raise ValueError("candidate has no concept")
    with store.concept_lock(concept):
        candidate = store.read_candidate(candidate_id)
        status = str(candidate.get("status") or "")
        if status == "approved" and candidate.get("approved_by") == actor:
            return candidate
        if status not in {"ready_for_review", "paused", "changes_requested"}:
            raise ValueError(f"candidate cannot be approved from status: {status}")
        if not (candidate.get("source_refs") or candidate.get("evidence")):
            raise ValueError("candidate cannot be approved without evidence sources")
        content_path = Path(str(candidate.get("content_path") or store.content_path(candidate_id)))
        if not content_path.is_absolute():
            content_path = skill_root / content_path
        if not content_path.is_file():
            raise FileNotFoundError(f"candidate content missing: {content_path}")
        approved_hash = sha256_text(content_path.read_text(encoding="utf-8"))
        if approved_hash != candidate.get("content_hash"):
            raise RuntimeError("candidate content no longer matches its proposal hash")
        approved = store.update_candidate(
            candidate_id,
            expected_statuses={status},
            status="approved",
            approved_by=actor,
            approved_at=now_iso(),
            approved_content_hash=approved_hash,
            approval_note=note,
            approval_source="shengsuan-concepts-cli",
        )
        append_agent_audit(
            skill_root,
            "candidate.approved",
            {
                "candidate_id": candidate_id,
                "concept": concept,
                "approved_by": actor,
                "approved_content_hash": approved_hash,
                "note": note,
            },
        )
        return approved


def publish_one(skill_root: Path, candidate_id: str, actor: str = "zhujie14") -> Dict[str, Any]:
    store = ConceptLearningStore(skill_root)
    candidate = store.read_candidate(candidate_id)
    concept = str(candidate.get("concept") or "")
    if not concept:
        raise ValueError("candidate has no concept")
    with store.concept_lock(concept):
        with store.ledger_lock():
            return _publish_one_locked(skill_root, candidate_id, actor=actor, store=store)


def _publish_one_locked(
    skill_root: Path,
    candidate_id: str,
    *,
    actor: str,
    store: ConceptLearningStore,
) -> Dict[str, Any]:
    candidate = store.read_candidate(candidate_id)
    if candidate.get("status") == "published":
        return candidate
    # Publication is an approved action.  Requiring both the state transition
    # and a durable reviewer identity prevents a direct CLI/worker invocation
    # from turning a proposal into Active without the human gate.
    if str(candidate.get("approved_by") or "").strip() != "zhujie14":
        raise ValueError("candidate has no valid human approval from zhujie14")
    status = str(candidate.get("status") or "")
    concept = str(candidate["concept"])
    page_path = skill_root / "state" / "pages" / f"{concept}.md"
    page_exists = page_path.is_file()
    old_content = page_path.read_text(encoding="utf-8") if page_exists else ""
    ledger = store.load_ledger()
    record = dict(ledger.get(concept) or {})

    # The durable Active commit precedes the Candidate projection.  If a crash
    # lost only that final projection write, reconcile it without uploading or
    # incrementing the version a second time.
    approved_hash = str(candidate.get("approved_content_hash") or "")
    active_commit_matches = (
        status in {"approved", "publishing", "publish_failed"}
        and approved_hash
        and record.get("last_candidate_id") == candidate_id
        and page_exists
        and sha256_text(old_content) == approved_hash
        and bool(record.get("current_version"))
    )
    if active_commit_matches:
        return store.update_candidate(
            candidate_id,
            expected_statuses={status},
            status="published",
            published_at=candidate.get("published_at") or now_iso(),
            published_by=actor,
            proposed_version=record.get("current_version"),
            published_uri=record.get("viking_uri"),
            recovered_from_active_commit=True,
        )

    if status == "approved":
        candidate = store.update_candidate(
            candidate_id,
            expected_statuses={"approved"},
            status="publishing",
            publishing_at=now_iso(),
        )
    elif status != "publishing":
        raise ValueError(f"candidate is not publishable: {status}")
    content_path = Path(str(candidate.get("content_path") or store.content_path(candidate_id)))
    if not content_path.is_absolute():
        content_path = skill_root / content_path
    if not content_path.is_file():
        raise FileNotFoundError(f"candidate content missing: {content_path}")
    content = content_path.read_text(encoding="utf-8")
    actual_content_hash = sha256_text(content)
    if actual_content_hash != candidate.get("content_hash"):
        store.update_candidate(candidate_id, expected_statuses={"publishing"}, status="stale", error="candidate content changed after proposal")
        raise RuntimeError("candidate content no longer matches its proposal hash")
    if actual_content_hash != candidate.get("approved_content_hash"):
        store.update_candidate(candidate_id, expected_statuses={"publishing"}, status="stale", error="candidate content changed after approval")
        raise RuntimeError("candidate content no longer matches the approved snapshot")
    expected = candidate.get("base_page_sha256")
    # A new-concept proposal has no Active page and therefore no base hash.
    # If a page appears before publication, treating it as an empty base would
    # overwrite concurrent work; classify the candidate as stale instead.
    base_changed = page_exists if expected is None else sha256_text(old_content) != expected
    if base_changed:
        store.update_candidate(candidate_id, expected_statuses={"publishing"}, status="stale", error="active page changed after proposal")
        raise RuntimeError("candidate base is stale; regenerate evidence before publishing")
    old_ledger = dict(ledger)
    history_dir = skill_root / "state" / "history" / concept
    history_dir.mkdir(parents=True, exist_ok=True)
    current_version = record.get("current_version") or record.get("latest_version") or "v0"
    try:
        version_number = int(str(current_version).lstrip("v")) + 1
    except ValueError:
        version_number = len(list(history_dir.glob("*.md"))) + 1
    new_version = f"v{version_number}"
    if old_content:
        (history_dir / f"{current_version}.md").write_text(old_content, encoding="utf-8")
    fm, lib_pages, _ov_search, _run_prompt = _skill_modules(skill_root)
    namespace = str((load_config(skill_root).get("settings") or {}).get("viking_namespace") or "viking://resources/shengsuan/concepts")
    # lib_pages writes the local page before upload.  Keep a recoverable copy
    # and restore it if Viking rejects the upload.
    try:
        target_uri = lib_pages.upload_page(concept, content, namespace)
        if not target_uri:
            raise RuntimeError("OpenViking upload failed")
        if candidate.get("source_strategy") == "replace":
            sources = list(dict.fromkeys(candidate.get("source_refs") or []))
        else:
            sources = list(dict.fromkeys([*(record.get("sources") or []), *(candidate.get("source_refs") or [])]))
        # A newly discovered concept is not present in config.yaml yet.  Keep
        # its reviewed category (or an empty value) instead of dereferencing a
        # missing config record during the first publish.
        concept_meta = concept_config(load_config(skill_root), concept) or {}
        record.update({
            "status": "active",
            "current_version": new_version,
            "viking_uri": target_uri,
            "last_updated": now_iso(),
            "sources": sources,
            "category": record.get("category") or concept_meta.get("category", ""),
            "last_candidate_id": candidate_id,
            "last_review_actor": actor,
        })
        ledger[concept] = record
        store.save_ledger(ledger)
    except Exception as exc:
        if old_content:
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_path.write_text(old_content, encoding="utf-8")
        elif page_path.exists():
            page_path.unlink()
        store.save_ledger(old_ledger)
        try:
            store.update_candidate(
                candidate_id,
                expected_statuses={"publishing"},
                status="publish_failed",
                error=f"{type(exc).__name__}: {exc}",
                failed_at=now_iso(),
            )
        except ValueError:
            pass
        raise
    published = store.update_candidate(
        candidate_id,
        expected_statuses={"publishing"},
        status="published",
        published_at=now_iso(),
        published_by=actor,
        proposed_version=new_version,
        published_uri=target_uri,
    )
    append_agent_audit(
        skill_root,
        "candidate.published",
        {
            "candidate_id": candidate_id,
            "concept": concept,
            "published_by": actor,
            "published_uri": target_uri,
            "version": new_version,
            "content_hash": actual_content_hash,
        },
    )
    return published


def proposal_names(skill_root: Path) -> List[str]:
    config_names = [
        str(item.get("name"))
        for item in load_config(skill_root).get("concepts", [])
        if isinstance(item, dict) and item.get("name")
    ]
    store = ConceptLearningStore(skill_root)
    active_names = [
        str(name)
        for name, record in store.load_ledger().items()
        if isinstance(record, dict) and str(record.get("status") or "active") == "active"
    ]
    return list(dict.fromkeys([*config_names, *active_names]))


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Concept Learning Loop safe refresh adapter")
    parser.add_argument("--skill-root", type=Path, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--propose", action="store_true")
    group.add_argument("--propose-file", type=Path)
    group.add_argument("--approve")
    group.add_argument("--publish")
    parser.add_argument("--actor", default="zhujie14")
    parser.add_argument("--note", default="")
    parser.add_argument("concepts", nargs="*")
    parser.add_argument("--run-id")
    parser.add_argument("--all", action="store_true")
    parser.add_argument(
        "--jobs",
        type=int,
        default=_env_positive_int("CONCEPTS_REFRESH_JOBS", 1, maximum=32),
        help="并行处理的概念数；每个概念仍由 concept_lock 串行保护（默认 1）",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if CONCEPT_REFRESH_DISABLED:
        # Keep the historical CLI addressable for diagnostics without
        # allowing proposal, approval, or publication side effects.
        return emit_disabled("concept-refresh-adapter")
    skill_root = args.skill_root.expanduser().resolve()
    # The launchd-readable runtime mirror does not carry a second copy of the
    # helper.  Resolve it from the installed skill after parsing --skill-root;
    # this keeps Codex and Claude/project imports independent.
    skill_scripts = str(skill_root / "scripts")
    if skill_scripts not in sys.path:
        sys.path.insert(0, skill_scripts)
    from process_utils import install_process_group_signal_handlers  # type: ignore

    # LLM and upload calls run in worker threads.  Register one main-thread
    # handler so an outer weekly timeout also cleans those isolated sessions.
    install_process_group_signal_handlers()
    try:
        if args.approve:
            result = approve_one(skill_root, args.approve, actor=args.actor, note=args.note)
            print(json.dumps({"status": "ok", "candidate_id": result.get("candidate_id"), "concept": result.get("concept"), "state": result.get("status")}, ensure_ascii=False))
            return 0
        if args.publish:
            result = publish_one(skill_root, args.publish, actor=args.actor)
            print(json.dumps({"status": "ok", "candidate_id": result.get("candidate_id"), "concept": result.get("concept"), "state": result.get("status")}, ensure_ascii=False))
            return 0
        if args.propose_file:
            if len(args.concepts) != 1:
                parser.error("--propose-file requires exactly one concept name")
            propose_file(
                skill_root,
                args.concepts[0],
                args.propose_file,
                actor=args.actor,
                run_id=args.run_id,
            )
            return 0
        names = list(args.concepts)
        if args.all:
            names = proposal_names(skill_root)
        if not names:
            parser.error("--propose requires at least one concept or --all")
        if args.jobs < 1:
            parser.error("--jobs must be >= 1")

        def run_one(name: str) -> Dict[str, Any]:
            return propose_one(skill_root, name, args.run_id)

        failed = 0
        # Evidence search and the one-shot Agent call are independent across
        # concepts.  Keep the per-concept lock in propose_one so retries or a
        # concurrent manual refresh cannot create competing Candidates.  The
        # bounded pool avoids making the weekly orchestrator hostage to one
        # slow concept while also avoiding an unbounded Agent fan-out.
        worker_count = min(args.jobs, len(names))
        if worker_count == 1:
            futures = []
            for name in names:
                try:
                    result = run_one(name)
                    print(
                        json.dumps(
                            {
                                "status": "ok",
                                "candidate_id": result.get("candidate_id"),
                                "concept": result.get("concept"),
                                "kind": result.get("kind"),
                                "confidence": result.get("confidence"),
                                "deduplicated": bool(result.get("deduplicated", False)),
                                "deferred": bool(result.get("deferred", False)),
                                "candidate_status": result.get("status"),
                            },
                            ensure_ascii=False,
                        )
                    )
                except Exception as exc:
                    failed += 1
                    print(
                        json.dumps(
                            {"status": "failed", "concept": name, "error": f"{type(exc).__name__}: {exc}"},
                            ensure_ascii=False,
                        ),
                        file=sys.stderr,
                    )
        else:
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="concept-refresh") as pool:
                pending = {pool.submit(run_one, name): name for name in names}
                for future in as_completed(pending):
                    name = pending[future]
                    try:
                        result = future.result()
                        print(
                            json.dumps(
                                {
                                    "status": "ok",
                                    "candidate_id": result.get("candidate_id"),
                                    "concept": result.get("concept"),
                                    "kind": result.get("kind"),
                                    "confidence": result.get("confidence"),
                                    "deduplicated": bool(result.get("deduplicated", False)),
                                    "deferred": bool(result.get("deferred", False)),
                                    "candidate_status": result.get("status"),
                                },
                                ensure_ascii=False,
                            )
                        )
                    except Exception as exc:
                        failed += 1
                        print(
                            json.dumps(
                                {"status": "failed", "concept": name, "error": f"{type(exc).__name__}: {exc}"},
                                ensure_ascii=False,
                            ),
                            file=sys.stderr,
                        )
        return 0 if failed == 0 else 1
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
