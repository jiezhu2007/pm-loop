#!/usr/bin/env python3
"""Turn unmatched discovery evidence into reviewable new-concept Candidates."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import os
import time
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from concept_learning import ConceptLearningStore, make_candidate, now_iso
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


_CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]{2,16}")
_ASCII_TERM_RE = re.compile(r"(?<![A-Za-z0-9_])[A-Za-z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*(?![A-Za-z0-9_])")


def _env_positive_number(name: str, default: float, *, minimum: float = 0.1) -> float:
    """Read a bounded numeric setting without letting a bad env break triage."""
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_positive_int(name: str, default: int, *, maximum: Optional[int] = None) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    value = max(1, value)
    return min(value, maximum) if maximum is not None else value


def _active_taxonomy(store: ConceptLearningStore) -> List[Dict[str, Any]]:
    """Load the current Active taxonomy, including legacy config aliases.

    The ledger is authoritative for membership: a config-only name must not
    become an Active match before it has been published.  Older ledger rows do
    not persist aliases, so aliases are merged from the skill config when it
    is available.  Pending Candidates are intentionally excluded.
    """

    ledger = store.load_ledger()
    rows: Dict[str, Dict[str, Any]] = {}
    for key, value in ledger.items():
        if not isinstance(value, dict):
            continue
        status = str(value.get("status") or "active").strip().casefold()
        if status != "active":
            continue
        name = str(value.get("name") or key).strip()
        if not name:
            continue
        raw_aliases = value.get("aliases")
        if isinstance(raw_aliases, str):
            raw_aliases = [raw_aliases]
        aliases = [str(alias).strip() for alias in (raw_aliases or []) if str(alias).strip()]
        rows[name] = {
            "name": name,
            "aliases": list(dict.fromkeys(aliases)),
            "category": str(value.get("category") or "product_capability"),
            "source": "active",
            "status": "active",
        }

    # Config aliases are useful metadata only for names already in the Active
    # ledger.  A malformed optional config must never block triage.
    try:
        import yaml

        config_path = store.skill_root / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        for item in config.get("concepts", []) if isinstance(config, dict) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            row = rows.get(name)
            if row is None:
                continue
            aliases = item.get("aliases")
            if isinstance(aliases, str):
                aliases = [aliases]
            for alias in aliases or []:
                text = str(alias).strip()
                if text and text not in row["aliases"]:
                    row["aliases"].append(text)
    except Exception:
        pass
    return sorted(rows.values(), key=lambda row: str(row.get("name") or ""))


def _normalise_term(value: Any) -> str:
    return re.sub(r"[\s_\-./·:：,，()（）\[\]【】]+", "", str(value or "").casefold())


def _matcher(active_rows: Sequence[Mapping[str, Any]]) -> Tuple[Any, Any]:
    """Return the shared deterministic matcher, with a safe exact fallback."""

    try:
        # Keep one matching policy for full inventory and weekly discovery.
        from concept_deep_inventory import _active_concept_index, _active_match_for_term

        index = _active_concept_index(active_rows)
        return index, _active_match_for_term
    except Exception:
        index: Dict[str, List[Dict[str, Any]]] = {}
        for row in active_rows:
            target = str(row.get("name") or "").strip()
            for surface in [target, *(row.get("aliases") or [])]:
                key = _normalise_term(surface)
                if key:
                    index.setdefault(key, []).append(
                        {
                            "target": target,
                            "matched_surface": str(surface),
                            "match_type": "exact",
                            "decision": "alias",
                            "score": 1.0,
                            "category": row.get("category") or "product_capability",
                        }
                    )

        def exact(term: str, active_index: Mapping[str, Sequence[Mapping[str, Any]]]) -> Optional[Dict[str, Any]]:
            items = active_index.get(_normalise_term(term)) or []
            targets = {str(item.get("target") or "") for item in items if item.get("target")}
            if len(targets) != 1:
                return None
            return dict(items[0])

        return index, exact


def _term_candidates(item: Mapping[str, Any], active_rows: Sequence[Mapping[str, Any]]) -> List[str]:
    """Extract conservative semantic terms from an evidence item.

    URI path fragments are deliberately excluded.  Signal producers may give
    an explicit ``term``; otherwise scan the body for CJK/ASCII runs and
    direct Active surfaces.  The shared matcher filters generic overlaps.
    """

    text = str(item.get("text") or "")
    terms: List[str] = []
    explicit = str(item.get("term") or "").strip()
    if explicit:
        terms.append(explicit)
    for row in active_rows:
        for surface in [str(row.get("name") or ""), *(row.get("aliases") or [])]:
            surface = str(surface).strip()
            if surface and len(_normalise_term(surface)) >= 2 and surface.casefold() in text.casefold():
                terms.append(surface)
    terms.extend(_CJK_RUN_RE.findall(text))
    terms.extend(_ASCII_TERM_RE.findall(text))
    return list(dict.fromkeys(term for term in terms if term.strip()))


def _active_match_for_item(
    item: Mapping[str, Any],
    active_rows: Sequence[Mapping[str, Any]],
    active_index: Mapping[str, Sequence[Mapping[str, Any]]],
    matcher: Any,
) -> Optional[Dict[str, Any]]:
    """Return an unambiguous Active match for one evidence item."""

    matches: List[Dict[str, Any]] = []
    for term in _term_candidates(item, active_rows):
        try:
            match = matcher(term, active_index)
        except Exception:
            match = None
        if not isinstance(match, Mapping) or not str(match.get("target") or "").strip():
            continue
        value = dict(match)
        value["matched_term"] = term
        matches.append(value)
        # An explicit signal term is stronger than terms inferred from body.
        if str(item.get("term") or "").strip() == term:
            break
    if not matches:
        return None
    targets = {str(item.get("target") or "").strip() for item in matches}
    if len(targets) > 1:
        # Do not silently assign an ambiguous document.  Let the Agent (and
        # ultimately the reviewer) decide using the complete evidence.
        explicit = str(item.get("term") or "").strip()
        explicit_matches = [row for row in matches if row.get("matched_term") == explicit]
        if len({str(row.get("target") or "") for row in explicit_matches}) == 1 and explicit_matches:
            return explicit_matches[0]
        return None
    matches.sort(key=lambda row: (-float(row.get("score") or 0), -len(str(row.get("matched_term") or ""))))
    return matches[0]


def _active_decision(item: Mapping[str, Any], match: Mapping[str, Any]) -> Dict[str, Any]:
    target = str(match.get("target") or "").strip()
    term = str(match.get("matched_term") or item.get("term") or "").strip()
    decision = str(match.get("decision") or "merge").strip()
    if decision not in {"alias", "merge"}:
        decision = "merge"
    aliases = [term] if decision == "alias" and term and _normalise_term(term) != _normalise_term(target) else []
    match_type = str(match.get("match_type") or "fuzzy")
    return {
        "group_term": term or str(item.get("uri") or ""),
        "decision": decision,
        "name": target,
        "target": target,
        "aliases": aliases,
        "category": str(match.get("category") or "product_capability"),
        "content": "",
        "evidence_uris": [str(item.get("uri"))] if item.get("uri") else [],
        "reason": [f"active_match:{match_type}->{target}"],
        "confidence": max(0.65, min(1.0, float(match.get("score") or 0.9))),
        "active_match": dict(match),
    }


def _config() -> Dict[str, Any]:
    path = Path.home() / ".openviking" / "ovcli.conf"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        value = {}
    return value if isinstance(value, dict) else {}


def _ov_request(
    base: str,
    endpoint: str,
    uri: str,
    headers: Mapping[str, str],
    timeout: float,
    query: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Read one OpenViking JSON endpoint without exposing response bodies."""
    params = {"uri": uri}
    if query:
        params.update(query)
    url = f"{base}{endpoint}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers=dict(headers))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            value = json.loads(response.read().decode("utf-8", errors="replace"))
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _result_text(value: Optional[Mapping[str, Any]], limit: int) -> str:
    if not isinstance(value, Mapping):
        return ""
    result = value.get("result")
    if isinstance(result, str):
        return result[:limit]
    # Keep compatibility with OpenViking versions that wrap content in a
    # result/content/text/data object instead of returning a bare string.
    if isinstance(result, Mapping):
        for key in ("content", "text", "result", "data"):
            text = result.get(key)
            if isinstance(text, str) and text:
                return text[:limit]
    for key in ("content", "text", "data"):
        text = value.get(key)
        if isinstance(text, str) and text:
            return text[:limit]
    return ""


def _directory_leaves(
    base: str,
    root_uri: str,
    headers: Mapping[str, str],
    *,
    max_depth: int,
    max_leaves: int,
    timeout: float,
) -> List[str]:
    """Resolve a chunk-directory URI to a bounded list of file URIs.

    OpenViking stores uploaded Markdown as ``<name>.md/<chunk>.md``.  Search
    and discovery commonly return the directory URI, while ``content/read``
    only accepts the leaf.  A small breadth-first walk keeps triage bounded
    and avoids downloading an entire large document just to establish that
    evidence is readable.
    """
    queue = deque([(root_uri, 0)])
    seen = {root_uri}
    leaves: List[str] = []
    while queue and len(leaves) < max_leaves:
        uri, depth = queue.popleft()
        listing = _ov_request(base, "/api/v1/fs/ls", uri, headers, timeout)
        rows = listing.get("result") if isinstance(listing, Mapping) else None
        if isinstance(rows, Mapping):
            rows = rows.get("items") or rows.get("resources") or []
        if not isinstance(rows, list):
            continue
        for item in rows:
            if not isinstance(item, Mapping):
                continue
            child = str(item.get("uri") or "").strip()
            if not child or child in seen:
                continue
            seen.add(child)
            if bool(item.get("isDir")) and depth < max_depth:
                queue.append((child, depth + 1))
            elif not bool(item.get("isDir")):
                leaves.append(child)
                if len(leaves) >= max_leaves:
                    break
    return leaves


def read_evidence(uri: str, limit: int = 3500) -> Dict[str, Any]:
    config = _config()
    base = str(os.environ.get("OPENVIKING_URL") or config.get("url") or "http://127.0.0.1:1933").rstrip("/")
    headers = {"Accept": "application/json"}
    key = os.environ.get("OPENVIKING_API_KEY") or config.get("api_key")
    if key:
        headers["Authorization"] = f"Bearer {key}"

    read_query = {"offset": 0, "limit": limit, "raw": "false"}
    direct = _ov_request(base, "/api/v1/content/read", uri, headers, 8, read_query)
    text = _result_text(direct, limit)
    if text:
        return {"uri": uri, "status": "available", "text": text}

    # The URI may be a directory wrapper. Resolve only a few leaves and stop
    # as soon as the evidence budget is filled. This is also the fallback for
    # older OpenViking response shapes where a directory read returns 400.
    leaves = _directory_leaves(
        base,
        uri,
        headers,
        max_depth=_env_positive_int("CONCEPT_DISCOVERY_MAX_DEPTH", 4),
        max_leaves=_env_positive_int("CONCEPT_DISCOVERY_MAX_LEAVES", 3),
        timeout=_env_positive_number("CONCEPT_DISCOVERY_READ_TIMEOUT", 8),
    )
    chunks: List[str] = []
    for leaf in leaves:
        value = _ov_request(base, "/api/v1/content/read", leaf, headers, 8, read_query)
        chunk = _result_text(value, limit - sum(len(item) for item in chunks))
        if chunk:
            chunks.append(chunk)
        if sum(len(item) for item in chunks) >= limit:
            break
    if chunks:
        return {"uri": uri, "status": "available", "text": "\n\n".join(chunks)[:limit], "resolved_uri": leaves[0]}
    return {"uri": uri, "status": "unavailable", "text": "", "error": "unreadable_or_empty"}


def _read_evidence_batch(
    uris: Sequence[str],
    embedded: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Read discovery evidence with a small bounded pool, preserving order.

    Futures are consumed as they complete so one slow URI cannot hold all
    faster evidence behind it.  The batch deadline is separate from the
    per-request timeout and leaves every unfinished URI explicitly marked as
    unavailable for a later retry.
    """
    results: Dict[str, Dict[str, Any]] = {}
    pending: List[str] = []
    for uri in uris:
        item = embedded.get(uri)
        if item and item.get("text"):
            results[uri] = {
                "uri": uri,
                "status": "available",
                "text": str(item.get("text"))[:3500],
                "source": item.get("source"),
                "term": item.get("term"),
            }
        else:
            pending.append(uri)
    if pending:
        configured = _env_positive_int("CONCEPT_DISCOVERY_READ_JOBS", 4, maximum=8)
        workers = min(len(pending), configured)
        batch_deadline = time.monotonic() + _env_positive_number(
            "CONCEPT_DISCOVERY_READ_BATCH_TIMEOUT", 120
        )
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="discovery-read")
        futures = {pool.submit(read_evidence, uri): uri for uri in pending}
        pending_futures = set(futures)
        try:
            while pending_futures:
                remaining = batch_deadline - time.monotonic()
                if remaining <= 0:
                    break
                done, pending_futures = wait(
                    pending_futures,
                    timeout=remaining,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    uri = futures[future]
                    try:
                        results[uri] = future.result()
                    except Exception as exc:
                        results[uri] = {
                            "uri": uri,
                            "status": "unavailable",
                            "text": "",
                            "error": type(exc).__name__,
                        }
                if not done:
                    break
        finally:
            for future in pending_futures:
                future.cancel()
                uri = futures[future]
                results[uri] = {
                    "uri": uri,
                    "status": "unavailable",
                    "text": "",
                    "error": "batch_timeout",
                }
            # A foreign worker can ignore urllib's timeout. Waiting here would
            # silently turn the batch deadline back into an unbounded join.
            pool.shutdown(wait=False, cancel_futures=True)
    return [results[uri] for uri in uris if uri in results]


def _triage_evidence_state(
    discovery_run: Mapping[str, Any],
    evidence: Sequence[Mapping[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[str]]:
    """Merge per-URI read outcomes into the durable triage cursor.

    ``processed_uris`` is a work cursor, not a claim that the evidence was
    usable.  Recording an unavailable URI in that cursor prevents a broken
    resource from being retried on every weekly invocation.  The separate
    ``unavailable_uris`` list keeps the backlog visible for an explicit retry
    or repair workflow.
    """
    raw_state = discovery_run.get("triage_evidence")
    state: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_state, Mapping):
        for raw_uri, raw_value in raw_state.items():
            uri = str(raw_uri).strip()
            if not uri or not isinstance(raw_value, Mapping):
                continue
            state[uri] = {
                key: value
                for key, value in raw_value.items()
                if key in {"status", "error", "errors", "attempted_at", "attempts", "resolved_uri"}
            }

    unavailable = {
        str(uri).strip()
        for uri in discovery_run.get("unavailable_uris") or []
        if str(uri).strip()
    }
    attempted: List[str] = []
    attempted_at = now_iso()
    for raw_item in evidence:
        if not isinstance(raw_item, Mapping):
            continue
        uri = str(raw_item.get("uri") or "").strip()
        if not uri:
            continue
        attempted.append(uri)
        available = str(raw_item.get("status") or "") == "available" and bool(raw_item.get("text"))
        prior = state.get(uri) or {}
        try:
            prior_attempts = max(0, int(prior.get("attempts") or 0))
        except (TypeError, ValueError):
            prior_attempts = 0
        raw_errors = prior.get("errors")
        if isinstance(raw_errors, str):
            raw_errors = [raw_errors]
        prior_errors = [
            str(error)
            for error in (raw_errors or [])
            if str(error).strip()
        ]
        if prior.get("error") and str(prior.get("error")) not in prior_errors:
            prior_errors.append(str(prior.get("error")))
        record: Dict[str, Any] = {
            "status": "available" if available else "unavailable",
            "attempted_at": str(raw_item.get("attempted_at") or attempted_at),
            "attempts": prior_attempts + 1,
        }
        if prior_errors:
            record["errors"] = list(dict.fromkeys(prior_errors))
        if raw_item.get("resolved_uri"):
            record["resolved_uri"] = str(raw_item["resolved_uri"])
        if not available:
            record["error"] = str(raw_item.get("error") or "unreadable_or_empty")
            record["errors"] = list(dict.fromkeys([*prior_errors, record["error"]]))
            unavailable.add(uri)
        else:
            # A later explicit retry may have repaired the resource.
            unavailable.discard(uri)
        state[uri] = record

    return state, sorted(unavailable), list(dict.fromkeys(attempted))


def _load_llm(codex_root: Path) -> Any:
    path = codex_root / "skills" / "shengsuan-concepts" / "scripts" / "llm_runner.py"
    spec = importlib.util.spec_from_file_location("concept_discovery_llm", path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(f"missing shared Codex runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _prompt(
    items: List[Dict[str, Any]],
    existing: List[str],
    active_taxonomy: Optional[Sequence[Mapping[str, Any]]] = None,
) -> str:
    """Build the one-shot triage prompt.

    ``EXISTING_ACTIVE`` is intentionally separate from historical Candidate
    names.  A pending Candidate is not an Active concept and must not block a
    genuinely new proposal.
    """

    active = list(active_taxonomy or [])
    return f"""你是概念治理 Agent。请从真实未归类证据中提出 0~3 个新概念候选。

规则：
- 只使用 INPUT 证据，不从 URI 猜测不存在的产品能力；证据 unavailable 时不要提出候选。
- 先逐条比对 EXISTING_ACTIVE 的 name/aliases：命中时只能输出 alias 或 merge，name/target 指向已有 canonical 概念，绝不能输出 new_concept；只有不命中时才允许 new_concept。
- name 必须是稳定、短的中文产品概念；不得与 EXISTING_ACTIVE 或 EXISTING_CANDIDATES 同名或明显同义。
- 每个候选必须绑定 evidence_uris，content 是可供本人审核的 Markdown 概念卡草稿，明确边界、别名、证据和待确认点。
- 输出单个 JSON，不要 Markdown fence，不要解释：{{"decisions":[{{"group_term":"","decision":"alias|merge|new_concept|ignore","name":"","target":"","aliases":[],"category":"","content":"","evidence_uris":[],"reason":[],"confidence":0.0}}]}}

EXISTING_ACTIVE={json.dumps(active, ensure_ascii=False)}
EXISTING_CANDIDATES={json.dumps(existing, ensure_ascii=False)}
INPUT={json.dumps(items, ensure_ascii=False)}
"""


def propose(
    store: ConceptLearningStore,
    discovery_run: Dict[str, Any],
    codex_root: Path,
    max_items: int = 20,
    invoker: Optional[Any] = None,
    retry_unavailable: bool = False,
) -> Dict[str, Any]:
    all_unmatched = [str(uri) for uri in discovery_run.get("unmatched_uris") or []]
    processed = {str(uri) for uri in discovery_run.get("processed_uris") or []}
    try:
        page_size = max(1, int(max_items))
    except (TypeError, ValueError):
        page_size = 20
    prior_decisions = [
        item
        for item in discovery_run.get("triage_decisions") or []
        if isinstance(item, Mapping)
    ]
    prior_unavailable = {
        str(uri).strip()
        for uri in discovery_run.get("unavailable_uris") or []
        if str(uri).strip()
    }
    if retry_unavailable:
        # Explicit retries are bounded by the same page size as normal triage;
        # never reopen unrelated processed URIs or mutate the persisted cursor
        # until a new read outcome is available.
        processed.difference_update(prior_unavailable & set(all_unmatched))
        uris = [uri for uri in all_unmatched if uri in prior_unavailable][:page_size]
    else:
        uris = [uri for uri in all_unmatched if uri not in processed][:page_size]
    if not uris:
        if retry_unavailable and not prior_unavailable and any(uri not in processed for uri in all_unmatched):
            # Explicit retry mode is a no-op when this run has no unavailable
            # backlog; leave ordinary pending work for the normal cursor.
            return dict(discovery_run)
        # A drained cursor can still contain resources that were explicitly
        # recorded as unavailable.  Keep that state visible instead of
        # silently converting a blocked/partial run into a false success.
        if prior_unavailable:
            prior_status = str(discovery_run.get("status") or "")
            if prior_status in {"triage_blocked", "triage_failed"}:
                status = prior_status
                triage_status = "blocked" if status == "triage_blocked" else "failed"
            else:
                status = "triage_partial"
                triage_status = "complete_with_unavailable"
            remaining = len([uri for uri in all_unmatched if uri not in processed])
            return store.update_discovery_run(
                discovery_run["run_id"],
                status=status,
                triage_status=triage_status,
                triage_completed_at=now_iso(),
                triage_remaining=remaining,
                unavailable_uris=sorted(prior_unavailable),
                triage_unavailable_count=len(prior_unavailable),
                triage_active_match_count=sum(1 for item in prior_decisions if item.get("decision") in {"alias", "merge"}),
                triage_new_concept_count=sum(1 for item in prior_decisions if item.get("decision") == "new_concept"),
            )
        return store.update_discovery_run(
            discovery_run["run_id"],
            status="triaged" if discovery_run.get("candidate_ids") else "triage_no_candidate",
            triage_status="complete",
            triage_completed_at=now_iso(),
            triage_remaining=0,
            triage_active_match_count=sum(1 for item in prior_decisions if item.get("decision") in {"alias", "merge"}),
            triage_new_concept_count=sum(1 for item in prior_decisions if item.get("decision") == "new_concept"),
        )
    embedded = {str(item.get("uri")): dict(item) for item in discovery_run.get("evidence_items") or [] if isinstance(item, dict) and item.get("uri")}
    evidence = _read_evidence_batch(uris, embedded)
    evidence_state, unavailable_uris, attempted_uris = _triage_evidence_state(discovery_run, evidence)
    # Unavailable reads are terminal for this cursor but remain visible in a
    # separate backlog for an explicit retry/repair workflow.  Usable reads
    # stay unprocessed until their Active/Agent decision is durable.
    processed_unavailable = processed | (set(attempted_uris) & set(unavailable_uris))
    remaining_after_unavailable = len([uri for uri in all_unmatched if uri not in processed_unavailable])
    usable = [item for item in evidence if item.get("status") == "available" and item.get("text")]
    if not usable:
        batches = list(discovery_run.get("triage_batches") or [])
        batches.append(
            {
                "at": now_iso(),
                "input_count": 0,
                "skipped_count": len(evidence),
                "active_match_count": 0,
                "unmatched_count": 0,
                "decision_counts": {kind: sum(1 for row in prior_decisions if row.get("decision") == kind) for kind in ("alias", "merge", "new_concept", "ignore")},
                "candidate_ids": [str(item) for item in discovery_run.get("candidate_ids") or []],
                "uris": [],
                "attempted_uris": attempted_uris,
                "unavailable_uris": sorted(set(attempted_uris) & set(unavailable_uris)),
            }
        )
        status = "triage_blocked" if remaining_after_unavailable == 0 else "triage_partial"
        return store.update_discovery_run(
            discovery_run["run_id"],
            status=status,
            triage_status="blocked" if status == "triage_blocked" else "in_progress",
            triage_error="no readable OpenViking evidence",
            triage_completed_at=now_iso(),
            processed_uris=sorted(processed_unavailable),
            triage_remaining=remaining_after_unavailable,
            triage_batches=batches,
            triage_evidence=evidence_state,
            unavailable_uris=unavailable_uris,
            triage_unavailable_count=len(unavailable_uris),
            triage_input_count=0,
            triage_skipped_count=len(evidence),
            triage_active_match_count=sum(1 for item in prior_decisions if item.get("decision") in {"alias", "merge"}),
            triage_new_concept_count=sum(1 for item in prior_decisions if item.get("decision") == "new_concept"),
        )
    active_taxonomy = _active_taxonomy(store)
    active_index, active_matcher = _matcher(active_taxonomy)
    matched_decisions: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    for item in usable:
        match = _active_match_for_item(item, active_taxonomy, active_index, active_matcher)
        if match:
            matched_decisions.append(_active_decision(item, match))
        else:
            unmatched.append(item)

    existing = list(store.load_ledger().keys())
    for prior in store.list_candidates():
        existing.append(str(prior.get("concept") or ""))
        existing.extend(str(alias) for alias in prior.get("aliases") or [])
    prior_decisions = [dict(item) for item in prior_decisions]
    decisions: List[Dict[str, Any]] = [*prior_decisions, *matched_decisions]
    proposals: List[Dict[str, Any]] = []

    def persist_agent_failure(message: str) -> Dict[str, Any]:
        """Keep unavailable read outcomes while leaving usable inputs retryable."""
        return store.update_discovery_run(
            discovery_run["run_id"],
            status="triage_failed",
            triage_status="failed",
            triage_error=message,
            triage_completed_at=now_iso(),
            processed_uris=sorted(processed_unavailable),
            triage_remaining=remaining_after_unavailable,
            triage_evidence=evidence_state,
            unavailable_uris=unavailable_uris,
            triage_unavailable_count=len(unavailable_uris),
            triage_input_count=len(usable),
            triage_skipped_count=len(evidence) - len(usable),
        )

    if unmatched:
        prompt = _prompt(unmatched, [str(item) for item in existing], active_taxonomy)
        module = _load_llm(codex_root) if invoker is None else None
        triage_timeout = float(os.environ.get("CONCEPTS_DISCOVERY_LLM_TIMEOUT", "180"))
        caller = invoker or (lambda text: module.run_prompt(text, triage_timeout))
        result = caller(prompt)
        if isinstance(result, tuple):
            completed, output = result[0], result[1]
        else:
            completed, output = 0, str(result)
        returncode = int(getattr(completed, "returncode", completed))
        if returncode != 0:
            return persist_agent_failure(f"Codex exited {returncode}")
        try:
            raw = json.loads(str(output).strip().strip("`").strip())
        except json.JSONDecodeError as exc:
            return persist_agent_failure(f"invalid JSON: {exc}")
        if isinstance(raw, dict):
            proposals = raw.get("decisions") if isinstance(raw.get("decisions"), list) else raw.get("proposals")
        if not isinstance(proposals, list):
            proposals = []
    known = {str(name).casefold() for name in existing if str(name).strip()}
    candidate_ids: List[str] = [str(item) for item in discovery_run.get("candidate_ids") or []]
    for item in proposals[:3]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        decision = str(item.get("decision") or "new_concept").strip()
        if decision not in {"new_concept", "alias", "merge", "ignore"}:
            decision = "new_concept"
        confidence = item.get("confidence")
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = 0.0
        refs = [str(uri) for uri in item.get("evidence_uris") or [] if str(uri) in {row["uri"] for row in unmatched}]
        content = str(item.get("content") or "").strip()
        # Hard guard: the Agent cannot override deterministic Active matching
        # by returning ``new_concept`` for a known name/alias or a governed
        # sub-capability such as 资源队列/行权限.
        proposed_item = {"text": "", "term": name}
        forced_match = _active_match_for_item(proposed_item, active_taxonomy, active_index, active_matcher) if name else None
        if forced_match:
            forced = _active_decision({"uri": refs[0] if refs else "", "term": name}, forced_match)
            forced["reason"] = ["active_match:llm_guard", *forced.get("reason", [])]
            decisions.append(forced)
            continue
        target = str(item.get("target") or name).strip()
        if decision in {"alias", "merge"}:
            decisions.append(
                {
                    "group_term": str(item.get("group_term") or name),
                    "decision": decision,
                    "name": target,
                    "target": target,
                    "aliases": [str(alias) for alias in item.get("aliases") or [] if str(alias).strip()],
                    "category": str(item.get("category") or "product_capability"),
                    "content": "",
                    "evidence_uris": refs,
                    "reason": [str(reason) for reason in item.get("reason") or [] if str(reason).strip()],
                    "confidence": confidence_value,
                }
            )
            continue
        if decision == "ignore":
            decisions.append(
                {
                    "group_term": str(item.get("group_term") or name),
                    "decision": "ignore",
                    "name": name,
                    "target": target,
                    "aliases": [],
                    "category": str(item.get("category") or "product_capability"),
                    "content": "",
                    "evidence_uris": refs,
                    "reason": [str(reason) for reason in item.get("reason") or [] if str(reason).strip()],
                    "confidence": confidence_value,
                }
            )
            continue
        if not name or name.casefold() in known or not refs or len(content) < 80 or confidence_value < 0.55:
            continue
        candidate = make_candidate(
            concept=name,
            kind="new_concept",
            content=content,
            source_refs=refs,
            evidence=[{"uri": uri, "status": "available", "source": "discovery_triage"} for uri in refs],
            reason=[str(reason) for reason in item.get("reason") or [] if str(reason).strip()],
            confidence=confidence_value,
            base_version="new",
            aliases=[str(alias) for alias in item.get("aliases") or [] if str(alias).strip()],
            category=str(item.get("category") or "待归类"),
            discovery_run_id=discovery_run["run_id"],
            triage_kind="new_concept",
            suspected_existing=False,
            suspected_existing_matches=[],
            suspected_existing_reason="未命中当前 Active 概念 name/aliases",
        )
        saved = store.save_candidate(candidate, content)
        candidate_ids.append(str(saved["candidate_id"]))
        known.add(name.casefold())
        decisions.append(
            {
                "group_term": str(item.get("group_term") or name),
                "decision": "new_concept",
                "name": name,
                "target": "",
                "aliases": [str(alias) for alias in item.get("aliases") or [] if str(alias).strip()],
                "category": str(item.get("category") or "待归类"),
                "content": content,
                "evidence_uris": refs,
                "reason": [str(reason) for reason in item.get("reason") or [] if str(reason).strip()],
                "confidence": confidence_value,
                "candidate_id": saved["candidate_id"],
            }
        )
    processed.update(item["uri"] for item in usable)
    processed.update(processed_unavailable)
    remaining = len([uri for uri in all_unmatched if uri not in processed])
    batches = list(discovery_run.get("triage_batches") or [])
    batches.append(
        {
            "at": now_iso(),
            "input_count": len(usable),
            "skipped_count": len(evidence) - len(usable),
            "active_match_count": len(matched_decisions),
            "unmatched_count": len(unmatched),
            "decision_counts": {kind: sum(1 for row in decisions if row.get("decision") == kind) for kind in ("alias", "merge", "new_concept", "ignore")},
            "candidate_ids": candidate_ids,
            "uris": [item["uri"] for item in usable],
            "attempted_uris": attempted_uris,
            "unavailable_uris": sorted(set(attempted_uris) & set(unavailable_uris)),
        }
    )
    status = "triage_partial" if remaining > 0 or unavailable_uris else ("triaged" if candidate_ids else "triage_no_candidate")
    triage_status = (
        "in_progress"
        if remaining > 0
        else ("complete_with_unavailable" if unavailable_uris else "complete")
    )
    return store.update_discovery_run(
        discovery_run["run_id"],
        status=status,
        candidate_ids=candidate_ids,
        processed_uris=sorted(processed),
        triage_status=triage_status,
        triage_remaining=remaining,
        triage_batches=batches,
        triage_decisions=decisions,
        triage_active_match_count=sum(1 for row in decisions if row.get("decision") in {"alias", "merge"}),
        triage_new_concept_count=sum(1 for row in decisions if row.get("decision") == "new_concept"),
        triage_completed_at=now_iso(),
        triage_input_count=len(usable),
        triage_skipped_count=len(evidence) - len(usable),
        triage_evidence=evidence_state,
        unavailable_uris=unavailable_uris,
        triage_unavailable_count=len(unavailable_uris),
        triage_error=(f"{len(unavailable_uris)} unavailable evidence URI(s)" if unavailable_uris else None),
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Discovery inbox Agent triage")
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-items", type=int, default=20)
    parser.add_argument(
        "--retry-unavailable",
        action="store_true",
        help="retry only this run's previously unavailable evidence URIs",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    # Triage reads evidence, may invoke an LLM, and persists Candidates.  The
    # retired workflow must stop before constructing a store or loading any
    # evidence so a stale direct CLI call is a harmless no-op.
    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("discovery-triage")
    store = ConceptLearningStore(args.codex_root.expanduser() / "skills" / "shengsuan-concepts")
    rows = [row for row in store.discovery_runs() if str(row.get("run_id")) == args.run_id]
    if not rows:
        raise FileNotFoundError(args.run_id)
    print(
        json.dumps(
            propose(
                store,
                rows[0],
                args.codex_root,
                args.max_items,
                retry_unavailable=args.retry_unavailable,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
