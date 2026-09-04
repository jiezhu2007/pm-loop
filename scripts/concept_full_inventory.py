#!/usr/bin/env python3
"""Deep, resumable concept inventory over the local OpenViking corpus.

This runner inventories evidence and proposes Candidates.  For an explicitly
requested bootstrap, ``--auto-approve-publish`` can approve and publish only
the Candidates created by this run through the existing concept publisher.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from concept_learning import ConceptLearningStore, content_hash, make_candidate, now_iso
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


INVENTORY_SCHEMA = "concept-learning.inventory.v1"
DEFAULT_ROOTS = (
    "viking://resources/shengsuan/data-agent",
    "viking://resources/shengsuan/datasearch",
    "viking://resources/shengsuan/feature-list",
    "viking://resources/shengsuan/ontology",
    "viking://resources/shengsuan/pipeline-logic-fde",
    "viking://resources/shengsuan/product-management",
    "viking://resources/shengsuan/public-docs",
)
DEFAULT_EXCLUDES = ("viking://resources/shengsuan/concepts",)
TERMINAL_STATUSES = {"completed"}


def _config() -> Dict[str, Any]:
    path = Path.home() / ".openviking" / "ovcli.conf"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        value = {}
    return value if isinstance(value, dict) else {}


def _extract_result(value: Any) -> Any:
    if isinstance(value, dict) and value.get("status") == "ok" and "result" in value:
        return value["result"]
    return value


def _extract_text(value: Any) -> str:
    value = _extract_result(value)
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return ""
    for key in ("abstract", "overview", "content", "text"):
        item = value.get(key)
        if isinstance(item, str) and item.strip():
            return item
    data = value.get("data")
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        return _extract_text(data)
    return ""


class OpenVikingClient:
    """Small local REST client, kept injectable for deterministic tests."""

    def __init__(self, timeout: int = 20) -> None:
        config = _config()
        self.base = str(os.environ.get("OPENVIKING_URL") or config.get("url") or "http://127.0.0.1:1933").rstrip("/")
        self.api_key = str(os.environ.get("OPENVIKING_API_KEY") or config.get("api_key") or "")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, query: Optional[Dict[str, Any]] = None) -> Any:
        url = self.base + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def glob(self, root: str, pattern: str, node_limit: int) -> List[str]:
        value = _extract_result(self._request("POST", "/api/v1/search/glob", {"uri": root, "pattern": pattern, "node_limit": node_limit}))
        matches = value.get("matches") if isinstance(value, dict) else []
        return [str(uri) for uri in matches or [] if str(uri).startswith("viking://")]

    def evidence_text(self, uri: str, limit: int) -> str:
        try:
            text = _extract_text(self._request("GET", "/api/v1/content/abstract", query={"uri": uri}))
        except Exception:
            text = ""
        # OpenViking can return a non-empty directory placeholder from the
        # abstract endpoint. It is not evidence and must never reach the LLM.
        placeholder = text.strip() in {
            "[Directory overview is not generated]",
            "Directory overview is not generated",
        }
        if text.strip() and not placeholder:
            return text[:limit]
        value = self._request(
            "GET",
            "/api/v1/content/read",
            query={"uri": uri, "offset": 0, "limit": limit, "raw": "false"},
        )
        return _extract_text(value)[:limit]


def _atomic_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def manual_seed_terms(state_dir: Optional[Path]) -> List[str]:
    if state_dir is None:
        return []
    path = state_dir.expanduser() / "concept-review" / "manual-seeds.jsonl"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    terms: List[str] = []
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        term = str(value.get("term") or "").strip() if isinstance(value, dict) else ""
        if term:
            terms.append(term)
    return list(dict.fromkeys(terms))


def inventory_path(store: ConceptLearningStore, run_id: str) -> Path:
    return store.state_root / "full-inventory" / "runs" / f"{run_id}.json"


def latest_resumable_run_id(store: ConceptLearningStore) -> Optional[str]:
    root = store.state_root / "full-inventory" / "runs"
    for path in sorted(root.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        value = _read_json(path)
        if value and str(value.get("status") or "") not in TERMINAL_STATUSES:
            return str(value.get("run_id") or path.stem)
    return None


def enumerate_resources(
    client: Any,
    roots: Sequence[str],
    excludes: Sequence[str],
    node_limit: int,
) -> List[str]:
    """Recursively enumerate Markdown evidence and reject silent truncation."""
    found: set[str] = set()
    normalized_excludes = tuple(prefix.rstrip("/") + "/" for prefix in excludes)
    for root in roots:
        matches = client.glob(root.rstrip("/"), "**/*.md", node_limit)
        if len(matches) >= node_limit:
            raise RuntimeError(f"OpenViking glob reached node_limit={node_limit} for {root}; inventory is not provably complete")
        for uri in matches:
            normalized = str(uri).strip()
            if not normalized or any(normalized == prefix[:-1] or normalized.startswith(prefix) for prefix in normalized_excludes):
                continue
            found.add(normalized)
    if not found:
        raise RuntimeError("OpenViking inventory returned no resources; snapshot is not valid")
    return sorted(found)


def _even_sample(values: Sequence[str], count: int) -> List[str]:
    if count <= 0:
        return []
    if count >= len(values):
        return list(values)
    if count == 1:
        return [values[len(values) // 2]]
    indexes = {round(position * (len(values) - 1) / (count - 1)) for position in range(count)}
    return [values[index] for index in sorted(indexes)]


def select_evidence_resources(
    resources: Sequence[str],
    roots: Sequence[str],
    maximum: int,
    seed_terms: Sequence[str] = (),
) -> List[str]:
    """Choose a stable, root-stratified deep-read set from a complete URI snapshot.

    Paths matching explicit reviewer seed terms are always considered first.
    Remaining capacity is apportioned across all configured roots, then sampled
    evenly instead of taking only the lexicographic head of a large PRD bucket.
    """
    unique = sorted(set(str(uri) for uri in resources))
    if maximum <= 0 or len(unique) <= maximum:
        return unique
    normalized_seeds = [_normalize_term(term) for term in seed_terms if _normalize_term(term)]
    priority = [uri for uri in unique if any(term in _normalize_term(urllib.parse.unquote(uri)) for term in normalized_seeds)]
    selected: List[str] = priority[:maximum]
    selected_set = set(selected)
    capacity = maximum - len(selected)
    if capacity <= 0:
        return sorted(selected)

    buckets: Dict[str, List[str]] = {}
    for root in roots:
        prefix = root.rstrip("/") + "/"
        buckets[root] = [uri for uri in unique if uri.startswith(prefix) and uri not in selected_set]
    outside = [uri for uri in unique if uri not in selected_set and not any(uri.startswith(root.rstrip("/") + "/") for root in roots)]
    if outside:
        buckets["__other__"] = outside
    active = [name for name, values in buckets.items() if values]
    if not active:
        return sorted(selected)

    minimum = min(20, capacity // len(active))
    allocations = {name: min(minimum, len(buckets[name])) for name in active}
    remaining = capacity - sum(allocations.values())
    while remaining > 0:
        candidates = [name for name in active if allocations[name] < len(buckets[name])]
        if not candidates:
            break
        total_left = sum(len(buckets[name]) - allocations[name] for name in candidates)
        progressed = False
        for name in candidates:
            share = max(1, round(remaining * (len(buckets[name]) - allocations[name]) / total_left))
            increment = min(share, len(buckets[name]) - allocations[name], remaining)
            allocations[name] += increment
            remaining -= increment
            progressed = progressed or increment > 0
            if remaining == 0:
                break
        if not progressed:
            break
    for name in active:
        selected.extend(_even_sample(buckets[name], allocations[name]))
    return sorted(dict.fromkeys(selected))[:maximum]


def _normalize_term(value: Any) -> str:
    return re.sub(r"[\s_\-./·]+", "", str(value or "").strip()).casefold()


def _load_yaml_terms(path: Path) -> List[Dict[str, Any]]:
    try:
        import yaml

        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    return [dict(item) for item in value.get("concepts", []) if isinstance(item, dict)]


def taxonomy(store: ConceptLearningStore) -> List[Dict[str, Any]]:
    """Return all names and aliases that a proposal must not duplicate."""
    rows: List[Dict[str, Any]] = []
    for item in _load_yaml_terms(store.skill_root / "config.yaml"):
        name = str(item.get("name") or "").strip()
        if name:
            rows.append(
                {
                    "name": name,
                    "aliases": [str(value) for value in item.get("aliases") or []],
                    "category": str(item.get("category") or "product_capability"),
                    "status": str(item.get("status") or "active"),
                    "source": "config",
                }
            )
    for name, record in store.load_ledger().items():
        if isinstance(record, dict):
            rows.append(
                {
                    "name": str(name),
                    "aliases": [str(value) for value in record.get("aliases") or []],
                    "category": str(record.get("category") or "product_capability"),
                    "status": str(record.get("status") or "active"),
                    "source": "active",
                }
            )
    for item in store.list_candidates():
        rows.append(
            {
                "name": str(item.get("concept") or ""),
                "aliases": [str(value) for value in item.get("aliases") or []],
                "category": str(item.get("category") or "product_capability"),
                "status": str(item.get("status") or "ready_for_review"),
                "source": "candidate",
            }
        )
    unique: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = _normalize_term(row["name"])
        if key:
            previous = unique.get(key)
            if previous is None:
                unique[key] = row
                continue
            previous_source = str(previous.get("source") or "").casefold()
            current_source = str(row.get("source") or "").casefold()
            # Ledger state is authoritative over the static config.  A
            # Candidate with the same canonical name must not hide that
            # active/retired state, but its aliases can still enrich the row.
            if current_source == "active" or (
                current_source == "config" and previous_source == "candidate"
            ):
                preferred, secondary = row, previous
            else:
                preferred, secondary = previous, row
            aliases = [
                str(value).strip()
                for value in [
                    *list(preferred.get("aliases") or []),
                    *list(secondary.get("aliases") or []),
                ]
                if str(value).strip()
            ]
            preferred = dict(preferred)
            preferred["aliases"] = list(dict.fromkeys(aliases))
            unique[key] = preferred
    return list(unique.values())


def _known_term_index(rows: Iterable[Mapping[str, Any]]) -> set[str]:
    result: set[str] = set()
    for row in rows:
        result.add(_normalize_term(row.get("name")))
        result.update(_normalize_term(value) for value in row.get("aliases") or [])
    return {value for value in result if value}


def _load_llm(skill_root: Path) -> Any:
    path = skill_root / "scripts" / "llm_runner.py"
    spec = importlib.util.spec_from_file_location("concept_inventory_llm", path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(f"missing shared Codex runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _invoke_json(invoker: Callable[[str], Any], prompt: str) -> Dict[str, Any]:
    result = invoker(prompt)
    if isinstance(result, tuple):
        completed, output = result[0], result[1]
        returncode = int(getattr(completed, "returncode", completed))
        if returncode != 0:
            raise RuntimeError(f"Codex exited {returncode}")
    else:
        output = result
    text = str(output).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL)
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Agent output must be a JSON object")
    return value


def _discovery_prompt(items: List[Dict[str, Any]], existing: List[Dict[str, Any]]) -> str:
    return f"""你是胜算产品概念盘点 Agent。逐条阅读真实摘要，发现稳定产品概念，并作四选一判断。

decision 只能是 new_concept / alias / merge / ignore：
- new_concept：有独立定义和能力边界，且不是现有概念同义词；
- alias：只是现有概念的中英文名、缩写或同义词；target 填现有概念；
- merge：只是现有概念的子能力或组合表达；target 填应合并概念；
- ignore：项目名、客户名、页面标题、版本名或临时术语。

不得从 URI 或常识补造能力。evidence_uris 只能引用 INPUT 中可读证据。输出单个 JSON：
new_concept 必须同时输出可审核的 content（Markdown，包含定义、能力边界、已知限制、关联概念、证据与待确认点）。单批次只有一个证据时仍可提出，但不要编造第二份证据；最终合并阶段会验证至少两份真实证据。
{{"decisions":[{{"decision":"new_concept","name":"","aliases":[],"target":"","category":"","content":"","evidence_uris":[],"reason":[],"confidence":0.0}}]}}

EXISTING_TAXONOMY={json.dumps(existing, ensure_ascii=False)}
INPUT={json.dumps(items, ensure_ascii=False)}
"""


def _consolidation_prompt(decisions: List[Dict[str, Any]], existing: List[Dict[str, Any]]) -> str:
    return f"""你是概念治理 Agent。合并跨批次发现，消除同义和上下位重复，输出最终四选一决策。

硬规则：
- new_concept 不得与 EXISTING_TAXONOMY 的 name/aliases 同义；同义则改为 alias。
- new_concept 至少绑定 2 个不同真实 evidence_uris，confidence >= 0.65。
- alias/merge 的 target 必须优先指向现有正式概念。
- 不要创造 INPUT 中没有出现的 evidence URI。
- content 仅对 new_concept 必填，必须是可直接发布的完整概念页：第一行 YAML frontmatter，至少包含 concept、aliases、category、last_updated、sources、related_concepts、related_customers、latest_version；正文必须包含“定义”“能力边界（能做什么）”“已知限制（不能做什么/需定制）”“版本演进”“关联概念”“出现过的客户/评估”“证据与待确认点”。能力和限制逐条标注真实 evidence URI。
- 输出单个 JSON：{{"decisions":[{{"decision":"new_concept","name":"","aliases":[],"target":"","category":"","content":"","evidence_uris":[],"reason":[],"confidence":0.0}}]}}

EXISTING_TAXONOMY={json.dumps(existing, ensure_ascii=False)}
        INPUT={json.dumps(decisions, ensure_ascii=False)}
"""


def _validated_decisions(raw: Any, allowed_uris: set[str]) -> List[Dict[str, Any]]:
    rows = raw.get("decisions") if isinstance(raw, dict) else []
    result: List[Dict[str, Any]] = []
    for item in rows if isinstance(rows, list) else []:
        if not isinstance(item, dict):
            continue
        decision = str(item.get("decision") or "").strip()
        name = str(item.get("name") or "").strip()
        if decision not in {"new_concept", "alias", "merge", "ignore"} or not name:
            continue
        try:
            confidence = float(item.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        refs = list(dict.fromkeys(str(uri) for uri in item.get("evidence_uris") or [] if str(uri) in allowed_uris))
        result.append(
            {
                "decision": decision,
                "name": name,
                "aliases": [str(value).strip() for value in item.get("aliases") or [] if str(value).strip()],
                "target": str(item.get("target") or "").strip(),
                "category": str(item.get("category") or "待归类").strip(),
                "content": str(item.get("content") or "").strip(),
                "evidence_uris": refs,
                "reason": [str(value).strip() for value in item.get("reason") or [] if str(value).strip()],
                "confidence": confidence,
            }
        )
    return result


def create_run(
    store: ConceptLearningStore,
    resources: Sequence[str],
    roots: Sequence[str],
    excludes: Sequence[str],
    selected_resources: Sequence[str],
    seed_terms: Sequence[str],
    max_evidence: int,
) -> Dict[str, Any]:
    digest = hashlib.sha256("\n".join(resources).encode("utf-8")).hexdigest()
    run_id = "inventory-" + now_iso().replace(":", "").replace("-", "") + "-" + uuid.uuid4().hex[:6]
    value = {
        "schema_version": INVENTORY_SCHEMA,
        "run_id": run_id,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "status": "scanning",
        "roots": list(roots),
        "excluded_prefixes": list(excludes),
        "resource_snapshot_hash": "sha256:" + digest,
        "resource_count": len(resources),
        "resources": list(resources),
        "selected_resource_count": len(selected_resources),
        "selected_resources": list(selected_resources),
        "max_evidence": max_evidence,
        "seed_terms": list(seed_terms),
        "scan_cursor": 0,
        "evidence_items": [],
        "unreadable_uris": [],
        "raw_decisions": [],
        "decisions": [],
        "candidate_ids": [],
    }
    _atomic_json(inventory_path(store, run_id), value)
    return value


def _persist(store: ConceptLearningStore, run: Dict[str, Any], **updates: Any) -> Dict[str, Any]:
    run.update(updates)
    run["updated_at"] = now_iso()
    _atomic_json(inventory_path(store, str(run["run_id"])), run)
    return run


def _default_invoker(store: ConceptLearningStore) -> Callable[[str], Any]:
    module = _load_llm(store.skill_root)
    return lambda prompt: module.run_prompt(prompt, 1200)


def _scan_batch(
    client: Any,
    caller: Callable[[str], Any],
    uris: Sequence[str],
    evidence_limit: int,
    taxonomy_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    unreadable: List[Dict[str, str]] = []
    for uri in uris:
        try:
            text = str(client.evidence_text(uri, evidence_limit)).strip()
        except Exception as exc:
            unreadable.append({"uri": uri, "error": type(exc).__name__})
            continue
        if not text:
            unreadable.append({"uri": uri, "error": "empty_evidence"})
            continue
        items.append({"uri": uri, "summary": text, "sha256": content_hash(text)})
    decisions: List[Dict[str, Any]] = []
    if items:
        output = _invoke_json(caller, _discovery_prompt(items, taxonomy_rows))
        decisions = _validated_decisions(output, {item["uri"] for item in items})
    return {"items": items, "unreadable": unreadable, "decisions": decisions}


def execute(
    store: ConceptLearningStore,
    client: Any,
    invoker: Optional[Callable[[str], Any]] = None,
    *,
    roots: Sequence[str] = DEFAULT_ROOTS,
    excludes: Sequence[str] = DEFAULT_EXCLUDES,
    node_limit: int = 50000,
    batch_size: int = 20,
    evidence_limit: int = 3000,
    max_evidence: int = 800,
    workers: int = 1,
    seed_terms: Sequence[str] = (),
    resume_run_id: Optional[str] = None,
    auto_approve_publish: bool = False,
) -> Dict[str, Any]:
    """Execute or resume a full inventory, checkpointing after every batch."""
    if resume_run_id:
        run = _read_json(inventory_path(store, resume_run_id))
        if not run:
            raise FileNotFoundError(resume_run_id)
        if run.get("status") in TERMINAL_STATUSES:
            return run
    else:
        resources = enumerate_resources(client, roots, excludes, node_limit)
        selected_resources = select_evidence_resources(resources, roots, max_evidence, seed_terms)
        run = create_run(store, resources, roots, excludes, selected_resources, seed_terms, max_evidence)
    caller = invoker or _default_invoker(store)
    snapshot_resources = [str(uri) for uri in run.get("resources") or []]
    resources = [str(uri) for uri in run.get("selected_resources") or snapshot_resources]
    taxonomy_rows = taxonomy(store)
    allowed_uris = set(resources)

    try:
        cursor = int(run.get("scan_cursor") or 0)
        worker_count = max(1, int(workers))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            while cursor < len(resources):
                pending = []
                for worker_index in range(worker_count):
                    start = cursor + worker_index * batch_size
                    uris = resources[start : start + batch_size]
                    if not uris:
                        break
                    future = executor.submit(
                        _scan_batch,
                        client,
                        caller,
                        uris,
                        evidence_limit,
                        taxonomy_rows,
                    )
                    pending.append((start, uris, future))

                # Commit in source order so scan_cursor remains a contiguous,
                # replay-safe checkpoint even when later batches finish first.
                for start, uris, future in pending:
                    batch = future.result()
                    raw_decisions = list(run.get("raw_decisions") or []) + list(batch["decisions"])
                    evidence_items = list(run.get("evidence_items") or []) + list(batch["items"])
                    unreadable = list(run.get("unreadable_uris") or []) + list(batch["unreadable"])
                    cursor = start + len(uris)
                    _persist(
                        store,
                        run,
                        scan_cursor=cursor,
                        evidence_items=evidence_items,
                        unreadable_uris=unreadable,
                        raw_decisions=raw_decisions,
                        progress={"processed": cursor, "total": len(resources), "workers": worker_count},
                    )

        _persist(store, run, status="consolidating", error=None)
        raw_decisions = list(run.get("raw_decisions") or [])
        if raw_decisions:
            output = _invoke_json(caller, _consolidation_prompt(raw_decisions, taxonomy_rows))
            decisions = _validated_decisions(output, allowed_uris)
        else:
            decisions = []

        known = _known_term_index(taxonomy_rows)
        candidate_ids = list(run.get("candidate_ids") or [])
        # A process can stop after save_candidate() but before the run
        # checkpoint. Recover those Candidates by provenance on resume so a
        # retry neither duplicates nor loses them from the result contract.
        for existing_candidate in store.list_candidates():
            if str(existing_candidate.get("inventory_run_id") or "") == str(run["run_id"]):
                candidate_id = str(existing_candidate.get("candidate_id") or "")
                if candidate_id and candidate_id not in candidate_ids:
                    candidate_ids.append(candidate_id)
        accepted: List[Dict[str, Any]] = []
        for decision in decisions:
            if decision["decision"] != "new_concept":
                accepted.append(decision)
                continue
            terms = {_normalize_term(decision["name"]), *(_normalize_term(value) for value in decision["aliases"])}
            refs = decision["evidence_uris"]
            content = decision["content"]
            if known.intersection(terms) or len(refs) < 2 or decision["confidence"] < 0.65 or len(content) < 80:
                continue
            candidate = make_candidate(
                concept=decision["name"],
                kind="new_concept",
                content=content,
                source_refs=refs,
                evidence=[{"uri": uri, "status": "available", "source": "full_inventory"} for uri in refs],
                reason=decision["reason"],
                confidence=decision["confidence"],
                base_version="new",
                aliases=decision["aliases"],
                category=decision["category"],
                inventory_run_id=run["run_id"],
            )
            saved = store.save_candidate(candidate, content)
            candidate_ids.append(str(saved["candidate_id"]))
            if auto_approve_publish:
                # Explicit bootstrap mode: preserve the normal candidate hash
                # and publication implementation, but record the user's
                # requested default approval before invoking the publisher.
                approved = store.update_candidate(
                    str(saved["candidate_id"]),
                    expected_statuses={"ready_for_review"},
                    status="approved",
                    approved_by="zhujie14",
                    approved_at=now_iso(),
                    approved_content_hash=saved.get("content_hash"),
                    approval_note="用户明确要求：全量盘点生成概念默认审批通过",
                    approval_run_id=run["run_id"],
                    auto_approved=True,
                    auto_approval_reason="explicit_user_request",
                )
                try:
                    # This is the same audited Active writer used by the
                    # Control Plane, called directly per the user's request.
                    from concept_refresh_adapter import publish_one
                    published = publish_one(store.skill_root, str(approved["candidate_id"]), actor="zhujie14")
                    if str(published.get("status")) != "published":
                        raise RuntimeError(f"publisher returned {published.get('status')}")
                except Exception as exc:
                    store.update_candidate(
                        str(saved["candidate_id"]),
                        expected_statuses={"approved", "publishing", "publish_failed"},
                        error=f"auto_publish_failed: {type(exc).__name__}: {exc}",
                    )
                    raise
            _persist(store, run, candidate_ids=candidate_ids)
            known.update(terms)
            accepted.append(decision)
        evidence_revisions = {
            str(item["uri"]): str(item["sha256"])
            for item in run.get("evidence_items") or []
            if isinstance(item, dict) and item.get("uri") and item.get("sha256")
        }
        discovery = store.append_discovery_run(
            {
                "source": "full_inventory",
                "updated_uris": sorted(evidence_revisions),
                "unmatched_uris": sorted(evidence_revisions),
                "processed_uris": sorted(evidence_revisions),
                "evidence_revisions": evidence_revisions,
                "inventory_run_id": run["run_id"],
                "candidate_ids": candidate_ids,
                "status": "triaged" if candidate_ids else "triage_no_candidate",
                "triage_status": "complete",
                "triage_remaining": 0,
            }
        )
        # append_discovery_run is evidence-idempotent and may intentionally
        # return a prior run for an unchanged snapshot. Preserve that run's
        # provenance instead of rewriting it to the newer inventory id.
        if str(discovery.get("inventory_run_id") or run["run_id"]) == str(run["run_id"]):
            discovery = store.update_discovery_run(
                str(discovery["run_id"]),
                inventory_run_id=run["run_id"],
                candidate_ids=candidate_ids,
                status="triaged" if candidate_ids else "triage_no_candidate",
                triage_status="complete",
                triage_remaining=0,
            )
        finished_at = now_iso()
        triage_status = "partial-complete" if run.get("unreadable_uris") else "complete"
        decision_counts = {name: sum(1 for item in accepted if item.get("decision") == name) for name in ("new_concept", "alias", "merge", "ignore")}
        result_contract = {
            "schema_version": INVENTORY_SCHEMA,
            "run_id": run["run_id"],
            "snapshot": {
                "status": "ok",
                "resource_count": len(snapshot_resources),
                "file_count": len(snapshot_resources),
                "roots": list(run.get("roots") or roots),
                "excluded": list(run.get("excluded_prefixes") or excludes),
                "snapshot_hash": run.get("resource_snapshot_hash"),
                "scan_mode": "full_document_deep_read" if len(resources) >= len(snapshot_resources) else "full_metadata_bounded_deep_read",
                "deep_read_count": len(resources),
                "deep_read_limit": int(run.get("max_evidence") or len(resources)),
                "deep_read_coverage": round(len(resources) / len(snapshot_resources), 4) if snapshot_resources else 0,
            },
            "discovery_run_ids": [str(discovery["run_id"])],
            "candidate_ids": candidate_ids,
            "decision_counts": decision_counts,
            "decisions": [
                {key: item.get(key) for key in ("decision", "name", "target", "aliases", "evidence_uris", "confidence")}
                for item in accepted
            ],
            "triage": {
                "status": triage_status,
                "processed": len(run.get("evidence_items") or []),
                "unreadable": len(run.get("unreadable_uris") or []),
            },
            "finished_at": finished_at,
        }
        return _persist(
            store,
            run,
            status="completed",
            completed_at=finished_at,
            finished_at=finished_at,
            decisions=accepted,
            candidate_ids=candidate_ids,
            discovery_run_ids=result_contract["discovery_run_ids"],
            snapshot=result_contract["snapshot"],
            triage=result_contract["triage"],
            result=result_contract,
            summary={
                "resources": len(snapshot_resources),
                "deep_read_selected": len(resources),
                "readable": len(run.get("evidence_items") or []),
                "unreadable": len(run.get("unreadable_uris") or []),
                "raw_decisions": len(raw_decisions),
                "final_decisions": len(accepted),
                "new_candidates": len(candidate_ids),
            },
        )
    except Exception as exc:
        _persist(store, run, status="failed", error=f"{type(exc).__name__}: {exc}", recoverable=True)
        raise


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="递归盘点 OpenViking 全量文档并生成概念 Candidate")
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--state-dir", type=Path, help="Control Plane run root; accepted for a uniform runner contract")
    parser.add_argument("--root", action="append", dest="roots")
    parser.add_argument("--exclude", action="append", dest="excludes")
    parser.add_argument("--node-limit", type=int, default=50000)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--evidence-limit", type=int, default=3000)
    parser.add_argument("--max-evidence", type=int, default=800)
    parser.add_argument("--workers", type=int, default=1, help="并发深读批次数；checkpoint 仍按原文档顺序提交")
    parser.add_argument("--auto-approve-publish", action="store_true", help="将本次新概念默认批准并直接发布为 Active")
    parser.add_argument("--seed-term", action="append", dest="seed_terms")
    parser.add_argument("--resume-run-id")
    parser.add_argument("--resume-latest", action="store_true", help="resume the newest non-terminal inventory checkpoint")
    parser.add_argument("--result-path", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("legacy-full-inventory")
    store = ConceptLearningStore(args.codex_root.expanduser() / "skills" / "shengsuan-concepts")
    try:
        seed_terms = list(dict.fromkeys([*(args.seed_terms or []), *manual_seed_terms(args.state_dir)]))
        resume_run_id = args.resume_run_id or (latest_resumable_run_id(store) if args.resume_latest else None)
        result = execute(
            store,
            OpenVikingClient(),
            roots=args.roots or DEFAULT_ROOTS,
            excludes=args.excludes or DEFAULT_EXCLUDES,
            node_limit=args.node_limit,
            batch_size=max(1, args.batch_size),
            evidence_limit=max(500, args.evidence_limit),
            max_evidence=max(1, args.max_evidence),
            workers=max(1, args.workers),
            seed_terms=seed_terms,
            resume_run_id=resume_run_id,
            auto_approve_publish=args.auto_approve_publish,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False, indent=2))
        return 1
    contract = result.get("result") if isinstance(result.get("result"), dict) else result
    if args.result_path:
        _atomic_json(args.result_path.expanduser(), contract)
    print(json.dumps(contract, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
