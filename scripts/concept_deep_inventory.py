#!/usr/bin/env python3
"""Resumable full-document concept inventory with bounded LLM reduce.

Every resource is paged to EOF and hashed locally. Deterministic term mapping
produces compact cross-document groups; only those groups are sent to the LLM.
Candidates are written only after every LLM batch succeeds and always remain in
``ready_for_review`` for the normal human review flow.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml

from concept_full_inventory import (
    DEFAULT_EXCLUDES,
    DEFAULT_ROOTS,
    OpenVikingClient,
    _extract_text,
    enumerate_resources,
    manual_seed_terms,
    taxonomy,
)
from concept_learning import ConceptLearningStore, content_hash, make_candidate, now_iso
from concept_refresh_adapter import append_agent_audit
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


MANIFEST_SCHEMA = "concept-learning.deep-inventory.v2"
EVIDENCE_SCHEMA = "concept-learning.deep-inventory.evidence-batch.v1"
EVIDENCE_CACHE_SCHEMA = "concept-learning.deep-inventory.evidence-cache.v1"
BASELINE_SCHEMA = "concept-learning.deep-inventory.incremental-baseline.v1"
CONTENT_DEDUP_SCHEMA = "concept-learning.deep-inventory.content-dedup.v1"
LLM_BATCH_SCHEMA = "concept-learning.deep-inventory.llm-batch.v2"
RESULT_SCHEMA = "concept-learning.deep-inventory.result.v2"
TERM_GROUPS_SCHEMA = "concept-learning.deep-inventory.term-groups.v3"
TRIAGE_SELECTION_SCHEMA = "concept-learning.deep-inventory.triage-selection.v2"
ARTIFACT_STORAGE_SCHEMA = "concept-learning.deep-inventory.artifact-storage.v1"
TERMINAL_STATUSES = {"completed"}

# Evidence artifacts can be several megabytes per batch.  Persisting the
# complete artifact after every URI multiplies disk I/O without improving the
# resume contract: the batch remains replayable as long as a bounded number of
# completed futures can be replayed.  Keep the defaults conservative so a
# long-running scan still leaves a recent checkpoint.
EVIDENCE_CHECKPOINT_EVERY = 8
EVIDENCE_CHECKPOINT_INTERVAL_SECONDS = 2.0
EVIDENCE_CACHE_FLUSH_EVERY = 8
EVIDENCE_CACHE_FLUSH_INTERVAL_SECONDS = 2.0

# The terms/excerpts stored in the cross-run evidence cache are derived from
# the full body and the manually seeded terms.  Bump this when extraction
# semantics change so an old cache can never silently hide a newly seeded term.
TERM_EXTRACTION_SCHEMA = "concept-learning.deep-inventory.terms.v1"

ASCII_TERM_RE = re.compile(
    r"(?<![A-Za-z0-9_])[A-Z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*(?![A-Za-z0-9_])"
)
CONTROLLED_PRODUCT_TERMS = ("dataAgent", "datasearch")
CONTROLLED_PRODUCT_TERM_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:"
    + "|".join(re.escape(term) for term in CONTROLLED_PRODUCT_TERMS)
    + r")(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
CHINESE_TERM_RE = re.compile(r"[\u4e00-\u9fff]{3,16}")
STRUCTURAL_LINE_RE = re.compile(r"^\s*(?:#{1,6}\s+|[-*+]\s+|\|)")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)
RANDOM_ID_RE = re.compile(
    r"^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)[A-Za-z0-9_]{8,}$"
)
ASCII_CODE_MARKERS = (
    "accesspolicy",
    "buffer",
    "clientconfig",
    "endpoint",
    "searchcontext",
    "_belong_",
    "_data",
    "_from",
    "_local",
    "_task",
    "_test",
)
GENERIC = {
    "数据",
    "平台",
    "功能",
    "概述",
    "介绍",
    "说明",
    "使用",
    "配置",
    "管理",
    "文档",
    "问题",
    "方案",
    "背景",
    "结果",
    "版本",
    "能力边界",
    "已知限制",
}
ASCII_NOISE = {
    "accept",
    "action",
    "active",
    "add",
    "all",
    "and",
    "any",
    "api",
    "array",
    "batch",
    "bigint",
    "boolean",
    "body",
    "case",
    "code",
    "column",
    "comment",
    "count",
    "cpu",
    "create",
    "csv",
    "data",
    "date",
    "decimal",
    "default",
    "delete",
    "dict",
    "distinct",
    "double",
    "else",
    "empty",
    "end",
    "error",
    "exception",
    "execute",
    "expression",
    "false",
    "file",
    "filter",
    "float",
    "from",
    "full",
    "get",
    "global",
    "gpu",
    "group",
    "host",
    "html",
    "input",
    "int",
    "integer",
    "java",
    "join",
    "json",
    "key",
    "left",
    "link",
    "list",
    "local",
    "long",
    "main",
    "manage",
    "management",
    "map",
    "max",
    "md5",
    "merged",
    "mode",
    "model",
    "modify",
    "mon",
    "none",
    "not",
    "null",
    "object",
    "open",
    "operatorid",
    "order",
    "path",
    "pdf",
    "post",
    "project",
    "put",
    "python",
    "resource",
    "review",
    "row",
    "running",
    "scala",
    "select",
    "server",
    "service",
    "set",
    "source",
    "string",
    "struct",
    "success",
    "sum",
    "system",
    "table",
    "task",
    "text",
    "then",
    "timestamp",
    "todo",
    "token",
    "tool",
    "transform",
    "true",
    "type",
    "union",
    "url",
    "use",
    "user",
    "user-agent",
    "valueerror",
    "varchar",
    "view",
    "when",
    "where",
    "xml",
}
CHINESE_NOISE = {
    "一级功能",
    "一级功能点",
    "一级信息项",
    "二级功能",
    "二级功能点",
    "二级信息项",
    "三级功能",
    "上线时间",
    "下划线",
    "不做有损失",
    "不做的损失",
    "不超过",
    "不支持",
    "为什么做",
    "主要变更内容",
    "产生的价值是什么",
    "交互说明",
    "什么客户",
    "仅支持",
    "优先级",
    "做了有收益",
    "做了的收益",
    "做什么",
    "修订记录",
    "兼容性要求",
    "创建人",
    "创建时间",
    "前置条件",
    "功能列表",
    "功能描述",
    "功能清单",
    "功能点",
    "功能详述",
    "功能说明",
    "参数位置",
    "参数名称",
    "参数名",
    "参考资料",
    "变更人",
    "响应参数",
    "响应示例",
    "响应头域",
    "响应码",
    "基本信息",
    "字段信息",
    "字段名称",
    "字段名",
    "字段描述",
    "字段类型",
    "字段说明",
    "性能要求",
    "效果验证方法",
    "接口描述",
    "是否必填",
    "更新时间",
    "最大值",
    "默认值",
    "请求参数",
    "请求示例",
    "请求结构",
    "请求头域",
    "运行中",
    "运营需求",
    "错误信息",
    "错误描述",
    "错误码",
}
PRODUCT_HINTS = (
    "agent",
    "builder",
    "catalog",
    "data",
    "fde",
    "foundry",
    "logic",
    "ontology",
    "search",
    "skill",
    "studio",
    "分析",
    "安全",
    "本体",
    "查询",
    "服务",
    "工作区",
    "工作台",
    "工作流",
    "工具",
    "管理",
    "监控",
    "计算",
    "检索",
    "目录",
    "模型",
    "权限",
    "数据",
    "知识",
    "管道",
    "算子",
    "调度",
    "资源",
    "资产",
)
CHINESE_ACTION_PREFIXES = (
    "创建",
    "删除",
    "修改",
    "编辑",
    "获取",
    "调用",
    "选择",
    "支持",
    "配置",
    "完成",
    "进行",
    "使用",
    "新建",
    "写入",
    "输出",
    "发布",
    "执行",
    "用于",
    "屏蔽",
    "将",
    "对",
    "和",
    "个",
    "条",
    "的",
)
CHINESE_FIELD_SUFFIXES = (
    "名称",
    "列表",
    "类型",
    "说明",
    "范围",
    "结果",
    "路径",
    "规格",
    "格式",
    "时间",
    "状态",
    "来源",
    "对象",
)
REQUIRED_CONTENT_HEADINGS = (
    "## 定义",
    "## 能力边界",
    "## 已知限制",
    "## 版本演进",
    "## 关联概念",
    "## 出现过的客户/评估",
    "## 证据与待确认点",
)
REQUIRED_FRONTMATTER_FIELDS = (
    "concept",
    "aliases",
    "category",
    "last_updated",
    "sources",
    "related_concepts",
    "related_customers",
    "latest_version",
)


def _atomic_json(path: Path, value: Any) -> None:
    """Atomically write JSON, optionally using a deterministic gzip stream.

    Large run artifacts use a ``.json.gz`` suffix.  Keeping the transaction
    as temp-file + fsync + replace is important: a killed scan must leave the
    previous checkpoint readable for resume.  Small manifests remain plain
    JSON so existing Control Plane readers and operators can inspect them.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        if path.name.endswith(".gz"):
            # ``mtime=0`` makes identical checkpoints byte-stable, which keeps
            # backup/diff tooling from reporting changes caused only by gzip
            # header timestamps.
            with os.fdopen(fd, "wb") as raw:
                with gzip.GzipFile(
                    fileobj=raw,
                    mode="wb",
                    compresslevel=1,
                    mtime=0,
                ) as encoded:
                    with io.TextIOWrapper(encoded, encoding="utf-8") as stream:
                        json.dump(value, stream, ensure_ascii=False, indent=2)
                        stream.write("\n")
                        stream.flush()
                raw.flush()
                os.fsync(raw.fileno())
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_json(path: Path, default: Any = None) -> Any:
    """Read plain or gzip JSON with a bidirectional filename fallback.

    New callers use ``.json.gz`` for large artifacts.  A missing compressed
    file falls back to the same path without ``.gz`` so runs created by older
    versions can be resumed.  Conversely, callers still asking for the old
    ``.json`` path can see a newly compressed artifact.  If an existing file
    is malformed, fail closed instead of silently selecting a stale sibling.
    """
    candidates = [path]
    if path.name.endswith(".gz"):
        candidates.append(Path(str(path)[:-3]))
    else:
        candidates.append(Path(f"{path}.gz"))
    for candidate in candidates:
        try:
            if candidate.name.endswith(".gz"):
                with gzip.open(candidate, "rt", encoding="utf-8") as stream:
                    return json.load(stream)
            return json.loads(candidate.read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError, json.JSONDecodeError):
            return default
    return default


def _normalize_term(value: Any) -> str:
    normalized = re.sub(r"[\s_\-./\u00b7]+", "", str(value or "").strip()).casefold()
    # ``PiplineBuilder`` is a repeated historical spelling in source docs.
    # Normalizing it here lets the Active ``pipeline`` alias win before the
    # term can become another Candidate, without maintaining a duplicate
    # product alias in config.
    return normalized.replace("pipline", "pipeline")


ACTIVE_MATCH_FIELDS = ("active_match", "active_match_candidates")
# A two-character overlap is useful for Chinese product terms (for example
# ``行权限`` -> ``行列权限``), but generic words such as ``数据`` would create
# false matches across almost every concept.  Longer overlaps are accepted
# without this allow-list because they carry enough lexical signal by
# themselves.
ACTIVE_OVERLAP_MARKERS = {
    "资源",
    "权限",
}
ACTIVE_RESOURCE_TERM_MARKERS = {
    "计算资源",
    "资源池",
    "资源组",
    "资源配置",
    "资源规格",
    "资源调度",
    "资源队列",
    "队列资源",
}
ACTIVE_GENERIC_OVERLAPS = {
    "数据",
    "管理",
    "功能",
    "任务",
    "信息",
    "系统",
    "应用",
    "配置",
    "支持",
}
ACTIVE_GENERIC_ASCII_OVERLAPS = {
    "data",
    "dataset",
    "model",
    "service",
    "system",
    "builder",
    "search",
    "manager",
    "agent",
    "line",
    "and",
    "or",
    "not",
    "select",
    "from",
    "where",
    "join",
    "into",
    "null",
    "true",
    "false",
}


def _row_is_active(row: Mapping[str, Any]) -> bool:
    """Return whether a taxonomy row represents a formal active concept.

    ``taxonomy()`` historically returned only ``name``/``aliases``/``source``
    and callers also pass hand-built rows in tests.  Missing status therefore
    means active for backwards compatibility.  Pending Candidates are never
    allowed to become an active match.
    """

    source = str(row.get("source") or "").strip().casefold()
    if source == "candidate":
        return False
    status = str(row.get("status") or "active").strip().casefold()
    return status == "active"


def _active_concept_index(
    existing: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Index active canonical names and aliases by normalized spelling.

    The value is a list rather than a single row so an accidentally shared
    alias remains visible as ambiguous evidence.  Callers can then avoid
    silently assigning a new term to the wrong concept.
    """

    index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    # Prefer an explicitly active ledger row over its config duplicate while
    # retaining aliases from both rows.  This also makes the helper useful for
    # the 40 config concepts plus the four ledger-only active concepts.
    by_target: Dict[str, Dict[str, Any]] = {}
    for raw in existing:
        if not isinstance(raw, Mapping) or not _row_is_active(raw):
            continue
        name = str(raw.get("name") or "").strip()
        canonical_key = _normalize_term(name)
        if not canonical_key:
            continue
        row = by_target.setdefault(
            canonical_key,
            {
                "name": name,
                "aliases": [],
                "category": str(raw.get("category") or "product_capability").strip(),
                "source": str(raw.get("source") or "").strip(),
            },
        )
        if str(raw.get("source") or "").strip().casefold() == "active":
            row["source"] = "active"
        category = str(raw.get("category") or "").strip()
        if category and row.get("category") == "product_capability":
            row["category"] = category
        for value in [*list(raw.get("aliases") or [])]:
            alias = str(value).strip()
            if alias and alias not in row["aliases"]:
                row["aliases"].append(alias)

    for row in by_target.values():
        surfaces = [str(row["name"]), *[str(value) for value in row["aliases"]]]
        for surface in surfaces:
            key = _normalize_term(surface)
            if not key:
                continue
            item = {
                "target": str(row["name"]),
                "surface": surface,
                "surface_key": key,
                "category": str(row.get("category") or "product_capability"),
            }
            if not any(
                existing_item.get("target") == item["target"]
                and existing_item.get("surface_key") == item["surface_key"]
                for existing_item in index[key]
            ):
                index[key].append(item)
    return index


def _active_taxonomy_rows(
    existing: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Return a compact canonical-only taxonomy for prompts and triage."""

    index = _active_concept_index(existing)
    rows: Dict[str, Dict[str, Any]] = {}
    for items in index.values():
        for item in items:
            target = str(item.get("target") or "").strip()
            target_key = _normalize_term(target)
            if not target_key:
                continue
            row = rows.setdefault(
                target_key,
                {
                    "name": target,
                    "aliases": [],
                    "category": str(item.get("category") or "product_capability"),
                    "source": "active",
                    "status": "active",
                },
            )
            surface = str(item.get("surface") or "").strip()
            if surface and _normalize_term(surface) != target_key and surface not in row["aliases"]:
                row["aliases"].append(surface)
    return sorted(rows.values(), key=lambda row: _normalize_term(row.get("name")))


def _longest_common_substring(left: str, right: str) -> str:
    """Return the longest contiguous substring shared by two normalized terms."""

    if not left or not right:
        return ""
    # Terms are short (usually < 32 chars), so the straightforward dynamic
    # program is clearer and cheaper than pulling in a tokenization package.
    previous = [0] * (len(right) + 1)
    best = ""
    for left_char_index, left_char in enumerate(left, 1):
        current = [0] * (len(right) + 1)
        for right_char_index, right_char in enumerate(right, 1):
            if left_char == right_char:
                current[right_char_index] = previous[right_char_index - 1] + 1
                length = current[right_char_index]
                if length > len(best):
                    best = left[left_char_index - length : left_char_index]
        previous = current
    return best


def _is_subsequence(shorter: str, longer: str) -> bool:
    if not shorter or len(shorter) > len(longer):
        return False
    cursor = iter(longer)
    return all(char in cursor for char in shorter)


def _active_match_for_term(
    term: str,
    active_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Optional[Dict[str, Any]]:
    """Find one unambiguous active concept for a discovered term.

    Exact normalized matches are authoritative.  A small, deterministic
    lexical fallback catches product sub-capabilities that the extractor
    emits as separate Chinese phrases (``资源队列``/``行权限``).  Ambiguous
    fuzzy matches are deliberately left for the LLM and are not forced into a
    new concept by this helper.
    """

    term_text = str(term or "").strip()
    term_key = _normalize_term(term_text)
    if not term_key:
        return None

    def choose_exact(items: Sequence[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        targets = {str(item.get("target") or "").strip() for item in items if item.get("target")}
        targets.discard("")
        if len(targets) != 1:
            return None
        item = next(item for item in items if str(item.get("target") or "").strip() in targets)
        target = str(item.get("target") or "").strip()
        return {
            "target": target,
            "matched_surface": str(item.get("surface") or target),
            "match_type": "exact",
            "decision": "alias",
            "score": 1.0,
            "category": str(item.get("category") or "product_capability"),
        }

    exact = choose_exact(active_index.get(term_key) or [])
    if exact:
        return exact

    scored: List[Tuple[float, int, str, Dict[str, Any]]] = []
    for surface_key, items in active_index.items():
        if not surface_key:
            continue
        common = _longest_common_substring(term_key, surface_key)
        common_len = len(common)
        if common_len < 2:
            continue
        common_folded = common.casefold()
        if common_folded in ACTIVE_GENERIC_ASCII_OVERLAPS:
            continue
        if common == "结构化" and not term_key.startswith("非结构化"):
            continue
        term_contains_surface = surface_key in term_key
        surface_contains_term = term_key in surface_key
        containment = term_contains_surface or surface_contains_term
        permission_suffix = common.endswith("权限") and term_key.endswith("权限")
        resource_overlap = common == "资源" and any(
            marker in term_key for marker in ACTIVE_RESOURCE_TERM_MARKERS
        )
        controlled_overlap = resource_overlap or permission_suffix
        # A broader discovered phrase must not be folded into a narrower
        # Active concept merely because the Active alias contains it.  For
        # example ``大模型`` is not automatically ``大模型算子``.  Resource
        # and row/column permission fragments are the deliberate exceptions
        # because those are governed sub-capability vocabularies.
        if surface_contains_term and not term_contains_surface and not controlled_overlap:
            continue
        # For non-containment matches only governed domain markers are strong
        # enough to force an existing-concept merge.  This avoids treating
        # arbitrary English substrings (``age`` in Agent) or generic Chinese
        # prefixes (``数据`` in 数据分析) as a product match.
        if not containment:
            if not controlled_overlap:
                continue
        # A two-character overlap is only meaningful for known product/domain
        # markers.  This filters generic ``数据``/``管理`` collisions.
        if common_len == 2 and (
            common in ACTIVE_GENERIC_OVERLAPS
            or (not term_contains_surface and not controlled_overlap)
        ):
            continue
        shorter = min(len(term_key), len(surface_key))
        longer = max(len(term_key), len(surface_key))
        score = common_len / shorter
        if term_contains_surface:
            # Prefer the longest Active surface covered by the discovered
            # phrase.  This makes ``数据集成任务`` choose 数据集成 rather than
            # the shorter overlapping 数据集.
            score = 0.58 + 0.42 * (len(surface_key) / len(term_key))
        elif surface_contains_term:
            score = 0.58 + 0.42 * (len(term_key) / len(surface_key))
        elif common_len == 2:
            score *= 0.9
        elif score < 0.55:
            # A long-looking overlap can still be a coincidental prefix.  A
            # stronger ratio is required unless the phrase is an explicit
            # containment match handled above.
            continue
        for item in items:
            target = str(item.get("target") or "").strip()
            if not target:
                continue
            scored.append((score, common_len, surface_key, dict(item)))

    if not scored:
        return None

    # Prefer an explicit subsequence match (行权限 -> 行列权限) when the
    # overlap itself is tied with a broader concept such as 权限体系.
    for index, (score, common_len, surface_key, item) in enumerate(scored):
        containment = term_key in surface_key or surface_key in term_key
        if not containment and (
            _is_subsequence(term_key, surface_key) or _is_subsequence(surface_key, term_key)
        ):
            scored[index] = (score + 0.12, common_len, surface_key, item)
    scored.sort(key=lambda value: (-value[0], -value[1], -len(value[2]), value[3]["target"], value[2]))
    best_score, best_common_len, best_surface_key, best_item = scored[0]
    # If two different active concepts are effectively tied, do not make a
    # silent merge.  The prompt will show the existing taxonomy for review.
    top_targets: Dict[str, float] = {}
    for score, _length, _surface, item in scored:
        target = str(item.get("target") or "").strip()
        if target:
            top_targets[target] = max(top_targets.get(target, 0.0), score)
    ranked_targets = sorted(top_targets.items(), key=lambda value: (-value[1], value[0]))
    if len(ranked_targets) > 1 and ranked_targets[0][1] - ranked_targets[1][1] <= 0.05:
        return None
    target = str(best_item.get("target") or "").strip()
    if not target:
        return None
    return {
        "target": target,
        "matched_surface": str(best_item.get("surface") or target),
        "match_type": "fuzzy",
        "decision": "merge",
        "score": round(min(0.99, max(0.65, best_score)), 4),
        "common": _longest_common_substring(term_key, best_surface_key),
        "category": str(best_item.get("category") or "product_capability"),
    }


def _active_match_candidates(
    term: str,
    active_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    """Expose deterministic match metadata without forcing ambiguous targets."""

    match = _active_match_for_term(term, active_index)
    return [match] if match else []


def _is_noise_term(term: str) -> bool:
    value = str(term or "").strip()
    if not value or len(value) > 48:
        return True
    folded = value.casefold()
    if folded in ASCII_NOISE or value in CHINESE_NOISE:
        return True
    if RANDOM_ID_RE.fullmatch(value) or re.fullmatch(r"[A-Z]\d+", value):
        return True
    if re.search(r"-\d{3,}$", value):
        return True
    if "_" in value and value.upper() == value:
        return True
    if any(marker in folded for marker in ASCII_CODE_MARKERS):
        return True
    if re.fullmatch(r"\d+(?:\.\d+)*", value):
        return True
    chinese = CHINESE_TERM_RE.fullmatch(value)
    if chinese:
        if len(value) > 12:
            return True
        if value.startswith(CHINESE_ACTION_PREFIXES):
            return True
        if value.endswith(CHINESE_FIELD_SUFFIXES):
            return True
        if value.endswith(("中", "的", "与", "和")):
            return True
        if any(marker in value for marker in ("随意", "是一层", "的数据", "的模型")):
            return True
    return False


def _term_quality_score(term: str) -> Optional[int]:
    if _is_noise_term(term):
        return None
    value = str(term).strip()
    folded = value.casefold()
    score = 0
    if any(hint in folded for hint in PRODUCT_HINTS):
        score += 600
    if CHINESE_TERM_RE.fullmatch(value):
        score += 220
        if 3 <= len(value) <= 8:
            score += 80
    elif "-" in value or "_" in value:
        score += 320
    elif re.search(r"[a-z][A-Z]", value):
        score += 320
    elif value.isupper() and 2 <= len(value) <= 8:
        score += 170
    elif value[:1].isupper() and value[1:].islower():
        score += 120
    else:
        score += 60
    return score


def _snapshot_hash(uris: Sequence[str]) -> str:
    digest = hashlib.sha256("\n".join(uris).encode("utf-8")).hexdigest()
    return "sha256:" + digest


def _new_run_id() -> str:
    stamp = now_iso().replace(":", "").replace("-", "")
    return f"deep-inventory-{stamp}-{uuid.uuid4().hex[:6]}"


def _run_root(state_dir: Path, run_id: str) -> Path:
    return state_dir.expanduser() / "runs" / run_id


def _manifest_path(state_dir: Path, run_id: str) -> Path:
    return _run_root(state_dir, run_id) / "manifest.json"


def _flat_manifest_path(state_dir: Path, run_id: str) -> Path:
    return state_dir.expanduser() / "runs" / f"{run_id}.json"


def _evidence_path(run_root: Path, index: int) -> Path:
    # New checkpoints are compressed; `_read_json` transparently falls back to
    # the legacy `.json` sibling when resuming a run created before v1 storage.
    return run_root / "evidence" / f"batch-{index:05d}.json.gz"


def _evidence_cache_path(state_dir: Path) -> Path:
    """Return the preferred compressed cross-run evidence cache location.

    `_read_json` falls back to the historical `.json` sibling, so changing
    this write path does not invalidate an existing baseline or cache.
    """
    return state_dir.expanduser() / "evidence-cache.json.gz"


def _baseline_path(state_dir: Path) -> Path:
    """Return the committed source/evidence baseline for incremental runs."""
    return state_dir.expanduser() / "incremental-baseline.json"


def _content_dedup_path(state_dir: Path) -> Path:
    """Return the preferred compressed content-hash index location.

    Historical `content-dedup.json` files remain readable through the shared
    plain/gzip JSON loader and are never removed by the runner.
    """
    return state_dir.expanduser() / "content-dedup.json.gz"


def _normalize_source_revision(value: Any) -> str:
    """Normalize a ledger SHA-256 while rejecting weak/non-content versions."""
    text = str(value or "").strip()
    if text.lower().startswith("sha256:"):
        text = text[7:]
    if not re.fullmatch(r"[0-9a-fA-F]{64}", text):
        return ""
    return "sha256:" + text.lower()


def _source_revision_map(skill_root: Path) -> Dict[str, str]:
    """Load trusted URI revisions from the two local document ledgers.

    OpenViking exposes modification metadata but no stable body hash.  The
    sync ledgers are therefore the only safe source for cross-run reuse.  A
    conflicting hash for the same URI is dropped rather than guessed.
    """
    codex_root = skill_root.expanduser().resolve().parent.parent
    ledger_paths = (
        codex_root / "skills" / "shengsuan-sync" / "state" / "ledger.json",
        codex_root / "skills" / "databuilder-public-docs" / "state" / "ledger.json",
    )
    revisions: Dict[str, str] = {}
    conflicted: set[str] = set()
    for ledger_path in ledger_paths:
        ledger = _read_json(ledger_path, {})
        if not isinstance(ledger, dict):
            continue
        for row in ledger.values():
            if not isinstance(row, dict):
                continue
            uri = str(
                row.get("target_uri")
                or row.get("viking_uri")
                or row.get("uri")
                or ""
            ).strip()
            revision = _normalize_source_revision(
                row.get("sha256") or row.get("content_hash")
            )
            if not uri or not revision or uri in conflicted:
                continue
            previous = revisions.get(uri)
            if previous and previous != revision:
                revisions.pop(uri, None)
                conflicted.add(uri)
                continue
            revisions[uri] = revision
    return revisions


def _source_revision_for_uri(
    uri: str,
    revisions: Mapping[str, str],
) -> str:
    """Resolve a trusted source hash for a leaf URI.

    Sync ledgers usually point at the imported resource directory (for
    example ``.../page.html``), while ``glob`` returns generated Markdown
    leaves below that directory.  Exact mappings win; otherwise use the
    longest unambiguous parent prefix.  A conflicting prefix is deliberately
    treated as unknown so a stale cache can never be reused by guesswork.
    """
    value = str(uri or "").strip()
    if not value:
        return ""
    exact = str(revisions.get(value) or "").strip()
    if exact:
        return exact
    matches: List[Tuple[int, str]] = []
    for parent, revision in revisions.items():
        prefix = str(parent or "").strip().rstrip("/")
        candidate = str(revision or "").strip()
        if not prefix or not candidate:
            continue
        if value.startswith(prefix + "/"):
            matches.append((len(prefix), candidate))
    if not matches:
        return ""
    longest = max(length for length, _ in matches)
    candidates = {revision for length, revision in matches if length == longest}
    return next(iter(candidates)) if len(candidates) == 1 else ""


def _resolved_source_revisions(
    uris: Sequence[str],
    revisions: Mapping[str, str],
) -> Dict[str, str]:
    """Build the per-leaf revision map used by evidence scanning/cache."""
    return {
        str(uri): revision
        for uri in uris
        if (revision := _source_revision_for_uri(str(uri), revisions))
    }


def _load_baseline(path: Path) -> Dict[str, Any]:
    """Load a committed incremental baseline, tolerating legacy/partial files."""
    value = _read_json(path, {})
    if not isinstance(value, dict):
        return {}
    if value.get("schema_version") != BASELINE_SCHEMA:
        return {}
    return value


def _baseline_revision_rows(value: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Normalize baseline revision rows for changed-document comparison."""
    rows = value.get("source_revisions")
    if isinstance(rows, dict):
        normalized: Dict[str, Dict[str, Any]] = {}
        for uri, raw in rows.items():
            if isinstance(raw, Mapping):
                revision = str(raw.get("revision") or "").strip()
                if revision:
                    normalized[str(uri)] = dict(raw)
            elif raw is not None and str(raw).strip():
                normalized[str(uri)] = {"revision": str(raw).strip()}
        return normalized
    # A compact map was used by an early prototype.  Reading it here keeps a
    # partially materialized baseline useful without weakening hash checks.
    revisions = value.get("revisions")
    if isinstance(revisions, dict):
        return {
            str(uri): {"revision": str(revision)}
            for uri, revision in revisions.items()
            if str(uri).strip() and str(revision).strip()
        }
    return {}


def _changed_documents(
    uris: Sequence[str],
    current_revisions: Mapping[str, str],
    previous_baseline: Mapping[str, Any],
) -> Dict[str, Any]:
    """Classify source URIs without guessing when a current hash is missing.

    Missing current revisions are deliberately reported as ``unknown`` and
    retained in ``changed_uris``.  Callers can therefore safely force a read;
    an absent ledger hash never silently turns into an unchanged document.
    """
    current = {str(uri): str(revision) for uri, revision in current_revisions.items() if str(uri).strip()}
    previous = _baseline_revision_rows(previous_baseline)
    ordered = list(dict.fromkeys(str(uri) for uri in uris if str(uri).strip()))
    changed: List[str] = []
    unchanged: List[str] = []
    unknown: List[str] = []
    new: List[str] = []
    for uri in ordered:
        current_revision = str(current.get(uri) or "")
        previous_revision = str((previous.get(uri) or {}).get("revision") or "")
        if not current_revision:
            unknown.append(uri)
            changed.append(uri)
        elif not previous_revision:
            new.append(uri)
            changed.append(uri)
        elif current_revision != previous_revision:
            changed.append(uri)
        else:
            unchanged.append(uri)
    current_set = set(ordered)
    removed = sorted(set(previous) - current_set)
    return {
        "baseline_exists": bool(previous_baseline),
        "changed_uris": changed,
        "unchanged_uris": unchanged,
        "unknown_revision_uris": unknown,
        "new_uris": new,
        "removed_uris": removed,
        "changed_count": len(changed),
        "unchanged_count": len(unchanged),
        "unknown_revision_count": len(unknown),
        "new_count": len(new),
        "removed_count": len(removed),
    }


def _content_dedup_summary(
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build a URI-preserving content hash index.

    Identical bodies share a group record, while every URI remains listed in
    that group so source attribution and Active concept intersections are not
    lost.  The index intentionally stores metadata/terms only; full text
    remains in the existing evidence checkpoint artifacts.
    """
    groups: Dict[str, Dict[str, Any]] = {}
    uri_to_group: Dict[str, str] = {}
    for document in documents:
        uri = str(document.get("uri") or "").strip()
        digest = str(document.get("content_hash") or "").strip()
        if not uri or not digest:
            continue
        uri_to_group[uri] = digest
        group = groups.setdefault(
            digest,
            {
                "content_hash": digest,
                "canonical_uri": uri,
                "uris": [],
                "char_count": int(document.get("char_count") or 0),
                "byte_count": int(document.get("byte_count") or 0),
                "page_count": int(document.get("page_count") or 0),
                "terms": [dict(item) for item in document.get("terms") or [] if isinstance(item, dict)],
            },
        )
        if uri not in group["uris"]:
            group["uris"].append(uri)
        # Prefer the most complete terms list if a legacy artifact contains
        # the same hash with a truncated record.
        if len(document.get("terms") or []) > len(group.get("terms") or []):
            group["terms"] = [dict(item) for item in document.get("terms") or [] if isinstance(item, dict)]
    for group in groups.values():
        group["uris"] = sorted(group["uris"])
        group["duplicate_count"] = max(0, len(group["uris"]) - 1)
    total = len(uri_to_group)
    unique = len(groups)
    duplicate = max(0, total - unique)
    return {
        "schema_version": CONTENT_DEDUP_SCHEMA,
        "document_count": total,
        "unique_content_count": unique,
        "duplicate_document_count": duplicate,
        "duplicate_ratio": round(duplicate / total, 6) if total else 0.0,
        "uri_to_content_hash": dict(sorted(uri_to_group.items())),
        "groups": {key: groups[key] for key in sorted(groups)},
    }


def _materialize_baseline(
    state_dir: Path,
    *,
    run_id: str,
    uris: Sequence[str],
    documents: Sequence[Mapping[str, Any]],
    source_revisions: Mapping[str, str],
    evidence_cache: Mapping[str, Mapping[str, Any]],
    previous_baseline: Mapping[str, Any],
    snapshot_hash: str,
) -> Dict[str, Any]:
    """Commit source/evidence/content indexes after a complete deep read."""
    by_uri = {
        str(document.get("uri")): document
        for document in documents
        if str(document.get("uri") or "").strip()
    }
    source_rows: Dict[str, Dict[str, Any]] = {}
    for uri in uris:
        value = str(uri)
        document = by_uri.get(value)
        if document is None:
            continue
        trusted = str(source_revisions.get(value) or "")
        revision = trusted or str(document.get("content_hash") or "")
        if not revision:
            continue
        source_rows[value] = {
            "revision": revision,
            "revision_kind": "ledger_sha256" if trusted else "inventory_content_hash",
            "trusted": bool(trusted),
            "content_hash": str(document.get("content_hash") or ""),
        }
    dedup = _content_dedup_summary(documents)
    changed = _changed_documents(uris, source_revisions, previous_baseline)
    resource_count = len(uris)
    deep_read_count = len(by_uri)
    trusted_hash_count = sum(1 for row in source_rows.values() if row.get("trusted"))
    cache_count = sum(1 for uri in uris if isinstance(evidence_cache.get(str(uri)), Mapping))
    source_coverage = trusted_hash_count / resource_count if resource_count else 1.0
    cache_coverage = cache_count / resource_count if resource_count else 1.0
    deep_coverage = deep_read_count / resource_count if resource_count else 1.0
    baseline_ready = bool(
        deep_read_count == resource_count
        and source_coverage == 1.0
        and cache_coverage == 1.0
    )
    baseline = {
        "schema_version": BASELINE_SCHEMA,
        "status": "ready" if baseline_ready else "incomplete",
        "baseline_ready": baseline_ready,
        "run_id": run_id,
        "materialized_at": now_iso(),
        "snapshot_hash": snapshot_hash,
        "resource_count": resource_count,
        "deep_read_count": deep_read_count,
        "deep_read_coverage": round(deep_coverage, 6),
        "source_hash_count": trusted_hash_count,
        "source_hash_coverage": round(source_coverage, 6),
        "content_hash_count": len(source_rows),
        "evidence_cache_count": cache_count,
        "evidence_cache_coverage": round(cache_coverage, 6),
        "source_revisions": dict(sorted(source_rows.items())),
        "changed_documents": changed,
        "content_dedup": {
            key: value
            for key, value in dedup.items()
            if key not in {"groups", "uri_to_content_hash"}
        },
        "content_dedup_artifact": _content_dedup_path(state_dir).name,
    }
    _atomic_json(_content_dedup_path(state_dir), dedup)
    _atomic_json(_baseline_path(state_dir), baseline)
    return baseline


def materialize_baseline_from_run(
    store: ConceptLearningStore,
    *,
    state_dir: Path,
    run_id: str,
) -> Dict[str, Any]:
    """Materialize cache/baseline indexes from a completed legacy run.

    This migration is intentionally evidence-only: it validates every old
    checkpoint, writes no Candidate/Active data, and tags hashes that are not
    present in a live sync ledger as ``inventory_content_hash``.  Such rows
    improve auditability and deduplication immediately but do *not* satisfy
    ``baseline_ready`` until the sync ledger's trusted SHA-256 is backfilled.
    """
    state_dir = state_dir.expanduser()
    manifest = _read_json(_manifest_path(state_dir, run_id), None)
    if not isinstance(manifest, dict):
        raise FileNotFoundError(run_id)
    if str(manifest.get("status") or "") != "completed":
        raise RuntimeError("baseline migration requires a completed inventory run")
    root = _run_root(state_dir, run_id)
    resources = _read_json(root / str(manifest.get("resources_artifact") or "resources.json"), {})
    uris = [str(uri) for uri in resources.get("uris") or [] if str(uri).strip()]
    if not uris or _snapshot_hash(uris) != manifest.get("resource_snapshot_hash"):
        raise RuntimeError("resource snapshot artifact does not match manifest")
    configured_batches = int((manifest.get("evidence") or {}).get("batch_count") or 0)
    batch_count = configured_batches or math.ceil(len(uris) / int((manifest.get("config") or {}).get("read_batch_size") or 50))
    for index in range(batch_count):
        artifact = _read_json(_evidence_path(root, index), {})
        if artifact.get("status") != "completed":
            raise RuntimeError(f"evidence checkpoint {index} is not complete")
        if artifact.get("errors"):
            raise RuntimeError(f"evidence checkpoint {index} contains errors")
    documents = _load_documents(root, batch_count)
    document_uris = {str(item.get("uri") or "") for item in documents}
    if len(documents) != len(uris) or document_uris != set(uris):
        raise RuntimeError("completed run does not cover the full resource snapshot")
    config = dict(manifest.get("config") or {})
    seed_terms = list(config.get("seed_terms") or [])
    terms_fingerprint = _terms_fingerprint(seed_terms)
    source_revisions = _resolved_source_revisions(
        uris,
        _source_revision_map(store.skill_root),
    )
    cache_path = _evidence_cache_path(state_dir)
    cache = _load_evidence_cache(cache_path)
    cache = _merge_evidence_cache(
        cache_path,
        cache,
        documents,
        source_revisions,
        terms_fingerprint,
        persist=False,
        allow_untrusted=True,
    )
    _persist_evidence_cache(cache_path, cache)
    previous_baseline = _load_baseline(_baseline_path(state_dir))
    baseline = _materialize_baseline(
        state_dir,
        run_id=run_id,
        uris=uris,
        documents=documents,
        source_revisions=source_revisions,
        evidence_cache=cache,
        previous_baseline=previous_baseline,
        snapshot_hash=str(manifest["resource_snapshot_hash"]),
    )
    baseline["materialized_from_run"] = run_id
    _atomic_json(_baseline_path(state_dir), baseline)
    manifest["baseline"] = baseline
    manifest["baseline_ready"] = bool(baseline.get("baseline_ready"))
    manifest["changed_documents"] = baseline.get("changed_documents") or {}
    manifest["content_dedup"] = baseline.get("content_dedup") or {}
    _persist_manifest(
        state_dir,
        manifest,
        baseline=baseline,
        baseline_ready=bool(baseline.get("baseline_ready")),
        changed_documents=baseline.get("changed_documents") or {},
        content_dedup=baseline.get("content_dedup") or {},
    )
    return {
        "status": "completed",
        "run_id": run_id,
        "baseline_ready": bool(baseline.get("baseline_ready")),
        "baseline": baseline,
        "cache_entry_count": len(cache),
    }


def _terms_fingerprint(seed_terms: Sequence[str]) -> str:
    normalized = sorted(
        {
            _normalize_term(term)
            for term in seed_terms
            if _normalize_term(term)
        }
    )
    payload = json.dumps(
        {"schema": TERM_EXTRACTION_SCHEMA, "seed_terms": normalized},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return content_hash(payload)


def _load_evidence_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    artifact = _read_json(path, {})
    if not isinstance(artifact, dict) or artifact.get("schema_version") != EVIDENCE_CACHE_SCHEMA:
        return {}
    entries = artifact.get("entries")
    if not isinstance(entries, dict):
        return {}
    return {
        str(uri): dict(entry)
        for uri, entry in entries.items()
        if str(uri).strip() and isinstance(entry, dict)
    }


def _cached_document(
    entry: Mapping[str, Any],
    *,
    uri: str,
    source_revision: str,
    terms_fingerprint: str,
) -> Optional[Dict[str, Any]]:
    """Validate and return one cache record, or None when it is stale."""
    if str(entry.get("source_revision") or "") != source_revision:
        return None
    if str(entry.get("terms_fingerprint") or "") != terms_fingerprint:
        return None
    record = entry.get("record")
    if not isinstance(record, dict) or str(record.get("uri") or "") != uri:
        return None
    required = ("content_hash", "char_count", "byte_count", "page_count", "terms")
    if any(field not in record for field in required) or not isinstance(record.get("terms"), list):
        return None
    try:
        normalized = {
            "uri": uri,
            "content_hash": str(record["content_hash"]),
            "char_count": int(record["char_count"]),
            "byte_count": int(record["byte_count"]),
            "page_count": int(record["page_count"]),
            "terms": [dict(item) for item in record["terms"] if isinstance(item, dict)],
        }
    except (TypeError, ValueError):
        return None
    if not normalized["content_hash"] or normalized["char_count"] < 0:
        return None
    return normalized


def _merge_evidence_cache(
    path: Path,
    entries: Mapping[str, Mapping[str, Any]],
    documents: Sequence[Mapping[str, Any]],
    source_revisions: Mapping[str, str],
    terms_fingerprint: str,
    *,
    persist: bool = True,
    allow_untrusted: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Merge successful records whose source has a trusted revision.

    ``persist=False`` lets a long scan batch several in-memory updates before
    rewriting the (potentially large) cache file.  The primary evidence
    checkpoints remain independent and are always safe to resume from.
    """
    merged = dict(entries)
    changed = False
    for document in documents:
        uri = str(document.get("uri") or "").strip()
        revision = str(source_revisions.get(uri) or "")
        if not revision and allow_untrusted:
            # A baseline materialization has just read the body completely.
            # Persisting its body hash is safe as an evidence record, but the
            # normal cross-run lookup still requires a live trusted ledger
            # revision before it can skip a read.
            revision = str(document.get("content_hash") or "")
        if not uri or not revision:
            continue
        record = {
            "uri": uri,
            "content_hash": str(document.get("content_hash") or ""),
            "char_count": int(document.get("char_count") or 0),
            "byte_count": int(document.get("byte_count") or 0),
            "page_count": int(document.get("page_count") or 0),
            "terms": [dict(item) for item in document.get("terms") or [] if isinstance(item, dict)],
        }
        if not record["content_hash"]:
            continue
        previous = merged.get(uri)
        stable_fields = {
            "source_revision": revision,
            "source_revision_kind": (
                "ledger_sha256" if uri in source_revisions else "inventory_content_hash"
            ),
            "content_hash": record["content_hash"],
            "terms_fingerprint": terms_fingerprint,
            "record": record,
        }
        if isinstance(previous, Mapping) and all(
            previous.get(key) == value for key, value in stable_fields.items()
        ):
            # Do not rewrite an unchanged entry just because the current run
            # reached it again; ``cached_at`` is intentionally stable.
            next_entry = dict(previous)
        else:
            next_entry = {**stable_fields, "cached_at": now_iso()}
        if previous != next_entry:
            merged[uri] = next_entry
            changed = True
    if changed and persist:
        _persist_evidence_cache(path, merged)
    return merged


def _persist_evidence_cache(
    path: Path,
    entries: Mapping[str, Mapping[str, Any]],
) -> None:
    """Atomically persist the current cross-run cache snapshot."""
    if not entries:
        return
    updated_at = now_iso()
    _atomic_json(
        path,
        {
            "schema_version": EVIDENCE_CACHE_SCHEMA,
            "updated_at": updated_at,
            "entries": dict(entries),
        },
    )
    # Control Plane status polling must not parse the potentially large cache
    # body just to show hit coverage.  Keep the sidecar name stable across the
    # plain-to-gzip migration; old caches without this file remain valid but
    # report unknown coverage until the next successful flush.
    _atomic_json(
        path.with_name("evidence-cache.meta.json"),
        {
            "schema_version": EVIDENCE_CACHE_SCHEMA,
            "updated_at": updated_at,
            "entry_count": len(entries),
        },
    )


def _llm_path(run_root: Path, index: int) -> Path:
    return run_root / "llm" / f"batch-{index:05d}.json"


def _term_groups_path(run_root: Path) -> Path:
    # Keep the historical basename/version and only add the transport suffix;
    # this avoids invalidating manifests or external run inspection tooling.
    return run_root / "term-groups-v2.json.gz"


def _triage_selection_path(run_root: Path) -> Path:
    return run_root / "triage-selection.json"


@contextmanager
def _single_flight(state_dir: Path) -> Iterable[None]:
    """Reject overlapping inventory processes, including direct CLI starts."""
    lock_path = state_dir.expanduser() / ".deep-inventory.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another deep concept inventory is already running") from exc
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _read_page(client: Any, uri: str, offset: int, limit: int) -> str:
    if hasattr(client, "read_content_page"):
        return str(client.read_content_page(uri, offset, limit) or "")
    value = client._request(
        "GET",
        "/api/v1/content/read",
        query={"uri": uri, "offset": offset, "limit": limit, "raw": "true"},
    )
    return _extract_text(value)


def read_full_document(
    client: Any,
    uri: str,
    page_size: int = 12000,
    max_pages: int = 10000,
) -> Dict[str, Any]:
    """Read one document by OpenViking line pages to EOF and hash the body."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    pages: List[str] = []
    offset = 0
    for _ in range(max_pages):
        page = _read_page(client, uri, offset, page_size)
        if not page:
            break
        pages.append(page)
        line_count = len(page.splitlines(keepends=True))
        if line_count <= 0:
            raise RuntimeError(f"content/read returned a page without lines for {uri}")
        offset += line_count
        if line_count < page_size:
            break
    else:
        raise RuntimeError(f"pagination exceeded max_pages for {uri}")
    text = "".join(pages)
    if not text.strip():
        raise ValueError("empty_evidence")
    return {
        "uri": uri,
        "content_hash": content_hash(text),
        "char_count": len(text),
        "byte_count": len(text.encode("utf-8")),
        "page_count": len(pages),
        "text": text,
    }


def _terms(text: str, seed_terms: Sequence[str] = ()) -> Iterable[str]:
    """Yield deterministic product-term candidates from the full body."""
    seen: set[str] = set()
    discovery_text = FENCED_CODE_RE.sub(" ", text)
    discovery_text = URL_RE.sub(" ", discovery_text)
    for match in ASCII_TERM_RE.finditer(discovery_text):
        term = match.group(0).strip("-_")
        if len(term) >= 3 and not _is_noise_term(term):
            key = _normalize_term(term)
            if key and key not in seen:
                seen.add(key)
                yield term
    for match in CONTROLLED_PRODUCT_TERM_RE.finditer(discovery_text):
        term = match.group(0)
        key = _normalize_term(term)
        if key and key not in seen:
            seen.add(key)
            yield term
    for line in discovery_text.splitlines():
        if not (STRUCTURAL_LINE_RE.match(line) or "**" in line):
            continue
        clean = re.sub(r"[`*_#>|]", " ", line)
        for term in CHINESE_TERM_RE.findall(clean):
            key = _normalize_term(term)
            if (
                len(term) >= 3
                and term not in GENERIC
                and not _is_noise_term(term)
                and key not in seen
            ):
                seen.add(key)
                yield term
    folded = discovery_text.casefold()
    for seed in seed_terms:
        seed = str(seed).strip()
        key = _normalize_term(seed)
        if seed and seed.casefold() in folded and key not in seen:
            seen.add(key)
            yield seed


def _term_excerpt(text: str, term: str, limit: int = 640) -> str:
    position = text.casefold().find(term.casefold())
    if position < 0:
        position = 0
    before = max(0, position - limit // 3)
    excerpt = text[before : before + limit]
    return re.sub(r"\s+", " ", excerpt).strip()


def _document_record(document: Mapping[str, Any], seed_terms: Sequence[str]) -> Dict[str, Any]:
    return _document_record_with_cache(document, seed_terms, None)


def _document_record_with_cache(
    document: Mapping[str, Any],
    seed_terms: Sequence[str],
    term_cache: Optional[Dict[str, List[Dict[str, str]]]],
) -> Dict[str, Any]:
    text = str(document["text"])
    digest = str(document["content_hash"])
    cached_terms = term_cache.get(digest) if term_cache is not None else None
    if cached_terms is None:
        hits = [
            {"term": term, "excerpt": _term_excerpt(text, term)}
            for term in _terms(text, seed_terms)
        ]
        if term_cache is not None:
            term_cache[digest] = [dict(item) for item in hits]
    else:
        hits = [dict(item) for item in cached_terms]
    return {
        "uri": str(document["uri"]),
        "content_hash": digest,
        "char_count": int(document["char_count"]),
        "byte_count": int(document["byte_count"]),
        "page_count": int(document["page_count"]),
        "terms": hits,
    }


def _scan_evidence_batch(
    client: Any,
    uris: Sequence[str],
    index: int,
    path: Path,
    *,
    page_size: int,
    max_workers: int,
    seed_terms: Sequence[str],
    cache_entries: Optional[Mapping[str, Mapping[str, Any]]] = None,
    source_revisions: Optional[Mapping[str, str]] = None,
    terms_fingerprint: str = "",
    term_cache: Optional[Dict[str, List[Dict[str, str]]]] = None,
    checkpoint_every: int = EVIDENCE_CHECKPOINT_EVERY,
    checkpoint_interval: float = EVIDENCE_CHECKPOINT_INTERVAL_SECONDS,
) -> Dict[str, Any]:
    """Scan one URI batch with bounded, replay-safe checkpoint writes."""
    if checkpoint_every <= 0:
        raise ValueError("checkpoint_every must be positive")
    if checkpoint_interval < 0:
        raise ValueError("checkpoint_interval must be non-negative")
    previous = _read_json(path, {})
    completed = {
        str(item.get("uri")): dict(item)
        for item in previous.get("documents", [])
        if isinstance(item, dict) and item.get("uri")
    }
    errors: Dict[str, Dict[str, str]] = {
        str(item.get("uri")): dict(item)
        for item in previous.get("errors", [])
        if isinstance(item, dict) and item.get("uri")
    }
    cache_entries = cache_entries or {}
    source_revisions = source_revisions or {}
    term_cache = term_cache if term_cache is not None else {}
    cache_hits = int(previous.get("cache_hits") or 0)
    cache_misses = int(previous.get("cache_misses") or 0)
    source_hash_rows = int(previous.get("source_hash_rows") or 0)
    pending = [uri for uri in uris if uri not in completed]
    pending_with_trusted_revision = 0
    for uri in pending:
        revision = str(source_revisions.get(uri) or "")
        if not revision:
            continue
        pending_with_trusted_revision += 1
        cache_entry = cache_entries.get(uri)
        if not isinstance(cache_entry, Mapping):
            cache_entry = {}
        cached = _cached_document(
            cache_entry,
            uri=uri,
            source_revision=revision,
            terms_fingerprint=terms_fingerprint,
        )
        if cached is not None:
            completed[uri] = cached
            errors.pop(uri, None)
            cache_hits += 1
    source_hash_rows += pending_with_trusted_revision
    cache_misses += pending_with_trusted_revision - (
        cache_hits - int(previous.get("cache_hits") or 0)
    )
    pending = [uri for uri in uris if uri not in completed]
    completed_since_checkpoint = 0
    last_checkpoint_at = time.monotonic()

    def checkpoint(*, force: bool = False) -> bool:
        nonlocal completed_since_checkpoint, last_checkpoint_at
        if not force:
            elapsed = time.monotonic() - last_checkpoint_at
            if (
                completed_since_checkpoint < checkpoint_every
                and elapsed < checkpoint_interval
            ):
                return False
        remaining = [uri for uri in uris if uri not in completed]
        artifact = {
            "schema_version": EVIDENCE_SCHEMA,
            "batch_index": index,
            "status": "completed" if not remaining else "retry_pending",
            "uris": list(uris),
            "documents": [completed[uri] for uri in uris if uri in completed],
            "errors": [errors[uri] for uri in remaining if uri in errors],
            "completed_count": len(completed),
            "pending_count": len(remaining),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "source_hash_rows": source_hash_rows,
            "content_dedup_hits": max(
                0,
                len(completed)
                - len(
                    {
                        str(item.get("content_hash") or "")
                        for item in completed.values()
                        if str(item.get("content_hash") or "")
                    }
                ),
            ),
            "updated_at": now_iso(),
        }
        _atomic_json(path, artifact)
        completed_since_checkpoint = 0
        last_checkpoint_at = time.monotonic()
        return True

    def read(uri: str) -> Tuple[str, Dict[str, Any]]:
        document = read_full_document(client, uri, page_size)
        return uri, _document_record_with_cache(document, seed_terms, term_cache)

    try:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
            futures = {pool.submit(read, uri): uri for uri in pending}
            for future in as_completed(futures):
                uri = futures[future]
                try:
                    _, record = future.result()
                except Exception as exc:
                    errors[uri] = {"uri": uri, "error": f"{type(exc).__name__}: {exc}"}
                else:
                    completed[uri] = record
                    errors.pop(uri, None)
                completed_since_checkpoint += 1
                checkpoint()
    finally:
        # Always persist the latest completed/error state before returning or
        # propagating an interruption, so resume never depends on the final
        # future reaching the throttled checkpoint threshold.
        checkpoint(force=True)
    return _read_json(path, {})


def _load_documents(run_root: Path, batch_count: int) -> List[Dict[str, Any]]:
    documents: List[Dict[str, Any]] = []
    for index in range(batch_count):
        artifact = _read_json(_evidence_path(run_root, index), {})
        documents.extend(
            dict(item) for item in artifact.get("documents", []) if isinstance(item, dict)
        )
    return sorted(documents, key=lambda item: str(item.get("uri") or ""))


def build_term_groups(
    documents: Sequence[Mapping[str, Any]],
    existing: Sequence[Mapping[str, Any]],
    seed_terms: Sequence[str] = (),
    max_evidence_per_term: int = 4,
) -> List[Dict[str, Any]]:
    active_index = _active_concept_index(existing)
    # Pending/rejected Candidates still suppress exact re-discovery, but they
    # must not participate in the active match decision.  Otherwise a pending
    # fragment could accidentally become the target for a future concept.
    blocked_pending: set[str] = set()
    for row in existing:
        if _row_is_active(row):
            continue
        blocked_pending.add(_normalize_term(row.get("name")))
        blocked_pending.update(_normalize_term(alias) for alias in row.get("aliases") or [])
    blocked_pending.discard("")
    seeded = {
        _normalize_term(seed): str(seed).strip()
        for seed in seed_terms
        if str(seed).strip()
    }
    variants: Dict[str, Counter[str]] = defaultdict(Counter)
    evidence: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    uri_counts: Counter[str] = Counter()
    active_matches: Dict[str, Dict[str, Any]] = {}
    match_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    for document in sorted(documents, key=lambda item: str(item.get("uri") or "")):
        uri = str(document.get("uri") or "")
        revision = str(document.get("content_hash") or uri)
        for hit in document.get("terms") or []:
            if not isinstance(hit, dict):
                continue
            term = str(hit.get("term") or "").strip()
            key = _normalize_term(term)
            if not key:
                continue
            # A term commonly appears in many documents.  Matching is purely
            # a function of its normalized spelling and the immutable active
            # index for this build, so cache both hits and misses.
            if key not in match_cache:
                match_cache[key] = _active_match_for_term(term, active_index)
            match = match_cache[key]
            if key in blocked_pending and not match:
                continue
            variants[key][term] += 1
            uri_counts[key] += 1
            if match:
                previous = active_matches.get(key)
                # The same normalized term can be observed with punctuation or
                # casing variants.  Keep the strongest active match metadata.
                if previous is None or float(match.get("score") or 0) > float(previous.get("score") or 0):
                    active_matches[key] = match
            evidence[key].setdefault(
                revision,
                {
                    "uri": uri,
                    "content_hash": revision,
                    "excerpt": str(hit.get("excerpt") or ""),
                },
            )
    groups: List[Dict[str, Any]] = []
    for key, counter in variants.items():
        refs = sorted(
            evidence[key].values(),
            key=lambda item: str(item.get("uri") or ""),
        )
        if len(refs) < 2:
            continue
        term = seeded.get(key) or sorted(
            counter,
            key=lambda value: (-counter[value], value.casefold(), value),
        )[0]
        groups.append(
            {
                "term": term,
                "normalized_term": key,
                "document_count": len(refs),
                "uri_count": int(uri_counts[key]),
                "evidence": refs[:max_evidence_per_term],
                "seeded": key in seeded,
                "active_match": copy.deepcopy(active_matches.get(key))
                if active_matches.get(key)
                else None,
            }
        )
    groups.sort(
        key=lambda item: (
            not bool(item["seeded"]),
            -int(item["document_count"]),
            str(item["normalized_term"]),
        )
    )
    return groups


def select_term_groups(
    groups: Sequence[Mapping[str, Any]],
    max_groups: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Select a bounded high-signal tranche while retaining the full backlog."""
    if max_groups <= 0:
        raise ValueError("max_llm_groups must be positive")
    ranked: List[Tuple[Tuple[int, int, int, int, str], Dict[str, Any]]] = []
    noise_count = 0
    for raw in groups:
        group = dict(raw)
        term = str(group.get("term") or "").strip()
        quality = _term_quality_score(term)
        # Exact/short active matches already pass normal quality checks. Keep
        # the same noise gate for fuzzy matches so sentence fragments cannot
        # crowd the bounded triage tranche merely because they contain a
        # marker such as ``资源``.
        if quality is None and not group.get("seeded"):
            noise_count += 1
            continue
        roots = {
            str(item.get("uri") or "").split("/shengsuan/", 1)[-1].split("/", 1)[0]
            for item in group.get("evidence") or []
            if isinstance(item, Mapping) and item.get("uri")
        }
        score = int(quality or 0)
        rank = (
            1 if group.get("seeded") else 0,
            score,
            len(roots),
            int(group.get("document_count") or 0),
            str(group.get("normalized_term") or _normalize_term(term)),
        )
        group["triage_score"] = score
        group["evidence_root_count"] = len(roots)
        ranked.append((rank, group))
    ranked.sort(
        key=lambda item: (
            -item[0][0],
            -item[0][1],
            -item[0][2],
            -item[0][3],
            item[0][4],
        )
    )
    selected: List[Dict[str, Any]] = []
    selected_keys: set[str] = set()

    def add(group: Dict[str, Any]) -> None:
        key = str(group.get("normalized_term") or _normalize_term(group.get("term")))
        if key and key not in selected_keys and len(selected) < max_groups:
            selected.append(group)
            selected_keys.add(key)

    for _, group in ranked:
        if group.get("seeded"):
            add(group)

    ascii_quota = min(max(8, max_groups // 5), max_groups - len(selected))
    ascii_added = 0
    for _, group in ranked:
        term = str(group.get("term") or "")
        if (
            not CHINESE_TERM_RE.fullmatch(term)
            and int(group.get("triage_score") or 0) >= 600
        ):
            before = len(selected)
            add(group)
            ascii_added += len(selected) - before
            if ascii_added >= ascii_quota:
                break

    for _, group in ranked:
        if CHINESE_TERM_RE.fullmatch(str(group.get("term") or "")):
            add(group)
        if len(selected) >= max_groups:
            break

    for _, group in ranked:
        add(group)
        if len(selected) >= max_groups:
            break
    summary = {
        "schema_version": TRIAGE_SELECTION_SCHEMA,
        "observed_term_count": len(groups),
        "eligible_term_count": len(ranked),
        "noise_term_count": noise_count,
        "selected_term_count": len(selected),
        "active_match_count": sum(1 for item in selected if item.get("active_match")),
        "unmatched_selected_term_count": sum(
            1 for item in selected if not item.get("active_match")
        ),
        "deferred_term_count": max(0, len(ranked) - len(selected)),
        "max_llm_groups": max_groups,
        "mode": "bounded_high_signal",
        "updated_at": now_iso(),
    }
    return selected, summary


def _prompt(groups: List[Dict[str, Any]], existing: List[Dict[str, Any]]) -> str:
    return f'''你是胜算概念治理 Agent。以下候选来自全量正文分页扫描后的确定性术语聚合，只能依据给出的证据判断。
EXISTING_TAXONOMY 只包含当前 active 概念（不包含待审核 Candidate）。必须先逐组比对 active 概念的 name/aliases：命中已有概念时只能输出 alias 或 merge，name/target 指向已有 canonical 概念，绝不能输出 new_concept；只有没有 active 命中时才允许 new_concept。
如果 CANDIDATES 中的 active_match 不为空，必须遵守该匹配，不要改成 new_concept。精确名称/别名用 alias，明确属于已有概念的子能力或功能切面用 merge。
新概念必须至少引用两个不同 evidence_uri，confidence>=0.65。
new_concept 的 content 必须是可直接审核的完整 Markdown 概念页，包含 YAML frontmatter（concept, aliases, category, last_updated, sources, related_concepts, related_customers, latest_version）以及：定义、能力边界、已知限制、版本演进、关联概念、出现过的客户/评估、证据与待确认点。能力与限制逐条引用真实 URI。不得从常识、URI 名称或未提供的文档补造信息。
每个 CANDIDATES group 必须恰好输出一个 decision；用 group_term 原样标识它。alias/merge 的 name 可以是已有 canonical 名称，但 group_term 仍必须是本组候选术语。
只输出 JSON：{{"decisions":[{{"group_term":"","decision":"new_concept|alias|merge|ignore","name":"","aliases":[],"target":"","category":"","content":"","evidence_uris":[],"reason":[],"confidence":0.0}}]}}
EXISTING={json.dumps(existing, ensure_ascii=False)}
CANDIDATES={json.dumps(groups, ensure_ascii=False)}'''


def _fit_group(
    group: Dict[str, Any],
    existing: List[Dict[str, Any]],
    budget: int,
) -> Dict[str, Any]:
    compact = copy.deepcopy(group)
    while len(_prompt([compact], existing)) > budget:
        excerpts = [
            str(item.get("excerpt") or "") for item in compact.get("evidence") or []
        ]
        longest = max(excerpts, key=len, default="")
        if len(longest) <= 80:
            raise ValueError(
                f"prompt budget {budget} is too small for term group {group.get('term')}"
            )
        for item in compact.get("evidence") or []:
            if str(item.get("excerpt") or "") == longest:
                item["excerpt"] = longest[: max(80, len(longest) // 2)]
                break
    return compact


def partition_term_groups(
    groups: Sequence[Dict[str, Any]],
    existing: List[Dict[str, Any]],
    char_budget: int,
    max_groups: int,
) -> List[List[Dict[str, Any]]]:
    if char_budget <= len(_prompt([], existing)):
        raise ValueError("prompt_char_budget is smaller than the prompt preamble")
    if max_groups <= 0:
        raise ValueError("batch_size must be positive")
    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    for original in groups:
        group = _fit_group(original, existing, char_budget)
        proposed = [*current, group]
        if current and (
            len(proposed) > max_groups
            or len(_prompt(proposed, existing)) > char_budget
        ):
            batches.append(current)
            current = [group]
        else:
            current = proposed
    if current:
        batches.append(current)
    return batches


def _invoke(prompt: str, timeout: int = 300) -> List[Dict[str, Any]]:
    sys.path.insert(
        0,
        str(Path.home() / ".codex/skills/shengsuan-concepts/scripts"),
    )
    from llm_runner import run_prompt

    result, output = run_prompt(prompt, timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-1000:] or f"llm exit {result.returncode}")
    text = str(output or "").strip()
    if text.startswith("```"):
        text = re.sub(
            r"^```(?:json)?\s*|\s*```$",
            "",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    data = json.loads(text)
    decisions = data.get("decisions") if isinstance(data, dict) else None
    if not isinstance(decisions, list):
        raise ValueError("LLM output must contain a decisions list")
    return [dict(item) for item in decisions if isinstance(item, dict)]


def _frontmatter(content: str) -> Dict[str, Any]:
    if not content.startswith("---\n"):
        raise ValueError("concept content must start with YAML frontmatter")
    marker = content.find("\n---\n", 4)
    if marker < 0:
        raise ValueError("concept content has no closing YAML frontmatter marker")
    try:
        metadata = yaml.safe_load(content[4:marker])
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid concept YAML frontmatter: {exc}") from exc
    if not isinstance(metadata, dict):
        raise ValueError("concept YAML frontmatter must be an object")
    return metadata


def _validate_content_contract(
    content: str,
    *,
    name: str,
    aliases: Sequence[str],
    category: str,
    refs: Sequence[str],
) -> None:
    if len(content) < 300:
        raise ValueError("new_concept content is too short")
    if not all(heading in content for heading in REQUIRED_CONTENT_HEADINGS):
        raise ValueError("new_concept content is missing required headings")
    metadata = _frontmatter(content)
    missing = [field for field in REQUIRED_FRONTMATTER_FIELDS if field not in metadata]
    if missing:
        raise ValueError(f"concept frontmatter missing fields: {missing}")
    if str(metadata.get("concept") or "").strip() != name:
        raise ValueError("frontmatter concept does not match decision name")
    if str(metadata.get("category") or "").strip() != category:
        raise ValueError("frontmatter category does not match decision category")
    for field in ("aliases", "sources", "related_concepts", "related_customers"):
        value = metadata.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"frontmatter {field} must be a string list")
    if metadata["aliases"] != list(aliases):
        raise ValueError("frontmatter aliases do not match decision aliases")
    if metadata["sources"] != list(refs):
        raise ValueError("frontmatter sources do not match decision evidence_uris")
    for field in ("last_updated", "latest_version"):
        value = metadata.get(field)
        if value is None or isinstance(value, (dict, list)) or not str(value).strip():
            raise ValueError(f"frontmatter {field} must be a non-empty scalar")


def _sanitize_decisions(
    raw: Sequence[Mapping[str, Any]],
    groups: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    group_index: Dict[str, Mapping[str, Any]] = {}
    for group in groups:
        key = _normalize_term(group.get("normalized_term") or group.get("term"))
        if not key:
            raise ValueError("term group has no normalized term")
        if key in group_index:
            raise ValueError(f"duplicate term group: {group.get('term')}")
        group_index[key] = group

    result: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("each LLM decision must be an object")
        decision = str(item.get("decision") or "").strip()
        name = str(item.get("name") or "").strip()
        if decision not in {"new_concept", "alias", "merge", "ignore"}:
            raise ValueError(f"unknown decision for {name or '<unnamed>'}: {decision}")
        group_term = str(item.get("group_term") or "").strip()
        binding_name = group_term or name
        key = _normalize_term(binding_name)
        # Older runners occasionally omitted group_term when returning an
        # alias/merge and used the canonical target as name.  Resolve that
        # shape against the active target before declaring the batch invalid.
        if key not in group_index and name:
            target_key = _normalize_term(name)
            matching_groups = [
                (candidate_key, candidate)
                for candidate_key, candidate in group_index.items()
                if _normalize_term((candidate.get("active_match") or {}).get("target"))
                == target_key
            ]
            if len(matching_groups) == 1:
                key, group = matching_groups[0]
                binding_name = str(group.get("term") or binding_name)
            else:
                group = None
        else:
            group = group_index.get(key)
        if not name or key not in group_index:
            if group is None:
                raise ValueError(
                    f"decision group_term does not match a term group: {binding_name}"
                )
        if key in seen:
            raise ValueError(f"duplicate decision for term group: {name}")
        group = group or group_index[key]
        allowed = {
            str(evidence.get("uri"))
            for evidence in group.get("evidence") or []
            if isinstance(evidence, dict) and evidence.get("uri")
        }
        try:
            confidence = float(item.get("confidence") or 0)
        except (TypeError, ValueError):
            raise ValueError(f"invalid confidence for term group: {name}")
        if not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise ValueError(f"confidence must be between 0 and 1 for term group: {name}")
        raw_refs = item.get("evidence_uris")
        if not isinstance(raw_refs, list) or not all(isinstance(uri, str) for uri in raw_refs):
            raise ValueError(f"evidence_uris must be a string list for term group: {name}")
        refs = list(dict.fromkeys(uri.strip() for uri in raw_refs if uri.strip()))
        unknown_refs = [uri for uri in refs if uri not in allowed]
        if unknown_refs:
            raise ValueError(f"evidence is not from term group {name}: {unknown_refs}")
        raw_aliases = item.get("aliases")
        raw_reasons = item.get("reason")
        if not isinstance(raw_aliases, list) or not all(
            isinstance(value, str) for value in raw_aliases
        ):
            raise ValueError(f"aliases must be a string list for term group: {name}")
        if not isinstance(raw_reasons, list) or not all(
            isinstance(value, str) for value in raw_reasons
        ):
            raise ValueError(f"reason must be a string list for term group: {name}")
        aliases = [value.strip() for value in raw_aliases if value.strip()]
        reasons = [value.strip() for value in raw_reasons if value.strip()]
        category = str(item.get("category") or "product_capability").strip()
        content = str(item.get("content") or "").strip()
        active_match = group.get("active_match")
        if isinstance(active_match, Mapping):
            # A model cannot override deterministic active matching.  This
            # also normalizes a response that chose the right concept but
            # returned the discovered fragment as ``name``.
            sanitized = _coerce_active_decision(
                item,
                group,
                active_match,
                refs=refs,
                aliases=aliases,
                reasons=reasons,
                confidence=confidence,
                category=category,
            )
            result.append(sanitized)
            seen.add(key)
            continue
        sanitized = {
            "group_term": str(group.get("term") or binding_name),
            "decision": decision,
            "name": name,
            "aliases": aliases,
            "target": str(item.get("target") or "").strip(),
            "category": category,
            "content": content,
            "evidence_uris": refs,
            "reason": reasons,
            "confidence": confidence,
        }
        if decision == "new_concept":
            if len(refs) < 2:
                raise ValueError(f"new_concept needs two evidence URIs: {name}")
            if confidence < 0.65:
                raise ValueError(f"new_concept confidence is below 0.65: {name}")
            _validate_content_contract(
                content,
                name=name,
                aliases=aliases,
                category=category,
                refs=refs,
            )
        result.append(sanitized)
        seen.add(key)

    missing = [
        str(group_index[key].get("term") or key)
        for key in group_index
        if key not in seen
    ]
    if missing:
        raise ValueError(f"missing decisions for term groups: {missing}")
    return result


def _process_llm_batch(
    path: Path,
    index: int,
    groups: List[Dict[str, Any]],
    existing: List[Dict[str, Any]],
    invoker: Callable[[str, int], List[Dict[str, Any]]],
    *,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> Dict[str, Any]:
    artifact = _read_json(path, {})
    if (
        artifact.get("status") == "completed"
        and artifact.get("schema_version") == LLM_BATCH_SCHEMA
    ):
        try:
            artifact["decisions"] = _sanitize_decisions(
                artifact.get("decisions") or [], groups
            )
        except Exception as exc:
            errors = list(artifact.get("errors") or [])
            errors.append(
                {
                    "at": now_iso(),
                    "error": f"cached decision contract invalid: {type(exc).__name__}: {exc}",
                }
            )
            artifact["errors"] = errors
        else:
            return artifact
    prompt = _prompt(groups, existing)
    total_attempts = int(artifact.get("attempts") or 0)
    errors = list(artifact.get("errors") or [])
    for attempt in range(max(0, retries) + 1):
        total_attempts += 1
        try:
            decisions = _sanitize_decisions(invoker(prompt, timeout), groups)
        except Exception as exc:
            errors.append({"at": now_iso(), "error": f"{type(exc).__name__}: {exc}"})
            if attempt < retries and retry_delay > 0:
                time.sleep(retry_delay * (2**attempt))
            continue
        artifact = {
            "schema_version": LLM_BATCH_SCHEMA,
            "batch_index": index,
            "status": "completed",
            "groups": groups,
            "prompt_chars": len(prompt),
            "attempts": total_attempts,
            "errors": errors,
            "decisions": decisions,
            "updated_at": now_iso(),
        }
        _atomic_json(path, artifact)
        return artifact
    artifact = {
        "schema_version": LLM_BATCH_SCHEMA,
        "batch_index": index,
        "status": "retry_pending",
        "groups": groups,
        "prompt_chars": len(prompt),
        "attempts": total_attempts,
        "errors": errors,
        "decisions": [],
        "updated_at": now_iso(),
    }
    _atomic_json(path, artifact)
    return artifact


def _deterministic_decisions(
    groups: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for group in groups:
        name = str(group["term"])
        refs = [str(item["uri"]) for item in group.get("evidence") or []]
        excerpt_lines = [
            f"- {item['uri']}：{str(item.get('excerpt') or '')[:280]}"
            for item in group.get("evidence") or []
        ]
        metadata = {
            "concept": name,
            "aliases": [],
            "category": "product_capability",
            "last_updated": now_iso()[:10],
            "sources": refs,
            "related_concepts": [],
            "related_customers": [],
            "latest_version": "未标注",
        }
        content = (
            "---\n"
            + json.dumps(metadata, ensure_ascii=False, indent=2)
            + "\n---\n\n"
            + f"# {name}\n\n## 定义\n{name} 是在胜算知识库中跨文档出现、尚待本人确认边界的产品术语。\n\n"
            + "## 能力边界\n- 仅确认该术语在下列正文证据中重复出现，具体能力必须回看来源。\n\n"
            + "## 已知限制\n- 当前证据不足以自动确认产品版本、交付范围或可用性。\n\n"
            + "## 版本演进\n- 来源文档未形成可验证的统一版本记录。\n\n"
            + "## 关联概念\n- 暂无经过治理的关联概念。\n\n"
            + "## 出现过的客户/评估\n- 不从术语出现位置自动推断客户关系。\n\n"
            + "## 证据与待确认点\n"
            + "\n".join(excerpt_lines)
            + "\n- 待确认：是否应并入已有概念，以及正式能力边界。"
        )
        decisions.append(
            {
                "decision": "new_concept",
                "name": name,
                "aliases": [],
                "category": "product_capability",
                "content": content,
                "evidence_uris": refs,
                "reason": ["跨至少两份完整正文出现"],
                "confidence": 0.70,
            }
        )
    return decisions


def _active_match_decisions(
    groups: Sequence[Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split matched groups from groups eligible for new-concept discovery."""

    matched: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    for raw_group in groups:
        group = dict(raw_group)
        match = group.get("active_match")
        if not isinstance(match, Mapping) or not str(match.get("target") or "").strip():
            unmatched.append(group)
            continue
        term = str(group.get("term") or "").strip()
        target = str(match.get("target") or "").strip()
        refs = list(
            dict.fromkeys(
                str(item.get("uri"))
                for item in group.get("evidence") or []
                if isinstance(item, Mapping) and item.get("uri")
            )
        )
        aliases: List[str] = []
        if (
            str(match.get("decision") or "") == "alias"
            and _normalize_term(term)
            and _normalize_term(term) != _normalize_term(target)
            and _normalize_term(term) != _normalize_term(match.get("matched_surface"))
        ):
            aliases = [term]
        matched.append(
            {
                "group_term": term,
                "decision": str(match.get("decision") or "merge"),
                "name": target,
                "aliases": aliases,
                "target": target,
                "category": str(match.get("category") or "product_capability"),
                "content": "",
                "evidence_uris": refs,
                "reason": [
                    f"active_match:{match.get('match_type') or 'fuzzy'}->{target}"
                ],
                "confidence": max(0.65, min(1.0, float(match.get("score") or 0.9))),
            }
        )
    return matched, unmatched


def _valid_content(
    content: str,
    *,
    name: str,
    aliases: Sequence[str],
    category: str,
    refs: Sequence[str],
) -> bool:
    try:
        _validate_content_contract(
            content,
            name=name,
            aliases=aliases,
            category=category,
            refs=refs,
        )
    except ValueError:
        return False
    return True


def _coerce_active_decision(
    item: Mapping[str, Any],
    group: Mapping[str, Any],
    match: Mapping[str, Any],
    *,
    refs: Sequence[str],
    aliases: Sequence[str],
    reasons: Sequence[str],
    confidence: float,
    category: str,
) -> Dict[str, Any]:
    """Turn any LLM response for an active match into alias/merge form.

    This is a hard guardrail, not a prompt preference.  It protects against a
    model returning ``new_concept`` for a group that deterministic triage has
    already matched to one of the 44 active concepts.
    """

    target = str(match.get("target") or "").strip()
    term = str(group.get("term") or "").strip()
    decision = str(match.get("decision") or "merge").strip()
    if decision not in {"alias", "merge"}:
        decision = "merge"
    normalized_target = _normalize_term(target)
    normalized_term = _normalize_term(term)
    merged_aliases = [str(value).strip() for value in aliases if str(value).strip()]
    if (
        normalized_term
        and normalized_term != normalized_target
        and normalized_term not in {_normalize_term(value) for value in merged_aliases}
        and decision == "alias"
    ):
        merged_aliases.append(term)
    merged_reasons = [str(value).strip() for value in reasons if str(value).strip()]
    marker = (
        f"active_match:{match.get('match_type') or 'fuzzy'}"
        f"->{target}"
    )
    if marker not in merged_reasons:
        merged_reasons.insert(0, marker)
    return {
        "group_term": term,
        "decision": decision,
        "name": target,
        "aliases": list(dict.fromkeys(merged_aliases)),
        "target": target,
        "category": str(match.get("category") or category or "product_capability"),
        "content": "",
        "evidence_uris": list(refs),
        "reason": merged_reasons,
        "confidence": max(0.65, min(1.0, confidence or float(match.get("score") or 0.9))),
    }


def _candidate_id(run_id: str, normalized_name: str) -> str:
    digest = hashlib.sha256(
        f"{run_id}\0{normalized_name}".encode("utf-8")
    ).hexdigest()[:12]
    return f"cand-{digest}"


def _save_candidates(
    store: ConceptLearningStore,
    run_id: str,
    decisions: Sequence[Mapping[str, Any]],
    existing: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    current_candidates = [
        item
        for item in store.list_candidates()
        if item.get("concept") or item.get("name")
    ]
    own_candidates = {
        _normalize_term(item.get("concept")): item
        for item in current_candidates
        if str(item.get("inventory_run_id") or "") == run_id
    }
    known = {_normalize_term(row.get("name")) for row in existing}
    known.update(
        _normalize_term(alias) for row in existing for alias in row.get("aliases") or []
    )
    known.update(
        _normalize_term(item.get("concept") or item.get("name"))
        for item in current_candidates
        if item.get("concept") or item.get("name")
    )
    known.discard("")
    accepted: List[Dict[str, Any]] = []
    for decision in decisions:
        if decision.get("decision") != "new_concept":
            continue
        name = str(decision.get("name") or "").strip()
        normalized = _normalize_term(name)
        aliases = [
            str(value).strip()
            for value in decision.get("aliases") or []
            if str(value).strip()
        ]
        decision_terms = {normalized, *(_normalize_term(alias) for alias in aliases)}
        decision_terms.discard("")
        if normalized in own_candidates:
            accepted.append(own_candidates[normalized])
            known.update(decision_terms)
            continue
        refs = list(
            dict.fromkeys(
                str(uri) for uri in decision.get("evidence_uris") or [] if str(uri)
            )
        )
        content = str(decision.get("content") or "").strip()
        category = str(decision.get("category") or "product_capability").strip()
        try:
            confidence = float(decision.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        if (
            not normalized
            or bool(decision_terms.intersection(known))
            or len(refs) < 2
            or not math.isfinite(confidence)
            or confidence < 0.65
            or not _valid_content(
                content,
                name=name,
                aliases=aliases,
                category=category,
                refs=refs,
            )
        ):
            continue
        candidate_id = _candidate_id(run_id, normalized)
        try:
            saved = store.read_candidate(candidate_id)
        except FileNotFoundError:
            candidate = make_candidate(
                concept=name,
                kind="new_concept",
                content=content,
                base_version="new",
                source_refs=refs,
                evidence=[
                    {"uri": uri, "status": "available", "source": "deep_inventory"}
                    for uri in refs
                ],
                reason=decision.get("reason") or [],
                confidence=confidence,
                status="ready_for_review",
                aliases=aliases,
                category=category,
                inventory_run_id=run_id,
                inventory_mode="full_document_deep_read",
            )
            candidate["candidate_id"] = candidate_id
            saved = store.save_candidate(candidate, content)
            append_agent_audit(
                store.skill_root,
                "candidate.created",
                {
                    "candidate_id": saved["candidate_id"],
                    "concept": name,
                    "kind": saved["kind"],
                    "content_hash": saved.get("content_hash"),
                    "source_refs": refs,
                    "run_id": run_id,
                    "inventory_mode": "full_document_deep_read",
                },
            )
        if str(saved.get("inventory_run_id") or "") != run_id:
            continue
        accepted.append(saved)
        known.update(decision_terms)
    return accepted


STAGE_NAMES = ("document_read", "term_aggregation", "llm_reduce", "candidate_write")


def _initial_stage_progress() -> Dict[str, Dict[str, Any]]:
    return {
        name: {
            "status": "pending",
            "processed": 0,
            "total": 0,
            "elapsed_seconds": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0,
            "cursor": 0,
            "eta_seconds": None,
        }
        for name in STAGE_NAMES
    }


def _stage_progress(
    current: Mapping[str, Any],
    *,
    status: Optional[str] = None,
    processed: Optional[int] = None,
    total: Optional[int] = None,
    elapsed_seconds: Optional[float] = None,
    cache_hits: Optional[int] = None,
    cache_misses: Optional[int] = None,
    errors: Optional[int] = None,
    cursor: Optional[int] = None,
    started_monotonic: Optional[float] = None,
) -> Dict[str, Any]:
    """Return a stable, JSON-friendly progress record with an ETA."""
    record = dict(current)
    if status is not None:
        record["status"] = status
    if processed is not None:
        record["processed"] = max(0, int(processed))
    if total is not None:
        record["total"] = max(0, int(total))
    if cache_hits is not None:
        record["cache_hits"] = max(0, int(cache_hits))
    if cache_misses is not None:
        record["cache_misses"] = max(0, int(cache_misses))
    if errors is not None:
        record["errors"] = max(0, int(errors))
    if cursor is not None:
        record["cursor"] = max(0, int(cursor))
    if elapsed_seconds is None and started_monotonic is not None:
        elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
    if elapsed_seconds is not None:
        record["elapsed_seconds"] = round(float(elapsed_seconds), 3)
    done = int(record.get("processed") or 0)
    target = int(record.get("total") or 0)
    elapsed = float(record.get("elapsed_seconds") or 0.0)
    if record.get("status") in {"completed", "skipped", "failed"} or done >= target:
        record["eta_seconds"] = 0.0
    elif done > 0 and elapsed > 0 and target > done:
        record["eta_seconds"] = round((target - done) * elapsed / done, 3)
    else:
        record["eta_seconds"] = None
    return record


def _create_manifest(
    state_dir: Path,
    uris: Sequence[str],
    existing: Sequence[Mapping[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    run_id = _new_run_id()
    root = _run_root(state_dir, run_id)
    _atomic_json(
        root / "resources.json",
        {"uris": list(uris), "snapshot_hash": _snapshot_hash(uris)},
    )
    _atomic_json(root / "taxonomy.json", list(existing))
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "run_id": run_id,
        "status": "scanning",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "resource_count": len(uris),
        "resource_snapshot_hash": _snapshot_hash(uris),
        "scan_cursor": 0,
        "progress": {
            "processed": 0,
            "read": 0,
            "unreadable": 0,
            "total": len(uris),
        },
        "stage_progress": {
            name: {
                **record,
                "total": len(uris) if name == "document_read" else 0,
            }
            for name, record in _initial_stage_progress().items()
        },
        "resources_artifact": "resources.json",
        "taxonomy_artifact": "taxonomy.json",
        # Large artifacts are gzip-compressed from this schema revision.  The
        # reader still accepts the historical `.json` siblings, so manifests
        # created before this field (and their partial checkpoints) remain
        # resumable.
        "artifact_storage": {
            "schema_version": ARTIFACT_STORAGE_SCHEMA,
            "large_json_encoding": "gzip",
            "compressed_artifacts": [
                "evidence",
                "term_groups",
                "evidence_cache",
                "content_dedup",
            ],
            "evidence_pattern": "evidence/batch-{index:05d}.json.gz",
            "term_groups": "term-groups-v2.json.gz",
            "evidence_cache": "evidence-cache.json.gz",
            "content_dedup": "content-dedup.json.gz",
            "legacy_paths": {
                "evidence_cache": "evidence-cache.json",
                "content_dedup": "content-dedup.json",
            },
            "legacy_fallback": True,
        },
        "config": config,
        "evidence": {
            "batch_count": 0,
            "completed_batches": 0,
            "pending_batches": 0,
        },
        "llm": {
            "batch_count": 0,
            "completed_batches": 0,
            "pending_batches": 0,
        },
        "candidate_ids": [],
    }
    _atomic_json(_manifest_path(state_dir, run_id), manifest)
    _atomic_json(_flat_manifest_path(state_dir, run_id), manifest)
    return manifest


def _persist_manifest(
    state_dir: Path,
    manifest: Dict[str, Any],
    **updates: Any,
) -> Dict[str, Any]:
    manifest.update(updates)
    manifest["updated_at"] = now_iso()
    _atomic_json(_manifest_path(state_dir, str(manifest["run_id"])), manifest)
    _atomic_json(_flat_manifest_path(state_dir, str(manifest["run_id"])), manifest)
    return manifest


def _result(
    manifest: Mapping[str, Any],
    *,
    documents: Sequence[Mapping[str, Any]],
    unreadable_count: int,
    term_count: int,
    decisions: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    triaged_term_count: int = 0,
    deferred_term_count: int = 0,
) -> Dict[str, Any]:
    read_count = len(documents)
    decision_counts = {
        kind: sum(1 for item in decisions if item.get("decision") == kind)
        for kind in ("new_concept", "alias", "merge", "ignore")
    }
    return {
        "schema_version": RESULT_SCHEMA,
        "run_id": manifest["run_id"],
        "status": manifest["status"],
        "mode": str((manifest.get("config") or {}).get("mode") or "full_inventory"),
        "resource_count": int(manifest["resource_count"]),
        "read_count": read_count,
        "unreadable_count": unreadable_count,
        "full_page_count": sum(int(item.get("page_count") or 0) for item in documents),
        "full_char_count": sum(int(item.get("char_count") or 0) for item in documents),
        "full_byte_count": sum(int(item.get("byte_count") or 0) for item in documents),
        "term_count": term_count,
        "triaged_term_count": triaged_term_count,
        "deferred_term_count": deferred_term_count,
        "decision_count": len(decisions),
        "decision_counts": decision_counts,
        "candidate_count": len(candidates),
        "candidate_ids": [item.get("candidate_id") for item in candidates],
        "snapshot": {
            "status": (
                "ok"
                if read_count == int(manifest["resource_count"])
                else "incomplete"
            ),
            "resource_count": int(manifest["resource_count"]),
            "snapshot_hash": manifest["resource_snapshot_hash"],
            "scan_mode": "full_document_deep_read",
            "deep_read_count": read_count,
            "deep_read_coverage": (
                round(read_count / int(manifest["resource_count"]), 4)
                if manifest["resource_count"]
                else 1.0
            ),
        },
        "evidence_cache": {
            "cache_hits": int((manifest.get("evidence") or {}).get("cache_hits") or 0),
            "cache_misses": int((manifest.get("evidence") or {}).get("cache_misses") or 0),
            "source_hash_rows": int(
                (manifest.get("evidence") or {}).get("source_hash_rows") or 0
            ),
            "content_dedup_hits": int(
                (manifest.get("evidence") or {}).get("content_dedup_hits") or 0
            ),
        },
        "baseline_ready": bool(manifest.get("baseline_ready")),
        "baseline": dict(manifest.get("baseline") or {}),
        "changed_documents": dict(manifest.get("changed_documents") or {}),
        "content_dedup": dict(manifest.get("content_dedup") or {}),
        "stage_progress": {
            name: dict(value)
            for name, value in (manifest.get("stage_progress") or {}).items()
            if isinstance(value, Mapping)
        },
        "triage": {
            "status": (
                "complete" if manifest["status"] == "completed" else "retry_pending"
            ),
            "mode": "bounded_high_signal",
            "observed_term_count": term_count,
            "triaged_term_count": triaged_term_count,
            "deferred_term_count": deferred_term_count,
            "active_match_count": int(
                (manifest.get("triage_selection") or {}).get("active_match_count") or 0
            ),
            "unmatched_selected_term_count": int(
                (manifest.get("triage_selection") or {}).get(
                    "unmatched_selected_term_count"
                )
                or 0
            ),
            "pending_batches": int(
                (manifest.get("llm") or {}).get("pending_batches") or 0
            ),
        },
        "finished_at": now_iso() if manifest["status"] == "completed" else None,
    }


def _execute_unlocked(
    store: ConceptLearningStore,
    client: Any,
    *,
    state_dir: Path,
    invoker: Callable[[str, int], List[Dict[str, Any]]] = _invoke,
    roots: Sequence[str] = DEFAULT_ROOTS,
    excludes: Sequence[str] = DEFAULT_EXCLUDES,
    node_limit: int = 50000,
    max_workers: int = 12,
    batch_size: int = 8,
    max_llm_groups: int = 160,
    read_batch_size: int = 50,
    page_size: int = 12000,
    prompt_char_budget: int = 16000,
    llm_timeout: int = 300,
    llm_retries: int = 2,
    retry_delay: float = 1.0,
    seed_terms: Sequence[str] = (),
    deterministic: bool = False,
    force_deterministic: bool = False,
    resume_run_id: Optional[str] = None,
    baseline_only: bool = False,
) -> Dict[str, Any]:
    if (
        read_batch_size <= 0
        or max_workers <= 0
        or batch_size <= 0
        or max_llm_groups <= 0
    ):
        raise ValueError("worker and batch sizes must be positive")
    state_dir = state_dir.expanduser()
    if resume_run_id:
        manifest = _read_json(_manifest_path(state_dir, resume_run_id), None)
        if not isinstance(manifest, dict):
            raise FileNotFoundError(resume_run_id)
        if manifest.get("status") in TERMINAL_STATUSES:
            return dict(manifest.get("result") or manifest)
        root = _run_root(state_dir, resume_run_id)
        resources = _read_json(root / str(manifest["resources_artifact"]), {})
        uris = [str(uri) for uri in resources.get("uris") or []]
        if _snapshot_hash(uris) != manifest.get("resource_snapshot_hash"):
            raise RuntimeError("resource snapshot artifact does not match manifest")
        existing = _read_json(root / str(manifest["taxonomy_artifact"]), [])
        config = dict(manifest.get("config") or {})
        deterministic = (
            True
            if force_deterministic
            else bool(config.get("deterministic", deterministic))
        )
        seed_terms = list(config.get("seed_terms") or seed_terms)
        baseline_only = bool(config.get("baseline_only", baseline_only))
        page_size = int(config.get("page_size") or page_size)
        read_batch_size = int(config.get("read_batch_size") or read_batch_size)
        batch_size = int(config.get("batch_size") or batch_size)
        max_llm_groups = int(config.get("max_llm_groups") or max_llm_groups)
        prompt_char_budget = int(
            config.get("prompt_char_budget") or prompt_char_budget
        )
    else:
        uris = enumerate_resources(client, roots, excludes, node_limit)
        existing = taxonomy(store)
        config = {
            "roots": list(roots),
            "excludes": list(excludes),
            "node_limit": node_limit,
            "read_batch_size": read_batch_size,
            "page_size": page_size,
            "batch_size": batch_size,
            "max_llm_groups": max_llm_groups,
            "prompt_char_budget": prompt_char_budget,
            "deterministic": deterministic,
            "seed_terms": list(seed_terms),
            "baseline_only": bool(baseline_only),
            "mode": "baseline_only" if baseline_only else "full_inventory",
        }
        manifest = _create_manifest(state_dir, uris, existing, config)
        root = _run_root(state_dir, str(manifest["run_id"]))

    # Reuse only records backed by a trusted source-ledger content revision.
    # A missing ledger row deliberately falls through to the normal full read.
    source_revisions = _resolved_source_revisions(
        uris,
        _source_revision_map(store.skill_root),
    )
    previous_baseline = _load_baseline(_baseline_path(state_dir))
    changed_summary = _changed_documents(uris, source_revisions, previous_baseline)
    _persist_manifest(
        state_dir,
        manifest,
        changed_documents=changed_summary,
        baseline_ready=False,
    )
    terms_fingerprint = _terms_fingerprint(seed_terms)
    evidence_cache_path = _evidence_cache_path(state_dir)
    evidence_cache = _load_evidence_cache(evidence_cache_path)
    evidence_batches = [
        uris[index : index + read_batch_size]
        for index in range(0, len(uris), read_batch_size)
    ]
    evidence_artifacts: List[Dict[str, Any]] = []
    term_cache: Dict[str, List[Dict[str, str]]] = {}
    cache_dirty = False
    last_cache_flush_at = time.monotonic()
    stage_progress = {
        name: dict(value)
        for name, value in (manifest.get("stage_progress") or _initial_stage_progress()).items()
        if isinstance(value, Mapping)
    }
    document_stage_started = time.monotonic()
    stage_progress["document_read"] = _stage_progress(
        stage_progress.get("document_read", {}),
        status="running",
        total=len(uris),
        started_monotonic=document_stage_started,
    )
    _persist_manifest(state_dir, manifest, stage_progress=stage_progress)
    for index, batch_uris in enumerate(evidence_batches):
        path = _evidence_path(root, index)
        artifact = _read_json(path, {})
        if artifact.get("status") != "completed":
            artifact = _scan_evidence_batch(
                client,
                batch_uris,
                index,
                path,
                page_size=page_size,
                max_workers=max_workers,
                seed_terms=seed_terms,
                cache_entries=evidence_cache,
                source_revisions=source_revisions,
                terms_fingerprint=terms_fingerprint,
                term_cache=term_cache,
            )
        evidence_artifacts.append(artifact)
        if artifact.get("status") == "completed":
            previous_cache = evidence_cache
            evidence_cache = _merge_evidence_cache(
                evidence_cache_path,
                evidence_cache,
                artifact.get("documents") or [],
                source_revisions,
                terms_fingerprint,
                persist=False,
                allow_untrusted=baseline_only,
            )
            # Only flush when the merge actually changed an entry.  A cache
            # hit still produces a completed evidence artifact, but rewriting
            # the full cache on every rerun would turn a cheap incremental
            # scan into an O(cache-size) write workload.
            cache_dirty = cache_dirty or evidence_cache != previous_cache
        cache_elapsed = time.monotonic() - last_cache_flush_at
        if cache_dirty and (
            (index + 1) % EVIDENCE_CACHE_FLUSH_EVERY == 0
            or cache_elapsed >= EVIDENCE_CACHE_FLUSH_INTERVAL_SECONDS
        ):
            _persist_evidence_cache(evidence_cache_path, evidence_cache)
            cache_dirty = False
            last_cache_flush_at = time.monotonic()
        completed_batches = sum(
            1 for item in evidence_artifacts if item.get("status") == "completed"
        )
        read_count = sum(
            int(item.get("completed_count") or 0) for item in evidence_artifacts
        )
        unreadable_count = sum(
            int(item.get("pending_count") or 0) for item in evidence_artifacts
        )
        processed_count = read_count + unreadable_count
        cache_hits = sum(int(item.get("cache_hits") or 0) for item in evidence_artifacts)
        cache_misses = sum(int(item.get("cache_misses") or 0) for item in evidence_artifacts)
        source_hash_rows = sum(
            int(item.get("source_hash_rows") or 0) for item in evidence_artifacts
        )
        evidence_documents = [
            item
            for artifact in evidence_artifacts
            for item in artifact.get("documents") or []
            if isinstance(item, Mapping)
        ]
        content_dedup_hits = max(
            0,
            len(evidence_documents)
            - len(
                {
                    str(item.get("content_hash") or "")
                    for item in evidence_documents
                    if str(item.get("content_hash") or "")
                }
            ),
        )
        checkpoint_dedup_hits = sum(
            int(item.get("content_dedup_hits") or 0) for item in evidence_artifacts
        )
        _persist_manifest(
            state_dir,
            manifest,
            status="scanning",
            evidence={
                "batch_count": len(evidence_batches),
                "completed_batches": completed_batches,
                "pending_batches": len(evidence_batches) - completed_batches,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "source_hash_rows": source_hash_rows,
                "content_dedup_hits": max(content_dedup_hits, checkpoint_dedup_hits),
            },
            scan_cursor=processed_count,
            progress={
                "processed": processed_count,
                "read": read_count,
                "unreadable": unreadable_count,
                "total": len(uris),
            },
            changed_documents=changed_summary,
            stage_progress={
                **stage_progress,
                "document_read": _stage_progress(
                    stage_progress.get("document_read", {}),
                    status="running" if processed_count < len(uris) else "completed",
                    processed=read_count,
                    total=len(uris),
                    cache_hits=cache_hits,
                    cache_misses=cache_misses,
                    errors=unreadable_count,
                    cursor=processed_count,
                    started_monotonic=document_stage_started,
                ),
            },
        )
        stage_progress["document_read"] = manifest["stage_progress"]["document_read"]

    # Cache persistence is deliberately independent from the evidence
    # checkpoint cadence.  Flush once at the end so a successful run never
    # leaves reusable records only in memory.
    if cache_dirty:
        _persist_evidence_cache(evidence_cache_path, evidence_cache)

    documents = _load_documents(root, len(evidence_batches))
    completed_document_count = len(documents)
    stage_progress["document_read"] = _stage_progress(
        stage_progress.get("document_read", {}),
        status="completed" if completed_document_count == len(uris) else "paused",
        processed=completed_document_count,
        total=len(uris),
        cursor=completed_document_count,
        started_monotonic=document_stage_started,
    )
    _persist_manifest(state_dir, manifest, stage_progress=stage_progress)
    pending_evidence = sum(
        1 for item in evidence_artifacts if item.get("status") != "completed"
    )
    if pending_evidence:
        _persist_manifest(
            state_dir,
            manifest,
            status="paused_retryable",
            candidate_ids=[],
            stage_progress=stage_progress,
        )
        result = _result(
            manifest,
            documents=documents,
            unreadable_count=int(manifest["resource_count"]) - len(documents),
            term_count=0,
            decisions=[],
            candidates=[],
        )
        _persist_manifest(state_dir, manifest, result=result)
        return result

    if baseline_only:
        baseline = _materialize_baseline(
            state_dir,
            run_id=str(manifest["run_id"]),
            uris=uris,
            documents=documents,
            source_revisions=source_revisions,
            evidence_cache=evidence_cache,
            previous_baseline=previous_baseline,
            snapshot_hash=str(manifest["resource_snapshot_hash"]),
        )
        skipped_stages = dict(stage_progress)
        for name in ("term_aggregation", "llm_reduce", "candidate_write"):
            skipped_stages[name] = _stage_progress(
                skipped_stages.get(name, {}),
                status="skipped",
                processed=0,
                total=0,
                cursor=0,
                elapsed_seconds=0.0,
            )
            skipped_stages[name]["skip_reason"] = "baseline_only"
        dedup_stats = dict(baseline.get("content_dedup") or {})
        _persist_manifest(
            state_dir,
            manifest,
            status="completed",
            baseline_ready=bool(baseline.get("baseline_ready")),
            baseline=baseline,
            changed_documents=changed_summary,
            content_dedup=dedup_stats,
            stage_progress=skipped_stages,
            candidate_ids=[],
            scan_cursor=len(documents),
            progress={
                "processed": len(documents),
                "read": len(documents),
                "unreadable": 0,
                "total": len(uris),
            },
            completed_at=now_iso(),
        )
        result = _result(
            manifest,
            documents=documents,
            unreadable_count=0,
            term_count=0,
            decisions=[],
            candidates=[],
        )
        _persist_manifest(state_dir, manifest, result=result)
        return result

    active_existing = _active_taxonomy_rows(existing)
    groups_path = _term_groups_path(root)
    term_stage_started = time.monotonic()
    stage_progress["term_aggregation"] = _stage_progress(
        stage_progress.get("term_aggregation", {}),
        status="running",
        total=len(documents),
        started_monotonic=term_stage_started,
    )
    _persist_manifest(state_dir, manifest, stage_progress=stage_progress)
    groups_artifact = _read_json(groups_path, None)
    if not isinstance(groups_artifact, dict) or groups_artifact.get(
        "schema_version"
    ) != TERM_GROUPS_SCHEMA:
        groups = build_term_groups(documents, existing, seed_terms)
        groups_artifact = {
            "schema_version": TERM_GROUPS_SCHEMA,
            "groups": groups,
            "term_count": len(groups),
            "created_at": now_iso(),
        }
        _atomic_json(groups_path, groups_artifact)
    groups = [dict(group) for group in groups_artifact.get("groups") or []]
    dedup_summary = _content_dedup_summary(documents)
    _atomic_json(_content_dedup_path(state_dir), dedup_summary)
    stage_progress["term_aggregation"] = _stage_progress(
        stage_progress.get("term_aggregation", {}),
        status="completed",
        processed=len(documents),
        total=len(documents),
        cursor=len(documents),
        elapsed_seconds=time.monotonic() - term_stage_started,
    )
    _persist_manifest(
        state_dir,
        manifest,
        stage_progress=stage_progress,
        content_dedup={
            key: value
            for key, value in dedup_summary.items()
            if key not in {"groups", "uri_to_content_hash"}
        },
    )

    selection_path = _triage_selection_path(root)
    selection_artifact = _read_json(selection_path, None)
    if (
        not isinstance(selection_artifact, dict)
        or selection_artifact.get("schema_version") != TRIAGE_SELECTION_SCHEMA
        or int(selection_artifact.get("max_llm_groups") or 0) != max_llm_groups
        or int(selection_artifact.get("observed_term_count") or -1) != len(groups)
    ):
        selected_groups, selection_summary = select_term_groups(
            groups, max_llm_groups
        )
        selection_artifact = {
            **selection_summary,
            "groups": selected_groups,
        }
        _atomic_json(selection_path, selection_artifact)
    selected_groups = [
        dict(group) for group in selection_artifact.get("groups") or []
    ]
    selection_summary = {
        key: value
        for key, value in selection_artifact.items()
        if key != "groups"
    }
    _persist_manifest(
        state_dir,
        manifest,
        status="triaging",
        triage_selection=selection_summary,
    )
    print(
        " ".join(
            [
                f"scanned={len(uris)}",
                f"read={len(documents)}",
                f"candidate_terms={len(groups)}",
                f"selected_terms={len(selected_groups)}",
                f"deferred_terms={selection_summary.get('deferred_term_count', 0)}",
            ]
        ),
        flush=True,
    )

    # Resolve active matches before invoking Codex.  Matched groups are
    # represented in the final decision stream as alias/merge and are never
    # eligible for the new-concept writer.
    active_decisions, unmatched_groups = _active_match_decisions(selected_groups)
    decisions: List[Dict[str, Any]] = list(active_decisions)
    if deterministic:
        decisions.extend(_deterministic_decisions(unmatched_groups))
        stage_progress["llm_reduce"] = _stage_progress(
            stage_progress.get("llm_reduce", {}),
            status="skipped",
            processed=0,
            total=0,
            elapsed_seconds=0.0,
        )
        stage_progress["llm_reduce"]["skip_reason"] = "deterministic"
        llm_summary = {
            "batch_count": 0,
            "completed_batches": 0,
            "pending_batches": 0,
        }
    else:
        llm_batches = partition_term_groups(
            unmatched_groups,
            list(active_existing),
            prompt_char_budget,
            batch_size,
        )
        llm_artifacts: List[Dict[str, Any]] = []
        llm_stage_started = time.monotonic()
        stage_progress["llm_reduce"] = _stage_progress(
            stage_progress.get("llm_reduce", {}),
            status="running",
            total=len(llm_batches),
            started_monotonic=llm_stage_started,
        )
        llm_summary = {
            "batch_count": len(llm_batches),
            "completed_batches": 0,
            "pending_batches": len(llm_batches),
        }
        _persist_manifest(
            state_dir,
            manifest,
            status="triaging",
            llm=llm_summary,
            triage_selection=selection_summary,
            stage_progress=stage_progress,
        )
        for index, batch_groups in enumerate(llm_batches):
            artifact = _process_llm_batch(
                _llm_path(root, index),
                index,
                batch_groups,
                list(active_existing),
                invoker,
                timeout=llm_timeout,
                retries=llm_retries,
                retry_delay=retry_delay,
            )
            llm_artifacts.append(artifact)
            completed_llm = sum(
                1 for item in llm_artifacts if item.get("status") == "completed"
            )
            llm_summary = {
                "batch_count": len(llm_batches),
                "completed_batches": completed_llm,
                "pending_batches": len(llm_batches) - completed_llm,
            }
            _persist_manifest(
                state_dir,
                manifest,
                status="triaging",
                llm=llm_summary,
                triage_selection=selection_summary,
                stage_progress={
                    **stage_progress,
                    "llm_reduce": _stage_progress(
                        stage_progress.get("llm_reduce", {}),
                        status="running" if completed_llm < len(llm_batches) else "completed",
                        processed=completed_llm,
                        total=len(llm_batches),
                        errors=sum(len(item.get("errors") or []) for item in llm_artifacts),
                        cursor=completed_llm,
                        started_monotonic=llm_stage_started,
                    ),
                },
            )
            stage_progress["llm_reduce"] = manifest["stage_progress"]["llm_reduce"]
        if llm_summary["pending_batches"]:
            stage_progress["llm_reduce"] = _stage_progress(
                stage_progress.get("llm_reduce", {}),
                status="paused",
                processed=llm_summary["completed_batches"],
                total=len(llm_batches),
                cursor=llm_summary["completed_batches"],
                started_monotonic=llm_stage_started,
            )
            _persist_manifest(
                state_dir,
                manifest,
                status="paused_retryable",
                candidate_ids=[],
                stage_progress=stage_progress,
            )
            result = _result(
                manifest,
                documents=documents,
                unreadable_count=0,
                term_count=len(groups),
                triaged_term_count=len(selected_groups),
                deferred_term_count=int(
                    selection_summary.get("deferred_term_count") or 0
                ),
                decisions=active_decisions,
                candidates=[],
            )
            _persist_manifest(state_dir, manifest, result=result)
            return result
        for artifact in llm_artifacts:
            decisions.extend(
                dict(item)
                for item in artifact.get("decisions") or []
                if isinstance(item, dict)
            )
        if not llm_batches:
            stage_progress["llm_reduce"] = _stage_progress(
                stage_progress.get("llm_reduce", {}),
                status="completed",
                processed=0,
                total=0,
                cursor=0,
                elapsed_seconds=time.monotonic() - llm_stage_started,
            )

    candidate_stage_started = time.monotonic()
    stage_progress["candidate_write"] = _stage_progress(
        stage_progress.get("candidate_write", {}),
        status="running",
        total=len(decisions),
        started_monotonic=candidate_stage_started,
    )
    _persist_manifest(state_dir, manifest, stage_progress=stage_progress)
    candidates = _save_candidates(
        store,
        str(manifest["run_id"]),
        decisions,
        existing,
    )
    stage_progress["candidate_write"] = _stage_progress(
        stage_progress.get("candidate_write", {}),
        status="completed",
        processed=len(decisions),
        total=len(decisions),
        cursor=len(decisions),
        elapsed_seconds=time.monotonic() - candidate_stage_started,
    )
    baseline = _materialize_baseline(
        state_dir,
        run_id=str(manifest["run_id"]),
        uris=uris,
        documents=documents,
        source_revisions=source_revisions,
        evidence_cache=evidence_cache,
        previous_baseline=previous_baseline,
        snapshot_hash=str(manifest["resource_snapshot_hash"]),
    )
    _persist_manifest(
        state_dir,
        manifest,
        status="completed",
        baseline_ready=bool(baseline.get("baseline_ready")),
        baseline=baseline,
        changed_documents=changed_summary,
        content_dedup={
            key: value
            for key, value in dedup_summary.items()
            if key not in {"groups", "uri_to_content_hash"}
        },
        llm=llm_summary,
        triage_selection=selection_summary,
        stage_progress=stage_progress,
        candidate_ids=[item.get("candidate_id") for item in candidates],
        scan_cursor=len(documents),
        progress={
            "processed": len(documents),
            "read": len(documents),
            "unreadable": 0,
            "total": len(uris),
        },
        completed_at=now_iso(),
    )
    result = _result(
        manifest,
        documents=documents,
        unreadable_count=0,
        term_count=len(groups),
        triaged_term_count=len(selected_groups),
        deferred_term_count=int(
            selection_summary.get("deferred_term_count") or 0
        ),
        decisions=decisions,
        candidates=candidates,
    )
    _persist_manifest(state_dir, manifest, result=result)
    return result


def execute(
    store: ConceptLearningStore,
    client: Any,
    *,
    state_dir: Path,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Run one inventory under a process-wide single-flight lock."""
    with _single_flight(state_dir):
        return _execute_unlocked(store, client, state_dir=state_dir, **kwargs)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    ap.add_argument(
        "--state-dir",
        type=Path,
        help="兼容 Control Plane run root；盘点 checkpoint 始终归属 concept skill state",
    )
    ap.add_argument("--inventory-state-dir", type=Path)
    ap.add_argument("--root", action="append", dest="roots")
    ap.add_argument("--exclude", action="append", dest="excludes")
    ap.add_argument("--node-limit", type=int, default=50000)
    ap.add_argument("--max-workers", "--workers", dest="max_workers", type=int, default=12)
    ap.add_argument(
        "--batch-size",
        type=int,
        default=8,
        help="每个 LLM 批次最多聚合术语数",
    )
    ap.add_argument(
        "--max-llm-groups",
        type=int,
        default=160,
        help="本轮进入 LLM 的高信号术语上限；其余保留为后续 triage backlog",
    )
    ap.add_argument("--read-batch-size", type=int, default=50)
    ap.add_argument(
        "--page-size",
        "--evidence-limit",
        dest="page_size",
        type=int,
        default=12000,
        help="OpenViking content/read 每页行数（不是字符数）",
    )
    ap.add_argument(
        "--max-evidence",
        type=int,
        help="旧 CLI 兼容参数；deep inventory 始终读取完整资源快照，不执行抽样",
    )
    ap.add_argument("--prompt-char-budget", type=int, default=16000)
    ap.add_argument("--llm-timeout", type=int, default=300)
    ap.add_argument("--llm-retries", type=int, default=2)
    ap.add_argument("--retry-delay", type=float, default=1.0)
    ap.add_argument("--seed-term", action="append", default=[])
    ap.add_argument("--resume-run-id")
    ap.add_argument(
        "--from-run-id",
        help="从已有 completed deep-inventory run 只物化增量基线，不重新读取文档",
    )
    ap.add_argument(
        "--baseline-only",
        "--materialize-baseline",
        dest="baseline_only",
        action="store_true",
        help="只物化 source/evidence/content 增量基线，不进入术语/LLM/Candidate 阶段",
    )
    ap.add_argument(
        "--output",
        "--result-path",
        dest="output",
        type=Path,
        default=Path("/tmp/concept-deep-inventory.json"),
    )
    ap.add_argument(
        "--deterministic",
        action="store_true",
        help="仅使用正文证据生成待审核 Candidate，不调用 LLM",
    )
    ap.add_argument(
        "--deterministic-fallback",
        action="store_true",
        help="恢复任务时切换为证据驱动的 deterministic triage，跳过失败的 LLM 批次",
    )
    ap.add_argument(
        "--auto-approve-publish",
        action="store_true",
        help="已禁用：deep inventory 只生成 ready_for_review Candidate",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)
    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("full-inventory")
    skill_root = args.codex_root.expanduser() / "skills" / "shengsuan-concepts"
    store = ConceptLearningStore(skill_root)
    state_dir = args.inventory_state_dir or store.state_root / "full-inventory"
    try:
        if args.auto_approve_publish:
            raise ValueError("auto approval and publication are forbidden for deep inventory")
        seed_terms = list(
            dict.fromkeys([*args.seed_term, *manual_seed_terms(args.state_dir)])
        )
        if args.from_run_id:
            result = materialize_baseline_from_run(
                store,
                state_dir=state_dir,
                run_id=args.from_run_id,
            )
        else:
            result = execute(
                store,
                OpenVikingClient(),
                state_dir=state_dir,
                roots=args.roots or DEFAULT_ROOTS,
                excludes=args.excludes or DEFAULT_EXCLUDES,
                node_limit=args.node_limit,
                max_workers=args.max_workers,
                batch_size=args.batch_size,
                max_llm_groups=args.max_llm_groups,
                read_batch_size=args.read_batch_size,
                page_size=args.page_size,
                prompt_char_budget=args.prompt_char_budget,
                llm_timeout=args.llm_timeout,
                llm_retries=args.llm_retries,
                retry_delay=args.retry_delay,
                seed_terms=seed_terms,
                deterministic=args.deterministic,
                force_deterministic=args.deterministic_fallback,
                resume_run_id=args.resume_run_id,
                baseline_only=args.baseline_only,
            )
    except Exception as exc:
        result = {
            "schema_version": RESULT_SCHEMA,
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
        }
        _atomic_json(args.output.expanduser(), result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1
    _atomic_json(args.output.expanduser(), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
