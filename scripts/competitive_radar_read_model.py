#!/usr/bin/env python3
"""Read-only projection for the competitive radar latest report."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _safe_source_url(value: Any) -> str | None:
    """Allow only navigable public URL schemes in the read model.

    The ingest path normally normalizes URLs, but latest-ingest is still an
    input boundary.  Escaping HTML is not enough when a value is later used in
    an ``href`` attribute: ``javascript:`` and other schemes must be dropped.
    """
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = urlsplit(text)
    except ValueError:
        return None
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password:
        return None
    return text


CAPABILITY_DOMAIN_LABELS = {
    "ontology/action/governance": "Ontology / Action / 治理",
    "data/agent runtime": "数据与 Agent Runtime",
    "runtime/mcp/skills": "Agent Runtime / MCP / Skills",
    "productization/market signal": "产品化与市场信号",
    "community signal": "社区关注信号",
    "research/personal workbench": "研究与专业工作台",
    "computer use/agent": "Computer Use / Agent",
    "video evidence": "视频证据",
}

EVIDENCE_LEVELS = {
    "A": "官方正文/发布记录",
    "B": "官方列表卡片或摘要",
    "C": "社区热度与上下文",
    "D": "标题或 Meta 仅索引",
}


def _capability_domain(row: dict[str, Any]) -> str:
    layer = str(row.get("capability_layer") or "").strip()
    key = layer.casefold()
    if key in CAPABILITY_DOMAIN_LABELS:
        return CAPABILITY_DOMAIN_LABELS[key]
    if "ontology" in key or "governance" in key:
        return "Ontology / Action / 治理"
    if "data" in key:
        return "数据与 Agent Runtime"
    if "runtime" in key or "mcp" in key or "skill" in key:
        return "Agent Runtime / MCP / Skills"
    if "computer" in key:
        return "Computer Use / Agent"
    if "research" in key or "workbench" in key:
        return "研究与专业工作台"
    if "community" in key:
        return "社区关注信号"
    if "video" in key:
        return "视频证据"
    return layer or "未分类能力域"


def _evidence_level(row: dict[str, Any]) -> str:
    depth = str(row.get("content_depth") or "").casefold()
    fact_type = str(row.get("fact_type") or "").casefold()
    # Community heat/context is C-level regardless of whether the collector
    # called the captured block a card or summary.  The evidence grade must
    # reflect source provenance, not only transport shape.
    if fact_type in {"community_feedback", "community_signal"}:
        return "C" if depth in {"detail", "body", "summary", "card", "community", "community_metrics"} else "D"
    # A is reserved for official detail/body evidence.  Official summaries
    # and list cards are B-level evidence by definition, even when the source
    # is trusted, because they do not carry the full release正文.
    if depth in {"detail", "body"} and fact_type == "official_fact":
        return "A"
    if depth in {"detail", "body", "summary", "card"}:
        return "B"
    if depth in {"community", "community_metrics"} or fact_type == "community_feedback":
        return "C"
    return "D"


def _relation_values(row: dict[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    for key in keys:
        raw = row.get(key)
        if isinstance(raw, str):
            raw = [part.strip() for part in raw.split(",") if part.strip()]
        if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes, dict)):
            for value in raw:
                text = str(value).strip()
                if text and text not in values:
                    values.append(text)
    return values


def _signal_cards(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible_by_domain: dict[str, set[str]] = {}
    for row in signals:
        if row.get("retrieval_status") == "ok" and _evidence_level(row) != "D":
            source_id = str(row.get("source_id") or "").strip()
            if source_id:
                eligible_by_domain.setdefault(_capability_domain(row), set()).add(source_id)
    cards: list[dict[str, Any]] = []
    for row in signals:
        level = _evidence_level(row)
        domain = _capability_domain(row)
        evidence = row.get("original_evidence") if isinstance(row.get("original_evidence"), list) else []
        first_evidence = evidence[0] if evidence and isinstance(evidence[0], dict) else {}
        requirements = _relation_values(row, "requirement_ids", "requirement_id")
        tasks = _relation_values(row, "task_ids", "task_id")
        versions = _relation_values(row, "version_ids", "version_id")
        explicit = bool(requirements or tasks or versions)
        retrieval_ok = row.get("retrieval_status") == "ok"
        raw_source_url = row.get("resolved_source_url") or row.get("source_url")
        source_url = _safe_source_url(raw_source_url)
        if not retrieval_ok or level == "D":
            signal_kind = "待补证"
            threat_opportunity = "待定"
            action = "补证"
            confidence = "低"
            judgement_basis = "来源不可用或仅有标题/Meta；这是证据状态判断，不是产品结论"
        elif str(row.get("fact_type") or "").casefold() == "official_fact":
            signal_kind = "潜在竞争压力"
            threat_opportunity = "威胁候选"
            action = "验证"
            confidence = "高" if level == "A" else "中"
            judgement_basis = "官方来源能力信号映射为潜在压力（分析推断）；不证明胜算已支持、已 GA 或有官网承诺"
        else:
            signal_kind = "市场关注机会"
            threat_opportunity = "机会候选"
            action = "观察"
            confidence = "中" if level == "B" else "低"
            judgement_basis = "社区/非官方关注信号映射为机会候选（分析推断）；不证明客户需求或产品能力"
        unknown_scope = ["公开来源未验证完整产品边界"]
        if not explicit:
            unknown_scope.append("未建立显式 requirement/task/version 关联")
        if row.get("visibility_gap_count") is None:
            unknown_scope.append("来源可见性盲区未记录")
        if raw_source_url and not source_url:
            unknown_scope.append("来源链接协议或格式不受支持，已抑制跳转")
        multi_source_count = len(eligible_by_domain.get(domain, set()))
        multi_source_status = (
            "同能力域多源候选" if multi_source_count > 1
            else "单源" if multi_source_count == 1
            else "来源标识未记录"
        )
        cards.append({
            "signal_id": row.get("signal_id"),
            "source_id": row.get("source_id"),
            "source": row.get("source"),
            "source_url": source_url,
            "vendor": row.get("vendor") or "未知厂商",
            "product": row.get("product") or "未知产品",
            "captured_at": row.get("captured_at"),
            "capability_domain": domain,
            "route": row.get("route"),
            "signal_kind": signal_kind,
            "threat_opportunity": threat_opportunity,
            "judgement_type": "分析推断",
            "judgement_basis": judgement_basis,
            "impact_scope": f"外部竞品方向 · {domain}",
            "confidence": confidence,
            "confidence_basis": f"依据证据等级 {level}（规则化初筛，不代表产品结论）",
            "suggested_action": action,
            "action_basis": "只读建议；需人工确认后才进入产品决策",
            "evidence_level": level,
            "evidence_level_label": EVIDENCE_LEVELS[level],
            "content_depth": row.get("content_depth") or "未记录",
            "multi_source_count": multi_source_count,
            "multi_source_status": multi_source_status,
            "multi_source_basis": "按能力域聚合，仅表示候选印证，不等于同一事实已互证",
            "visibility_gap_count": row.get("visibility_gap_count") if row.get("visibility_gap_count") is not None else None,
            "unknown_scope": unknown_scope,
            "association_status": "explicit" if explicit else "not_established",
            "association_label": "已有显式关联" if explicit else "未建立显式关联",
            "association_note": "仅消费 signal 中显式的 ID；禁止从标题、厂商或路由语义推断关联",
            "requirements": requirements,
            "tasks": tasks,
            "versions": versions,
            "evidence_id": row.get("evidence_id"),
            "source_snapshot_uri": row.get("source_snapshot_uri"),
            "content_hash": row.get("content_hash"),
            "locator": row.get("locator"),
            "original": first_evidence.get("original") or row.get("body_excerpt") or row.get("title_excerpt") or "",
            "translation_zh": first_evidence.get("translation_zh") or "",
            "translation_status": first_evidence.get("translation_status") or row.get("translation_status") or "missing",
        })
    cards.sort(key=lambda item: (item["evidence_level"], str(item.get("captured_at") or "")), reverse=False)
    return cards


def _capability_map(cards: list[dict[str, Any]]) -> dict[str, Any]:
    vendors = sorted({str(card.get("vendor") or "未知厂商") for card in cards})
    domains = sorted({str(card.get("capability_domain") or "未分类能力域") for card in cards})
    cells: list[dict[str, Any]] = []
    for vendor in vendors:
        for domain in domains:
            matches = [card for card in cards if card.get("vendor") == vendor and card.get("capability_domain") == domain]
            if not matches:
                continue
            requirement_ids = sorted({item for card in matches for item in card.get("requirements", [])})
            task_ids = sorted({item for card in matches for item in card.get("tasks", [])})
            version_ids = sorted({item for card in matches for item in card.get("versions", [])})
            explicit = bool(requirement_ids or task_ids or version_ids)
            cells.append({
                "vendor": vendor,
                "capability_domain": domain,
                "signal_count": len(matches),
                "signal_ids": [card.get("signal_id") for card in matches],
                "evidence_levels": sorted({card.get("evidence_level") for card in matches}),
                "association_status": "explicit" if explicit else "not_established",
                "association_label": "显式关联" if explicit else "未建立关联",
                "requirements": requirement_ids,
                "tasks": task_ids,
                "versions": version_ids,
            })
    return {
        "vendors": vendors,
        "domains": domains,
        "cells": cells,
        "association_policy": "只展示 signal 中显式 requirement_id/task_id/version_id；禁止标题语义映射",
    }


class CompetitiveRadarReadModel:
    read_only = True

    def __init__(self, *, db_path: Path, state_root: Path, project_root: Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.state_root = Path(state_root).expanduser().resolve()
        self.project_root = Path(project_root).expanduser().resolve()

    def latest(self) -> dict[str, Any] | None:
        value: dict[str, Any] | None = None
        if self.db_path.is_file():
            import sqlite3

            try:
                with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True) as connection:
                    connection.row_factory = sqlite3.Row
                    row = connection.execute("SELECT * FROM competitive_radar_latest WHERE pointer_id=1").fetchone()
                    if row is not None:
                        value = dict(row)
            except (OSError, sqlite3.Error):
                value = None
        # The PM system DB is the only canonical latest pointer.  The report
        # directory cache is intentionally not a read fallback because it can
        # survive a failed coordination write and present an unpublished draft.
        return value

    def _latest_signals(self) -> tuple[list[dict[str, Any]], str | None]:
        path = self.state_root / "latest-ingest.json"
        if not path.is_file():
            return [], None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return [], None
        signals = payload.get("signals") if isinstance(payload, dict) else None
        if not isinstance(signals, list):
            return [], None
        rows = [dict(item) for item in signals if isinstance(item, dict)]
        return rows, _hash({"run_id": payload.get("run_id"), "captured_at": payload.get("captured_at"), "signals": rows})

    def snapshot(self) -> dict[str, Any]:
        read_at = _now()
        pointer = self.latest()
        if not pointer:
            return {"schema_version": "competitive-radar.read-model.v2", "read_only": True, "read_at": read_at, "as_of": read_at, "source_status": "not_recorded", "freshness": "unknown", "gate_status": "unknown", "report_status": "not_recorded", "latest": None, "stale_sources": [], "evidence_coverage": 0.0, "signal_cards": [], "capability_map": {"vendors": [], "domains": [], "cells": [], "association_policy": "只展示显式关联"}, "source_version": _hash({"latest": None})}
        report_path = Path(str(pointer.get("report_uri") or "")).expanduser()
        html_path = Path(str(pointer.get("html_uri") or "")).expanduser()
        report_available = report_path.is_file()
        html_available = html_path.is_file()
        signals, ingest_hash = self._latest_signals()
        cards = _signal_cards(signals)
        capability_map = _capability_map(cards)
        status = str(pointer.get("report_status") or "unknown")
        freshness = "fresh" if report_available and status == "reviewed" else "stale"
        latest = dict(pointer)
        latest.update({"report_available": report_available, "html_available": html_available})
        source_version = str(pointer.get("report_hash") or _hash(pointer))
        if ingest_hash:
            source_version = f"{source_version}:ingest-{ingest_hash[7:23]}"
        value = {"schema_version": "competitive-radar.read-model.v2", "read_only": True, "read_at": read_at, "as_of": pointer.get("published_at") or read_at, "source_status": "observed" if report_available else "unavailable", "freshness": freshness, "gate_status": pointer.get("gate_status") or "unknown", "report_status": status, "latest": latest, "stale_sources": [], "evidence_coverage": float(pointer.get("evidence_coverage") or 0), "signal_cards": cards, "capability_map": capability_map, "source_version": source_version}
        return value
