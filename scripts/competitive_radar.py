#!/usr/bin/env python3
"""Deterministic PM Loop competitive-radar handlers.

The first production slice deliberately keeps collection and report assembly
local and replayable.  Network reads are bounded, every result gets a content
hash, and the worker remains the only owner of the coordination database.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import html
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT_ROOT = Path(os.environ.get("PM_COMPETITIVE_RADAR_PROJECT_ROOT", str(PROJECT_ROOT))).expanduser().resolve()
DEFAULT_STATE_ROOT = Path(
    os.environ.get("PM_COMPETITIVE_RADAR_STATE_ROOT", str(Path.home() / ".codex" / "pm-loop" / "state" / "competitive-radar"))
).expanduser().resolve()
REGISTRY_RELATIVE = Path("docs") / "产品情报监控" / "竞品雷达" / "source-registry.json"
TRANSLATION_RELATIVE = Path("docs") / "产品情报监控" / "竞品雷达" / "translation-overrides.json"
TRACKING_KEYS = {"gclid", "fbclid", "mc_cid", "mc_eid"}
MAX_BODY_BYTES = 256 * 1024
URL_RE = re.compile(r"^https?://", re.IGNORECASE)
DEFAULT_FETCH_HEADERS = {
    "User-Agent": "PM-Loop-Competitive-Radar/1.0",
    "Accept": "text/html,application/json,application/xml,text/plain",
}
BROWSER_FALLBACK_EXECUTABLE = Path(
    os.environ.get("PM_COMPETITIVE_RADAR_BROWSER", str(Path.home() / ".local" / "bin" / "ego-browser"))
).expanduser()
BROWSER_FALLBACK_TIMEOUT_SECONDS = 35
# Browser fallback is limited to the curated public-source registry.  It is
# not enabled for arbitrary URLs submitted through the quick-inbox.
BROWSER_FALLBACK_HOSTS = {
    "www.palantir.com",
    "www.databricks.com",
    "github.com",
    "www.producthunt.com",
    "news.ycombinator.com",
    "openai.com",
    "www.anthropic.com",
    "www.youtube.com",
}
# Detail pages are reached only when their URL is discovered on a registered
# source page and the host is explicitly allow-listed here.  This keeps the
# browser enrichment bounded and prevents a source page from becoming an
# arbitrary URL proxy.
DETAIL_HOSTS = BROWSER_FALLBACK_HOSTS | {
    "developer.meta.com",
    "blog.google",
    "www.thetimes.com",
    "trellner.com",
    "nature.com",
    "help.mistral.ai",
    "www.nytimes.com",
    "werwolv.net",
    "www.science.org",
    "www.wikipedia.org",
}
FETCH_WALL_TIMEOUT_GRACE_SECONDS = 2


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Keep source fetches on the URL explicitly accepted by the registry."""

    def redirect_request(self, req: urllib.request.Request, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        raise urllib.error.HTTPError(req.full_url, code, "redirect_rejected", headers, None)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


class _VisibleTextParser(HTMLParser):
    """Extract readable text from HTML source excerpts."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth and data.strip():
            self.parts.append(data)


class _StructuredPageParser(HTMLParser):
    """Extract page-level fields that survive server-rendered HTML fetches."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.meta_description = ""
        self.headings: list[str] = []
        self._ignored_depth = 0
        self._active: str | None = None
        self._active_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "template"}:
            self._ignored_depth += 1
            return
        attributes = {key.lower(): value or "" for key, value in attrs}
        if tag == "meta" and not self.meta_description:
            key = (attributes.get("name") or attributes.get("property") or "").lower()
            if key in {"description", "og:description", "twitter:description"}:
                self.meta_description = attributes.get("content", "").strip()
        if tag == "title":
            self._active = "title"
            self._active_parts = []
        elif tag in {"h1", "h2", "h3"} and len(self.headings) < 12:
            self._active = tag
            self._active_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "template"} and self._ignored_depth:
            self._ignored_depth -= 1
            return
        if self._active == "title" and tag == "title":
            value = " ".join(self._active_parts).strip()
            if value:
                self.title_parts.append(value)
            self._active = None
            self._active_parts = []
        elif self._active in {"h1", "h2", "h3"} and tag == self._active:
            value = " ".join(self._active_parts).strip()
            if value and value not in self.headings:
                self.headings.append(value)
            self._active = None
            self._active_parts = []

    def handle_data(self, data: str) -> None:
        if self._ignored_depth or not data.strip() or self._active is None:
            return
        self._active_parts.append(data)


def structured_page_fields(value: str) -> dict[str, Any]:
    """Return compact title/description/headline fields for report assembly."""
    parser = _StructuredPageParser()
    try:
        parser.feed(value)
    except Exception:
        return {"page_title": "", "meta_description": "", "headlines": []}
    title = " ".join(parser.title_parts).strip()
    return {
        "page_title": title[:280],
        "meta_description": visible_excerpt(parser.meta_description, 360),
        "headlines": [visible_excerpt(item, 180) for item in parser.headings if item.strip()],
    }


def visible_excerpt(value: Any, limit: int = 240) -> str:
    """Return compact human-readable text for a source excerpt."""
    text = str(value or "")
    if "<" in text and ">" in text:
        parser = _VisibleTextParser()
        try:
            parser.feed(text)
            text = " ".join(parser.parts)
        except Exception:
            text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(re.sub(r"\s+", " ", text)).strip()
    return text[:limit] + ("..." if len(text) > limit else "")


ROUTE_LABELS = {
    "A": "企业数据与 Agent 平台",
    "B": "个人专业工作台",
    "C": "通用 Agent / Coworker",
}

ROUTE_REFERENCE = {
    "A": "把数据/语义层、工具与 Action、治理审计连成可交付闭环",
    "B": "验证持久上下文、研究引用、结构化输出与 token 商业化",
    "C": "跟踪 Runtime、MCP/Skills、Computer Use、执行日志与失败恢复",
}


def _is_markup_noise(value: str) -> bool:
    lowered = value.casefold()
    return not value or "<!doctype" in lowered or "<html" in lowered or "<script" in lowered or len(value.strip()) < 8


GENERIC_HEADLINES = {
    "navigation menu", "news", "newsroom", "products", "models", "solutions", "resources", "platform", "agent",
    "claude platform", "programs", "help and security", "company", "terms and policies", "all", "全部",
}
GENERIC_PAGE_TITLES = {"hacker news", "newsroom", "news", "ai products", "artificial intelligence | product hunt"}


def _dedupe_evidence(items: Iterable[str], limit: int = 4) -> list[str]:
    result: list[str] = []
    for raw in items:
        value = visible_excerpt(raw, 220)
        if not value or _is_markup_noise(value) or value in result:
            continue
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _hacker_news_items(value: str) -> list[tuple[str, str]]:
    """Extract a community title and its observable ranking context.

    Hacker News is a community source, so ranking context is evidence of
    attention rather than evidence of a product capability.  Keeping it next
    to the title avoids presenting a bare headline as a market fact.
    """
    matches = re.findall(
        r"(?:^|\s)\d+\.\s+(.+?)\s+\(\s*([^()]+)\s*\)\s+(\d+)\s+points\b.*?\|\s*(\d+)\s+comments",
        value,
    )
    items: list[tuple[str, str]] = []
    seen: set[str] = set()
    for title, domain, points, comments in matches:
        title = visible_excerpt(title, 220)
        if not title or title in seen:
            continue
        seen.add(title)
        items.append((title, f"Source: {domain.strip()}; {points} points; {comments} comments"))
        if len(items) >= 4:
            break
    return items


def _youtube_atom_items(value: str) -> list[str]:
    """Extract title/date pairs from the browser-rendered Atom feed text."""
    parts = [part.strip() for part in value.split("；")]
    items: list[str] = []
    for index in range(len(parts) - 2):
        title, published, link = parts[index:index + 3]
        if title and re.match(r"^20\d{2}-\d{2}-\d{2}T", published) and URL_RE.match(link):
            items.append(f"{title}（{published[:10]}）")
    return _dedupe_evidence(items)


def _atom_entries(value: str, limit: int = 8) -> list[dict[str, str]]:
    """Extract Atom entry fields from XML without retaining feed boilerplate."""
    try:
        root = ET.fromstring(value)
    except (ET.ParseError, ValueError):
        return []
    entries: list[dict[str, str]] = []
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1].casefold() != "entry":
            continue
        values: dict[str, str] = {}
        for child in list(node):
            name = child.tag.rsplit("}", 1)[-1].casefold()
            if name in {"title", "published", "updated", "summary", "content"} and child.text:
                values[name] = child.text.strip()
            elif name == "link":
                href = str(child.attrib.get("href") or child.text or "").strip()
                if href:
                    values["link"] = href
        # YouTube Atom feeds place the usable description below media:group
        # rather than in a direct Atom <summary>.  Preserve it as summary so
        # a video title is never the only evidence in a report.
        if not values.get("summary") and not values.get("content"):
            for child in node.iter():
                name = child.tag.rsplit("}", 1)[-1].casefold()
                if name in {"description", "summary", "content"} and child.text:
                    values["summary"] = child.text.strip()
                    break
        title = visible_excerpt(values.get("title"), 180)
        if title:
            entries.append({
                "title": title,
                "published": visible_excerpt(values.get("published") or values.get("updated"), 40),
                "link": values.get("link", ""),
                "summary": visible_excerpt(values.get("summary") or values.get("content"), 720),
            })
        if len(entries) >= limit:
            break
    return entries


def _openai_news_items(value: str) -> list[str]:
    """Extract card titles around the dated category labels in OpenAI News."""
    marker = re.compile(r"\s+(?:AI\s*采用|安全|产品|公司)\s+20\d{2}年\d{1,2}月\d{1,2}日")
    cursor = 0
    items: list[str] = []
    for match in marker.finditer(value):
        candidate = value[cursor:match.start()].strip()
        cursor = match.end()
        for separator in ("切换卡片以隐藏媒体", "加载更多", "OpenAI 新闻 | OpenAI"):
            if separator in candidate:
                candidate = candidate.rsplit(separator, 1)[-1].strip()
        if 4 <= len(candidate) <= 220:
            items.append(candidate)
    return _dedupe_evidence(items)


def _anthropic_news_items(value: str) -> list[str]:
    """Extract newsroom descriptions that accompany the product title."""
    patterns = [
        r"Our most advanced models for coding and knowledge work\. Their research capabilities also offer an early glimpse of how AI models will contribute to scientific progress\.",
        r"Previewing the Model Hardware Standard.*?advanced manufacturers\.",
    ]
    items: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, value):
            item = visible_excerpt(match.group(0), 420)
            if item and item not in items:
                items.append(item)
    return items[:4]


def _load_translation_overrides(project_root: Path) -> dict[str, str]:
    path = project_root / TRANSLATION_RELATIVE
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    translations = value.get("translations") if isinstance(value, Mapping) else None
    if not isinstance(translations, Mapping):
        return {}
    return {str(key): str(item) for key, item in translations.items() if str(key).strip() and str(item).strip()}


def _lookup_translation(lookup: Mapping[str, str], original: str) -> str:
    """Find an exact override, or an intentional stable-prefix override.

    Browser-visible card copy can gain a trailing timestamp or ellipsis.  A
    prefix entry lets the curated translation remain reusable without treating
    an unrelated short title as translated.  Prefix keys are explicit in the
    override file as ``prefix:<stable source text>``.
    """
    exact = str(lookup.get(original, "") or "").strip()
    if exact:
        return exact
    for key, translation in lookup.items():
        if not key.startswith("prefix:"):
            continue
        prefix = key[len("prefix:"):].strip()
        if len(prefix) >= 40 and original.startswith(prefix):
            return str(translation).strip()
    return ""


def _automatic_translation(source_id: str, original: str) -> str:
    """Translate deterministic dynamic labels without calling a remote model."""
    if source_id == "github-agent":
        match = re.fullmatch(r"Here are ([\d,]+) public repositories matching this topic\.\.\.", original.strip())
        if match:
            return f"有 {match.group(1)} 个公开仓库匹配该主题……"
    if source_id == "hackernews-ai":
        match = re.fullmatch(r"(.+?) \(Source: (.+?); (\d+) points; (\d+) comments\)", original.strip())
        if match:
            return f"{match.group(1)}（来源：{match.group(2)}；{match.group(3)} 分；{match.group(4)} 条评论）"
    return ""


def _source_evidence_records(
    row: Mapping[str, Any],
    *,
    translations: Mapping[str, str] | None = None,
    limit: int = 6,
) -> list[dict[str, Any]]:
    """Return auditable source evidence with original text and Chinese translation."""
    source_id = str(row.get("source_id") or "")
    body = str(row.get("body_excerpt") or "")
    candidates: list[tuple[str, str, str, str]] = []

    def add(original: Any, kind: str, published: Any = "", link: Any = "") -> None:
        value = visible_excerpt(original, 420)
        if not value or _is_markup_noise(value) or any(item[0] == value for item in candidates):
            return
        candidates.append((value, kind, visible_excerpt(published, 40), str(link or "").strip()))

    entries = row.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, Mapping):
                title = visible_excerpt(entry.get("title"), 180)
                summary = visible_excerpt(entry.get("summary"), 720)
                if title and summary:
                    add(f"{title}：{summary}", "summary", entry.get("published"), entry.get("link"))
                    break
    details = row.get("details")
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, Mapping):
                continue
            title = visible_excerpt(detail.get("title"), 240)
            description = visible_excerpt(detail.get("description"), 420)
            text = visible_excerpt(detail.get("text"), 2400)
            # Detail pages often repeat the headline in body text and may
            # append subscription or cookie copy.  A title plus the page's
            # own description is still source-original content and is the
            # cleanest report quote; use body text only when no description
            # was exposed.
            original = "；".join(item for item in (title, description or text) if item)
            add(original, "detail", link=detail.get("href") or detail.get("url"))
            if candidates:
                break
    blocks = row.get("content_blocks")
    if isinstance(blocks, list) and not candidates:
        ranked_blocks = [block for block in blocks if isinstance(block, Mapping)]
        if source_id == "github-agent":
            ranked_blocks.sort(key=lambda block: 0 if "langgenius / dify" in str(block.get("text") or "").casefold() else 1 if "openhands / openhands" in str(block.get("text") or "").casefold() else 2)
        elif source_id == "anthropic-news":
            ranked_blocks.sort(key=lambda block: 0 if "introducing claude fable" in str(block.get("text") or "").casefold() else 1 if "model hardware standard" in str(block.get("text") or "").casefold() else 2)
        for block in ranked_blocks:
            if isinstance(block, Mapping):
                # A card title is an index signal.  Only retain cards which
                # carry their explanatory copy, description, or activity
                # context as report evidence.
                if len(visible_excerpt(block.get("text"), 720)) >= 100:
                    add(block.get("text"), "card", link=block.get("href"))
                    if candidates:
                        break
    if source_id == "youtube-ai":
        for item in _youtube_atom_items(body):
            add(item, "entry")
    elif source_id == "hackernews-ai":
        community_items = _hacker_news_items(body)
        community_items.sort(key=lambda item: 0 if re.search(r"\b(agent|ai|muse|gemini|llm|model)\b", item[0], re.IGNORECASE) else 1)
        for title, metrics in community_items:
            add(f"{title} ({metrics})", "community")
            break
    elif source_id == "openai-agents":
        for item in _openai_news_items(body):
            add(item, "headline")
    elif source_id == "anthropic-news":
        for item in _anthropic_news_items(body):
            add(item, "description")
    if not candidates:
        description = visible_excerpt(row.get("meta_description"), 720)
        if source_id == "producthunt-ai" and len(description) >= 100:
            add(description, "summary")
        else:
            headlines = row.get("headlines")
            if isinstance(headlines, list):
                for item in headlines:
                    value = visible_excerpt(item, 180)
                    if value.casefold() not in GENERIC_HEADLINES:
                        add(value, "headline")
                        break
            page_title = visible_excerpt(row.get("page_title") or row.get("title_excerpt"), 180)
            if page_title and page_title.casefold() not in GENERIC_PAGE_TITLES:
                add(page_title, "page_title")
            if description:
                add(description, "description")
        excerpt = visible_excerpt(body, 320)
        if excerpt and not candidates:
            add(excerpt, "body_excerpt")

    lookup = dict(translations or {})
    records: list[dict[str, Any]] = []
    for original, kind, published, link in candidates[:limit]:
        translated = _lookup_translation(lookup, original) or _automatic_translation(source_id, original)
        if not translated and re.search(r"[\u3400-\u9fff]", original):
            translated = original
            status = "not_needed"
        elif translated:
            status = "translated"
        else:
            status = "missing"
        records.append({
            "original": original,
            "translation_zh": translated,
            "translation_status": status,
            "kind": kind,
            "published": published,
            "link": link,
        })
    return records


def _source_evidence_items(row: Mapping[str, Any], limit: int = 4) -> list[str]:
    """Return concrete retrieved page fields for display, without analysis."""
    records = row.get("original_evidence")
    if not isinstance(records, list):
        records = _source_evidence_records(row, limit=limit)
    items: list[str] = []
    for record in records[:limit]:
        if not isinstance(record, Mapping):
            continue
        original = visible_excerpt(record.get("original"), 220)
        if not original:
            continue
        kind = str(record.get("kind") or "")
        prefix = "页面标题：" if kind == "page_title" else "页面摘要：" if kind == "description" else "详情：" if kind == "detail" else ""
        published = visible_excerpt(record.get("published"), 40)
        suffix = f"（{published[:10]}）" if published and kind == "entry" else ""
        items.append(f"{prefix}{original}{suffix}")
    return _dedupe_evidence(items, limit)


def _signal_content(row: Mapping[str, Any]) -> str:
    """Select the most useful evidence text without inventing an update."""
    items = _source_evidence_items(row, limit=2)
    if items:
        return "；".join(items)[:420]
    for key in ("page_title", "meta_description", "content_excerpt", "body_excerpt", "title_excerpt"):
        value = visible_excerpt(row.get(key) or "", 420)
        if key == "page_title" and value.casefold() in GENERIC_PAGE_TITLES:
            continue
        if value and not _is_markup_noise(value):
            return value
    return ""


def _content_depth(row: Mapping[str, Any]) -> str:
    """Classify whether a source contains detail text or only an index signal."""
    explicit = str(row.get("content_depth") or "").strip().casefold()
    if explicit in {"detail", "card", "summary", "body", "community"}:
        return explicit
    records = row.get("original_evidence")
    if isinstance(records, list) and any(isinstance(item, Mapping) and str(item.get("kind") or "") in {"detail", "card", "summary", "body_excerpt", "community", "community_metrics"} for item in records):
        return "body"
    source_id = str(row.get("source_id") or "")
    if source_id == "hackernews-ai" and _hacker_news_items(str(row.get("body_excerpt") or "")):
        return "community"
    if source_id == "producthunt-ai" and len(visible_excerpt(row.get("meta_description"), 720)) >= 100:
        return "summary"
    if row.get("details") or row.get("content_blocks"):
        return "card"
    if isinstance(row.get("entries"), list) and any(isinstance(item, Mapping) and str(item.get("summary") or "").strip() for item in row.get("entries")):
        return "summary"
    return "metadata"


def _has_content_evidence(row: Mapping[str, Any]) -> bool:
    """Whether a retrieved row can support a report conclusion.

    A transport success is not sufficient.  Titles, category metadata, and
    navigation are useful diagnostics, but they must remain outside industry
    conclusions until the collector has actual card, summary, detail, or
    community-context text.
    """
    if row.get("retrieval_status") != "ok" or _content_depth(row) == "metadata":
        return False
    records = row.get("original_evidence")
    if not isinstance(records, list):
        records = _source_evidence_records(row, limit=6)
    for item in records:
        if not isinstance(item, Mapping):
            continue
        kind = str(item.get("kind") or "")
        original = visible_excerpt(item.get("original"), 720)
        if kind in {"detail", "summary", "body_excerpt"} and len(original) >= 80:
            return True
        if kind == "card" and len(original) >= 100:
            return True
        if kind in {"community", "community_metrics"} and len(original) >= 25:
            return True
    return False


def _source_summary(row: Mapping[str, Any]) -> str:
    vendor = str(row.get("vendor") or "该厂商")
    product = str(row.get("product") or "该产品")
    content = _signal_content(row)
    depth = _content_depth(row)
    if depth == "metadata":
        return f"{vendor}「{product}」本次只抓到来源索引信息（标题/分类描述），未抓到详情正文；不能据此断言新增能力。"
    if content:
        return f"{vendor}「{product}」本次具体抓取到：{content}。"
    return f"{vendor}「{product}」来源已成功读取，但本期快照未提取到具体版本或功能标题，不能据此断言新增能力。"


def _route_summary(route: str, valid: list[Mapping[str, Any]], all_signals: list[Mapping[str, Any]]) -> tuple[str, str]:
    rows = [row for row in valid if row.get("route") == route]
    unavailable = [row for row in all_signals if row.get("route") == route and row.get("retrieval_status") != "ok"]
    label = ROUTE_LABELS.get(route, "观察项")
    if not rows:
        missing = "、".join(str(row.get("source") or row.get("source_id")) for row in unavailable) or "本路线来源"
        return f"{label}本期没有可复核来源（{missing}不可用），暂不形成行业判断。", f"先补齐{label}的来源覆盖，再决定跟随或投入。"
    names = "、".join(str(row.get("vendor") or row.get("source") or "未知来源") for row in rows)
    caveat = f"；另有{len(unavailable)}个来源不可用，不能横向代表全行业" if unavailable else ""
    evidence = "；".join(item for item in (_signal_content(row) for row in rows) if item)
    evidence_clause = f"主要线索：{evidence[:360]}。" if evidence else ""
    return f"{label}本期有{len(rows)}条有效信号，来自{names}{caveat}。{evidence_clause}当前可确认的是{ROUTE_REFERENCE.get(route, '持续观察产品能力与交付价值')}，不把页面可用性等同于新增功能。", f"{ROUTE_REFERENCE.get(route, '持续观察产品能力与交付价值')}；以成功来源作为观察依据{caveat}。"


def _report_summaries(all_signals: list[Mapping[str, Any]], valid: list[Mapping[str, Any]]) -> dict[str, Any]:
    route_details = {}
    for route in ("A", "B", "C"):
        summary, reference = _route_summary(route, valid, all_signals)
        route_details[route] = {"summary": summary, "reference": reference}
    route_counts = {route: sum(1 for row in valid if row.get("route") == route) for route in ("A", "B", "C")}
    strongest = max(route_counts, key=route_counts.get) if valid else None
    if strongest and route_counts[strongest] >= 2:
        industry = f"本期可复核信息主要集中在路线 {strongest}（{ROUTE_LABELS[strongest]}），有{route_counts[strongest]}条有效信号；它代表能力/关注方向正在被持续讨论或发布，但不等于市场份额或商业成功。"
    elif valid:
        industry = f"本期{len(valid)}条有效信号覆盖{sum(1 for count in route_counts.values() if count)}条路线；来源缺口使横向比较不完整，以下结论只覆盖已成功读取的页面。"
    else:
        industry = "本期没有可复核信号，不能形成竞品或行业结论。"
    if any(row.get("route") == "C" for row in valid):
        shengsuan = "对胜算最直接的参考是：把 Agent 的执行 Runtime、MCP/Skills 或 Computer Use 能力，与企业数据/语义层、治理和 Action 闭环结合，形成可审计、可恢复的交付能力。"
    elif any(row.get("route") == "A" for row in valid):
        shengsuan = "当前可落地的参考是：继续强化企业数据/语义层、治理和 Action 闭环；其他路线证据不足，暂不据此调整个人化产品方向。"
    else:
        shengsuan = "当前证据不足以调整胜算路线；优先补齐官方产品和能力证据。"
    actions = []
    if any(row.get("route") == "A" for row in valid):
        actions.append(("跟随", "将数据/语义层、治理与 Action 闭环作为企业 Agent 的核心验收项。"))
    if any(row.get("route") == "C" for row in valid):
        actions.append(("借鉴", "评估 Runtime、MCP/Skills、Computer Use 的执行日志、权限控制和失败恢复。"))
    if any(row.get("route") == "B" for row in valid):
        actions.append(("验证", "用个人专业工作台场景验证持久上下文、引用溯源和 token 成本模型。"))
    if not actions:
        actions.append(("补证", "先恢复来源或补充人工专题，再形成产品动作。"))
    return {"industry": industry, "shengsuan": shengsuan, "route_details": route_details, "actions": actions}


def atomic_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(value, encoding="utf-8")
    os.replace(temp, path)


def atomic_json(path: Path, value: Any) -> None:
    atomic_write(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def registry_path(project_root: Path) -> Path:
    return project_root / REGISTRY_RELATIVE


def load_registry(project_root: Path) -> dict[str, Any]:
    path = registry_path(project_root)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("sources"), list):
        raise ValueError("competitive source registry must contain sources[]")
    return value


def normalize_url(raw: Any) -> tuple[str | None, str | None]:
    value = str(raw or "").strip()
    if not URL_RE.match(value):
        return None, "url_scheme_not_allowed"
    try:
        parsed = urllib.parse.urlsplit(value)
        if parsed.username or parsed.password:
            return None, "credentials_in_url"
        host = (parsed.hostname or "").lower().rstrip(".")
        if not host:
            return None, "url_host_missing"
        if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
            return None, "private_host_rejected"
        try:
            address = ipaddress.ip_address(host)
            if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
                return None, "private_host_rejected"
        except ValueError:
            # DNS names are accepted.  Resolution is intentionally not used
            # as an authorization bypass; private DNS is handled as fetch
            # failure and never receives credentials.
            pass
        query = []
        for key, val in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower().startswith("utm_") or key.lower() in TRACKING_KEYS:
                continue
            query.append((key, val))
        normalized = urllib.parse.urlunsplit(
            (parsed.scheme.lower(), host + (f":{parsed.port}" if parsed.port else ""), parsed.path or "/", urllib.parse.urlencode(sorted(query)), "")
        )
        return normalized, None
    except (ValueError, UnicodeError):
        return None, "url_invalid"


def _fetch(url: str, timeout: int = 8, headers: Mapping[str, str] | None = None) -> dict[str, Any]:
    request_headers = dict(DEFAULT_FETCH_HEADERS)
    request_headers.update({str(key): str(value) for key, value in dict(headers or {}).items()})
    request = urllib.request.Request(url, headers=request_headers)
    captured_at = now_iso()
    try:
        parsed = urllib.parse.urlsplit(url)
        host = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
        # Resolve before opening the socket and reject every address class that
        # must never be reached by a public-source collector.  Redirects are
        # disabled above, so this check covers the only URL connection made by
        # this bounded fetch.
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        if not addresses:
            return {"retrieval_status": "unavailable", "failure_reason": "dns_no_address", "captured_at": captured_at}
        for _family, _socktype, _proto, _canonname, sockaddr in addresses:
            address = ipaddress.ip_address(str(sockaddr[0]))
            if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_unspecified or address.is_multicast:
                return {"retrieval_status": "rejected", "failure_reason": "private_resolved_host_rejected", "captured_at": captured_at}
        opener = urllib.request.build_opener(_NoRedirect)
        with opener.open(request, timeout=timeout) as response:
            body = response.read(MAX_BODY_BYTES + 1)
            truncated = len(body) > MAX_BODY_BYTES
            body = body[:MAX_BODY_BYTES]
            content_type = str(response.headers.get("Content-Type") or "")
            text = body.decode("utf-8", errors="replace")
            is_markup = "html" in content_type.lower() or "xml" in content_type.lower()
            fields = structured_page_fields(text) if is_markup else {"page_title": "", "meta_description": "", "headlines": []}
            atom_entries = _atom_entries(text) if "xml" in content_type.lower() else []
            readable = visible_excerpt(text, 1200)
            return {
                "retrieval_status": "ok",
                "http_status": int(getattr(response, "status", 200) or 200),
                "content_type": content_type,
                "captured_at": captured_at,
                "content_hash": sha256_bytes(body),
                "body_bytes": len(body),
                "truncated": truncated,
                "title_excerpt": fields["page_title"] or readable[:280],
                "body_excerpt": readable,
                **fields,
                "entries": atom_entries,
            }
    except urllib.error.HTTPError as exc:
        return {"retrieval_status": "unavailable", "failure_reason": f"http_{exc.code}", "captured_at": captured_at}
    except ValueError:
        return {"retrieval_status": "rejected", "failure_reason": "private_resolved_host_rejected", "captured_at": captured_at}
    except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
        return {"retrieval_status": "unavailable", "failure_reason": type(exc).__name__, "captured_at": captured_at}


def _fetch_bounded(url: str, *, timeout: int, headers: Mapping[str, str], wall_timeout: int) -> dict[str, Any]:
    """Run one HTTP fetch outside the handler process and enforce a wall clock.

    urllib's connect timeout is not a reliable total deadline on every macOS
    network path.  A stuck TCP handshake must not occupy a PM Worker slot.
    """
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "fetch",
        url,
        "--timeout",
        str(timeout),
        "--headers-json",
        json.dumps(dict(headers), ensure_ascii=False, separators=(",", ":")),
    ]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=wall_timeout, check=False)
    except subprocess.TimeoutExpired:
        return {"retrieval_status": "unavailable", "failure_reason": "fetch_wall_timeout", "captured_at": now_iso()}
    if completed.returncode != 0:
        return {"retrieval_status": "unavailable", "failure_reason": f"fetch_exit_{completed.returncode}", "captured_at": now_iso()}
    for line in reversed(completed.stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("retrieval_status"):
            return value
    return {"retrieval_status": "unavailable", "failure_reason": "fetch_invalid_output", "captured_at": now_iso()}


def _bounded_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(value)))
    except (TypeError, ValueError):
        return default


def _fetch_policy(source: Mapping[str, Any]) -> dict[str, Any]:
    raw = source.get("fetch_policy")
    policy = dict(raw) if isinstance(raw, Mapping) else {}
    raw_headers = policy.get("headers")
    headers = {str(key): str(value) for key, value in raw_headers.items()} if isinstance(raw_headers, Mapping) else {}
    return {
        "timeout_seconds": _bounded_int(policy.get("timeout_seconds"), 8, minimum=1, maximum=30),
        "wall_timeout_seconds": _bounded_int(policy.get("wall_timeout_seconds"), 10, minimum=3, maximum=35),
        "max_attempts": _bounded_int(policy.get("max_attempts"), 1, minimum=1, maximum=3),
        "retry_delay_seconds": _bounded_int(policy.get("retry_delay_seconds"), 0, minimum=0, maximum=5),
        "headers": headers,
    }


def _fetch_source(source: Mapping[str, Any], url: str) -> dict[str, Any]:
    """Fetch one registry source with its bounded, source-local HTTP policy."""
    policy = _fetch_policy(source)
    latest: dict[str, Any] = {}
    for attempt in range(1, policy["max_attempts"] + 1):
        latest = _fetch_bounded(
            url,
            timeout=policy["timeout_seconds"],
            headers=policy["headers"],
            wall_timeout=max(policy["wall_timeout_seconds"], policy["timeout_seconds"] + FETCH_WALL_TIMEOUT_GRACE_SECONDS),
        )
        latest["request_attempts"] = attempt
        if latest.get("retrieval_status") in {"ok", "rejected"}:
            return latest
        if attempt < policy["max_attempts"] and policy["retry_delay_seconds"]:
            time.sleep(policy["retry_delay_seconds"])
    return latest


def _browser_fallback_policy(source: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return an explicit browser fallback policy for a curated registry source.

    Browser navigation is intentionally never enabled for the PM quick-inbox:
    it is only available to named, locally maintained public sources.
    """
    raw = source.get("browser_fallback")
    if not isinstance(raw, Mapping) or raw.get("mode") != "public_dom":
        return None
    try:
        host = (urllib.parse.urlsplit(str(source.get("url") or "")).hostname or "").casefold().rstrip(".")
    except ValueError:
        return None
    if host not in BROWSER_FALLBACK_HOSTS:
        return None
    failures = raw.get("on_failure")
    if not isinstance(failures, list) or not all(isinstance(item, str) for item in failures):
        return None
    content_policy = source.get("content_policy")
    if not isinstance(content_policy, Mapping):
        content_policy = {}
    return {
        "on_failure": set(failures),
        "timeout_seconds": _bounded_int(raw.get("timeout_seconds"), BROWSER_FALLBACK_TIMEOUT_SECONDS, minimum=10, maximum=45),
        "page_timeout_seconds": _bounded_int(content_policy.get("page_timeout_seconds"), 11, minimum=6, maximum=20),
        "detail_timeout_seconds": _bounded_int(content_policy.get("detail_timeout_seconds"), 8, minimum=5, maximum=12),
        "max_detail_pages": _bounded_int(content_policy.get("max_detail_pages"), 0, minimum=0, maximum=1),
    }


def _browser_script(
    task_name: str,
    url: str,
    page_timeout_seconds: int,
    detail_timeout_seconds: int,
    max_detail_pages: int,
) -> str:
    """Build a small public-DOM-only ego-browser extraction script."""
    allowed_hosts = sorted(DETAIL_HOSTS)
    return f'''const task = await useOrCreateTaskSpace({json.dumps(task_name)})
const tab = await openOrReuseTab({json.dumps(url)}, {{ wait: false }})
await switchTab(tab.targetId)
await gotoAndWait({json.dumps(url)}, {{ timeout: {page_timeout_seconds}, settle: 1 }})
const initialRaw = await js(String.raw`(() => {{
  const pickMeta = () => {{
    for (const selector of ['meta[name="description"]', 'meta[property="og:description"]', 'meta[name="twitter:description"]']) {{
      const value = document.querySelector(selector)?.getAttribute('content')?.trim()
      if (value) return value
    }}
    return ''
  }}
  const headings = [...document.querySelectorAll('h1,h2,h3')]
    .map((node) => node.innerText.trim())
    .filter(Boolean)
    .filter((value, index, values) => values.indexOf(value) === index)
    .slice(0, 12)
  const entries = [...document.querySelectorAll('entry')].slice(0, 8).map((entry) => {{
    const link = entry.querySelector('link')?.getAttribute('href') || entry.querySelector('link')?.textContent?.trim() || ''
    const title = entry.querySelector('title')?.textContent?.trim() || ''
    const published = entry.querySelector('published')?.textContent?.trim() || entry.querySelector('updated')?.textContent?.trim() || ''
    const summary = entry.querySelector('summary,content')?.textContent?.trim() || ''
    return {{ title, published, link, summary }}
  }}).filter((entry) => entry.title || entry.link)
  const allowedHosts = {json.dumps(allowed_hosts, ensure_ascii=False)}
  const publicHref = (href) => {{
    try {{
      const parsed = new URL(href, location.href)
      if (!['http:', 'https:'].includes(parsed.protocol)) return ''
      if (!allowedHosts.includes(parsed.hostname.toLowerCase())) return ''
      parsed.hash = ''
      return parsed.href
    }} catch (_) {{ return '' }}
  }}
  const links = [...document.querySelectorAll('a[href]')].map((anchor) => ({{
    text: anchor.innerText?.replace(/\\s+/g, ' ').trim() || '',
    href: publicHref(anchor.href)
  }})).filter((item) => item.text && item.href)
    .filter((item, index, values) => values.findIndex((candidate) => candidate.href === item.href) === index)
    .slice(0, 80)
  const contentBlocks = [...document.querySelectorAll('article, main li, main [data-testid*="card"], main [class*="card"], main [class*="Card"], main .Box-row, tr.athing, li')]
    .filter((node) => !node.closest('header, nav, footer'))
    .map((node) => ({{
      text: node.innerText?.replace(/\\s+/g, ' ').trim() || '',
      href: publicHref(node.querySelector('a[href]')?.href || '')
    }}))
    .filter((item) => item.text && item.text.length >= 40 && item.text.length <= 1400)
    .filter((item, index, values) => values.findIndex((candidate) => candidate.text === item.text) === index);
  const richLinks = links
    .filter((item) => item.text.length >= 80 && item.text.length <= 1400)
    .filter((item) => !/^(skip to main content|policy|privacy|terms|careers|help|learn more)$/i.test(item.text));
  const combinedBlocks = [...contentBlocks, ...richLinks]
    .filter((item, index, values) => values.findIndex((candidate) => candidate.text === item.text) === index)
    .slice(0, 30)
  return JSON.stringify({{
    url: location.href,
    title: document.title.trim(),
    description: pickMeta(),
    headings,
    entries,
    links,
    content_blocks: combinedBlocks,
    text: document.body?.innerText?.replace(/\\s+/g, ' ').trim().slice(0, 16384) || ''
  }})
}})()`)
let initial = initialRaw
try {{ initial = typeof initialRaw === 'string' ? JSON.parse(initialRaw) : initialRaw }} catch (_) {{ initial = {{}} }}
const details = []
if ({max_detail_pages} > 0) {{
  const candidates = [...(initial.content_blocks || []), ...(initial.links || [])]
    .filter((item) => item && item.href && item.href !== initial.url && item.text)
    .filter((item, index, values) => values.findIndex((candidate) => candidate.href === item.href) === index)
    .filter((item) => item.text.length >= 40 && !/sitemap|\\.xml($|\\?)/i.test(item.href) && !/cookie|privacy|consent/i.test(item.text) && !/^(navigation menu|news|products|models|solutions|resources|platform|company|terms and policies|skip to main content|policy)$/i.test(item.text))
    .slice(0, {max_detail_pages})
  for (const candidate of candidates) {{
    try {{
      const detailTab = await openOrReuseTab(candidate.href, {{ wait: false }})
      await switchTab(detailTab.targetId)
      await gotoAndWait(candidate.href, {{ timeout: {detail_timeout_seconds}, settle: 1 }})
      const detailRaw = await js(String.raw`(() => {{
        const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim()
        const meta = [...document.querySelectorAll('meta[name="description"], meta[property="og:description"], meta[name="twitter:description"]')]
          .map((node) => node.getAttribute('content') || '').map(clean).find(Boolean) || ''
        const nodes = ['article', 'main', '[role="main"]'].map((selector) => document.querySelector(selector)).filter(Boolean)
        const node = nodes.find((candidate) => clean(candidate.innerText).length >= 80) || document.body
        return JSON.stringify({{ url: location.href, title: clean(document.title), description: meta, text: clean(node?.innerText).slice(0, 4200) }})
      }})()`)
      let detail = detailRaw
      try {{ detail = typeof detailRaw === 'string' ? JSON.parse(detailRaw) : detailRaw }} catch (_) {{ detail = {{}} }}
      if (detail && (detail.text || detail.description || detail.title)) details.push({{ ...candidate, ...detail, content_kind: 'detail' }})
    }} catch (_) {{ /* one bad detail must not discard the source page */ }}
  }}
}}
cliLog('PM_COMPETITIVE_RADAR_BROWSER_RESULT:' + JSON.stringify({{ ...initial, details }}))
cliLog('PM_COMPETITIVE_RADAR_BROWSER_TASK:' + task.id)
'''


def _browser_complete_script(task_id: int) -> str:
    return f"await completeTaskSpace({task_id}, {{ keep: false }})\n"


def _browser_output_payload(output: str) -> tuple[dict[str, Any] | None, int | None]:
    payload: dict[str, Any] | None = None
    task_id: int | None = None
    for line in output.splitlines():
        if line.startswith("PM_COMPETITIVE_RADAR_BROWSER_RESULT:"):
            try:
                candidate = json.loads(line.split(":", 1)[1])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                payload = candidate
        elif line.startswith("PM_COMPETITIVE_RADAR_BROWSER_TASK:"):
            try:
                task_id = int(line.split(":", 1)[1])
            except ValueError:
                continue
    return payload, task_id


def _browser_fetch(
    url: str,
    *,
    source_id: str,
    run_id: str,
    timeout: int,
    max_detail_pages: int = 0,
    page_timeout_seconds: int = 11,
    detail_timeout_seconds: int = 8,
) -> dict[str, Any]:
    """Use a browser only as a bounded fallback for curated public sources."""
    captured_at = now_iso()
    task_name = f"pm-loop-radar-{re.sub(r'[^a-z0-9-]+', '-', source_id.casefold())[:32]}-{hashlib.sha256(run_id.encode()).hexdigest()[:8]}"
    task_id: int | None = None
    try:
        completed = subprocess.run(
            [str(BROWSER_FALLBACK_EXECUTABLE), "nodejs"],
            input=_browser_script(
                task_name,
                url,
                _bounded_int(page_timeout_seconds, 11, minimum=6, maximum=20),
                _bounded_int(detail_timeout_seconds, 8, minimum=5, maximum=12),
                _bounded_int(max_detail_pages, 0, minimum=0, maximum=1),
            ),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        # ego-browser writes cliLog to stderr in some launchd/PTY contexts.
        # Parse both streams while keeping only the explicit result markers.
        payload, task_id = _browser_output_payload("\n".join((completed.stdout, completed.stderr)))
        if completed.returncode != 0 or not payload:
            reason = "browser_no_public_dom"
            if completed.returncode != 0:
                reason = f"browser_exit_{completed.returncode}"
            return {"retrieval_status": "unavailable", "failure_reason": reason, "captured_at": captured_at}
        resolved, reason = normalize_url(payload.get("url"))
        if not resolved or reason:
            return {"retrieval_status": "unavailable", "failure_reason": "browser_resolved_url_rejected", "captured_at": captured_at}
        text = str(payload.get("text") or "")
        title = visible_excerpt(payload.get("title"), 280)
        description = visible_excerpt(payload.get("description"), 360)
        headlines = [visible_excerpt(item, 180) for item in payload.get("headings", []) if isinstance(item, str) and item.strip()]
        entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
        content_blocks = payload.get("content_blocks") if isinstance(payload.get("content_blocks"), list) else []
        details = payload.get("details") if isinstance(payload.get("details"), list) else []
        links = payload.get("links") if isinstance(payload.get("links"), list) else []
        entry_lines = []
        for entry in entries[:8]:
            if not isinstance(entry, Mapping):
                continue
            entry_title = visible_excerpt(entry.get("title"), 180)
            entry_date = visible_excerpt(entry.get("published"), 40)
            entry_link = str(entry.get("link") or "").strip()
            entry_summary = visible_excerpt(entry.get("summary"), 600)
            if entry_title:
                entry_lines.append("；".join(item for item in (entry_title, entry_summary, entry_date, entry_link) if item))
        if entry_lines:
            # XML viewers prepend a generic explanatory sentence and flatten
            # namespace nodes into noise.  Atom entry fields are the durable
            # evidence we want in the report.
            text = "；".join(entry_lines)
            if not title or title.casefold() in {"xml", "youtube"}:
                title = entry_lines[0].split("；", 1)[0]
        block_lines = [visible_excerpt(item.get("text"), 1200) for item in content_blocks if isinstance(item, Mapping) and visible_excerpt(item.get("text"), 1200)]
        detail_lines = ["；".join(item for item in (visible_excerpt(item.get("title"), 240), visible_excerpt(item.get("description"), 360), visible_excerpt(item.get("text"), 2400)) if item) for item in details if isinstance(item, Mapping)]
        detail_lines = [item for item in detail_lines if item and not item.casefold().startswith("this xml file does not appear") and "document tree is shown below" not in item.casefold()]
        details = [item for item in details if isinstance(item, Mapping) and not visible_excerpt(item.get("text"), 2400).casefold().startswith("this xml file does not appear")]
        evidence_text = "\n".join((title, description, "\n".join(headlines), "\n".join(block_lines), "\n".join(detail_lines), text)).strip()
        if not evidence_text:
            return {"retrieval_status": "unavailable", "failure_reason": "browser_empty_public_dom", "captured_at": captured_at}
        body = evidence_text.encode("utf-8")[:MAX_BODY_BYTES]
        return {
            "retrieval_status": "ok",
            "http_status": 200,
            "content_type": "text/html; source=browser-dom",
            "captured_at": captured_at,
            "content_hash": sha256_bytes(body),
            "body_bytes": len(body),
            "truncated": len(evidence_text.encode("utf-8")) > len(body),
            "title_excerpt": title or visible_excerpt(text, 280),
            "body_excerpt": visible_excerpt("\n".join(detail_lines or block_lines or [text]), 1200),
            "page_title": title,
            "meta_description": description,
            "headlines": headlines,
            "entries": [
                {"title": visible_excerpt(entry.get("title"), 180), "published": visible_excerpt(entry.get("published"), 40), "link": str(entry.get("link") or "").strip(), "summary": visible_excerpt(entry.get("summary"), 720)}
                for entry in entries[:8]
                if isinstance(entry, Mapping) and visible_excerpt(entry.get("title"), 180)
            ],
            "links": [
                {"text": visible_excerpt(item.get("text"), 240), "href": str(item.get("href") or "").strip()}
                for item in links[:80]
                if isinstance(item, Mapping) and str(item.get("href") or "").strip()
            ],
            "content_blocks": [
                {"text": visible_excerpt(item.get("text"), 1400), "href": str(item.get("href") or "").strip()}
                for item in content_blocks[:30]
                if isinstance(item, Mapping) and visible_excerpt(item.get("text"), 1400)
            ],
            "details": [
                {"title": visible_excerpt(item.get("title"), 240), "description": visible_excerpt(item.get("description"), 420), "text": visible_excerpt(item.get("text"), 3000), "href": str(item.get("url") or item.get("href") or "").strip(), "content_kind": "detail"}
                for item in details[:4]
                if isinstance(item, Mapping) and (visible_excerpt(item.get("text"), 3000) or visible_excerpt(item.get("description"), 420))
            ],
            "content_depth": "detail" if details else "card" if content_blocks else "summary" if any(isinstance(item, Mapping) and visible_excerpt(item.get("summary"), 40) for item in entries) else "index",
            "resolved_source_url": resolved,
            "retrieval_method": "browser_dom_enrichment" if max_detail_pages else "browser_dom_fallback",
        }
    except subprocess.TimeoutExpired:
        return {"retrieval_status": "unavailable", "failure_reason": "browser_timeout", "captured_at": captured_at}
    except OSError as exc:
        return {"retrieval_status": "unavailable", "failure_reason": f"browser_{type(exc).__name__}", "captured_at": captured_at}
    finally:
        if task_id is not None:
            try:
                subprocess.run(
                    [str(BROWSER_FALLBACK_EXECUTABLE), "nodejs"],
                    input=_browser_complete_script(task_id),
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        stream.write(json.dumps(dict(value), ensure_ascii=False, separators=(",", ":")) + "\n")
        stream.flush()
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def submit(raw_url: str, focus: str = "", *, state_root: Path = DEFAULT_STATE_ROOT) -> dict[str, Any]:
    normalized, reason = normalize_url(raw_url)
    submitted_at = now_iso()
    if not normalized:
        result = {"schema_version": "competitive-radar.inbox.v1", "status": "rejected", "url": str(raw_url), "reason": reason, "submitted_at": submitted_at}
        _append_jsonl(state_root / "inbox.jsonl", result)
        return result
    inbox_id = "inbox:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    path = state_root / "inbox.jsonl"
    existing = next((row for row in _read_jsonl(path) if row.get("inbox_id") == inbox_id), None)
    if existing:
        return {**existing, "status": "deduplicated"}
    result = {"schema_version": "competitive-radar.inbox.v1", "status": "accepted", "inbox_id": inbox_id, "url": normalized, "focus": str(focus or "").strip()[:240], "submitted_by": os.environ.get("SANDBOX_USERNAME", "zhujie14"), "submitted_at": submitted_at}
    _append_jsonl(path, result)
    return result


def _source_record(
    source: Mapping[str, Any],
    *,
    run_id: str,
    state_root: Path,
    translations: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    source_id = str(source.get("source_id") or "").strip()
    normalized, reason = normalize_url(source.get("url"))
    signal_seed = f"{source_id}:{normalized or str(source.get('url') or '')}"
    record: dict[str, Any] = {
        "signal_id": "signal:" + hashlib.sha256(signal_seed.encode()).hexdigest()[:20],
        "source_id": source_id,
        "source": source.get("source") or source_id,
        "source_url": normalized or str(source.get("url") or ""),
        "vendor": source.get("vendor"),
        "product": source.get("product"),
        "route": source.get("route"),
        "capability_layer": source.get("capability_layer"),
        "fact_type": "official_fact" if source.get("trust") == "official" else "community_feedback",
        "captured_at": now_iso(),
        "retrieval_method": source.get("fetch_mode") or "web",
        "freshness": "unknown",
        "review_status": "pending",
    }
    if not normalized:
        record.update({"retrieval_status": "rejected", "failure_reason": reason, "freshness": "unavailable"})
        return record
    fetched = _fetch_source(source, normalized)
    fallback = _browser_fallback_policy(source)
    if fallback and fetched.get("failure_reason") in fallback["on_failure"]:
        browser_result = _browser_fetch(
            normalized,
            source_id=source_id,
            run_id=run_id,
            timeout=fallback["timeout_seconds"],
            max_detail_pages=fallback["max_detail_pages"],
            page_timeout_seconds=fallback["page_timeout_seconds"],
            detail_timeout_seconds=fallback["detail_timeout_seconds"],
        )
        browser_result["http_failure_reason"] = fetched.get("failure_reason")
        if browser_result.get("retrieval_status") == "ok":
            fetched = browser_result
        else:
            fetched["fallback_attempted"] = True
            fetched["fallback_failure_reason"] = browser_result.get("failure_reason")
    elif fallback and fetched.get("retrieval_status") == "ok" and _content_depth({**source, **fetched}) == "metadata":
        # A 200 response can still be an index shell (or a JS challenge).
        # Enrich it once through the same public browser policy used for
        # failures so a successful transport is not mistaken for useful text.
        browser_result = _browser_fetch(
            normalized,
            source_id=source_id,
            run_id=run_id,
            timeout=fallback["timeout_seconds"],
            max_detail_pages=fallback["max_detail_pages"],
            page_timeout_seconds=fallback["page_timeout_seconds"],
            detail_timeout_seconds=fallback["detail_timeout_seconds"],
        )
        if browser_result.get("retrieval_status") == "ok" and _content_depth(browser_result) != "metadata":
            fetched = {**fetched, **browser_result, "http_content_hash": fetched.get("content_hash"), "retrieval_method": browser_result.get("retrieval_method")}
        else:
            fetched["enrichment_attempted"] = True
            fetched["enrichment_failure_reason"] = browser_result.get("failure_reason") or "browser_no_detail_content"
    record.update(fetched)
    if fetched.get("retrieval_status") == "ok":
        record["freshness"] = "fresh"
        record["signal_id"] = "signal:" + hashlib.sha256(f"{signal_seed}:{fetched.get('content_hash')}".encode()).hexdigest()[:20]
        # One source contributes one curated, content-level signal per run.
        # This prevents index titles and navigation labels from diluting the
        # report and makes the original/Chinese evidence pair auditable.
        original_evidence = _source_evidence_records({**source, **fetched}, translations=translations, limit=1)
        record["original_evidence"] = original_evidence
        record["content_depth"] = _content_depth({**record, **fetched, "original_evidence": original_evidence})
        record["translation_status"] = "translated" if original_evidence and all(item.get("translation_status") in {"translated", "not_needed"} for item in original_evidence) else "missing"
        evidence_id = "evidence:" + hashlib.sha256(f"{normalized}:{fetched.get('content_hash')}".encode()).hexdigest()[:20]
        snapshot_path = state_root / "raw" / source_id / f"{run_id}.json"
        snapshot = {
            "schema_version": "competitive-radar.evidence-snapshot.v2",
            "evidence_id": evidence_id,
            "source_url": normalized,
            "captured_at": fetched.get("captured_at"),
            "content_hash": fetched.get("content_hash"),
            "page_title": fetched.get("page_title") or fetched.get("title_excerpt") or "",
            "meta_description": fetched.get("meta_description") or "",
            "headlines": fetched.get("headlines") if isinstance(fetched.get("headlines"), list) else [],
            "entries": fetched.get("entries") if isinstance(fetched.get("entries"), list) else [],
            "content_blocks": fetched.get("content_blocks") if isinstance(fetched.get("content_blocks"), list) else [],
            "details": fetched.get("details") if isinstance(fetched.get("details"), list) else [],
            "content_depth": _content_depth(fetched),
            "body_excerpt": fetched.get("body_excerpt", ""),
            "original_evidence": original_evidence,
            "translation_status": record["translation_status"],
            "locator": {"kind": "body_excerpt", "value": "first_1200_chars"},
        }
        atomic_json(snapshot_path, snapshot)
        record.update({"evidence_id": evidence_id, "source_snapshot_uri": str(snapshot_path), "quote_hash": sha256_text(str(fetched.get("body_excerpt") or "")), "locator": snapshot["locator"]})
    return record


def ingest(*, state_root: Path = DEFAULT_STATE_ROOT, project_root: Path = DEFAULT_PROJECT_ROOT, run_id: str | None = None) -> dict[str, Any]:
    state_root.mkdir(parents=True, exist_ok=True)
    # Resolve the Worker-owned id after static command validation so artifacts
    # remain traceable without putting an empty environment value in argv.
    run_id = run_id or os.environ.get("PM_SCHEDULE_RUN_ID") or "ingest-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    configured = load_registry(project_root)
    sources = [item for item in configured["sources"] if isinstance(item, dict)]
    translations = _load_translation_overrides(project_root)
    rows = [_source_record(item, run_id=run_id, state_root=state_root, translations=translations) for item in sources]
    inbox = _read_jsonl(state_root / "inbox.jsonl")
    inbox_rows = [item for item in inbox if item.get("status") in {"accepted", "deduplicated"}]
    for item in inbox_rows:
        rows.append(_source_record({"source_id": item.get("inbox_id"), "source": "PM 快速投递", "url": item.get("url"), "vendor": "待归一化", "product": item.get("focus") or "待识别", "route": "观察项", "trust": "community", "fetch_mode": "manual"}, run_id=run_id, state_root=state_root, translations=translations))
    ledger_path = state_root / "signal-ledger.jsonl"
    existing_signal_ids = {str(item.get("signal_id")) for item in _read_jsonl(ledger_path) if item.get("signal_id")}
    for row in rows:
        signal_id = str(row.get("signal_id") or "")
        if signal_id and signal_id in existing_signal_ids:
            continue
        _append_jsonl(ledger_path, row)
        if signal_id:
            existing_signal_ids.add(signal_id)
    # Keep a compact per-source high-water mark for operators and replay tools.
    # The ledger remains the durable signal history; this file only records the
    # latest successfully observed content hash for each source.
    watermarks = {
        str(row.get("source_id")): {
            "content_hash": row.get("content_hash"),
            "signal_id": row.get("signal_id"),
            "captured_at": row.get("captured_at"),
        }
        for row in rows
        if row.get("source_id") and row.get("retrieval_status") == "ok" and row.get("content_hash")
    }
    if watermarks:
        existing_watermarks = {}
        watermark_path = state_root / "source-watermarks.json"
        if watermark_path.is_file():
            try:
                existing_watermarks = json.loads(watermark_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing_watermarks = {}
        if not isinstance(existing_watermarks, dict):
            existing_watermarks = {}
        existing_watermarks.update(watermarks)
        atomic_json(watermark_path, existing_watermarks)
    result = {"schema_version": "competitive-radar.ingest.v1", "run_id": run_id, "captured_at": now_iso(), "registry_hash": sha256_text(json.dumps(configured, ensure_ascii=False, sort_keys=True)), "source_count": len(rows), "ok_count": sum(1 for row in rows if row.get("retrieval_status") == "ok"), "stale_count": sum(1 for row in rows if row.get("freshness") == "stale"), "unavailable_count": sum(1 for row in rows if row.get("retrieval_status") not in {"ok"}), "signals": rows}
    atomic_json(state_root / "ingest" / f"{run_id}.json", result)
    atomic_json(state_root / "latest-ingest.json", result)
    return {"ingest": str((state_root / "ingest" / f"{run_id}.json").resolve()), "snapshot": str((state_root / "latest-ingest.json").resolve()), "status_file": str((state_root / "latest-ingest.json").resolve()), **result}


def reannotate(*, state_root: Path = DEFAULT_STATE_ROOT, project_root: Path = DEFAULT_PROJECT_ROOT, run_id: str) -> dict[str, Any]:
    """Refresh translated, curated evidence for an existing immutable fetch run.

    This does not fetch a page or alter raw source text.  It is for the
    bounded case where a PM adds a reviewed translation override after a run;
    the same captured page fields are reselected and its evidence snapshot is
    updated before a report is assembled.
    """
    if not run_id:
        raise ValueError("reannotate requires an ingest run id")
    ingest_path = state_root / "ingest" / f"{run_id}.json"
    if not ingest_path.is_file():
        raise FileNotFoundError(f"ingest run not found: {run_id}")
    value = json.loads(ingest_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("signals"), list):
        raise ValueError("ingest run has invalid signals")
    configured = load_registry(project_root)
    sources = {
        str(item.get("source_id")): item
        for item in configured.get("sources", [])
        if isinstance(item, Mapping) and item.get("source_id")
    }
    translations = _load_translation_overrides(project_root)
    updated = 0
    for row in value["signals"]:
        if not isinstance(row, dict) or row.get("retrieval_status") != "ok":
            continue
        source = sources.get(str(row.get("source_id") or ""), {})
        evidence = _source_evidence_records({**source, **row}, translations=translations, limit=1)
        row["original_evidence"] = evidence
        row["content_depth"] = _content_depth({**source, **row, "original_evidence": evidence})
        row["translation_status"] = "translated" if evidence and all(item.get("translation_status") in {"translated", "not_needed"} for item in evidence) else "missing"
        snapshot_path = Path(str(row.get("source_snapshot_uri") or ""))
        if snapshot_path.is_file():
            try:
                snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                snapshot = {}
            if isinstance(snapshot, dict):
                snapshot.update({"original_evidence": evidence, "content_depth": row["content_depth"], "translation_status": row["translation_status"]})
                atomic_json(snapshot_path, snapshot)
        updated += 1
    value["reannotated_at"] = now_iso()
    value["reannotation_count"] = updated
    atomic_json(ingest_path, value)
    atomic_json(state_root / "latest-ingest.json", value)
    return {"run_id": run_id, "updated": updated, "ingest": str(ingest_path.resolve()), "snapshot": str((state_root / "latest-ingest.json").resolve())}


def _review(markdown: str, signals: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(signals)
    p0_p1: list[dict[str, Any]] = []
    shallow_sources: list[str] = []
    for row in rows:
        if row.get("retrieval_status") != "ok":
            continue
        missing = [key for key in ("evidence_id", "source_snapshot_uri", "content_hash", "locator") if not row.get(key)]
        evidence = row.get("original_evidence")
        if not isinstance(evidence, list) or not evidence:
            missing.append("original_evidence")
        else:
            for index, item in enumerate(evidence):
                if not isinstance(item, Mapping) or not str(item.get("original") or "").strip():
                    missing.append(f"original_evidence[{index}].original")
                if not isinstance(item, Mapping) or item.get("translation_status") not in {"translated", "not_needed"} or not str(item.get("translation_zh") or "").strip():
                    missing.append(f"original_evidence[{index}].translation_zh")
        if missing:
            p1_p = {"severity": "P1", "signal_id": row.get("signal_id"), "reason": "重要信号缺少原文或中文翻译证据", "missing": missing}
            p0_p1.append(p1_p)
        if _content_depth(row) == "metadata":
            shallow_sources.append(str(row.get("source") or row.get("source_id") or "未命名来源"))
    manual = [{"severity": "P2", "item": "来源保留周期、脱敏和采集授权仍需人工确认"}, {"severity": "P2", "item": "YouTube 深度字幕预算与频道范围仍需人工确认"}]
    if shallow_sources:
        manual.append({"severity": "P2", "item": f"以下来源仅有标题或分类描述，报告已标为索引级缺口，不能作为能力结论：{'、'.join(shallow_sources)}"})
    status = "PASS" if not p0_p1 else "BLOCKED"
    return {"schema_version": "competitive-radar.review.v3", "review_run_id": "review:" + sha256_text(markdown)[:20], "reviewed_at": now_iso(), "status": status, "p0_p1": p0_p1, "manual_decision_items": manual, "shallow_sources": shallow_sources, "reviewer": "competitive_radar_reviewer", "evidence_policy": "P0/P1 requires evidence_id+snapshot+content_hash+locator+original_evidence[].original+translation_zh; title-only sources are P2 index gaps"}


def review_draft(markdown_path: Path, signals_path: Path, output_path: Path) -> dict[str, Any]:
    """Run reviewer logic in an independent process and persist its ledger."""
    markdown = markdown_path.read_text(encoding="utf-8")
    raw = json.loads(signals_path.read_text(encoding="utf-8")) if signals_path.is_file() else []
    value = _review(markdown, raw if isinstance(raw, list) else [])
    atomic_json(output_path, value)
    return value


def _report_html(
    markdown: str = "",
    *,
    run_id: str = "",
    captured_at: str = "",
    signals: Iterable[Mapping[str, Any]] = (),
    valid: Iterable[Mapping[str, Any]] = (),
    report_status: str = "degraded",
    gate_status: str = "unknown",
    review: Mapping[str, Any] | None = None,
) -> str:
    """Render a readable, self-contained report document."""
    all_signals = [dict(row) for row in signals]
    valid_signals = [dict(row) for row in valid]
    review = dict(review or {})
    stale = [row for row in all_signals if row.get("retrieval_status") != "ok"]
    status_label = "已发布" if report_status == "reviewed" and gate_status == "PASS" else (
        "已发布 · 含数据缺口" if report_status == "reviewed" else "未发布 · 数据不足"
    )
    status_class = "ok" if report_status == "reviewed" and gate_status == "PASS" else "warn"
    route_counts = {route: sum(1 for row in valid_signals if row.get("route") == route) for route in ("A", "B", "C")}
    deep_valid = sum(1 for row in valid_signals if _content_depth(row) != "metadata")
    report_title = "Agent 竞品雷达"
    generated = html.escape(captured_at or "未记录")
    warning = "本期有来源不可用，结论仅覆盖已成功读取的来源。" if stale else "本期来源均成功读取。"
    if deep_valid < len(valid_signals):
        warning += f" 其中 {len(valid_signals) - deep_valid} 个来源只有索引级内容，不能替代详情正文。"
    coverage = f"{(len(valid_signals) / len(all_signals) * 100):.0f}%" if all_signals else "0%"
    content_coverage = f"{(deep_valid / len(valid_signals) * 100):.0f}%" if valid_signals else "0%"
    summaries = _report_summaries(all_signals, valid_signals)

    def esc(value: Any) -> str:
        return html.escape(str(value or ""), quote=True)

    route_items = "".join(
        f'<div class="route-item"><span class="route-key">路线 {route}</span><strong>{route_counts[route]}</strong><span class="muted">条可复核信号</span></div>'
        for route in ("A", "B", "C")
    )
    capture_rows = []
    for row in valid_signals:
        source_url = esc(row.get("source_url"))
        source_name = esc(row.get("source") or row.get("source_id"))
        source_link = f'<a href="{source_url}" target="_blank" rel="noreferrer">{source_name}</a>' if source_url else source_name
        method = "浏览器公开 DOM 详情补强" if row.get("retrieval_method") == "browser_dom_enrichment" else "浏览器公开 DOM 兜底" if row.get("retrieval_method") == "browser_dom_fallback" else "HTTP/API/RSS"
        records = row.get("original_evidence") if isinstance(row.get("original_evidence"), list) else _source_evidence_records(row, limit=6)
        concrete_parts = []
        for item in records[:6]:
            if not isinstance(item, Mapping):
                continue
            original = esc(item.get("original") or "")
            translation = esc(item.get("translation_zh") or "待补充中文翻译")
            kind = esc(item.get("kind") or "evidence")
            item_link = str(item.get("link") or "").strip()
            link_html = f' <a href="{esc(item_link)}" target="_blank" rel="noreferrer">详情页</a>' if item_link else ""
            concrete_parts.append(f'<li class="evidence-record"><span class="source-kind">{kind}{link_html}</span><div class="original"><strong>原文：</strong>{original}</div><div class="translation"><strong>中文：</strong>{translation}</div></li>')
        concrete = "".join(concrete_parts) or '<li class="empty">已读取页面，但未提取到标题、摘要或条目。</li>'
        capture_rows.append(
            f'<tr><td><strong>{source_link}</strong><br><span class="muted">{esc(row.get("vendor") or "未知厂商")} / {esc(row.get("product") or "未知产品")}</span></td>'
            f'<td>{method}<br><span class="muted">{esc(row.get("captured_at") or "未记录")} · 内容深度 {_content_depth(row)}</span></td>'
            f'<td><ul class="capture-list">{concrete}</ul></td>'
            f'<td><span class="evidence">{esc(row.get("evidence_id") or "未记录")}</span></td></tr>'
        )
    capture_table = "".join(capture_rows) or '<tr><td colspan="4" class="empty">本期没有可复核抓取内容。</td></tr>'
    signal_items = []
    for row in valid_signals[:6]:
        source_url = esc(row.get("source_url"))
        link = f'<a href="{source_url}" target="_blank" rel="noreferrer">打开来源</a>' if source_url else ""
        records = row.get("original_evidence") if isinstance(row.get("original_evidence"), list) else _source_evidence_records(row, limit=4)
        evidence_blocks = []
        for item in records[:4]:
            if not isinstance(item, Mapping):
                continue
            item_link = str(item.get("link") or "").strip()
            link_html = f' <a href="{esc(item_link)}" target="_blank" rel="noreferrer">详情页</a>' if item_link else ""
            evidence_blocks.append(
                f'<div class="evidence-record"><div class="source-kind">{esc(item.get("kind") or "evidence")}{link_html}</div><div class="original"><strong>原文：</strong>{esc(item.get("original") or "")}</div><div class="translation"><strong>中文：</strong>{esc(item.get("translation_zh") or "待补充中文翻译")}</div></div>'
            )
        evidence_html = "".join(evidence_blocks) or '<p class="empty">没有可展示的原文证据。</p>'
        signal_items.append(
            "<article class=\"signal\">"
            f"<div class=\"signal-top\"><span class=\"tag\">路线 {esc(row.get('route') or '观察项')}</span><span class=\"muted\">{esc(row.get('capability_layer') or '能力信号')}</span></div>"
            f"<h3>{esc(row.get('vendor') or '未知厂商')} <span>/</span> {esc(row.get('product') or '未知产品')}</h3>"
            f"<p class=\"signal-summary\">{esc(_source_summary(row))}</p>"
            f"<div class=\"signal-evidence\">{evidence_html}</div>"
            f"<p class=\"signal-reference\"><strong>对胜算：</strong>{esc(ROUTE_REFERENCE.get(str(row.get('route') or ''), '持续观察产品能力与交付价值'))}</p>"
            f"<div class=\"signal-foot\"><span class=\"evidence\">证据 {esc(row.get('evidence_id') or '未记录')}</span>{link}</div>"
            "</article>"
        )
    if not signal_items:
        signal_items.append('<p class="empty">没有通过读取门禁的信号；这不代表行业没有变化。</p>')

    source_rows = []
    for row in all_signals:
        ok = row.get("retrieval_status") == "ok"
        state = "成功" if ok else "不可用"
        state_class = "ok" if ok else "warn"
        url = esc(row.get("source_url"))
        source_link = f'<a href="{url}" target="_blank" rel="noreferrer">{esc(row.get("source") or row.get("source_id"))}</a>' if url else esc(row.get("source") or row.get("source_id"))
        if ok:
            method = str(row.get("retrieval_method") or "")
            detail = "浏览器公开页面详情补强，已保存证据快照" if method == "browser_dom_enrichment" else "浏览器公开页面兜底，已保存证据快照" if method == "browser_dom_fallback" else "已保存证据快照"
            detail += f"；内容深度：{_content_depth(row)}"
        else:
            detail = esc(row.get("failure_reason") or "未记录原因")
            if row.get("fallback_attempted"):
                detail += f'；浏览器兜底失败：{esc(row.get("fallback_failure_reason") or "未记录原因")}'
        source_rows.append(f'<tr><td>{source_link}</td><td><span class="status {state_class}">{state}</span></td><td>{detail}</td></tr>')
    source_table = "".join(source_rows) or '<tr><td colspan="3" class="empty">暂无来源记录</td></tr>'
    review_status = esc(review.get("status") or gate_status)
    markdown_block = f'<details><summary>查看 Markdown 原文</summary><pre>{html.escape(markdown)}</pre></details>' if markdown else ""
    route_detail_items = "".join(
        f'<article class="route-detail"><div class="route-detail-top"><span class="tag">路线 {route} · {esc(ROUTE_LABELS[route])}</span><strong>{route_counts[route]} 条</strong></div><p>{esc(summaries["route_details"][route]["summary"])}</p><p class="signal-reference"><strong>对胜算：</strong>{esc(summaries["route_details"][route]["reference"])}</p></article>'
        for route in ("A", "B", "C")
    )
    action_items = "".join(f'<article class="signal action"><span class="tag">{esc(kind)}</span><h3>{esc(text)}</h3></article>' for kind, text in summaries["actions"])
    return f'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{report_title}</title>
  <style>
    :root {{ color-scheme: light; --ink:#18242e; --muted:#667680; --line:#d8e1e5; --canvas:#f4f7f8; --paper:#fff; --accent:#0b6d8a; --accent-soft:#e6f2f6; --ok:#197348; --ok-soft:#e5f4ec; --warn:#a25d09; --warn-soft:#fff2df; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--canvas); color:var(--ink); font:15px/1.65 -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; }}
    .report-shell {{ max-width:1080px; margin:0 auto; padding:42px 28px 64px; }}
    .report-header {{ display:flex; justify-content:space-between; gap:28px; align-items:flex-start; padding-bottom:28px; border-bottom:1px solid var(--line); }}
    .eyebrow {{ margin:0 0 8px; color:var(--accent); font-size:12px; font-weight:700; letter-spacing:.12em; }}
    h1 {{ margin:0; font-size:clamp(30px,5vw,46px); line-height:1.12; letter-spacing:0; }}
    .lede {{ max-width:660px; margin:15px 0 0; color:var(--muted); font-size:17px; }}
    .status {{ display:inline-flex; align-items:center; white-space:nowrap; padding:4px 9px; border:1px solid currentColor; border-radius:999px; font-size:12px; font-weight:700; line-height:1.2; }}
    .status.ok {{ color:var(--ok); background:var(--ok-soft); }} .status.warn {{ color:var(--warn); background:var(--warn-soft); }}
    .meta {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; min-width:260px; margin-top:4px; }}
    .meta-item {{ padding-left:14px; border-left:3px solid var(--accent); }} .meta-label {{ display:block; color:var(--muted); font-size:12px; }} .meta-value {{ display:block; margin-top:2px; font-weight:650; overflow-wrap:anywhere; }}
    .metrics {{ display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin:24px 0 40px; }}
    .metric {{ padding:18px 20px; background:var(--paper); border:1px solid var(--line); border-radius:8px; }} .metric strong {{ display:block; color:var(--accent); font-size:30px; line-height:1.1; }} .metric span {{ color:var(--muted); font-size:13px; }}
    .section {{ margin-top:38px; }} .section-heading {{ display:flex; align-items:baseline; justify-content:space-between; gap:16px; padding-top:13px; border-top:2px solid var(--ink); }} h2 {{ margin:0; font-size:22px; }} .muted {{ color:var(--muted); font-size:13px; }}
    .notice {{ margin-top:16px; padding:13px 16px; border-left:4px solid var(--warn); background:var(--warn-soft); color:#70440c; }}
    .route-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-top:16px; }} .route-item {{ display:flex; flex-wrap:wrap; align-items:baseline; gap:8px; padding:16px 18px; background:var(--accent-soft); border:1px solid #c8e1e9; border-radius:8px; }} .route-key {{ width:100%; color:var(--accent); font-weight:700; }} .route-item strong {{ font-size:28px; }}
    .signal-list {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; margin-top:16px; }} .signal {{ padding:18px 20px; background:var(--paper); border:1px solid var(--line); border-radius:8px; }} .signal-top,.signal-foot,.route-detail-top {{ display:flex; justify-content:space-between; gap:12px; align-items:center; }} .tag {{ color:var(--accent); font-size:12px; font-weight:700; }} h3 {{ margin:13px 0 8px; font-size:17px; }} h3 span {{ color:var(--muted); font-weight:400; }} .signal p,.route-detail p {{ margin:0; color:#33434d; overflow-wrap:anywhere; }} .signal-reference {{ margin-top:12px !important; padding-top:10px; border-top:1px solid var(--line); color:#425963 !important; font-size:13px; }} .signal-foot {{ margin-top:16px; padding-top:12px; border-top:1px solid var(--line); }} .evidence {{ color:var(--muted); font:12px ui-monospace,SFMono-Regular,Menlo,monospace; overflow-wrap:anywhere; }} .signal-evidence {{ margin-top:14px; }} .evidence-record {{ margin-top:10px; padding:10px 12px; background:#f8fafb; border-left:3px solid var(--accent); overflow-wrap:anywhere; }} .evidence-record:first-child {{ margin-top:0; }} .original {{ color:#26343c; }} .translation {{ margin-top:6px; color:#234f5e; }} .source-kind {{ color:var(--muted); font-size:11px; text-transform:uppercase; }} a {{ color:var(--accent); text-underline-offset:3px; }}
    .summary-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; margin-top:16px; }} .summary-card {{ padding:20px; background:var(--paper); border:1px solid var(--line); border-radius:8px; }} .summary-card h3 {{ margin-top:0; }} .summary-card p {{ margin:0; color:#33434d; overflow-wrap:anywhere; }} .summary-card.reference {{ background:var(--accent-soft); border-color:#c8e1e9; }} .route-details {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:14px; margin-top:16px; }} .route-detail {{ padding:16px 18px; background:var(--paper); border:1px solid var(--line); border-radius:8px; }} .route-detail-top strong {{ color:var(--accent); font-size:21px; }} .route-detail p {{ margin-top:12px; font-size:14px; }}
    table {{ width:100%; margin-top:16px; border-collapse:collapse; background:var(--paper); border:1px solid var(--line); }} th,td {{ padding:12px 14px; text-align:left; vertical-align:top; border-bottom:1px solid var(--line); }} th {{ color:var(--muted); background:#f8fafb; font-size:12px; font-weight:700; }} tr:last-child td {{ border-bottom:0; }} .capture-list {{ margin:0; padding-left:20px; }} .capture-list li {{ margin:4px 0; }}
    .empty {{ color:var(--muted); }} details {{ margin-top:28px; }} summary {{ cursor:pointer; color:var(--accent); font-weight:650; }} pre {{ margin-top:12px; padding:16px; overflow:auto; background:#eef3f5; border:1px solid var(--line); border-radius:8px; white-space:pre-wrap; }}
    @media (max-width:900px) {{ .metrics {{ grid-template-columns:repeat(2,1fr); }} }}
    @media (max-width:720px) {{ .report-shell {{ padding:28px 16px 48px; }} .report-header {{ display:block; }} .meta {{ margin-top:24px; }} .metrics,.route-grid,.signal-list,.summary-grid,.route-details {{ grid-template-columns:1fr; }} table {{ display:block; overflow-x:auto; white-space:nowrap; }} }}
    @media print {{ body {{ background:#fff; }} .report-shell {{ max-width:none; padding:0; }} .metric,.signal,table {{ break-inside:avoid; }} a {{ color:inherit; }} }}
  </style>
</head>
<body>
  <main class="report-shell">
    <header class="report-header">
      <div><p class="eyebrow">PM INTELLIGENCE / WEEKLY BRIEF</p><h1>{report_title}</h1><p class="lede">按 A/B/C 三条路线观察 Agent 行业变化，以 Palantir 作为企业 Agent 平台锚点，输出对胜算的跟随、借鉴与暂缓动作。</p></div>
      <div class="meta"><div class="meta-item"><span class="meta-label">报告状态</span><span class="meta-value"><span class="status {status_class}">{status_label}</span></span></div><div class="meta-item"><span class="meta-label">审查</span><span class="meta-value">{review_status}</span></div><div class="meta-item"><span class="meta-label">Run</span><span class="meta-value">{esc(run_id) or "未记录"}</span></div><div class="meta-item"><span class="meta-label">采集时间</span><span class="meta-value">{generated}</span></div></div>
    </header>
    <section class="metrics" aria-label="报告摘要"><div class="metric"><strong>{len(valid_signals)}</strong><span>可复核信号</span></div><div class="metric"><strong>{len(all_signals)}</strong><span>来源总数</span></div><div class="metric"><strong>{len(stale)}</strong><span>不可用来源</span></div><div class="metric"><strong>{coverage}</strong><span>证据覆盖率</span></div><div class="metric"><strong>{content_coverage}</strong><span>正文/摘要覆盖率</span></div></section>
    <section class="section"><div class="section-heading"><h2>结论先行</h2><span class="muted">{esc(warning)}</span></div><div class="summary-grid"><article class="summary-card"><h3>本期竞品总结 / 本期结论</h3><p>{esc(summaries["industry"])}</p></article><article class="summary-card reference"><h3>对胜算的参考 / 建议</h3><p>{esc(summaries["shengsuan"])}</p></article></div><div class="notice">{esc(warning)} 缺失来源不会被解释为“行业无变化”。</div></section>
    <section class="section"><div class="section-heading"><h2>Situation：本期事实</h2><span class="muted">页面实际读取的原文与中文翻译</span></div><table><thead><tr><th>来源 / 对象</th><th>获取方式 / 时间</th><th>具体标题、摘要或条目</th><th>证据</th></tr></thead><tbody>{capture_table}</tbody></table></section>
    <section class="section"><div class="section-heading"><h2>Complication：本期变化</h2><span class="muted">按路线汇总，事实与判断分层</span></div><div class="route-grid">{route_items}</div><div class="route-details">{route_detail_items}</div></section>
    <section class="section"><div class="section-heading"><h2>Resolution：对胜算的动作</h2><span class="muted">由本期有效信号生成</span></div><div class="signal-list">{action_items}</div></section>
    <section class="section"><div class="section-heading"><h2>重点信号：原文与中文翻译</h2><span class="muted">最多展示 6 条，每条可回读</span></div><div class="signal-list">{"".join(signal_items)}</div></section>
    <section class="section"><div class="section-heading"><h2>来源状态</h2><span class="muted">成功 {len(valid_signals)} / 总计 {len(all_signals)}</span></div><table><thead><tr><th>来源</th><th>状态</th><th>说明</th></tr></thead><tbody>{source_table}</tbody></table></section>
    {markdown_block}
  </main>
</body>
</html>'''


def brief(*, state_root: Path = DEFAULT_STATE_ROOT, project_root: Path = DEFAULT_PROJECT_ROOT, run_id: str | None = None) -> dict[str, Any]:
    state_root.mkdir(parents=True, exist_ok=True)
    run_id = run_id or os.environ.get("PM_SCHEDULE_RUN_ID") or "brief-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    latest = json.loads((state_root / "latest-ingest.json").read_text(encoding="utf-8")) if (state_root / "latest-ingest.json").is_file() else {"signals": [], "captured_at": None, "registry_hash": None}
    signals = [row for row in latest.get("signals", []) if isinstance(row, dict)]
    retrieved = [row for row in signals if row.get("retrieval_status") == "ok"]
    valid = [row for row in retrieved if _has_content_evidence(row)]
    route_counts = {route: sum(1 for row in valid if row.get("route") == route) for route in ("A", "B", "C")}
    summaries = _report_summaries(signals, valid)
    highlights = valid[:6]
    lines = ["# Agent 竞品雷达", "", f"- 报告 Run：`{run_id}`", f"- 采集时间：`{latest.get('captured_at') or '未记录'}`", f"- 来源状态：读取成功 {len(retrieved)} / 总计 {len(signals)}；其中正文/摘要级有效信号 {len(valid)} 条", "", "## 结论先行", "", "### 本期结论", summaries["industry"], "", "### 对胜算的建议", summaries["shengsuan"], "", "## Situation：本期事实（本期实际抓取内容）", "", "以下内容是从来源页面实际读取的原文与中文翻译；它们是证据事实，不等同于产品判断。仅正文、卡片摘要、Atom 摘要或带热度上下文的社区信号会进入本节。", ""]
    if valid:
        for row in valid:
            method = "浏览器公开 DOM 详情补强" if row.get("retrieval_method") == "browser_dom_enrichment" else "浏览器公开 DOM 兜底" if row.get("retrieval_method") == "browser_dom_fallback" else "HTTP/API/RSS"
            lines.extend([f"### {row.get('source') or row.get('source_id')} / {row.get('vendor') or '未知厂商'}「{row.get('product') or '未知产品'}」", "", f"- 获取方式：{method}；时间：`{row.get('captured_at') or '未记录'}`；内容深度：`{_content_depth(row)}`", "- 具体内容："])
            records = row.get("original_evidence") if isinstance(row.get("original_evidence"), list) else _source_evidence_records(row, limit=6)
            if records:
                for item in records[:6]:
                    detail_link = f"\n    - 详情：{item.get('link')}" if item.get("link") else ""
                    lines.extend([f"  - 原文（{item.get('kind') or 'evidence'}）：{item.get('original') or ''}", f"    - 中文：{item.get('translation_zh') or '待补充中文翻译'}" + detail_link])
            else:
                lines.append("  - 已读取页面，但未提取到标题、摘要或条目。")
            lines.extend([f"- 证据：`{row.get('evidence_id') or '未记录'}`；快照：`{row.get('source_snapshot_uri') or '未记录'}`", ""])
    else:
        lines.append("本期没有可复核抓取内容。")
    lines.extend(["## Complication：本期变化", "", "按路线汇总；缺失来源和页面可用性不会被解释为产品能力变化。", ""])
    for route in ("A", "B", "C"):
        lines.extend([f"- 路线 {route}（{ROUTE_LABELS[route]}）：{route_counts[route]} 条可复核信号。", f"  - 竞品观察：{summaries['route_details'][route]['summary']}", f"  - 胜算参考：{summaries['route_details'][route]['reference']}"])
    lines.extend(["", "## Resolution：对胜算的动作", ""])
    lines.extend(f"- {kind}：{text}" for kind, text in summaries["actions"])
    lines.extend(["", "## 重点信号：原文与中文翻译", ""])
    if not highlights:
        lines.append("- 本期没有通过读取门禁的信号；请查看来源状态，不将其解释为行业无变化。")
    for row in highlights:
        evidence = row.get("evidence_id") or "无证据"
        lines.extend([f"### {row.get('vendor') or '未知厂商'} / {row.get('product') or '未知产品'}（路线 {row.get('route') or '观察项'}）", "", f"- 判断：{_source_summary(row)}", f"- 对胜算：{ROUTE_REFERENCE.get(str(row.get('route') or ''), '持续观察产品能力与交付价值')}", f"- 证据：`{evidence}`"])
        records = row.get("original_evidence") if isinstance(row.get("original_evidence"), list) else _source_evidence_records(row, limit=4)
        for item in records[:4]:
            lines.extend([f"- 原文：{item.get('original') or ''}", f"  - 中文：{item.get('translation_zh') or '待补充中文翻译'}"])
        lines.append("")
    palantir = [row for row in valid if str(row.get("vendor") or "").casefold() == "palantir"]
    metadata_sources = [row for row in retrieved if not _has_content_evidence(row)]
    lines.extend(["## Palantir 对照", f"- 本期可复核 Palantir 信号：{len(palantir)} 条；若为 0，标记为 unknown，不替代既有 product-intelligence-monitor 周度 delta。", "", "## 来源状态", f"- unavailable：{sum(1 for row in signals if row.get('retrieval_status') != 'ok')} 条；读取成功但仅标题/Meta、未进入结论：{len(metadata_sources)} 条。", ""])
    markdown = "\n".join(lines)
    report_dir = project_root / "docs" / "产品情报监控" / "竞品雷达" / "周报"
    week_key = datetime.now(timezone.utc).astimezone(timezone.utc).strftime("%G-W%V")
    md_path = report_dir / f"{week_key}-Agent竞品雷达.md"
    html_path = report_dir / f"{week_key}-Agent竞品雷达.html"
    draft_dir = report_dir / "drafts"
    draft_md_path = draft_dir / f"{run_id}-Agent竞品雷达.md"
    draft_html_path = draft_dir / f"{run_id}-Agent竞品雷达.html"
    # Never overwrite the current report before the independent reviewer has
    # passed the new draft.  The read model points at the stable weekly path.
    atomic_write(draft_md_path, markdown)
    signals_path = state_root / "brief" / f"{run_id}.signals.json"
    atomic_json(signals_path, valid)
    review_path = state_root / "reviews" / f"{run_id}.json"
    reviewer = subprocess.run([sys.executable, str(Path(__file__).resolve()), "review", str(draft_md_path), "--signals", str(signals_path), "--output", str(review_path)], capture_output=True, text=True, timeout=20, check=False)
    if reviewer.returncode != 0 or not review_path.is_file():
        review = {"schema_version": "competitive-radar.review.v1", "review_run_id": "review:" + sha256_text(markdown)[:20], "reviewed_at": now_iso(), "status": "BLOCKED", "p0_p1": [{"severity": "P1", "reason": "independent reviewer failed"}], "manual_decision_items": [], "reviewer": "competitive_radar_reviewer", "reviewer_error": (reviewer.stderr or reviewer.stdout or "reviewer failed")[-500:]}
        atomic_json(review_path, review)
    else:
        review = json.loads(review_path.read_text(encoding="utf-8"))
    # The independent reviewer is the source of truth for publication status.
    # A partial collection can publish with an explicit warning; an empty
    # collection stays degraded so it cannot replace a useful latest pointer.
    report_status = "reviewed" if review.get("status") == "PASS" and bool(valid) else "degraded"
    content_coverage = len(valid) / len(retrieved) if retrieved else 0.0
    gate_status = "PASS" if report_status == "reviewed" and len(valid) == len(signals) else "PASS_WITH_WARN" if report_status == "reviewed" else str(review.get("status") or "BLOCKED")
    rendered_html = _report_html(markdown, run_id=run_id, captured_at=str(latest.get("captured_at") or ""), signals=signals, valid=valid, report_status=report_status, gate_status=gate_status, review=review)
    atomic_write(draft_html_path, rendered_html)
    report_hash = sha256_text(markdown)
    pointer_path = state_root / "latest.json"
    pointer_updated = False
    if report_status == "reviewed":
        atomic_write(md_path, markdown)
        atomic_write(html_path, rendered_html)
        pointer = {"schema_version": "competitive-radar.latest.v2", "report_status": report_status, "run_id": run_id, "report_uri": str(md_path.resolve()), "html_uri": str(html_path.resolve()), "report_hash": report_hash, "review_run_id": review["review_run_id"], "published_at": now_iso(), "gate_status": gate_status, "evidence_coverage": len(valid) / len(signals) if signals else 0.0, "content_coverage": content_coverage}
        atomic_json(pointer_path, pointer)
        pointer_updated = True
    result = {"schema_version": "competitive-radar.brief.v2", "run_id": run_id, "status": report_status, "gate_status": gate_status, "markdown": str((md_path if pointer_updated else draft_md_path).resolve()), "html": str((html_path if pointer_updated else draft_html_path).resolve()), "draft_markdown": str(draft_md_path.resolve()), "draft_html": str(draft_html_path.resolve()), "review": str(review_path.resolve()), "review_run_id": review["review_run_id"], "latest": str(pointer_path.resolve()) if pointer_path.is_file() else None, "report_hash": report_hash, "evidence_coverage": len(valid) / len(signals) if signals else 0.0, "content_coverage": content_coverage, "stale_sources": [row.get("source_id") for row in signals if row.get("retrieval_status") != "ok"], "metadata_sources": [row.get("source_id") for row in metadata_sources], "latest_updated": pointer_updated}
    atomic_json(state_root / "brief" / f"{run_id}.json", result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("fetch", "submit", "ingest", "reannotate", "brief", "review"))
    parser.add_argument("url", nargs="?")
    parser.add_argument("--focus", default="")
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--headers-json", default="{}")
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--signals", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.mode == "fetch":
        if not args.url:
            parser.error("fetch requires URL")
        try:
            headers = json.loads(args.headers_json)
        except json.JSONDecodeError:
            parser.error("--headers-json must be a JSON object")
        if not isinstance(headers, dict) or not all(isinstance(key, str) and isinstance(value, str) for key, value in headers.items()):
            parser.error("--headers-json must be a JSON object with string keys and values")
        value = _fetch(args.url, timeout=_bounded_int(args.timeout, 8, minimum=1, maximum=30), headers=headers)
    elif args.mode == "submit":
        if not args.url:
            parser.error("submit requires URL")
        value = submit(args.url, args.focus, state_root=args.state_root)
    elif args.mode == "ingest":
        value = ingest(state_root=args.state_root, project_root=args.project_root, run_id=args.run_id)
    elif args.mode == "reannotate":
        if not args.run_id:
            parser.error("reannotate requires --run-id")
        value = reannotate(state_root=args.state_root, project_root=args.project_root, run_id=args.run_id)
    elif args.mode == "brief":
        value = brief(state_root=args.state_root, project_root=args.project_root, run_id=args.run_id)
    else:
        if not args.url:
            parser.error("review requires markdown path")
        if args.signals and args.output:
            value = review_draft(Path(args.url), args.signals, args.output)
        else:
            markdown = Path(args.url).read_text(encoding="utf-8")
            value = _review(markdown, [])
    print(json.dumps(value, ensure_ascii=False))
    return 0 if value.get("status") not in {"rejected", "BLOCKED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
