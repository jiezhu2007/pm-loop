#!/usr/bin/env python3
"""Build the standalone, annotatable HTML version of the PM AI guide.

Workflow decision (2026-08-24): before generating a new HTML artifact, confirm
whether this run should include annotation capability instead of carrying over
the previous run's choice. This builder currently emits the annotatable guide.
"""

from __future__ import annotations

import hashlib
import html
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs/07-团队管理/PM的AI工作指南-新员工30天上手.md"
TARGET = ROOT / "docs/07-团队管理/PM的AI工作指南-新员工30天上手.html"

TOP_IDS = {
    "先做这三步": "quick-start",
    "这份指南怎么读": "reading-guide",
    "1. 为什么 PM 现在需要用 AI": "sec-why",
    "2. AI 能帮 PM 做什么": "sec-capabilities",
    "3. AI 工具怎么使用": "sec-usage",
    "4. 让 AI 输出可靠：信任，但必须验证": "sec-reliability",
    "5. 安全与边界": "sec-safety",
    "6. 把一次使用变成可复利资产": "sec-compounding",
    "7. 判断“是否真正用起来”：四标准飞轮": "sec-adoption",
    "8. 新员工 30 天上手路径": "sec-onboarding",
    "9. 一页速查卡": "sec-cheatsheet",
    "来源与说明": "sources",
}


def clean_visible(value: str) -> str:
    # The page style guide disallows em dashes; preserve meaning with a colon.
    return re.sub(r"—+", "：", value)


def inline(value: str) -> str:
    """Render the small Markdown inline subset used by this document."""

    value = clean_visible(value)
    escaped = html.escape(value, quote=False)
    tokens: dict[str, str] = {}

    def token(markup: str) -> str:
        key = f"\x00{len(tokens)}\x00"
        tokens[key] = markup
        return key

    escaped = re.sub(
        r"`([^`]+)`",
        lambda match: token(f"<code>{match.group(1)}</code>"),
        escaped,
    )
    escaped = re.sub(
        r"\[([^\]]+)\]\((https?://[^)]+)\)",
        lambda match: token(
            f'<a href="{html.escape(html.unescape(match.group(2)), quote=True)}" '
            'target="_blank" rel="noopener noreferrer">'
            f"{match.group(1)}</a>"
        ),
        escaped,
    )
    escaped = re.sub(
        r"\*\*([^*]+)\*\*",
        lambda match: token(f"<strong>{match.group(1)}</strong>"),
        escaped,
    )
    escaped = re.sub(
        r"\*([^*]+)\*",
        lambda match: token(f"<em>{match.group(1)}</em>"),
        escaped,
    )
    for key, markup in tokens.items():
        escaped = escaped.replace(key, markup)
    return escaped


def table_row(line: str) -> list[str]:
    value = line.strip()
    if value.startswith("|"):
        value = value[1:]
    if value.endswith("|"):
        value = value[:-1]
    return [cell.strip() for cell in value.split("|")]


def is_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in cells)


def make_id(text: str, level: int, seen: set[str]) -> str:
    if level == 2 and text in TOP_IDS:
        candidate = TOP_IDS[text]
    else:
        digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
        candidate = f"heading-{digest}"
    original = candidate
    suffix = 2
    while candidate in seen:
        candidate = f"{original}-{suffix}"
        suffix += 1
    seen.add(candidate)
    return candidate


def render_table(lines: list[str], start: int) -> tuple[str, int]:
    headers = table_row(lines[start])
    rows: list[list[str]] = []
    index = start + 2
    while index < len(lines) and lines[index].strip().startswith("|"):
        rows.append(table_row(lines[index]))
        index += 1

    caption = clean_visible("、".join(headers[:2]))
    output = [
        '<div class="table-wrap" tabindex="0" data-annotatable="table">',
        f'<table aria-label="{html.escape(caption, quote=True)}">',
        f'<caption class="sr-only">{html.escape(caption)}</caption>',
        "<thead><tr>",
    ]
    output.extend(f'<th scope="col">{inline(cell)}</th>' for cell in headers)
    output.append("</tr></thead><tbody>")
    for row in rows:
        output.append('<tr data-annotatable="row">')
        for cell_index in range(len(headers)):
            cell = row[cell_index] if cell_index < len(row) else ""
            tag = "th scope=\"row\"" if cell_index == 0 else "td"
            output.append(f"<{tag}>{inline(cell)}</{tag.split()[0]}>")
        output.append("</tr>")
    output.append("</tbody></table></div>")
    return "".join(output), index


def render(markdown: str) -> tuple[str, str]:
    lines = markdown.splitlines()
    if lines and lines[0].strip() == "---":
        end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), 0)
        if end:
            lines = lines[end + 1 :]

    output: list[str] = []
    headings: list[tuple[str, str]] = []
    seen_ids: set[str] = set()
    paragraph: list[str] = []
    list_items: list[str] = []
    list_type = ""
    section_open = False
    heading_index = 0

    def flush_paragraph() -> None:
        if paragraph:
            text = inline(" ".join(paragraph))
            output.append(f'<p data-annotatable="paragraph">{text}</p>')
            paragraph.clear()

    def flush_list() -> None:
        nonlocal list_type
        if list_items:
            output.append(f'<{list_type} data-annotatable="list">')
            output.extend(f'<li data-annotatable="item">{item}</li>' for item in list_items)
            output.append(f"</{list_type}>")
            list_items.clear()
            list_type = ""

    def close_section() -> None:
        nonlocal section_open
        if section_open:
            output.append("</section>")
            section_open = False

    index = 0
    while index < len(lines):
        raw = lines[index]
        stripped = raw.strip()
        if not stripped:
            flush_paragraph()
            flush_list()
            index += 1
            continue

        if stripped == "---":
            flush_paragraph()
            flush_list()
            index += 1
            continue

        fence = re.match(r"^```\s*([\w+-]*)\s*$", stripped)
        if fence:
            flush_paragraph()
            flush_list()
            language = fence.group(1) or "text"
            index += 1
            code_lines: list[str] = []
            while index < len(lines) and not lines[index].strip().startswith("```"):
                code_lines.append(clean_visible(lines[index]))
                index += 1
            if index < len(lines):
                index += 1
            code = html.escape("\n".join(code_lines), quote=False)
            output.append(
                f'<pre class="code-block" data-language="{html.escape(language, quote=True)}" '
                'data-annotatable="code"><code>'
                f"{code}</code></pre>"
            )
            continue

        heading = re.match(r"^(#{1,6})\s+(.+?)\s*$", stripped)
        if heading:
            flush_paragraph()
            flush_list()
            level = len(heading.group(1))
            text = clean_visible(heading.group(2))
            anchor = make_id(text, level, seen_ids)
            if level == 1:
                output.append(f'<h1 id="{anchor}" data-annotatable="heading">{inline(text)}</h1>')
            elif level == 2:
                close_section()
                section_open = True
                headings.append((text, anchor))
                output.append(f'<section class="guide-section" id="{anchor}"><h2 data-annotatable="heading">{inline(text)}</h2>')
            else:
                tag = f"h{min(level, 6)}"
                output.append(f'<{tag} id="{anchor}" data-annotatable="heading">{inline(text)}</{tag}>')
            heading_index += 1
            index += 1
            continue

        if stripped.startswith(">"):
            flush_paragraph()
            flush_list()
            quote_lines: list[str] = []
            while index < len(lines) and lines[index].strip().startswith(">"):
                quote_lines.append(lines[index].strip()[1:].strip())
                index += 1
            content = "".join(f"<p>{inline(line)}</p>" for line in quote_lines)
            output.append(f'<aside class="callout" role="note" data-annotatable="callout">{content}</aside>')
            continue

        ordered = re.match(r"^\d+\.\s+(.+)$", stripped)
        unordered = re.match(r"^[-*]\s+(.+)$", stripped)
        if ordered or unordered:
            flush_paragraph()
            desired = "ol" if ordered else "ul"
            if list_type and list_type != desired:
                flush_list()
            list_type = desired
            list_items.append(inline((ordered or unordered).group(1)))
            index += 1
            continue

        if stripped.startswith("|") and index + 1 < len(lines):
            header_cells = table_row(raw)
            separator_cells = table_row(lines[index + 1])
            if is_separator(separator_cells):
                flush_paragraph()
                flush_list()
                table_html, index = render_table(lines, index)
                output.append(table_html)
                continue

        paragraph.append(stripped)
        index += 1

    flush_paragraph()
    flush_list()
    close_section()
    nav = "".join(f'<a href="#{anchor}">{inline(text)}</a>' for text, anchor in headings)
    return "".join(output), nav


CSS = r"""
:root {
  color-scheme: light;
  --ink: #17201f;
  --ink-2: #3e4a47;
  --muted: #6b7672;
  --line: #dfe5e2;
  --line-strong: #b9c5c0;
  --paper: #ffffff;
  --canvas: #f1f4f3;
  --soft: #f7f9f8;
  --charcoal: #202827;
  --accent: #0f766e;
  --accent-dark: #09584f;
  --accent-soft: #e7f4f1;
  --yellow: rgba(255, 214, 10, .48);
  --green: rgba(52, 211, 153, .42);
  --blue: rgba(96, 165, 250, .42);
  --red: rgba(248, 113, 113, .42);
  --shadow: 0 18px 42px rgba(23, 32, 31, .16);
  --sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; scroll-padding-top: 76px; background: var(--canvas); }
body { margin: 0; color: var(--ink); background: var(--canvas); font: 15px/1.7 var(--sans); letter-spacing: 0; -webkit-font-smoothing: antialiased; overflow-x: hidden; }
button, input, select, textarea { font: inherit; letter-spacing: 0; }
button, a { touch-action: manipulation; }
button { cursor: pointer; }
a { color: var(--accent-dark); text-underline-offset: 3px; overflow-wrap: anywhere; }
a:focus-visible, button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible, [tabindex]:focus-visible { outline: 2px solid var(--accent); outline-offset: 3px; }
::selection { background: #bde9df; color: var(--ink); }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.skip-link { position: fixed; left: 14px; top: 10px; z-index: 100; transform: translateY(-160%); padding: 8px 11px; border-radius: 5px; background: var(--paper); color: var(--ink); box-shadow: var(--shadow); }
.skip-link:focus { transform: translateY(0); }

.topbar { position: sticky; top: 0; z-index: 30; min-height: 64px; border-bottom: 1px solid rgba(255,255,255,.14); background: rgba(32,40,39,.98); color: #fff; }
.topbar-inner { width: min(1260px, calc(100% - 40px)); min-height: 64px; margin: 0 auto; display: flex; align-items: center; gap: 22px; }
.brand { min-width: 260px; display: inline-flex; align-items: center; gap: 10px; color: #fff; text-decoration: none; }
.brand-mark { width: 30px; height: 30px; display: grid; place-items: center; border: 1px solid rgba(255,255,255,.34); border-radius: 6px; background: #fff; color: var(--charcoal); font-size: 14px; font-weight: 800; }
.brand strong { display: block; font-size: 14px; line-height: 1.2; }
.brand small { display: block; color: #b9c4c0; font-size: 11px; line-height: 1.3; }
.topnav { min-width: 0; display: flex; align-items: center; gap: 2px; overflow-x: auto; scrollbar-width: none; }
.topnav::-webkit-scrollbar { display: none; }
.topnav a { flex: 0 0 auto; padding: 8px 9px; border-radius: 5px; color: #cbd5d1; font-size: 12px; text-decoration: none; white-space: nowrap; }
.topnav a:hover, .topnav a:focus-visible { background: rgba(255,255,255,.09); color: #fff; }
.topbar-spacer { margin-left: auto; }
.annotation-toggle { min-height: 36px; display: inline-flex; align-items: center; gap: 7px; padding: 7px 11px; border: 1px solid rgba(255,255,255,.28); border-radius: 5px; background: rgba(255,255,255,.08); color: #fff; white-space: nowrap; }
.annotation-toggle:hover, .annotation-toggle[aria-expanded="true"] { border-color: rgba(255,255,255,.62); background: rgba(255,255,255,.17); }
.annotation-badge { min-width: 19px; height: 19px; display: inline-grid; place-items: center; padding: 0 5px; border-radius: 10px; background: #69d8be; color: var(--charcoal); font-size: 11px; font-weight: 750; }
.annotation-badge[hidden] { display: none; }

.page-shell { width: min(1260px, calc(100% - 40px)); margin: 0 auto; padding-bottom: 70px; }
.layout { display: grid; grid-template-columns: 222px minmax(0, 1fr); gap: 0 48px; align-items: start; }
.toc { position: sticky; top: 64px; max-height: calc(100dvh - 64px); overflow-y: auto; padding: 30px 20px 34px 0; border-right: 1px solid var(--line); }
.toc strong { display: block; margin-bottom: 11px; color: var(--ink); font-size: 13px; }
.toc a { display: block; margin-right: -1px; padding: 6px 11px 6px 0; border-right: 2px solid transparent; color: var(--muted); font-size: 12px; line-height: 1.45; text-decoration: none; }
.toc a:hover, .toc a:focus-visible, .toc a.is-active { color: var(--accent-dark); border-right-color: var(--accent); }
.toc-note { margin-top: 20px; padding-top: 14px; border-top: 1px solid var(--line); color: var(--muted); font-size: 11px; line-height: 1.55; }

#guide-content { position: relative; min-width: 0; padding: 42px 0 24px; }
#guide-content > h1 { max-width: 840px; margin: 0 0 12px; color: var(--ink); font-size: clamp(34px, 5vw, 58px); font-weight: 760; line-height: 1.08; text-wrap: balance; }
.guide-meta { display: flex; flex-wrap: wrap; gap: 7px 16px; margin-bottom: 24px; color: var(--muted); font: 12px/1.5 var(--mono); }
.guide-actions { display: flex; flex-wrap: wrap; gap: 9px; margin: 0 0 26px; }
.guide-action { min-height: 38px; display: inline-flex; align-items: center; gap: 7px; padding: 7px 12px; border: 1px solid var(--line-strong); border-radius: 5px; background: var(--paper); color: var(--ink); text-decoration: none; }
.guide-action:hover { border-color: var(--accent); color: var(--accent-dark); transform: translateY(-1px); }
.guide-action.primary { border-color: var(--charcoal); background: var(--charcoal); color: #fff; }
.guide-action.primary:hover { background: #101513; color: #fff; }
.guide-section { margin-top: 48px; }
.guide-section > h2 { margin: 0 0 17px; padding-top: 22px; border-top: 1px solid var(--line-strong); color: var(--ink); font-size: 27px; font-weight: 720; line-height: 1.25; scroll-margin-top: 82px; text-wrap: balance; }
#guide-content h3 { margin: 32px 0 10px; color: var(--accent-dark); font-size: 19px; font-weight: 700; line-height: 1.35; scroll-margin-top: 82px; text-wrap: balance; }
#guide-content h4 { margin: 25px 0 7px; color: var(--ink); font-size: 16px; font-weight: 700; line-height: 1.4; scroll-margin-top: 82px; }
#guide-content p { max-width: 88ch; margin: 11px 0; color: var(--ink-2); }
#guide-content strong { color: var(--ink); }
#guide-content ul, #guide-content ol { max-width: 100ch; margin: 12px 0 19px; padding-left: 24px; color: var(--ink-2); }
#guide-content li { margin: 7px 0; padding-left: 3px; }
#guide-content li::marker { color: var(--accent); font-weight: 700; }
#guide-content em { font-style: italic; }
#guide-content code { padding: 1px 5px; border-radius: 4px; background: #eef2f0; color: #6b430d; font: .9em/1.4 var(--mono); overflow-wrap: anywhere; }
.callout { display: grid; gap: 2px; max-width: 90ch; margin: 0 0 25px; padding: 17px 20px; border-left: 4px solid var(--accent); background: var(--accent-soft); color: #304a45; }
.callout p { margin: 3px 0 !important; color: inherit !important; font-size: 13px; }
.code-block { position: relative; max-width: 94ch; overflow: auto; margin: 18px 0 27px; padding: 17px 18px; border: 1px solid #cfd9d5; border-radius: 5px; background: #202827; color: #edf5f2; font: 13px/1.65 var(--mono); tab-size: 2; }
.code-block code { padding: 0; background: none; color: inherit; font: inherit; white-space: pre; }
.table-wrap { max-width: 100%; overflow: auto; margin: 18px 0 29px; border-top: 1px solid var(--line-strong); border-bottom: 1px solid var(--line-strong); scrollbar-color: var(--line-strong) transparent; }
.table-wrap:focus-visible { outline: 2px solid var(--accent); outline-offset: 3px; }
table { width: 100%; min-width: 760px; border-collapse: separate; border-spacing: 0; font-size: 13px; line-height: 1.55; font-variant-numeric: tabular-nums; }
th, td { padding: 11px 12px; border-bottom: 1px solid var(--line); vertical-align: top; text-align: left; }
thead th { position: sticky; top: 0; z-index: 2; color: #f4f8f6; background: #273432; font-weight: 700; white-space: nowrap; }
tbody th { color: var(--ink); font-weight: 700; }
tbody tr:nth-child(even) { background: var(--soft); }
tbody tr:hover { background: var(--accent-soft); }
tbody tr:last-child th, tbody tr:last-child td { border-bottom: 0; }
.footer-note { margin-top: 42px; padding-top: 17px; border-top: 1px solid var(--line); color: var(--muted); font-size: 12px; }

.annotation-drawer { position: fixed; top: 64px; right: 0; z-index: 70; width: min(390px, calc(100vw - 18px)); height: calc(100dvh - 64px); display: grid; grid-template-rows: auto auto auto minmax(0, 1fr); border-left: 1px solid var(--line-strong); background: var(--paper); box-shadow: -14px 0 34px rgba(23, 32, 31, .14); overscroll-behavior: contain; }
.annotation-drawer[hidden], .annotation-popover[hidden] { display: none !important; }
.annotation-drawer-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 14px; padding: 18px 18px 14px; border-bottom: 1px solid var(--line); }
.annotation-drawer-head h2 { margin: 0 0 3px; color: var(--ink); font-size: 20px; line-height: 1.25; }
.annotation-drawer-head p { margin: 0; color: var(--muted); font-size: 12px; }
.annotation-drawer-actions { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; }
.annotation-icon-btn { min-height: 32px; padding: 6px 8px; border: 1px solid var(--line); border-radius: 5px; background: var(--paper); color: var(--ink); font-size: 12px; }
.annotation-icon-btn:hover { border-color: var(--accent); color: var(--accent-dark); }
.annotation-tool-row { display: grid; grid-template-columns: minmax(0, 1fr) 116px; gap: 8px; padding: 14px 18px; border-bottom: 1px solid var(--line); }
.annotation-tool-row input, .annotation-tool-row select { min-height: 36px; width: 100%; border: 1px solid var(--line-strong); border-radius: 5px; padding: 7px 9px; background: #fff; color: var(--ink); }
.annotation-tool-row input::placeholder, .annotation-popover input::placeholder, .annotation-popover textarea::placeholder { color: #7b8783; }
.annotation-tool-row input:focus-visible, .annotation-tool-row select:focus-visible, .annotation-popover input:focus-visible, .annotation-popover textarea:focus-visible { outline: 2px solid rgba(15, 118, 110, .22); border-color: var(--accent); }
.annotation-count { padding: 10px 18px; border-bottom: 1px solid var(--line); color: var(--muted); font-size: 12px; }
.annotation-list { min-height: 0; overflow: auto; padding: 12px; }
.annotation-empty { padding: 18px 14px; border: 1px dashed var(--line-strong); border-radius: 6px; color: var(--muted); text-align: center; font-size: 13px; }
.annotation-card { display: grid; gap: 8px; width: 100%; margin-bottom: 10px; padding: 12px; border: 1px solid var(--line); border-left: 4px solid #facc15; border-radius: 6px; background: #fff; color: var(--ink); text-align: left; }
.annotation-card:hover, .annotation-card:focus-visible { border-color: var(--line-strong); box-shadow: 0 3px 12px rgba(23, 32, 31, .07); }
.annotation-card[data-color="green"] { border-left-color: #10b981; }
.annotation-card[data-color="blue"] { border-left-color: #3b82f6; }
.annotation-card[data-color="red"] { border-left-color: #ef4444; }
.annotation-quote { width: 100%; padding: 0; border: 0; background: transparent; color: var(--ink); font: inherit; font-weight: 700; text-align: left; overflow-wrap: anywhere; cursor: pointer; }
.annotation-quote:hover { color: var(--accent-dark); }
.annotation-comment { color: var(--ink-2); font-size: 13px; overflow-wrap: anywhere; }
.annotation-meta { display: flex; flex-wrap: wrap; gap: 5px; }
.annotation-tag { display: inline-flex; align-items: center; min-height: 21px; padding: 2px 7px; border-radius: 10px; background: #edf3ff; color: #2457d6; font-size: 11px; }
.annotation-card-actions { display: flex; justify-content: flex-end; gap: 10px; }
.annotation-text-btn { padding: 0; border: 0; background: transparent; color: #2457d6; font-size: 12px; }
.annotation-text-btn:hover { color: var(--accent-dark); text-decoration: underline; }
.annotation-text-btn.danger { color: #b42318; }
.annotation-status { min-height: 1.3em; margin: 0 18px; color: var(--muted); font-size: 12px; }
.annotation-status.is-error { color: #b42318; }
.annotation-popover { position: fixed; z-index: 85; width: min(350px, calc(100vw - 24px)); max-height: calc(100dvh - 24px); overflow: auto; display: grid; gap: 12px; padding: 14px; border: 1px solid var(--line); border-radius: 7px; background: var(--paper); box-shadow: var(--shadow); overscroll-behavior: contain; }
.annotation-selected { max-height: 92px; overflow: auto; padding: 8px; border-radius: 5px; background: var(--soft); color: var(--ink-2); font-size: 12px; overflow-wrap: anywhere; }
.annotation-popover label { display: grid; gap: 6px; color: var(--muted); font-size: 12px; }
.annotation-popover input, .annotation-popover textarea { width: 100%; border: 1px solid var(--line-strong); border-radius: 5px; padding: 8px 9px; background: #fff; color: var(--ink); }
.annotation-popover textarea { resize: vertical; min-height: 78px; }
.annotation-colors { display: flex; gap: 8px; }
.annotation-color { width: 28px; height: 28px; padding: 0; border: 2px solid transparent; border-radius: 50%; }
.annotation-color[data-color="yellow"] { background: #facc15; }
.annotation-color[data-color="green"] { background: #10b981; }
.annotation-color[data-color="blue"] { background: #3b82f6; }
.annotation-color[data-color="red"] { background: #ef4444; }
.annotation-color.is-active { border-color: var(--ink); box-shadow: 0 0 0 2px var(--paper); outline: 2px solid var(--ink); }
.annotation-popover-actions { display: flex; justify-content: flex-end; gap: 8px; }
.annotation-popover-actions button { min-height: 36px; padding: 7px 12px; border-radius: 5px; }
.annotation-cancel { border: 1px solid var(--line-strong); background: #fff; color: var(--ink); }
.annotation-save { border: 1px solid var(--charcoal); background: var(--charcoal); color: #fff; }
.annotation-save:hover { background: #101513; }
.html-annotation-highlight { position: absolute; z-index: 4; border-radius: 2px; pointer-events: auto; cursor: pointer; mix-blend-mode: multiply; }
.html-annotation-highlight[data-color="yellow"] { background: var(--yellow); }
.html-annotation-highlight[data-color="green"] { background: var(--green); }
.html-annotation-highlight[data-color="blue"] { background: var(--blue); }
.html-annotation-highlight[data-color="red"] { background: var(--red); }

@media (max-width: 1050px) {
  .topnav { display: none; }
  .brand { min-width: 0; }
  .layout { grid-template-columns: 190px minmax(0, 1fr); gap: 0 30px; }
}
@media (max-width: 780px) {
  html { scroll-padding-top: 60px; }
  .topbar, .topbar-inner { min-height: 58px; }
  .topbar-inner { width: min(100% - 28px, 1260px); gap: 10px; }
  .brand small { display: none; }
  .topbar-spacer { display: none; }
  .annotation-toggle { margin-left: auto; }
  .page-shell { width: min(100% - 28px, 1260px); }
  .layout { display: block; }
  .toc { position: sticky; top: 58px; z-index: 15; display: flex; align-items: center; gap: 0 16px; max-height: none; overflow-x: auto; padding: 13px 0; border-right: 0; border-bottom: 1px solid var(--line); background: var(--canvas); white-space: nowrap; }
  .toc strong { position: sticky; left: 0; z-index: 1; flex: 0 0 auto; margin: 0; padding-right: 10px; background: var(--canvas); }
  .toc a { flex: 0 0 auto; margin: 0; padding: 0; border: 0; }
  #guide-content { padding-top: 30px; }
  #guide-content > h1 { font-size: clamp(32px, 10vw, 46px); }
  .guide-section { margin-top: 38px; }
  .guide-section > h2 { font-size: 23px; }
  .table-wrap { margin-right: -14px; }
  table { min-width: 700px; }
  .annotation-drawer { top: auto; width: 100%; height: auto; max-height: 64vh; bottom: 0; border-left: 0; border-top: 1px solid var(--line-strong); box-shadow: 0 -14px 30px rgba(23, 32, 31, .14); }
  .annotation-list { max-height: 35vh; }
  .annotation-popover { width: min(360px, calc(100vw - 24px)); }
}
@media (max-width: 460px) {
  .page-shell { width: calc(100% - 24px); }
  .guide-actions .guide-action { width: 100%; justify-content: center; }
  .annotation-drawer-head { padding-inline: 14px; }
  .annotation-tool-row { grid-template-columns: 1fr; padding-inline: 14px; }
  .annotation-count { padding-inline: 14px; }
  .annotation-status { margin-inline: 14px; }
  .annotation-list { padding: 10px; }
}
@media (prefers-reduced-motion: reduce) {
  html { scroll-behavior: auto; }
  *, *::before, *::after { animation-duration: .01ms !important; animation-iteration-count: 1 !important; transition-duration: .01ms !important; }
}
@media print {
  body { background: #fff; }
  .topbar, .toc, .guide-actions, .annotation-drawer, .annotation-popover, .skip-link { display: none !important; }
  .page-shell { width: 100%; padding: 0; }
  #guide-content { padding: 0; }
  .table-wrap { overflow: visible; break-inside: avoid; }
}
"""


JS = r"""
(() => {
  const ROOT = document.querySelector('[data-annotation-root]');
  const DRAWER = document.getElementById('annotationDrawer');
  const TOP_TOGGLE = document.getElementById('topbarAnnotationToggle');
  const HERO_TOGGLE = document.getElementById('heroAnnotationToggle');
  const CLOSE = document.getElementById('annotationClose');
  const LIST = document.getElementById('annotationList');
  const COUNT = document.getElementById('annotationCount');
  const BADGE = document.getElementById('annotationBadge');
  const SEARCH = document.getElementById('annotationSearch');
  const TAG_FILTER = document.getElementById('annotationTagFilter');
  const IMPORT = document.getElementById('annotationImport');
  const IMPORT_INPUT = document.getElementById('annotationImportInput');
  const EXPORT = document.getElementById('annotationExport');
  const POPOVER = document.getElementById('annotationPopover');
  const SELECTED = document.getElementById('annotationSelected');
  const TAG_INPUT = document.getElementById('annotationTagInput');
  const COMMENT_INPUT = document.getElementById('annotationCommentInput');
  const CANCEL = document.getElementById('annotationCancel');
  const STATUS = document.getElementById('annotationStatus');
  const COLORS = new Set(['yellow', 'green', 'blue', 'red']);
  const STORAGE_KEY = `annotations:${location.href.split('#')[0]}`;
  const DOCUMENT_ID = 'pm-ai-guide';
  const SCHEMA_VERSION = 1;
  const HIGHLIGHT_CLASS = 'html-annotation-highlight';
  const HIGHLIGHT_COLORS = {
    yellow: 'rgba(255, 214, 10, .48)',
    green: 'rgba(52, 211, 153, .42)',
    blue: 'rgba(96, 165, 250, .42)',
    red: 'rgba(248, 113, 113, .42)'
  };

  let annotations = [];
  let pendingAnchor = null;
  let editingId = null;
  let selectedColor = 'yellow';
  let previousFocus = null;
  let recomputeTimer = null;
  let recomputing = false;
  const rendered = new Map();

  const setStatus = (text, error = false) => {
    STATUS.textContent = text;
    STATUS.classList.toggle('is-error', error);
  };
  const generateId = () => `ann-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
  const normalize = (text) => text.replace(/\s+/g, ' ').trim();
  const rootTextNodes = () => {
    if (!ROOT) return [];
    const walker = document.createTreeWalker(ROOT, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        if (parent?.closest('.annotation-drawer, .annotation-popover, .html-annotation-highlight, [data-no-annotation], script, style')) return NodeFilter.FILTER_REJECT;
        return NodeFilter.FILTER_ACCEPT;
      }
    });
    const nodes = [];
    let node = walker.nextNode();
    while (node) { nodes.push(node); node = walker.nextNode(); }
    return nodes;
  };
  const rootText = () => rootTextNodes().map((node) => node.nodeValue).join('');
  const offsetAt = (container, offset) => {
    const nodes = rootTextNodes();
    if (container.nodeType === Node.TEXT_NODE) {
      let total = 0;
      for (const node of nodes) {
        if (node === container) return total + offset;
        total += node.nodeValue.length;
      }
    }
    const range = document.createRange();
    range.selectNodeContents(ROOT);
    try { range.setEnd(container, offset); } catch (_) { return 0; }
    return range.toString().length;
  };
  const rangeOffsets = (range) => ({ start: offsetAt(range.startContainer, range.startOffset), end: offsetAt(range.endContainer, range.endOffset) });
  const buildAnchor = (range) => {
    const raw = range.toString();
    const exact = raw.trim();
    const leading = raw.indexOf(exact);
    const offsets = rangeOffsets(range);
    const start = offsets.start + Math.max(0, leading);
    const end = start + exact.length;
    const text = rootText();
    return {
      selector: { type: 'TextQuoteSelector', exact, prefix: text.slice(Math.max(0, start - 40), start), suffix: text.slice(end, end + 40) },
      positionSelector: { type: 'TextPositionSelector', start, end }
    };
  };
  const findRangeByOffsets = (start, end, exact) => {
    const nodes = rootTextNodes();
    let cursor = 0;
    let from = null;
    let to = null;
    for (const node of nodes) {
      const next = cursor + node.nodeValue.length;
      if (!from && start >= cursor && start <= next) from = { node, offset: start - cursor };
      if (!to && end >= cursor && end <= next) to = { node, offset: end - cursor };
      cursor = next;
      if (from && to) break;
    }
    if (!from || !to) return null;
    const range = document.createRange();
    range.setStart(from.node, from.offset);
    range.setEnd(to.node, to.offset);
    return !exact || normalize(range.toString()) === normalize(exact) ? range : null;
  };
  const findRangeByQuote = (selector) => {
    const exact = selector?.exact;
    if (!exact) return null;
    const text = rootText();
    let start = -1;
    const quote = `${selector.prefix || ''}${exact}${selector.suffix || ''}`;
    if (selector.prefix || selector.suffix) {
      const quoteIndex = text.indexOf(quote);
      if (quoteIndex >= 0) start = quoteIndex + (selector.prefix || '').length;
    }
    if (start < 0) start = text.indexOf(exact);
    return start < 0 ? null : findRangeByOffsets(start, start + exact.length, exact);
  };
  const resolveRange = (annotation) => {
    const position = annotation.positionSelector;
    return (position && findRangeByOffsets(position.start, position.end, annotation.selector?.exact)) || findRangeByQuote(annotation.selector);
  };
  const removeHighlight = (id) => {
    ROOT?.querySelectorAll(`[data-ann-id="${CSS.escape(id)}"]`).forEach((node) => node.remove());
    rendered.delete(id);
  };
  const renderHighlight = (annotation) => {
    if (!ROOT || !annotation?.id) return false;
    const range = resolveRange(annotation);
    if (!range) return false;
    removeHighlight(annotation.id);
    const rects = Array.from(range.getClientRects()).filter((rect) => rect.width > 0 && rect.height > 0);
    const rootRect = ROOT.getBoundingClientRect();
    rects.forEach((rect) => {
      const mark = document.createElement('span');
      mark.className = HIGHLIGHT_CLASS;
      mark.dataset.annId = annotation.id;
      mark.dataset.color = annotation.highlight?.color || 'yellow';
      mark.style.left = `${rect.left - rootRect.left + ROOT.scrollLeft}px`;
      mark.style.top = `${rect.top - rootRect.top + ROOT.scrollTop}px`;
      mark.style.width = `${rect.width}px`;
      mark.style.height = `${rect.height}px`;
      mark.style.background = HIGHLIGHT_COLORS[annotation.highlight?.color] || HIGHLIGHT_COLORS.yellow;
      mark.title = annotation.highlight?.comment || annotation.selector?.exact || '标注';
      mark.setAttribute('aria-label', `标注：${annotation.selector?.exact || ''}`);
      mark.addEventListener('click', () => window.dispatchEvent(new CustomEvent('guide:annotation-click', { detail: { id: annotation.id } })));
      ROOT.appendChild(mark);
    });
    rendered.set(annotation.id, annotation);
    return rects.length > 0;
  };
  const clearHighlights = () => {
    ROOT?.querySelectorAll(`.${HIGHLIGHT_CLASS}`).forEach((node) => node.remove());
    rendered.clear();
  };
  const rehydrate = (list) => {
    clearHighlights();
    return list.map((annotation) => ({ id: annotation.id, ok: renderHighlight(annotation) }));
  };
  const scheduleRecompute = (delay = 90) => {
    clearTimeout(recomputeTimer);
    recomputeTimer = setTimeout(() => {
      if (recomputing) return;
      recomputing = true;
      const list = Array.from(rendered.values());
      clearHighlights();
      list.forEach(renderHighlight);
      recomputing = false;
    }, delay);
  };
  const scrollToAnnotation = (id) => {
    const mark = ROOT?.querySelector(`[data-ann-id="${CSS.escape(id)}"]`);
    if (!mark) return;
    mark.scrollIntoView({ behavior: 'smooth', block: 'center' });
    mark.animate([{ outline: '2px solid rgba(15,118,110,0)' }, { outline: '2px solid rgba(15,118,110,.95)' }, { outline: '2px solid rgba(15,118,110,0)' }], { duration: 850 });
  };
  window.htmlAnnotator = { renderHighlight, removeHighlight, clearHighlights, rehydrate, scrollToAnnotation };

  const readStorage = () => {
    try {
      const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
      if (!Array.isArray(parsed)) return [];
      return parsed.filter((item) => item && item.id && item.selector?.exact).map((item) => ({
        ...item,
        documentId: item.documentId || DOCUMENT_ID,
        schemaVersion: item.schemaVersion || SCHEMA_VERSION,
        highlight: { color: COLORS.has(item.highlight?.color) ? item.highlight.color : 'yellow', tags: Array.isArray(item.highlight?.tags) ? item.highlight.tags.filter((tag) => typeof tag === 'string') : [], comment: typeof item.highlight?.comment === 'string' ? item.highlight.comment : '' }
      }));
    } catch (_) { return []; }
  };
  const writeStorage = () => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(annotations)); }
    catch (_) { setStatus('浏览器未允许本地保存，当前内容仍可继续阅读。', true); }
    updateBadge();
  };
  const updateBadge = () => {
    BADGE.textContent = String(annotations.length);
    BADGE.hidden = annotations.length === 0;
  };
  const setToggleState = (open) => [TOP_TOGGLE, HERO_TOGGLE].filter(Boolean).forEach((button) => {
    button.setAttribute('aria-expanded', String(open));
    button.title = open ? '关闭标注面板' : '打开标注面板';
  });
  const openDrawer = (focus = false) => {
    if (DRAWER.hidden) previousFocus = document.activeElement;
    DRAWER.hidden = false;
    setToggleState(true);
    refreshTagFilter();
    renderList();
    if (focus) SEARCH.focus();
  };
  const closeDrawer = () => {
    closePopover();
    DRAWER.hidden = true;
    setToggleState(false);
    if (previousFocus?.focus) previousFocus.focus();
  };
  const allTags = () => Array.from(new Set(annotations.flatMap((item) => item.highlight?.tags || []))).sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const refreshTagFilter = () => {
    const current = TAG_FILTER.value;
    TAG_FILTER.innerHTML = '<option value="">全部标签</option>';
    allTags().forEach((tag) => { const option = document.createElement('option'); option.value = tag; option.textContent = tag; TAG_FILTER.appendChild(option); });
    if (allTags().includes(current)) TAG_FILTER.value = current;
  };
  const visibleAnnotations = () => {
    const query = SEARCH.value.trim().toLowerCase();
    const tag = TAG_FILTER.value;
    return annotations.filter((item) => {
      const tags = item.highlight?.tags || [];
      const haystack = [item.selector?.exact || '', item.highlight?.comment || '', ...tags].join(' ').toLowerCase();
      return (!query || haystack.includes(query)) && (!tag || tags.includes(tag));
    });
  };
  const renderList = () => {
    const visible = visibleAnnotations();
    COUNT.textContent = `${visible.length} / ${annotations.length} 条标注`;
    LIST.replaceChildren();
    if (!annotations.length) { LIST.innerHTML = '<div class="annotation-empty">选中正文中的文字即可新建标注。</div>'; updateBadge(); return; }
    if (!visible.length) { LIST.innerHTML = '<div class="annotation-empty">没有符合条件的标注。</div>'; updateBadge(); return; }
    visible.forEach((annotation) => {
      const card = document.createElement('article');
      card.className = 'annotation-card'; card.dataset.id = annotation.id; card.dataset.color = annotation.highlight?.color || 'yellow';
      const quote = document.createElement('button'); quote.type = 'button'; quote.className = 'annotation-quote'; const exact = annotation.selector?.exact || ''; quote.textContent = `“${exact.slice(0, 96)}${exact.length > 96 ? '…' : ''}”`; quote.title = '定位到正文中的标注'; card.appendChild(quote);
      if (annotation.highlight?.comment) { const comment = document.createElement('div'); comment.className = 'annotation-comment'; comment.textContent = annotation.highlight.comment; card.appendChild(comment); }
      if (annotation.highlight?.tags?.length) { const meta = document.createElement('div'); meta.className = 'annotation-meta'; annotation.highlight.tags.forEach((tag) => { const el = document.createElement('span'); el.className = 'annotation-tag'; el.textContent = tag; meta.appendChild(el); }); card.appendChild(meta); }
      const actions = document.createElement('div'); actions.className = 'annotation-card-actions';
      const edit = document.createElement('button'); edit.type = 'button'; edit.className = 'annotation-text-btn'; edit.textContent = '编辑';
      const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'annotation-text-btn danger'; remove.textContent = '删除';
      actions.append(edit, remove); card.appendChild(actions);
      quote.addEventListener('click', () => scrollToAnnotation(annotation.id));
      edit.addEventListener('click', (event) => { event.stopPropagation(); openEdit(annotation.id); });
      remove.addEventListener('click', (event) => { event.stopPropagation(); deleteAnnotation(annotation.id); });
      LIST.appendChild(card);
    });
    updateBadge();
  };
  const closePopover = () => { POPOVER.hidden = true; pendingAnchor = null; editingId = null; };
  const setColor = (color) => { selectedColor = COLORS.has(color) ? color : 'yellow'; document.querySelectorAll('.annotation-color').forEach((button) => button.classList.toggle('is-active', button.dataset.color === selectedColor)); };
  const placePopover = (rect) => {
    POPOVER.hidden = false; POPOVER.style.transform = ''; POPOVER.style.left = '0px'; POPOVER.style.top = '0px';
    const margin = 12; const size = POPOVER.getBoundingClientRect();
    const left = Math.min(Math.max(margin, rect.left), Math.max(margin, window.innerWidth - size.width - margin));
    const below = rect.top + rect.height + 8; const above = rect.top - size.height - 8;
    const preferred = below + size.height + margin <= window.innerHeight ? below : above;
    const top = Math.min(Math.max(margin, preferred), Math.max(margin, window.innerHeight - size.height - margin));
    POPOVER.style.left = `${left}px`; POPOVER.style.top = `${top}px`;
  };
  const centerPopover = () => { POPOVER.hidden = false; POPOVER.style.left = '50%'; POPOVER.style.top = '50%'; POPOVER.style.transform = 'translate(-50%, -50%)'; };
  const openEdit = (id) => {
    const annotation = annotations.find((item) => item.id === id); if (!annotation) return;
    editingId = id; pendingAnchor = null; SELECTED.textContent = annotation.selector.exact; TAG_INPUT.value = (annotation.highlight.tags || []).join(', '); COMMENT_INPUT.value = annotation.highlight.comment || ''; setColor(annotation.highlight.color); centerPopover(); COMMENT_INPUT.focus();
  };
  const deleteAnnotation = (id) => {
    const annotation = annotations.find((item) => item.id === id); if (!annotation) return;
    const preview = annotation.selector.exact.slice(0, 50);
    if (!window.confirm(`确认删除这条标注吗？\n\n“${preview}${preview.length >= 50 ? '…' : ''}”`)) return;
    annotations = annotations.filter((item) => item.id !== id); window.htmlAnnotator.removeHighlight(id); writeStorage(); refreshTagFilter(); renderList(); setStatus('标注已删除。');
  };
  const exportAnnotations = () => {
    const blob = new Blob([JSON.stringify(annotations, null, 2)], { type: 'application/json' }); const url = URL.createObjectURL(blob); const link = document.createElement('a'); link.href = url; link.download = 'PM的AI工作指南-annotations.json'; link.click(); URL.revokeObjectURL(url); setStatus('已导出标注 JSON。');
  };
  const importAnnotations = async (file) => {
    let data;
    try { data = JSON.parse(await file.text()); } catch (_) { setStatus('导入失败：JSON 格式不正确。', true); return; }
    if (!Array.isArray(data)) { setStatus('导入失败：标注文件应为数组。', true); return; }
    const existing = new Set(annotations.map((item) => item.id));
    const incoming = data.filter((item) => item && item.id && item.selector?.exact && !existing.has(item.id)).map((item) => ({ ...item, documentId: item.documentId || DOCUMENT_ID, schemaVersion: item.schemaVersion || SCHEMA_VERSION, highlight: { color: COLORS.has(item.highlight?.color) ? item.highlight.color : 'yellow', tags: Array.isArray(item.highlight?.tags) ? item.highlight.tags.filter((tag) => typeof tag === 'string') : [], comment: typeof item.highlight?.comment === 'string' ? item.highlight.comment : '' } }));
    if (!incoming.length) { setStatus('没有新的标注可导入。'); return; }
    annotations = annotations.concat(incoming); writeStorage(); refreshTagFilter(); renderList(); const restored = window.htmlAnnotator.rehydrate(annotations).filter((item) => item.ok).length; setStatus(`已导入 ${incoming.length} 条标注，恢复 ${restored} 条。`);
  };
  const handleSelection = (detail) => {
    if (DRAWER.hidden || !detail?.anchor) return;
    pendingAnchor = detail.anchor; editingId = null; SELECTED.textContent = detail.preview || detail.anchor.selector.exact; TAG_INPUT.value = ''; COMMENT_INPUT.value = ''; setColor('yellow'); placePopover(detail.rect || { left: 16, top: 16, width: 0, height: 0 }); TAG_INPUT.focus();
  };

  [TOP_TOGGLE, HERO_TOGGLE].filter(Boolean).forEach((button) => button.addEventListener('click', () => DRAWER.hidden ? openDrawer(true) : closeDrawer()));
  CLOSE.addEventListener('click', closeDrawer);
  CANCEL.addEventListener('click', closePopover);
  document.querySelectorAll('.annotation-color').forEach((button) => button.addEventListener('click', () => setColor(button.dataset.color)));
  SEARCH.addEventListener('input', renderList); TAG_FILTER.addEventListener('change', renderList); EXPORT.addEventListener('click', exportAnnotations);
  IMPORT.addEventListener('click', () => IMPORT_INPUT.click());
  IMPORT_INPUT.addEventListener('change', () => { const file = IMPORT_INPUT.files?.[0]; IMPORT_INPUT.value = ''; if (file) importAnnotations(file); });
  POPOVER.addEventListener('submit', (event) => {
    event.preventDefault();
    const tags = Array.from(new Set(TAG_INPUT.value.split(',').map((tag) => tag.trim()).filter(Boolean)));
    const highlight = { color: selectedColor, tags, comment: COMMENT_INPUT.value.trim() };
    const wasEditing = Boolean(editingId);
    if (wasEditing) { const annotation = annotations.find((item) => item.id === editingId); if (annotation) { annotation.highlight = highlight; window.htmlAnnotator.renderHighlight(annotation); } }
    else if (pendingAnchor) { const annotation = { id: generateId(), documentId: DOCUMENT_ID, schemaVersion: SCHEMA_VERSION, documentPath: location.href.split('#')[0], selector: pendingAnchor.selector, positionSelector: pendingAnchor.positionSelector, highlight, createdAt: new Date().toISOString() }; annotations.push(annotation); window.htmlAnnotator.renderHighlight(annotation); }
    writeStorage(); refreshTagFilter(); renderList(); closePopover(); window.getSelection()?.removeAllRanges(); setStatus(wasEditing ? '标注已更新。' : '标注已保存。');
  });
  document.addEventListener('keydown', (event) => { if (event.key !== 'Escape') return; if (!POPOVER.hidden) { event.preventDefault(); closePopover(); } else if (!DRAWER.hidden) { event.preventDefault(); closeDrawer(); } });
  window.addEventListener('guide:text-selection', (event) => handleSelection(event.detail));
  window.addEventListener('guide:annotation-click', (event) => { openDrawer(false); LIST.querySelector(`[data-id="${CSS.escape(event.detail.id)}"]`)?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); });
  window.addEventListener('resize', () => scheduleRecompute());
  window.addEventListener('scroll', () => scheduleRecompute(120), { passive: true });
  document.fonts?.ready?.then(() => scheduleRecompute(200));
  document.querySelectorAll('.table-wrap').forEach((table) => table.addEventListener('scroll', () => scheduleRecompute(40), { passive: true }));
  if (typeof ResizeObserver === 'function') new ResizeObserver(() => scheduleRecompute(120)).observe(ROOT);
  if (typeof MutationObserver === 'function') new MutationObserver((records) => { if (records.some((record) => Array.from(record.addedNodes).some((node) => !node.classList?.contains(HIGHLIGHT_CLASS)))) scheduleRecompute(150); }).observe(ROOT, { childList: true, subtree: true });

  annotations = readStorage(); updateBadge(); refreshTagFilter(); renderList(); const restored = window.htmlAnnotator.rehydrate(annotations).filter((item) => item.ok).length; if (annotations.length) setStatus(`已恢复 ${restored}/${annotations.length} 条标注。`);
})();

// Keep the selection/highlight runtime in the same document. The reference
// implementation uses postMessage for an iframe; this page can dispatch a
// typed custom event directly and keep the same annotation JSON contract.
(() => {
  const root = document.querySelector('[data-annotation-root]');
  const rootTextNodes = () => {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        if (!node.nodeValue?.trim()) return NodeFilter.FILTER_REJECT;
        if (node.parentElement?.closest('.annotation-drawer, .annotation-popover, .html-annotation-highlight, [data-no-annotation], script, style')) return NodeFilter.FILTER_REJECT;
        return NodeFilter.FILTER_ACCEPT;
      }
    });
    const nodes = []; let node = walker.nextNode(); while (node) { nodes.push(node); node = walker.nextNode(); } return nodes;
  };
  const getText = () => rootTextNodes().map((node) => node.nodeValue).join('');
  const offsetAt = (container, offset) => {
    const nodes = rootTextNodes(); let total = 0;
    if (container.nodeType === Node.TEXT_NODE) { for (const node of nodes) { if (node === container) return total + offset; total += node.nodeValue.length; } }
    const range = document.createRange(); range.selectNodeContents(root); try { range.setEnd(container, offset); } catch (_) { return 0; } return range.toString().length;
  };
  const buildAnchor = (range) => {
    const raw = range.toString(); const exact = raw.trim(); const trimOffset = Math.max(0, raw.indexOf(exact)); const rawStart = offsetAt(range.startContainer, range.startOffset); const start = rawStart + trimOffset; const end = start + exact.length; const text = getText();
    return { selector: { type: 'TextQuoteSelector', exact, prefix: text.slice(Math.max(0, start - 40), start), suffix: text.slice(end, end + 40) }, positionSelector: { type: 'TextPositionSelector', start, end } };
  };
  document.addEventListener('mouseup', () => {
    const selection = window.getSelection(); if (!selection || selection.isCollapsed || !selection.rangeCount) return;
    const anchorParent = selection.anchorNode?.parentElement; const focusParent = selection.focusNode?.parentElement;
    if (anchorParent?.closest('.annotation-drawer, .annotation-popover') || focusParent?.closest('.annotation-drawer, .annotation-popover')) return;
    if (!root.contains(selection.anchorNode) || !root.contains(selection.focusNode)) return;
    const preview = selection.toString().trim(); if (!preview) return;
    const range = selection.getRangeAt(0).cloneRange(); const rect = range.getBoundingClientRect();
    window.dispatchEvent(new CustomEvent('guide:text-selection', { detail: { anchor: buildAnchor(range), preview, rect: { left: rect.left, top: rect.top, width: rect.width, height: rect.height } } }));
  });
})();
"""


def build() -> None:
    body, nav = render(SOURCE.read_text(encoding="utf-8"))
    meta = {
        "title": "PM 的 AI 工作指南：从会问到会用，从提效到判断",
        "version": "v1.0",
        "updated_at": "2026-08-24",
        "audience": "PM 新员工（兼顾在岗 PM）",
        "status": "内部试行",
        "owner": "PM 团队",
    }
    document = f'''<!doctype html>
<html lang="zh-CN" data-document-id="pm-ai-guide" data-document-version="{meta["version"]}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="theme-color" content="#202827">
  <meta name="robots" content="noindex,nofollow">
  <meta name="description" content="面向 PM 新员工的 AI 认知、工作地图、任务卡、安全边界、四标准飞轮与 30 天上手路径。">
  <link rel="icon" href="data:,">
  <title>{html.escape(meta["title"])}</title>
  <style>{CSS}</style>
</head>
<body>
  <a class="skip-link" href="#guide-content">跳到正文</a>
  <header class="topbar">
    <div class="topbar-inner">
      <a class="brand" href="#top" aria-label="回到 PM 的 AI 工作指南顶部">
        <span class="brand-mark" aria-hidden="true">AI</span>
        <span><strong>PM 的 AI 工作指南</strong><small>新员工 30 天上手 · 内部试行</small></span>
      </a>
      <nav class="topnav" aria-label="章节导航">{nav}</nav>
      <span class="topbar-spacer"></span>
      <button class="annotation-toggle" id="topbarAnnotationToggle" type="button" aria-label="打开标注面板" aria-expanded="false" aria-controls="annotationDrawer">标注 <span class="annotation-badge" id="annotationBadge" hidden>0</span></button>
    </div>
  </header>

  <div class="page-shell" id="top">
    <div class="layout">
      <nav class="toc" aria-label="本页目录" role="doc-toc">
        <strong>本页目录</strong>
        {nav}
        <p class="toc-note">选中文字前先打开标注面板。标注保存在当前浏览器，可导出为 JSON 备份。</p>
      </nav>
      <main id="guide-content" data-annotation-root data-document-id="pm-ai-guide" data-annotation-version="{meta["version"]}" tabindex="-1">
        <div class="guide-meta"><span>{meta["status"]}</span><span>{meta["version"]}</span><span>更新于 {meta["updated_at"]}</span><span>{meta["audience"]}</span><span>Owner：{meta["owner"]}</span></div>
        <div class="guide-actions" data-no-annotation="true">
          <button class="guide-action primary" id="heroAnnotationToggle" type="button" aria-expanded="false" aria-controls="annotationDrawer">打开标注</button>
          <a class="guide-action" href="#quick-start">从今天开始</a>
        </div>
        {body}
        <p class="footer-note">本页面由 Markdown 源文档生成，正文事实与内部链接以知识库最新版本为准。标注只保存在当前浏览器，不会自动写回源文档。</p>
      </main>
    </div>
  </div>

  <aside class="annotation-drawer" id="annotationDrawer" aria-label="页面标注" hidden>
    <header class="annotation-drawer-head">
      <div><h2>标注</h2><p>选中正文中的文字，保存你的判断、证据或待确认项。</p></div>
      <div class="annotation-drawer-actions">
        <input id="annotationImportInput" name="annotationImportInput" type="file" accept=".json" hidden>
        <button class="annotation-icon-btn" id="annotationImport" type="button" title="导入标注 JSON">导入 JSON</button>
        <button class="annotation-icon-btn" id="annotationExport" type="button" title="导出标注 JSON">导出 JSON</button>
        <button class="annotation-icon-btn" id="annotationClose" type="button" title="关闭标注面板" aria-label="关闭标注面板">关闭</button>
      </div>
    </header>
    <div class="annotation-tool-row">
      <label class="sr-only" for="annotationSearch">搜索标注</label>
      <input id="annotationSearch" name="annotationSearch" type="search" autocomplete="off" placeholder="搜索原文、评论、标签…" aria-label="搜索原文、评论、标签">
      <label class="sr-only" for="annotationTagFilter">按标签筛选</label>
      <select id="annotationTagFilter" name="annotationTagFilter" autocomplete="off" aria-label="按标签筛选"><option value="">全部标签</option></select>
    </div>
    <div class="annotation-count" id="annotationCount" aria-live="polite">0 / 0 条标注</div>
    <p class="annotation-status" id="annotationStatus" aria-live="polite"></p>
    <div id="annotationList" class="annotation-list"></div>
  </aside>

  <form class="annotation-popover" id="annotationPopover" aria-label="编辑标注" hidden>
    <div class="annotation-selected" id="annotationSelected"></div>
    <label>颜色 <span class="annotation-colors" id="annotationColors">
      <button class="annotation-color is-active" type="button" data-color="yellow" aria-label="黄色标注"></button>
      <button class="annotation-color" type="button" data-color="green" aria-label="绿色标注"></button>
      <button class="annotation-color" type="button" data-color="blue" aria-label="蓝色标注"></button>
      <button class="annotation-color" type="button" data-color="red" aria-label="红色标注"></button>
    </span></label>
    <label for="annotationTagInput">标签<input id="annotationTagInput" name="annotationTagInput" type="text" autocomplete="off" placeholder="例如：流程，风险…"></label>
    <label for="annotationCommentInput">评论<textarea id="annotationCommentInput" name="annotationCommentInput" rows="3" placeholder="写下这处标注的判断或证据…"></textarea></label>
    <div class="annotation-popover-actions">
      <button class="annotation-cancel" id="annotationCancel" type="button">取消</button>
      <button class="annotation-save" type="submit">保存标注</button>
    </div>
  </form>

  <script>{JS}</script>
</body>
</html>
'''
    TARGET.write_text(document, encoding="utf-8")
    print(TARGET)


if __name__ == "__main__":
    build()
