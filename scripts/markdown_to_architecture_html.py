#!/usr/bin/env python3
"""Convert an internal architecture Markdown document into a standalone HTML report."""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path


FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})([^`]*)$" )
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


def slug(value: str, used: set[str]) -> str:
    """Create a stable, readable anchor for Chinese or ASCII headings."""
    anchor = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", value.lower()).strip("-") or "section"
    candidate = anchor
    suffix = 2
    while candidate in used:
        candidate = f"{anchor}-{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def inline(value: str) -> str:
    """Render the small Markdown inline subset used by the source document."""
    escaped = html.escape(value, quote=False)
    # Protect code spans before emphasis parsing. Identifiers such as
    # ``__canary__`` are literal values, not Markdown emphasis.
    code_spans: list[str] = []

    def protect_code(match: re.Match[str]) -> str:
        code_spans.append(match.group(1))
        return f"@@CODE_SPAN_{len(code_spans) - 1}@@"

    escaped = re.sub(r"`([^`]+)`", protect_code, escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"__([^_]+)__", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<em>\1</em>", escaped)

    def link(match: re.Match[str]) -> str:
        label, target = match.group(1), match.group(2)
        return f'<a href="{target}" rel="noopener">{label}</a>'

    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", link, escaped)
    for index, code in enumerate(code_spans):
        escaped = escaped.replace(f"@@CODE_SPAN_{index}@@", f"<code>{code}</code>")
    return escaped


def table_row(line: str) -> list[str]:
    value = line.strip()
    if value.startswith("|"):
        value = value[1:]
    if value.endswith("|"):
        value = value[:-1]
    return [cell.strip() for cell in value.split("|")]


def is_table_separator(line: str, width: int) -> bool:
    cells = table_row(line)
    return len(cells) == width and all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in cells)


def render(markdown: str) -> str:
    lines = markdown.splitlines()
    body: list[str] = []
    headings: list[tuple[str, str, int]] = []
    metadata: list[tuple[str, str]] = []
    used_anchors: set[str] = set()
    paragraph: list[str] = []
    list_items: list[str] = []
    list_type = ""
    title = "架构设计文档"
    i = 0

    def flush_paragraph() -> None:
        if paragraph:
            text = " ".join(part.strip() for part in paragraph).strip()
            if text:
                body.append(f"<p>{inline(text)}</p>")
            paragraph.clear()

    def flush_list() -> None:
        nonlocal list_type
        if list_items:
            tag = "ol" if list_type == "ol" else "ul"
            body.append(f"<{tag}>" + "".join(f"<li>{item}</li>" for item in list_items) + f"</{tag}>")
            list_items.clear()
            list_type = ""

    def add_heading(level: int, text: str) -> None:
        nonlocal title
        clean = re.sub(r"^#+\s*", "", text).strip()
        anchor = slug(clean, used_anchors)
        headings.append((clean, anchor, level))
        if level == 1 and not title:
            title = clean
        body.append(f'<h{level} id="{anchor}">{inline(clean)}</h{level}>')

    # A YAML frontmatter block is optional. The current source uses a leading
    # blockquote, which is parsed below into the same metadata panel.
    if lines and lines[0].strip() == "---":
        end = next((idx for idx in range(1, len(lines)) if lines[idx].strip() == "---"), None)
        if end is not None:
            for item in lines[1:end]:
                if ":" in item:
                    key, value = item.split(":", 1)
                    metadata.append((key.strip(), value.strip().strip("'\"")))
            lines = lines[end + 1 :]

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            flush_paragraph()
            flush_list()
            i += 1
            continue

        fence = FENCE_RE.match(line)
        if fence:
            flush_paragraph()
            flush_list()
            marker, language_text = fence.groups()
            language = language_text.strip().split()[0].lower() if language_text.strip() else "text"
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith(marker[0] * len(marker)):
                code_lines.append(lines[i])
                i += 1
            if i < len(lines):
                i += 1
            code = "\n".join(code_lines)
            escaped_code = html.escape(code, quote=False)
            if language == "mermaid":
                body.append(
                    '<div class="diagram" data-mermaid="true">'
                    f'<div class="mermaid" aria-label="Mermaid 架构图">{escaped_code}</div>'
                    f'<details class="diagram-source"><summary>查看 Mermaid 源码</summary><pre><code>{escaped_code}</code></pre></details>'
                    "</div>"
                )
            else:
                body.append(f'<pre class="code-block"><code class="language-{html.escape(language)}">{escaped_code}</code></pre>')
            continue

        heading = HEADING_RE.match(stripped)
        if heading:
            flush_paragraph()
            flush_list()
            level = len(heading.group(1))
            text = heading.group(2).strip()
            if level == 1 and not headings:
                title = text
            add_heading(level, text)
            i += 1
            continue

        if stripped.startswith(">"):
            flush_paragraph()
            flush_list()
            quote_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(lines[i].strip()[1:].strip())
                i += 1
            quote_html: list[str] = []
            for quote in quote_lines:
                # Source metadata uses a full-width colon; ASCII colons may
                # occur inside URLs and must remain part of the value.
                if "：" in quote:
                    key, value = quote.split("：", 1)
                    metadata.append((key.strip(), value.strip()))
                    quote_html.append(f'<div class="meta-item"><dt>{inline(key.strip())}</dt><dd>{inline(value.strip())}</dd></div>')
                elif quote:
                    quote_html.append(f"<p>{inline(quote)}</p>")
            if quote_html:
                if all(item.startswith('<div class="meta-item"') for item in quote_html):
                    body.append('<dl class="document-meta">' + "".join(quote_html) + "</dl>")
                else:
                    body.append('<aside class="callout">' + "".join(quote_html) + "</aside>")
            continue

        ordered = re.match(r"^\d+\.\s+(.+)$", stripped)
        unordered = re.match(r"^[-*+]\s+(.+)$", stripped)
        if ordered or unordered:
            flush_paragraph()
            wanted = "ol" if ordered else "ul"
            if list_type and list_type != wanted:
                flush_list()
            list_type = wanted
            list_items.append(inline((ordered or unordered).group(1)))
            i += 1
            continue

        if stripped.startswith("|") and i + 1 < len(lines):
            headers_cells = table_row(line)
            if is_table_separator(lines[i + 1], len(headers_cells)):
                flush_paragraph()
                flush_list()
                rows: list[list[str]] = []
                i += 2
                while i < len(lines) and lines[i].strip().startswith("|"):
                    rows.append(table_row(lines[i]))
                    i += 1
                table_parts = ['<div class="table-wrap"><table><thead><tr>']
                table_parts.extend(f"<th>{inline(cell)}</th>" for cell in headers_cells)
                table_parts.append("</tr></thead><tbody>")
                for row in rows:
                    table_parts.append("<tr>")
                    for column in range(len(headers_cells)):
                        cell = row[column] if column < len(row) else ""
                        table_parts.append(f"<td>{inline(cell)}</td>")
                    table_parts.append("</tr>")
                table_parts.append("</tbody></table></div>")
                body.append("".join(table_parts))
                continue

        if stripped == "---":
            flush_paragraph()
            flush_list()
            body.append('<hr class="section-rule">')
            i += 1
            continue

        paragraph.append(stripped)
        i += 1

    flush_paragraph()
    flush_list()

    # Avoid duplicating the H1 in the content area; the header carries it.
    if body and re.match(r"<h1\b", body[0]):
        body = body[1:]
    nav = "".join(f'<a href="#{anchor}">{inline(text)}</a>' for text, anchor, level in headings if level == 2)
    meta_chips = "".join(f'<span>{html.escape(key)}：{html.escape(value)}</span>' for key, value in metadata[:5])
    body_html = "".join(body)
    mermaid_loader = """
<script type="module">
  const diagrams = [...document.querySelectorAll('[data-mermaid] .mermaid')];
  if (diagrams.length) {
    import('https://cdn.jsdelivr.net/npm/mermaid@11.12.1/dist/mermaid.esm.min.mjs')
      .then(({ default: mermaid }) => {
        mermaid.initialize({ startOnLoad: false, securityLevel: 'strict', theme: 'neutral' });
        return mermaid.run({ nodes: diagrams });
      })
      .then(() => document.querySelectorAll('[data-mermaid] .mermaid').forEach(node => {
        node.closest('.diagram')?.classList.add('diagram-rendered');
      }))
      .catch(() => {});
  }
</script>"""
    return f'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex,nofollow">
  <meta name="description" content="{html.escape(title)}">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #182321; --muted: #66736f; --line: #d7dfdc; --line-strong: #b7c4bf;
      --paper: #ffffff; --canvas: #edf2f0; --soft: #f5f8f6; --accent: #0f766e;
      --accent-dark: #075e58; --accent-soft: #e5f2ef; --code: #202b2a;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; background: var(--canvas); }}
    body {{ margin: 0; color: var(--ink); background: var(--canvas); font: 15px/1.7 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; letter-spacing: 0; }}
    a {{ color: var(--accent-dark); text-underline-offset: 3px; }}
    a:focus-visible {{ outline: 2px solid var(--accent); outline-offset: 3px; }}
    .masthead {{ color: #f5faf8; background: #172321; border-bottom: 4px solid var(--accent); }}
    .masthead-inner {{ width: min(1500px, 100%); margin: 0 auto; padding: 38px 32px 28px; }}
    .eyebrow {{ margin: 0 0 12px; color: #91aaa4; font: 650 11px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; letter-spacing: .12em; text-transform: uppercase; }}
    h1 {{ max-width: 980px; margin: 0; font-size: clamp(31px, 4vw, 50px); font-weight: 720; line-height: 1.14; letter-spacing: 0; }}
    .subtitle {{ max-width: 850px; margin: 13px 0 0; color: #b9c9c4; font-size: 16px; }}
    .meta-chips {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 22px; }}
    .meta-chips span {{ padding: 5px 9px; color: #c9d5d1; border: 1px solid #40504c; border-radius: 4px; font-size: 12px; }}
    .content-shell {{ width: min(1500px, 100%); margin: 0 auto; padding: 0 32px 72px; }}
    .layout {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); gap: 0 48px; align-items: start; }}
    .toc {{ position: sticky; top: 0; max-height: 100dvh; overflow-y: auto; padding: 34px 24px 34px 0; border-right: 1px solid var(--line); }}
    .toc strong {{ display: block; margin-bottom: 12px; color: #26332f; font-size: 13px; }}
    .toc a {{ display: block; padding: 6px 10px 6px 0; border-right: 2px solid transparent; color: var(--muted); font-size: 13px; line-height: 1.45; text-decoration: none; }}
    .toc a:hover, .toc a:focus-visible {{ color: var(--accent-dark); border-right-color: var(--accent); }}
    main {{ min-width: 0; padding: 34px 0 24px; }}
    h2, h3, h4, h5, h6 {{ scroll-margin-top: 22px; letter-spacing: 0; }}
    h2 {{ margin: 52px 0 16px; padding-top: 22px; border-top: 1px solid var(--line-strong); color: #13201d; font-size: 27px; font-weight: 700; line-height: 1.3; }}
    h2:first-of-type {{ margin-top: 24px; }}
    h3 {{ margin: 34px 0 12px; color: var(--accent-dark); font-size: 19px; font-weight: 700; line-height: 1.35; }}
    h4, h5, h6 {{ margin: 24px 0 9px; color: #25332f; font-size: 16px; }}
    p {{ max-width: 90ch; margin: 11px 0; }}
    .document-meta {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 26px; margin: 0 0 30px; padding: 17px 20px; background: var(--accent-soft); border-left: 4px solid var(--accent); border-radius: 4px; }}
    .meta-item {{ display: grid; grid-template-columns: 90px minmax(0, 1fr); gap: 10px; padding: 5px 0; }}
    .meta-item dt {{ color: #52706a; font-size: 12px; font-weight: 650; }}
    .meta-item dd {{ margin: 0; color: #263e39; font-size: 13px; }}
    .callout {{ margin: 18px 0 24px; padding: 15px 19px; color: #304740; background: var(--accent-soft); border-left: 4px solid var(--accent); }}
    .callout p {{ margin: 4px 0; }}
    ul, ol {{ max-width: 100ch; padding-left: 23px; }}
    li {{ margin: 7px 0; padding-left: 3px; }}
    li::marker {{ color: var(--accent); font-weight: 700; }}
    strong {{ color: #10201b; }}
    code {{ padding: 1px 5px; color: #70420d; background: #f3eee5; border-radius: 4px; font: .9em/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .code-block {{ overflow-x: auto; margin: 17px 0 27px; padding: 17px 19px; color: #e6f0ec; background: var(--code); border: 1px solid #354541; border-radius: 4px; font: 13px/1.62 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; white-space: pre; }}
    .code-block code {{ padding: 0; color: inherit; background: none; border-radius: 0; font: inherit; }}
    .table-wrap {{ overflow: auto; max-height: 72dvh; margin: 18px 0 28px; border-top: 1px solid var(--line-strong); border-bottom: 1px solid var(--line-strong); scrollbar-color: var(--line-strong) transparent; }}
    table {{ width: 100%; min-width: 760px; border-collapse: separate; border-spacing: 0; font-size: 13px; line-height: 1.55; }}
    th {{ position: sticky; top: 0; z-index: 2; color: #f4faf7; background: #263633; font-weight: 650; text-align: left; white-space: nowrap; }}
    th, td {{ padding: 11px 12px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    tbody tr:last-child td {{ border-bottom: 0; }}
    tbody tr:nth-child(even) {{ background: var(--soft); }}
    tbody tr:hover {{ background: var(--accent-soft); }}
    .diagram {{ margin: 21px 0 30px; padding: 17px 18px; background: #fbfdfc; border: 1px solid var(--line-strong); border-left: 4px solid var(--accent); border-radius: 4px; }}
    .mermaid {{ overflow-x: auto; min-height: 34px; color: #253e39; white-space: pre; font: 13px/1.55 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .diagram-rendered .mermaid {{ white-space: normal; }}
    .diagram-rendered .diagram-source {{ margin-top: 9px; }}
    .diagram-source {{ color: var(--muted); font-size: 12px; }}
    .diagram-source summary {{ cursor: pointer; }}
    .diagram-source pre {{ overflow-x: auto; margin: 9px 0 0; padding: 12px; color: #33433f; background: #f1f5f3; border-radius: 3px; white-space: pre-wrap; }}
    .section-rule {{ margin: 34px 0; border: 0; border-top: 1px solid var(--line); }}
    .footer {{ margin-top: 42px; padding-top: 17px; border-top: 1px solid var(--line); color: var(--muted); font-size: 12px; }}
    @media (max-width: 900px) {{
      .masthead-inner {{ padding: 29px 20px 24px; }}
      .content-shell {{ padding: 0 20px 56px; }}
      .layout {{ display: block; }}
      .toc {{ position: static; display: flex; gap: 0 17px; max-height: none; overflow-x: auto; padding: 15px 0; border-right: 0; border-bottom: 1px solid var(--line); white-space: nowrap; }}
      .toc strong {{ position: sticky; left: 0; flex: 0 0 auto; margin: 0; padding-right: 8px; background: var(--canvas); }}
      .toc a {{ flex: 0 0 auto; padding: 0; border: 0; }}
      main {{ padding-top: 8px; }}
      h2 {{ margin-top: 40px; font-size: 23px; }}
      .document-meta {{ grid-template-columns: 1fr; padding: 14px 16px; }}
      .table-wrap {{ max-height: 68dvh; margin-right: -20px; }}
      th:first-child, td:first-child {{ position: sticky; left: 0; z-index: 1; box-shadow: 1px 0 0 var(--line-strong); }}
      th:first-child {{ z-index: 3; background: #263633; }}
      tbody tr:nth-child(odd) td:first-child {{ background: var(--paper); }}
      tbody tr:nth-child(even) td:first-child {{ background: var(--soft); }}
    }}
    @media print {{
      body {{ background: white; }}
      .masthead {{ color: var(--ink); background: white; border-bottom-color: var(--ink); }}
      .subtitle, .eyebrow, .meta-chips span {{ color: var(--muted); }}
      .toc {{ display: none; }}
      .layout {{ display: block; }}
      .content-shell {{ width: auto; padding: 0; }}
      .table-wrap {{ max-height: none; overflow: visible; break-inside: avoid; }}
      .code-block, .diagram {{ break-inside: avoid; }}
      .mermaid {{ white-space: pre-wrap; }}
    }}
  </style>
</head>
<body>
  <header class="masthead">
    <div class="masthead-inner">
      <p class="eyebrow">内部架构设计 · PM Loop</p>
      <h1>{html.escape(title)}</h1>
      <p class="subtitle">PM 系统、OpenViking 数据集成与概念学习链路的重构方案及迁移计划</p>
      <div class="meta-chips">{meta_chips}</div>
    </div>
  </header>
  <div class="content-shell">
    <div class="layout">
      <nav class="toc" aria-label="文档目录"><strong>章节目录</strong>{nav}</nav>
      <main id="document-content">{body_html}<div class="footer">本页面为内部方案草案，状态和指标以源 Markdown 及后续决策记录为准。</div></main>
    </div>
  </div>
  {mermaid_loader}
</body>
</html>
'''


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: markdown_to_architecture_html.py input.md output.html", file=sys.stderr)
        return 2
    source, target = map(Path, sys.argv[1:])
    target.write_text(render(source.read_text(encoding="utf-8")), encoding="utf-8")
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
