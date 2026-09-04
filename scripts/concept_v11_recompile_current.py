#!/usr/bin/env python3
"""Recompile concept pages from the current mapped/substituted evidence.

The source coverage report is the admission boundary.  This compiler only
rewrites cards that are refreshable and still contain historical references;
the historical URIs remain visible in a dedicated audit section and never
appear in the current definition/capability sections.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import yaml


COVERAGE_SCHEMA = "concept-v11.source-coverage-report.v1"
PREFLIGHT_SCHEMA = "concept-v11.content-source-preflight.v1"
RECOMPILE_SCHEMA = "concept-v11.current-source-recompile.v2"
DEFAULT_COVERAGE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_PREFLIGHT = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "content-source-preflight-current.json"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
DEFAULT_EVIDENCE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "current-source-evidence-v2.json"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "page-rebuild-backups"
OV_REST = Path.home() / ".codex" / "skills" / "openviking-rest" / "scripts" / "ov_rest.py"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text_hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _load_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"json_not_object:{path}")
    return value


def _coverage_record(coverage: Mapping[str, Any], concept: str) -> Mapping[str, Any]:
    rows = coverage.get("concepts")
    if not isinstance(rows, list):
        raise ValueError("coverage_concepts_missing")
    row = next((item for item in rows if isinstance(item, Mapping) and str(item.get("concept") or "") == concept), None)
    if not isinstance(row, Mapping):
        raise ValueError(f"coverage_concept_missing:{concept}")
    return row


def _current_sources(record: Mapping[str, Any]) -> tuple[str, ...]:
    refs = record.get("references") if isinstance(record.get("references"), list) else []
    values = {
        str(item.get("source_uri") or "")
        for item in refs
        if isinstance(item, Mapping)
        and str(item.get("disposition") or "") in {"mapped", "substituted"}
        and str(item.get("source_map_status") or "") == "mapped"
        and str(item.get("source_uri") or "")
    }
    if not values:
        raise ValueError("current_sources_missing")
    return tuple(sorted(values))


def _frontmatter(path: Path) -> dict[str, Any]:
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        raise ValueError(f"page_frontmatter_missing:{path.name}")
    closing = content.find("\n---\n", 4)
    if closing < 0:
        raise ValueError(f"page_frontmatter_unclosed:{path.name}")
    metadata = yaml.safe_load(content[4:closing]) or {}
    if not isinstance(metadata, dict):
        raise ValueError(f"page_frontmatter_invalid:{path.name}")
    return metadata


def target_concepts(*, coverage: Mapping[str, Any], preflight: Mapping[str, Any], expected_count: int = 34) -> list[dict[str, Any]]:
    if str(coverage.get("schema") or "") != COVERAGE_SCHEMA or str(coverage.get("status") or "") != "PASS":
        raise ValueError("coverage_not_pass")
    if str(preflight.get("schema") or "") != PREFLIGHT_SCHEMA or str(preflight.get("status") or "") != "PASS":
        raise ValueError("preflight_not_pass")
    rows = preflight.get("concepts")
    if not isinstance(rows, list):
        raise ValueError("preflight_concepts_missing")
    targets = [
        dict(row)
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("content_status") or "") == "ready"
        and isinstance(row.get("historical_source_refs"), list)
        and len(row.get("historical_source_refs") or []) > 0
    ]
    targets.sort(key=lambda row: str(row.get("concept") or ""))
    if len(targets) != expected_count:
        raise ValueError(f"target_count_mismatch:{len(targets)}!={expected_count}")
    for row in targets:
        concept = str(row.get("concept") or "")
        record = _coverage_record(coverage, concept)
        status = str(record.get("coverage_status") or "")
        if status not in {"refreshable", "substituted"}:
            raise ValueError(f"target_not_refreshable:{concept}:{status}")
        row["current_sources"] = list(_current_sources(record))
        row["historical_sources"] = sorted({str(uri) for uri in row.get("historical_source_refs", []) if str(uri)})
    return targets


def _read_source(uri: str, *, limit: int = 16000, timeout: int = 60) -> dict[str, Any]:
    command = [sys.executable, str(OV_REST), "read", uri, "--limit", str(limit)]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"uri": uri, "status": "error", "error": f"{type(exc).__name__}:{exc}", "content": ""}
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        payload = {}
    content = payload.get("result") if isinstance(payload, Mapping) else None
    if completed.returncode != 0 or not isinstance(content, str) or not content.strip():
        error = payload.get("error") if isinstance(payload, Mapping) else None
        return {"uri": uri, "status": "error", "error": str(error or completed.stderr or "read_failed").strip(), "content": ""}
    return {"uri": uri, "status": "ok", "error": None, "content": content, "content_sha256": _text_hash(content)}


def collect_evidence(*, coverage_path: Path, preflight_path: Path, output: Path, expected_count: int = 34, workers: int = 6) -> dict[str, Any]:
    coverage = _load_json(coverage_path)
    preflight = _load_json(preflight_path)
    targets = target_concepts(coverage=coverage, preflight=preflight, expected_count=expected_count)
    uris = sorted({uri for row in targets for uri in row["current_sources"]})
    evidence: dict[str, dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_read_source, uri): uri for uri in uris}
        for future in concurrent.futures.as_completed(futures):
            uri = futures[future]
            try:
                evidence[uri] = future.result()
            except Exception as exc:  # fail closed in the plan stage
                evidence[uri] = {"uri": uri, "status": "error", "error": f"{type(exc).__name__}:{exc}", "content": ""}
    body = {
        "schema": RECOMPILE_SCHEMA + ".evidence.v1",
        "observed_at": _now(),
        "coverage_report_hash": str(coverage.get("report_hash") or ""),
        "coverage_source_manifest_hash": str(coverage.get("source_manifest_hash") or ""),
        "target_count": len(targets),
        "source_count": len(uris),
        "sources": [evidence[uri] for uri in uris],
    }
    result = {**body, "evidence_hash": _hash(body)}
    _write_json(output, result)
    return result


def _clean_excerpt(text: str, limit: int = 420) -> str:
    value = re.sub(r"\s+", " ", text).strip(" |\t")
    if len(value) > limit:
        value = value[: limit - 1].rstrip() + "…"
    return value


def _evidence_points(content: str, *, max_points: int = 6) -> list[str]:
    points: list[str] = []
    for block in re.split(r"\n\s*\n", content):
        cleaned = _clean_excerpt(block)
        if not cleaned or cleaned.startswith("#") or cleaned.startswith("---"):
            continue
        if re.fullmatch(r"(?:\|[^|]+)+\|?", cleaned):
            continue
        if cleaned not in points:
            points.append(cleaned)
        if len(points) >= max_points:
            break
    if not points:
        compact = _clean_excerpt(content)
        if compact:
            points.append(compact)
    return points


def _source_label(uri: str) -> str:
    return uri.rstrip("/").rsplit("/", 1)[-1] or uri


def _render_body(concept: str, metadata: Mapping[str, Any], current_sources: list[str], historical_sources: list[str], evidence: Mapping[str, Mapping[str, Any]], observed_at: str) -> str:
    category = str(metadata.get("category") or "未分类")
    current_ok = [uri for uri in current_sources if str(evidence.get(uri, {}).get("status") or "") == "ok"]
    lines = [
        f"# {concept}",
        "",
        "<!-- current-source-rebuild:v2; current facts are limited to mapped/substituted evidence -->",
        "",
        "## 定义",
        f"本概念属于“{category}”。本页于 {observed_at} 按 V11 当前来源重编译；当前定义和能力边界只引用 coverage 中已验证的 `mapped/substituted` 来源，不继承历史资料中的能力断言。",
        "",
        "## 能力边界（能做什么）",
        "以下是当前来源正文的可回读事实摘录。摘录用于说明当前可验证范围，不对来源未明确写出的能力作推断：",
    ]
    for uri in current_sources:
        item = evidence.get(uri, {})
        lines.extend(["", f"### {_source_label(uri)}", f"来源：`{uri}`"])
        if str(item.get("status") or "") != "ok":
            lines.append(f"- 当前来源读取失败，不能据此做能力承诺：{item.get('error') or 'unknown_error'}")
            continue
        for point in _evidence_points(str(item.get("content") or "")):
            lines.append(f"- {point}")
    lines.extend([
        "",
        "## 已知限制（不能做什么/需定制）",
        "- 当前卡只承诺上面列出的当前来源明确内容；来源没有说明的接口、版本、引擎、权限、性能或交付范围，均需在目标版本和环境中单独确认。",
        "- 当前来源读取失败的条目不构成能力证据；发布前必须补齐读取证据。" if len(current_ok) != len(current_sources) else "- 当前来源均已成功回读；仍需结合目标版本、部署形态和权限配置完成交付验收。",
        "",
        "## 版本演进",
        f"- {observed_at}：按 coverage `{str(metadata.get('latest_version') or 'legacy')}` 的当前 mapped/substituted 来源完成正文重编译。历史版本和历史处置不在本节作为当前能力依据。",
        "",
        "## 关联概念",
    ])
    related = metadata.get("related_concepts") if isinstance(metadata.get("related_concepts"), list) else []
    lines.extend([f"- {item}" for item in related] or ["- 暂无已登记关联概念"])
    lines.extend(["", "## 出现过的客户/评估", "以下信息仅保留作为历史使用线索，不构成当前能力证据："])
    customers = metadata.get("related_customers") if isinstance(metadata.get("related_customers"), list) else []
    lines.extend([f"- {item}" for item in customers] or ["- 本轮未从页面元数据读取到客户记录"])
    lines.extend([
        "",
        "## 历史资料边界（不作为当前能力依据）",
        "以下 URI 保留用于审计和回滚边界；其中的旧能力描述、历史引用和客户评估不得直接支撑本页当前定义或交付承诺：",
    ])
    lines.extend([f"- `{uri}`" for uri in historical_sources] or ["- 本卡无历史 URI"])
    lines.extend([
        "",
        "## 证据与待确认点",
        f"- 当前来源回读成功 {len(current_ok)}/{len(current_sources)} 条；source evidence hash：`{_hash({uri: evidence.get(uri, {}).get('content_sha256') for uri in current_sources})}`。",
        "- 本页不删除 coverage、source-map、版本、Hot/Publish 或历史审计记录；这些记录仍由 V11 控制面管理。",
    ])
    return "\n".join(lines).rstrip() + "\n"


def build_plan(*, coverage_path: Path, preflight_path: Path, evidence_path: Path, concept_root: Path, observed_at: Optional[str] = None, expected_count: int = 34) -> dict[str, Any]:
    coverage = _load_json(coverage_path)
    preflight = _load_json(preflight_path)
    evidence_doc = _load_json(evidence_path)
    targets = target_concepts(coverage=coverage, preflight=preflight, expected_count=expected_count)
    evidence_rows = evidence_doc.get("sources") if isinstance(evidence_doc.get("sources"), list) else []
    evidence = {str(row.get("uri")): row for row in evidence_rows if isinstance(row, Mapping) and str(row.get("uri") or "")}
    errors: list[str] = []
    changes: list[dict[str, Any]] = []
    timestamp = observed_at or _now()
    if str(evidence_doc.get("schema") or "") != RECOMPILE_SCHEMA + ".evidence.v1":
        errors.append("evidence_schema_invalid")
    if str(evidence_doc.get("coverage_report_hash") or "") != str(coverage.get("report_hash") or ""):
        errors.append("evidence_coverage_hash_drift")
    for row in targets:
        concept = str(row.get("concept") or "")
        page = concept_root / "state" / "pages" / f"{concept}.md"
        try:
            if not page.is_file():
                raise ValueError("page_missing")
            metadata = _frontmatter(page)
            if str(metadata.get("concept") or "") != concept:
                raise ValueError("page_concept_mismatch")
            missing = [uri for uri in row["current_sources"] if str(evidence.get(uri, {}).get("status") or "") != "ok"]
            if missing:
                raise ValueError("source_read_failed:" + ",".join(missing))
            new_metadata = dict(metadata)
            new_metadata["sources"] = list(row["current_sources"])
            new_metadata["last_updated"] = timestamp
            new_metadata["latest_version"] = "current-source-rebuild-v2"
            header = yaml.safe_dump(new_metadata, allow_unicode=True, sort_keys=False, width=160).strip()
            body = _render_body(concept, new_metadata, row["current_sources"], row["historical_sources"], evidence, timestamp)
            rebuilt = f"---\n{header}\n---\n\n{body}"
            changes.append({
                "concept": concept,
                "page_path": str(page),
                "before_sha256": _file_hash(page),
                "after_sha256": _text_hash(rebuilt),
                "current_sources": row["current_sources"],
                "historical_sources": row["historical_sources"],
                "content": rebuilt,
            })
        except (OSError, ValueError, yaml.YAMLError) as exc:
            errors.append(f"{concept}:{type(exc).__name__}:{exc}")
    body = {
        "schema": RECOMPILE_SCHEMA,
        "status": "PASS" if not errors and len(changes) == expected_count else "HOLD",
        "read_only": True,
        "external_calls": {"oneapi": 0, "openviking": 0},
        "observed_at": timestamp,
        "coverage_report_hash": str(coverage.get("report_hash") or ""),
        "coverage_source_manifest_hash": str(coverage.get("source_manifest_hash") or ""),
        "evidence_hash": str(evidence_doc.get("evidence_hash") or ""),
        "concept_root": str(concept_root),
        "target_count": expected_count,
        "change_count": len(changes),
        "changes": changes,
        "errors": sorted(set(errors)),
    }
    return {**body, "plan_hash": _hash(body)}


def apply_plan(plan: Mapping[str, Any], *, backup_root: Path) -> dict[str, Any]:
    if str(plan.get("status") or "") != "PASS":
        return {"status": "HOLD", "applied": False, "errors": ["plan_not_pass"]}
    changes = plan.get("changes") if isinstance(plan.get("changes"), list) else []
    validated: list[tuple[Path, str]] = []
    for change in changes:
        if not isinstance(change, Mapping):
            return {"status": "HOLD", "applied": False, "errors": ["change_invalid"]}
        page = Path(str(change.get("page_path") or ""))
        if not page.is_file() or _file_hash(page) != str(change.get("before_sha256") or ""):
            return {"status": "HOLD", "applied": False, "errors": [f"page_changed_since_plan:{page.name}"]}
        validated.append((page, str(change.get("content") or "")))
    backup_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + str(plan.get("plan_hash") or "").split(":")[-1][:12]
    backup_dir = backup_root / backup_id
    if backup_dir.exists():
        return {"status": "HOLD", "applied": False, "errors": [f"backup_id_exists:{backup_id}"]}
    backup_dir.mkdir(parents=True, exist_ok=False)
    try:
        for page, _ in validated:
            shutil.copy2(page, backup_dir / page.name)
        _write_json(backup_dir / "manifest.json", {
            "schema": RECOMPILE_SCHEMA + ".backup.v1",
            "plan_hash": plan.get("plan_hash"),
            "observed_at": plan.get("observed_at"),
            "pages": [{"name": page.name, "sha256": _file_hash(page)} for page, _ in validated],
        })
    except OSError as exc:
        return {"status": "HOLD", "applied": False, "backup_dir": str(backup_dir), "errors": [f"backup_failed:{type(exc).__name__}"]}
    written: list[str] = []
    try:
        for page, content in validated:
            temporary = page.with_name(f".{page.name}.current-source-rebuild-v2.tmp")
            temporary.write_text(content, encoding="utf-8")
            os.replace(temporary, page)
            written.append(str(page))
    except OSError as exc:
        restored: list[str] = []
        for written_path in written:
            page = Path(written_path)
            temporary = page.with_name(f".{page.name}.current-source-rollback.tmp")
            try:
                shutil.copy2(backup_dir / page.name, temporary)
                os.replace(temporary, page)
                restored.append(written_path)
            except OSError:
                pass
        return {"status": "HOLD", "applied": False, "backup_dir": str(backup_dir), "written": written, "restored": restored, "errors": [f"write_failed:{type(exc).__name__}"]}
    return {"status": "PASS", "applied": True, "backup_dir": str(backup_dir), "written": written, "errors": []}


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--preflight", type=Path, default=DEFAULT_PREFLIGHT)
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--expected-count", type=int, default=34)
    parser.add_argument("--observed-at")
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.collect:
            collect_evidence(
                coverage_path=args.coverage.expanduser().resolve(),
                preflight_path=args.preflight.expanduser().resolve(),
                output=args.evidence.expanduser().resolve(),
                expected_count=args.expected_count,
                workers=args.workers,
            )
        plan = build_plan(
            coverage_path=args.coverage.expanduser().resolve(),
            preflight_path=args.preflight.expanduser().resolve(),
            evidence_path=args.evidence.expanduser().resolve(),
            concept_root=args.concept_root.expanduser().resolve(),
            observed_at=args.observed_at,
            expected_count=args.expected_count,
        )
        result = {key: value for key, value in plan.items() if key != "changes"}
        result["collect"] = bool(args.collect)
        result["apply"] = bool(args.apply)
        result["apply_result"] = apply_plan(plan, backup_root=args.backup_root.expanduser().resolve()) if args.apply else {"status": "DRY_RUN", "applied": False, "errors": []}
        if args.apply and result["apply_result"].get("status") != "PASS":
            result["status"] = "HOLD"
    except Exception as exc:
        result = {"schema": RECOMPILE_SCHEMA, "status": "HOLD", "collect": bool(args.collect), "apply": bool(args.apply), "errors": [f"{type(exc).__name__}:{exc}"]}
    _write_json(args.report.expanduser().resolve(), result)
    print(json.dumps({key: result.get(key) for key in ("schema", "status", "target_count", "change_count", "errors", "plan_hash", "apply_result")}, ensure_ascii=False))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
