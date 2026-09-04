#!/usr/bin/env python3
"""Build a human-review worksheet for P3 quarantined concept references.

The output is deliberately *not* a source-coverage disposition ledger.  It
contains no accepted ledger schema or prefilled decision, so it cannot be
mistaken for evidence by ``concept_v11_source_coverage.py``.  A reviewer must
first make an explicit decision, then create a separately validated append-only
JSONL ledger entry for each accepted disposition.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


CLOSURE_SCHEMA = "concept-v11.c7-source-map-evidence.v1"
COVERAGE_SCHEMA = "concept-v11.source-coverage-report.v1"
CANDIDATE_SCHEMA = "concept-v11.source-candidate-discovery.v1"
PACKAGE_SCHEMA = "concept-v11.p3-review-package.v1"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON: {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise RuntimeError(f"JSON root must be an object: {path}")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def _qualified_candidates(candidates: Mapping[str, Any]) -> dict[str, list[dict[str, str]]]:
    if str(candidates.get("schema") or "") != CANDIDATE_SCHEMA:
        raise RuntimeError("unsupported candidate discovery report")
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in candidates.get("candidates") or []:
        if not isinstance(row, Mapping) or not row.get("qualified_for_human_review"):
            continue
        readback = row.get("readback") if isinstance(row.get("readback"), Mapping) else {}
        concept = str(row.get("concept") or "")
        uri = str(row.get("candidate_uri") or "")
        content_hash = str(readback.get("content_sha256") or "")
        if concept and uri and content_hash.startswith("sha256:"):
            grouped.setdefault(concept, []).append(
                {
                    "candidate_uri": uri,
                    "source_id": str(row.get("source_id") or ""),
                    "content_sha256": content_hash,
                }
            )
    for concept in grouped:
        grouped[concept].sort(key=lambda item: (not item["source_id"].startswith("public-docs:"), item["candidate_uri"]))
    return grouped


def build_package(
    *,
    closure: Mapping[str, Any],
    coverage: Mapping[str, Any],
    candidates: Mapping[str, Any],
    expected_concept_count: int = 45,
    expected_quarantine_count: int = 281,
) -> dict[str, Any]:
    if str(closure.get("schema") or "") != CLOSURE_SCHEMA:
        raise RuntimeError("unsupported C7 closure evidence")
    if str(coverage.get("schema") or "") != COVERAGE_SCHEMA:
        raise RuntimeError("unsupported coverage report")
    closure_hash = str(closure.get("closure_hash") or "")
    manifest_hash = str((closure.get("source_manifest") or {}).get("sha256") or "")
    if not closure_hash.startswith("sha256:") or not manifest_hash.startswith("sha256:"):
        raise RuntimeError("closure lacks immutable evidence hashes")
    if str(coverage.get("closure_hash") or "") != closure_hash:
        raise RuntimeError("coverage closure hash mismatch")
    if str(coverage.get("source_manifest_hash") or "") != manifest_hash:
        raise RuntimeError("coverage source manifest hash mismatch")
    if str(candidates.get("coverage_report_hash") or "") != str(coverage.get("report_hash") or ""):
        raise RuntimeError("candidate discovery coverage report hash mismatch")

    coverage_rows = coverage.get("concepts")
    if not isinstance(coverage_rows, list) or len(coverage_rows) != expected_concept_count:
        raise RuntimeError("coverage concept count mismatch")
    coverage_by_concept = {
        str(row.get("concept") or ""): row
        for row in coverage_rows
        if isinstance(row, Mapping) and str(row.get("concept") or "")
    }
    if len(coverage_by_concept) != expected_concept_count:
        raise RuntimeError("coverage concepts are not unique")

    quarantined = sorted(
        (
            row
            for row in closure.get("rows") or []
            if isinstance(row, Mapping) and str(row.get("status") or "") == "quarantined"
        ),
        key=lambda row: (str(row.get("concept") or ""), str(row.get("map_id") or "")),
    )
    if len(quarantined) != expected_quarantine_count:
        raise RuntimeError(f"quarantine count mismatch: {len(quarantined)} != {expected_quarantine_count}")
    candidates_by_concept = _qualified_candidates(candidates)

    worksheet_rows: list[dict[str, Any]] = []
    for index, row in enumerate(quarantined, start=1):
        concept = str(row.get("concept") or "")
        coverage_row = coverage_by_concept.get(concept)
        if coverage_row is None:
            raise RuntimeError(f"closure concept missing from coverage: {concept}")
        concept_candidates = candidates_by_concept.get(concept, [])
        worksheet_rows.append(
            {
                "review_row": index,
                "map_id": str(row.get("map_id") or ""),
                "concept": concept,
                "source_uri": str(row.get("source_uri") or ""),
                "c7_reason": str(row.get("resolution_reason") or ""),
                "c7_evidence_set_hash": str(row.get("evidence_set_hash") or ""),
                "coverage_status_before_review": str(coverage_row.get("coverage_status") or ""),
                "qualified_candidate_count": len(concept_candidates),
                "qualified_candidates": concept_candidates,
                "allowed_decisions": "substituted|retired_with_evidence|historical_exclusion|needs_repair",
                "review_decision": "",
                "review_evidence_uri": "",
                "review_evidence_sha256": "",
                "review_notes": "",
                "reviewer": "",
                "reviewed_at": "",
            }
        )
    no_mapped_concepts = sorted(
        str(row.get("concept") or "")
        for row in coverage_rows
        if isinstance(row, Mapping) and not int((row.get("disposition_counts") or {}).get("mapped") or 0)
    )
    package = {
        "schema": PACKAGE_SCHEMA,
        "artifact_kind": "human_review_worksheet_not_ledger",
        "write_authority": {"source_map": False, "coverage_ledger": False, "admission": False, "openviking": False},
        "inputs": {
            "closure_hash": closure_hash,
            "source_manifest_hash": manifest_hash,
            "coverage_report_hash": str(coverage.get("report_hash") or ""),
            "candidate_report_hash": str(candidates.get("report_hash") or ""),
        },
        "summary": {
            "concept_count": expected_concept_count,
            "quarantine_count": len(worksheet_rows),
            "coverage_status_counts": dict(coverage.get("concept_status_counts") or {}),
            "no_mapped_concepts": no_mapped_concepts,
            "qualified_candidate_count": sum(len(items) for items in candidates_by_concept.values()),
            "candidate_concepts": sorted(candidates_by_concept),
        },
        "worksheet_rows": worksheet_rows,
    }
    package["package_hash"] = _hash({key: value for key, value in package.items() if key != "package_hash"})
    return package


def render_markdown(package: Mapping[str, Any]) -> str:
    summary = package["summary"]
    inputs = package["inputs"]
    lines = [
        "# 概念自动刷新 P3 来源处置决策工作包",
        "",
        "> 状态：仅供人工审查，不是 `source_coverage/disposition` ledger，不能直接被 C7 或 Planner 消费。",
        "> 生成日期：2026-09-02",
        "",
        "## 当前门禁",
        "",
        f"- 概念总数：{summary['concept_count']}；当前 coverage：`{summary['coverage_status_counts']}`。",
        f"- 待审 quarantine：{summary['quarantine_count']} 条；当前均保持 `needs_repair`。",
        f"- C7 closure：`{inputs['closure_hash']}`。",
        f"- source manifest：`{inputs['source_manifest_hash']}`。",
        f"- coverage report：`{inputs['coverage_report_hash']}`。",
        "- 所有 review decision 字段为空；没有任何默认 `substituted`、`retired_with_evidence` 或 `historical_exclusion`。",
        "",
        "## 决策规则",
        "",
        "1. `substituted` 只能引用同一概念、当前 `mapped` 且正文 hash 一致的替代 leaf。",
        "2. `retired_with_evidence` 必须提供独立 tombstone 的可回读 URI 与正文 hash。",
        "3. `historical_exclusion` 只能排除历史引用，不能为无当前来源的概念建立覆盖。",
        "4. 无法满足上述条件时，选择 `needs_repair`；不要猜测映射。",
        "5. 审核结论确认后，另行写入 append-only JSONL ledger 并用 coverage runner 重算；本工作包本身永不转入生产账本。",
        "",
        "## 无当前 Mapped 来源概念",
        "",
        "| 概念 | 当前 ledger 支持且可回读的候选 | 需要人工决定 |",
        "|---|---|---|",
    ]
    candidates_by_concept: dict[str, list[Mapping[str, str]]] = {}
    for row in package["worksheet_rows"]:
        concept = str(row["concept"])
        candidates_by_concept.setdefault(concept, list(row["qualified_candidates"]))
    for concept in summary["no_mapped_concepts"]:
        candidates = candidates_by_concept.get(concept, [])
        candidate = candidates[0]["candidate_uri"] if candidates else "无"
        decision = "确认唯一替代来源、提供退役证据，或保留 needs_repair"
        lines.append(f"| {concept} | `{candidate}` | {decision} |")
    lines.extend(
        [
            "",
            "## 工作表文件",
            "",
            "- CSV 含全部 281 条引用、C7 原因、证据集 hash、候选、空白审核字段；用于逐条审查。",
            "- JSON 保存机器可验证的输入 hash 和同一批工作表行；用于防止在审核期间混用不同的 C7/coverage 版本。",
            "- 审核完成的退出条件仍是：45/45 coverage `PASS` 且 `needs_repair=0`；否则 Admission 保持 `disabled`。",
            "",
        ]
    )
    return "\n".join(lines)


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "review_row", "map_id", "concept", "source_uri", "c7_reason", "c7_evidence_set_hash",
        "coverage_status_before_review", "qualified_candidate_count", "qualified_candidates",
        "allowed_decisions", "review_decision", "review_evidence_uri", "review_evidence_sha256",
        "review_notes", "reviewer", "reviewed_at",
    ]
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            value = dict(row)
            value["qualified_candidates"] = _canonical(value.get("qualified_candidates") or [])
            writer.writerow({key: value.get(key, "") for key in headers})
    temporary.replace(path)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closure", type=Path, required=True)
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--expected-concept-count", type=int, default=45)
    parser.add_argument("--expected-quarantine-count", type=int, default=281)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        package = build_package(
            closure=_read_json(args.closure.expanduser().resolve()),
            coverage=_read_json(args.coverage.expanduser().resolve()),
            candidates=_read_json(args.candidates.expanduser().resolve()),
            expected_concept_count=args.expected_concept_count,
            expected_quarantine_count=args.expected_quarantine_count,
        )
        _write_json(args.json.expanduser().resolve(), package)
        _write_text(args.markdown.expanduser().resolve(), render_markdown(package))
        write_csv(args.csv.expanduser().resolve(), package["worksheet_rows"])
    except Exception as exc:
        print(json.dumps({"schema": PACKAGE_SCHEMA, "status": "HOLD", "error": f"{type(exc).__name__}:{exc}"}, ensure_ascii=False))
        return 1
    print(json.dumps({"schema": PACKAGE_SCHEMA, "status": "completed", "summary": package["summary"], "package_hash": package["package_hash"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
