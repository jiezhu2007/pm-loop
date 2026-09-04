#!/usr/bin/env python3
"""Check that Active concept pages use the current P3 source disposition.

P3 source coverage closes the evidence ledger, but it does not alter the
installed Markdown pages.  This read-only preflight prevents an Admission
from treating that ledger result as a content migration: a refreshable page
must cite at least one current ``mapped``/``substituted`` source.
``historical_exclusion`` references remain visible as audit information: they
cannot qualify a page for refresh, but preserving a historical citation does
not by itself make the page false.  A retired concept is deliberately
excluded from this page-refresh requirement because its tombstone, rather
than a new page body, is the governing evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


REPORT_SCHEMA = "concept-v11.content-source-preflight.v1"
COVERAGE_SCHEMA = "concept-v11.source-coverage-report.v1"
DEFAULT_COVERAGE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
CURRENT_DISPOSITIONS = {"mapped", "substituted"}
PAGE_URI_RE = re.compile(r"viking://[^\s<>()\[\]{}\"']+")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"coverage_invalid:{path}:{type(exc).__name__}") from exc
    if not isinstance(value, Mapping):
        raise ValueError("coverage_not_object")
    return value


def _page_uris(path: Path, known_uris: Iterable[str] = ()) -> list[str]:
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8")
    # A valid native Viking URI may contain a literal space because source
    # titles are preserved in the path.  First test the exact URI identities
    # supplied by the coverage ledger; the regex fallback still finds older
    # citations which are not in that ledger.
    exact = {str(uri) for uri in known_uris if str(uri) and str(uri) in content}
    # A URI may be followed by Markdown punctuation.  The URI identity itself
    # never includes sentence punctuation, so remove only that terminal form.
    scanned = {match.rstrip(".,;:!?，。；：！？") for match in PAGE_URI_RE.findall(content)}
    return sorted(exact | scanned)


def build_preflight(
    *,
    coverage: Mapping[str, Any],
    concept_root: Path,
    expected_concept_count: int = 45,
    observed_at: Optional[str] = None,
) -> dict[str, Any]:
    errors: list[str] = []
    if str(coverage.get("schema") or "") != COVERAGE_SCHEMA:
        errors.append("coverage_schema_invalid")
    if str(coverage.get("status") or "") != "PASS":
        errors.append("coverage_not_pass")
    concepts = coverage.get("concepts")
    if not isinstance(concepts, list):
        concepts = []
        errors.append("coverage_concepts_missing")
    if len(concepts) != int(expected_concept_count):
        errors.append(f"coverage_concept_count_mismatch:{len(concepts)}!={int(expected_concept_count)}")

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for raw in sorted((item for item in concepts if isinstance(item, Mapping)), key=lambda item: str(item.get("concept") or "")):
        concept = str(raw.get("concept") or "").strip()
        coverage_status = str(raw.get("coverage_status") or "")
        if not concept or concept in seen:
            errors.append("coverage_concept_identity_invalid")
            continue
        seen.add(concept)
        refs = raw.get("references") if isinstance(raw.get("references"), list) else []
        current = {
            str(item.get("source_uri") or "")
            for item in refs
            if isinstance(item, Mapping) and str(item.get("disposition") or "") in CURRENT_DISPOSITIONS
        }
        historical = {
            str(item.get("source_uri") or "")
            for item in refs
            if isinstance(item, Mapping) and str(item.get("disposition") or "") == "historical_exclusion"
        }
        current.discard("")
        historical.discard("")
        page = concept_root / "state" / "pages" / f"{concept}.md"
        page_uris = set(_page_uris(page, current | historical))
        row_errors: list[str] = []
        if coverage_status == "retired_with_evidence":
            content_status = "retired_excluded"
        elif coverage_status not in {"refreshable", "substituted"}:
            content_status = "blocked"
            row_errors.append(f"coverage_not_refreshable:{coverage_status or 'missing'}")
        elif not page.is_file():
            content_status = "needs_source_rebuild"
            row_errors.append("page_missing")
        else:
            historical_hits = sorted(page_uris & historical)
            current_hits = sorted(page_uris & current)
            if not current_hits:
                row_errors.append("current_source_not_referenced")
            content_status = "ready" if not row_errors else "needs_source_rebuild"
        rows.append(
            {
                "concept": concept,
                "concept_id": str(raw.get("concept_id") or ""),
                "coverage_status": coverage_status,
                "content_status": content_status,
                "page_path": str(page),
                "page_sha256": _file_hash(page) if page.is_file() else None,
                "current_source_count": len(current),
                "current_source_refs": sorted(page_uris & current),
                "historical_source_refs": sorted(page_uris & historical),
                "errors": row_errors,
            }
        )
    status_counts = {
        name: sum(1 for row in rows if row["content_status"] == name)
        for name in ("ready", "needs_source_rebuild", "retired_excluded", "blocked")
    }
    if len(rows) != int(expected_concept_count):
        errors.append(f"preflight_concept_count_mismatch:{len(rows)}!={int(expected_concept_count)}")
    blocking = [row["concept"] for row in rows if row["content_status"] in {"needs_source_rebuild", "blocked"}]
    if blocking:
        errors.append("content_source_rebuild_required:" + ",".join(blocking))
    body = {
        "schema": REPORT_SCHEMA,
        "status": "PASS" if not errors else "HOLD",
        "read_only": True,
        "external_calls": {"oneapi": 0, "openviking": 0},
        "observed_at": observed_at or _now(),
        "coverage_report_hash": str(coverage.get("report_hash") or ""),
        "coverage_source_manifest_hash": str(coverage.get("source_manifest_hash") or ""),
        "expected_concept_count": int(expected_concept_count),
        "concept_root": str(concept_root),
        "summary": {**status_counts, "blocking_concepts": blocking},
        "concepts": rows,
        "errors": sorted(set(errors)),
    }
    return {**body, "report_hash": _hash(body)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--expected-concept-count", type=int, default=45)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = build_preflight(
            coverage=_read_json(args.coverage.expanduser().resolve()),
            concept_root=args.concept_root.expanduser().resolve(),
            expected_concept_count=args.expected_concept_count,
        )
    except Exception as exc:
        report = {
            "schema": REPORT_SCHEMA,
            "status": "HOLD",
            "read_only": True,
            "external_calls": {"oneapi": 0, "openviking": 0},
            "errors": [f"{type(exc).__name__}:{exc}"],
        }
    _write_json(args.report.expanduser().resolve(), report)
    print(json.dumps({key: report.get(key) for key in ("schema", "status", "summary", "errors", "report_hash")}, ensure_ascii=False))
    return 0 if report.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
