#!/usr/bin/env python3
"""Discover review-only C7 replacement candidates from current source evidence.

Candidates are limited to URI entries that the supplied current source manifest
already maps uniquely.  A candidate still is not a disposition: this script
never updates a source map, coverage ledger, database, or OpenViking resource.
It only records searchable, independently readable alternatives for a human
reviewer to accept or reject in the append-only P3 ledger.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional


SOURCE_MANIFEST_SCHEMA = "concept-source-manifest.v1"
COVERAGE_REPORT_SCHEMA = "concept-v11.source-coverage-report.v1"
REPORT_SCHEMA = "concept-v11.source-candidate-discovery.v1"
DEFAULT_OV_REST = Path.home() / ".codex" / "skills" / "openviking-rest" / "scripts" / "ov_rest.py"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON evidence: {path}: {exc}") from exc


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _run_json(command: list[str], *, timeout: int) -> Mapping[str, Any]:
    completed = subprocess.run(command, capture_output=True, text=True, timeout=max(1, timeout), check=False)
    raw = (completed.stdout or "").strip()
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or raw or f"exit:{completed.returncode}").strip()[:500])
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OpenViking response is not JSON") from exc
    if not isinstance(value, Mapping) or str(value.get("status") or "") != "ok":
        raise RuntimeError("OpenViking response is not ok")
    return value


def _manifest_index(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if str(manifest.get("schema_version") or "") != SOURCE_MANIFEST_SCHEMA:
        raise RuntimeError("unsupported source manifest schema")
    rows = manifest.get("document_mappings")
    if not isinstance(rows, list):
        raise RuntimeError("source manifest document mappings are missing")
    index: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        uri = str(row.get("uri") or "")
        if uri and str(row.get("status") or "") == "mapped" and str(row.get("source_id") or ""):
            index[uri] = row
    return index


def _concepts_needing_repair(coverage: Mapping[str, Any]) -> list[str]:
    if str(coverage.get("schema") or "") != COVERAGE_REPORT_SCHEMA:
        raise RuntimeError("unsupported source coverage report schema")
    rows = coverage.get("concepts")
    if not isinstance(rows, list):
        raise RuntimeError("source coverage concepts are missing")
    return sorted(
        str(row.get("concept") or "")
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("coverage_status") or "") == "needs_repair"
        and str(row.get("concept") or "")
    )


def _resource_uris(response: Mapping[str, Any]) -> list[str]:
    result = response.get("result")
    resources = result.get("resources") if isinstance(result, Mapping) else None
    if not isinstance(resources, list):
        return []
    return sorted(
        {
            str(row.get("uri") or "")
            for row in resources
            if isinstance(row, Mapping) and str(row.get("uri") or "")
        }
    )


def _lexical_current_uris(concept: str, index: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """Find exact concept-name occurrences only; synonyms need human input."""
    return sorted(uri for uri in index if concept in uri)


def _readback(uri: str, response: Mapping[str, Any]) -> dict[str, Any]:
    body = response.get("result")
    if isinstance(body, str):
        content = body
    elif body is None:
        return {"status": "failed", "error": "empty_content"}
    else:
        content = _canonical(body)
    return {
        "status": "verified",
        "bytes": len(content.encode("utf-8")),
        "content_sha256": "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


def _directory_error(response: Mapping[str, Any]) -> bool:
    error = response.get("error")
    message = str(error.get("message") or "") if isinstance(error, Mapping) else ""
    return "Directory URI is not readable as a file" in message


def build_report(
    *,
    source_manifest: Mapping[str, Any],
    coverage_report: Mapping[str, Any],
    search: Callable[[str], Mapping[str, Any]],
    read: Callable[[str], Mapping[str, Any]],
    glob: Optional[Callable[[str], list[str]]] = None,
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    index = _manifest_index(source_manifest)
    concepts = _concepts_needing_repair(coverage_report)
    candidates: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for concept in concepts:
        origins: dict[str, set[str]] = {uri: {"lexical"} for uri in _lexical_current_uris(concept, index)}
        try:
            found = _resource_uris(search(concept))
        except Exception as exc:
            failures.append({"concept": concept, "stage": "find", "error": f"{type(exc).__name__}:{exc}"})
            found = []
        # Exact current-ledger identity is a prerequisite; search similarity is
        # not a source-map match and remains intentionally excluded.
        for uri in found:
            if uri in index:
                origins.setdefault(uri, set()).add("semantic")
        for uri in sorted(origins):
            mapping = index[uri]
            leaf_uris = [uri]
            direct_response: Optional[Mapping[str, Any]] = None
            try:
                direct_response = read(uri)
                if _directory_error(direct_response) and glob is not None:
                    leaf_uris = sorted(set(glob(uri)))
            except Exception as exc:
                if glob is not None:
                    try:
                        leaf_uris = sorted(set(glob(uri)))
                    except Exception:
                        leaf_uris = [uri]
            if not leaf_uris:
                leaf_uris = [uri]
            for leaf_uri in leaf_uris:
                if leaf_uri == uri and direct_response is not None and str(direct_response.get("status") or "") == "ok":
                    readback = _readback(leaf_uri, direct_response)
                else:
                    try:
                        readback = _readback(leaf_uri, read(leaf_uri))
                    except Exception as exc:
                        readback = {"status": "failed", "error": f"{type(exc).__name__}:{exc}"}
                candidates.append(
                    {
                        "concept": concept,
                        "candidate_uri": leaf_uri,
                        "source_directory_uri": uri if leaf_uri != uri else None,
                        "source_id": str(mapping.get("source_id") or ""),
                        "identity_method": str(mapping.get("match_mode") or ""),
                        "discovery_origin": sorted(origins[uri]),
                        "readback": readback,
                        "qualified_for_human_review": readback.get("status") == "verified",
                        "automatic_disposition": None,
                    }
                )
    candidates.sort(key=lambda row: (str(row["concept"]), str(row["candidate_uri"])))
    report = {
        "schema": REPORT_SCHEMA,
        "generated_at": generated_at or _now(),
        "source_manifest_generated_at": source_manifest.get("generated_at"),
        "source_manifest_document_count": len(index),
        "coverage_report_hash": str(coverage_report.get("report_hash") or ""),
        "concepts_needing_repair": concepts,
        "candidates": candidates,
        "failures": failures,
        "candidate_count": len(candidates),
        "qualified_candidate_count": sum(1 for row in candidates if row["qualified_for_human_review"]),
        "writes": {"database": 0, "openviking": 0, "dispositions": 0},
        "status": "partial" if failures else "completed",
    }
    report["report_hash"] = _hash({key: value for key, value in report.items() if key != "report_hash"})
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--coverage-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ov-rest", type=Path, default=DEFAULT_OV_REST)
    parser.add_argument("--target-uri", default="viking://resources/shengsuan/")
    parser.add_argument("--concept", action="append", default=[], help="restrict discovery to a needs_repair concept; repeatable")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=30)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        source_manifest = _read_json(args.source_manifest.expanduser().resolve())
        coverage_report = _read_json(args.coverage_report.expanduser().resolve())
        ov_rest = args.ov_rest.expanduser().resolve()
        requested = {str(value).strip() for value in args.concept if str(value).strip()}
        if requested:
            available = set(_concepts_needing_repair(coverage_report))
            unknown = sorted(requested - available)
            if unknown:
                raise RuntimeError("requested concepts are not needs_repair: " + ",".join(unknown))
            coverage_report = {
                **coverage_report,
                "concepts": [
                    row for row in coverage_report.get("concepts") or []
                    if isinstance(row, Mapping) and str(row.get("concept") or "") in requested
                ],
            }

        def search(concept: str) -> Mapping[str, Any]:
            return _run_json(
                [sys.executable, str(ov_rest), "find", concept, "--target-uri", str(args.target_uri), "--limit", str(args.limit)],
                timeout=args.timeout,
            )

        def read(uri: str) -> Mapping[str, Any]:
            return _run_json([sys.executable, str(ov_rest), "read", uri, "--limit", "-1"], timeout=args.timeout)

        def glob(uri: str) -> list[str]:
            response = _run_json([sys.executable, str(ov_rest), "glob", "*", "--uri", uri, "--node-limit", str(args.limit)], timeout=args.timeout)
            result = response.get("result")
            matches = result.get("matches") if isinstance(result, Mapping) else None
            return [str(value) for value in matches or [] if str(value)]

        report = build_report(source_manifest=source_manifest, coverage_report=coverage_report, search=search, read=read, glob=glob)
        _write_json(args.output.expanduser().resolve(), report)
    except Exception as exc:
        print(json.dumps({"schema": REPORT_SCHEMA, "status": "HOLD", "error": f"{type(exc).__name__}:{exc}"}, ensure_ascii=False))
        return 1
    print(json.dumps({key: report[key] for key in ("schema", "status", "candidate_count", "qualified_candidate_count", "failures", "report_hash")}, ensure_ascii=False))
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
