#!/usr/bin/env python3
"""Validate append-only dispositions for C7 quarantined concept references.

This runner does not change ``concept_source_map`` or call OpenViking.  It
turns immutable C7 closure evidence plus a human-reviewed JSONL ledger into a
per-reference and per-concept coverage report.  A quarantined reference stays
``needs_repair`` unless its ledger entry contains verifiable replacement or
retirement evidence.  Names and paths alone are never evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


CLOSURE_SCHEMA = "concept-v11.c7-source-map-evidence.v1"
READBACK_SCHEMA = "concept-v11.c7-content-readback.v1"
LEDGER_SCHEMA = "concept-v11.source-coverage-disposition.v1"
CONCEPT_RETIREMENT_SCHEMA = "concept-v11.concept-retirement.v1"
REPORT_SCHEMA = "concept-v11.source-coverage-report.v1"
VALID_DISPOSITIONS = {
    "substituted",
    "retired_with_evidence",
    "historical_exclusion",
    "needs_repair",
}


def _concept_id(name: str) -> str:
    return "concept-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:24]


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON evidence: {path}: {exc}") from exc


def _read_jsonl(path: Optional[Path]) -> list[dict[str, Any]]:
    if path is None or not path.is_file():
        return []
    entries: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid disposition JSONL at {path}:{line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"disposition entry must be an object at {path}:{line_number}")
        entries.append(value)
    return entries


def _validate_concept_retirements(
    closure: Mapping[str, Any],
    entries: Iterable[Mapping[str, Any]],
    supplemental_readback: Optional[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], list[str]]:
    """Validate concept-level retirement evidence without rewriting references.

    A retirement decision is deliberately separate from the per-reference
    disposition ledger.  This lets an already-approved historical exclusion
    remain immutable while the concept itself is removed from the refresh
    population with a verifiable tombstone.
    """
    rows = [row for row in closure.get("rows") or [] if isinstance(row, Mapping)]
    concepts = {str(row.get("concept") or "") for row in rows if str(row.get("concept") or "")}
    verified = _verified_readback_rows(supplemental_readback)
    accepted: dict[str, Mapping[str, Any]] = {}
    errors: list[str] = []
    for index, entry in enumerate(entries):
        prefix = f"retirement[{index}]"
        if str(entry.get("schema") or "") != CONCEPT_RETIREMENT_SCHEMA:
            errors.append(f"{prefix}:schema_invalid")
            continue
        concept = _required_text(entry, "concept", errors, prefix)
        concept_id = _required_text(entry, "concept_id", errors, prefix)
        if concept not in concepts:
            errors.append(f"{prefix}:concept_unknown")
        if concept_id != _concept_id(concept):
            errors.append(f"{prefix}:concept_id_mismatch")
        if concept in accepted:
            errors.append(f"{prefix}:concept_duplicate")
            continue
        if str(entry.get("decision") or "") != "retired_with_evidence":
            errors.append(f"{prefix}:decision_invalid")
        _required_text(entry, "operator", errors, prefix)
        _required_text(entry, "observed_at", errors, prefix)
        retirement_uri = _required_text(entry, "retirement_uri", errors, prefix)
        retirement_hash = _required_text(entry, "retirement_content_sha256", errors, prefix)
        refs = _evidence_refs(entry, errors, prefix)
        verified_row = verified.get(retirement_uri)
        if verified_row is None:
            errors.append(f"{prefix}:retirement_readback_missing")
        elif retirement_hash != str(verified_row.get("content_sha256") or ""):
            errors.append(f"{prefix}:retirement_hash_mismatch")
        if refs and not any(
            str(ref.get("sha256") or ref.get("content_sha256") or "").startswith("sha256:")
            for ref in refs
        ):
            errors.append(f"{prefix}:evidence_hash_missing")
        accepted[concept] = entry
    return accepted, errors


def _verified_readback_rows(readback: Optional[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    if not readback:
        return {}
    if str(readback.get("schema") or "") != READBACK_SCHEMA:
        raise RuntimeError("unsupported supplemental read-back schema")
    rows = readback.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("supplemental read-back rows are missing")
    return {
        str(row.get("uri") or ""): row
        for row in rows
        if isinstance(row, Mapping)
        and row.get("uri")
        and str(row.get("status") or "") == "verified"
        and str(row.get("content_sha256") or "").startswith("sha256:")
    }


def _mapped_content_hash(row: Mapping[str, Any]) -> Optional[str]:
    for ref in row.get("evidence_refs") or []:
        if isinstance(ref, Mapping) and ref.get("kind") == "content_readback":
            value = str(ref.get("content_sha256") or "")
            if value.startswith("sha256:"):
                return value
    return None


def _required_text(entry: Mapping[str, Any], key: str, errors: list[str], prefix: str) -> str:
    value = str(entry.get(key) or "").strip()
    if not value:
        errors.append(f"{prefix}:missing_{key}")
    return value


def _evidence_refs(entry: Mapping[str, Any], errors: list[str], prefix: str) -> list[Mapping[str, Any]]:
    refs = entry.get("evidence_refs")
    if not isinstance(refs, list) or not refs or not all(isinstance(ref, Mapping) for ref in refs):
        errors.append(f"{prefix}:evidence_refs_invalid")
        return []
    return [ref for ref in refs if isinstance(ref, Mapping)]


def _validate_ledger(
    closure: Mapping[str, Any],
    entries: list[Mapping[str, Any]],
    supplemental_readback: Optional[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], list[str]]:
    rows = closure.get("rows")
    if str(closure.get("schema") or "") != CLOSURE_SCHEMA or not isinstance(rows, list):
        raise RuntimeError("unsupported C7 closure evidence")
    closure_hash = str(closure.get("closure_hash") or "")
    if not closure_hash.startswith("sha256:"):
        raise RuntimeError("C7 closure hash is missing")
    by_map_id: dict[str, Mapping[str, Any]] = {}
    by_concept_uri: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise RuntimeError("invalid C7 closure row")
        map_id = str(row.get("map_id") or "")
        concept = str(row.get("concept") or "")
        source_uri = str(row.get("source_uri") or "")
        if not map_id or not concept or not source_uri:
            raise RuntimeError("C7 closure row lacks map_id/concept/source_uri")
        by_map_id[map_id] = row
        by_concept_uri[(concept, source_uri)] = row
    verified = _verified_readback_rows(supplemental_readback)
    accepted: dict[str, Mapping[str, Any]] = {}
    errors: list[str] = []
    seen_entry_ids: set[str] = set()

    for index, entry in enumerate(entries):
        prefix = f"entry[{index}]"
        if str(entry.get("schema") or "") != LEDGER_SCHEMA:
            errors.append(f"{prefix}:schema_invalid")
            continue
        entry_id = _required_text(entry, "entry_id", errors, prefix)
        map_id = _required_text(entry, "map_id", errors, prefix)
        if entry_id in seen_entry_ids:
            errors.append(f"{prefix}:entry_id_duplicate")
        seen_entry_ids.add(entry_id)
        if str(entry.get("closure_hash") or "") != closure_hash:
            errors.append(f"{prefix}:closure_hash_mismatch")
        target = by_map_id.get(map_id)
        if target is None:
            errors.append(f"{prefix}:map_id_unknown")
            continue
        if str(entry.get("concept") or "") != str(target.get("concept") or ""):
            errors.append(f"{prefix}:concept_mismatch")
        if str(entry.get("source_uri") or "") != str(target.get("source_uri") or ""):
            errors.append(f"{prefix}:source_uri_mismatch")
        if str(target.get("status") or "") != "quarantined":
            errors.append(f"{prefix}:target_is_not_quarantined")
        if map_id in accepted:
            errors.append(f"{prefix}:map_id_duplicate")
            continue
        disposition = str(entry.get("disposition") or "")
        if disposition not in VALID_DISPOSITIONS:
            errors.append(f"{prefix}:disposition_invalid")
            continue
        _required_text(entry, "operator", errors, prefix)
        _required_text(entry, "observed_at", errors, prefix)
        refs = _evidence_refs(entry, errors, prefix)
        if disposition == "substituted":
            replacement_uri = _required_text(entry, "replacement_source_uri", errors, prefix)
            replacement_hash = _required_text(entry, "replacement_content_sha256", errors, prefix)
            replacement = by_concept_uri.get((str(target.get("concept") or ""), replacement_uri))
            if replacement is None or str(replacement.get("status") or "") != "mapped":
                errors.append(f"{prefix}:replacement_not_current_mapped_source")
            elif replacement_uri == str(target.get("source_uri") or ""):
                errors.append(f"{prefix}:replacement_must_differ")
            elif replacement_hash != _mapped_content_hash(replacement):
                errors.append(f"{prefix}:replacement_hash_mismatch")
        elif disposition == "retired_with_evidence":
            retirement_uri = _required_text(entry, "retirement_uri", errors, prefix)
            retirement_hash = _required_text(entry, "retirement_content_sha256", errors, prefix)
            verified_row = verified.get(retirement_uri)
            if verified_row is None:
                errors.append(f"{prefix}:retirement_readback_missing")
            elif retirement_hash != str(verified_row.get("content_sha256") or ""):
                errors.append(f"{prefix}:retirement_hash_mismatch")
        elif disposition == "historical_exclusion":
            _required_text(entry, "exclusion_reason", errors, prefix)
        elif disposition == "needs_repair":
            _required_text(entry, "next_action", errors, prefix)
        if refs and not any(
            str(ref.get("sha256") or ref.get("content_sha256") or "").startswith("sha256:")
            for ref in refs
        ):
            errors.append(f"{prefix}:evidence_hash_missing")
        accepted[map_id] = entry
    return accepted, errors


def build_report(
    *,
    closure: Mapping[str, Any],
    dispositions: Iterable[Mapping[str, Any]] = (),
    concept_retirements: Iterable[Mapping[str, Any]] = (),
    supplemental_readback: Optional[Mapping[str, Any]] = None,
    expected_concept_count: Optional[int] = None,
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    entries = list(dispositions)
    accepted, errors = _validate_ledger(closure, entries, supplemental_readback)
    retirement_entries, retirement_errors = _validate_concept_retirements(
        closure, concept_retirements, supplemental_readback
    )
    errors.extend(retirement_errors)
    per_concept: dict[str, list[dict[str, Any]]] = {}
    for raw in closure.get("rows") or []:
        if not isinstance(raw, Mapping):
            continue
        concept = str(raw.get("concept") or "")
        map_id = str(raw.get("map_id") or "")
        source_status = str(raw.get("status") or "")
        entry = accepted.get(map_id)
        disposition = "mapped" if source_status == "mapped" else str((entry or {}).get("disposition") or "needs_repair")
        item = {
            "map_id": map_id,
            "source_uri": str(raw.get("source_uri") or ""),
            "source_map_status": source_status,
            "disposition": disposition,
            "evidence_set_hash": str(raw.get("evidence_set_hash") or ""),
            "ledger_entry_id": str((entry or {}).get("entry_id") or "") or None,
            "reason": str((entry or {}).get("exclusion_reason") or (entry or {}).get("next_action") or raw.get("resolution_reason") or ""),
        }
        per_concept.setdefault(concept, []).append(item)

    concepts: list[dict[str, Any]] = []
    counts: dict[str, int] = {key: 0 for key in ("refreshable", "substituted", "retired_with_evidence", "needs_repair")}
    for concept in sorted(per_concept):
        rows = per_concept[concept]
        dispositions_by_name: dict[str, int] = {}
        for row in rows:
            name = str(row["disposition"])
            dispositions_by_name[name] = dispositions_by_name.get(name, 0) + 1
        names = set(dispositions_by_name)
        retirement = retirement_entries.get(concept)
        has_current = bool(names & {"mapped", "substituted"})
        if retirement is not None and has_current:
            errors.append(f"concept_retirement_conflicts_with_current_source:{concept}")
            coverage = "needs_repair"
        elif retirement is not None and names - {"historical_exclusion"}:
            errors.append(f"concept_retirement_conflicts_with_reference_disposition:{concept}")
            coverage = "needs_repair"
        elif retirement is not None:
            coverage = "retired_with_evidence"
        elif "needs_repair" in names:
            coverage = "needs_repair"
        elif names == {"retired_with_evidence"}:
            coverage = "retired_with_evidence"
        elif "substituted" in names:
            coverage = "substituted"
        elif has_current:
            coverage = "refreshable"
        else:
            coverage = "needs_repair"
        counts[coverage] = counts.get(coverage, 0) + 1
        concept_item = {
                "concept": concept,
                "concept_id": _concept_id(concept),
                "coverage_status": coverage,
                "reference_count": len(rows),
                "disposition_counts": dict(sorted(dispositions_by_name.items())),
                "references": rows,
            }
        if retirement is not None:
            concept_item["retirement"] = {
                "decision": str(retirement.get("decision") or ""),
                "retirement_uri": str(retirement.get("retirement_uri") or ""),
                "retirement_content_sha256": str(retirement.get("retirement_content_sha256") or ""),
                "evidence_refs": list(retirement.get("evidence_refs") or []),
                "operator": str(retirement.get("operator") or ""),
                "observed_at": str(retirement.get("observed_at") or ""),
            }
        concepts.append(concept_item)
    if expected_concept_count is not None and len(concepts) != int(expected_concept_count):
        errors.append(f"concept_count_mismatch:{len(concepts)}!={int(expected_concept_count)}")
    status = "PASS" if not errors and counts.get("needs_repair", 0) == 0 else "HOLD"
    report = {
        "schema": REPORT_SCHEMA,
        "generated_at": generated_at or _now(),
        "closure_hash": str(closure.get("closure_hash") or ""),
        "closure_schema": str(closure.get("schema") or ""),
        "source_manifest_hash": str((closure.get("source_manifest") or {}).get("sha256") or ""),
        "expected_concept_count": expected_concept_count,
        "reference_count": sum(len(rows) for rows in per_concept.values()),
        "concept_count": len(concepts),
        "concept_status_counts": counts,
        "ledger_entry_count": len(entries),
        "concept_retirement_count": len(retirement_entries),
        "validation_errors": errors,
        "concepts": concepts,
        "status": status,
        "gate": {
            "p3_closed": status == "PASS",
            "admission_target": "disabled",
            "external_calls": {"oneapi": 0, "openviking": 0},
        },
    }
    report["report_hash"] = _hash({key: value for key, value in report.items() if key != "report_hash"})
    return report


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closure", type=Path, required=True)
    parser.add_argument("--dispositions", type=Path)
    parser.add_argument("--concept-retirements", type=Path)
    parser.add_argument("--supplemental-readback", type=Path)
    parser.add_argument("--expected-concept-count", type=int, default=45)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        closure = _read_json(args.closure.expanduser().resolve())
        entries = _read_jsonl(args.dispositions.expanduser().resolve() if args.dispositions else None)
        retirements = _read_jsonl(args.concept_retirements.expanduser().resolve() if args.concept_retirements else None)
        supplemental = _read_json(args.supplemental_readback.expanduser().resolve()) if args.supplemental_readback else None
        report = build_report(
            closure=closure,
            dispositions=entries,
            concept_retirements=retirements,
            supplemental_readback=supplemental,
            expected_concept_count=args.expected_concept_count,
        )
        _write_json(args.output.expanduser().resolve(), report)
    except Exception as exc:
        print(json.dumps({"schema": REPORT_SCHEMA, "status": "HOLD", "error": f"{type(exc).__name__}:{exc}"}, ensure_ascii=False))
        return 1
    print(json.dumps({key: report[key] for key in ("schema", "status", "concept_count", "concept_status_counts", "validation_errors", "report_hash")}, ensure_ascii=False))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
