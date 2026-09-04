#!/usr/bin/env python3
"""Build the metadata-only source manifest used by concept refresh.

The weekly refresh needs a cheap change signal, but the sync ledgers and the
deep-inventory URI list are not the same representation of a document.  This
module keeps those concerns separate: it indexes source metadata and reports
which Active concept sources are mapped, ambiguous, or currently outside the
sync ledger.  It never reads document bodies and never writes concept pages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SCHEMA_VERSION = "concept-source-manifest.v1"
NAME_HASH_RULE = "source+path+name:v1"
NAME_HASH_PREFIX = "namepath-v1:"
LEGACY_NAME_HASH_PREFIX = "sha256:"
NAME_HASH_FORMAT = "namepath-v1"
_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*://)(.*)$")


def _text(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "").strip())


def _uri_text(value: Any) -> str:
    """Trim URI text without compatibility-folding path characters.

    OpenViking paths are identity-bearing strings.  In particular, full-width
    punctuation in a directory name is distinct from its ASCII equivalent;
    applying NFKC here makes a manifest URI impossible to read back.
    """
    return unicodedata.normalize("NFC", str(value or "").strip())


def normalize_path(value: Any) -> str:
    # Keep URI/path code points intact.  Display names may use NFKC, but path
    # identity must remain byte-for-byte equivalent to the OpenViking URI.
    text = _uri_text(value).replace("\\", "/")
    match = _SCHEME_RE.match(text)
    if match:
        scheme, rest = match.groups()
        return scheme.lower() + re.sub(r"/+", "/", rest).strip("/")
    return re.sub(r"/+", "/", text).strip("/")


def basename(value: Any) -> str:
    path = normalize_path(value).rstrip("/")
    return path.rsplit("/", 1)[-1] if path else ""


def infer_source(uri: Any, explicit: Any = "") -> str:
    source = _text(explicit)
    if source:
        return source
    path = normalize_path(uri)
    marker = "/shengsuan/"
    if marker in path:
        rest = path.split(marker, 1)[1]
        return rest.split("/", 1)[0] or "shengsuan"
    if "/project-docs/" in path:
        return "project-docs"
    return "unknown-source"


def name_hash(source: Any, path: Any, name: Any) -> str:
    """Hash source/path/name metadata, explicitly separate from content hash."""
    source_value = _text(source) or "unknown-source"
    path_value = normalize_path(path)
    name_value = _text(name) or basename(path_value)
    payload = "\0".join((NAME_HASH_RULE, source_value, path_value, name_value))
    return NAME_HASH_PREFIX + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_name_hash(value: Any) -> str:
    """Normalize old and new name hashes without changing body hash helpers."""
    text = _text(value)
    if text.startswith(NAME_HASH_PREFIX):
        return text
    if text.startswith(LEGACY_NAME_HASH_PREFIX):
        return NAME_HASH_PREFIX + text[len(LEGACY_NAME_HASH_PREFIX):]
    return text


def name_hash_equal(left: Any, right: Any) -> bool:
    """Compare name-hash fields across the prefix migration."""
    left_value = canonical_name_hash(left)
    right_value = canonical_name_hash(right)
    return bool(left_value and right_value and left_value == right_value)


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _rows_from_value(value: Any) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """Yield (ledger key, row) while preserving a guid held only as a key."""
    if isinstance(value, Mapping):
        if any(key in value for key in ("target_uri", "viking_uri", "uri", "name", "title")):
            yield "", dict(value)
            return
        for key, raw in value.items():
            if isinstance(raw, Mapping):
                yield str(key), dict(raw)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for raw in value:
            if isinstance(raw, Mapping):
                yield "", dict(raw)


def _source_id(row: Mapping[str, Any]) -> str:
    source = _text(row.get("source")) or "unknown-source"
    repo_id = _text(row.get("repo_id") or row.get("repoId"))
    guid = _text(row.get("doc_guid") or row.get("docGuid"))
    if guid:
        # repo_id is the strongest available identity when a producer
        # exposes it.  Keep source in the fallback for old ledgers.
        return f"{repo_id}:{guid}" if repo_id else f"{source}:{guid}"
    return f"{source}:{normalize_path(row.get('uri')) or 'unknown-uri'}"


def _metadata_row(raw: Mapping[str, Any], *, ledger_key: str = "", origin: str = "ledger") -> Optional[Dict[str, Any]]:
    uri = normalize_path(raw.get("target_uri") or raw.get("viking_uri") or raw.get("uri"))
    if not uri:
        return None
    source = infer_source(uri, raw.get("source"))
    repo_id = _text(raw.get("repo_id") or raw.get("repoId") or "")
    guid = _text(raw.get("doc_guid") or raw.get("docGuid") or "") or _text(ledger_key)
    # A URI-shaped dictionary key is a path, not a stable doc id.
    if guid.startswith(("viking://", "http://", "https://")):
        guid = ""
    name = _text(raw.get("name") or raw.get("title") or basename(uri))
    row: Dict[str, Any] = {
        "source_id": _source_id(
            {"source": source, "repo_id": repo_id, "doc_guid": guid, "uri": uri}
        ),
        "source": source,
        "repo_id": repo_id or None,
        "doc_guid": guid or None,
        "path": uri,
        "name": name,
        "name_hash": name_hash(source, uri, name),
        "name_hash_rule": NAME_HASH_RULE,
        "name_hash_prefix": NAME_HASH_PREFIX,
        "name_hash_format": NAME_HASH_FORMAT,
        "revision_kind": "name_hash",
        "heuristic": True,
        "origin": origin,
        "metadata_status": "observed" if origin == "ledger" else "inventory_only",
    }
    for key in ("publishTime", "fetched_at", "updated_at", "sha256", "sha256_mode"):
        if raw.get(key) not in (None, ""):
            row[key] = raw[key]
    return row


def load_metadata_rows(ledger_paths: Iterable[Path], inventory_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_inventory: set[str] = set()
    for path in ledger_paths:
        value = _read_json(Path(path), {})
        for key, raw in _rows_from_value(value):
            row = _metadata_row(raw, ledger_key=key, origin="ledger")
            if row:
                rows.append(row)
    ledger_uris = {str(row["path"]) for row in rows}
    if inventory_path:
        value = _read_json(Path(inventory_path), {})
        uris = value.get("uris") if isinstance(value, Mapping) else value
        if isinstance(uris, Sequence) and not isinstance(uris, (str, bytes, bytearray)):
            for raw_uri in uris:
                uri = normalize_path(raw_uri)
                if not uri or uri in ledger_uris or uri in seen_inventory:
                    continue
                seen_inventory.add(uri)
                row = _metadata_row({"uri": uri}, origin="inventory")
                if row:
                    rows.append(row)
    # Keep all rows. Duplicate target URIs are important evidence of a mapping
    # conflict; silently collapsing them would hide the very issue this file
    # is intended to expose.
    return rows


def _tree_intersects(left: str, right: str) -> bool:
    a, b = normalize_path(left).rstrip("/"), normalize_path(right).rstrip("/")
    return bool(a and b and (a == b or a.startswith(b + "/") or b.startswith(a + "/")))


def _ledger_candidates(
    source_uri: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    by_uri: Optional[Mapping[str, Sequence[Mapping[str, Any]]]] = None,
    allow_child_scan: bool = True,
) -> Tuple[List[Dict[str, Any]], str]:
    """Resolve one source URI to the most specific ledger metadata.

    Inventory-only rows describe what was observed in OpenViking, but they do
    not establish a source identity.  They are intentionally excluded from
    this resolver.  Exact URI matches win; otherwise only rows at the
    longest slash-boundary parent path are considered.  This prevents a leaf
    from being ambiguously mapped to both a broad root and a more specific
    imported page.
    """
    target = normalize_path(source_uri)
    ledger_rows = [row for row in rows if str(row.get("origin") or "ledger") == "ledger"]
    index = by_uri or {}
    exact = [dict(row) for row in index.get(target, ()) if str(row.get("origin") or "ledger") == "ledger"]
    if exact:
        return exact, "exact"
    # Most inventory leaves have a ledger directory as an ancestor.  Walking
    # the finite URI prefixes avoids an O(inventory * ledger) scan on every
    # refresh (the current baseline has more than 7k observed URIs).
    prefixes: List[str] = []
    for position in range(len(target) - 1, 0, -1):
        if target[position] == "/":
            prefix = target[:position].rstrip("/")
            if prefix and prefix not in prefixes:
                prefixes.append(prefix)
    for prefix in prefixes:
        candidates = [
            dict(row)
            for row in index.get(prefix, ())
            if str(row.get("origin") or "ledger") == "ledger"
        ]
        if candidates:
            return candidates, "tree"

    # A concept may retain a parent URI while a producer records only a leaf
    # URI.  This reverse direction is less common, so retain a bounded scan as
    # a compatibility fallback after the fast ancestor lookup.  Document
    # mapping calls disable it because an unmapped inventory URI should stay
    # cheap to classify; Active source checks keep it enabled for compatibility.
    if not allow_child_scan:
        return [], "none"
    matches = [
        dict(row)
        for row in ledger_rows
        if _tree_intersects(target, str(row.get("path") or ""))
        and normalize_path(str(row.get("path") or "")).rstrip("/") != target.rstrip("/")
    ]
    if not matches:
        return [], "none"
    longest = max(len(normalize_path(str(row.get("path") or "")).rstrip("/")) for row in matches)
    return [
        row
        for row in matches
        if len(normalize_path(str(row.get("path") or "")).rstrip("/")) == longest
    ], "tree"


def _mapping_record(
    uri: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    by_uri: Mapping[str, Sequence[Mapping[str, Any]]],
    conflict_uris: Mapping[str, Sequence[str]],
) -> Dict[str, Any]:
    candidates, mode = _ledger_candidates(
        uri,
        rows,
        by_uri=by_uri,
        allow_child_scan=False,
    )
    source_ids = sorted({str(row.get("source_id") or "") for row in candidates if row.get("source_id")})
    path_values = sorted({str(row.get("path") or "") for row in candidates if row.get("path")})
    target = normalize_path(uri)
    # A conflict at the exact/longest candidate URI must remain a conflict
    # even when malformed producers happened to reuse the same source_id.
    candidate_conflict = any(path in conflict_uris for path in path_values)
    if candidate_conflict or len(source_ids) > 1:
        status = "conflict"
    elif len(source_ids) == 1:
        status = "mapped"
    else:
        status = "unmapped"
    return {
        "uri": target,
        "status": status,
        "match_mode": mode if source_ids else "none",
        "source_id": source_ids[0] if status == "mapped" else None,
        "matched_source_ids": source_ids,
        "matched_paths": path_values,
    }


def _active_sources(concepts: Mapping[str, Any]) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    for concept, raw in concepts.items():
        if not isinstance(raw, Mapping) or str(raw.get("status") or "active") != "active":
            continue
        for source in raw.get("sources") or []:
            value = normalize_path(source)
            if value:
                result.append((str(concept), value))
    return result


def build_manifest(
    rows: Sequence[Mapping[str, Any]],
    concepts: Mapping[str, Any],
    *,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    normalized = [dict(row) for row in rows if row.get("path")]
    by_uri: Dict[str, List[Dict[str, Any]]] = {}
    for row in normalized:
        by_uri.setdefault(str(row["path"]), []).append(row)
    ledger_by_uri: Dict[str, List[Dict[str, Any]]] = {}
    for row in normalized:
        if str(row.get("origin") or "ledger") == "ledger":
            ledger_by_uri.setdefault(str(row["path"]), []).append(row)
    conflicts = {
        uri: sorted({str(row["source_id"]) for row in values})
        for uri, values in ledger_by_uri.items()
        if len({str(row.get("source_id") or "") for row in values}) > 1
    }
    for uri, ids in conflicts.items():
        for row in ledger_by_uri[uri]:
            row["metadata_status"] = "conflict"
            row["conflict_source_ids"] = ids

    active = _active_sources(concepts)
    source_checks: List[Dict[str, Any]] = []
    mapped_count = 0
    conflict_source_count = 0
    for concept, source_uri in active:
        matches, mode = _ledger_candidates(source_uri, normalized, by_uri=by_uri)
        unique_ids = sorted({str(row["source_id"]) for row in matches})
        candidate_conflict = any(str(row.get("path") or "") in conflicts for row in matches)
        if len(unique_ids) == 1 and not candidate_conflict:
            status = "mapped"
            mapped_count += 1
        elif len(unique_ids) > 1 or candidate_conflict:
            status = "conflict"
            conflict_source_count += 1
        else:
            status = "unmapped"
        source_checks.append({
            "concept": concept,
            "source_uri": source_uri,
            "status": status,
            "match_mode": mode if unique_ids else "none",
            "matched_source_ids": unique_ids,
            "matched_paths": sorted({str(row.get("path")) for row in matches}),
        })
    total_active = len(source_checks)
    name_observed = sum(1 for row in normalized if row.get("name_hash"))
    inventory_count = sum(1 for row in normalized if row.get("origin") == "inventory")
    ledger_count = len(normalized) - inventory_count
    observed_uris = sorted(
        {
            str(row.get("path") or "")
            for row in normalized
            if str(row.get("path") or "")
        }
    )
    document_mappings = [
        _mapping_record(
            uri,
            normalized,
            by_uri=by_uri,
            conflict_uris=conflicts,
        )
        for uri in observed_uris
    ]
    document_mapping_counts = {
        status: sum(1 for row in document_mappings if row["status"] == status)
        for status in ("mapped", "unmapped", "conflict")
    }
    observed_document_count = len(observed_uris)

    # ``source_checks`` intentionally keeps one row per concept-source
    # reference.  A URI can therefore occur multiple times when several
    # concepts cite the same document (or a single concept contains a
    # duplicated source entry).  Preserve those reference-level counts for
    # backwards compatibility, and derive a separate URI-level view for
    # coverage reporting.  Aggregation is conservative if malformed input
    # gives the same URI different statuses: conflict wins over unmapped,
    # which wins over mapped.
    active_source_reference_count = len(source_checks)
    unique_active_checks: Dict[str, Dict[str, Any]] = {}
    status_priority = {"mapped": 0, "unmapped": 1, "conflict": 2}
    for check in source_checks:
        uri = str(check.get("source_uri") or "")
        if not uri:
            continue
        current = unique_active_checks.get(uri)
        if current is None:
            current = {
                "source_uri": uri,
                "status": str(check.get("status") or "unmapped"),
                "concepts": [],
                "matched_source_ids": set(),
                "matched_paths": set(),
                "reference_count": 0,
            }
            unique_active_checks[uri] = current
        current["reference_count"] += 1
        concept = str(check.get("concept") or "")
        if concept and concept not in current["concepts"]:
            current["concepts"].append(concept)
        current["matched_source_ids"].update(str(value) for value in check.get("matched_source_ids") or [])
        current["matched_paths"].update(str(value) for value in check.get("matched_paths") or [])
        status = str(check.get("status") or "unmapped")
        if status_priority.get(status, 1) > status_priority.get(str(current["status"]), 1):
            current["status"] = status

    # Convert set accumulators into deterministic JSON-safe lists.  Keep this
    # compact projection alongside the full reference rows for diagnostics.
    active_source_unique_checks: List[Dict[str, Any]] = []
    for uri in sorted(unique_active_checks):
        value = unique_active_checks[uri]
        active_source_unique_checks.append(
            {
                "source_uri": uri,
                "status": value["status"],
                "concepts": sorted(value["concepts"]),
                "reference_count": int(value["reference_count"]),
                "matched_source_ids": sorted(value["matched_source_ids"]),
                "matched_paths": sorted(value["matched_paths"]),
            }
        )
    active_source_unique_count = len(active_source_unique_checks)
    reference_status_counts = {
        "mapped": mapped_count,
        "unmapped": sum(1 for row in source_checks if row["status"] == "unmapped"),
        "conflict": conflict_source_count,
    }
    unique_status_counts = {
        status: sum(1 for row in active_source_unique_checks if row["status"] == status)
        for status in ("mapped", "unmapped", "conflict")
    }
    generated_at = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "revision_mode": "name_hash",
        "name_hash_rule": NAME_HASH_RULE,
        "name_hash_prefix": NAME_HASH_PREFIX,
        "name_hash_format": NAME_HASH_FORMAT,
        "heuristic": True,
        "documents": sorted(normalized, key=lambda row: (str(row.get("path") or ""), str(row.get("source_id") or ""))),
        "document_mappings": document_mappings,
        "active_source_checks": source_checks,
        "active_source_unique_checks": active_source_unique_checks,
        "metrics": {
            "document_count": len(normalized),
            "ledger_document_count": ledger_count,
            "inventory_only_document_count": inventory_count,
            "unique_uri_count": len(by_uri),
            "name_hash_observed": name_observed,
            "name_hash_coverage": round(name_observed / len(normalized), 6) if normalized else 1.0,
            # Legacy fields retain their historical reference-level meaning.
            "active_source_count": total_active,
            "mapped_active_source_count": mapped_count,
            "unmapped_active_source_count": reference_status_counts["unmapped"],
            "conflict_active_source_count": conflict_source_count,
            "active_source_reference_count": active_source_reference_count,
            "mapped_active_source_reference_count": reference_status_counts["mapped"],
            "unmapped_active_source_reference_count": reference_status_counts["unmapped"],
            "conflict_active_source_reference_count": reference_status_counts["conflict"],
            "active_source_unique_count": active_source_unique_count,
            "mapped_active_source_unique_count": unique_status_counts["mapped"],
            "unmapped_active_source_unique_count": unique_status_counts["unmapped"],
            "conflict_active_source_unique_count": unique_status_counts["conflict"],
            "metadata_conflict_uri_count": len(conflicts),
            "mapping_coverage": round(mapped_count / total_active, 6) if total_active else 1.0,
            "mapping_unique_coverage": round(
                unique_status_counts["mapped"] / active_source_unique_count, 6
            )
            if active_source_unique_count
            else 1.0,
            "observed_document_count": observed_document_count,
            "mapped_document_count": document_mapping_counts["mapped"],
            "unmapped_document_count": document_mapping_counts["unmapped"],
            "conflict_document_count": document_mapping_counts["conflict"],
            "document_mapping_coverage": round(
                document_mapping_counts["mapped"] / observed_document_count, 6
            )
            if observed_document_count
            else 1.0,
            "ledger_name_hash_count": sum(1 for row in normalized if row.get("origin") == "ledger" and row.get("name_hash")),
            "ledger_name_hash_coverage": round(
                sum(1 for row in normalized if row.get("origin") == "ledger" and row.get("name_hash")) / ledger_count,
                6,
            )
            if ledger_count
            else 1.0,
        },
        "conflicts": conflicts,
        "unmapped_active_sources": [row for row in source_checks if row["status"] == "unmapped"],
        "unmapped_documents": [row for row in document_mappings if row["status"] == "unmapped"],
        "conflict_documents": [row for row in document_mappings if row["status"] == "conflict"],
    }


def _load_concepts(path: Optional[Path]) -> Dict[str, Any]:
    value = _read_json(path, {}) if path else {}
    return value if isinstance(value, dict) else {}


def write_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def compact_manifest(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the bounded sidecar consumed by six-second Control Plane polls."""
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    return {
        "schema_version": payload.get("schema_version", SCHEMA_VERSION),
        "generated_at": payload.get("generated_at"),
        "revision_mode": payload.get("revision_mode", "name_hash"),
        "name_hash_rule": payload.get("name_hash_rule", NAME_HASH_RULE),
        "name_hash_prefix": payload.get("name_hash_prefix", NAME_HASH_PREFIX),
        "name_hash_format": payload.get("name_hash_format", NAME_HASH_FORMAT),
        "heuristic": bool(payload.get("heuristic", True)),
        "metrics": dict(metrics),
        "active_source_count": metrics.get("active_source_count"),
        "active_source_reference_count": metrics.get("active_source_reference_count"),
        "active_source_unique_count": metrics.get("active_source_unique_count"),
        "mapped_active_source_count": metrics.get("mapped_active_source_count"),
        "mapped_active_source_reference_count": metrics.get("mapped_active_source_reference_count"),
        "mapped_active_source_unique_count": metrics.get("mapped_active_source_unique_count"),
        "unmapped_active_source_count": metrics.get("unmapped_active_source_count"),
        "unmapped_active_source_reference_count": metrics.get("unmapped_active_source_reference_count"),
        "unmapped_active_source_unique_count": metrics.get("unmapped_active_source_unique_count"),
        "conflict_active_source_count": metrics.get("conflict_active_source_count"),
        "conflict_active_source_reference_count": metrics.get("conflict_active_source_reference_count"),
        "conflict_active_source_unique_count": metrics.get("conflict_active_source_unique_count"),
        "metadata_conflict_uri_count": metrics.get("metadata_conflict_uri_count"),
        "ledger_name_hash_coverage": metrics.get("ledger_name_hash_coverage"),
        "document_mapping_coverage": metrics.get("document_mapping_coverage"),
        "mapping_unique_coverage": metrics.get("mapping_unique_coverage"),
        "unmapped_active_sources": payload.get("unmapped_active_sources", []),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build a metadata-only concept source manifest")
    parser.add_argument("--ledger", action="append", type=Path, default=[])
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--concepts-ledger", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--meta-output",
        type=Path,
        help="optional compact sidecar for polling; the full manifest remains in --output",
    )
    args = parser.parse_args(argv)
    rows = load_metadata_rows(args.ledger, args.inventory)
    payload = build_manifest(rows, _load_concepts(args.concepts_ledger))
    write_manifest(args.output, payload)
    if args.meta_output:
        write_manifest(args.meta_output, compact_manifest(payload))
    print(json.dumps(payload["metrics"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
