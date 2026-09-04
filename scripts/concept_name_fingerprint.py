#!/usr/bin/env python3
"""Name/path based revision helpers for the lightweight concept refresh mode.

This module deliberately keeps name fingerprints separate from content hashes.
They are cheap change signals for the weekly concept impact pass, not proof that
the document body is unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple


NAME_HASH_RULE = "source+path+name:v1"
NAME_HASH_PREFIX = "namepath-v1:"
# Older baselines used the generic content-hash prefix for this metadata
# fingerprint.  Keep accepting it at comparison boundaries, but never emit
# it for a new name/path fingerprint.
LEGACY_NAME_HASH_PREFIX = "sha256:"
NAME_HASH_FORMAT = "namepath-v1"
_SCHEME_PATH_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*://)(.*)$")


def _text(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "").strip())


def normalize_path(value: Any) -> str:
    """Normalize a URI/path without changing its scheme or Unicode meaning."""
    text = _text(value).replace("\\", "/")
    match = _SCHEME_PATH_RE.match(text)
    if match:
        scheme, rest = match.groups()
        rest = re.sub(r"/+", "/", rest).strip("/")
        return scheme.lower() + rest
    return re.sub(r"/+", "/", text).strip("/")


def basename(path: Any) -> str:
    value = normalize_path(path).rstrip("/")
    return value.rsplit("/", 1)[-1] if value else ""


def name_hash(
    source: Any,
    path: Any,
    name: Any,
    *,
    rule: str = NAME_HASH_RULE,
) -> str:
    """Return a namespaced SHA-256 metadata fingerprint.

    The NUL separators prevent ambiguous concatenations such as ``ab/c`` vs
    ``a/bc``.  The distinct ``namepath-v1:`` prefix prevents this metadata
    signal from being mistaken for a trusted body hash.
    """
    source_value = _text(source) or "unknown-source"
    path_value = normalize_path(path)
    name_value = _text(name) or basename(path_value)
    payload = "\0".join((rule, source_value, path_value, name_value))
    return NAME_HASH_PREFIX + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_name_hash(value: Any) -> str:
    """Normalize current and legacy name fingerprints for comparison.

    This helper is intentionally scoped to name-hash fields.  Callers handling
    document bodies must continue to treat ``sha256:`` as a content hash.
    Unknown/empty values are returned as normalized text so malformed rows do
    not become equal merely because both are missing.
    """
    text = _text(value)
    if text.startswith(NAME_HASH_PREFIX):
        return text
    if text.startswith(LEGACY_NAME_HASH_PREFIX):
        return NAME_HASH_PREFIX + text[len(LEGACY_NAME_HASH_PREFIX):]
    return text


def name_hash_equal(left: Any, right: Any) -> bool:
    """Compare name hashes while accepting the pre-`namepath-v1` prefix."""
    left_value = canonical_name_hash(left)
    right_value = canonical_name_hash(right)
    return bool(left_value and right_value and left_value == right_value)


def document_key(row: Mapping[str, Any], *, source: str = "") -> str:
    """Build a stable display/index key, preferring source + docGuid."""
    source_value = _text(row.get("source") or source) or "unknown-source"
    guid = _text(row.get("docGuid") or row.get("doc_guid"))
    if guid:
        return f"{source_value}:{guid}"
    path = normalize_path(row.get("target_uri") or row.get("viking_uri") or row.get("uri"))
    return f"{source_value}:{path}"


def fingerprint_row(row: Mapping[str, Any], *, source: str = "") -> Dict[str, Any]:
    """Return a normalized row suitable for a name baseline."""
    source_value = _text(row.get("source") or source) or "unknown-source"
    path = normalize_path(row.get("target_uri") or row.get("viking_uri") or row.get("uri"))
    name = _text(row.get("name") or row.get("title") or basename(path))
    return {
        "document_id": document_key(row, source=source_value),
        "source": source_value,
        "doc_guid": _text(row.get("docGuid") or row.get("doc_guid")) or None,
        "path": path,
        "name": name,
        "name_hash": name_hash(source_value, path, name),
        "name_hash_rule": NAME_HASH_RULE,
        "name_hash_prefix": NAME_HASH_PREFIX,
        "name_hash_format": NAME_HASH_FORMAT,
        "revision_kind": "name_hash",
        "heuristic": True,
    }


def _iter_rows(value: Any, *, default_source: str = "") -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        row_markers = {"target_uri", "viking_uri", "uri", "name", "title", "docGuid", "doc_guid"}
        rows = (value,) if row_markers.intersection(value) else value.values()
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows = value
    else:
        rows = ()
    for row in rows:
        if isinstance(row, Mapping):
            yield row


def snapshot_rows(
    values: Iterable[Any],
    *,
    default_source: str = "",
) -> Dict[str, Dict[str, Any]]:
    """Build a document-id -> fingerprint map from ledger/resource rows."""
    result: Dict[str, Dict[str, Any]] = {}
    for value in values:
        for row in _iter_rows(value, default_source=default_source):
            item = fingerprint_row(row, source=default_source)
            if not item["path"]:
                continue
            key = str(item["document_id"])
            previous = result.get(key)
            # A duplicate doc id should be impossible, but retaining the
            # lexicographically stable row makes materialization deterministic.
            if previous is None or tuple(str(item.get(k) or "") for k in ("path", "name")) < tuple(
                str(previous.get(k) or "") for k in ("path", "name")
            ):
                result[key] = item
    return dict(sorted(result.items()))


def compare_snapshots(
    current: Mapping[str, Mapping[str, Any]],
    previous: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    """Compare name fingerprints and return changed/new/removed document ids."""
    current_keys = set(current)
    previous_keys = set(previous)
    new = sorted(current_keys - previous_keys)
    removed = sorted(previous_keys - current_keys)
    changed = sorted(
        key
        for key in current_keys & previous_keys
        if not name_hash_equal(
            current[key].get("name_hash"),
            previous[key].get("name_hash"),
        )
    )
    unchanged = sorted((current_keys & previous_keys) - set(changed))
    return {
        "new_documents": new,
        "removed_documents": removed,
        "changed_documents": changed,
        "unchanged_documents": unchanged,
        "new_count": len(new),
        "removed_count": len(removed),
        "changed_count": len(changed),
        "unchanged_count": len(unchanged),
        "observed_count": len(current),
        "previous_count": len(previous),
        "revision_kind": "name_hash",
        "heuristic": True,
    }


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Materialize a name/path fingerprint baseline")
    parser.add_argument("--input", action="append", type=Path, required=True)
    parser.add_argument("--source", default="")
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    snapshots = []
    for path in args.input:
        try:
            snapshots.append(_load(path))
        except (OSError, json.JSONDecodeError) as exc:
            parser.error(f"cannot read {path}: {exc}")
    current = snapshot_rows(snapshots, default_source=args.source)
    previous: Dict[str, Dict[str, Any]] = {}
    if args.previous and args.previous.exists():
        value = _load(args.previous)
        if isinstance(value, Mapping) and isinstance(value.get("documents"), Mapping):
            value = value["documents"]
        if isinstance(value, Mapping):
            previous = {str(k): dict(v) for k, v in value.items() if isinstance(v, Mapping)}
    payload = {
        "schema_version": "concept-name-baseline.v1",
        "revision_kind": "name_hash",
        "name_hash_rule": NAME_HASH_RULE,
        "name_hash_prefix": NAME_HASH_PREFIX,
        "name_hash_format": NAME_HASH_FORMAT,
        "heuristic": True,
        "documents": current,
        "comparison": compare_snapshots(current, previous),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(args.output)
    print(json.dumps(payload["comparison"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
