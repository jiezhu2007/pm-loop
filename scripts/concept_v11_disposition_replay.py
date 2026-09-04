#!/usr/bin/env python3
"""Replay an append-only source disposition ledger onto a new C7 closure.

The source URI normalizer changed from NFKC to NFC in order to preserve
OpenViking path identity.  This utility keeps the historical ledger intact
while re-keying its entries against the new closure.  Compatibility
normalization is used only to bridge old map identities; it never changes the
URI written to the new ledger.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


CLOSURE_SCHEMA = "concept-v11.c7-source-map-evidence.v1"
LEDGER_SCHEMA = "concept-v11.source-coverage-disposition.v1"
REPLAY_SCHEMA = "concept-v11.source-coverage-disposition-replay.v1"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"ledger row {line_number} is not an object")
        rows.append(value)
    return rows


def _uri_compat(value: Any) -> str:
    """Bridge legacy NFKC paths without using it for new URI output."""
    return unicodedata.normalize("NFKC", str(value or "").strip()).replace("\\", "/").strip("/")


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def replay(
    *,
    closure: Mapping[str, Any],
    old_entries: Iterable[Mapping[str, Any]],
    c7_evidence_sha256: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if str(closure.get("schema") or "") != CLOSURE_SCHEMA:
        raise ValueError("unsupported C7 closure schema")
    closure_hash = str(closure.get("closure_hash") or "")
    if not closure_hash.startswith("sha256:"):
        raise ValueError("C7 closure hash is missing")
    if not c7_evidence_sha256.startswith("sha256:"):
        raise ValueError("C7 evidence hash is missing")

    old_entries = list(old_entries)
    rows = [row for row in closure.get("rows") or [] if isinstance(row, Mapping)]
    by_map_id: dict[str, Mapping[str, Any]] = {}
    by_exact: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    by_compat: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        map_id = str(row.get("map_id") or "")
        concept = str(row.get("concept") or "")
        uri = str(row.get("source_uri") or "")
        if not map_id or not concept or not uri:
            raise ValueError("closure row lacks map_id/concept/source_uri")
        if map_id in by_map_id:
            raise ValueError(f"duplicate closure map_id: {map_id}")
        by_map_id[map_id] = row
        by_exact.setdefault((concept, uri), []).append(row)
        by_compat.setdefault((concept, _uri_compat(uri)), []).append(row)

    output: list[dict[str, Any]] = []
    dropped_mapped = 0
    matched_by = {"map_id": 0, "exact_uri": 0, "compat_uri": 0}
    for index, raw in enumerate(old_entries):
        entry = copy.deepcopy(dict(raw))
        if str(entry.get("schema") or "") != LEDGER_SCHEMA:
            raise ValueError(f"old entry {index} has unsupported schema")
        old_map_id = str(entry.get("map_id") or "")
        concept = str(entry.get("concept") or "")
        old_uri = str(entry.get("source_uri") or "")
        target = by_map_id.get(old_map_id)
        matched_by_key = "map_id"
        if target is None:
            exact = by_exact.get((concept, old_uri), [])
            if len(exact) > 1:
                raise ValueError(f"old entry {index} has ambiguous exact URI match")
            if exact:
                target = exact[0]
                matched_by_key = "exact_uri"
        if target is None:
            compat = by_compat.get((concept, _uri_compat(old_uri)), [])
            if len(compat) > 1:
                raise ValueError(f"old entry {index} has ambiguous compatibility URI match")
            if compat:
                target = compat[0]
                matched_by_key = "compat_uri"
        if target is None:
            raise ValueError(f"old entry {index} cannot match map_id={old_map_id}")

        matched_by[matched_by_key] += 1
        status = str(target.get("status") or "")
        if status == "mapped":
            dropped_mapped += 1
            continue
        if status != "quarantined":
            raise ValueError(f"old entry {index} matched unsupported target status={status}")
        entry["map_id"] = str(target["map_id"])
        entry["concept"] = str(target["concept"])
        entry["source_uri"] = str(target["source_uri"])
        entry["closure_hash"] = closure_hash
        refs = entry.get("evidence_refs")
        if isinstance(refs, list) and refs:
            first = dict(refs[0]) if isinstance(refs[0], Mapping) else {}
            first["sha256"] = c7_evidence_sha256
            refs[0] = first
        else:
            entry["evidence_refs"] = [{"kind": "c7_source_map_evidence", "sha256": c7_evidence_sha256}]
        output.append(entry)

    if len(output) + dropped_mapped != len(old_entries):
        raise AssertionError("replay accounting mismatch")
    audit = {
        "schema": REPLAY_SCHEMA,
        "generated_at": _now(),
        "old_entry_count": len(output) + dropped_mapped,
        "replay_entry_count": len(output),
        "dropped_now_mapped": dropped_mapped,
        "new_closure_hash": closure_hash,
        "new_c7_evidence_sha256": c7_evidence_sha256,
        "matched_by": matched_by,
    }
    return output, audit


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closure", type=Path, required=True)
    parser.add_argument("--old-ledger", type=Path, required=True)
    parser.add_argument("--c7-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    closure = _read_json(args.closure.expanduser().resolve())
    old_entries = _read_jsonl(args.old_ledger.expanduser().resolve())
    c7_path = args.c7_evidence.expanduser().resolve()
    replayed, audit = replay(
        closure=closure,
        old_entries=old_entries,
        c7_evidence_sha256=_sha256(c7_path),
    )
    _write_jsonl(args.output.expanduser().resolve(), replayed)
    _write_json(args.audit_output.expanduser().resolve(), audit)
    print(json.dumps(audit, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
