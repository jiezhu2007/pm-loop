#!/usr/bin/env python3
"""Close the V1.1 concept source map from immutable local evidence.

This stage never discovers or guesses source identities. It consumes a fresh
metadata manifest plus an independently collected OpenViking content read-back
manifest. A reference is ``mapped`` only when the metadata identity is unique
and the exact leaf URI was read successfully; every other reference is moved
to the explicit ``quarantined`` terminal state with an owner and next action.

The runner creates a stage-specific SQLite backup before applying changes. It
only mutates ``concept_source_map`` (plus the shared migration lease used to
serialize the stage), keeps concept admission disabled, and makes no network
calls itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_migration import _copy_backup, foundation_check, write_manifest  # noqa: E402
from concept_v11_schema_v2 import TARGET_SCHEMA_VERSION, schema_v2_state  # noqa: E402
from pm_system_store import PMSystemStore, now_iso  # noqa: E402


SCHEMA = "concept-v11.c7-source-map-closure.v1"
EVIDENCE_SCHEMA = "concept-v11.c7-source-map-evidence.v1"
READBACK_SCHEMA = "concept-v11.c7-content-readback.v1"
STAGE_ID = "C7-SOURCE-MAP-CLOSURE"
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11"
DEFAULT_NAMESPACE_EPOCH = "v45-r2-20260830"
DEFAULT_OWNER = f"codex-concept-c7:{os.getpid()}"
QUARANTINE_TTL_DAYS = 30


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON evidence: {path}: {exc}") from exc


def _concept_id(name: str) -> str:
    return "concept-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:24]


def _map_id(namespace_epoch: str, concept: str, source_uri: str) -> str:
    payload = namespace_epoch + concept + source_uri
    return "map-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _quarantine_reason(status: str, source_ids: list[str], readback: Optional[Mapping[str, Any]]) -> str:
    if status == "conflict":
        return "source_identity_conflict"
    if status != "mapped":
        return "source_not_in_current_ledger"
    if len(source_ids) != 1:
        return "mapped_identity_not_unique"
    if not readback or str(readback.get("status") or "") != "verified":
        return "content_readback_failed"
    if not str(readback.get("content_sha256") or "").startswith("sha256:"):
        return "content_hash_missing"
    return "unknown_source_map_failure"


def build_closure(
    *,
    manifest: Mapping[str, Any],
    manifest_hash: str,
    readback: Mapping[str, Any],
    readback_hash: str,
    namespace_epoch: str,
    owner: str,
    observed_at: Optional[str] = None,
) -> dict[str, Any]:
    if str(manifest.get("schema_version") or "") != "concept-source-manifest.v1":
        raise RuntimeError("unsupported source manifest schema")
    if str(readback.get("schema") or "") != READBACK_SCHEMA:
        raise RuntimeError("unsupported content read-back schema")
    checks = manifest.get("active_source_checks")
    readback_rows = readback.get("rows")
    if not isinstance(checks, list) or not isinstance(readback_rows, list):
        raise RuntimeError("source manifest or read-back rows are missing")
    readback_by_uri = {
        str(row.get("uri") or ""): row
        for row in readback_rows
        if isinstance(row, Mapping) and row.get("uri")
    }
    observed_at = observed_at or now_iso()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(days=QUARANTINE_TTL_DAYS)
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    source_status_counts: dict[str, int] = {}
    terminal_status_counts = {"mapped": 0, "quarantined": 0}

    for index, raw in enumerate(checks):
        if not isinstance(raw, Mapping):
            raise RuntimeError(f"invalid source check at index {index}")
        concept = str(raw.get("concept") or "").strip()
        source_uri = str(raw.get("source_uri") or "").strip()
        if not concept or not source_uri:
            raise RuntimeError(f"source check lacks concept/source_uri at index {index}")
        identity = (concept, source_uri)
        if identity in seen:
            raise RuntimeError(f"duplicate concept source reference: {concept}: {source_uri}")
        seen.add(identity)
        source_status = str(raw.get("status") or "unmapped").lower()
        source_status_counts[source_status] = source_status_counts.get(source_status, 0) + 1
        source_ids = sorted({str(value) for value in raw.get("matched_source_ids") or [] if str(value)})
        body = readback_by_uri.get(source_uri)
        mapped = (
            source_status == "mapped"
            and len(source_ids) == 1
            and isinstance(body, Mapping)
            and str(body.get("status") or "") == "verified"
            and str(body.get("content_sha256") or "").startswith("sha256:")
        )
        terminal_status = "mapped" if mapped else "quarantined"
        terminal_status_counts[terminal_status] += 1
        evidence_refs: list[dict[str, Any]] = [
            {"kind": "source_manifest", "sha256": manifest_hash},
            {"kind": "content_readback_manifest", "sha256": readback_hash},
        ]
        if mapped:
            evidence_refs.append(
                {
                    "kind": "content_readback",
                    "uri": source_uri,
                    "content_sha256": body.get("content_sha256"),
                    "bytes": body.get("bytes"),
                }
            )
        reason = "content_and_identity_verified" if mapped else _quarantine_reason(source_status, source_ids, body)
        lineage = {
            "stage_id": STAGE_ID,
            "source_manifest_generated_at": manifest.get("generated_at"),
            "source_manifest_status": source_status,
            "source_index": index,
            "matched_paths": sorted({str(value) for value in raw.get("matched_paths") or [] if str(value)}),
            "escalation": None
            if mapped
            else {
                "route": "source-map-review",
                "severity": "warning",
                "action": "verify_or_replace_source",
            },
        }
        row = {
            "map_id": _map_id(namespace_epoch, concept, source_uri),
            "concept": concept,
            "concept_id": _concept_id(concept),
            "namespace_epoch": namespace_epoch,
            "source_id": source_ids[0] if len(source_ids) == 1 else "unknown",
            "source_uri": source_uri,
            "leaf_uri": source_uri if mapped else None,
            "identity_method": str(raw.get("match_mode") or "none"),
            "status": terminal_status,
            "confidence": 1.0 if mapped else None,
            "conflict_set_id": _hash(source_ids) if source_status == "conflict" else None,
            "owner": None if mapped else owner,
            "evidence_refs": evidence_refs,
            "evidence_set_hash": _hash(
                {
                    "concept": concept,
                    "source_uri": source_uri,
                    "source_ids": source_ids,
                    "status": terminal_status,
                    "evidence_refs": evidence_refs,
                }
            ),
            "next_action": None if mapped else "verify_or_replace_source",
            "expires_at": None if mapped else expires_at,
            "lineage": lineage,
            "resolved_at": observed_at,
            "resolved_by": owner,
            "resolution_reason": reason,
        }
        rows.append(row)

    payload: dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA,
        "stage_id": STAGE_ID,
        "observed_at": observed_at,
        "namespace_epoch": namespace_epoch,
        "owner": owner,
        "source_manifest": {
            "schema": manifest.get("schema_version"),
            "generated_at": manifest.get("generated_at"),
            "sha256": manifest_hash,
            "heuristic": bool(manifest.get("heuristic")),
        },
        "content_readback": {
            "schema": readback.get("schema"),
            "observed_at": readback.get("observed_at"),
            "sha256": readback_hash,
            "unique_uri_count": readback.get("unique_uri_count"),
            "verified_count": readback.get("verified_count"),
            "failed_count": readback.get("failed_count"),
        },
        "input_status_counts": source_status_counts,
        "terminal_status_counts": terminal_status_counts,
        "reference_count": len(rows),
        "rows": rows,
    }
    payload["closure_hash"] = _hash({key: value for key, value in payload.items() if key != "closure_hash"})
    return payload


def apply_closure(
    store: PMSystemStore,
    *,
    closure: Mapping[str, Any],
    namespace_epoch: str,
    owner: str,
    lease_id: str,
) -> dict[str, Any]:
    rows = closure.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("closure evidence rows are missing")
    now = now_iso()
    inserted = 0
    updated = 0
    unchanged = 0
    closure_hash = str(closure.get("closure_hash") or "")
    with store.transaction() as connection:
        freeze = store._freeze_blocks(connection)
        if freeze is not None:
            raise RuntimeError("source-map closure cannot run while PM Runtime is frozen")
        lease = connection.execute(
            "SELECT migration_epoch,owner,state FROM migration_leases WHERE lease_id=?",
            (lease_id,),
        ).fetchone()
        if lease is None or lease[0] != namespace_epoch or lease[1] != owner or lease[2] != "active":
            raise RuntimeError("C7 stage lease is missing or mismatched")
        admission = connection.execute(
            "SELECT admission_state FROM concept_admissions WHERE namespace_epoch=?",
            (namespace_epoch,),
        ).fetchone()
        if admission is None or str(admission[0]) != "disabled":
            raise RuntimeError("C7 source-map closure requires concept_admission=disabled")
        schema = connection.execute(
            "SELECT 1 FROM concept_schema_meta WHERE schema_version=?",
            (TARGET_SCHEMA_VERSION,),
        ).fetchone()
        if schema is None:
            raise RuntimeError("concept schema v2 is required before C7")

        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("invalid closure row")
            existing = connection.execute(
                "SELECT * FROM concept_source_map WHERE concept_id=? AND namespace_epoch=? AND source_uri=?",
                (row["concept_id"], namespace_epoch, row["source_uri"]),
            ).fetchone()
            lineage = dict(row.get("lineage") or {})
            lineage["closure_hash"] = closure_hash
            if existing is not None:
                previous_lineage = {}
                try:
                    previous_lineage = json.loads(str(existing["lineage_json"] or "{}"))
                except json.JSONDecodeError:
                    previous_lineage = {"invalid_legacy_lineage": True}
                if str(previous_lineage.get("closure_hash") or "") == closure_hash:
                    unchanged += 1
                    continue
                lineage["previous"] = {
                    "status": existing["status"],
                    "source_id": existing["source_id"],
                    "evidence_set_hash": existing["evidence_set_hash"],
                    "resolution_reason": existing["resolution_reason"],
                    "updated_at": existing["updated_at"],
                }
                connection.execute(
                    """
                    UPDATE concept_source_map
                    SET source_id=?,leaf_uri=?,identity_method=?,status=?,confidence=?,conflict_set_id=?,
                        owner=?,evidence_refs_json=?,evidence_set_hash=?,next_action=?,expires_at=?,
                        lineage_json=?,resolved_at=?,resolved_by=?,resolution_reason=?,updated_at=?
                    WHERE map_id=?
                    """,
                    (
                        row["source_id"], row.get("leaf_uri"), row["identity_method"], row["status"],
                        row.get("confidence"), row.get("conflict_set_id"), row.get("owner"),
                        _canonical(row.get("evidence_refs") or []), row["evidence_set_hash"],
                        row.get("next_action"), row.get("expires_at"), _canonical(lineage),
                        row.get("resolved_at"), row.get("resolved_by"), row.get("resolution_reason"),
                        now, existing["map_id"],
                    ),
                )
                updated += 1
            else:
                connection.execute(
                    """
                    INSERT INTO concept_source_map(
                        map_id,concept_id,namespace_epoch,source_id,source_uri,leaf_uri,identity_method,
                        status,confidence,conflict_set_id,owner,evidence_refs_json,evidence_set_hash,
                        next_action,expires_at,lineage_json,resolved_at,resolved_by,resolution_reason,
                        created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["map_id"], row["concept_id"], namespace_epoch, row["source_id"],
                        row["source_uri"], row.get("leaf_uri"), row["identity_method"], row["status"],
                        row.get("confidence"), row.get("conflict_set_id"), row.get("owner"),
                        _canonical(row.get("evidence_refs") or []), row["evidence_set_hash"],
                        row.get("next_action"), row.get("expires_at"), _canonical(lineage),
                        row.get("resolved_at"), row.get("resolved_by"), row.get("resolution_reason"),
                        now, now,
                    ),
                )
                inserted += 1
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged}


def verify_closure(
    store: PMSystemStore,
    *,
    closure: Mapping[str, Any],
    namespace_epoch: str,
) -> dict[str, Any]:
    expected = {
        (str(row["concept_id"]), str(row["source_uri"])): row
        for row in closure.get("rows") or []
        if isinstance(row, Mapping)
    }
    errors: list[str] = []
    counts: dict[str, int] = {}
    retained_historical_rows = 0
    retained_historical_errors: list[str] = []
    with store.connect() as connection:
        admission = connection.execute(
            "SELECT admission_state,version FROM concept_admissions WHERE namespace_epoch=?",
            (namespace_epoch,),
        ).fetchone()
        rows = connection.execute(
            "SELECT * FROM concept_source_map WHERE namespace_epoch=?",
            (namespace_epoch,),
        ).fetchall()
        actual = {(str(row["concept_id"]), str(row["source_uri"])): row for row in rows}
        for key, wanted in expected.items():
            row = actual.get(key)
            if row is None:
                errors.append(f"missing:{key[0]}:{key[1]}")
                continue
            status = str(row["status"])
            counts[status] = counts.get(status, 0) + 1
            if status != wanted["status"]:
                errors.append(f"status_mismatch:{key[0]}:{key[1]}")
            if row["evidence_set_hash"] != wanted["evidence_set_hash"]:
                errors.append(f"evidence_mismatch:{key[0]}:{key[1]}")
            if status == "mapped":
                if not row["leaf_uri"] or row["source_id"] == "unknown":
                    errors.append(f"mapped_evidence_incomplete:{key[0]}:{key[1]}")
            elif status == "quarantined":
                if not row["owner"] or not row["next_action"] or not row["expires_at"]:
                    errors.append(f"quarantine_metadata_incomplete:{key[0]}:{key[1]}")
                try:
                    lineage = json.loads(str(row["lineage_json"] or "{}"))
                except json.JSONDecodeError:
                    lineage = {}
                if not isinstance(lineage.get("escalation"), Mapping):
                    errors.append(f"quarantine_escalation_missing:{key[0]}:{key[1]}")
        # Source-map rows are append-only evidence.  A newer closure can
        # replace a URI or normalize an identity while the prior row remains
        # available for rollback/audit.  Count such rows separately instead
        # of treating them as current closure corruption; reject only extra
        # rows that lack a verifiable prior closure lineage.
        stale = sorted(set(actual) - set(expected))
        for key in stale:
            row = actual[key]
            try:
                lineage = json.loads(str(row["lineage_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                lineage = {}
            prior_hash = str(lineage.get("closure_hash") or "")
            if (
                prior_hash.startswith("sha256:")
                and prior_hash != str(closure.get("closure_hash") or "")
                and str(row["status"]) in {"mapped", "quarantined"}
            ):
                retained_historical_rows += 1
            else:
                retained_historical_errors.append(
                    f"unaccounted_extra_source_map_row:{row['map_id']}"
                )
        if retained_historical_errors:
            errors.extend(retained_historical_errors[:100])
        if admission is None or str(admission[0]) != "disabled":
            errors.append("concept_admission_not_disabled")
        naked = sum(1 for row in rows if str(row["status"]) in {"unmapped", "conflict", "unknown"})
        if naked:
            errors.append(f"naked_nonterminal_rows:{naked}")
    return {
        "status": "PASS" if not errors else "HOLD",
        "expected_reference_count": len(expected),
        "database_reference_count": len(actual),
        "status_counts": counts,
        "stale_row_count": len(stale),
        "retained_historical_row_count": retained_historical_rows,
        "unaccounted_extra_row_count": len(retained_historical_errors),
        "naked_nonterminal_count": naked,
        "concept_admission": dict(admission) if admission is not None else None,
        "errors": errors[:100],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = args.db_path.expanduser().resolve()
    manifest_path = args.manifest.expanduser().resolve()
    readback_path = args.readback.expanduser().resolve()
    evidence_path = args.evidence_output.expanduser().resolve()
    store = PMSystemStore(db_path)
    manifest = _read_json(manifest_path)
    readback = _read_json(readback_path)
    closure = build_closure(
        manifest=manifest,
        manifest_hash=_file_hash(manifest_path),
        readback=readback,
        readback_hash=_file_hash(readback_path),
        namespace_epoch=args.namespace_epoch,
        owner=args.owner,
    )
    write_manifest(evidence_path, closure)
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "stage_id": STAGE_ID,
        "mode": "apply" if args.apply else "dry_run",
        "status": "DRY_RUN",
        "migration_id": args.migration_id,
        "migration_epoch": args.namespace_epoch,
        "runtime_epoch": None,
        "evidence_manifest": {
            "path": str(evidence_path),
            "sha256": _file_hash(evidence_path),
            "closure_hash": closure["closure_hash"],
            "reference_count": closure["reference_count"],
            "terminal_status_counts": closure["terminal_status_counts"],
        },
        "external_provider_calls": 0,
        "openviking_writes": 0,
        "concept_admission_target": "disabled",
    }
    if not args.apply:
        return result

    runtime_epoch = str(
        args.runtime_epoch
        or ((store.migration_freeze() or {}).get("migration_epoch") or args.namespace_epoch)
    )
    result["runtime_epoch"] = runtime_epoch
    before = foundation_check(store, expected_epoch=runtime_epoch)
    result["before"] = before
    if before["status"] != "PASS":
        result.update({"status": "HOLD", "errors": ["foundation_check_failed"]})
        return result
    lease = store.acquire_migration_lease(
        migration_id=args.migration_id,
        stage_id=STAGE_ID,
        migration_epoch=args.namespace_epoch,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
    )
    try:
        backup = _copy_backup(
            db_path,
            args.backup_root.expanduser().resolve(),
            args.migration_id,
            stage_id=STAGE_ID,
            migration_epoch=args.namespace_epoch,
        )
        applied = apply_closure(
            store,
            closure=closure,
            namespace_epoch=args.namespace_epoch,
            owner=args.owner,
            lease_id=str(lease["lease_id"]),
        )
    finally:
        released = store.release_migration_lease(lease_id=str(lease["lease_id"]))
        lease["state"] = "released" if released else "release_unknown"
        if released:
            lease["released_at"] = now_iso()
    verification = verify_closure(store, closure=closure, namespace_epoch=args.namespace_epoch)
    after = foundation_check(store, expected_epoch=runtime_epoch)
    quarantine_count = int(closure["terminal_status_counts"].get("quarantined") or 0)
    passed = verification["status"] == "PASS" and after["status"] == "PASS" and lease["state"] == "released"
    result.update(
        {
            "status": ("PASS_WITH_QUARANTINE" if quarantine_count else "PASS") if passed else "HOLD",
            "stage_lease": lease,
            "backup": backup,
            "apply_result": applied,
            "verification": verification,
            "after": after,
            "concept_schema_v2": schema_v2_state(store),
            "next_gate": (
                "C8 may select only concepts whose every source reference remains mapped; "
                "quarantined references are excluded from admission"
            ),
        }
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--readback", type=Path, required=True)
    parser.add_argument("--evidence-output", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--migration-id", default="concept-v11-c7-source-map-20260831")
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE_EPOCH)
    parser.add_argument("--runtime-epoch")
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    result = run(args)
    if args.report:
        write_manifest(args.report.expanduser().resolve(), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in {"DRY_RUN", "PASS", "PASS_WITH_QUARANTINE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
