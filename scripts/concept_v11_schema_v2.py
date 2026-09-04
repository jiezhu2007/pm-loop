#!/usr/bin/env python3
"""Concept schema v2 physical corrections.

Schema v1 is retained as an immutable historical baseline.  This module only
performs additive/repair work in the existing PM SQLite database; it does not
create a second coordination store, queue, or writer.  All mutations happen
inside one short SQLite transaction and are safe to replay after an
interrupted process.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any, Dict, Mapping, Optional

from concept_v11_schema import CONCEPT_SCHEMA_VERSION as LEGACY_SCHEMA_VERSION
from pm_system_store import PMSystemStore, now_iso


TARGET_SCHEMA_VERSION = 2
TARGET_SCHEMA_ID = "pm-concept.schema.v2.2"
DEFAULT_PENDING_SOFT_LIMIT = 2
CONCEPT_ADMISSION_STATES = {"disabled", "shadow", "canary", "incremental", "hold"}
ADMISSION_RENEWAL_POLICIES = {"snapshot_ttl", "continuous"}


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


V2_DDL = (
    """
    CREATE TABLE IF NOT EXISTS concept_admission_events (
        event_id TEXT PRIMARY KEY,
        namespace_epoch TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL CHECK(to_state IN ('disabled','shadow','canary','incremental','hold')),
        expected_version INTEGER,
        new_version INTEGER NOT NULL,
        admission_snapshot_id TEXT,
        policy_version TEXT,
        operator TEXT NOT NULL,
        evidence_hash TEXT,
        renewal_policy TEXT NOT NULL DEFAULT 'snapshot_ttl',
        reason TEXT NOT NULL DEFAULT '',
        observed_at TEXT NOT NULL,
        UNIQUE(namespace_epoch, new_version)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_concept_admission_events_epoch ON concept_admission_events(namespace_epoch, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_concept_hot_projection_epoch ON concept_hot_projection(namespace_epoch, projection_state, updated_at)",
)


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _hot_is_composite(connection: sqlite3.Connection) -> bool:
    rows = connection.execute("PRAGMA table_info(concept_hot_projection)").fetchall()
    keys = {str(row[1]): int(row[5]) for row in rows}
    return keys.get("concept_id") == 1 and keys.get("namespace_epoch") == 2


def _rebuild_hot_projection(connection: sqlite3.Connection) -> bool:
    """Rebuild the v1 single-key table while preserving every row and id."""
    if _hot_is_composite(connection):
        return False
    if not _table_exists(connection, "concept_hot_projection"):
        raise RuntimeError("concept_hot_projection is missing; run schema v1 first")
    columns = _columns(connection, "concept_hot_projection")
    provenance_expr = "provenance" if "provenance" in columns else "'runtime'"
    connection.execute(
        """
        CREATE TABLE concept_hot_projection_v2 (
            concept_id TEXT NOT NULL,
            namespace_epoch TEXT NOT NULL,
            generation_id TEXT NOT NULL,
            projection_state TEXT NOT NULL,
            outbox_item_id TEXT,
            observed_content_hash TEXT,
            observed_at TEXT,
            provenance TEXT NOT NULL DEFAULT 'runtime',
            updated_at TEXT NOT NULL,
            PRIMARY KEY(concept_id, namespace_epoch)
        )
        """
    )
    connection.execute(
        f"""
        INSERT INTO concept_hot_projection_v2(
            concept_id,namespace_epoch,generation_id,projection_state,outbox_item_id,
            observed_content_hash,observed_at,provenance,updated_at
        )
        SELECT concept_id,namespace_epoch,generation_id,projection_state,outbox_item_id,
               observed_content_hash,observed_at,{provenance_expr},updated_at
        FROM concept_hot_projection
        """
    )
    connection.execute("DROP TABLE concept_hot_projection")
    connection.execute("ALTER TABLE concept_hot_projection_v2 RENAME TO concept_hot_projection")
    return True


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> bool:
    if column in _columns(connection, table):
        return False
    connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    return True


def _legacy_provenance(connection: sqlite3.Connection) -> Dict[str, int]:
    changed: Dict[str, int] = {}
    for table, predicate in (
        ("concept_versions", "generation_id LIKE 'legacy-import-%' OR compiler_version='legacy-import'"),
        ("concept_publish_ledger", "current_generation LIKE 'legacy-import-%' OR desired_hot_generation LIKE 'legacy-import-%'"),
        ("concept_hot_projection", "generation_id LIKE 'legacy-import-%'"),
    ):
        cursor = connection.execute(
            f"UPDATE {table} SET provenance='legacy_import' WHERE ({predicate}) AND COALESCE(provenance,'runtime') <> 'legacy_import'"
        )
        changed[table] = int(cursor.rowcount)
    # A legacy import is a historical baseline, not evidence that the
    # production registry has applied a Hot generation.  Keep the row and its
    # original generation, but make the projection state explicit.
    for table in ("concept_publish_ledger", "concept_hot_projection"):
        cursor = connection.execute(
            f"UPDATE {table} SET projection_state='legacy_imported' WHERE provenance='legacy_import' AND projection_state='applied'"
        )
        changed[f"{table}:state"] = int(cursor.rowcount)
    return changed


def _ensure_baseline_admission_event(connection: sqlite3.Connection, namespace_epoch: str) -> bool:
    row = connection.execute(
        "SELECT admission_state,COALESCE(version,1),admission_snapshot_id,policy_version,operator,evidence_hash,renewal_policy,reason,updated_at FROM concept_admissions WHERE namespace_epoch=?",
        (namespace_epoch,),
    ).fetchone()
    if row is None:
        return False
    exists = connection.execute(
        "SELECT 1 FROM concept_admission_events WHERE namespace_epoch=? AND new_version=?",
        (namespace_epoch, int(row[1])),
    ).fetchone()
    if exists is not None:
        return False
    connection.execute(
        "INSERT INTO concept_admission_events(event_id,namespace_epoch,from_state,to_state,expected_version,new_version,admission_snapshot_id,policy_version,operator,evidence_hash,renewal_policy,reason,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            f"admission-baseline-{namespace_epoch}-{int(row[1])}",
            namespace_epoch,
            None,
            str(row[0]),
            None,
            int(row[1]),
            row[2],
            row[3],
            str(row[4] or "migration"),
            row[5],
            str(row[6] or "snapshot_ttl"),
            str(row[7] or "schema-v2 baseline"),
            str(row[8] or now_iso()),
        ),
    )
    return True


def _bind_profile_policy_unlocked(
    connection: sqlite3.Connection,
    *,
    namespace_epoch: str,
    profile: str = "pm-semantic",
    policy_version: Optional[str] = None,
) -> Dict[str, Any]:
    policies = connection.execute(
        "SELECT policy_version,policy_hash,provider,requested_model FROM concept_model_policies "
        "WHERE status='active' ORDER BY policy_version"
    ).fetchall()
    if len(policies) != 1:
        return {"bound": False, "reason": f"active_policy_not_unique:{len(policies)}"}
    policy = policies[0]
    if policy_version and str(policy[0]) != str(policy_version):
        return {"bound": False, "reason": "active_policy_version_mismatch"}
    if str(policy[2]) != "oneapi" or str(policy[3]) != "auto" or not str(policy[1] or ""):
        return {"bound": False, "reason": "active_policy_not_oneapi_auto"}
    admission = connection.execute(
        "SELECT admission_state FROM concept_admissions WHERE namespace_epoch=?",
        (namespace_epoch,),
    ).fetchone()
    if admission is None or str(admission[0]) not in {"disabled", "hold"}:
        return {"bound": False, "reason": "admission_must_be_disabled_or_hold"}
    row = connection.execute(
        "SELECT pending_count,policy_hash FROM concept_profile_admissions "
        "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
        (profile, namespace_epoch),
    ).fetchone()
    if row is None:
        return {"bound": False, "reason": "profile_missing"}
    if int(row[0] or 0) != 0:
        return {"bound": False, "reason": "profile_not_empty"}
    current_hash = str(row[1] or "")
    target_hash = str(policy[1])
    if current_hash and current_hash != target_hash:
        return {"bound": False, "reason": "profile_policy_hash_conflict", "current_policy_hash": current_hash}
    if current_hash == target_hash:
        return {
            "bound": True,
            "deduplicated": True,
            "policy_version": str(policy[0]),
            "policy_hash": target_hash,
        }
    connection.execute(
        "UPDATE concept_profile_admissions SET policy_hash=?,updated_at=? "
        "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=? AND policy_hash IS NULL",
        (target_hash, now_iso(), profile, namespace_epoch),
    )
    return {
        "bound": True,
        "deduplicated": False,
        "policy_version": str(policy[0]),
        "policy_hash": target_hash,
    }


def bind_profile_policy(
    store: PMSystemStore,
    *,
    namespace_epoch: str,
    profile: str = "pm-semantic",
    policy_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Bind an empty disabled/hold profile to the one active model policy."""
    with store.transaction() as connection:
        result = _bind_profile_policy_unlocked(
            connection,
            namespace_epoch=namespace_epoch,
            profile=profile,
            policy_version=policy_version,
        )
        if not result.get("bound"):
            raise RuntimeError(f"concept profile policy bind blocked: {result.get('reason')}")
        return result


def schema_v2_state(store: PMSystemStore) -> Dict[str, Any]:
    with store.connect() as connection:
        meta = connection.execute(
            "SELECT schema_version,schema_id,migration_id,migration_epoch,ddl_sha256,applied_at FROM concept_schema_meta WHERE schema_version=?",
            (TARGET_SCHEMA_VERSION,),
        ).fetchone()
        table_info = {table: sorted(_columns(connection, table)) for table in ("concept_admissions", "concept_profile_admissions", "concept_versions", "concept_publish_ledger", "concept_hot_projection") if _table_exists(connection, table)}
        legacy_rows = {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE provenance='legacy_import'").fetchone()[0])
            for table in ("concept_versions", "concept_publish_ledger", "concept_hot_projection")
            if "provenance" in _columns(connection, table)
        }
        events = int(connection.execute("SELECT COUNT(*) FROM concept_admission_events").fetchone()[0]) if _table_exists(connection, "concept_admission_events") else 0
        return {
            "schema_version": int(meta[0]) if meta is not None else 0,
            "schema_id": meta[1] if meta is not None else None,
            "migration_id": meta[2] if meta is not None else None,
            "migration_epoch": meta[3] if meta is not None else None,
            "ddl_sha256": meta[4] if meta is not None else None,
            "hot_projection_composite_key": _hot_is_composite(connection) if _table_exists(connection, "concept_hot_projection") else False,
            "admission_events": events,
            "legacy_provenance_rows": legacy_rows,
            "tables": table_info,
        }


def ddl_hash() -> str:
    return _hash({"schema_id": TARGET_SCHEMA_ID, "ddl": [statement.strip() for statement in V2_DDL], "hot_key": ["concept_id", "namespace_epoch"], "legacy_provenance": True, "admission_renewal_policy": True})


def migrate_schema_v2(
    store: PMSystemStore,
    *,
    migration_id: str,
    migration_epoch: str,
    owner: str,
    lease_id: Optional[str] = None,
    pending_soft_limit: int = DEFAULT_PENDING_SOFT_LIMIT,
) -> Dict[str, Any]:
    if not migration_id or not migration_epoch or not owner:
        raise ValueError("migration_id, migration_epoch and owner are required")
    if int(pending_soft_limit) <= 0:
        raise ValueError("pending_soft_limit must be positive")
    before = schema_v2_state(store)
    changed: Dict[str, Any] = {}
    with store.transaction() as connection:
        freeze = store._freeze_blocks(connection)
        if freeze is not None:
            raise RuntimeError("concept schema v2 migration cannot run while PM Runtime is frozen")
        if lease_id:
            lease = connection.execute(
                "SELECT migration_epoch,owner,state,lease_expires_at FROM migration_leases WHERE lease_id=?",
                (lease_id,),
            ).fetchone()
            if lease is None or lease[2] != "active" or lease[0] != migration_epoch or lease[1] != owner:
                raise RuntimeError("concept v2 stage lease is missing, expired, or mismatched")
        baseline = connection.execute(
            "SELECT schema_version FROM concept_schema_meta WHERE schema_version=?",
            (LEGACY_SCHEMA_VERSION,),
        ).fetchone()
        if baseline is None:
            raise RuntimeError("concept schema v1 baseline is required before v2")
        for table in ("concept_admissions", "concept_profile_admissions", "concept_versions", "concept_publish_ledger", "concept_hot_projection", "concept_model_resolutions"):
            if not _table_exists(connection, table):
                raise RuntimeError(f"concept table missing: {table}")

        changed["profile_soft_limit_added"] = _ensure_column(
            connection, "concept_profile_admissions", "pending_soft_limit", f"INTEGER NOT NULL DEFAULT {int(pending_soft_limit)} CHECK(pending_soft_limit > 0)"
        )
        # Existing profiles get a conservative, explicit limit.  High-water is
        # left untouched as an observation metric.
        connection.execute(
            "UPDATE concept_profile_admissions SET pending_soft_limit=? WHERE pending_soft_limit IS NULL OR pending_soft_limit<=0",
            (int(pending_soft_limit),),
        )
        changed["admission_version_added"] = _ensure_column(connection, "concept_admissions", "version", "INTEGER NOT NULL DEFAULT 1 CHECK(version > 0)")
        changed["admission_renewal_policy_added"] = _ensure_column(
            connection,
            "concept_admissions",
            "renewal_policy",
            "TEXT NOT NULL DEFAULT 'snapshot_ttl' CHECK(renewal_policy IN ('snapshot_ttl','continuous'))",
        )
        connection.execute(
            "UPDATE concept_admissions SET renewal_policy='snapshot_ttl' "
            "WHERE renewal_policy IS NULL OR renewal_policy NOT IN ('snapshot_ttl','continuous')"
        )
        for table in ("concept_versions", "concept_publish_ledger", "concept_hot_projection"):
            changed[f"{table}_provenance_added"] = _ensure_column(connection, table, "provenance", "TEXT NOT NULL DEFAULT 'runtime'")
        changed["hot_projection_rebuilt"] = _rebuild_hot_projection(connection)
        for statement in V2_DDL:
            connection.execute(statement)
        changed["admission_event_renewal_policy_added"] = _ensure_column(
            connection,
            "concept_admission_events",
            "renewal_policy",
            "TEXT NOT NULL DEFAULT 'snapshot_ttl' CHECK(renewal_policy IN ('snapshot_ttl','continuous'))",
        )
        changed["legacy_provenance"] = _legacy_provenance(connection)
        admissions = [str(row[0]) for row in connection.execute("SELECT namespace_epoch FROM concept_admissions").fetchall()]
        changed["baseline_events"] = sum(1 for epoch in admissions if _ensure_baseline_admission_event(connection, epoch))
        changed["profile_policy_binding"] = {
            epoch: _bind_profile_policy_unlocked(connection, namespace_epoch=epoch)
            for epoch in admissions
        }
        connection.execute(
            "INSERT INTO concept_schema_meta(schema_version,schema_id,migration_id,migration_epoch,ddl_sha256,applied_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(schema_version) DO UPDATE SET schema_id=excluded.schema_id,migration_id=excluded.migration_id,migration_epoch=excluded.migration_epoch,ddl_sha256=excluded.ddl_sha256",
            (TARGET_SCHEMA_VERSION, TARGET_SCHEMA_ID, migration_id, migration_epoch, ddl_hash(), now_iso()),
        )
    after = schema_v2_state(store)
    return {"status": "ok", "before": before, "after": after, "changed": changed, "ddl_sha256": ddl_hash()}


def set_admission_cas(
    store: PMSystemStore,
    *,
    namespace_epoch: str,
    expected_state: str,
    expected_version: int,
    state: str,
    snapshot_id: str,
    policy_version: str,
    operator: str,
    evidence_hash: str,
    reason: str = "",
    ttl_seconds: int = 900,
    renewal_policy: Optional[str] = None,
) -> Dict[str, Any]:
    state = str(state).strip().lower()
    if state not in CONCEPT_ADMISSION_STATES:
        raise ValueError(f"invalid concept admission state: {state}")
    required = (namespace_epoch, expected_state, snapshot_id, policy_version, operator, evidence_hash)
    if any(not str(value).strip() for value in required):
        raise ValueError("namespace_epoch, expected state, snapshot, policy, operator and evidence are required")
    if int(expected_version) <= 0:
        raise ValueError("expected_version must be positive")
    from datetime import datetime, timedelta, timezone

    policy = str(renewal_policy or ("continuous" if state == "incremental" else "snapshot_ttl")).strip().lower()
    if policy not in ADMISSION_RENEWAL_POLICIES:
        raise ValueError(f"invalid admission renewal policy: {policy}")
    if policy == "continuous" and state != "incremental":
        raise ValueError("continuous admission renewal policy is only valid for incremental")
    at = now_iso()
    expires = None if policy == "continuous" else (
        datetime.now(timezone.utc) + timedelta(seconds=max(1, int(ttl_seconds)))
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    event_id = f"admission-{uuid.uuid4().hex}"
    with store.transaction() as connection:
        row = connection.execute(
            "SELECT admission_state,version FROM concept_admissions WHERE namespace_epoch=?",
            (namespace_epoch,),
        ).fetchone()
        if row is None:
            raise RuntimeError("concept admission namespace is not initialized")
        current_state, current_version = str(row[0]), int(row[1])
        if current_state != expected_state or current_version != int(expected_version):
            raise RuntimeError(f"admission CAS mismatch: expected {expected_state}/{expected_version}, got {current_state}/{current_version}")
        new_version = current_version + 1
        cursor = connection.execute(
            "UPDATE concept_admissions SET admission_state=?,admission_snapshot_id=?,policy_version=?,operator=?,observed_at=?,expires_at=?,evidence_hash=?,renewal_policy=?,reason=?,version=?,updated_at=? WHERE namespace_epoch=? AND admission_state=? AND version=?",
            (state, snapshot_id, policy_version, operator, at, expires, evidence_hash, policy, reason, new_version, at, namespace_epoch, expected_state, int(expected_version)),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("admission CAS lost race")
        connection.execute(
            "INSERT INTO concept_admission_events(event_id,namespace_epoch,from_state,to_state,expected_version,new_version,admission_snapshot_id,policy_version,operator,evidence_hash,renewal_policy,reason,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, namespace_epoch, expected_state, state, int(expected_version), new_version, snapshot_id, policy_version, operator, evidence_hash, policy, reason, at),
        )
    with store.connect() as connection:
        result = connection.execute("SELECT * FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)).fetchone()
    return dict(result) if result is not None else {}


def admission_is_live(admission: Mapping[str, Any], *, at: Optional[str] = None) -> bool:
    """Return whether a persisted Admission currently authorizes execution.

    Canary remains a short-lived experiment.  Only an explicitly-recorded
    incremental/continuous transition is durable; all legacy rows preserve
    the historical snapshot-TTL behaviour.
    """
    state = str(admission.get("admission_state") or "").strip()
    policy = str(admission.get("renewal_policy") or "snapshot_ttl").strip()
    if state == "incremental" and policy == "continuous":
        return True
    if state not in {"canary", "incremental"} or policy != "snapshot_ttl":
        return False
    expires = str(admission.get("expires_at") or "")
    return bool(expires and expires > str(at or now_iso()))


def profile_accept_v2(store: PMSystemStore, *, workload: str, profile: str, namespace_epoch: str) -> Dict[str, Any]:
    """Reserve capacity using the soft limit for backpressure and hard cap for safety."""
    with store.transaction() as connection:
        row = connection.execute(
            "SELECT pending_count,pending_soft_limit,pending_high_water,outbox_hard_cap,pause_fence,throttle_until FROM concept_profile_admissions WHERE workload=? AND profile=? AND namespace_epoch=?",
            (workload, profile, namespace_epoch),
        ).fetchone()
        if row is None:
            raise RuntimeError("concept profile admission is not initialized")
        pending, soft, high, hard, fence, throttle = map(lambda value: int(value) if isinstance(value, int) else value, row)
        if fence != "open":
            return {"accepted": False, "reason": "pause_fence", "pending_count": int(pending), "pending_soft_limit": int(soft), "outbox_hard_cap": int(hard)}
        if throttle and str(throttle) > now_iso():
            return {"accepted": False, "reason": "throttle", "pending_count": int(pending), "pending_soft_limit": int(soft), "outbox_hard_cap": int(hard)}
        if int(pending) >= int(hard):
            return {"accepted": False, "reason": "hard_cap", "pending_count": int(pending), "pending_soft_limit": int(soft), "outbox_hard_cap": int(hard)}
        if int(pending) >= int(soft):
            return {"accepted": False, "reason": "soft_limit", "pending_count": int(pending), "pending_soft_limit": int(soft), "outbox_hard_cap": int(hard)}
        new_count = int(pending) + 1
        connection.execute(
            "UPDATE concept_profile_admissions SET pending_count=?,pending_high_water=MAX(pending_high_water,?),updated_at=? WHERE workload=? AND profile=? AND namespace_epoch=?",
            (new_count, new_count, now_iso(), workload, profile, namespace_epoch),
        )
        return {"accepted": True, "pending_count": new_count, "pending_soft_limit": int(soft), "pending_high_water": max(int(high), new_count), "outbox_hard_cap": int(hard)}


def record_model_resolution_append(store: PMSystemStore, resolution: Mapping[str, Any]) -> Dict[str, Any]:
    """Append one model resolution; identical replays are idempotent.

    A different payload for the same (run, stage, attempt) is a provenance
    conflict and must be quarantined by the caller rather than overwritten.
    """
    values = dict(resolution)
    required = ("resolution_id", "stage", "attempt", "model_requested", "resolution_status", "provider")
    missing = [name for name in required if values.get(name) in (None, "")]
    if missing:
        raise ValueError("missing model resolution fields: " + ",".join(missing))
    fields = ("resolution_id", "run_id", "call_id", "stage", "attempt", "model_requested", "model_resolved", "resolution_status", "policy_version", "provider", "resolution_changed", "model_input_hash", "evidence_hash", "created_at")
    values.setdefault("created_at", now_iso())
    # created_at and the generated resolution_id are provenance metadata, not
    # logical content. Excluding them makes a replay idempotent while all
    # model/policy/evidence fields remain immutable.
    fingerprint = _hash({field: values.get(field) for field in fields[1:-1]})
    with store.transaction() as connection:
        existing = connection.execute(
            "SELECT resolution_id,call_id,model_requested,model_resolved,resolution_status,policy_version,provider,resolution_changed,model_input_hash,evidence_hash,created_at FROM concept_model_resolutions WHERE run_id IS ? AND stage=? AND attempt=?",
            (values.get("run_id"), str(values["stage"]), int(values["attempt"])),
        ).fetchone()
        if existing is not None:
            existing_fingerprint = _hash({
                "run_id": values.get("run_id"),
                "call_id": existing[1],
                "stage": values.get("stage"),
                "attempt": int(values.get("attempt")),
                "model_requested": existing[2],
                "model_resolved": existing[3],
                "resolution_status": existing[4],
                "policy_version": existing[5],
                "provider": existing[6],
                "resolution_changed": existing[7],
                "model_input_hash": existing[8],
                "evidence_hash": existing[9],
            })
            if existing_fingerprint != fingerprint:
                raise RuntimeError("model resolution provenance conflict; quarantine required")
            return {**values, "deduplicated": True, "resolution_fingerprint": existing_fingerprint}
        connection.execute(
            "INSERT INTO concept_model_resolutions(resolution_id,run_id,call_id,stage,attempt,model_requested,model_resolved,resolution_status,policy_version,provider,resolution_changed,model_input_hash,evidence_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(values.get(name) for name in fields),
        )
    return {**values, "deduplicated": False, "resolution_fingerprint": fingerprint}


__all__ = [
    "ADMISSION_RENEWAL_POLICIES",
    "DEFAULT_PENDING_SOFT_LIMIT",
    "TARGET_SCHEMA_ID",
    "TARGET_SCHEMA_VERSION",
    "admission_is_live",
    "bind_profile_policy",
    "ddl_hash",
    "migrate_schema_v2",
    "profile_accept_v2",
    "record_model_resolution_append",
    "schema_v2_state",
    "set_admission_cas",
]
