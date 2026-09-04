#!/usr/bin/env python3
"""Concept v1.1 schema and admission primitives.

The concept domain is an additive extension of the PM coordination database.
It deliberately does not change the core schema version and never creates a
second queue, writer, or database.  All operations are short SQLite
transactions; callers must perform network work after releasing the
transaction.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Optional

from pm_system_store import PMSystemStore, now_iso


CONCEPT_SCHEMA_VERSION = 1
CONCEPT_SCHEMA_ID = "pm-concept.schema.v1.1"
CONCEPT_ADMISSION_STATES = {"disabled", "shadow", "canary", "incremental", "hold"}
CONCEPT_ALLOWED_RUNTIME_STATES = {"canary", "incremental"}


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _future_iso(seconds: int) -> str:
    at = datetime.now(timezone.utc) + timedelta(seconds=max(1, int(seconds)))
    return at.isoformat(timespec="seconds").replace("+00:00", "Z")


DDL = (
    """
    CREATE TABLE IF NOT EXISTS concept_schema_meta (
        schema_version INTEGER PRIMARY KEY,
        schema_id TEXT NOT NULL,
        migration_id TEXT NOT NULL,
        migration_epoch TEXT NOT NULL,
        ddl_sha256 TEXT NOT NULL,
        applied_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_admissions (
        namespace_epoch TEXT PRIMARY KEY,
        admission_state TEXT NOT NULL CHECK(admission_state IN ('disabled','shadow','canary','incremental','hold')),
        admission_snapshot_id TEXT,
        policy_version TEXT,
        operator TEXT,
        observed_at TEXT,
        expires_at TEXT,
        evidence_hash TEXT,
        reason TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_source_map (
        map_id TEXT PRIMARY KEY,
        concept_id TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_uri TEXT NOT NULL,
        leaf_uri TEXT,
        identity_method TEXT NOT NULL,
        status TEXT NOT NULL,
        confidence REAL,
        conflict_set_id TEXT,
        owner TEXT,
        evidence_refs_json TEXT NOT NULL DEFAULT '[]',
        evidence_set_hash TEXT,
        next_action TEXT,
        expires_at TEXT,
        lineage_json TEXT NOT NULL DEFAULT '{}',
        resolved_at TEXT,
        resolved_by TEXT,
        resolution_reason TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(concept_id, namespace_epoch, source_uri)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_candidates (
        candidate_id TEXT PRIMARY KEY,
        concept_id TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        base_generation TEXT,
        proposed_version TEXT,
        content TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        diff_json TEXT NOT NULL DEFAULT '{}',
        evidence_refs_json TEXT NOT NULL DEFAULT '[]',
        evidence_set_hash TEXT,
        quality_score REAL,
        policy_decision TEXT NOT NULL,
        block_reasons_json TEXT NOT NULL DEFAULT '[]',
        model_requested TEXT,
        model_resolved TEXT,
        model_policy_version TEXT,
        status TEXT NOT NULL DEFAULT 'ready_for_review',
        expires_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_versions (
        version_id TEXT PRIMARY KEY,
        concept_id TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        version TEXT NOT NULL,
        generation_id TEXT NOT NULL,
        content TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        source_snapshot_hash TEXT,
        evidence_set_hash TEXT,
        compiler_version TEXT,
        policy_version TEXT,
        status TEXT NOT NULL DEFAULT 'staged',
        created_at TEXT NOT NULL,
        UNIQUE(concept_id, namespace_epoch, version),
        UNIQUE(concept_id, namespace_epoch, content_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_publish_ledger (
        publish_id TEXT PRIMARY KEY,
        concept_id TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        version_id TEXT NOT NULL,
        previous_generation TEXT,
        current_generation TEXT NOT NULL,
        current_hot_generation TEXT,
        desired_hot_generation TEXT NOT NULL,
        projection_state TEXT NOT NULL,
        projection_outbox_id TEXT,
        operator TEXT NOT NULL,
        evidence_hash TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_hot_projection (
        concept_id TEXT PRIMARY KEY,
        namespace_epoch TEXT NOT NULL,
        generation_id TEXT NOT NULL,
        projection_state TEXT NOT NULL,
        outbox_item_id TEXT,
        observed_content_hash TEXT,
        observed_at TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_capability_probes (
        probe_id TEXT PRIMARY KEY,
        probe_type TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        profile TEXT NOT NULL,
        processing_mode TEXT,
        provider TEXT,
        model_policy_version TEXT,
        capability_state TEXT NOT NULL,
        accepted_latency_ms REAL,
        semantic_latency_ms REAL,
        task_id TEXT,
        response_json TEXT NOT NULL DEFAULT '{}',
        evidence_hash TEXT,
        observed_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_profile_admissions (
        workload TEXT NOT NULL,
        profile TEXT NOT NULL,
        namespace_epoch TEXT NOT NULL,
        pending_count INTEGER NOT NULL DEFAULT 0 CHECK(pending_count >= 0),
        pending_high_water INTEGER NOT NULL DEFAULT 0 CHECK(pending_high_water > 0),
        outbox_hard_cap INTEGER NOT NULL DEFAULT 0 CHECK(outbox_hard_cap > 0),
        pause_fence TEXT NOT NULL DEFAULT 'open',
        throttle_until TEXT,
        probe_at TEXT,
        provider_budget_remaining INTEGER,
        policy_hash TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(workload, profile, namespace_epoch)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_model_policies (
        policy_version TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        requested_model TEXT NOT NULL,
        allowed_models_json TEXT NOT NULL DEFAULT '[]',
        capability_class TEXT,
        privacy_scope TEXT,
        cost_limit REAL,
        latency_limit_seconds REAL,
        policy_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_model_resolutions (
        resolution_id TEXT PRIMARY KEY,
        run_id TEXT,
        call_id TEXT,
        stage TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        model_requested TEXT NOT NULL,
        model_resolved TEXT,
        resolution_status TEXT NOT NULL,
        policy_version TEXT,
        provider TEXT NOT NULL,
        resolution_changed INTEGER NOT NULL DEFAULT 0,
        model_input_hash TEXT,
        evidence_hash TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(run_id, stage, attempt)
    )
    """,
)


def ddl_hash() -> str:
    return _sha256(";".join(statement.strip() for statement in DDL))


def concept_tables() -> tuple[str, ...]:
    return (
        "concept_schema_meta",
        "concept_admissions",
        "concept_source_map",
        "concept_candidates",
        "concept_versions",
        "concept_publish_ledger",
        "concept_hot_projection",
        "concept_capability_probes",
        "concept_profile_admissions",
        "concept_model_policies",
        "concept_model_resolutions",
    )


def schema_state(store: PMSystemStore) -> Dict[str, Any]:
    with store.connect() as connection:
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        row = connection.execute("SELECT * FROM concept_schema_meta WHERE schema_version=?", (CONCEPT_SCHEMA_VERSION,)).fetchone() if "concept_schema_meta" in tables else None
        return {
            "schema_version": CONCEPT_SCHEMA_VERSION if row is not None else 0,
            "schema_id": row[1] if row is not None else None,
            "migration_id": row[2] if row is not None else None,
            "migration_epoch": row[3] if row is not None else None,
            "ddl_sha256": row[4] if row is not None else None,
            "tables_present": sorted(set(concept_tables()) & tables),
            "tables_missing": sorted(set(concept_tables()) - tables),
        }


def migrate_schema(
    store: PMSystemStore,
    *,
    migration_id: str,
    migration_epoch: str,
    owner: str,
    lease_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create the additive concept schema under a short concept lease."""
    if not migration_id or not migration_epoch or not owner:
        raise ValueError("migration_id, migration_epoch and owner are required")
    before = schema_state(store)
    with store.transaction() as connection:
        freeze = store._freeze_blocks(connection)
        if freeze is not None:
            raise RuntimeError("concept schema migration cannot run while PM Runtime is frozen")
        if lease_id:
            lease = connection.execute(
                "SELECT migration_epoch,owner,state,lease_expires_at FROM migration_leases WHERE lease_id=?",
                (lease_id,),
            ).fetchone()
            if lease is None or lease[2] != "active" or lease[0] != migration_epoch or lease[1] != owner:
                raise RuntimeError("concept stage lease is missing, expired, or mismatched")
        for statement in DDL:
            connection.execute(statement)
        existing = connection.execute(
            "SELECT schema_id,ddl_sha256,migration_epoch FROM concept_schema_meta WHERE schema_version=?",
            (CONCEPT_SCHEMA_VERSION,),
        ).fetchone()
        if existing is not None and (existing[0] != CONCEPT_SCHEMA_ID or existing[1] != ddl_hash()):
            raise RuntimeError("concept schema checksum mismatch")
        connection.execute(
            "INSERT INTO concept_schema_meta(schema_version,schema_id,migration_id,migration_epoch,ddl_sha256,applied_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(schema_version) DO UPDATE SET migration_id=excluded.migration_id,migration_epoch=excluded.migration_epoch",
            (CONCEPT_SCHEMA_VERSION, CONCEPT_SCHEMA_ID, migration_id, migration_epoch, ddl_hash(), now_iso()),
        )
        connection.execute(
            "INSERT INTO concept_admissions(namespace_epoch,admission_state,reason,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(namespace_epoch) DO NOTHING",
            (migration_epoch, "disabled", "schema_migrated; admission requires shadow and canary", now_iso()),
        )
        connection.execute(
            "INSERT INTO concept_profile_admissions(workload,profile,namespace_epoch,pending_high_water,outbox_hard_cap,pause_fence,policy_hash,updated_at) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(workload,profile,namespace_epoch) DO NOTHING",
            ("concept-semantic", "pm-semantic", migration_epoch, 2, 8, "open", None, now_iso()),
        )
    after = schema_state(store)
    return {"status": "ok", "before": before, "after": after, "ddl_sha256": ddl_hash()}


def get_admission(store: PMSystemStore, namespace_epoch: str) -> Optional[Dict[str, Any]]:
    with store.connect() as connection:
        row = connection.execute("SELECT * FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)).fetchone()
    return dict(row) if row is not None else None


def set_admission(
    store: PMSystemStore,
    *,
    namespace_epoch: str,
    state: str,
    snapshot_id: str,
    policy_version: str,
    operator: str,
    evidence_hash: str,
    reason: str = "",
    ttl_seconds: int = 900,
) -> Dict[str, Any]:
    state = str(state).strip().lower()
    if state not in CONCEPT_ADMISSION_STATES:
        raise ValueError(f"invalid concept admission state: {state}")
    if not snapshot_id or not policy_version or not operator or not evidence_hash:
        raise ValueError("snapshot_id, policy_version, operator and evidence_hash are required")
    at = now_iso()
    expires = _future_iso(ttl_seconds)
    with store.transaction() as connection:
        connection.execute(
            "UPDATE concept_admissions SET admission_state=?,admission_snapshot_id=?,policy_version=?,operator=?,observed_at=?,expires_at=?,evidence_hash=?,reason=?,updated_at=? WHERE namespace_epoch=?",
            (state, snapshot_id, policy_version, operator, at, expires, evidence_hash, reason, at, namespace_epoch),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise RuntimeError("concept admission namespace is not initialized")
    result = get_admission(store, namespace_epoch)
    if result is None:
        raise RuntimeError("concept admission disappeared after update")
    return result


def admission_allows(store: PMSystemStore, namespace_epoch: str, *, now: Optional[str] = None) -> bool:
    current = now or now_iso()
    row = get_admission(store, namespace_epoch)
    if row is None or row.get("admission_state") not in CONCEPT_ALLOWED_RUNTIME_STATES:
        return False
    if row.get("admission_state") == "incremental" and row.get("renewal_policy") == "continuous":
        return True
    expires = str(row.get("expires_at") or "")
    return bool(expires and expires > current)


def profile_accept(
    store: PMSystemStore,
    *,
    workload: str,
    profile: str,
    namespace_epoch: str,
) -> Dict[str, Any]:
    """Atomically reserve one pending concept projection slot."""
    with store.transaction() as connection:
        row = connection.execute(
            "SELECT pending_count,pending_high_water,outbox_hard_cap,pause_fence,throttle_until FROM concept_profile_admissions WHERE workload=? AND profile=? AND namespace_epoch=?",
            (workload, profile, namespace_epoch),
        ).fetchone()
        if row is None:
            raise RuntimeError("concept profile admission is not initialized")
        if row[3] != "open":
            return {"accepted": False, "reason": "pause_fence", "pending_count": int(row[0])}
        if row[4] and row[4] > now_iso():
            return {"accepted": False, "reason": "throttle", "pending_count": int(row[0])}
        if int(row[0]) >= int(row[2]):
            return {"accepted": False, "reason": "hard_cap", "pending_count": int(row[0])}
        new_count = int(row[0]) + 1
        connection.execute(
            "UPDATE concept_profile_admissions SET pending_count=?,pending_high_water=MAX(pending_high_water,?),updated_at=? WHERE workload=? AND profile=? AND namespace_epoch=?",
            (new_count, new_count, now_iso(), workload, profile, namespace_epoch),
        )
        return {"accepted": True, "pending_count": new_count, "hard_cap": int(row[2])}


def profile_complete(store: PMSystemStore, *, workload: str, profile: str, namespace_epoch: str) -> int:
    with store.transaction() as connection:
        connection.execute(
            "UPDATE concept_profile_admissions SET pending_count=MAX(0,pending_count-1),updated_at=? WHERE workload=? AND profile=? AND namespace_epoch=?",
            (now_iso(), workload, profile, namespace_epoch),
        )
        row = connection.execute(
            "SELECT pending_count FROM concept_profile_admissions WHERE workload=? AND profile=? AND namespace_epoch=?",
            (workload, profile, namespace_epoch),
        ).fetchone()
    return int(row[0]) if row is not None else 0


def record_probe(store: PMSystemStore, probe: Mapping[str, Any]) -> Dict[str, Any]:
    values = dict(probe)
    required = ("probe_id", "probe_type", "namespace_epoch", "profile", "capability_state", "observed_at", "expires_at")
    missing = [name for name in required if not str(values.get(name) or "").strip()]
    if missing:
        raise ValueError("missing probe fields: " + ",".join(missing))
    with store.transaction() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO concept_capability_probes(probe_id,probe_type,namespace_epoch,profile,processing_mode,provider,model_policy_version,capability_state,accepted_latency_ms,semantic_latency_ms,task_id,response_json,evidence_hash,observed_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(values.get(name) for name in ("probe_id", "probe_type", "namespace_epoch", "profile", "processing_mode", "provider", "model_policy_version", "capability_state", "accepted_latency_ms", "semantic_latency_ms", "task_id", "response_json", "evidence_hash", "observed_at", "expires_at")),
        )
    return dict(values)


def record_model_policy(store: PMSystemStore, policy: Mapping[str, Any]) -> Dict[str, Any]:
    values = dict(policy)
    version = str(values.get("policy_version") or "").strip()
    if not version:
        raise ValueError("policy_version is required")
    allowed = values.get("allowed_models") if isinstance(values.get("allowed_models"), list) else []
    canonical = _json({k: values.get(k) for k in ("provider", "requested_model", "allowed_models", "capability_class", "privacy_scope", "cost_limit", "latency_limit_seconds")})
    policy_hash = str(values.get("policy_hash") or _sha256(canonical))
    with store.transaction() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO concept_model_policies(policy_version,provider,requested_model,allowed_models_json,capability_class,privacy_scope,cost_limit,latency_limit_seconds,policy_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (version, str(values.get("provider") or "oneapi"), str(values.get("requested_model") or "auto"), _json(allowed), values.get("capability_class"), values.get("privacy_scope"), values.get("cost_limit"), values.get("latency_limit_seconds"), policy_hash, str(values.get("status") or "active"), str(values.get("created_at") or now_iso())),
        )
    return {"policy_version": version, "policy_hash": policy_hash}


def record_model_resolution(store: PMSystemStore, resolution: Mapping[str, Any]) -> Dict[str, Any]:
    values = dict(resolution)
    required = ("resolution_id", "stage", "attempt", "model_requested", "resolution_status", "provider")
    missing = [name for name in required if values.get(name) in (None, "")]
    if missing:
        raise ValueError("missing model resolution fields: " + ",".join(missing))
    with store.transaction() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO concept_model_resolutions(resolution_id,run_id,call_id,stage,attempt,model_requested,model_resolved,resolution_status,policy_version,provider,resolution_changed,model_input_hash,evidence_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(values.get(name) for name in ("resolution_id", "run_id", "call_id", "stage", "attempt", "model_requested", "model_resolved", "resolution_status", "policy_version", "provider", "resolution_changed", "model_input_hash", "evidence_hash", "created_at")),
        )
    return values


__all__ = [
    "CONCEPT_ALLOWED_RUNTIME_STATES",
    "CONCEPT_ADMISSION_STATES",
    "CONCEPT_SCHEMA_ID",
    "CONCEPT_SCHEMA_VERSION",
    "admission_allows",
    "concept_tables",
    "ddl_hash",
    "get_admission",
    "migrate_schema",
    "profile_accept",
    "profile_complete",
    "record_model_policy",
    "record_model_resolution",
    "record_probe",
    "schema_state",
    "set_admission",
]
