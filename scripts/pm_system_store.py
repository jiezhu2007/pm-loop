#!/usr/bin/env python3
"""Durable coordination store for the V4.4 PM system.

This module is intentionally small and local.  It owns the SQLite schema and
transaction boundaries used by the future scheduler/gateway, while the
existing file RunStore remains available as a read-only recovery path.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple, Union


SCHEMA_VERSION = 13
SCHEMA_ID = "pm-system.schema.v1"
DEFAULT_BUSY_TIMEOUT_MS = 5000


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


CANONICAL_TERMINAL_STATUSES = frozenset({"completed", "failed", "dead_letter", "quarantine"})


def canonical_status(status: Any, *, failure_class: Optional[str] = None) -> str:
    """Map legacy/internal terminal labels to the V4.5 public vocabulary."""
    value = str(status or "").strip().lower()
    if value == "permanent_failed" or str(failure_class or "").lower() == "permanent":
        return "failed"
    if value in {"done", "success", "succeeded", "complete"}:
        return "completed"
    if value in {"dead-letter", "deadletter"}:
        return "dead_letter"
    if value in {"quarantined", "isolated"}:
        return "quarantine"
    return value


MIGRATIONS: Tuple[Tuple[int, str, str], ...] = (
    (
        1,
        SCHEMA_ID,
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            job_type TEXT NOT NULL,
            run_id TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 50,
            profile TEXT NOT NULL DEFAULT 'interactive',
            payload_json TEXT NOT NULL DEFAULT '{}',
            attempt INTEGER NOT NULL DEFAULT 0,
            queued_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            next_attempt_at TEXT,
            error_fingerprint TEXT
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            loop_id TEXT NOT NULL,
            status TEXT NOT NULL,
            profile TEXT NOT NULL DEFAULT 'interactive',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            deadline_at TEXT,
            snapshot_id TEXT,
            model_input_hash TEXT,
            error TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );

        CREATE TABLE IF NOT EXISTS run_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            occurred_at TEXT NOT NULL,
            UNIQUE(run_id, seq),
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS checkpoints (
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            checkpoint_key TEXT NOT NULL,
            input_hash TEXT,
            artifact_uri TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(run_id, stage, checkpoint_key),
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS execution_slots (
            slot_id TEXT PRIMARY KEY,
            lease_id TEXT UNIQUE,
            run_id TEXT,
            status TEXT NOT NULL DEFAULT 'free',
            profile TEXT,
            leased_at TEXT,
            heartbeat_at TEXT,
            expires_at TEXT,
            pid INTEGER,
            process_group_id INTEGER,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS model_calls (
            call_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            status TEXT NOT NULL,
            model_input_hash TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            provider TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            artifact_uri TEXT,
            error_fingerprint TEXT,
            UNIQUE(run_id, stage, attempt),
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS provider_buckets (
            provider_key TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            model TEXT NOT NULL,
            throttle_until TEXT,
            circuit_state TEXT NOT NULL DEFAULT 'closed',
            consecutive_429 INTEGER NOT NULL DEFAULT 0,
            last_retry_after TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox_items (
            outbox_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            resource_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            processing_mode TEXT NOT NULL,
            provider TEXT NOT NULL,
            profile TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            attempt INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            error_fingerprint TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS semantic_tasks (
            semantic_task_id TEXT PRIMARY KEY,
            dedupe_key TEXT NOT NULL UNIQUE,
            outbox_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            processing_mode TEXT NOT NULL,
            provider TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            attempt INTEGER NOT NULL DEFAULT 0,
            openviking_task_id TEXT,
            error_fingerprint TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(outbox_id) REFERENCES outbox_items(outbox_id)
        );

        CREATE TABLE IF NOT EXISTS error_events (
            error_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            severity TEXT NOT NULL,
            module TEXT NOT NULL,
            run_id TEXT,
            message TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS module_health_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            module TEXT NOT NULL,
            status TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}',
            source_version TEXT
        );

        CREATE TABLE IF NOT EXISTS metric_rollups (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT NOT NULL,
            bucket_start TEXT NOT NULL,
            value REAL NOT NULL,
            dimensions_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            UNIQUE(metric_name, bucket_start, dimensions_json)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, priority DESC, queued_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_run ON jobs(run_id);
        CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_run_events_cursor ON run_events(occurred_at, event_id);
        CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id, seq);
        CREATE INDEX IF NOT EXISTS idx_checkpoints_updated ON checkpoints(updated_at);
        CREATE INDEX IF NOT EXISTS idx_slots_status ON execution_slots(status, expires_at);
        CREATE INDEX IF NOT EXISTS idx_model_calls_run ON model_calls(run_id, stage, attempt);
        CREATE INDEX IF NOT EXISTS idx_outbox_claim ON outbox_items(status, next_attempt_at, created_at);
        CREATE INDEX IF NOT EXISTS idx_semantic_claim ON semantic_tasks(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_errors_fingerprint ON error_events(fingerprint, occurred_at);
        CREATE INDEX IF NOT EXISTS idx_errors_run ON error_events(run_id, occurred_at);
        CREATE INDEX IF NOT EXISTS idx_health_module ON module_health_snapshots(module, observed_at);
        CREATE INDEX IF NOT EXISTS idx_metrics_name ON metric_rollups(metric_name, bucket_start);
        """,
    ),
    (
        2,
        "pm-system.schema.v2-evidence",
        """
        CREATE TABLE IF NOT EXISTS source_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_revision TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            manifest_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'committed',
            captured_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(source_id, source_revision, content_sha256)
        );

        CREATE TABLE IF NOT EXISTS source_items (
            source_item_id TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            uri TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'verified',
            created_at TEXT NOT NULL,
            UNIQUE(snapshot_id, resource_id, revision_id),
            FOREIGN KEY(snapshot_id) REFERENCES source_snapshots(snapshot_id)
        );

        CREATE TABLE IF NOT EXISTS generations (
            generation_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            generation_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'staged',
            source_watermark TEXT,
            knowledge_watermark TEXT,
            created_at TEXT NOT NULL,
            active_at TEXT,
            UNIQUE(domain, generation_hash)
        );

        CREATE TABLE IF NOT EXISTS evidence_refs (
            evidence_id TEXT PRIMARY KEY,
            generation_id TEXT,
            snapshot_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            evidence_role TEXT NOT NULL,
            excerpt_hash TEXT NOT NULL,
            verified INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(snapshot_id, resource_id, revision_id, evidence_role, excerpt_hash),
            FOREIGN KEY(snapshot_id) REFERENCES source_snapshots(snapshot_id),
            FOREIGN KEY(generation_id) REFERENCES generations(generation_id)
        );

        CREATE TABLE IF NOT EXISTS timeline_events (
            timeline_event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            source_run_id TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            occurred_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_source ON source_snapshots(source_id, captured_at);
        CREATE INDEX IF NOT EXISTS idx_source_items_snapshot ON source_items(snapshot_id, status);
        CREATE INDEX IF NOT EXISTS idx_generations_domain ON generations(domain, status, created_at);
        CREATE INDEX IF NOT EXISTS idx_evidence_generation ON evidence_refs(generation_id, verified);
        CREATE INDEX IF NOT EXISTS idx_timeline_occurred ON timeline_events(occurred_at, timeline_event_id);
        """,
    ),
    (
        3,
        "pm-system.schema.v3-external-task-observations",
        """
        CREATE TABLE IF NOT EXISTS external_task_observations (
            task_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            external_status TEXT NOT NULL,
            classification TEXT NOT NULL,
            resource_uri TEXT,
            created_at TEXT,
            observed_at TEXT NOT NULL,
            payload_sha256 TEXT,
            reason TEXT NOT NULL DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_external_task_classification
            ON external_task_observations(classification, observed_at);
        CREATE INDEX IF NOT EXISTS idx_external_task_resource
            ON external_task_observations(resource_uri, observed_at);
        """,
    ),
    (
        4,
        "pm-system.schema.v4-reliability-fencing",
        """
        CREATE TABLE IF NOT EXISTS outbox_dispatch_leases (
            outbox_id TEXT PRIMARY KEY,
            dispatch_token TEXT NOT NULL UNIQUE,
            owner TEXT NOT NULL,
            leased_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY(outbox_id) REFERENCES outbox_items(outbox_id)
        );

        CREATE TABLE IF NOT EXISTS provider_probe_leases (
            provider_key TEXT PRIMARY KEY,
            probe_token TEXT NOT NULL UNIQUE,
            leased_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY(provider_key) REFERENCES provider_buckets(provider_key)
        );

        CREATE TABLE IF NOT EXISTS provider_rate_limit_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_key TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            retry_after_seconds INTEGER,
            FOREIGN KEY(provider_key) REFERENCES provider_buckets(provider_key)
        );

        CREATE TABLE IF NOT EXISTS cancellation_intents (
            run_id TEXT PRIMARY KEY,
            requested_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            actor TEXT NOT NULL DEFAULT 'scheduler',
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_outbox_dispatch_lease_expiry
            ON outbox_dispatch_leases(expires_at);
        CREATE INDEX IF NOT EXISTS idx_provider_rate_limit_events
            ON provider_rate_limit_events(provider_key, occurred_at);
        """,
    ),
    (
        5,
        "pm-system.schema.v5-semantic-task-observation",
        """
        CREATE TABLE IF NOT EXISTS semantic_task_observations (
            semantic_task_id TEXT PRIMARY KEY,
            observation_attempt INTEGER NOT NULL DEFAULT 0,
            last_observed_at TEXT,
            next_attempt_at TEXT,
            deadline_at TEXT,
            last_error_fingerprint TEXT,
            FOREIGN KEY(semantic_task_id) REFERENCES semantic_tasks(semantic_task_id)
        );

        CREATE INDEX IF NOT EXISTS idx_semantic_observation_due
            ON semantic_task_observations(next_attempt_at, deadline_at);
        """,
    ),
    (
        6,
        "pm-system.schema.v6-outbox-retry-deadline",
        """
        ALTER TABLE outbox_items ADD COLUMN retry_deadline_at TEXT;
        CREATE INDEX IF NOT EXISTS idx_outbox_retry_deadline
            ON outbox_items(status, retry_deadline_at, next_attempt_at);
        """,
    ),
    (
        7,
        "pm-system.schema.v7-runtime-governance",
        """
        -- Applied by _apply_schema_v7 because SQLite table rebuilds and
        -- compatibility checks must be kept in one explicit operation.
        """,
    ),
    (
        8,
        "pm-system.schema.v8-unified-scheduler",
        """
        -- Applied by _apply_schema_v8 so partially completed ALTER TABLE
        -- operations remain repairable on the next open.
        """,
    ),
    (
        9,
        "pm-system.schema.v9-delivery-intent",
        """
        CREATE TABLE IF NOT EXISTS delivery_intents (
            intent_id TEXT PRIMARY KEY,
            schedule_key TEXT NOT NULL,
            period_key TEXT NOT NULL,
            person_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('intent','attempting','uncertain','confirmed','failed_after_effect','suppressed')),
            receipt_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(schedule_key, period_key, person_id),
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_delivery_intents_period
            ON delivery_intents(schedule_key, period_key, status);
        """,
    ),
    (
        10,
        "pm-system.schema.v10-competitive-radar-latest",
        """
        CREATE TABLE IF NOT EXISTS competitive_radar_latest (
            pointer_id INTEGER PRIMARY KEY CHECK (pointer_id = 1),
            run_id TEXT NOT NULL,
            report_uri TEXT NOT NULL,
            html_uri TEXT,
            report_hash TEXT NOT NULL,
            report_status TEXT NOT NULL CHECK (report_status IN ('reviewed','degraded')),
            gate_status TEXT NOT NULL,
            review_run_id TEXT,
            evidence_coverage REAL NOT NULL DEFAULT 0,
            published_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        11,
        "pm-system.schema.v11-scheduled-dependency-events",
        """
        -- Applied by _apply_schema_v11 because SQLite needs an explicit
        -- rebuild to widen the existing schedule_occurrences trigger check.
        """,
    ),
    (
        12,
        "pm-system.schema.v12-concept-refresh-projections",
        """
        -- Applied by _apply_schema_v12 because SQLite needs a table rebuild
        -- to widen the immutable v11 concept-refresh audit checks.
        """,
    ),
    (
        13,
        "pm-system.schema.v13-retention-ledger",
        """
        CREATE TABLE IF NOT EXISTS retention_observer_runs (
            run_id TEXT PRIMARY KEY,
            occurrence_id TEXT,
            artifact_digest TEXT NOT NULL UNIQUE,
            plan_id TEXT NOT NULL UNIQUE,
            snapshot_token TEXT NOT NULL,
            status TEXT NOT NULL,
            source_registry_hash TEXT NOT NULL,
            policy_hash TEXT NOT NULL,
            deletion_capability_hash TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS retention_sources (
            source_id TEXT PRIMARY KEY,
            observer_run_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            inventory_complete INTEGER,
            deletion_conclusion_allowed INTEGER,
            freshness TEXT NOT NULL,
            object_count INTEGER,
            logical_bytes INTEGER,
            allocated_bytes INTEGER,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(observer_run_id) REFERENCES retention_observer_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS retention_inventory (
            observer_run_id TEXT NOT NULL,
            object_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            retention_class TEXT NOT NULL,
            processability TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            allocated_bytes INTEGER NOT NULL,
            due_at TEXT,
            content_hash TEXT,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY(observer_run_id, object_id),
            FOREIGN KEY(observer_run_id) REFERENCES retention_observer_runs(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_retention_inventory_source
            ON retention_inventory(source_id, observed_at DESC);

        CREATE TABLE IF NOT EXISTS retention_unknowns (
            unknown_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            resolved_at TEXT,
            observer_run_id TEXT NOT NULL,
            logical_bytes INTEGER,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(observer_run_id) REFERENCES retention_observer_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS retention_unknown_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            unknown_id TEXT NOT NULL,
            observer_run_id TEXT NOT NULL,
            status TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(unknown_id, observer_run_id, status)
        );

        CREATE TABLE IF NOT EXISTS retention_plans (
            plan_id TEXT PRIMARY KEY,
            observer_run_id TEXT NOT NULL UNIQUE,
            observer_occurrence_id TEXT NOT NULL,
            artifact_digest TEXT NOT NULL,
            status TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            nonce_hash TEXT NOT NULL UNIQUE,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(observer_run_id) REFERENCES retention_observer_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS retention_nonce_consumptions (
            nonce_hash TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL UNIQUE,
            observer_occurrence_id TEXT NOT NULL,
            reclaimer_occurrence_id TEXT NOT NULL,
            artifact_digest TEXT NOT NULL,
            consumed_at TEXT NOT NULL,
            FOREIGN KEY(plan_id) REFERENCES retention_plans(plan_id)
        );

        CREATE TABLE IF NOT EXISTS retention_fence_tokens (
            fencing_token INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS retention_leases (
            scope_key TEXT PRIMARY KEY,
            lease_id TEXT NOT NULL UNIQUE,
            owner TEXT NOT NULL,
            fencing_token INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('active','released','expired')),
            FOREIGN KEY(fencing_token) REFERENCES retention_fence_tokens(fencing_token)
        );
        CREATE INDEX IF NOT EXISTS idx_retention_leases_expiry
            ON retention_leases(state, expires_at);

        CREATE TABLE IF NOT EXISTS retention_actions (
            action_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            plan_id TEXT NOT NULL,
            object_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            action_profile TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('prepared','applied','verified','rolled_back','manual_attention','held')),
            fencing_token INTEGER NOT NULL,
            expected_reclaim_bytes INTEGER NOT NULL DEFAULT 0,
            reclaimed_logical_bytes INTEGER NOT NULL DEFAULT 0,
            reclaimed_allocated_bytes INTEGER NOT NULL DEFAULT 0,
            reason_code TEXT,
            message TEXT NOT NULL DEFAULT '',
            prepared_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(plan_id) REFERENCES retention_plans(plan_id)
        );
        CREATE INDEX IF NOT EXISTS idx_retention_actions_recent
            ON retention_actions(updated_at DESC, state, source_id);

        CREATE TABLE IF NOT EXISTS retention_action_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            state TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(action_id, seq),
            FOREIGN KEY(action_id) REFERENCES retention_actions(action_id)
        );

        CREATE TABLE IF NOT EXISTS retention_holds (
            hold_id TEXT PRIMARY KEY,
            object_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('active','released')),
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            released_at TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS retention_metrics_daily (
            metric_day TEXT NOT NULL,
            source_id TEXT NOT NULL,
            logical_bytes INTEGER,
            allocated_bytes INTEGER,
            object_count INTEGER,
            unknown_count INTEGER,
            observed_at TEXT NOT NULL,
            PRIMARY KEY(metric_day, source_id)
        );
        """,
    ),
)


class StoreUnavailable(RuntimeError):
    """Raised when the coordination DB cannot be opened or migrated."""


class ReadOnlyStoreError(RuntimeError):
    """Raised when a write is attempted through the legacy fallback."""


class MigrationFrozen(RuntimeError):
    """Raised when a durable migration fence blocks a new mutation."""


class MigrationLeaseConflict(RuntimeError):
    """Raised when another owner already holds the stage lease."""


@dataclass(frozen=True)
class StoreOpenResult:
    store: Optional["PMSystemStore"]
    fallback: Optional["LegacyRunStoreReadOnlyAdapter"]
    reason: Optional[str] = None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _has_single_column_unique(connection: sqlite3.Connection, table: str, column: str) -> bool:
    """Return whether a legacy table still has a global one-column UNIQUE.

    ``ALTER TABLE ADD COLUMN`` can leave a v6 inline UNIQUE in place after a
    partially applied v7 migration.  The composite v7 indexes also contain
    the identity column, so inspect their full column list instead of only the
    ``unique`` flag.
    """
    for index in connection.execute(f"PRAGMA index_list({table})").fetchall():
        if len(index) < 3 or int(index[2]) != 1:
            continue
        index_name = str(index[1])
        columns = [str(row[2]) for row in connection.execute(f"PRAGMA index_info({index_name})").fetchall()]
        if columns == [column]:
            return True
    return False


def _source_expr(
    alias: str,
    column: str,
    columns: set[str],
    *,
    default: str = "NULL",
) -> str:
    """Build a safe source expression for a partially upgraded table."""
    return f"{alias}.{column}" if column in columns else default


def _apply_schema_v7(connection: sqlite3.Connection) -> None:
    """Upgrade v6 in one explicit, repeatable operation.

    SQLite cannot drop an inline UNIQUE constraint.  The two affected queue
    tables (and the jobs table, whose key also needs profile/epoch scope) are
    rebuilt with compatible columns, then indexed with the new composite
    identity.  Existing rows retain their IDs and are assigned the legacy
    namespace epoch so callbacks remain readable during the cutover.
    """
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table in ("jobs_v7", "outbox_items_v7", "semantic_tasks_v7"):
            connection.execute(f"DROP TABLE IF EXISTS {table}")

        jobs_columns = _columns(connection, "jobs")
        rebuild_jobs = "namespace_epoch" not in jobs_columns or _has_single_column_unique(connection, "jobs", "idempotency_key")
        if rebuild_jobs:
            connection.execute(
                """CREATE TABLE jobs_v7 (
                    job_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 50,
                    profile TEXT NOT NULL DEFAULT 'interactive',
                    owner TEXT NOT NULL DEFAULT 'pm-system',
                    namespace_epoch TEXT NOT NULL DEFAULT 'v4',
                    deadline_at TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    attempt INTEGER NOT NULL DEFAULT 0,
                    queued_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    next_attempt_at TEXT,
                    error_fingerprint TEXT,
                    terminal_reason TEXT
                )"""
            )
            deadline_expr = _source_expr("j", "deadline_at", jobs_columns, default=_source_expr("r", "deadline_at", _columns(connection, "runs")))
            connection.execute(
                """INSERT INTO jobs_v7(job_id,idempotency_key,job_type,run_id,status,priority,profile,
                    owner,namespace_epoch,deadline_at,payload_json,attempt,queued_at,updated_at,
                    started_at,completed_at,next_attempt_at,error_fingerprint,terminal_reason)
                    SELECT {job_id},{idempotency_key},{job_type},{run_id},{status},{priority},{profile},
                        {owner},{namespace_epoch},{deadline},{payload_json},{attempt},{queued_at},{updated_at},
                        {started_at},{completed_at},{next_attempt_at},{error_fingerprint},{terminal_reason}
                    FROM jobs j LEFT JOIN runs r ON r.job_id=j.job_id""".format(
                    job_id=_source_expr("j", "job_id", jobs_columns),
                    idempotency_key=_source_expr("j", "idempotency_key", jobs_columns),
                    job_type=_source_expr("j", "job_type", jobs_columns),
                    run_id=_source_expr("j", "run_id", jobs_columns),
                    status=_source_expr("j", "status", jobs_columns),
                    priority=_source_expr("j", "priority", jobs_columns, default="50"),
                    profile=_source_expr("j", "profile", jobs_columns, default="'interactive'"),
                    owner=_source_expr("j", "owner", jobs_columns, default="'pm-system'"),
                    namespace_epoch=_source_expr("j", "namespace_epoch", jobs_columns, default="'v4'"),
                    deadline=deadline_expr,
                    payload_json=_source_expr("j", "payload_json", jobs_columns, default="'{}'"),
                    attempt=_source_expr("j", "attempt", jobs_columns, default="0"),
                    queued_at=_source_expr("j", "queued_at", jobs_columns),
                    updated_at=_source_expr("j", "updated_at", jobs_columns),
                    started_at=_source_expr("j", "started_at", jobs_columns),
                    completed_at=_source_expr("j", "completed_at", jobs_columns),
                    next_attempt_at=_source_expr("j", "next_attempt_at", jobs_columns),
                    error_fingerprint=_source_expr("j", "error_fingerprint", jobs_columns),
                    terminal_reason=_source_expr("j", "terminal_reason", jobs_columns),
                )
            )
            connection.execute("DROP TABLE jobs")
            connection.execute("ALTER TABLE jobs_v7 RENAME TO jobs")
        else:
            for column, definition in (
                ("owner", "TEXT NOT NULL DEFAULT 'pm-system'"),
                ("namespace_epoch", "TEXT NOT NULL DEFAULT 'v4'"),
                ("deadline_at", "TEXT"),
                ("terminal_reason", "TEXT"),
            ):
                if column not in _columns(connection, "jobs"):
                    connection.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")

        outbox_columns = _columns(connection, "outbox_items")
        rebuild_outbox = "kind" not in outbox_columns or _has_single_column_unique(connection, "outbox_items", "idempotency_key")
        if rebuild_outbox:
            connection.execute(
                """CREATE TABLE outbox_items_v7 (
                    outbox_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'resource',
                    resource_id TEXT NOT NULL,
                    revision_id TEXT NOT NULL,
                    processing_mode TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    owner TEXT NOT NULL DEFAULT 'pm-system',
                    namespace_epoch TEXT NOT NULL DEFAULT 'v4',
                    deadline_at TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    retry_deadline_at TEXT,
                    error_fingerprint TEXT,
                    terminal_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )
            connection.execute(
                """INSERT INTO outbox_items_v7(outbox_id,idempotency_key,kind,resource_id,revision_id,
                    processing_mode,provider,profile,owner,namespace_epoch,deadline_at,payload_json,status,
                    attempt,next_attempt_at,retry_deadline_at,error_fingerprint,terminal_reason,created_at,updated_at)
                    SELECT {outbox_id},{idempotency_key},{kind},{resource_id},{revision_id},{processing_mode},{provider},
                        {profile},{owner},{namespace_epoch},{deadline_at},{payload_json},{status},{attempt},{next_attempt_at},
                        {retry_deadline_at},{error_fingerprint},{terminal_reason},{created_at},{updated_at}
                    FROM outbox_items o""".format(
                    outbox_id=_source_expr("o", "outbox_id", outbox_columns),
                    idempotency_key=_source_expr("o", "idempotency_key", outbox_columns),
                    kind=_source_expr("o", "kind", outbox_columns, default="'resource'"),
                    resource_id=_source_expr("o", "resource_id", outbox_columns),
                    revision_id=_source_expr("o", "revision_id", outbox_columns),
                    processing_mode=_source_expr("o", "processing_mode", outbox_columns),
                    provider=_source_expr("o", "provider", outbox_columns),
                    profile=_source_expr("o", "profile", outbox_columns, default="'interactive'"),
                    owner=_source_expr("o", "owner", outbox_columns, default="'pm-system'"),
                    namespace_epoch=_source_expr("o", "namespace_epoch", outbox_columns, default="'v4'"),
                    deadline_at=_source_expr("o", "deadline_at", outbox_columns),
                    payload_json=_source_expr("o", "payload_json", outbox_columns, default="'{}'"),
                    status=_source_expr("o", "status", outbox_columns, default="'pending'"),
                    attempt=_source_expr("o", "attempt", outbox_columns, default="0"),
                    next_attempt_at=_source_expr("o", "next_attempt_at", outbox_columns),
                    retry_deadline_at=_source_expr("o", "retry_deadline_at", outbox_columns),
                    error_fingerprint=_source_expr("o", "error_fingerprint", outbox_columns),
                    terminal_reason=_source_expr("o", "terminal_reason", outbox_columns),
                    created_at=_source_expr("o", "created_at", outbox_columns),
                    updated_at=_source_expr("o", "updated_at", outbox_columns),
                )
            )
            connection.execute("DROP TABLE outbox_items")
            connection.execute("ALTER TABLE outbox_items_v7 RENAME TO outbox_items")
        else:
            for column, definition in (
                ("kind", "TEXT NOT NULL DEFAULT 'resource'"),
                ("owner", "TEXT NOT NULL DEFAULT 'pm-system'"),
                ("namespace_epoch", "TEXT NOT NULL DEFAULT 'v4'"),
                ("deadline_at", "TEXT"),
                ("retry_deadline_at", "TEXT"),
                ("terminal_reason", "TEXT"),
            ):
                if column not in _columns(connection, "outbox_items"):
                    connection.execute(f"ALTER TABLE outbox_items ADD COLUMN {column} {definition}")

        semantic_columns = _columns(connection, "semantic_tasks")
        rebuild_semantic = "profile" not in semantic_columns or _has_single_column_unique(connection, "semantic_tasks", "dedupe_key")
        if rebuild_semantic:
            connection.execute(
                """CREATE TABLE semantic_tasks_v7 (
                    semantic_task_id TEXT PRIMARY KEY,
                    dedupe_key TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'semantic',
                    outbox_id TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    revision_id TEXT NOT NULL,
                    processing_mode TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    profile TEXT NOT NULL DEFAULT 'pm-semantic',
                    owner TEXT NOT NULL DEFAULT 'pm-system',
                    namespace_epoch TEXT NOT NULL DEFAULT 'v4',
                    deadline_at TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempt INTEGER NOT NULL DEFAULT 0,
                    openviking_task_id TEXT,
                    error_fingerprint TEXT,
                    terminal_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(outbox_id) REFERENCES outbox_items(outbox_id)
                )"""
            )
            outbox_profile_expr = _source_expr("o", "profile", _columns(connection, "outbox_items"), default="'pm-semantic'")
            connection.execute(
                """INSERT INTO semantic_tasks_v7(semantic_task_id,dedupe_key,kind,outbox_id,resource_id,
                    revision_id,processing_mode,provider,profile,owner,namespace_epoch,deadline_at,status,
                    attempt,openviking_task_id,error_fingerprint,terminal_reason,created_at,updated_at)
                    SELECT {semantic_task_id},{dedupe_key},{kind},{outbox_id},{resource_id},{revision_id},
                        {processing_mode},{provider},{profile},{owner},{namespace_epoch},{deadline_at},{status},
                        {attempt},{openviking_task_id},{error_fingerprint},{terminal_reason},{created_at},{updated_at}
                    FROM semantic_tasks s LEFT JOIN outbox_items o ON o.outbox_id=s.outbox_id""".format(
                    semantic_task_id=_source_expr("s", "semantic_task_id", semantic_columns),
                    dedupe_key=_source_expr("s", "dedupe_key", semantic_columns),
                    kind=_source_expr("s", "kind", semantic_columns, default="'semantic'"),
                    outbox_id=_source_expr("s", "outbox_id", semantic_columns),
                    resource_id=_source_expr("s", "resource_id", semantic_columns),
                    revision_id=_source_expr("s", "revision_id", semantic_columns),
                    processing_mode=_source_expr("s", "processing_mode", semantic_columns),
                    provider=_source_expr("s", "provider", semantic_columns),
                    profile=_source_expr("s", "profile", semantic_columns, default=outbox_profile_expr),
                    owner=_source_expr("s", "owner", semantic_columns, default="'pm-system'"),
                    namespace_epoch=_source_expr("s", "namespace_epoch", semantic_columns, default="'v4'"),
                    deadline_at=_source_expr("s", "deadline_at", semantic_columns),
                    status=_source_expr("s", "status", semantic_columns, default="'queued'"),
                    attempt=_source_expr("s", "attempt", semantic_columns, default="0"),
                    openviking_task_id=_source_expr("s", "openviking_task_id", semantic_columns),
                    error_fingerprint=_source_expr("s", "error_fingerprint", semantic_columns),
                    terminal_reason=_source_expr("s", "terminal_reason", semantic_columns),
                    created_at=_source_expr("s", "created_at", semantic_columns),
                    updated_at=_source_expr("s", "updated_at", semantic_columns),
                )
            )
            connection.execute("DROP TABLE semantic_tasks")
            connection.execute("ALTER TABLE semantic_tasks_v7 RENAME TO semantic_tasks")
        else:
            for column, definition in (
                ("kind", "TEXT NOT NULL DEFAULT 'semantic'"),
                ("profile", "TEXT NOT NULL DEFAULT 'pm-semantic'"),
                ("owner", "TEXT NOT NULL DEFAULT 'pm-system'"),
                ("namespace_epoch", "TEXT NOT NULL DEFAULT 'v4'"),
                ("deadline_at", "TEXT"),
                ("terminal_reason", "TEXT"),
            ):
                if column not in _columns(connection, "semantic_tasks"):
                    connection.execute(f"ALTER TABLE semantic_tasks ADD COLUMN {column} {definition}")

        if "owner" not in _columns(connection, "runs"):
            connection.execute("ALTER TABLE runs ADD COLUMN owner TEXT NOT NULL DEFAULT 'pm-system'")
        if "namespace_epoch" not in _columns(connection, "runs"):
            connection.execute("ALTER TABLE runs ADD COLUMN namespace_epoch TEXT NOT NULL DEFAULT 'v4'")
        if "terminal_reason" not in _columns(connection, "runs"):
            connection.execute("ALTER TABLE runs ADD COLUMN terminal_reason TEXT")
        if "response_state" not in _columns(connection, "model_calls"):
            connection.execute("ALTER TABLE model_calls ADD COLUMN response_state TEXT")
        if "retry_deadline_at" not in _columns(connection, "model_calls"):
            connection.execute("ALTER TABLE model_calls ADD COLUMN retry_deadline_at TEXT")
        if "provider_token_id" not in _columns(connection, "model_calls"):
            connection.execute("ALTER TABLE model_calls ADD COLUMN provider_token_id TEXT")

        ddl = """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_logical_v7
                ON jobs(idempotency_key, profile, namespace_epoch);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_logical_v7
                ON outbox_items(kind, profile, idempotency_key, namespace_epoch);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_logical_v7
                ON semantic_tasks(kind, profile, dedupe_key, namespace_epoch);
            CREATE INDEX IF NOT EXISTS idx_jobs_claim_v7
                ON jobs(status, profile, namespace_epoch, next_attempt_at, priority DESC, queued_at);
            CREATE INDEX IF NOT EXISTS idx_outbox_claim_v7
                ON outbox_items(status, profile, namespace_epoch, next_attempt_at, created_at);
            CREATE INDEX IF NOT EXISTS idx_semantic_claim_v7
                ON semantic_tasks(status, profile, namespace_epoch, updated_at);

            CREATE TABLE IF NOT EXISTS migration_freeze (
                freeze_id INTEGER PRIMARY KEY CHECK (freeze_id = 1),
                migration_id TEXT NOT NULL UNIQUE,
                migration_epoch TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                state TEXT NOT NULL,
                deadline_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS migration_leases (
                migration_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                migration_epoch TEXT NOT NULL,
                owner TEXT NOT NULL,
                lease_id TEXT NOT NULL UNIQUE,
                acquired_at TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'active',
                PRIMARY KEY(migration_id, stage_id)
            );
            CREATE TABLE IF NOT EXISTS provider_tokens (
                token_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                model TEXT NOT NULL,
                owner TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                released_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_provider_tokens_active
                ON provider_tokens(provider, endpoint, model, released_at, expires_at);
            CREATE TABLE IF NOT EXISTS provider_capacity (
                provider TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                model TEXT NOT NULL,
                max_concurrency INTEGER NOT NULL CHECK (max_concurrency > 0),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(provider, endpoint, model)
            );
            CREATE TABLE IF NOT EXISTS watermarks (
                source_domain TEXT NOT NULL,
                watermark_name TEXT NOT NULL,
                captured_at INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                value_hash TEXT NOT NULL,
                value TEXT,
                producer TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'accepted',
                PRIMARY KEY(source_domain, watermark_name)
            );
            CREATE TABLE IF NOT EXISTS watermark_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_domain TEXT NOT NULL,
                watermark_name TEXT NOT NULL,
                captured_at INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                value_hash TEXT NOT NULL,
                state TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS resource_projections (
                resource_id TEXT NOT NULL,
                revision_id TEXT NOT NULL,
                content_state TEXT NOT NULL DEFAULT 'content_pending',
                semantic_state TEXT NOT NULL DEFAULT 'semantic_pending',
                memory_link_state TEXT NOT NULL DEFAULT 'memory_link_pending',
                verified_at TEXT,
                semantic_completed_at TEXT,
                memory_linked_at TEXT,
                last_side_effect_call_id TEXT,
                terminal_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(resource_id, revision_id)
            );
            CREATE TABLE IF NOT EXISTS memory_change_events (
                event_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mtime INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                snapshot_uri TEXT,
                observed_at TEXT NOT NULL,
                namespace_epoch TEXT NOT NULL,
                consumed_at TEXT,
                state TEXT NOT NULL DEFAULT 'pending',
                UNIQUE(name, content_hash, namespace_epoch)
            );
            CREATE TABLE IF NOT EXISTS operation_ledger (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                target_uri TEXT,
                request_hash TEXT,
                namespace_epoch TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                response_state TEXT NOT NULL DEFAULT 'unknown',
                response_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(operation_type, idempotency_key, attempt)
            );
            CREATE TABLE IF NOT EXISTS historical_failure_classifications (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                original_status TEXT NOT NULL,
                failure_class TEXT NOT NULL,
                evidence_hash TEXT,
                classified_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(entity_type, entity_id)
            );
            """
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement:
                connection.execute(statement)
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.DatabaseError:
            pass
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


def _ensure_memory_projection_schema(connection: sqlite3.Connection) -> None:
    """Ensure the file-mirror projection exists without changing core schema v7.

    Memory file mirroring is an additive lane owned by ``MemorySkillWriter``.
    Keeping its state in a separate table prevents Resource/SemanticTask
    projections from accidentally claiming or completing Memory work.
    """
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_projections (
            resource_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            target_uri TEXT NOT NULL,
            content_state TEXT NOT NULL DEFAULT 'content_pending',
            local_hash TEXT NOT NULL,
            remote_hash TEXT,
            openviking_task_id TEXT,
            operation_id TEXT,
            observation_attempt INTEGER NOT NULL DEFAULT 0,
            next_observation_at TEXT,
            observation_deadline_at TEXT,
            terminal_reason TEXT,
            verified_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(resource_id, revision_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_projection_pending "
        "ON memory_projections(content_state, next_observation_at, updated_at)"
    )


def _apply_schema_v8(connection: sqlite3.Connection) -> None:
    """Add the unified scheduler and operations evidence schema.

    The function is intentionally idempotent.  A process can stop after an
    individual column/table DDL statement and the next migration/open repairs
    the remaining pieces without rewriting existing Job/Run rows.
    """
    def add_column(table: str, column: str, definition: str) -> None:
        if column not in _columns(connection, table):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    for table, column, definition in (
        ("jobs", "occurrence_id", "TEXT"),
        ("jobs", "schedule_key", "TEXT"),
        ("jobs", "trigger_kind", "TEXT NOT NULL DEFAULT 'manual'"),
        ("jobs", "registry_hash", "TEXT"),
        ("jobs", "lock_key", "TEXT"),
        ("runs", "occurrence_id", "TEXT"),
        ("runs", "schedule_key", "TEXT"),
        ("runs", "trigger_kind", "TEXT NOT NULL DEFAULT 'manual'"),
        ("runs", "registry_hash", "TEXT"),
    ):
        add_column(table, column, definition)

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS schedule_registry_state (
            registry_id INTEGER PRIMARY KEY CHECK (registry_id = 1),
            registry_version INTEGER NOT NULL,
            registry_hash TEXT NOT NULL,
            source_path TEXT NOT NULL,
            canonical_json TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'valid' CHECK (state IN ('valid','invalid','stale','unknown')),
            error TEXT,
            loaded_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS schedule_occurrences (
            occurrence_id TEXT PRIMARY KEY,
            occurrence_key TEXT NOT NULL UNIQUE,
            schedule_key TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            local_scheduled_at TEXT NOT NULL,
            deadline_at TEXT NOT NULL,
            registry_hash TEXT NOT NULL,
            trigger_kind TEXT NOT NULL DEFAULT 'calendar' CHECK (trigger_kind IN ('calendar','manual_replay')),
            state TEXT NOT NULL DEFAULT 'due' CHECK (state IN ('due','accepted','deferred','running','completed','failed','dead_letter','suppressed','expired')),
            lock_key TEXT NOT NULL,
            job_id TEXT,
            run_id TEXT,
            attempt INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT,
            failure_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_due
            ON schedule_occurrences(state, scheduled_at, deadline_at);
        CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_schedule
            ON schedule_occurrences(schedule_key, scheduled_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_occurrence
            ON jobs(occurrence_id) WHERE occurrence_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_jobs_schedule
            ON jobs(schedule_key, trigger_kind, queued_at);
        CREATE INDEX IF NOT EXISTS idx_runs_schedule
            ON runs(schedule_key, trigger_kind, created_at);

        CREATE TABLE IF NOT EXISTS schedule_leases (
            lock_key TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            lease_id TEXT NOT NULL UNIQUE,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_schedule_leases_expiry
            ON schedule_leases(expires_at);

        CREATE TABLE IF NOT EXISTS scheduler_ticks (
            tick_id TEXT PRIMARY KEY,
            scheduler_id TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (mode IN ('shadow','calendar','catchup','manual_replay')),
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL CHECK (status IN ('running','completed','failed','suppressed')),
            registry_hash TEXT,
            accepted_count INTEGER NOT NULL DEFAULT 0,
            deduplicated_count INTEGER NOT NULL DEFAULT 0,
            deferred_count INTEGER NOT NULL DEFAULT 0,
            expired_count INTEGER NOT NULL DEFAULT 0,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scheduler_ticks_started
            ON scheduler_ticks(started_at DESC, tick_id);

        CREATE TABLE IF NOT EXISTS ops_alerts (
            alert_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            severity TEXT NOT NULL CHECK (severity IN ('P0','P1','P2','P3')),
            alert_type TEXT NOT NULL,
            module TEXT NOT NULL,
            message TEXT NOT NULL,
            occurrence_id TEXT,
            job_id TEXT,
            run_id TEXT,
            state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','acknowledged','resolved','suppressed')),
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            resolved_at TEXT,
            details_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(fingerprint, state)
        );
        CREATE INDEX IF NOT EXISTS idx_ops_alerts_attention
            ON ops_alerts(state, severity, last_seen_at DESC);

        CREATE TABLE IF NOT EXISTS notification_deliveries (
            notification_id TEXT PRIMARY KEY,
            alert_id TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'macos'
                CHECK (channel IN ('macos')),
            fingerprint TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending','sent','failed','deduplicated')),
            requested_at TEXT NOT NULL,
            delivered_at TEXT,
            error TEXT,
            UNIQUE(alert_id, channel, fingerprint),
            FOREIGN KEY(alert_id) REFERENCES ops_alerts(alert_id)
        );
        CREATE INDEX IF NOT EXISTS idx_notification_state
            ON notification_deliveries(state, requested_at DESC);

        -- V1.3 canonical workbench ledger.  These tables stay in the same
        -- coordination database and are additive: older rows remain valid,
        -- while the read model can distinguish a real empty ledger from a
        -- missing migration.
        CREATE TABLE IF NOT EXISTS plans (
            plan_id TEXT PRIMARY KEY,
            plan_type TEXT NOT NULL,
            title TEXT NOT NULL,
            stage TEXT,
            window_start TEXT,
            window_end TEXT,
            timezone TEXT,
            dependencies_json TEXT NOT NULL DEFAULT '[]',
            watermarks_json TEXT NOT NULL DEFAULT '{}',
            feature_gate TEXT,
            status TEXT NOT NULL DEFAULT 'planned'
                CHECK (status IN ('planned','active','completed','failed','unknown')),
            source_ref TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS plan_items (
            plan_item_id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL,
            item_key TEXT NOT NULL,
            item_type TEXT NOT NULL,
            sequence INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'planned',
            job_id TEXT,
            run_id TEXT,
            dependencies_json TEXT NOT NULL DEFAULT '[]',
            source_ref TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(plan_id, item_key),
            FOREIGN KEY(plan_id) REFERENCES plans(plan_id)
        );
        CREATE INDEX IF NOT EXISTS idx_plan_items_plan ON plan_items(plan_id, sequence, item_key);

        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL UNIQUE,
            plan_id TEXT,
            artifact_id TEXT,
            canonical_status TEXT NOT NULL,
            display_status TEXT NOT NULL,
            review_state TEXT NOT NULL,
            publish_state TEXT NOT NULL DEFAULT 'not_applicable',
            conclusion TEXT,
            gate_id TEXT,
            freshness TEXT NOT NULL DEFAULT 'unknown',
            evidence_hash TEXT,
            source_ref TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_reviews_updated ON reviews(updated_at DESC, review_id);

        CREATE TABLE IF NOT EXISTS review_evidence (
            review_id TEXT NOT NULL,
            evidence_id TEXT NOT NULL,
            evidence_ref TEXT NOT NULL,
            evidence_role TEXT,
            source_hash TEXT,
            status TEXT NOT NULL DEFAULT 'observed',
            observed_at TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(review_id, evidence_id),
            FOREIGN KEY(review_id) REFERENCES reviews(review_id)
        );
        CREATE INDEX IF NOT EXISTS idx_review_evidence_review ON review_evidence(review_id, observed_at);

        CREATE TABLE IF NOT EXISTS activity_events (
            activity_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            actor TEXT NOT NULL,
            correlation_id TEXT,
            entity_type TEXT,
            entity_id TEXT,
            run_id TEXT,
            job_id TEXT,
            occurrence_id TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_cursor TEXT,
            occurred_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_activity_occurred ON activity_events(occurred_at DESC, activity_id);
        CREATE INDEX IF NOT EXISTS idx_activity_run ON activity_events(run_id, occurred_at);

        CREATE TABLE IF NOT EXISTS operations (
            operation_id TEXT PRIMARY KEY,
            operation_key TEXT NOT NULL UNIQUE,
            module_id TEXT NOT NULL,
            schedule_key TEXT,
            process TEXT,
            heartbeat_at TEXT,
            lease_id TEXT,
            automation TEXT,
            current_run TEXT,
            last_exit_code INTEGER,
            status TEXT NOT NULL DEFAULT 'unknown',
            freshness TEXT NOT NULL DEFAULT 'unknown',
            reconcile_state TEXT NOT NULL DEFAULT 'unknown',
            incident_ids_json TEXT NOT NULL DEFAULT '[]',
            evidence_refs_json TEXT NOT NULL DEFAULT '[]',
            source_version TEXT,
            observed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_operations_module ON operations(module_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_operations_schedule ON operations(schedule_key, updated_at DESC);

        CREATE TABLE IF NOT EXISTS workbench_gate_manifest (
            gate_id TEXT PRIMARY KEY,
            manifest_version TEXT NOT NULL,
            owner TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            required_checks_json TEXT NOT NULL DEFAULT '[]',
            source_hashes_json TEXT NOT NULL DEFAULT '{}',
            decision TEXT NOT NULL CHECK (decision IN ('enabled','disabled','unknown')),
            protected_modules_json TEXT NOT NULL DEFAULT '[]',
            reason TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_gate_manifest_expiry ON workbench_gate_manifest(expires_at, gate_id);

        CREATE TABLE IF NOT EXISTS competitive_radar_latest (
            pointer_id INTEGER PRIMARY KEY CHECK (pointer_id = 1),
            run_id TEXT NOT NULL,
            report_uri TEXT NOT NULL,
            html_uri TEXT,
            report_hash TEXT NOT NULL,
            report_status TEXT NOT NULL CHECK (report_status IN ('reviewed','degraded')),
            gate_status TEXT NOT NULL,
            review_run_id TEXT,
            evidence_coverage REAL NOT NULL DEFAULT 0,
            published_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )


def _apply_schema_v11(connection: sqlite3.Connection) -> None:
    """Add dependency-trigger evidence without creating another scheduler.

    Version 8 constrained occurrence triggers to calendar/manual replay.  A
    dependency occurrence is still accepted by the same Scheduler and uses
    the same Job/Run path, so the only schema expansion is an append-only
    event inbox plus a disabled-mode concept planning ledger.
    """
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='schedule_occurrences'"
        ).fetchone()
        definition = str(row[0] or "") if row is not None else ""
        if "'dependency'" not in definition:
            connection.execute("DROP TABLE IF EXISTS schedule_occurrences_v11")
            connection.execute(
                """
                CREATE TABLE schedule_occurrences_v11 (
                    occurrence_id TEXT PRIMARY KEY,
                    occurrence_key TEXT NOT NULL UNIQUE,
                    schedule_key TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    local_scheduled_at TEXT NOT NULL,
                    deadline_at TEXT NOT NULL,
                    registry_hash TEXT NOT NULL,
                    trigger_kind TEXT NOT NULL DEFAULT 'calendar'
                        CHECK (trigger_kind IN ('calendar','dependency','manual_replay')),
                    state TEXT NOT NULL DEFAULT 'due'
                        CHECK (state IN ('due','accepted','deferred','running','completed','failed','dead_letter','suppressed','expired')),
                    lock_key TEXT NOT NULL,
                    job_id TEXT,
                    run_id TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    next_retry_at TEXT,
                    failure_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO schedule_occurrences_v11(
                    occurrence_id,occurrence_key,schedule_key,scheduled_at,
                    local_scheduled_at,deadline_at,registry_hash,trigger_kind,
                    state,lock_key,job_id,run_id,attempt,next_retry_at,
                    failure_reason,created_at,updated_at
                )
                SELECT occurrence_id,occurrence_key,schedule_key,scheduled_at,
                       local_scheduled_at,deadline_at,registry_hash,trigger_kind,
                       state,lock_key,job_id,run_id,attempt,next_retry_at,
                       failure_reason,created_at,updated_at
                FROM schedule_occurrences
                """
            )
            connection.execute("DROP TABLE schedule_occurrences")
            connection.execute("ALTER TABLE schedule_occurrences_v11 RENAME TO schedule_occurrences")
        # ``executescript`` commits any active SQLite transaction. Keep the
        # remaining DDL inside the explicit rebuild transaction above.
        ddl = """
            CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_due
                ON schedule_occurrences(state, scheduled_at, deadline_at);
            CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_schedule
                ON schedule_occurrences(schedule_key, scheduled_at DESC);

            CREATE TABLE IF NOT EXISTS scheduled_dependency_events (
                event_id TEXT PRIMARY KEY,
                event_key TEXT NOT NULL UNIQUE,
                dependent_schedule_key TEXT NOT NULL,
                upstream_schedule_key TEXT NOT NULL,
                upstream_occurrence_id TEXT NOT NULL,
                upstream_run_id TEXT NOT NULL,
                upstream_completed_at TEXT NOT NULL,
                source_manifest_path TEXT,
                source_manifest_hash TEXT,
                handler_evidence_path TEXT,
                handler_evidence_hash TEXT,
                planner_version TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','blocked_by_upstream','consumed')),
                reason TEXT NOT NULL DEFAULT '',
                occurrence_id TEXT,
                outcome_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                consumed_at TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_scheduled_dependency_events_pending
                ON scheduled_dependency_events(status, created_at, dependent_schedule_key);
            CREATE INDEX IF NOT EXISTS idx_scheduled_dependency_events_upstream
                ON scheduled_dependency_events(upstream_run_id, dependent_schedule_key);

            CREATE TABLE IF NOT EXISTS concept_refresh_runs (
                plan_id TEXT PRIMARY KEY,
                dependency_event_id TEXT NOT NULL UNIQUE,
                upstream_run_id TEXT NOT NULL,
                namespace_epoch TEXT,
                admission_state TEXT NOT NULL,
                planner_version TEXT NOT NULL,
                source_manifest_path TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                plan_path TEXT NOT NULL,
                plan_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('planned_disabled','blocked')),
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_concept_refresh_runs_created
                ON concept_refresh_runs(created_at DESC, admission_state);

            CREATE TABLE IF NOT EXISTS concept_refresh_items (
                plan_id TEXT NOT NULL,
                concept_id TEXT NOT NULL,
                coverage_status TEXT NOT NULL,
                decision TEXT NOT NULL CHECK (decision IN ('observe_only','blocked')),
                source_count INTEGER NOT NULL DEFAULT 0,
                evidence_hash TEXT,
                reason TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(plan_id, concept_id),
                FOREIGN KEY(plan_id) REFERENCES concept_refresh_runs(plan_id)
            );
            CREATE INDEX IF NOT EXISTS idx_concept_refresh_items_decision
                ON concept_refresh_items(plan_id, decision, coverage_status);
        """
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement:
                connection.execute(statement)
        if "upstream_completed_at" not in _columns(connection, "scheduled_dependency_events"):
            # A previous interrupted development migration may have created
            # this table before its immutable timestamp was introduced.
            connection.execute("ALTER TABLE scheduled_dependency_events ADD COLUMN upstream_completed_at TEXT")
        if "handler_evidence_path" not in _columns(connection, "scheduled_dependency_events"):
            connection.execute("ALTER TABLE scheduled_dependency_events ADD COLUMN handler_evidence_path TEXT")
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.DatabaseError:
            pass
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


def _apply_schema_v12(connection: sqlite3.Connection) -> None:
    """Widen only the concept-refresh audit vocabulary.

    The dependency event, Scheduler, Worker, Outbox, and Writer are unchanged.
    This migration keeps every v11 plan row intact while adding enough durable
    provenance to distinguish an observed disabled plan from an admitted
    isolated/candidate projection.
    """
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        run_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='concept_refresh_runs'"
        ).fetchone()
        run_definition = str(run_sql[0] or "") if run_sql is not None else ""
        if "planned_canary" not in run_definition:
            connection.execute("DROP TABLE IF EXISTS concept_refresh_runs_v12")
            connection.execute(
                """
                CREATE TABLE concept_refresh_runs_v12 (
                    plan_id TEXT PRIMARY KEY,
                    dependency_event_id TEXT NOT NULL UNIQUE,
                    upstream_run_id TEXT NOT NULL,
                    namespace_epoch TEXT,
                    admission_state TEXT NOT NULL,
                    planner_version TEXT NOT NULL,
                    source_manifest_path TEXT NOT NULL,
                    source_manifest_hash TEXT NOT NULL,
                    plan_path TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('planned_disabled','planned_canary','planned_incremental','blocked')),
                    reason TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO concept_refresh_runs_v12(
                    plan_id,dependency_event_id,upstream_run_id,namespace_epoch,
                    admission_state,planner_version,source_manifest_path,
                    source_manifest_hash,plan_path,plan_hash,status,reason,created_at
                )
                SELECT plan_id,dependency_event_id,upstream_run_id,namespace_epoch,
                       admission_state,planner_version,source_manifest_path,
                       source_manifest_hash,plan_path,plan_hash,status,reason,created_at
                FROM concept_refresh_runs
                """
            )
            connection.execute("DROP TABLE concept_refresh_runs")
            connection.execute("ALTER TABLE concept_refresh_runs_v12 RENAME TO concept_refresh_runs")

        item_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='concept_refresh_items'"
        ).fetchone()
        item_definition = str(item_sql[0] or "") if item_sql is not None else ""
        if "canary_projection" not in item_definition or "outbox_item_id" not in _columns(connection, "concept_refresh_items"):
            connection.execute("DROP TABLE IF EXISTS concept_refresh_items_v12")
            connection.execute(
                """
                CREATE TABLE concept_refresh_items_v12 (
                    plan_id TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    coverage_status TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK (decision IN ('observe_only','blocked','canary_projection','incremental_projection','deferred','retired_excluded')),
                    source_count INTEGER NOT NULL DEFAULT 0,
                    evidence_hash TEXT,
                    target_uri TEXT,
                    idempotency_key TEXT,
                    outbox_item_id TEXT,
                    reason TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(plan_id, concept_id),
                    FOREIGN KEY(plan_id) REFERENCES concept_refresh_runs(plan_id)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO concept_refresh_items_v12(
                    plan_id,concept_id,coverage_status,decision,source_count,evidence_hash,
                    target_uri,idempotency_key,outbox_item_id,reason
                )
                SELECT plan_id,concept_id,coverage_status,decision,source_count,evidence_hash,
                       NULL,NULL,NULL,reason
                FROM concept_refresh_items
                """
            )
            connection.execute("DROP TABLE concept_refresh_items")
            connection.execute("ALTER TABLE concept_refresh_items_v12 RENAME TO concept_refresh_items")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_concept_refresh_runs_created "
            "ON concept_refresh_runs(created_at DESC, admission_state)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_concept_refresh_items_decision "
            "ON concept_refresh_items(plan_id, decision, coverage_status)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_concept_refresh_items_outbox "
            "ON concept_refresh_items(outbox_item_id) WHERE outbox_item_id IS NOT NULL"
        )
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.DatabaseError:
            pass
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


class PMSystemStore:
    """SQLite coordination store with explicit, short transactions."""

    def __init__(
        self,
        db_path: Path,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        auto_migrate: bool = True,
        max_schema_version: Optional[int] = None,
        read_only: bool = False,
    ) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.read_only = bool(read_only)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self._migration_lock = threading.RLock()
        if self.read_only:
            if auto_migrate:
                raise ValueError("read-only store cannot auto-migrate")
            if not self.db_path.is_file():
                raise StoreUnavailable(f"coordination store does not exist: {self.db_path}")
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if auto_migrate:
            try:
                self.migrate(max_schema_version=max_schema_version)
            except (sqlite3.DatabaseError, OSError) as exc:
                raise StoreUnavailable(f"cannot initialize coordination store: {exc}") from exc

    def connect(self) -> sqlite3.Connection:
        if self.read_only:
            connection = sqlite3.connect(
                self.db_path.as_uri() + "?mode=ro",
                uri=True,
                timeout=max(0.001, self.busy_timeout_ms / 1000),
                isolation_level=None,
                check_same_thread=False,
            )
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            connection.execute("PRAGMA query_only=ON")
            return connection
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=max(0.001, self.busy_timeout_ms / 1000),
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys=ON")
        # WAL is persistent at the database level and is safe to request on
        # every connection.  A read-only fallback is used if this fails.
        mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            connection.close()
            raise StoreUnavailable(f"SQLite WAL unavailable (journal_mode={mode})")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        if self.read_only:
            raise StoreUnavailable("read-only coordination store cannot start a write transaction")
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.DatabaseError:
                pass
            raise
        finally:
            connection.close()

    def migrate(self, *, max_schema_version: Optional[int] = None) -> int:
        if self.read_only:
            raise StoreUnavailable("read-only coordination store cannot migrate")
        with self._migration_lock:
            connection = sqlite3.connect(
                str(self.db_path),
                timeout=max(0.001, self.busy_timeout_ms / 1000),
                isolation_level=None,
                check_same_thread=False,
            )
            try:
                connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
                mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if str(mode).lower() != "wal":
                    raise StoreUnavailable(f"SQLite WAL unavailable (journal_mode={mode})")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, schema_id TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
                )
                current = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
                for version, schema_id, sql in MIGRATIONS:
                    if max_schema_version is not None and version > int(max_schema_version):
                        break
                    checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                    row = connection.execute("SELECT schema_id, checksum FROM schema_migrations WHERE version=?", (version,)).fetchone()
                    if row is not None:
                        if row[0] != schema_id or row[1] != checksum:
                            raise StoreUnavailable(f"migration checksum mismatch at version {version}")
                        continue
                    # sqlite3.executescript() deliberately commits any active
                    # transaction.  Run the idempotent DDL first, then record
                    # the migration in its own short transaction.  If a
                    # process stops between the two, the DDL is harmlessly
                    # replayed on the next startup and the version row is
                    # written exactly once.
                    if version == 6:
                        # SQLite has no portable ``ADD COLUMN IF NOT EXISTS``.
                        # Apply this migration explicitly so a process stop
                        # after DDL but before the version row is recorded is
                        # harmless on the next startup.
                        columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(outbox_items)").fetchall()}
                        if "retry_deadline_at" not in columns:
                            connection.execute("ALTER TABLE outbox_items ADD COLUMN retry_deadline_at TEXT")
                        connection.execute(
                            "CREATE INDEX IF NOT EXISTS idx_outbox_retry_deadline ON outbox_items(status, retry_deadline_at, next_attempt_at)"
                        )
                    elif version == 7:
                        _apply_schema_v7(connection)
                    elif version == 8:
                        _apply_schema_v8(connection)
                    elif version == 11:
                        _apply_schema_v11(connection)
                    elif version == 12:
                        _apply_schema_v12(connection)
                    else:
                        connection.executescript(sql)
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "INSERT INTO schema_migrations(version, schema_id, checksum, applied_at) VALUES(?,?,?,?)",
                        (version, schema_id, checksum, now_iso()),
                    )
                    connection.execute("COMMIT")
                    current = max(current, version)
                # A process may have been interrupted after a v7 DDL change
                # but before all compatibility columns were added, or an
                # older runtime may have recorded the v7 marker early.  Run
                # the idempotent repair on every open so the runtime never
                # observes a partially upgraded schema.
                if current >= 7:
                    _apply_schema_v7(connection)
                    _ensure_memory_projection_schema(connection)
                if current >= 8:
                    _apply_schema_v8(connection)
                if current >= 11:
                    _apply_schema_v11(connection)
                if current >= 12:
                    _apply_schema_v12(connection)
                return current
            except Exception:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.DatabaseError:
                    pass
                raise
            finally:
                connection.close()

    def schema_version(self) -> int:
        with self.connect() as connection:
            row = connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
            return int(row[0])

    def pragmas(self) -> Dict[str, Any]:
        with self.connect() as connection:
            return {
                "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
                "busy_timeout": connection.execute("PRAGMA busy_timeout").fetchone()[0],
                "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
            }

    def migration_freeze(self) -> Optional[Dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM migration_freeze WHERE freeze_id=1").fetchone()
            return dict(row) if row is not None else None

    def set_migration_freeze(
        self,
        *,
        migration_id: str,
        migration_epoch: str,
        stage_id: str,
        owner: str,
        deadline_at: str,
        state: str = "freeze",
    ) -> Dict[str, Any]:
        timestamp = now_iso()
        values = (migration_id, migration_epoch, stage_id, owner, state, deadline_at, timestamp, timestamp)
        with self.transaction() as connection:
            current = connection.execute("SELECT migration_id,state FROM migration_freeze WHERE freeze_id=1").fetchone()
            if current is not None and current[0] != migration_id and current[1] != "released":
                raise MigrationLeaseConflict(f"migration freeze already held by {current[0]}")
            connection.execute(
                """INSERT INTO migration_freeze(freeze_id,migration_id,migration_epoch,stage_id,owner,state,deadline_at,created_at,updated_at)
                   VALUES(1,?,?,?,?,?,?,?,?)
                   ON CONFLICT(freeze_id) DO UPDATE SET migration_id=excluded.migration_id,
                     migration_epoch=excluded.migration_epoch,stage_id=excluded.stage_id,owner=excluded.owner,
                     state=excluded.state,deadline_at=excluded.deadline_at,updated_at=excluded.updated_at""",
                (migration_id, migration_epoch, stage_id, owner, state, deadline_at, timestamp, timestamp),
            )
        return dict(self.migration_freeze() or {})

    def update_migration_freeze(self, *, migration_id: str, state: str) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE migration_freeze SET state=?,updated_at=? WHERE freeze_id=1 AND migration_id=?",
                (state, now_iso(), migration_id),
            )
            return cursor.rowcount == 1

    def acquire_migration_lease(
        self,
        *,
        migration_id: str,
        stage_id: str,
        migration_epoch: str,
        owner: str,
        lease_seconds: int = 900,
    ) -> Dict[str, Any]:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        timestamp = datetime.now(timezone.utc)
        at = timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")
        expires = (timestamp + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
        lease_id = _new_id("migration-lease")
        with self.transaction() as connection:
            connection.execute("UPDATE migration_leases SET state='expired' WHERE state='active' AND lease_expires_at<=?", (at,))
            row = connection.execute(
                "SELECT lease_id,owner,lease_expires_at,state FROM migration_leases WHERE migration_id=? AND stage_id=?",
                (migration_id, stage_id),
            ).fetchone()
            if row is not None and row[3] == "active":
                raise MigrationLeaseConflict(f"stage lease held by {row[1]}")
            # Do not depend on a composite UNIQUE constraint here.  Production
            # schema v11 has PRIMARY KEY(migration_id, stage_id), but older
            # restored/test databases may only retain the lease_id key.  The
            # surrounding BEGIN IMMEDIATE transaction still makes this
            # select/update/insert sequence atomic.
            if row is not None:
                connection.execute(
                    """UPDATE migration_leases
                       SET migration_epoch=?,owner=?,lease_id=?,acquired_at=?,lease_expires_at=?,state='active'
                       WHERE migration_id=? AND stage_id=?""",
                    (migration_epoch, owner, lease_id, at, expires, migration_id, stage_id),
                )
            else:
                connection.execute(
                    """INSERT INTO migration_leases(migration_id,stage_id,migration_epoch,owner,lease_id,acquired_at,lease_expires_at,state)
                       VALUES(?,?,?,?,?,?,?,'active')""",
                    (migration_id, stage_id, migration_epoch, owner, lease_id, at, expires),
                )
        return {"migration_id": migration_id, "stage_id": stage_id, "migration_epoch": migration_epoch, "owner": owner, "lease_id": lease_id, "acquired_at": at, "lease_expires_at": expires, "state": "active"}

    def release_migration_lease(self, *, lease_id: str, state: str = "released") -> bool:
        with self.transaction() as connection:
            cursor = connection.execute("UPDATE migration_leases SET state=? WHERE lease_id=? AND state='active'", (state, lease_id))
            return cursor.rowcount == 1

    @staticmethod
    def _freeze_blocks(connection: sqlite3.Connection) -> Optional[sqlite3.Row]:
        try:
            row = connection.execute("SELECT * FROM migration_freeze WHERE freeze_id=1").fetchone()
        except sqlite3.OperationalError:
            return None
        if row is not None and str(row[5]).lower() in {"freeze", "draining", "read_only", "maintenance"}:
            return row
        return None

    def _accept_unlocked(self, connection: sqlite3.Connection, request: Mapping[str, Any], *, accepted_at: Optional[str] = None) -> Dict[str, Any]:
        """Insert or deduplicate a Job/Run using the caller's transaction.

        This helper deliberately does not begin or commit a transaction.  The
        scheduler uses it while the occurrence row and the Job/Run rows are
        protected by the same SQLite write transaction.
        """
        job_type = str(request.get("job_type") or request.get("kind") or "").strip()
        loop_id = str(request.get("loop_id") or "").strip()
        if not job_type:
            raise ValueError("job_type is required")
        if not loop_id:
            raise ValueError("loop_id is required")
        job_id = str(request.get("job_id") or _new_id("job"))
        run_id = str(request.get("run_id") or _new_id("run"))
        profile = str(request.get("profile") or request.get("workload_profile") or "interactive")
        owner = str(request.get("owner") or request.get("actor") or "pm-system")
        namespace_epoch = str(request.get("namespace_epoch") or "v4")
        priority = int(request.get("priority", 50))
        payload = request.get("payload") or {}
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be an object")
        # A missing idempotency key must still be replay-safe.  The payload is
        # the stable request identity; callers that intentionally want two
        # equivalent jobs must provide an explicit key.
        payload_hash = hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()[:24]
        trigger = request.get("trigger") if isinstance(request.get("trigger"), Mapping) else {}
        window = request.get("schedule_window") or trigger.get("schedule_window") or payload.get("schedule_window")
        identity = str(window or request.get("request_fingerprint") or payload_hash)
        idempotency_key = str(request.get("idempotency_key") or f"{loop_id}:{job_type}:{identity}")
        accepted_at = accepted_at or now_iso()
        existing = connection.execute(
            "SELECT job_id, run_id, status, queued_at FROM jobs WHERE idempotency_key=? AND profile=? AND namespace_epoch=?",
            (idempotency_key, profile, namespace_epoch),
        ).fetchone()
        if existing is not None:
            return {
                "status": "accepted",
                "deduplicated": True,
                "job_id": existing[0],
                "run_id": existing[1],
                "job_status": existing[2],
                "accepted_at": existing[3],
            }
        job_columns = _columns(connection, "jobs")
        run_columns = _columns(connection, "runs")
        connection.execute(
            "INSERT INTO jobs(job_id,idempotency_key,job_type,run_id,status,priority,profile,owner,namespace_epoch,deadline_at,payload_json,queued_at,updated_at,occurrence_id,schedule_key,trigger_kind,registry_hash,lock_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, idempotency_key, job_type, run_id, "queued", priority, profile, owner, namespace_epoch, request.get("deadline_at"), _json(payload), accepted_at, accepted_at, request.get("occurrence_id") if "occurrence_id" in job_columns else None, request.get("schedule_key") if "schedule_key" in job_columns else None, request.get("trigger_kind", "manual") if "trigger_kind" in job_columns else "manual", request.get("registry_hash") if "registry_hash" in job_columns else None, request.get("lock_key") if "lock_key" in job_columns else None),
        )
        connection.execute(
            "INSERT INTO runs(run_id,job_id,loop_id,status,profile,owner,namespace_epoch,created_at,updated_at,deadline_at,occurrence_id,schedule_key,trigger_kind,registry_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, job_id, loop_id, "queued", profile, owner, namespace_epoch, accepted_at, accepted_at, request.get("deadline_at"), request.get("occurrence_id") if "occurrence_id" in run_columns else None, request.get("schedule_key") if "schedule_key" in run_columns else None, request.get("trigger_kind", "manual") if "trigger_kind" in run_columns else "manual", request.get("registry_hash") if "registry_hash" in run_columns else None),
        )
        connection.execute(
            "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
            (run_id, 1, "run/accepted", owner, _json({"job_id": job_id, "profile": profile, "namespace_epoch": namespace_epoch, "occurrence_id": request.get("occurrence_id"), "schedule_key": request.get("schedule_key")}), accepted_at),
        )
        self._record_activity_unlocked(
            connection,
            event_type="run/accepted",
            actor=owner,
            payload={
                "job_id": job_id,
                "run_id": run_id,
                "profile": profile,
                "namespace_epoch": namespace_epoch,
                "occurrence_id": request.get("occurrence_id"),
                "schedule_key": request.get("schedule_key"),
            },
            run_id=run_id,
            job_id=job_id,
            occurrence_id=request.get("occurrence_id"),
            source_cursor=f"{run_id}:1",
            idempotency_key=f"run:{run_id}:1",
            occurred_at=accepted_at,
        )
        return {
            "status": "accepted",
            "deduplicated": False,
            "job_id": job_id,
            "run_id": run_id,
            "job_status": "queued",
            "accepted_at": accepted_at,
        }

    def accept(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        """Accept one job with one local transaction and no remote calls."""
        job_type = str(request.get("job_type") or request.get("kind") or "").strip()
        loop_id = str(request.get("loop_id") or "").strip()
        if not job_type:
            raise ValueError("job_type is required")
        if not loop_id:
            raise ValueError("loop_id is required")
        with self.transaction() as connection:
            freeze = self._freeze_blocks(connection)
            if freeze is not None:
                raise MigrationFrozen(f"admission blocked by migration {freeze[1]} stage {freeze[3]}")
            return self._accept_unlocked(connection, request)

    def accept_scheduled_occurrence(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        """Atomically accept one calendar occurrence and its Job/Run.

        Replays return ``deduplicated``; a held schedule lock returns
        ``deferred`` without creating a Job. Dependency occurrences are
        accepted only by the Scheduler after it consumes a durable upstream
        event; this Store does not infer dependencies itself.
        """
        schedule_key = str(request.get("schedule_key") or "").strip()
        occurrence_id = str(request.get("occurrence_id") or "").strip()
        occurrence_key = str(request.get("occurrence_key") or occurrence_id).strip()
        scheduled_at = str(request.get("scheduled_at") or "").strip()
        local_scheduled_at = str(request.get("local_scheduled_at") or scheduled_at).strip()
        deadline_at = str(request.get("deadline_at") or "").strip()
        registry_hash = str(request.get("registry_hash") or "").strip()
        lock_key = str(request.get("lock_key") or schedule_key).strip()
        trigger_kind = str(request.get("trigger_kind") or "calendar").strip()
        owner = str(request.get("owner") or "pm-scheduler")
        if not all((schedule_key, occurrence_id, occurrence_key, scheduled_at, deadline_at, registry_hash, lock_key)):
            raise ValueError("scheduled occurrence requires schedule_key, occurrence_id, occurrence_key, scheduled_at, deadline_at, registry_hash and lock_key")
        if trigger_kind not in {"calendar", "dependency", "manual_replay"}:
            raise ValueError("invalid trigger_kind")
        at = now_iso()
        with self.transaction() as connection:
            freeze = self._freeze_blocks(connection)
            if freeze is not None:
                raise MigrationFrozen(f"admission blocked by migration {freeze[1]} stage {freeze[3]}")
            existing = connection.execute("SELECT * FROM schedule_occurrences WHERE occurrence_key=?", (occurrence_key,)).fetchone()
            if existing is not None:
                value = dict(existing)
                if value.get("job_id"):
                    return {"status": "accepted", "deduplicated": True, "occurrence_id": value["occurrence_id"], "job_id": value["job_id"], "run_id": value.get("run_id"), "occurrence_state": value["state"]}
            connection.execute("INSERT INTO schedule_occurrences(occurrence_id,occurrence_key,schedule_key,scheduled_at,local_scheduled_at,deadline_at,registry_hash,trigger_kind,state,lock_key,attempt,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(occurrence_key) DO UPDATE SET updated_at=excluded.updated_at", (occurrence_id, occurrence_key, schedule_key, scheduled_at, local_scheduled_at, deadline_at, registry_hash, trigger_kind, "due", lock_key, int(existing["attempt"] if existing else 0), at, at))
            connection.execute("DELETE FROM schedule_leases WHERE expires_at<=?", (at,))
            lease = connection.execute("SELECT lease_id,owner FROM schedule_leases WHERE lock_key=?", (lock_key,)).fetchone()
            if lease is not None and str(lease[1]) != owner:
                connection.execute("UPDATE schedule_occurrences SET state='deferred',failure_reason='lock_conflict',updated_at=? WHERE occurrence_key=?", (at, occurrence_key))
                return {"status": "deferred", "deduplicated": False, "occurrence_id": occurrence_id, "occurrence_state": "deferred", "reason": "lock_conflict"}
            lease_id = _new_id("schedule-lease")
            expires = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(timespec="seconds").replace("+00:00", "Z")
            connection.execute("INSERT INTO schedule_leases(lock_key,owner,lease_id,acquired_at,expires_at) VALUES(?,?,?,?,?)", (lock_key, owner, lease_id, at, expires))
            payload = dict(request.get("payload")) if isinstance(request.get("payload"), Mapping) else {}
            # The dispatcher owns these values. Persist them with the Job so
            # a Worker can enforce the same concurrency/retry contract even
            # after a restart, without re-reading a mutable registry file.
            for key in ("concurrency_key", "retry"):
                if key in request and key not in payload:
                    payload[key] = request[key]
            job_request = dict(request)
            job_request.update({"job_type": request.get("job_type") or f"scheduled.{schedule_key}", "loop_id": request.get("loop_id") or schedule_key, "payload": payload, "idempotency_key": request.get("idempotency_key") or f"schedule:{occurrence_key}", "occurrence_id": occurrence_id, "schedule_key": schedule_key, "trigger_kind": trigger_kind, "registry_hash": registry_hash, "lock_key": lock_key, "owner": owner})
            accepted = self._accept_unlocked(connection, job_request, accepted_at=at)
            connection.execute("UPDATE schedule_occurrences SET state='accepted',job_id=?,run_id=?,attempt=attempt+1,updated_at=? WHERE occurrence_key=?", (accepted["job_id"], accepted["run_id"], at, occurrence_key))
            self._upsert_plan_unlocked(
                connection,
                plan_id=f"schedule:{schedule_key}",
                plan_type="schedule_registry",
                title=schedule_key,
                feature_gate="runtime_read_model_gate",
                status="active",
                source_ref=f"schedule_registry:{registry_hash}",
                updated_at=at,
            )
            self._upsert_plan_item_unlocked(
                connection,
                plan_id=f"schedule:{schedule_key}",
                item_key=occurrence_key,
                item_type="occurrence",
                sequence=int(connection.execute("SELECT COUNT(*) FROM plan_items WHERE plan_id=?", (f"schedule:{schedule_key}",)).fetchone()[0]),
                status="accepted",
                job_id=accepted["job_id"],
                run_id=accepted["run_id"],
                source_ref=f"schedule_occurrences:{occurrence_id}",
                updated_at=at,
            )
            connection.execute("DELETE FROM schedule_leases WHERE lease_id=?", (lease_id,))
            return {**accepted, "occurrence_id": occurrence_id, "occurrence_state": "accepted"}

    def append_scheduled_dependency_event(self, event: Mapping[str, Any]) -> Dict[str, Any]:
        """Append one upstream-completion event for Scheduler consumption.

        Worker code may create an event only after its own Run has terminal
        evidence.  It cannot create an occurrence directly.  Event identity
        is supplied by the caller and must include the upstream Run, manifest
        hash and planner version.
        """
        dependent = str(event.get("dependent_schedule_key") or "").strip()
        upstream = str(event.get("upstream_schedule_key") or "").strip()
        occurrence_id = str(event.get("upstream_occurrence_id") or "").strip()
        run_id = str(event.get("upstream_run_id") or "").strip()
        completed_at = str(event.get("upstream_completed_at") or "").strip()
        event_key = str(event.get("event_key") or "").strip()
        planner_version = str(event.get("planner_version") or "").strip()
        status = str(event.get("status") or "").strip()
        if status not in {"pending", "blocked_by_upstream"}:
            raise ValueError("dependency event status must be pending or blocked_by_upstream")
        if not all((dependent, upstream, occurrence_id, run_id, completed_at, event_key, planner_version)):
            raise ValueError("dependency event is missing identity fields")
        manifest_path = str(event.get("source_manifest_path") or "").strip()
        manifest_hash = str(event.get("source_manifest_hash") or "").strip()
        evidence_path = str(event.get("handler_evidence_path") or "").strip()
        evidence_hash = str(event.get("handler_evidence_hash") or "").strip()
        if status == "pending" and not all((manifest_path, manifest_hash, evidence_path, evidence_hash)):
            raise ValueError("pending dependency event requires manifest and handler evidence hashes")
        at = now_iso()
        event_id = "dependency-" + hashlib.sha256(event_key.encode("utf-8")).hexdigest()[:32]
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM scheduled_dependency_events WHERE event_key=?", (event_key,)
            ).fetchone()
            if existing is not None:
                return {"event_id": existing["event_id"], "status": existing["status"], "deduplicated": True}
            connection.execute(
                """
                INSERT INTO scheduled_dependency_events(
                    event_id,event_key,dependent_schedule_key,upstream_schedule_key,
                    upstream_occurrence_id,upstream_run_id,upstream_completed_at,source_manifest_path,
                    source_manifest_hash,handler_evidence_path,handler_evidence_hash,planner_version,status,
                    reason,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id, event_key, dependent, upstream, occurrence_id, run_id,
                    completed_at, manifest_path or None, manifest_hash or None, evidence_path or None, evidence_hash or None,
                    planner_version, status, str(event.get("reason") or ""), at, at,
                ),
            )
        return {"event_id": event_id, "status": status, "deduplicated": False}

    def list_pending_scheduled_dependency_events(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM scheduled_dependency_events WHERE status='pending' ORDER BY created_at,event_id LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_scheduled_dependency_event_consumed(
        self,
        event_id: str,
        *,
        occurrence_id: str,
        outcome: Mapping[str, Any],
    ) -> bool:
        """Mark an event materialized after its idempotent occurrence exists."""
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_dependency_events
                SET status='consumed',occurrence_id=?,outcome_json=?,consumed_at=?,updated_at=?
                WHERE event_id=? AND status='pending'
                """,
                (str(occurrence_id), _json(dict(outcome)), now_iso(), now_iso(), str(event_id)),
            )
        return cursor.rowcount == 1

    def mark_scheduled_dependency_event_blocked(
        self,
        event_id: str,
        *,
        reason: str,
        outcome: Mapping[str, Any],
    ) -> bool:
        """Block invalid upstream evidence without creating a downstream Job."""
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_dependency_events
                SET status='blocked_by_upstream',reason=?,outcome_json=?,updated_at=?
                WHERE event_id=? AND status='pending'
                """,
                (str(reason), _json(dict(outcome)), now_iso(), str(event_id)),
            )
        return cursor.rowcount == 1

    def get_scheduled_dependency_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM scheduled_dependency_events WHERE event_id=?", (str(event_id),)
            ).fetchone()
        return dict(row) if row is not None else None

    def record_concept_refresh_plan(
        self,
        plan: Mapping[str, Any],
        *,
        items: Iterable[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """Persist the planner's admission-scoped evidence and Outbox bindings."""
        plan_id = str(plan.get("plan_id") or "").strip()
        event_id = str(plan.get("dependency_event_id") or "").strip()
        required = {
            "plan_id": plan_id,
            "dependency_event_id": event_id,
            "upstream_run_id": str(plan.get("upstream_run_id") or "").strip(),
            "admission_state": str(plan.get("admission_state") or "").strip(),
            "planner_version": str(plan.get("planner_version") or "").strip(),
            "source_manifest_path": str(plan.get("source_manifest_path") or "").strip(),
            "source_manifest_hash": str(plan.get("source_manifest_hash") or "").strip(),
            "plan_path": str(plan.get("plan_path") or "").strip(),
            "plan_hash": str(plan.get("plan_hash") or "").strip(),
            "status": str(plan.get("status") or "").strip(),
        }
        if any(not value for value in required.values()):
            raise ValueError("concept refresh plan is missing required fields")
        if required["status"] not in {"planned_disabled", "planned_canary", "planned_incremental", "blocked"}:
            raise ValueError("invalid concept refresh plan status")
        rows = []
        for item in items:
            concept_id = str(item.get("concept_id") or "").strip()
            coverage = str(item.get("coverage_status") or "").strip()
            decision = str(item.get("decision") or "").strip()
            if not concept_id or not coverage or decision not in {
                "observe_only", "blocked", "canary_projection", "incremental_projection", "deferred", "retired_excluded"
            }:
                raise ValueError("concept refresh item is invalid")
            rows.append((
                plan_id, concept_id, coverage, decision, int(item.get("source_count") or 0),
                str(item.get("evidence_hash") or "") or None,
                str(item.get("target_uri") or "") or None,
                str(item.get("idempotency_key") or "") or None,
                str(item.get("outbox_item_id") or "") or None,
                str(item.get("reason") or ""),
            ))
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT plan_id FROM concept_refresh_runs WHERE dependency_event_id=?", (event_id,)
            ).fetchone()
            if existing is not None:
                return {"plan_id": existing[0], "deduplicated": True}
            connection.execute(
                """
                INSERT INTO concept_refresh_runs(
                    plan_id,dependency_event_id,upstream_run_id,namespace_epoch,
                    admission_state,planner_version,source_manifest_path,
                    source_manifest_hash,plan_path,plan_hash,status,reason,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    plan_id, event_id, required["upstream_run_id"],
                    str(plan.get("namespace_epoch") or "") or None,
                    required["admission_state"], required["planner_version"],
                    required["source_manifest_path"], required["source_manifest_hash"],
                    required["plan_path"], required["plan_hash"], required["status"],
                    str(plan.get("reason") or ""), now_iso(),
                ),
            )
            connection.executemany(
                """
                INSERT INTO concept_refresh_items(
                    plan_id,concept_id,coverage_status,decision,source_count,evidence_hash,
                    target_uri,idempotency_key,outbox_item_id,reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
        return {"plan_id": plan_id, "deduplicated": False, "item_count": len(rows)}

    def record_schedule_occurrence(self, request: Mapping[str, Any], *, state: str, reason: Optional[str] = None) -> Dict[str, Any]:
        """Record a non-accepted occurrence decision such as expired/suppressed."""
        allowed = {"due", "deferred", "suppressed", "expired", "failed", "dead_letter"}
        if state not in allowed:
            raise ValueError(f"invalid occurrence state: {state}")
        required = ("schedule_key", "occurrence_id", "occurrence_key", "scheduled_at", "deadline_at", "registry_hash", "lock_key")
        if any(not str(request.get(key) or "").strip() for key in required):
            raise ValueError("occurrence decision is missing required fields")
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute("SELECT * FROM schedule_occurrences WHERE occurrence_key=?", (str(request["occurrence_key"]),)).fetchone()
            if existing is not None and existing["job_id"]:
                return {"occurrence_id": existing["occurrence_id"], "occurrence_key": existing["occurrence_key"], "occurrence_state": existing["state"], "deduplicated": True, "job_id": existing["job_id"], "run_id": existing["run_id"]}
            connection.execute("INSERT INTO schedule_occurrences(occurrence_id,occurrence_key,schedule_key,scheduled_at,local_scheduled_at,deadline_at,registry_hash,trigger_kind,state,lock_key,attempt,failure_reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(occurrence_key) DO UPDATE SET state=excluded.state,failure_reason=excluded.failure_reason,updated_at=excluded.updated_at", (str(request["occurrence_id"]), str(request["occurrence_key"]), str(request["schedule_key"]), str(request["scheduled_at"]), str(request.get("local_scheduled_at") or request["scheduled_at"]), str(request["deadline_at"]), str(request["registry_hash"]), str(request.get("trigger_kind") or "calendar"), state, str(request["lock_key"]), int(existing["attempt"] if existing else 0), reason, at, at))
            row = connection.execute("SELECT * FROM schedule_occurrences WHERE occurrence_key=?", (str(request["occurrence_key"]),)).fetchone()
        return {"occurrence_id": row["occurrence_id"], "occurrence_key": row["occurrence_key"], "occurrence_state": row["state"], "deduplicated": existing is not None, "reason": row["failure_reason"]}

    def update_schedule_occurrence(self, occurrence_id: str, *, state: str, reason: Optional[str] = None) -> bool:
        """Project a Worker terminal state back to its accepted occurrence."""
        allowed = {"due", "accepted", "deferred", "running", "completed", "failed", "dead_letter", "suppressed", "expired"}
        if state not in allowed:
            raise ValueError(f"invalid occurrence state: {state}")
        with self.transaction() as connection:
            return self._update_schedule_occurrence_unlocked(
                connection, str(occurrence_id), state=state, reason=reason
            )

    @staticmethod
    def _update_schedule_occurrence_unlocked(
        connection: sqlite3.Connection,
        occurrence_id: str,
        *,
        state: str,
        reason: Optional[str] = None,
        next_retry_at: Optional[str] = None,
    ) -> bool:
        """Update an occurrence while the caller owns an existing transaction."""
        allowed = {
            "due",
            "accepted",
            "deferred",
            "running",
            "completed",
            "failed",
            "dead_letter",
            "suppressed",
            "expired",
        }
        if state not in allowed:
            raise ValueError(f"invalid occurrence state: {state}")
        cursor = connection.execute(
            "UPDATE schedule_occurrences SET state=?,failure_reason=?,next_retry_at=COALESCE(?,next_retry_at),updated_at=? WHERE occurrence_id=?",
            (state, reason, next_retry_at, now_iso(), str(occurrence_id)),
        )
        return cursor.rowcount == 1

    def reconcile_schedule_occurrences(
        self, *, limit: int = 1000
    ) -> Dict[str, int]:
        """Reconcile occurrence state with its linked Job/Run terminal state.

        Worker and scheduler paths normally update all three records in one
        transaction.  This repair pass handles the small crash window left by
        older releases (or a process dying between two independent updates)
        without creating jobs or rewriting Job/Run history.
        """
        bounded = max(1, min(int(limit), 5000))
        counts: Dict[str, int] = {
            "scanned": 0,
            "updated": 0,
            "accepted": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "dead_letter": 0,
            "quarantine": 0,
            "orphaned": 0,
        }
        terminal_occurrence_states = {
            "completed",
            "failed",
            "dead_letter",
            "quarantine",
        }
        with self.transaction() as connection:
            rows = connection.execute(
                """
                SELECT o.occurrence_id, o.state, o.job_id, o.run_id,
                       j.status AS job_status,
                       j.terminal_reason AS job_terminal_reason,
                       j.error_fingerprint AS job_error,
                       r.status AS run_status,
                       r.error AS run_error
                FROM schedule_occurrences AS o
                LEFT JOIN jobs AS j ON j.job_id=o.job_id
                LEFT JOIN runs AS r ON r.run_id=COALESCE(o.run_id, j.run_id)
                WHERE o.job_id IS NOT NULL OR o.run_id IS NOT NULL
                ORDER BY o.updated_at ASC, o.occurrence_id ASC
                LIMIT ?
                """,
                (bounded,),
            ).fetchall()
            for row in rows:
                counts["scanned"] += 1
                job_status = canonical_status(row[4])
                run_status = canonical_status(row[7])
                statuses = {value for value in (job_status, run_status) if value}
                if not statuses:
                    counts["orphaned"] += 1
                    continue

                # A failure wins over a mismatched completed status so the
                # cockpit cannot hide a partial or inconsistent terminal run.
                if "quarantine" in statuses:
                    projected = "quarantine"
                elif "dead_letter" in statuses:
                    projected = "dead_letter"
                elif statuses & {"failed", "degraded", "cancelled", "canceled", "interrupted"}:
                    projected = "failed"
                elif "completed" in statuses:
                    projected = "completed"
                elif "running" in statuses:
                    projected = "running"
                elif statuses & {"queued", "retry_wait"}:
                    projected = "accepted"
                else:
                    counts["orphaned"] += 1
                    continue

                current = str(row[1] or "")
                if current in terminal_occurrence_states and projected in {
                    "accepted",
                    "running",
                }:
                    # Never regress a known terminal occurrence because one
                    # linked row is stale or was written out of order.
                    continue
                if current == projected and projected != "failed":
                    continue

                reason = None
                if projected in {"failed", "dead_letter", "quarantine"}:
                    reason = (
                        row[5]
                        or row[6]
                        or row[8]
                        or ("status_mismatch" if len(statuses) > 1 else None)
                    )
                    if not reason:
                        reason = "terminal_failure"
                elif projected == "accepted" and row[6]:
                    reason = row[6]
                changed = self._update_schedule_occurrence_unlocked(
                    connection,
                    str(row[0]),
                    state=projected,
                    reason=reason,
                )
                if changed:
                    counts["updated"] += 1
                    counts[projected] = counts.get(projected, 0) + 1
        return counts

    def start_scheduler_tick(self, *, scheduler_id: str, mode: str, registry_hash: Optional[str], started_at: Optional[str] = None) -> str:
        tick_id = _new_id("scheduler-tick")
        with self.transaction() as connection:
            at = started_at or now_iso()
            connection.execute("INSERT INTO scheduler_ticks(tick_id,scheduler_id,mode,started_at,status,registry_hash) VALUES(?,?,?,?,?,?)", (tick_id, str(scheduler_id), str(mode), at, "running", registry_hash))
            self._upsert_operation_unlocked(
                connection,
                operation_key="scheduler",
                module_id="Scheduler",
                process="pm_loop_scheduler",
                heartbeat_at=at,
                status="running",
                freshness="fresh",
                reconcile_state="clear",
                evidence_refs=[f"scheduler_ticks:{tick_id}"],
                source_version=registry_hash,
                observed_at=at,
                updated_at=at,
            )
        return tick_id

    def finish_scheduler_tick(self, tick_id: str, *, status: str = "completed", accepted: int = 0, deduplicated: int = 0, deferred: int = 0, expired: int = 0, error: Optional[str] = None) -> bool:
        if status not in {"running", "completed", "failed", "suppressed"}:
            raise ValueError("invalid scheduler tick status")
        with self.transaction() as connection:
            at = now_iso()
            cursor = connection.execute("UPDATE scheduler_ticks SET completed_at=?,status=?,accepted_count=?,deduplicated_count=?,deferred_count=?,expired_count=?,error=? WHERE tick_id=?", (at, status, int(accepted), int(deduplicated), int(deferred), int(expired), error, str(tick_id)))
            tick = connection.execute("SELECT scheduler_id,registry_hash FROM scheduler_ticks WHERE tick_id=?", (str(tick_id),)).fetchone()
            if tick is not None:
                self._upsert_operation_unlocked(
                    connection,
                    operation_key="scheduler",
                    module_id="Scheduler",
                    process="pm_loop_scheduler",
                    heartbeat_at=at,
                    status="healthy" if status == "completed" else "incident" if status == "failed" else "unknown",
                    freshness="fresh",
                    reconcile_state="clear" if status == "completed" else "required",
                    evidence_refs=[f"scheduler_ticks:{tick_id}"],
                    source_version=tick[1],
                    observed_at=at,
                    updated_at=at,
                )
            return cursor.rowcount == 1

    @staticmethod
    def _v13_table_exists(connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone() is not None

    @staticmethod
    def _upsert_plan_unlocked(
        connection: sqlite3.Connection,
        *,
        plan_id: str,
        plan_type: str,
        title: str,
        stage: Optional[str] = None,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        timezone_name: Optional[str] = None,
        dependencies: Any = None,
        watermarks: Any = None,
        feature_gate: Optional[str] = None,
        status: str = "planned",
        source_ref: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> None:
        """Persist one V1.3 plan without opening a nested transaction."""
        if not PMSystemStore._v13_table_exists(connection, "plans"):
            return
        at = updated_at or now_iso()
        connection.execute(
            """INSERT INTO plans(plan_id,plan_type,title,stage,window_start,window_end,timezone,
                                  dependencies_json,watermarks_json,feature_gate,status,source_ref,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(plan_id) DO UPDATE SET plan_type=excluded.plan_type,title=excluded.title,
                 stage=excluded.stage,window_start=excluded.window_start,window_end=excluded.window_end,
                 timezone=excluded.timezone,dependencies_json=excluded.dependencies_json,
                 watermarks_json=excluded.watermarks_json,feature_gate=excluded.feature_gate,
                 status=excluded.status,source_ref=excluded.source_ref,updated_at=excluded.updated_at""",
            (
                str(plan_id),
                str(plan_type),
                str(title),
                stage,
                window_start,
                window_end,
                timezone_name,
                _json(dependencies if dependencies is not None else []),
                _json(watermarks if watermarks is not None else {}),
                feature_gate,
                str(status),
                source_ref,
                at,
                at,
            ),
        )

    @staticmethod
    def _upsert_plan_item_unlocked(
        connection: sqlite3.Connection,
        *,
        plan_id: str,
        item_key: str,
        item_type: str = "scheduled_job",
        sequence: int = 0,
        status: str = "planned",
        job_id: Optional[str] = None,
        run_id: Optional[str] = None,
        dependencies: Any = None,
        source_ref: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> None:
        if not PMSystemStore._v13_table_exists(connection, "plan_items"):
            return
        at = updated_at or now_iso()
        item_id = f"plan-item:{plan_id}:{item_key}"
        connection.execute(
            """INSERT INTO plan_items(plan_item_id,plan_id,item_key,item_type,sequence,status,job_id,run_id,
                                       dependencies_json,source_ref,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(plan_id,item_key) DO UPDATE SET item_type=excluded.item_type,
                 sequence=excluded.sequence,status=excluded.status,job_id=excluded.job_id,
                 run_id=excluded.run_id,dependencies_json=excluded.dependencies_json,
                 source_ref=excluded.source_ref,updated_at=excluded.updated_at""",
            (
                item_id,
                str(plan_id),
                str(item_key),
                str(item_type),
                int(sequence),
                str(status),
                job_id,
                run_id,
                _json(dependencies if dependencies is not None else []),
                source_ref,
                at,
                at,
            ),
        )

    @staticmethod
    def _upsert_operation_unlocked(
        connection: sqlite3.Connection,
        *,
        operation_key: str,
        module_id: str,
        schedule_key: Optional[str] = None,
        process: Optional[str] = None,
        heartbeat_at: Optional[str] = None,
        lease_id: Optional[str] = None,
        automation: Optional[str] = None,
        current_run: Optional[str] = None,
        last_exit_code: Optional[int] = None,
        status: str = "unknown",
        freshness: str = "unknown",
        reconcile_state: str = "unknown",
        incident_ids: Any = None,
        evidence_refs: Any = None,
        source_version: Optional[str] = None,
        observed_at: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> None:
        if not PMSystemStore._v13_table_exists(connection, "operations"):
            return
        at = updated_at or now_iso()
        observed = observed_at or at
        operation_id = f"operation:{operation_key}"
        connection.execute(
            """INSERT INTO operations(operation_id,operation_key,module_id,schedule_key,process,heartbeat_at,
                                       lease_id,automation,current_run,last_exit_code,status,freshness,
                                       reconcile_state,incident_ids_json,evidence_refs_json,source_version,
                                       observed_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(operation_key) DO UPDATE SET module_id=excluded.module_id,
                 schedule_key=excluded.schedule_key,process=excluded.process,heartbeat_at=excluded.heartbeat_at,
                 lease_id=excluded.lease_id,automation=excluded.automation,current_run=excluded.current_run,
                 last_exit_code=excluded.last_exit_code,status=excluded.status,freshness=excluded.freshness,
                 reconcile_state=excluded.reconcile_state,incident_ids_json=excluded.incident_ids_json,
                 evidence_refs_json=excluded.evidence_refs_json,source_version=excluded.source_version,
                 observed_at=excluded.observed_at,updated_at=excluded.updated_at""",
            (
                operation_id,
                str(operation_key),
                str(module_id),
                schedule_key,
                process,
                heartbeat_at,
                lease_id,
                automation,
                current_run,
                int(last_exit_code) if last_exit_code is not None else None,
                str(status),
                str(freshness),
                str(reconcile_state),
                _json(incident_ids if incident_ids is not None else []),
                _json(evidence_refs if evidence_refs is not None else []),
                source_version,
                observed,
                at,
            ),
        )

    @staticmethod
    def _record_activity_unlocked(
        connection: sqlite3.Connection,
        *,
        event_type: str,
        actor: str,
        payload: Any = None,
        occurred_at: Optional[str] = None,
        run_id: Optional[str] = None,
        job_id: Optional[str] = None,
        occurrence_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        source_cursor: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[str]:
        if not PMSystemStore._v13_table_exists(connection, "activity_events"):
            return None
        at = occurred_at or now_iso()
        payload_map = dict(payload) if isinstance(payload, Mapping) else {}
        run_value = str(run_id or payload_map.get("run_id") or "") or None
        job_value = str(job_id or payload_map.get("job_id") or "") or None
        occurrence_value = str(occurrence_id or payload_map.get("occurrence_id") or "") or None
        correlation = str(correlation_id or payload_map.get("correlation_id") or run_value or job_value or occurrence_value or "") or None
        cursor = str(source_cursor or "") or None
        key = str(idempotency_key or f"{run_value or correlation or 'event'}:{event_type}:{cursor or at}")
        activity_id = "activity-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
        entity_type = "run" if run_value else "job" if job_value else "occurrence" if occurrence_value else None
        entity_id = run_value or job_value or occurrence_value
        connection.execute(
            """INSERT INTO activity_events(activity_id,idempotency_key,event_type,actor,correlation_id,
                                             entity_type,entity_id,run_id,job_id,occurrence_id,payload_json,
                                             source_cursor,occurred_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(idempotency_key) DO NOTHING""",
            (
                activity_id,
                key,
                str(event_type),
                str(actor or "pm-system"),
                correlation,
                entity_type,
                entity_id,
                run_value,
                job_value,
                occurrence_value,
                _json(payload_map),
                cursor,
                at,
            ),
        )
        return activity_id

    @staticmethod
    def _upsert_review_unlocked(connection: sqlite3.Connection, run_id: str, *, updated_at: Optional[str] = None) -> Optional[str]:
        """Materialize one terminal Run as a read-only Review package."""
        if not PMSystemStore._v13_table_exists(connection, "reviews"):
            return None
        row = connection.execute("SELECT * FROM runs WHERE run_id=?", (str(run_id),)).fetchone()
        if row is None:
            return None
        run = dict(row)
        status = canonical_status(run.get("status"), failure_class=run.get("terminal_reason"))
        checkpoints = connection.execute(
            "SELECT checkpoint_key,input_hash,artifact_uri,payload_json,updated_at FROM checkpoints WHERE run_id=? ORDER BY updated_at DESC,stage,checkpoint_key",
            (str(run_id),),
        ).fetchall()
        artifacts = [str(item[2]) for item in checkpoints if item[2]]
        if status in {"failed", "dead_letter", "quarantine", "interrupted"}:
            review_state = "failed"
        elif artifacts:
            review_state = "result_ready"
        else:
            review_state = "verification_pending"
        review_id = f"review:{run_id}"
        at = updated_at or str(run.get("updated_at") or now_iso())
        plan_id = f"schedule:{run['schedule_key']}" if run.get("schedule_key") else None
        artifact_id = artifacts[0] if artifacts else None
        hashes = [str(item[1]) for item in checkpoints if str(item[1] or "").startswith("sha256:")]
        evidence_hash = "sha256:" + hashlib.sha256("|".join(sorted(hashes or artifacts)).encode("utf-8")).hexdigest() if (hashes or artifacts) else None
        conclusion = run.get("error") or run.get("terminal_reason")
        connection.execute(
            """INSERT INTO reviews(review_id,run_id,plan_id,artifact_id,canonical_status,display_status,
                                    review_state,publish_state,conclusion,gate_id,freshness,evidence_hash,
                                    source_ref,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET plan_id=excluded.plan_id,artifact_id=excluded.artifact_id,
                 canonical_status=excluded.canonical_status,display_status=excluded.display_status,
                 review_state=excluded.review_state,publish_state=excluded.publish_state,
                 conclusion=excluded.conclusion,gate_id=excluded.gate_id,freshness=excluded.freshness,
                 evidence_hash=excluded.evidence_hash,source_ref=excluded.source_ref,updated_at=excluded.updated_at""",
            (
                review_id,
                str(run_id),
                plan_id,
                artifact_id,
                status,
                "done" if status == "completed" else "failed" if status in {"failed", "dead_letter", "quarantine", "interrupted"} else "unknown",
                review_state,
                "not_applicable",
                conclusion,
                None,
                "fresh" if at else "unknown",
                evidence_hash,
                f"run:{run_id}",
                str(run.get("created_at") or at),
                at,
            ),
        )
        for checkpoint_key, input_hash, artifact_uri, _payload_json, checkpoint_at in checkpoints:
            evidence_id = f"checkpoint:{run_id}:{checkpoint_key}"
            source_hash = str(input_hash) if str(input_hash or "").startswith("sha256:") else None
            evidence_ref = str(artifact_uri or f"checkpoint:{run_id}:{checkpoint_key}")
            connection.execute(
                """INSERT INTO review_evidence(review_id,evidence_id,evidence_ref,evidence_role,source_hash,status,observed_at,created_at)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(review_id,evidence_id) DO UPDATE SET evidence_ref=excluded.evidence_ref,
                     evidence_role=excluded.evidence_role,source_hash=excluded.source_hash,status=excluded.status,
                     observed_at=excluded.observed_at""",
                (review_id, evidence_id, evidence_ref, str(checkpoint_key), source_hash, "observed", checkpoint_at, at),
            )
        return review_id

    def upsert_review_for_run(self, run_id: str) -> Optional[str]:
        with self.transaction() as connection:
            return self._upsert_review_unlocked(connection, str(run_id))

    def upsert_competitive_radar_latest(self, value: Mapping[str, Any]) -> None:
        """Publish one reviewed radar pointer without creating a second ledger."""
        required = ("run_id", "report_uri", "report_hash", "report_status", "gate_status")
        if any(not str(value.get(key) or "").strip() for key in required):
            raise ValueError("competitive radar latest pointer is incomplete")
        if str(value.get("report_status")) != "reviewed":
            raise ValueError("only reviewed competitive radar reports may become latest")
        at = str(value.get("published_at") or now_iso())
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO competitive_radar_latest(pointer_id,run_id,report_uri,html_uri,report_hash,report_status,gate_status,review_run_id,evidence_coverage,published_at,updated_at)
                   VALUES(1,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(pointer_id) DO UPDATE SET run_id=excluded.run_id,report_uri=excluded.report_uri,
                     html_uri=excluded.html_uri,report_hash=excluded.report_hash,report_status=excluded.report_status,
                     gate_status=excluded.gate_status,review_run_id=excluded.review_run_id,evidence_coverage=excluded.evidence_coverage,
                     published_at=excluded.published_at,updated_at=excluded.updated_at""",
                (
                    str(value["run_id"]),
                    str(value["report_uri"]),
                    str(value.get("html_uri") or ""),
                    str(value["report_hash"]),
                    str(value["report_status"]),
                    str(value["gate_status"]),
                    str(value.get("review_run_id") or ""),
                    float(value.get("evidence_coverage") or 0),
                    at,
                    now_iso(),
                ),
            )

    def get_competitive_radar_latest(self) -> Optional[Dict[str, Any]]:
        with self.connect() as connection:
            try:
                row = connection.execute("SELECT * FROM competitive_radar_latest WHERE pointer_id=1").fetchone()
            except sqlite3.OperationalError:
                return None
            return dict(row) if row is not None else None

    def record_activity_event(
        self,
        *,
        event_type: str,
        actor: str = "pm-system",
        payload: Any = None,
        run_id: Optional[str] = None,
        job_id: Optional[str] = None,
        occurrence_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        source_cursor: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[str]:
        with self.transaction() as connection:
            return self._record_activity_unlocked(
                connection,
                event_type=event_type,
                actor=actor,
                payload=payload,
                run_id=run_id,
                job_id=job_id,
                occurrence_id=occurrence_id,
                correlation_id=correlation_id,
                source_cursor=source_cursor,
                idempotency_key=idempotency_key,
            )

    def set_schedule_registry_state(self, *, registry_version: int, registry_hash: str, source_path: str, canonical_json: str, state: str = "valid", error: Optional[str] = None) -> None:
        if state not in {"valid", "invalid", "stale", "unknown"}:
            raise ValueError("invalid registry state")
        at = now_iso()
        with self.transaction() as connection:
            connection.execute("INSERT INTO schedule_registry_state(registry_id,registry_version,registry_hash,source_path,canonical_json,state,error,loaded_at,updated_at) VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(registry_id) DO UPDATE SET registry_version=excluded.registry_version,registry_hash=excluded.registry_hash,source_path=excluded.source_path,canonical_json=excluded.canonical_json,state=excluded.state,error=excluded.error,loaded_at=excluded.loaded_at,updated_at=excluded.updated_at", (int(registry_version), str(registry_hash), str(source_path), str(canonical_json), state, error, at, at))
            try:
                document = json.loads(canonical_json or "{}")
            except (TypeError, json.JSONDecodeError):
                document = {}
            tasks = document.get("tasks", []) if isinstance(document, Mapping) else []
            timezone_name = document.get("timezone") if isinstance(document, Mapping) else None
            if isinstance(tasks, list):
                for sequence, task in enumerate(tasks):
                    if not isinstance(task, Mapping) or not str(task.get("schedule_key") or "").strip():
                        continue
                    key = str(task["schedule_key"])
                    plan_id = f"schedule:{key}"
                    plan_status = "active" if state == "valid" else "unknown"
                    self._upsert_plan_unlocked(
                        connection,
                        plan_id=plan_id,
                        plan_type="schedule_registry",
                        title=key,
                        timezone_name=str(timezone_name or "") or None,
                        dependencies=task.get("dependencies") or [],
                        watermarks={"registry_hash": registry_hash},
                        feature_gate="runtime_read_model_gate",
                        status=plan_status,
                        source_ref=source_path,
                        updated_at=at,
                    )
                    self._upsert_plan_item_unlocked(
                        connection,
                        plan_id=plan_id,
                        item_key=key,
                        sequence=sequence,
                        status="planned" if state == "valid" else "unknown",
                        dependencies=task.get("dependencies") or [],
                        source_ref=source_path,
                        updated_at=at,
                    )
                    self._upsert_operation_unlocked(
                        connection,
                        operation_key=f"schedule:{key}",
                        module_id="Scheduler",
                        schedule_key=key,
                        process="pm-system-worker",
                        status="configured" if state == "valid" else "unknown",
                        freshness="fresh" if state == "valid" else "unknown",
                        reconcile_state="unknown",
                        evidence_refs=[f"schedule_registry_state:{key}"],
                        source_version=registry_hash,
                        observed_at=at,
                        updated_at=at,
                    )

    def set_workbench_gate_manifest(self, manifest: Mapping[str, Any]) -> None:
        """Persist one workbench gate decision for read-only consumers.

        Gate evaluation remains owned by the runtime/control-plane producer;
        this method only stores the signed-shaped evidence envelope in the
        shared coordination database so a later GET can reproduce the same
        decision without recomputing it from page state.
        """
        required = ("gate_id", "manifest_version", "owner", "observed_at", "expires_at", "decision")
        if any(not str(manifest.get(key) or "").strip() for key in required):
            raise ValueError("gate manifest is missing required fields")
        decision = str(manifest.get("decision") or "").strip().lower()
        if decision not in {"enabled", "disabled", "unknown"}:
            raise ValueError("invalid gate decision")
        at = now_iso()
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO workbench_gate_manifest(
                    gate_id,manifest_version,owner,observed_at,expires_at,
                    required_checks_json,source_hashes_json,decision,
                    protected_modules_json,reason,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(gate_id) DO UPDATE SET
                    manifest_version=excluded.manifest_version,
                    owner=excluded.owner,
                    observed_at=excluded.observed_at,
                    expires_at=excluded.expires_at,
                    required_checks_json=excluded.required_checks_json,
                    source_hashes_json=excluded.source_hashes_json,
                    decision=excluded.decision,
                    protected_modules_json=excluded.protected_modules_json,
                    reason=excluded.reason,
                    updated_at=excluded.updated_at""",
                (
                    str(manifest["gate_id"]),
                    str(manifest["manifest_version"]),
                    str(manifest["owner"]),
                    str(manifest["observed_at"]),
                    str(manifest["expires_at"]),
                    _json(manifest.get("required_checks") or []),
                    _json(manifest.get("source_hashes") or {}),
                    decision,
                    _json(manifest.get("protected_modules") or []),
                    str(manifest.get("reason") or "") or None,
                    at,
                ),
            )

    def list_schedule_occurrences(self, *, limit: int = 100, schedule_key: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            bounded = max(1, min(int(limit), 1000))
            if schedule_key:
                rows = connection.execute("SELECT * FROM schedule_occurrences WHERE schedule_key=? ORDER BY scheduled_at DESC LIMIT ?", (str(schedule_key), bounded)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM schedule_occurrences ORDER BY scheduled_at DESC LIMIT ?", (bounded,)).fetchall()
            return [dict(row) for row in rows]

    def latest_schedule_occurrence_at(self, schedule_key: str) -> Optional[str]:
        """Return the latest calendar window already evidenced for one task.

        Catchup uses this as its lower bound.  It deliberately returns only
        durable occurrence evidence, never a marker timestamp, so it cannot
        manufacture skipped historical windows on a newly migrated database.
        """
        with self.connect() as connection:
            row = connection.execute(
                "SELECT scheduled_at FROM schedule_occurrences WHERE schedule_key=? ORDER BY scheduled_at DESC LIMIT 1",
                (str(schedule_key),),
            ).fetchone()
            return str(row[0]) if row is not None and row[0] else None

    def list_scheduler_ticks(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM scheduler_ticks ORDER BY started_at DESC,tick_id DESC LIMIT ?", (max(1, min(int(limit), 1000)),)).fetchall()
            return [dict(row) for row in rows]

    def list_ops_alerts(self, *, limit: int = 100, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            if state:
                rows = connection.execute("SELECT * FROM ops_alerts WHERE state=? ORDER BY last_seen_at DESC LIMIT ?", (str(state), max(1, min(int(limit), 1000)))).fetchall()
            else:
                rows = connection.execute("SELECT * FROM ops_alerts ORDER BY last_seen_at DESC LIMIT ?", (max(1, min(int(limit), 1000)),)).fetchall()
            return [dict(row) for row in rows]

    def list_ops_alert_fingerprints(self, *, state: str, limit: int = 1000) -> set[str]:
        """Return durable alert fingerprints for projector-side state handling."""
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT fingerprint FROM ops_alerts WHERE state=? ORDER BY last_seen_at DESC LIMIT ?",
                (str(state), max(1, min(int(limit), 5000))),
            ).fetchall()
            return {str(row[0]) for row in rows if str(row[0] or "")}

    def suppress_ops_alerts(
        self,
        *,
        alert_ids: Iterable[str],
        reason: str,
        evidence: Mapping[str, Any],
        actor: str = "pm-ops-reconciliation",
    ) -> List[Dict[str, Any]]:
        """Suppress selected open alerts without changing canonical failures.

        Suppression is only for a verified historical baseline or a separately
        evidenced successful replay.  The original occurrence/Job/Run remains
        immutable failure history; the saved evidence also lets the projector
        skip reopening the exact same fingerprint on every refresh.
        """
        normalized_ids = sorted({str(value).strip() for value in alert_ids if str(value).strip()})
        reason_value = str(reason or "").strip()
        if not normalized_ids:
            return []
        if not reason_value:
            raise ValueError("suppression reason is required")
        if not isinstance(evidence, Mapping) or not evidence:
            raise ValueError("suppression evidence is required")
        at = now_iso()
        suppressed: List[Dict[str, Any]] = []
        with self.transaction() as connection:
            for alert_id in normalized_ids:
                row = connection.execute(
                    "SELECT * FROM ops_alerts WHERE alert_id=? AND state='open'",
                    (alert_id,),
                ).fetchone()
                if row is None:
                    continue
                value = dict(row)
                conflict = connection.execute(
                    "SELECT alert_id FROM ops_alerts WHERE fingerprint=? AND state='suppressed' AND alert_id<>?",
                    (value["fingerprint"], alert_id),
                ).fetchone()
                if conflict is not None:
                    raise ValueError(f"suppressed alert already exists for fingerprint: {value['fingerprint']}")
                try:
                    details = json.loads(value.get("details_json") or "{}")
                except json.JSONDecodeError:
                    details = {}
                if not isinstance(details, dict):
                    details = {"previous_details": details}
                details["suppression"] = {
                    "schema_version": "pm-loop.alert-suppression.v1",
                    "reason": reason_value,
                    "actor": str(actor or "pm-ops-reconciliation"),
                    "suppressed_at": at,
                    "evidence": dict(evidence),
                }
                connection.execute(
                    "UPDATE ops_alerts SET state='suppressed',resolved_at=?,details_json=? WHERE alert_id=? AND state='open'",
                    (at, _json(details), alert_id),
                )
                value.update({"state": "suppressed", "resolved_at": at, "details_json": _json(details)})
                suppressed.append(value)
        return suppressed

    def resolve_ops_alerts(
        self,
        *,
        alert_types: Iterable[str],
        active_fingerprints: Iterable[str],
    ) -> List[Dict[str, Any]]:
        """Resolve open alerts whose canonical fault is no longer present.

        Callers must only include alert types for which their source scan is
        complete. This preserves historical faults while allowing a recovered
        Job, Run, or Scheduler to close its active alert. A later recurrence
        creates a new ``open`` alert under the existing uniqueness rule.
        """
        types = sorted({str(value).strip() for value in alert_types if str(value).strip()})
        active = {str(value).strip() for value in active_fingerprints if str(value).strip()}
        if not types:
            return []
        placeholders = ",".join("?" for _ in types)
        at = now_iso()
        resolved: List[Dict[str, Any]] = []
        with self.transaction() as connection:
            rows = connection.execute(
                f"SELECT * FROM ops_alerts WHERE state='open' AND alert_type IN ({placeholders})",
                tuple(types),
            ).fetchall()
            for row in rows:
                value = dict(row)
                if str(value.get("fingerprint") or "") in active:
                    continue
                try:
                    connection.execute(
                        "UPDATE ops_alerts SET state='resolved',resolved_at=? WHERE alert_id=? AND state='open'",
                        (at, value["alert_id"]),
                    )
                    value.update({"state": "resolved", "resolved_at": at})
                except sqlite3.IntegrityError as exc:
                    # The v1 schema has a UNIQUE(fingerprint, state) guard.
                    # A recurrent fault may therefore have prior resolved and
                    # suppressed rows while its new open row is being closed.
                    # Preserve the historical rows and choose an available
                    # terminal state for the duplicate instead of allowing the
                    # projector to fail and leave a false open incident.
                    if "ops_alerts.fingerprint, ops_alerts.state" not in str(exc):
                        raise
                    try:
                        recovery_details = json.loads(value.get("details_json") or "{}")
                    except json.JSONDecodeError:
                        recovery_details = {}
                    if not isinstance(recovery_details, dict):
                        recovery_details = {"previous_details": recovery_details}
                    recovery_details["recovery"] = {
                        "schema_version": "pm-loop.alert-recovery.v1",
                        "reason": "resolved_fingerprint_exists",
                        "resolved_at": at,
                    }
                    terminal_states = {
                        str(item[0])
                        for item in connection.execute(
                            "SELECT state FROM ops_alerts WHERE fingerprint=? AND state IN ('resolved','suppressed','acknowledged')",
                            (value["fingerprint"],),
                        ).fetchall()
                    }
                    # Prefer an available terminal state.  A long-lived
                    # recurring fingerprint can eventually occupy all three
                    # terminal states; in that case keep this recovery row by
                    # assigning it a unique recovery fingerprint rather than
                    # leaving a false open incident or deleting history.
                    fallback_state = next(
                        (state for state in ("suppressed", "acknowledged", "resolved") if state not in terminal_states),
                        None,
                    )
                    recovery_details["recovery"]["fallback_state"] = fallback_state
                    if fallback_state:
                        connection.execute(
                            f"UPDATE ops_alerts SET state='{fallback_state}',resolved_at=?,details_json=? WHERE alert_id=? AND state='open'",
                            (at, _json(recovery_details), value["alert_id"]),
                        )
                        value.update({"state": fallback_state, "resolved_at": at, "details_json": _json(recovery_details)})
                    else:
                        recovery_fingerprint = f"{value['fingerprint']}:recovery:{value['alert_id']}"
                        recovery_details["recovery"]["original_fingerprint"] = value["fingerprint"]
                        recovery_details["recovery"]["recovery_fingerprint"] = recovery_fingerprint
                        connection.execute(
                            "UPDATE ops_alerts SET fingerprint=?,state='resolved',resolved_at=?,details_json=? WHERE alert_id=? AND state='open'",
                            (recovery_fingerprint, at, _json(recovery_details), value["alert_id"]),
                        )
                        value.update({"fingerprint": recovery_fingerprint, "state": "resolved", "resolved_at": at, "details_json": _json(recovery_details)})
                resolved.append(value)
        return resolved

    def upsert_ops_alert(
        self,
        *,
        fingerprint: str,
        severity: str,
        alert_type: str,
        module: str,
        message: str,
        occurrence_id: Optional[str] = None,
        job_id: Optional[str] = None,
        run_id: Optional[str] = None,
        details: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create or refresh one open operator alert by fingerprint."""
        severity_value = str(severity or "").strip().upper()
        if severity_value not in {"P0", "P1", "P2", "P3"}:
            raise ValueError("invalid alert severity")
        fields = (str(fingerprint).strip(), severity_value, str(alert_type).strip(), str(module).strip(), str(message).strip())
        if not all(fields):
            raise ValueError("alert fingerprint, severity, type, module and message are required")
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM ops_alerts WHERE fingerprint=? AND state='open' LIMIT 1",
                (fields[0],),
            ).fetchone()
            details_json = _json(details)
            if existing is not None:
                connection.execute(
                    "UPDATE ops_alerts SET severity=?,alert_type=?,module=?,message=?,occurrence_id=COALESCE(?,occurrence_id),job_id=COALESCE(?,job_id),run_id=COALESCE(?,run_id),last_seen_at=?,details_json=? WHERE alert_id=?",
                    (fields[1], fields[2], fields[3], fields[4], occurrence_id, job_id, run_id, at, details_json, existing["alert_id"]),
                )
                row = connection.execute("SELECT * FROM ops_alerts WHERE alert_id=?", (existing["alert_id"],)).fetchone()
                return {**dict(row), "deduplicated": True}
            alert_id = _new_id("alert")
            connection.execute(
                "INSERT INTO ops_alerts(alert_id,fingerprint,severity,alert_type,module,message,occurrence_id,job_id,run_id,state,first_seen_at,last_seen_at,details_json) VALUES(?,?,?,?,?,?,?,?,?,'open',?,?,?)",
                (alert_id, fields[0], fields[1], fields[2], fields[3], fields[4], occurrence_id, job_id, run_id, at, at, details_json),
            )
            row = connection.execute("SELECT * FROM ops_alerts WHERE alert_id=?", (alert_id,)).fetchone()
            return {**dict(row), "deduplicated": False}

    def record_notification_delivery(
        self,
        *,
        alert_id: str,
        fingerprint: str,
        state: str = "pending",
        channel: str = "macos",
        delivered_at: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record one local notification attempt idempotently."""
        if channel != "macos":
            raise ValueError("only macos notifications are supported")
        if state not in {"pending", "sent", "failed", "deduplicated"}:
            raise ValueError("invalid notification state")
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM notification_deliveries WHERE alert_id=? AND channel=? AND fingerprint=? LIMIT 1",
                (str(alert_id), channel, str(fingerprint)),
            ).fetchone()
            if existing is not None:
                if state in {"sent", "failed"} and existing["state"] not in {"sent", "failed"}:
                    connection.execute(
                        "UPDATE notification_deliveries SET state=?,delivered_at=?,error=? WHERE notification_id=?",
                        (state, delivered_at or at, error, existing["notification_id"]),
                    )
                row = connection.execute("SELECT * FROM notification_deliveries WHERE notification_id=?", (existing["notification_id"],)).fetchone()
                return {**dict(row), "deduplicated": True}
            notification_id = _new_id("notification")
            connection.execute(
                "INSERT INTO notification_deliveries(notification_id,alert_id,channel,fingerprint,state,requested_at,delivered_at,error) VALUES(?,?,?,?,?,?,?,?)",
                (notification_id, str(alert_id), channel, str(fingerprint), state, at, delivered_at, error),
            )
            row = connection.execute("SELECT * FROM notification_deliveries WHERE notification_id=?", (notification_id,)).fetchone()
            return {**dict(row), "deduplicated": False}

    def list_notification_deliveries(self, *, limit: int = 100, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            bounded = max(1, min(int(limit), 1000))
            if state:
                rows = connection.execute("SELECT * FROM notification_deliveries WHERE state=? ORDER BY requested_at DESC LIMIT ?", (str(state), bounded)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM notification_deliveries ORDER BY requested_at DESC LIMIT ?", (bounded,)).fetchall()
            return [dict(row) for row in rows]

    submit_job = accept
    accept_run = accept

    def append_run_event(
        self,
        run_id: str,
        event_type: str,
        payload: Optional[Mapping[str, Any]] = None,
        *,
        actor: str = "pm-system",
    ) -> Dict[str, Any]:
        occurred_at = now_iso()
        with self.transaction() as connection:
            row = connection.execute("SELECT COALESCE(MAX(seq), 0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()
            seq = int(row[0]) + 1
            connection.execute(
                "INSERT INTO run_events(run_id,seq,event_type,actor,payload_json,occurred_at) VALUES(?,?,?,?,?,?)",
                (run_id, seq, event_type, actor, _json(payload), occurred_at),
            )
            payload_map = dict(payload) if isinstance(payload, Mapping) else {}
            self._record_activity_unlocked(
                connection,
                event_type=event_type,
                actor=actor,
                payload=payload_map,
                run_id=run_id,
                job_id=payload_map.get("job_id"),
                occurrence_id=payload_map.get("occurrence_id"),
                correlation_id=payload_map.get("correlation_id") or run_id,
                source_cursor=f"{run_id}:{seq}",
                idempotency_key=f"run:{run_id}:{seq}",
                occurred_at=occurred_at,
            )
            connection.execute("UPDATE runs SET updated_at=? WHERE run_id=?", (occurred_at, run_id))
        return {"run_id": run_id, "seq": seq, "event_type": event_type, "occurred_at": occurred_at}

    def upsert_checkpoint(
        self,
        run_id: str,
        stage: str,
        checkpoint_key: str,
        *,
        input_hash: Optional[str] = None,
        artifact_uri: Optional[str] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        timestamp = now_iso()
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO checkpoints(run_id,stage,checkpoint_key,input_hash,artifact_uri,payload_json,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_id,stage,checkpoint_key) DO UPDATE SET
                     input_hash=excluded.input_hash, artifact_uri=excluded.artifact_uri,
                     payload_json=excluded.payload_json, updated_at=excluded.updated_at""",
                (run_id, stage, checkpoint_key, input_hash, artifact_uri, _json(payload), timestamp, timestamp),
            )
            if stage == "source" and checkpoint_key == "snapshot" and isinstance(payload, Mapping):
                snapshot_id = payload.get("snapshot_id")
                if snapshot_id:
                    connection.execute("UPDATE runs SET snapshot_id=?, updated_at=? WHERE run_id=?", (str(snapshot_id), timestamp, run_id))

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row is not None else None

    def get_checkpoint(self, run_id: str, stage: str, checkpoint_key: str) -> Optional[Dict[str, Any]]:
        """Read one durable checkpoint without mutating the coordination DB."""
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoints WHERE run_id=? AND stage=? AND checkpoint_key=?",
                (run_id, stage, checkpoint_key),
            ).fetchone()
            if row is None:
                return None
            value = dict(row)
            try:
                value["payload"] = json.loads(value.pop("payload_json") or "{}")
            except json.JSONDecodeError:
                value["payload"] = {}
            return value

    def claim_delivery_intents(
        self,
        *,
        schedule_key: str,
        period_key: str,
        person_ids: Iterable[str],
        run_id: str,
    ) -> Dict[str, List[str]]:
        """Atomically reserve one delivery intent per person and period.

        Existing intents are never returned as new work, including uncertain
        receipts.  This makes a retry safe after a crash between the remote
        side effect and the local result write.
        """
        normalized = sorted({str(item).strip() for item in person_ids if str(item).strip()})
        created: List[str] = []
        existing: List[str] = []
        at = now_iso()
        with self.transaction() as connection:
            for person_id in normalized:
                intent_id = _new_id("delivery-intent")
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO delivery_intents(intent_id,schedule_key,period_key,person_id,run_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (intent_id, str(schedule_key), str(period_key), person_id, str(run_id), "intent", at, at),
                )
                if cursor.rowcount == 1:
                    created.append(person_id)
                else:
                    existing.append(person_id)
        return {"created": created, "existing": existing}

    def update_delivery_intents(
        self,
        *,
        schedule_key: str,
        period_key: str,
        person_ids: Iterable[str],
        status: str,
        receipt: Optional[Mapping[str, Any]] = None,
    ) -> int:
        if status not in {"attempting", "uncertain", "confirmed", "failed_after_effect", "suppressed"}:
            raise ValueError("invalid delivery intent status")
        ids = sorted({str(item).strip() for item in person_ids if str(item).strip()})
        if not ids:
            return 0
        at = now_iso()
        payload = _json(receipt)
        with self.transaction() as connection:
            updated = 0
            for person_id in ids:
                cursor = connection.execute(
                    "UPDATE delivery_intents SET status=?,receipt_json=?,updated_at=? WHERE schedule_key=? AND period_key=? AND person_id=? AND status NOT IN ('confirmed','uncertain','failed_after_effect')",
                    (status, payload, at, str(schedule_key), str(period_key), person_id),
                )
                updated += cursor.rowcount
            return updated

    def list_delivery_intents(self, *, schedule_key: str, period_key: str) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM delivery_intents WHERE schedule_key=? AND period_key=? ORDER BY person_id",
                (str(schedule_key), str(period_key)),
            ).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                try:
                    value["receipt"] = json.loads(value.pop("receipt_json") or "{}")
                except json.JSONDecodeError:
                    value["receipt"] = {}
                result.append(value)
            return result

    def list_runs(self, *, limit: int = 100, status: Optional[str] = None) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        with self.connect() as connection:
            if status:
                rows = connection.execute("SELECT * FROM runs WHERE status=? ORDER BY created_at DESC LIMIT ?", (status, int(limit))).fetchall()
            else:
                rows = connection.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
            return [dict(row) for row in rows]

    def list_events(self, run_id: str, *, after_seq: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM run_events WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?",
                (run_id, int(after_seq), int(limit)),
            ).fetchall()
            values = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                values.append(item)
            return values

    @staticmethod
    def _watermark_hash(value: Any) -> str:
        if isinstance(value, (dict, list, tuple)):
            encoded = _json(value)
        else:
            encoded = str(value if value is not None else "")
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def put_watermark(
        self,
        *,
        source_domain: str,
        watermark_name: str,
        captured_at: int,
        sequence: int = 0,
        value: Any = None,
        producer: str,
        state: str = "accepted",
    ) -> Dict[str, Any]:
        """Accept one structured watermark and durably classify replays/conflicts.

        The current row is advanced only by a greater ``(captured_at,
        sequence)`` cursor. Equal cursors are idempotent when their hash is
        unchanged; equal-but-different values and older cursors are preserved
        as events without moving the current watermark.
        """
        domain = str(source_domain or "").strip()
        name = str(watermark_name or "").strip()
        if not domain or not name or not str(producer or "").strip():
            raise ValueError("source_domain, watermark_name and producer are required")
        captured = int(captured_at)
        seq = int(sequence)
        value_hash = self._watermark_hash(value)
        value_text = value if isinstance(value, str) else _json(value)
        observed_at = now_iso()
        with self.transaction() as connection:
            current = connection.execute(
                "SELECT captured_at,sequence,value_hash,value,producer,state FROM watermarks WHERE source_domain=? AND watermark_name=?",
                (domain, name),
            ).fetchone()
            cursor = (captured, seq)
            if current is None or cursor > (int(current[0]), int(current[1])):
                outcome = "accepted"
                connection.execute(
                    "INSERT INTO watermarks(source_domain,watermark_name,captured_at,sequence,value_hash,value,producer,state) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(source_domain,watermark_name) DO UPDATE SET captured_at=excluded.captured_at,sequence=excluded.sequence,value_hash=excluded.value_hash,value=excluded.value,producer=excluded.producer,state=excluded.state",
                    (domain, name, captured, seq, value_hash, value_text, producer, state),
                )
            elif cursor == (int(current[0]), int(current[1])) and value_hash == str(current[2]):
                outcome = "idempotent"
            elif cursor == (int(current[0]), int(current[1])):
                outcome = "quarantine"
            else:
                outcome = "replay_rejected"
            connection.execute(
                "INSERT INTO watermark_events(source_domain,watermark_name,captured_at,sequence,value_hash,state,observed_at,details_json) VALUES(?,?,?,?,?,?,?,?)",
                (domain, name, captured, seq, value_hash, outcome, observed_at, _json({"producer": producer, "value": value_text})),
            )
            row = connection.execute(
                "SELECT source_domain,watermark_name,captured_at,sequence,value_hash,value,producer,state FROM watermarks WHERE source_domain=? AND watermark_name=?",
                (domain, name),
            ).fetchone()
        return {"source_domain": domain, "watermark_name": name, "outcome": outcome, "current": dict(row) if row else None}

    # Explicit aliases make the API discoverable to migration/producer code.
    write_watermark = put_watermark

    def list_watermarks(self, *, source_domain: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            if source_domain:
                rows = connection.execute(
                    "SELECT * FROM watermarks WHERE source_domain=? ORDER BY watermark_name", (str(source_domain),)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM watermarks ORDER BY source_domain,watermark_name").fetchall()
            return [dict(row) for row in rows]

    def list_watermark_events(self, *, source_domain: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            params: Tuple[Any, ...]
            if source_domain:
                rows = connection.execute(
                    "SELECT * FROM watermark_events WHERE source_domain=? ORDER BY event_id DESC LIMIT ?",
                    (str(source_domain), max(1, min(int(limit), 1000))),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM watermark_events ORDER BY event_id DESC LIMIT ?", (max(1, min(int(limit), 1000)),)
                ).fetchall()
            return [dict(row) for row in rows]

    def record_memory_change_event(
        self,
        *,
        name: str,
        mtime: int,
        content_hash: str,
        snapshot_uri: Optional[str] = None,
        namespace_epoch: str = "v4",
    ) -> Dict[str, Any]:
        """Persist a watcher observation; this method never performs remote I/O."""
        note = str(name or "").strip()
        digest = str(content_hash or "").strip()
        if not note or not digest:
            raise ValueError("name and content_hash are required")
        event_id = _new_id("memory-event")
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM memory_change_events WHERE name=? AND content_hash=? AND namespace_epoch=?",
                (note, digest, str(namespace_epoch)),
            ).fetchone()
            if existing is not None:
                return {**dict(existing), "deduplicated": True}
            connection.execute(
                "INSERT INTO memory_change_events(event_id,name,mtime,content_hash,snapshot_uri,observed_at,namespace_epoch,state) VALUES(?,?,?,?,?,?,?,'pending')",
                (event_id, note, int(mtime), digest, snapshot_uri, at, str(namespace_epoch)),
            )
            row = connection.execute("SELECT * FROM memory_change_events WHERE event_id=?", (event_id,)).fetchone()
        return {**dict(row), "deduplicated": False} if row else {"event_id": event_id, "deduplicated": False}

    def consume_memory_change_event(self, event_id: str, *, state: str = "consumed") -> bool:
        terminal_state = str(state or "consumed").strip().lower()
        if terminal_state not in {"consumed", "quarantine", "skipped"}:
            raise ValueError("invalid memory event state")
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE memory_change_events SET state=?,consumed_at=? WHERE event_id=? AND consumed_at IS NULL",
                (terminal_state, now_iso(), str(event_id)),
            )
            return cursor.rowcount == 1

    def list_memory_change_events(self, *, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            if state:
                rows = connection.execute(
                    "SELECT * FROM memory_change_events WHERE state=? ORDER BY observed_at,event_id LIMIT ?",
                    (str(state), max(1, min(int(limit), 1000))),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM memory_change_events ORDER BY observed_at,event_id LIMIT ?",
                    (max(1, min(int(limit), 1000)),),
                ).fetchall()
            return [dict(row) for row in rows]

    def enqueue_memory_change(
        self,
        *,
        name: str,
        mtime: int,
        content_hash: str,
        snapshot_uri: Optional[str] = None,
        file_path: Optional[str] = None,
        namespace_epoch: str = "v4",
    ) -> Dict[str, Any]:
        """Atomically persist a watcher event and a ``memory-skill`` Outbox row."""
        note = str(name or "").strip()
        digest = str(content_hash or "").strip()
        epoch = str(namespace_epoch or "v4")
        if not note or not digest:
            raise ValueError("name and content_hash are required")
        at = now_iso()
        logical_key = f"memory:{note}:{digest}"
        resource_id = str(snapshot_uri or f"viking://resources/memory/{note}/{note}")
        with self.transaction() as connection:
            event = connection.execute(
                "SELECT * FROM memory_change_events WHERE name=? AND content_hash=? AND namespace_epoch=?",
                (note, digest, epoch),
            ).fetchone()
            if event is None:
                event_id = _new_id("memory-event")
                connection.execute(
                    "INSERT INTO memory_change_events(event_id,name,mtime,content_hash,snapshot_uri,observed_at,namespace_epoch,state) VALUES(?,?,?,?,?,?,?,'pending')",
                    (event_id, note, int(mtime), digest, snapshot_uri, at, epoch),
                )
            else:
                event_id = str(event[0])
            outbox = connection.execute(
                "SELECT * FROM outbox_items WHERE kind='memory' AND profile='memory-skill' AND idempotency_key=? AND namespace_epoch=?",
                (logical_key, epoch),
            ).fetchone()
            if outbox is None:
                outbox_id = f"outbox-{hashlib.sha256((logical_key + '|' + epoch).encode('utf-8')).hexdigest()[:24]}"
                payload = {"event_id": event_id, "name": note, "file_path": file_path, "target_uri": resource_id, "content_hash": digest, "processing_mode": "vectors_only", "kind": "memory"}
                retry_deadline_at = (datetime.now(timezone.utc) + timedelta(seconds=3600)).isoformat(timespec="seconds").replace("+00:00", "Z")
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,kind,resource_id,revision_id,processing_mode,provider,profile,owner,namespace_epoch,payload_json,status,retry_deadline_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (outbox_id, logical_key, "memory", resource_id, digest, "vectors_only", "openviking", "memory-skill", "memory-watcher", epoch, _json(payload), "pending", retry_deadline_at, at, at),
                )
            else:
                outbox_id = str(outbox[0])
        return {"event_id": event_id, "outbox_id": outbox_id, "idempotency_key": logical_key, "deduplicated": event is not None and outbox is not None}

    def begin_operation(
        self,
        *,
        operation_type: str,
        idempotency_key: str,
        target_uri: Optional[str] = None,
        request_hash: Optional[str] = None,
        namespace_epoch: str = "v4",
        attempt: int = 1,
    ) -> Dict[str, Any]:
        """Open or read one transport operation ledger row."""
        op_type = str(operation_type or "").strip()
        key = str(idempotency_key or "").strip()
        if not op_type or not key:
            raise ValueError("operation_type and idempotency_key are required")
        attempt_value = max(1, int(attempt))
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM operation_ledger WHERE operation_type=? AND idempotency_key=? AND attempt=?",
                (op_type, key, attempt_value),
            ).fetchone()
            if existing is not None:
                if request_hash and existing[4] and str(existing[4]) != str(request_hash):
                    raise ValueError("operation request hash conflicts with existing ledger row")
                return {**dict(existing), "deduplicated": True}
            operation_id = _new_id("operation")
            connection.execute(
                "INSERT INTO operation_ledger(operation_id,operation_type,idempotency_key,target_uri,request_hash,namespace_epoch,attempt,response_state,response_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending','{}',?,?)",
                (operation_id, op_type, key, target_uri, request_hash, str(namespace_epoch), attempt_value, at, at),
            )
            row = connection.execute("SELECT * FROM operation_ledger WHERE operation_id=?", (operation_id,)).fetchone()
        return {**dict(row), "deduplicated": False} if row else {"operation_id": operation_id, "deduplicated": False}

    def finish_operation(
        self,
        operation_id: str,
        *,
        response_state: str,
        response: Any = None,
    ) -> bool:
        state = str(response_state or "unknown").strip().lower()
        if state not in {"accepted", "completed", "failed", "unknown", "quarantine", "pending"}:
            raise ValueError("invalid operation response_state")
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE operation_ledger SET response_state=?,response_json=?,updated_at=? WHERE operation_id=?",
                (state, _json(response), now_iso(), str(operation_id)),
            )
            return cursor.rowcount == 1

    def list_operations(self, *, operation_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            if operation_type:
                rows = connection.execute(
                    "SELECT * FROM operation_ledger WHERE operation_type=? ORDER BY updated_at DESC LIMIT ?",
                    (str(operation_type), max(1, min(int(limit), 1000))),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM operation_ledger ORDER BY updated_at DESC LIMIT ?",
                    (max(1, min(int(limit), 1000)),),
                ).fetchall()
            return [dict(row) for row in rows]

    def classify_historical_failure(
        self,
        *,
        entity_type: str,
        entity_id: str,
        original_status: str,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Classify legacy failures once; incomplete evidence is quarantined."""
        original = str(original_status or "").strip().lower()
        evidence_map = dict(evidence or {})
        if original in {"failed", "permanent_failed"}:
            required = ("artifact_uri", "model_input_hash", "provider", "error_fingerprint", "owner", "revision_id")
            failure_class = "replayable" if all(str(evidence_map.get(key) or "").strip() for key in required) else "quarantine"
        elif original in {"interrupted", "cancelled", "canceled"}:
            failure_class = "terminal"
        elif canonical_status(original) == "completed":
            failure_class = "completed"
        else:
            failure_class = "quarantine"
        evidence_hash = hashlib.sha256(_json(evidence_map).encode("utf-8")).hexdigest() if evidence_map else None
        at = now_iso()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM historical_failure_classifications WHERE entity_type=? AND entity_id=?",
                (str(entity_type), str(entity_id)),
            ).fetchone()
            if existing is not None:
                return {**dict(existing), "deduplicated": True}
            connection.execute(
                "INSERT INTO historical_failure_classifications(entity_type,entity_id,original_status,failure_class,evidence_hash,classified_at,details_json) VALUES(?,?,?,?,?,?,?)",
                (str(entity_type), str(entity_id), original, failure_class, evidence_hash, at, _json(evidence_map)),
            )
            row = connection.execute(
                "SELECT * FROM historical_failure_classifications WHERE entity_type=? AND entity_id=?",
                (str(entity_type), str(entity_id)),
            ).fetchone()
        return {**dict(row), "deduplicated": False} if row else {"entity_type": str(entity_type), "entity_id": str(entity_id), "failure_class": failure_class, "deduplicated": False}

    def list_historical_failure_classifications(self, *, failure_class: Optional[str] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            if failure_class:
                rows = connection.execute(
                    "SELECT * FROM historical_failure_classifications WHERE failure_class=? ORDER BY classified_at,entity_id LIMIT ?",
                    (str(failure_class), max(1, min(int(limit), 5000))),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM historical_failure_classifications ORDER BY classified_at,entity_id LIMIT ?",
                    (max(1, min(int(limit), 5000)),),
                ).fetchall()
            return [dict(row) for row in rows]

    def record_retention_observation(self, value: Mapping[str, Any], *, artifact_digest: str) -> Dict[str, Any]:
        """Persist one immutable observer closure and refresh its read projections."""
        run_id = str(value.get("run_id") or "").strip()
        plan = value.get("plan")
        inventory = value.get("inventory")
        unknown_doc = value.get("unknowns")
        if not run_id or not isinstance(plan, Mapping) or not isinstance(inventory, Mapping) or not isinstance(unknown_doc, Mapping):
            raise ValueError("retention observation closure is incomplete")
        plan_id = str(plan.get("plan_id") or "").strip()
        nonce = str(plan.get("nonce") or "")
        observed_at = str(value.get("observed_at") or "").strip()
        if not plan_id or not nonce or not observed_at or not str(artifact_digest).startswith("sha256:"):
            raise ValueError("retention observation identity is invalid")
        nonce_hash = "sha256:" + hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        sources = [dict(item) for item in inventory.get("sources", []) if isinstance(item, Mapping)]
        items = [dict(item) for item in inventory.get("items", []) if isinstance(item, Mapping)]
        unknowns = [dict(item) for item in unknown_doc.get("items", []) if isinstance(item, Mapping)]
        all_sources_complete = bool(sources) and all(item.get("inventory_complete") is True for item in sources)
        with self.transaction() as connection:
            existing = connection.execute("SELECT artifact_digest FROM retention_observer_runs WHERE run_id=?", (run_id,)).fetchone()
            if existing is not None:
                if str(existing[0]) != str(artifact_digest):
                    raise ValueError("retention observer run conflicts with immutable artifact")
                return {"run_id": run_id, "plan_id": plan_id, "deduplicated": True}
            connection.execute(
                """INSERT INTO retention_observer_runs(
                       run_id,occurrence_id,artifact_digest,plan_id,snapshot_token,status,
                       source_registry_hash,policy_hash,deletion_capability_hash,observed_at,payload_json
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, value.get("occurrence_id"), str(artifact_digest), plan_id,
                    str(value.get("snapshot_token") or ""), str(value.get("status") or "unknown"),
                    str(value.get("source_registry_hash") or ""), str(value.get("policy_hash") or ""),
                    str(value.get("deletion_capability_hash") or ""), observed_at,
                    _json({"mode": value.get("mode"), "signature_status": value.get("signature_status"), "summary": value.get("summary")}),
                ),
            )
            for source in sources:
                connection.execute(
                    """INSERT INTO retention_sources(
                           source_id,observer_run_id,display_name,status,mode,inventory_complete,
                           deletion_conclusion_allowed,freshness,object_count,logical_bytes,allocated_bytes,observed_at,payload_json
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(source_id) DO UPDATE SET
                         observer_run_id=excluded.observer_run_id,display_name=excluded.display_name,
                         status=excluded.status,mode=excluded.mode,inventory_complete=excluded.inventory_complete,
                         deletion_conclusion_allowed=excluded.deletion_conclusion_allowed,freshness=excluded.freshness,
                         object_count=excluded.object_count,logical_bytes=excluded.logical_bytes,
                         allocated_bytes=excluded.allocated_bytes,observed_at=excluded.observed_at,payload_json=excluded.payload_json""",
                    (
                        str(source.get("source_id") or "unknown"), run_id, str(source.get("display_name") or "未命名来源"),
                        str(source.get("status") or "unknown"), str(source.get("mode") or "unknown"),
                        None if source.get("inventory_complete") is None else int(bool(source.get("inventory_complete"))),
                        None if source.get("deletion_conclusion_allowed") is None else int(bool(source.get("deletion_conclusion_allowed"))),
                        str(source.get("freshness") or "unknown"), source.get("object_count"), source.get("logical_bytes"),
                        source.get("allocated_bytes"), observed_at, _json(source),
                    ),
                )
                connection.execute(
                    """INSERT INTO retention_metrics_daily(
                           metric_day,source_id,logical_bytes,allocated_bytes,object_count,unknown_count,observed_at
                       ) VALUES(?,?,?,?,?,?,?)
                       ON CONFLICT(metric_day,source_id) DO UPDATE SET
                         logical_bytes=excluded.logical_bytes,allocated_bytes=excluded.allocated_bytes,
                         object_count=excluded.object_count,unknown_count=excluded.unknown_count,observed_at=excluded.observed_at""",
                    (
                        observed_at[:10], str(source.get("source_id") or "unknown"), source.get("logical_bytes"),
                        source.get("allocated_bytes"), source.get("object_count"),
                        sum(1 for item in unknowns if str(item.get("source_id")) == str(source.get("source_id"))), observed_at,
                    ),
                )
            for item in items:
                connection.execute(
                    """INSERT INTO retention_inventory(
                           observer_run_id,object_id,source_id,retention_class,processability,logical_bytes,
                           allocated_bytes,due_at,content_hash,observed_at,payload_json
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        run_id, str(item.get("object_id") or ""), str(item.get("source_id") or ""),
                        str(item.get("retention_class") or "R5"), str(item.get("processability") or "unknown"),
                        int(item.get("logical_bytes") or 0), int(item.get("allocated_bytes") or 0), item.get("due_at"),
                        item.get("content_hash"), str(item.get("observed_at") or observed_at), _json(item),
                    ),
                )
            current_unknown_ids = set()
            for item in unknowns:
                unknown_id = str(item.get("unknown_id") or "").strip()
                if not unknown_id:
                    continue
                current_unknown_ids.add(unknown_id)
                prior = connection.execute("SELECT first_seen_at FROM retention_unknowns WHERE unknown_id=?", (unknown_id,)).fetchone()
                first_seen = str(prior[0]) if prior is not None else str(item.get("first_seen_at") or observed_at)
                connection.execute(
                    """INSERT INTO retention_unknowns(
                           unknown_id,source_id,reason_code,severity,status,first_seen_at,last_seen_at,resolved_at,
                           observer_run_id,logical_bytes,payload_json
                       ) VALUES(?,?,?,?,?,?,?,NULL,?,?,?)
                       ON CONFLICT(unknown_id) DO UPDATE SET
                         source_id=excluded.source_id,reason_code=excluded.reason_code,severity=excluded.severity,
                         status='needs_decision',last_seen_at=excluded.last_seen_at,resolved_at=NULL,
                         observer_run_id=excluded.observer_run_id,logical_bytes=excluded.logical_bytes,payload_json=excluded.payload_json""",
                    (
                        unknown_id, str(item.get("source_id") or "unregistered"), str(item.get("reason_code") or "unknown"),
                        str(item.get("severity") or "P2"), "needs_decision", first_seen,
                        str(item.get("last_seen_at") or observed_at), run_id, item.get("logical_bytes"), _json(item),
                    ),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO retention_unknown_events(unknown_id,observer_run_id,status,occurred_at,payload_json) VALUES(?,?,?,?,?)",
                    (unknown_id, run_id, "needs_decision", observed_at, _json(item)),
                )
            if all_sources_complete:
                rows = connection.execute("SELECT unknown_id FROM retention_unknowns WHERE status='needs_decision'").fetchall()
                for row in rows:
                    unknown_id = str(row[0])
                    if unknown_id in current_unknown_ids:
                        continue
                    connection.execute(
                        "UPDATE retention_unknowns SET status='resolved',resolved_at=?,last_seen_at=? WHERE unknown_id=? AND status='needs_decision'",
                        (observed_at, observed_at, unknown_id),
                    )
                    connection.execute(
                        "INSERT OR IGNORE INTO retention_unknown_events(unknown_id,observer_run_id,status,occurred_at,payload_json) VALUES(?,?,?,?,?)",
                        (unknown_id, run_id, "resolved", observed_at, _json({"reason": "absent_from_complete_observation"})),
                    )
            connection.execute(
                """INSERT INTO retention_plans(
                       plan_id,observer_run_id,observer_occurrence_id,artifact_digest,status,issued_at,expires_at,nonce_hash,payload_json
                   ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    plan_id, run_id, str(plan.get("observer_occurrence_id") or ""), str(artifact_digest),
                    "signed" if plan.get("signature") else "unsigned", str(plan.get("issued_at") or observed_at),
                    str(plan.get("expires_at") or observed_at), nonce_hash, _json(plan),
                ),
            )
        return {"run_id": run_id, "plan_id": plan_id, "deduplicated": False}

    def claim_retention_plan(
        self,
        *,
        plan: Mapping[str, Any],
        artifact_digest: str,
        reclaimer_occurrence_id: str,
        owner: str,
        lease_seconds: int = 7200,
    ) -> Dict[str, Any]:
        """Consume a plan nonce and prepare every action under one fenced transaction."""
        plan_id = str(plan.get("plan_id") or "").strip()
        observer_occurrence_id = str(plan.get("observer_occurrence_id") or "").strip()
        nonce = str(plan.get("nonce") or "")
        items = plan.get("items")
        if not plan_id or not observer_occurrence_id or not nonce or not isinstance(items, list):
            raise ValueError("retention plan claim is incomplete")
        if not reclaimer_occurrence_id or not owner or lease_seconds <= 0:
            raise ValueError("retention reclaimer identity is incomplete")
        nonce_hash = "sha256:" + hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        current = datetime.now(timezone.utc)
        at = current.isoformat(timespec="seconds").replace("+00:00", "Z")
        expires = (current + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT observer_occurrence_id,artifact_digest,nonce_hash,status FROM retention_plans WHERE plan_id=?",
                (plan_id,),
            ).fetchone()
            if row is None:
                raise ValueError("retention plan is not registered in PM System Store")
            if (str(row[0]), str(row[1]), str(row[2]), str(row[3])) != (observer_occurrence_id, str(artifact_digest), nonce_hash, "signed"):
                raise ValueError("retention plan occurrence/artifact/nonce binding mismatch")
            if connection.execute("SELECT 1 FROM retention_nonce_consumptions WHERE nonce_hash=?", (nonce_hash,)).fetchone() is not None:
                raise ValueError("retention plan nonce was already consumed")
            daily_floor = at[:10] + "T00:00:00Z"
            grouped: Dict[Tuple[str, str, str], Dict[str, int]] = {}
            for item in items:
                if not isinstance(item, Mapping) or not isinstance(item.get("gate_results"), Mapping):
                    raise ValueError("retention plan quota boundary is missing")
                gate = item["gate_results"]
                key = (str(item.get("source_id") or ""), str(item.get("action_profile") or ""), str(gate.get("capability") or ""))
                entry = grouped.setdefault(key, {"count": 0, "bytes": 0, "max_objects": int(gate.get("max_objects_per_batch") or 0), "max_bytes": int(gate.get("max_bytes_per_day") or 0)})
                if entry["max_objects"] != int(gate.get("max_objects_per_batch") or 0) or entry["max_bytes"] != int(gate.get("max_bytes_per_day") or 0):
                    raise ValueError("retention plan contains inconsistent capability quotas")
                entry["count"] += 1
                entry["bytes"] += int(item.get("expected_reclaim_bytes") or 0)
            for (source_id, action_profile, _capability_id), quota in grouped.items():
                if quota["max_objects"] < 1 or quota["count"] > quota["max_objects"] or quota["max_bytes"] < 1:
                    raise ValueError("retention plan exceeds per-batch capability quota")
                prior = connection.execute(
                    """SELECT COUNT(*),COALESCE(SUM(expected_reclaim_bytes),0) FROM retention_actions
                       WHERE source_id=? AND action_profile=? AND prepared_at>=?""",
                    (source_id, action_profile, daily_floor),
                ).fetchone()
                if int(prior[1] or 0) + quota["bytes"] > quota["max_bytes"]:
                    raise ValueError("retention plan exceeds daily capability byte quota")
            connection.execute("UPDATE retention_leases SET state='expired' WHERE state='active' AND expires_at<=?", (at,))
            scopes = ["global:retention"]
            scopes.extend(f"source:{str(item.get('source_id') or '')}" for item in items if isinstance(item, Mapping))
            scopes.extend(f"object:{str(item.get('object_id') or '')}" for item in items if isinstance(item, Mapping))
            scopes = sorted(set(scopes))
            for scope in scopes:
                held = connection.execute(
                    "SELECT owner FROM retention_leases WHERE scope_key=? AND state='active' AND expires_at>?",
                    (scope, at),
                ).fetchone()
                if held is not None:
                    raise ValueError(f"retention lease already held: {scope}")
            connection.execute("INSERT INTO retention_fence_tokens(owner,created_at) VALUES(?,?)", (owner, at))
            fencing_token = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            lease_ids = []
            for scope in scopes:
                lease_id = _new_id("retention-lease")
                lease_ids.append(lease_id)
                connection.execute(
                    """INSERT INTO retention_leases(scope_key,lease_id,owner,fencing_token,acquired_at,expires_at,state)
                       VALUES(?,?,?,?,?,?,'active')
                       ON CONFLICT(scope_key) DO UPDATE SET
                         lease_id=excluded.lease_id,owner=excluded.owner,fencing_token=excluded.fencing_token,
                         acquired_at=excluded.acquired_at,expires_at=excluded.expires_at,state='active'""",
                    (scope, lease_id, owner, fencing_token, at, expires),
                )
            connection.execute(
                "INSERT INTO retention_nonce_consumptions(nonce_hash,plan_id,observer_occurrence_id,reclaimer_occurrence_id,artifact_digest,consumed_at) VALUES(?,?,?,?,?,?)",
                (nonce_hash, plan_id, observer_occurrence_id, reclaimer_occurrence_id, str(artifact_digest), at),
            )
            action_ids = []
            for item in items:
                if not isinstance(item, Mapping):
                    raise ValueError("retention plan item is invalid")
                material = [
                    item.get("object_id"), item.get("content_hash"), plan.get("source_registry_hash"),
                    plan.get("policy_hash"), item.get("action_profile"), item.get("due_at"),
                    plan.get("deletion_capability_hash"), plan.get("adapter_bundle_digest"),
                ]
                idempotency_key = "sha256:" + hashlib.sha256(_json(material).encode("utf-8")).hexdigest()
                existing = connection.execute("SELECT action_id FROM retention_actions WHERE idempotency_key=?", (idempotency_key,)).fetchone()
                if existing is not None:
                    raise ValueError("retention action was already prepared")
                action_id = _new_id("retention-action")
                action_ids.append(action_id)
                connection.execute(
                    """INSERT INTO retention_actions(
                           action_id,idempotency_key,plan_id,object_id,source_id,action_profile,state,fencing_token,
                           expected_reclaim_bytes,prepared_at,updated_at,payload_json
                       ) VALUES(?,?,?,?,?,?,'prepared',?,?,?,?,?)""",
                    (
                        action_id, idempotency_key, plan_id, str(item.get("object_id") or ""),
                        str(item.get("source_id") or ""), str(item.get("action_profile") or ""), fencing_token,
                        int(item.get("expected_reclaim_bytes") or 0), at, at, _json(item),
                    ),
                )
                connection.execute(
                    "INSERT INTO retention_action_events(action_id,seq,state,occurred_at,payload_json) VALUES(?,1,'prepared',?,?)",
                    (action_id, at, _json({"plan_id": plan_id, "reclaimer_occurrence_id": reclaimer_occurrence_id})),
                )
        return {
            "plan_id": plan_id, "artifact_digest": str(artifact_digest), "observer_occurrence_id": observer_occurrence_id,
            "reclaimer_occurrence_id": reclaimer_occurrence_id, "fencing_token": fencing_token,
            "lease_ids": lease_ids, "action_ids": action_ids, "claimed_at": at,
        }

    def transition_retention_action(
        self,
        action_id: str,
        *,
        fencing_token: int,
        state: str,
        reason_code: Optional[str] = None,
        message: str = "",
        reclaimed_logical_bytes: int = 0,
        reclaimed_allocated_bytes: int = 0,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        transitions = {
            "prepared": {"applied", "verified", "held", "manual_attention"},
            "applied": {"verified", "rolled_back", "manual_attention"},
            "verified": set(), "rolled_back": set(), "manual_attention": set(), "held": set(),
        }
        target = str(state or "").strip()
        if target not in transitions:
            raise ValueError("invalid retention action state")
        at = now_iso()
        with self.transaction() as connection:
            row = connection.execute("SELECT state,fencing_token FROM retention_actions WHERE action_id=?", (str(action_id),)).fetchone()
            if row is None:
                raise KeyError(action_id)
            if int(row[1]) != int(fencing_token):
                raise ValueError("stale retention fencing token")
            if target not in transitions[str(row[0])]:
                raise ValueError("invalid retention action transition")
            seq = int(connection.execute("SELECT COALESCE(MAX(seq),0)+1 FROM retention_action_events WHERE action_id=?", (str(action_id),)).fetchone()[0])
            connection.execute(
                """UPDATE retention_actions SET state=?,reason_code=?,message=?,reclaimed_logical_bytes=?,
                     reclaimed_allocated_bytes=?,updated_at=? WHERE action_id=? AND fencing_token=?""",
                (target, reason_code, str(message)[:500], max(0, int(reclaimed_logical_bytes)), max(0, int(reclaimed_allocated_bytes)), at, str(action_id), int(fencing_token)),
            )
            connection.execute(
                "INSERT INTO retention_action_events(action_id,seq,state,occurred_at,payload_json) VALUES(?,?,?,?,?)",
                (str(action_id), seq, target, at, _json(payload or {})),
            )
            result = connection.execute("SELECT * FROM retention_actions WHERE action_id=?", (str(action_id),)).fetchone()
        return dict(result) if result is not None else {"action_id": str(action_id), "state": target}

    def release_retention_leases(self, *, owner: str, fencing_token: int) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE retention_leases SET state='released' WHERE owner=? AND fencing_token=? AND state='active'",
                (str(owner), int(fencing_token)),
            )
            return int(cursor.rowcount)

    def list_retention_actions(self, *, limit: int = 1000) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM retention_actions ORDER BY updated_at DESC,action_id LIMIT ?",
                (max(1, min(int(limit), 5000)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def retention_reconciliation_queue(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM retention_actions WHERE state IN ('prepared','applied') ORDER BY prepared_at,action_id LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def retention_runtime_blockers(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        """Return current critical work or incidents that close the maintenance window."""
        result: List[Dict[str, Any]] = []
        cap = max(1, min(int(limit), 100))
        with self.connect() as connection:
            jobs = connection.execute(
                """SELECT job_id,run_id,status,priority,job_type FROM jobs
                   WHERE status IN ('queued','running') AND priority>=90
                   ORDER BY priority DESC,queued_at LIMIT ?""",
                (cap,),
            ).fetchall()
            result.extend({"kind": "critical_job", **dict(row)} for row in jobs)
            remaining = max(0, cap - len(result))
            if remaining:
                alerts = connection.execute(
                    """SELECT alert_id,severity,state,alert_type,module FROM ops_alerts
                       WHERE severity IN ('P0','P1') AND state IN ('open','acknowledged')
                       ORDER BY CASE severity WHEN 'P0' THEN 0 ELSE 1 END,last_seen_at DESC LIMIT ?""",
                    (remaining,),
                ).fetchall()
                result.extend({"kind": "active_incident", **dict(row)} for row in alerts)
        return result

    @classmethod
    def open_or_fallback(cls, db_path: Path, legacy_state_dir: Path) -> StoreOpenResult:
        try:
            return StoreOpenResult(store=cls(db_path), fallback=None)
        except (StoreUnavailable, sqlite3.DatabaseError, OSError) as exc:
            fallback = LegacyRunStoreReadOnlyAdapter(legacy_state_dir, reason=str(exc))
            return StoreOpenResult(store=None, fallback=fallback, reason=str(exc))


class LegacyRunStoreReadOnlyAdapter:
    """Read-only bridge used when the new DB is damaged or unavailable."""

    read_only = True

    def __init__(self, state_dir: Path, *, reason: Optional[str] = None) -> None:
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.reason = reason
        try:
            from pm_loop_runtime import RunStore
        except ImportError:
            self._legacy = None
        else:
            self._legacy = RunStore(self.state_dir)

    def list_states(self) -> List[Dict[str, Any]]:
        if self._legacy is None:
            return []
        return self._legacy.list_states_read_only()

    def state(self, run_id: str) -> Dict[str, Any]:
        if self._legacy is None:
            raise FileNotFoundError(run_id)
        return self._legacy.state_read_only(run_id)

    def events(self, run_id: str) -> List[Dict[str, Any]]:
        if self._legacy is None:
            return []
        return self._legacy.events_for(run_id)

    def accept(self, *_args: Any, **_kwargs: Any) -> None:
        raise ReadOnlyStoreError("legacy RunStore fallback is read-only")

    submit_job = accept
    accept_run = accept


def open_coordination_store(db_path: Path, legacy_state_dir: Path) -> Union[PMSystemStore, LegacyRunStoreReadOnlyAdapter]:
    """Open the new store, or explicitly return a read-only legacy fallback."""
    result = PMSystemStore.open_or_fallback(db_path, legacy_state_dir)
    return result.store if result.store is not None else result.fallback  # type: ignore[return-value]


__all__ = [
    "DEFAULT_BUSY_TIMEOUT_MS",
    "LegacyRunStoreReadOnlyAdapter",
    "MIGRATIONS",
    "MigrationFrozen",
    "MigrationLeaseConflict",
    "PMSystemStore",
    "ReadOnlyStoreError",
    "SCHEMA_ID",
    "SCHEMA_VERSION",
    "CANONICAL_TERMINAL_STATUSES",
    "canonical_status",
    "StoreOpenResult",
    "StoreUnavailable",
    "open_coordination_store",
]
