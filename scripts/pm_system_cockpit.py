#!/usr/bin/env python3
"""Read-only V4.4 cockpit projection backed by ``pm-system.db``.

The projection deliberately has no mutation methods and never probes a remote
dependency.  Missing S2+ signals are returned as ``unknown``/``stale`` until
their background producer is installed.
"""

from __future__ import annotations

import hashlib
import json
import os
import plistlib
import re
import subprocess
import threading
import tomllib
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

from pm_schedule_registry import RegistryError, ScheduleTask, latest_scheduled_at, next_scheduled_at, validate_document
from pm_system_store import PMSystemStore, StoreUnavailable, canonical_status


COCKPIT_SCHEMA = "pm-system.cockpit.v1.3"
MODULES = ("Scheduler", "Worker", "RunStore", "OneAPI", "Outbox", "Outbox Writer", "Memory watcher", "OpenViking", "Source", "Evidence", "Runtime")
# Health snapshots are produced asynchronously.  A missing or older snapshot
# must remain visible as unknown instead of being inferred healthy from empty
# queues.  The deadline is deliberately short enough for the 5-10s dashboard
# poll interval while allowing a transient producer delay.
HEALTH_FRESHNESS_DEADLINE_SECONDS = 15 * 60
KEY_SIGNAL_MODULES = ("Worker", "OneAPI", "OpenViking", "Source", "Evidence", "Runtime")
WORKBENCH_GATE_TTL_SECONDS = 15 * 60
ROLE_PROFILES = (
    {
        "role_id": "customer-requirement-analyst",
        "display_name": "客户需求分析员",
        "skills": ["requirement-fit-assessment", "pm-timeline"],
        "scopes": ["customer_materials", "requirements", "timeline"],
        "output_contract": ["requirement_checklist", "term_clarification", "evidence_set"],
        "runtime_subjects": ["cockpit-reader", "codex-operator:read-only-draft"],
    },
    {
        "role_id": "product-capability-evaluator",
        "display_name": "产品能力评估员",
        "skills": ["requirement-fit-assessment", "shengsuan-concepts"],
        "scopes": ["concepts", "product_docs", "delivery_signals"],
        "output_contract": ["coverage_decision", "capability_boundary", "version_evidence"],
        "runtime_subjects": ["concept-reader", "cockpit-reader"],
    },
    {
        "role_id": "product-gap-analyst",
        "display_name": "产品缺口分析员",
        "skills": ["capability-gap-aggregator", "shengsuan-ontology", "pm-timeline"],
        "scopes": ["cases", "capability_assessments", "version_tasks"],
        "output_contract": ["gap_ranking", "impact_scope", "evidence_count"],
        "runtime_subjects": ["cockpit-reader"],
    },
    {
        "role_id": "customer-follow-up-assistant",
        "display_name": "客户跟进助手",
        "skills": ["pm-timeline"],
        "scopes": ["customer_timeline", "assessment_history"],
        "output_contract": ["follow_up_summary", "open_questions", "message_draft"],
        "runtime_subjects": ["cockpit-reader", "codex-operator:read-only-draft"],
    },
    {
        "role_id": "concept-maintainer",
        "display_name": "概念维护员",
        "skills": ["shengsuan-concepts", "openviking-rest"],
        "scopes": ["concept_ledger", "candidate", "active_hot_projection", "usage"],
        "output_contract": ["maintenance_check", "candidate_diff", "pre_publish_checklist"],
        "runtime_subjects": ["concept-reader"],
    },
    {
        "role_id": "quality-reviewer",
        "display_name": "质量审查员",
        "skills": ["system-health-check", "cross-verify"],
        "scopes": ["cockpit", "runs", "outbox", "evidence", "health_snapshot"],
        "output_contract": ["review_findings", "severity", "reproduction_evidence"],
        "runtime_subjects": ["cockpit-reader", "codex-operator:diagnostic-only"],
    },
)

# Browser-readable reports are deliberately mapped in one small allowlist.
# The role workbench scans these directories instead of indexing arbitrary
# project files, then the Control Plane resolves an opaque ID back through the
# same allowlist when a user opens a historical artifact.
ROLE_OUTPUT_SPECS = (
    {
        "role_id": "product-gap-analyst",
        "schedule_key": "databuilder-product-gap-report",
        "title": "DataBuilder 产品缺口周报",
        "kind": "产品周报",
        "patterns": (
            "docs/产品缺口周报/产品缺口与安排建议-*.html",
            "docs/DataBuilder产品缺口与安排建议-*.html",
        ),
    },
    {
        "role_id": "product-capability-evaluator",
        "schedule_key": "product-docs-gap-report",
        "title": "胜算产品资料缺失周报",
        "kind": "资料周报",
        "patterns": (
            "docs/04-产品设计/资料缺失周报/胜算产品资料缺失周报-*.html",
            "docs/04-产品设计/胜算产品资料缺失分析与建议*.html",
            "docs/04-产品设计/基本概念-资料评审意见-*.html",
        ),
    },
    {
        "role_id": "customer-follow-up-assistant",
        "schedule_key": "pm-timeline-weekly",
        "title": "PM 周度总结",
        "kind": "周度复盘",
        "patterns": ("docs/reviews/????-W??-review.html",),
    },
)

SCHEDULE_OUTPUT_LABELS = {
    "weekly-sync-and-refresh": "资料与知识源同步结果",
    "concept-refresh-planner": "概念刷新计划",
    "product-intelligence-monitor": "产品情报周度快照",
    "pm-timeline-daily": "每日 PM 时间轴摘要",
    "pm-timeline-weekly": "PM 周度总结",
    "product-docs-gap-report": "胜算产品资料缺失周报",
    "databuilder-product-gap-report": "DataBuilder 产品缺口周报",
    "weekly-report-reminder": "周报催办执行回执",
    "competitive-radar-ingest": "竞品雷达抓取批次",
    "competitive-radar-brief": "竞品雷达周报",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _snapshot_freshness(observed_at: Any, *, now: Optional[datetime] = None) -> str:
    observed = _parse_timestamp(observed_at)
    if observed is None:
        return "stale"
    current = now or datetime.now(timezone.utc)
    age = (current - observed).total_seconds()
    # ``read_at`` is emitted at second precision while health producers may
    # retain microseconds.  Treat a small future skew as fresh rather than
    # raising a false stale P1 from the same snapshot transaction.
    return "fresh" if -5 <= age <= HEALTH_FRESHNESS_DEADLINE_SECONDS else "stale"


def _looks_like_error_fingerprint(value: str) -> bool:
    """Return true for compact hashes/fingerprints that are not explanations."""
    normalized = value.strip().lower()
    return bool(re.fullmatch(r"(?:sha256:)?[0-9a-f]{16,128}", normalized))


class CockpitReadModel:
    """Bounded read model used by the dashboard and six GET endpoints."""

    read_only = True

    def __init__(self, store: PMSystemStore, *, runtime_home: Optional[Path] = None, project_root: Optional[Path] = None) -> None:
        self.store = store
        # Files outside SQLite are read-only evidence for the schedules view.
        # Keeping the root injectable makes the projection deterministic in
        # isolated tests without changing the production home directory.
        self.runtime_home = Path(runtime_home or Path.home()).expanduser().resolve()
        self.project_root = Path(project_root).expanduser().resolve() if project_root else None

    @classmethod
    def open_existing(cls, db_path: Path, *, runtime_home: Optional[Path] = None, project_root: Optional[Path] = None) -> Optional["CockpitReadModel"]:
        path = Path(db_path).expanduser().resolve()
        if not path.is_file():
            return None
        try:
            return cls(PMSystemStore(path, auto_migrate=False, read_only=True), runtime_home=runtime_home, project_root=project_root)
        except (StoreUnavailable, OSError):
            return None

    @contextmanager
    def _snapshot_connection(self) -> Iterator[Any]:
        """Hold one SQLite read transaction for a complete immutable response."""
        connection = self.store.connect()
        try:
            connection.execute("BEGIN")
            yield connection
        finally:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
            connection.close()

    def _counts(self, connection: Any) -> Dict[str, int]:
        def count(table: str, where: str = "", params: tuple = ()) -> int:
            sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
            return int(connection.execute(sql, params).fetchone()[0])

        return {
            "jobs": count("jobs"),
            "queued_jobs": count("jobs", "status='queued'"),
            "running_jobs": count("jobs", "status='running'"),
            "runs": count("runs"),
            "running_runs": count("runs", "status='running'"),
            "retry_wait_runs": count("runs", "status='retry_wait'"),
            "active_slots": count("execution_slots", "status='leased'"),
            "max_slots": count("execution_slots"),
            "outbox_pending": count("outbox_items", "status IN ('pending','retry_wait','in_flight')"),
            "semantic_queued": count("semantic_tasks", "status IN ('queued','retry_wait','in_flight')"),
            "semantic_accepted": count("semantic_tasks", "status='accepted'"),
            "semantic_processing": count("semantic_tasks", "status='processing'"),
            "semantic_degraded": count("semantic_tasks", "status='degraded'"),
            # Terminal failures are distinct from dead-letter rows and from
            # active queue work. Keep both visible for operator triage.
            "failed": count("semantic_tasks", "status IN ('failed','permanent_failed')") + count("outbox_items", "status IN ('failed','permanent_failed')"),
            "failed_outbox": count("outbox_items", "status IN ('failed','permanent_failed')"),
            "failed_semantic": count("semantic_tasks", "status IN ('failed','permanent_failed')"),
            "dead_letter": count("semantic_tasks", "status='dead_letter'") + count("outbox_items", "status='dead_letter'"),
            "dead_letter_outbox": count("outbox_items", "status='dead_letter'"),
            "dead_letter_semantic": count("semantic_tasks", "status='dead_letter'"),
            "errors": count("error_events"),
            "quarantine": count("semantic_tasks", "status='quarantine'") + count("outbox_items", "status='quarantine'"),
        }

    @staticmethod
    def _table_exists(connection: Any, table: str) -> bool:
        """Return whether an optional projection table is present.

        The cockpit is deliberately usable during a staged migration.  Missing
        concept-domain tables therefore mean ``not_implemented``/``unknown``;
        they must never turn a read request into a 500 or trigger a migration.
        """
        return connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone() is not None

    @staticmethod
    def _optional_rows(
        connection: Any,
        table: str,
        columns: List[str],
        *,
        where: Optional[str] = None,
        params: tuple[Any, ...] = (),
        order_by: Optional[List[str]] = None,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Project an optional table without assuming every migration column.

        Concept tables can be introduced in several staged versions.  Missing
        columns are exposed as ``NULL`` so the dashboard remains truthful and
        readable while the schema gate reports the incomplete capability.
        """
        if not CockpitReadModel._table_exists(connection, table):
            return []
        quoted_table = '"' + table.replace('"', '""') + '"'
        available = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({quoted_table})").fetchall()
        }

        def quote(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        projection = [quote(name) if name in available else f"NULL AS {quote(name)}" for name in columns]
        sql = f"SELECT {','.join(projection)} FROM {quoted_table}"
        if where:
            # Callers pass fixed, internal predicates only. This preserves the
            # staged-schema behavior while allowing the dashboard to sample a
            # meaningful subset (for example quarantined source mappings).
            sql += " WHERE " + where
        order = [name for name in (order_by or []) if name in available]
        if order:
            direction = " DESC" if descending else " ASC"
            sql += " ORDER BY " + ",".join(quote(name) + direction for name in order)
        if limit is not None:
            sql += " LIMIT ?"
            query_params = (*params, max(1, int(limit)))
        else:
            query_params = params
        return [dict(row) for row in connection.execute(sql, query_params).fetchall()]

    @staticmethod
    def _watermark_value(connection: Any, name: str) -> Any:
        """Read only an accepted structured watermark in cursor order.

        Legacy tables remain readable for pre-G2 fixtures, but they are never
        used to manufacture an accepted value when a producer has explicitly
        recorded ``missing``/``unknown`` state.
        """
        row = None
        if CockpitReadModel._table_exists(connection, "watermarks"):
            row = connection.execute(
                "SELECT source_domain,watermark_name,captured_at,sequence,value_hash,value,producer,state "
                "FROM watermarks WHERE watermark_name=? ORDER BY captured_at DESC,sequence DESC,rowid DESC LIMIT 1",
                (name,),
            ).fetchone()
        if row is not None:
            # A structured row is authoritative even when it explicitly says
            # missing/unknown/replay_rejected/quarantine.  Falling back to a
            # legacy table here would turn an observed absence into a false
            # healthy value.
            if str(row[7] or "") != "accepted":
                return None
            return {"value": row[5], "source_domain": row[0], "captured_at": row[2], "sequence": row[3], "value_hash": row[4], "producer": row[6], "state": row[7]}
        if name == "source":
            if not CockpitReadModel._table_exists(connection, "source_snapshots"):
                return None
            row = connection.execute("SELECT source_id,source_revision,captured_at FROM source_snapshots WHERE status='committed' ORDER BY captured_at DESC,rowid DESC LIMIT 1").fetchone()
            return {"value": row[1], "source_domain": row[0], "captured_at": row[2], "legacy_fallback": True} if row else None
        if name == "content":
            if not CockpitReadModel._table_exists(connection, "source_snapshots"):
                return None
            row = connection.execute("SELECT source_id,content_sha256,captured_at FROM source_snapshots WHERE status='committed' ORDER BY captured_at DESC,rowid DESC LIMIT 1").fetchone()
            return {"value": row[1], "source_domain": row[0], "captured_at": row[2], "legacy_fallback": True} if row else None
        if name == "knowledge":
            if not CockpitReadModel._table_exists(connection, "generations"):
                return None
            row = connection.execute("SELECT domain,knowledge_watermark,created_at FROM generations WHERE status='active' ORDER BY created_at DESC,rowid DESC LIMIT 1").fetchone()
            return {"value": row[1], "source_domain": row[0], "captured_at": row[2], "legacy_fallback": True} if row else None
        if not CockpitReadModel._table_exists(connection, "generations"):
            return None
        row = connection.execute("SELECT domain,active_at FROM generations WHERE status='active' AND active_at IS NOT NULL ORDER BY active_at DESC,rowid DESC LIMIT 1").fetchone()
        return {"value": row[1], "source_domain": row[0], "captured_at": row[1], "legacy_fallback": True} if row else None

    @staticmethod
    def _stable_value(value: Any) -> Any:
        if isinstance(value, bytes):
            return {"bytes_hex": value.hex()}
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _source_version(self, connection: Any, _counts: Optional[Dict[str, int]] = None) -> str:
        """Hash every canonical table row so any projected mutation invalidates ETag."""
        digest = hashlib.sha256()
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            table_info = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
            columns = [str(row[1]) for row in table_info]
            digest.update(json.dumps([table, columns], ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            # Core tables currently use rowid, but a future projection may be
            # WITHOUT ROWID.  Prefer the declared primary key, then a stable
            # all-column order, and only fall back to planner order if SQLite
            # exposes no sortable columns.
            primary_key = [str(row[1]) for row in sorted(table_info, key=lambda value: int(value[5] or 0)) if int(row[5] or 0) > 0]
            order_columns = primary_key or columns
            order_sql = ""
            if order_columns:
                order_sql = " ORDER BY " + ",".join('"' + item.replace('"', '""') + '"' for item in order_columns)
            try:
                rows = connection.execute(f"SELECT * FROM {quoted}{order_sql}").fetchall()
            except Exception:
                rows = connection.execute(f"SELECT * FROM {quoted}").fetchall()
            for row in rows:
                values = [self._stable_value(value) for value in row]
                digest.update(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                digest.update(b"\n")
        return "sha256:" + digest.hexdigest()

    def modules(self, *, connection: Any = None) -> List[Dict[str, Any]]:
        owned = connection is None
        connection = connection or self.store.connect()
        try:
            rows: Dict[str, Dict[str, Any]] = {}
            for module in MODULES:
                row = connection.execute("SELECT status,observed_at,details_json,source_version FROM module_health_snapshots WHERE module=? ORDER BY observed_at DESC LIMIT 1", (module,)).fetchone()
                if row is None:
                    rows[module] = {"module": module, "status": "unknown", "freshness": "stale", "observed_at": None, "details": {}, "source_version": None}
                else:
                    try:
                        details = json.loads(row[2] or "{}")
                    except json.JSONDecodeError:
                        details = {}
                    raw_status = str(row[0])
                    freshness = _snapshot_freshness(row[1])
                    status = "unknown" if freshness == "stale" and raw_status in {"healthy", "degraded"} else raw_status
                    # ``maintenance`` snapshots written during the migration
                    # freeze are historical evidence.  Without a newer
                    # producer signal they must not make the current execution
                    # capability look healthy (or remain permanently in
                    # maintenance after the freeze has ended).
                    if module in {"Scheduler", "Worker"} and freshness == "stale" and raw_status == "maintenance":
                        status = "unknown"
                    rows[module] = {"module": module, "status": status, "freshness": freshness, "observed_at": row[1], "details": details, "source_version": row[3]}
            counts = self._counts(connection)
            schema_version = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
            # The coordination ledger itself is verifiable from this same
            # read transaction. Scheduler/Worker presence is never inferred
            # from an empty queue or free slots.  A fresh scheduler tick and a
            # valid registry supersede freeze-era health snapshots; the old
            # rows remain untouched for audit history.
            latest_tick = None
            registry = None
            if self._table_exists(connection, "scheduler_ticks"):
                latest_tick = connection.execute(
                    "SELECT tick_id,status,started_at,completed_at,registry_hash,error FROM scheduler_ticks ORDER BY started_at DESC,tick_id DESC LIMIT 1"
                ).fetchone()
            if self._table_exists(connection, "schedule_registry_state"):
                registry = connection.execute(
                    "SELECT state,registry_hash,updated_at,error FROM schedule_registry_state WHERE registry_id=1 LIMIT 1"
                ).fetchone()
            if latest_tick is not None:
                tick_at = latest_tick[3] or latest_tick[2]
                tick_freshness = _snapshot_freshness(tick_at)
                registry_valid = registry is None or str(registry[0] or "") == "valid"
                registry_match = registry is None or not latest_tick[4] or str(registry[1] or "") == str(latest_tick[4])
                scheduler_details = {
                    "active_slots": counts["active_slots"],
                    "max_slots": counts["max_slots"],
                    "presence_inferred": False,
                    "scheduler_tick_id": latest_tick[0],
                    "scheduler_tick_status": latest_tick[1],
                    "scheduler_tick_at": tick_at,
                    "registry_state": registry[0] if registry is not None else None,
                    "registry_hash": registry[1] if registry is not None else latest_tick[4],
                    "registry_match": registry_match,
                }
                if tick_freshness == "fresh" and str(latest_tick[1]) == "completed" and registry_valid and registry_match:
                    rows["Scheduler"].update(status="healthy", freshness="fresh", observed_at=tick_at, source_version=latest_tick[4] or (registry[1] if registry else None), details=scheduler_details)
                elif tick_freshness == "fresh" and str(latest_tick[1]) == "failed":
                    rows["Scheduler"].update(status="incident", freshness="fresh", observed_at=tick_at, details=scheduler_details | {"error": latest_tick[5]})
                elif tick_freshness == "stale":
                    rows["Scheduler"].update(status="unknown", freshness="stale", observed_at=tick_at, details=scheduler_details)
            else:
                rows["Scheduler"]["details"].update({"active_slots": counts["active_slots"], "max_slots": counts["max_slots"], "presence_inferred": False})

            # Worker snapshots are not refreshed on every idle poll.  Use a
            # fresh lease heartbeat or a recent event emitted by the actual
            # coordination worker as live evidence; a plain scheduler claim
            # (actor ``worker``) is intentionally insufficient.
            worker_evidence = None
            if self._table_exists(connection, "execution_slots"):
                worker_evidence = connection.execute(
                    "SELECT slot_id,run_id,status,heartbeat_at,leased_at,expires_at,pid FROM execution_slots WHERE status='leased' AND pid IS NOT NULL ORDER BY heartbeat_at DESC,leased_at DESC LIMIT 1"
                ).fetchone()
            worker_at = None
            worker_detail: Dict[str, Any] = {"presence_inferred": False}
            if worker_evidence is not None:
                worker_at = worker_evidence[3] or worker_evidence[4]
                worker_detail.update({"evidence_type": "lease", "slot_id": worker_evidence[0], "run_id": worker_evidence[1], "heartbeat_at": worker_evidence[3], "expires_at": worker_evidence[5], "pid": worker_evidence[6]})
            if worker_at is None and self._table_exists(connection, "run_events"):
                worker_event = connection.execute(
                    "SELECT event_type,actor,run_id,occurred_at,payload_json FROM run_events WHERE (actor='coordination-worker' OR actor LIKE 'worker-%') ORDER BY occurred_at DESC,event_id DESC LIMIT 1"
                ).fetchone()
                if worker_event is not None:
                    worker_at = worker_event[3]
                    worker_detail.update({"evidence_type": "run_event", "event_type": worker_event[0], "actor": worker_event[1], "run_id": worker_event[2], "payload": self._json_field(worker_event[4], {})})
            if worker_at is None or _snapshot_freshness(worker_at) == "stale":
                launchd = self._launchd_runtime_evidence("com.zhujie14.pm-system-worker", expected_db=self.store.db_path)
                if launchd and launchd.get("state") == "running":
                    worker_at = _now()
                    worker_detail.update({"evidence_type": "launchd", "launchd": launchd})
            if worker_at is not None and _snapshot_freshness(worker_at) == "fresh":
                rows["Worker"].update(status="healthy", freshness="fresh", observed_at=worker_at, details=worker_detail)
            elif worker_at is not None:
                rows["Worker"].update(status="unknown", freshness="stale", observed_at=worker_at, details=worker_detail)

            rows["RunStore"].update({"status": "healthy", "freshness": "fresh", "details": {"schema_version": schema_version, "connection_mode": "read_only" if self.store.read_only else "compatibility"}})
            # Failed/dead-letter rows are durable history.  Current attention
            # state comes from ops_alerts and fresh source evidence, so these
            # counters are exposed without turning an otherwise idle Outbox
            # into a current degraded module.
            rows["Outbox"].update({"status": "healthy", "freshness": "fresh", "details": {"pending": counts["outbox_pending"], "historical_failed": counts["failed_outbox"], "historical_dead_letter": counts["dead_letter_outbox"]}})
            return [rows[name] for name in MODULES]
        finally:
            if owned:
                connection.close()

    @staticmethod
    def _json_field(value: Any, default: Any) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return default

    @staticmethod
    def _gate_result(gate_id: str, observed_at: str, checks: List[Dict[str, Any]], protected_modules: List[str]) -> Dict[str, Any]:
        normalized_checks: List[Dict[str, Any]] = []
        for check in checks:
            item = dict(check)
            evidence_ref = str(item.get("evidence_ref") or "")
            item.setdefault("source_cursor", evidence_ref or None)
            item.setdefault("reason", "" if item.get("status") == "pass" else "evidence unavailable")
            if not item.get("source_hash") and evidence_ref:
                item["source_hash"] = evidence_ref if evidence_ref.startswith("sha256:") else "sha256:" + hashlib.sha256(evidence_ref.encode("utf-8")).hexdigest()
            normalized_checks.append(item)
        if any(item["status"] == "incident" for item in normalized_checks):
            decision = "disabled"
        elif any(item["status"] in {"unknown", "failed"} for item in normalized_checks):
            decision = "unknown"
        else:
            decision = "enabled"
        expires_at = (datetime.fromisoformat(observed_at.replace("Z", "+00:00")) + timedelta(seconds=WORKBENCH_GATE_TTL_SECONDS)).isoformat(timespec="seconds").replace("+00:00", "Z")
        return {
            "manifest_version": "pm-loop.workbench-gate.v1",
            "gate_id": gate_id,
            "owner": "control-plane",
            "observed_at": observed_at,
            "expires_at": expires_at,
            "required_checks": normalized_checks,
            "source_hashes": {item["check_id"]: item.get("source_hash") for item in normalized_checks},
            "decision": decision,
            "protected_modules": protected_modules,
        }

    @staticmethod
    def _apply_concept_recovery_gate(gate: Dict[str, Any], admission: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        """Keep Admission=disabled distinct from a retired concept workflow."""
        if not isinstance(admission, Mapping) or str(admission.get("admission_state") or "") != "disabled":
            return gate
        value = dict(gate)
        checks = [dict(item) for item in value.get("required_checks") or [] if isinstance(item, Mapping)]
        for check in checks:
            if check.get("check_id") == "admission_state":
                check["status"] = "pass"
                check["reason"] = "Admission=disabled 是恢复门禁：阻止未批准发布，不代表概念系统已停用。"
                check["evidence_ref"] = f"concept_admissions:{admission.get('version') or 'unknown'}"
        value["required_checks"] = checks
        value["source_hashes"] = {str(item.get("check_id") or ""): item.get("source_hash") for item in checks}
        value["decision"] = "recovery_gated"
        value["workflow_status"] = "recovery_gated"
        value["refresh_trigger"] = "pm_scheduler_dependency"
        value["reason"] = "概念刷新由 PM Scheduler 在 weekly-sync-and-refresh 成功后依赖触发；Admission=disabled 仅阻止未批准写入和发布。"
        return value

    def _gate_manifest(self, connection: Any, source_version: str, observed_at: str) -> Dict[str, Any]:
        admission_rows = self._optional_rows(
            connection,
            "concept_admissions",
            ["admission_state", "namespace_epoch", "version", "expires_at", "renewal_policy", "evidence_hash"],
            order_by=["updated_at", "observed_at"],
            descending=True,
            limit=1,
        )
        admission = admission_rows[0] if admission_rows else None
        if self._table_exists(connection, "workbench_gate_manifest"):
            rows = connection.execute(
                "SELECT * FROM workbench_gate_manifest ORDER BY updated_at DESC,gate_id DESC"
            ).fetchall()
            if rows:
                manifests: Dict[str, Dict[str, Any]] = {}
                now = _parse_timestamp(observed_at) or datetime.now(timezone.utc)
                for row in rows:
                    item = dict(row)
                    item["required_checks"] = self._json_field(item.pop("required_checks_json"), [])
                    item["source_hashes"] = self._json_field(item.pop("source_hashes_json"), {})
                    item["protected_modules"] = self._json_field(item.pop("protected_modules_json"), [])
                    expires = _parse_timestamp(item.get("expires_at"))
                    if expires is None or expires <= now:
                        item["decision"] = "unknown"
                        item["reason"] = item.get("reason") or "gate manifest expired"
                    item["read_only"] = True
                    manifests.setdefault(str(item.get("gate_id")), item)
                if manifests:
                    concept_gate = manifests.get("concept_view_gate")
                    if isinstance(concept_gate, dict):
                        manifests["concept_view_gate"] = self._apply_concept_recovery_gate(concept_gate, admission)
                    return manifests
        schema_version = int(connection.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0])
        freeze = connection.execute("SELECT state,migration_id,stage_id FROM migration_freeze WHERE freeze_id=1").fetchone() if self._table_exists(connection, "migration_freeze") else None
        g9 = connection.execute("SELECT state FROM migration_leases WHERE stage_id='G9' ORDER BY lease_expires_at DESC LIMIT 1").fetchone() if self._table_exists(connection, "migration_leases") else None
        runtime_checks = [
            {"check_id": "schema_v7", "status": "pass" if schema_version >= 7 else "failed", "evidence_ref": f"schema_migrations:{schema_version}"},
            {"check_id": "migration_fence_released", "status": "pass" if freeze is not None and str(freeze[0]) == "released" else "unknown", "evidence_ref": "migration_freeze:1"},
            {"check_id": "g9_released", "status": "pass" if g9 is not None and str(g9[0]) == "released" else "unknown", "evidence_ref": "migration_leases:G9"},
            {"check_id": "canonical_snapshot_hash", "status": "pass" if source_version else "unknown", "evidence_ref": source_version},
        ]
        if admission is None:
            concept_checks = [{"check_id": "admission_state", "status": "unknown", "evidence_ref": "concept_admissions:none"}]
        else:
            state = str(admission.get("admission_state") or "")
            expires = _parse_timestamp(admission.get("expires_at"))
            continuous = (
                str(admission.get("admission_state") or "") == "incremental"
                and str(admission.get("renewal_policy") or "") == "continuous"
            )
            expired = not continuous and (expires is None or expires <= datetime.now(timezone.utc))
            concept_checks = [
                {"check_id": "admission_state", "status": "pass" if state in {"canary", "incremental"} and not expired else "unknown" if state == "disabled" else "failed", "evidence_ref": f"concept_admissions:{admission.get('version')}"},
                {"check_id": "source_map_terminal", "status": "pass" if self._table_exists(connection, "concept_source_map") and int(connection.execute("SELECT COUNT(*) FROM concept_source_map WHERE status NOT IN ('mapped','quarantined')").fetchone()[0]) == 0 else "unknown", "evidence_ref": "concept_source_map"},
                {"check_id": "model_resolution", "status": "unknown", "evidence_ref": "concept_model_resolutions"},
            ]
        runtime_gate = self._gate_result("runtime_read_model_gate", observed_at, runtime_checks, ["summary", "activity", "work-items", "plans", "reviews", "operations", "roles"])
        concept_gate = self._gate_result("concept_view_gate", observed_at, concept_checks, ["concepts"])
        concept_gate = self._apply_concept_recovery_gate(concept_gate, admission)
        return {"runtime_read_model_gate": runtime_gate, "concept_view_gate": concept_gate}

    @staticmethod
    def _external_file_hash(path: Path) -> Optional[str]:
        """Return a content hash without creating or touching the file."""
        try:
            payload = path.read_bytes()
        except OSError:
            return None
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _read_json_evidence(path: Path, expected_schema: str) -> tuple[Optional[Mapping[str, Any]], Dict[str, Any]]:
        """Read a local evidence file without treating an invalid report as data."""
        record: Dict[str, Any] = {"path": str(path), "source_status": "unavailable", "file_hash": None}
        try:
            payload = path.read_bytes()
            value = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            record["reason"] = f"{type(exc).__name__}:{exc}"
            return None, record
        record["file_hash"] = "sha256:" + hashlib.sha256(payload).hexdigest()
        if not isinstance(value, Mapping) or str(value.get("schema") or "") != expected_schema:
            record["reason"] = "unsupported_schema"
            return None, record
        record["source_status"] = "observed"
        return value, record

    def _concept_source_coverage(self) -> Dict[str, Any]:
        """Project the P3 evidence chain without enabling its write paths."""
        coverage_path = self.runtime_home / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
        candidate_path = self.runtime_home / ".codex" / "pm-loop" / "runs" / "concept-v11" / "p3-source-candidates-current-coverage.json"
        coverage, coverage_record = self._read_json_evidence(coverage_path, "concept-v11.source-coverage-report.v1")
        result: Dict[str, Any] = {
            **coverage_record,
            "status": "unavailable",
            "candidate_discovery": {"path": str(candidate_path), "source_status": "unavailable", "status": "unavailable"},
            "review_package": {"path": None, "source_status": "unavailable", "status": "unavailable"},
        }
        if coverage is None:
            return result

        counts = coverage.get("concept_status_counts") if isinstance(coverage.get("concept_status_counts"), Mapping) else {}
        concepts = coverage.get("concepts") if isinstance(coverage.get("concepts"), list) else []
        no_mapped_concepts = sorted(
            str(item.get("concept") or "")
            for item in concepts
            if isinstance(item, Mapping)
            and str(item.get("coverage_status") or "") == "needs_repair"
            and not int((item.get("disposition_counts") or {}).get("mapped") or 0)
        )
        retired_concepts = sorted(
            str(item.get("concept") or "")
            for item in concepts
            if isinstance(item, Mapping) and str(item.get("coverage_status") or "") == "retired_with_evidence"
        )
        coverage_hash = str(coverage.get("report_hash") or "")
        result.update(
            {
                "status": str(coverage.get("status") or "unknown").lower(),
                "generated_at": coverage.get("generated_at"),
                "report_hash": coverage_hash or None,
                "closure_hash": coverage.get("closure_hash"),
                "source_manifest_hash": coverage.get("source_manifest_hash"),
                "concept_count": coverage.get("concept_count"),
                "reference_count": coverage.get("reference_count"),
                "concept_status_counts": dict(counts),
                "ledger_entry_count": coverage.get("ledger_entry_count"),
                "no_mapped_concepts": no_mapped_concepts,
                "retired_concepts": retired_concepts,
                "p3_closed": bool((coverage.get("gate") or {}).get("p3_closed")),
            }
        )

        candidates, candidate_record = self._read_json_evidence(candidate_path, "concept-v11.source-candidate-discovery.v1")
        candidate_value: Dict[str, Any] = {**candidate_record, "status": "unavailable"}
        if candidates is not None:
            candidate_coverage_hash = str(candidates.get("coverage_report_hash") or "")
            coverage_closed = bool((coverage.get("gate") or {}).get("p3_closed")) and str(coverage.get("status") or "").upper() == "PASS"
            candidate_status = "observed" if coverage_hash and candidate_coverage_hash == coverage_hash else "superseded" if coverage_closed else "stale"
            candidate_value.update(
                {
                    "status": candidate_status,
                    "generated_at": candidates.get("generated_at"),
                    "report_hash": candidates.get("report_hash"),
                    "coverage_report_hash": candidate_coverage_hash or None,
                }
            )
            if candidate_value["status"] == "observed":
                candidate_value.update(
                    {
                        "candidate_count": candidates.get("candidate_count"),
                        "qualified_candidate_count": candidates.get("qualified_candidate_count"),
                        "concepts_needing_repair": candidates.get("concepts_needing_repair"),
                    }
                )
            else:
                candidate_value["reason"] = "p3_closed_with_newer_coverage" if candidate_status == "superseded" else "coverage_report_hash_mismatch"
        result["candidate_discovery"] = candidate_value
        result["source_fingerprint"] = "sha256:" + hashlib.sha256(
            "|".join(str(item.get("file_hash") or "") for item in (coverage_record, candidate_record)).encode("utf-8")
        ).hexdigest()

        package_path = None
        if self.project_root and self.project_root.is_dir():
            packages = sorted(
                (self.project_root / "docs" / "03-产品架构").glob("概念自动刷新-P3来源处置决策工作包-*.json"),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
            package_path = packages[0] if packages else None
        if package_path is None:
            result["review_package"] = {"path": None, "source_status": "unavailable", "status": "unavailable", "reason": "package_not_found"}
            return result

        package, package_record = self._read_json_evidence(package_path, "concept-v11.p3-review-package.v1")
        package_value: Dict[str, Any] = {**package_record, "status": "unavailable"}
        if package is not None:
            inputs = package.get("inputs") if isinstance(package.get("inputs"), Mapping) else {}
            rows = package.get("worksheet_rows") if isinstance(package.get("worksheet_rows"), list) else []
            package_matches = (
                candidate_value.get("status") == "observed"
                and str(inputs.get("coverage_report_hash") or "") == coverage_hash
                and str(inputs.get("candidate_report_hash") or "") == str(candidate_value.get("report_hash") or "")
            )
            package_status = "observed" if package_matches else "superseded" if bool((coverage.get("gate") or {}).get("p3_closed")) and str(coverage.get("status") or "").upper() == "PASS" else "stale"
            package_value.update(
                {
                    "status": package_status,
                    "package_hash": package.get("package_hash"),
                    "coverage_report_hash": inputs.get("coverage_report_hash"),
                    "candidate_report_hash": inputs.get("candidate_report_hash"),
                    "worksheet_row_count": len(rows),
                    "review_decision_count": sum(1 for item in rows if isinstance(item, Mapping) and str(item.get("review_decision") or "").strip()),
                }
            )
            if not package_matches:
                package_value["reason"] = "p3_closed_with_newer_coverage" if package_status == "superseded" else "upstream_evidence_not_current"
        result["review_package"] = package_value
        signatures = [str(item.get("file_hash") or "") for item in (coverage_record, candidate_record, package_record)]
        result["source_fingerprint"] = "sha256:" + hashlib.sha256("|".join(signatures).encode("utf-8")).hexdigest()
        return result

    def _launch_agent_evidence(self) -> List[Dict[str, Any]]:
        """Read PM LaunchAgent configuration as immutable evidence.

        A plist being present is deliberately not treated as proof that
        launchd loaded it.  Runtime load state comes from health/launchd
        evidence; this view only reports the configured file and its hash.
        """
        root = self.runtime_home / "Library" / "LaunchAgents"
        if not root.is_dir():
            return []
        records: List[Dict[str, Any]] = []
        for path in sorted(root.glob("com.zhujie14.*.plist")):
            try:
                document = plistlib.loads(path.read_bytes())
            except (OSError, plistlib.InvalidFileException, ValueError) as exc:
                records.append({"label": path.stem, "path": str(path), "source_status": "unavailable", "error": f"{type(exc).__name__}: {exc}"})
                continue
            label = str(document.get("Label") or path.stem)
            arguments = [str(item) for item in (document.get("ProgramArguments") or [])]
            calendar = document.get("StartCalendarInterval")
            if isinstance(calendar, dict):
                calendar = {str(key): self._stable_value(value) for key, value in calendar.items()}
            elif calendar is not None:
                calendar = self._stable_value(calendar)
            records.append(
                {
                    "label": label,
                    "path": str(path),
                    "config_hash": self._external_file_hash(path),
                    "configured": True,
                    "disabled": bool(document.get("Disabled", False)),
                    "launchd_state": "unknown",
                    "program_arguments": arguments,
                    "calendar": calendar,
                    "start_interval": document.get("StartInterval"),
                    "working_directory": str(document.get("WorkingDirectory") or "") or None,
                    "migration_role": "scheduler" if label == "com.zhujie14.pm-scheduler" else "retained_rollback" if label in {"com.zhujie14.weekly-sync-and-refresh", "com.zhujie14.product-intelligence-monitor", "com.zhujie14.pm-timeline-daily", "com.zhujie14.pm-timeline-weekly", "com.zhujie14.catchup"} else "infrastructure",
                    "source_status": "observed",
                }
            )
        return records

    def _launchd_runtime_evidence(self, label: str, *, expected_db: Optional[Path] = None) -> Optional[Dict[str, Any]]:
        """Read loaded launchd state for one local service without mutation."""
        plist_path = self.runtime_home / "Library" / "LaunchAgents" / f"{label}.plist"
        if not plist_path.is_file():
            return None
        try:
            document = plistlib.loads(plist_path.read_bytes())
        except (OSError, plistlib.InvalidFileException, ValueError):
            return None
        arguments = [str(item) for item in (document.get("ProgramArguments") or [])]
        if expected_db is not None:
            try:
                db_index = arguments.index("--db-path")
            except ValueError:
                return None
            if db_index + 1 >= len(arguments) or Path(arguments[db_index + 1]).expanduser().resolve() != expected_db.expanduser().resolve():
                return None
        try:
            result = subprocess.run(
                ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        output = (result.stdout or "") + (result.stderr or "")
        if result.returncode != 0:
            return None
        state = "running" if "state = running" in output else "loaded" if "state = not running" not in output else "not_running"
        pid = None
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("pid = "):
                try:
                    pid = int(stripped.split("=", 1)[1].strip())
                except ValueError:
                    pass
                break
        return {"label": label, "state": state, "pid": pid, "plist": str(plist_path), "source_status": "observed"}

    def _automation_evidence(self) -> List[Dict[str, Any]]:
        """Read Codex automation manifests without exposing prompt bodies."""
        root = self.runtime_home / ".codex" / "automations"
        if not root.is_dir():
            return []
        records: List[Dict[str, Any]] = []
        for path in sorted(root.glob("*/automation.toml")):
            try:
                document = tomllib.loads(path.read_text(encoding="utf-8"))
            except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
                records.append({"id": path.parent.name, "path": str(path), "source_status": "unavailable", "error": f"{type(exc).__name__}: {exc}"})
                continue
            target = document.get("target") if isinstance(document.get("target"), dict) else {}
            records.append(
                {
                    "id": str(document.get("id") or path.parent.name),
                    "name": str(document.get("name") or path.parent.name),
                    "kind": str(document.get("kind") or "unknown"),
                    "status": str(document.get("status") or "unknown"),
                    "rrule": str(document.get("rrule") or "") or None,
                    "model": str(document.get("model") or "") or None,
                    "reasoning_effort": str(document.get("reasoning_effort") or "") or None,
                    "execution_environment": str(document.get("execution_environment") or "") or None,
                    "project_id": str(target.get("project_id") or target.get("projectId") or "") or None,
                    "cwd": [str(item) for item in (document.get("cwds") or [])] if isinstance(document.get("cwds"), list) else [],
                    "path": str(path),
                    "config_hash": self._external_file_hash(path),
                    "source_status": "observed",
                }
            )
        return records

    @staticmethod
    def _attention_source_type(alert_type: Any, module: Any) -> str:
        value = str(alert_type or "")
        if value.startswith("occurrence") or value == "dead_letter":
            return "schedule" if value.startswith("occurrence") else "job"
        if value in {"job_failed", "run_failed"}:
            return "job"
        if value in {"scheduler_tick_failed", "registry_invalid", "duplicate_scheduler", "database_unavailable"}:
            return "runtime"
        if value in {"heartbeat_stale", "health_check"}:
            return "health"
        if value in {"projection_timeout", "projection_failed"}:
            return "projection"
        return "error_event" if value == "error_event" or module else "runtime"

    @staticmethod
    def _attention_next_action(source_type: str, item: Mapping[str, Any]) -> str:
        if source_type == "schedule":
            return "查看 /api/control-plane/v4/schedules 对应 occurrence，并核对 marker/artifact"
        if source_type == "job":
            return "查看 /api/control-plane/v4/runs 对应 Run 和失败步骤"
        if source_type == "health":
            return "查看系统健康报告并核对对应模块的 heartbeat/freshness"
        if source_type == "runtime":
            return "查看 Scheduler/Worker 日志与最近 scheduler tick"
        return "查看异常详情中的证据引用，必要时复制 Codex 诊断命令"

    def _attention(
        self,
        connection: Any,
        *,
        limit: int,
        read_at: str,
        schedules: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the read-only ``ops_attention_view`` from canonical rows.

        The projector never upserts alerts during GET.  Existing alert rows
        are enriched; rows not yet processed by the background projector are
        represented ephemerally so a failed occurrence cannot disappear merely
        because the projector has not run.
        """
        bounded = max(1, min(int(limit), 1000))
        read_dt = _parse_timestamp(read_at) or datetime.now(timezone.utc)

        def derived_terminal_state(observed_at: Any) -> str:
            # A terminal row is retained forever for audit, but an ephemeral
            # cockpit incident is only actionable while it is fresh.  Older
            # failures are historical unless the background projector has
            # explicitly kept an ``ops_alerts.state=open`` row for them.
            return "open" if _snapshot_freshness(observed_at, now=read_dt) == "fresh" else "resolved"

        schedule_tasks = {str(item.get("schedule_key")): item for item in (schedules or {}).get("tasks", []) if isinstance(item, Mapping)}
        occurrences: Dict[str, Dict[str, Any]] = {}
        if self._table_exists(connection, "schedule_occurrences"):
            for row in connection.execute("SELECT * FROM schedule_occurrences ORDER BY updated_at DESC LIMIT ?", (bounded * 3,)).fetchall():
                occurrences[str(row["occurrence_id"])] = dict(row)
        jobs: Dict[str, Dict[str, Any]] = {}
        for row in connection.execute("SELECT * FROM jobs ORDER BY updated_at DESC LIMIT ?", (bounded * 3,)).fetchall():
            jobs[str(row["job_id"])] = dict(row)
        runs: Dict[str, Dict[str, Any]] = {}
        for row in connection.execute("SELECT * FROM runs ORDER BY updated_at DESC LIMIT ?", (bounded * 3,)).fetchall():
            runs[str(row["run_id"])] = dict(row)
        deliveries: Dict[tuple[str, str], Dict[str, Any]] = {}
        if self._table_exists(connection, "notification_deliveries"):
            for row in connection.execute("SELECT * FROM notification_deliveries ORDER BY requested_at DESC").fetchall():
                deliveries.setdefault((str(row["alert_id"]), str(row["fingerprint"])), dict(row))

        raw: List[Dict[str, Any]] = []
        if self._table_exists(connection, "ops_alerts"):
            # Gather a wider persistent window before adding live derived
            # rows.  Truncating first could hide a new P0/P1 behind older P2
            # alerts, which makes the operator view materially incorrect.
            raw.extend(dict(row) for row in connection.execute("SELECT * FROM ops_alerts ORDER BY last_seen_at DESC,alert_id DESC LIMIT ?", (bounded * 5,)).fetchall())

        def add_derived(kind: str, entity_id: str, *, message: str, module: str, detail: str, occurrence_id: Optional[str] = None, job_id: Optional[str] = None, run_id: Optional[str] = None, observed_at: Optional[str] = None, severity: Optional[str] = None, state: str = "open", registry_hash: Optional[str] = None) -> None:
            fingerprint = "sha256:" + hashlib.sha256(f"{kind}|{entity_id}|{detail}".encode("utf-8")).hexdigest()[:32]
            raw.append({"alert_id": f"alert:{fingerprint}:{observed_at or read_at}", "fingerprint": fingerprint, "severity": severity or ("P1" if kind not in {"health_check", "projection"} else "P2"), "alert_type": kind, "module": module, "message": message, "occurrence_id": occurrence_id, "job_id": job_id, "run_id": run_id, "state": state, "first_seen_at": observed_at or read_at, "last_seen_at": observed_at or read_at, "details_json": json.dumps({"detail": detail, "registry_hash": registry_hash} if registry_hash else {"detail": detail}, ensure_ascii=False)} )

        existing_occurrences = {str(item.get("occurrence_id")) for item in raw if item.get("occurrence_id")}
        existing_jobs = {str(item.get("job_id")) for item in raw if item.get("job_id")}
        if self._table_exists(connection, "scheduler_ticks"):
            for row in connection.execute("SELECT * FROM scheduler_ticks WHERE status='failed' ORDER BY started_at DESC LIMIT ?", (bounded,)).fetchall():
                if not any(str(item.get("alert_type")) == "scheduler_tick_failed" and str(item.get("run_id") or "") == str(row["tick_id"]) for item in raw):
                    add_derived("scheduler_tick_failed", str(row["tick_id"]), message="Scheduler tick 失败", module="Scheduler", detail=str(row["error"] or "tick failed"), observed_at=row["started_at"], severity="P0", state=derived_terminal_state(row["completed_at"] or row["started_at"]))
        for item in occurrences.values():
            if str(item.get("state")) in {"failed", "dead_letter", "expired", "deferred", "suppressed"} and str(item.get("occurrence_id")) not in existing_occurrences:
                state = derived_terminal_state(item.get("updated_at")) if str(item.get("state")) not in {"suppressed"} else "suppressed"
                severity = "P1" if str(item.get("state")) in {"failed", "dead_letter", "expired"} else "P2"
                add_derived("occurrence_" + str(item.get("state")), str(item["occurrence_id"]), message=f"计划 occurrence {item.get('schedule_key')} 进入 {item.get('state')}", module="Scheduler", detail=str(item.get("failure_reason") or item.get("state")), occurrence_id=str(item["occurrence_id"]), job_id=item.get("job_id"), run_id=item.get("run_id"), observed_at=item.get("updated_at"), severity=severity, state=state, registry_hash=item.get("registry_hash"))
        for item in jobs.values():
            if str(item.get("status")) in {"failed", "dead_letter", "interrupted"} and str(item.get("job_id")) not in existing_jobs:
                add_derived("dead_letter" if item.get("status") == "dead_letter" else "job_failed", str(item["job_id"]), message=f"Job {item['job_id']} 进入 {item.get('status')}", module="Worker", detail=str(item.get("terminal_reason") or item.get("error_fingerprint") or item.get("status")), occurrence_id=item.get("occurrence_id"), job_id=str(item["job_id"]), run_id=item.get("run_id"), observed_at=item.get("updated_at"), severity="P1", state=derived_terminal_state(item.get("updated_at")), registry_hash=item.get("registry_hash"))
        for item in runs.values():
            if str(item.get("status")) in {"failed", "dead_letter", "interrupted"} and not any(str(row.get("run_id") or "") == str(item["run_id"]) for row in raw):
                add_derived("run_failed", str(item["run_id"]), message=f"Run {item['run_id']} 进入 {item.get('status')}", module="Worker", occurrence_id=item.get("occurrence_id"), job_id=item.get("job_id"), run_id=str(item["run_id"]), detail=str(item.get("terminal_reason") or item.get("error") or item.get("status")), observed_at=item.get("updated_at"), severity="P1", state=derived_terminal_state(item.get("updated_at")), registry_hash=item.get("registry_hash"))
        for row in connection.execute("SELECT * FROM error_events ORDER BY occurred_at DESC,error_event_id DESC LIMIT ?", (bounded,)).fetchall():
            if not any(str(item.get("alert_type")) == "error_event" and str(item.get("run_id") or "") == str(row["run_id"] or "") and str(item.get("message") or "") == str(row["message"]) for item in raw):
                add_derived("error_event", str(row["fingerprint"] or row["error_event_id"]), message=str(row["message"]), module=str(row["module"]), detail=str(row["details_json"] or "{}"), run_id=row["run_id"], observed_at=row["occurred_at"], severity=str(row["severity"] or "P2").upper())

        # Health snapshots are canonical runtime evidence.  They belong in
        # attention even when an asynchronous alert projector has not yet run.
        # Scheduler/Worker stale or incident states are P1 because they can
        # block accepted work; other degraded/unknown modules remain P2.
        if self._table_exists(connection, "module_health_snapshots"):
            live_modules = {item["module"]: item for item in self.modules(connection=connection)}
            latest_health: Dict[str, Dict[str, Any]] = {}
            for row in connection.execute("SELECT module,status,observed_at,details_json FROM module_health_snapshots ORDER BY observed_at DESC,rowid DESC").fetchall():
                latest_health.setdefault(str(row["module"]), dict(row))
            for module, health in latest_health.items():
                live = live_modules.get(module) or {}
                if module in {"Scheduler", "Worker"} and live.get("status") == "healthy" and live.get("freshness") == "fresh":
                    # A fresh tick/lease/launchd signal supersedes a stale
                    # freeze-era health snapshot for current attention.
                    continue
                observed_at = health.get("observed_at")
                raw_status = str(health.get("status") or "unknown")
                freshness = _snapshot_freshness(observed_at, now=_parse_timestamp(read_at) or datetime.now(timezone.utc))
                attention_needed = raw_status in {"incident", "degraded", "failed", "unknown"} or freshness == "stale"
                if not attention_needed:
                    continue
                severity = "P1" if module in {"Scheduler", "Worker"} and (raw_status in {"incident", "failed", "unknown"} or freshness == "stale") else "P2"
                detail = f"status={raw_status};freshness={freshness}"
                if not any(str(item.get("alert_type")) == "health_check" and str(item.get("module")) == module and detail in str(item.get("details_json") or "") for item in raw):
                    add_derived("health_check", module, message=f"模块 {module} 健康状态 {raw_status}/{freshness}", module=module, detail=detail, observed_at=observed_at, severity=severity)

        items: List[Dict[str, Any]] = []
        raw.sort(key=lambda item: (str(item.get("last_seen_at") or item.get("occurred_at") or ""), str(item.get("alert_id") or "")), reverse=True)
        for row in raw[:bounded]:
            details = self._json_field(row.get("details_json"), {})
            occurrence = occurrences.get(str(row.get("occurrence_id") or ""))
            job = jobs.get(str(row.get("job_id") or ""))
            run = runs.get(str(row.get("run_id") or ""))
            occurrence = occurrence or (occurrences.get(str(job.get("occurrence_id"))) if job else None)
            job = job or (jobs.get(str(run.get("job_id"))) if run else None)
            schedule_key = str((occurrence or {}).get("schedule_key") or (job or {}).get("schedule_key") or (run or {}).get("schedule_key") or details.get("schedule_key") or "") or None
            task = schedule_tasks.get(schedule_key or "")
            source_type = self._attention_source_type(row.get("alert_type"), row.get("module"))
            evidence_refs: List[str] = []
            for prefix, value in (("occurrence", row.get("occurrence_id") or (occurrence or {}).get("occurrence_id")), ("job", row.get("job_id") or (job or {}).get("job_id")), ("run_events", row.get("run_id") or (run or {}).get("run_id"))):
                if value:
                    evidence_refs.append(f"{prefix}:{value}")
            if task:
                for key in ("marker", "log_dir"):
                    if task.get("evidence", {}).get(key):
                        evidence_refs.append(f"{key}:{task['evidence'][key]}")
            artifact_uri = details.get("artifact") or details.get("artifact_uri")
            if artifact_uri:
                evidence_refs.append(f"artifact:{artifact_uri}")
            fingerprint = str(row.get("fingerprint") or "")
            alert_id = str(row.get("alert_id") or "")
            delivery = deliveries.get((alert_id, fingerprint))
            delivery_state = str((delivery or {}).get("state") or "")
            notification_state = "sent" if delivery_state == "sent" else "pending" if delivery_state in {"pending", "failed"} else "not_applicable" if str(row.get("severity") or "P2").upper() not in {"P0", "P1"} else "pending"
            state = str(row.get("state") or "open")
            # Persistent terminal alerts can outlive the source failure by
            # days. Use the canonical occurrence/job/run timestamp when
            # available, rather than the alert's refreshed last_seen_at, so
            # historical failures remain visible without inflating current
            # P0/P1 counts. Alerts without a linked source keep their explicit
            # state and continue to support manual operator triage.
            terminal_alert_types = {"occurrence_failed", "occurrence_expired", "dead_letter", "job_failed", "run_failed"}
            alert_type = str(row.get("alert_type") or "")
            # Scheduler reconciliation refreshes an occurrence's updated_at
            # on every tick, even when its terminal state is unchanged. Prefer
            # immutable completion timestamps from the linked Run/Job; use
            # the occurrence deadline/scheduled time only for terminal windows
            # without an execution record.
            source_observed_at = (
                (run or {}).get("completed_at")
                or (job or {}).get("completed_at")
                or (occurrence or {}).get("deadline_at")
                or (occurrence or {}).get("scheduled_at")
                or (run or {}).get("updated_at")
                or (job or {}).get("updated_at")
                or (occurrence or {}).get("updated_at")
            )
            if state == "open" and alert_type in terminal_alert_types and source_observed_at:
                if _snapshot_freshness(source_observed_at, now=_parse_timestamp(read_at) or datetime.now(timezone.utc)) == "stale":
                    state = "resolved"
            item = {
                "alert_id": alert_id or None,
                "fingerprint": fingerprint or None,
                "severity": str(row.get("severity") or "P2").upper(),
                "state": state,
                "source_type": source_type,
                "schedule_key": schedule_key,
                "occurrence_id": row.get("occurrence_id") or (occurrence or {}).get("occurrence_id"),
                "job_id": row.get("job_id") or (job or {}).get("job_id"),
                "run_id": row.get("run_id") or (run or {}).get("run_id"),
                "first_seen_at": row.get("first_seen_at") or row.get("occurred_at") or read_at,
                "last_seen_at": row.get("last_seen_at") or row.get("occurred_at") or read_at,
                "impact": f"影响模块：{row.get('module') or 'unknown'}",
                "reason": str(row.get("message") or details.get("detail") or details.get("reason") or "未记录原因"),
                "evidence_refs": evidence_refs,
                "next_action": self._attention_next_action(source_type, row),
                "notify_policy": "desktop_once" if str(row.get("severity") or "P2").upper() in {"P0", "P1"} else "dashboard_only",
                "notification_state": notification_state,
                "notification_error": (delivery or {}).get("error"),
                "source_status": "observed",
                "freshness": _snapshot_freshness(row.get("last_seen_at") or row.get("occurred_at"), now=_parse_timestamp(read_at) or datetime.now(timezone.utc)),
                "registry_hash": row.get("registry_hash") or (occurrence or {}).get("registry_hash") or (job or {}).get("registry_hash") or (run or {}).get("registry_hash") or details.get("registry_hash"),
                "module": row.get("module"),
                "alert_type": row.get("alert_type"),
            }
            items.append(item)
        items.sort(key=lambda item: str(item.get("last_seen_at") or ""), reverse=True)
        open_items = [item for item in items if item.get("state") == "open"]
        return {"items": items, "count": len(items), "open_count": len(open_items), "p0_p1_open": sum(str(item.get("severity")) in {"P0", "P1"} for item in open_items), "source_status": "observed", "status": "observed", "read_only": True}

    def _activity(self, connection: Any, limit: int) -> List[Dict[str, Any]]:
        values: List[Dict[str, Any]] = []
        if self._table_exists(connection, "activity_events"):
            rows = connection.execute(
                "SELECT activity_id AS event_id,activity_id AS id,event_type,actor,correlation_id,"
                "entity_type,entity_id,run_id,job_id,occurrence_id,payload_json,source_cursor,occurred_at "
                "FROM activity_events ORDER BY occurred_at DESC,activity_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            for row in rows:
                item = dict(row)
                item["payload"] = self._json_field(item.pop("payload_json"), {})
                item["source_status"] = "observed"
                values.append(item)
            # A migrated database may have been created before the activity
            # mirror was enabled.  Only fall back when the canonical ledger is
            # genuinely empty; never merge two sources and double-count rows.
            if values:
                return values
        for row in connection.execute("SELECT event_id AS id,event_id,event_type,actor,run_id,payload_json,occurred_at FROM run_events ORDER BY occurred_at DESC,event_id DESC LIMIT ?", (limit,)).fetchall():
            item = dict(row)
            item["payload"] = self._json_field(item.pop("payload_json"), {})
            item["source_status"] = "observed"
            values.append(item)
        return values

    def _work_items(self, connection: Any, limit: int) -> List[Dict[str, Any]]:
        values = []
        for row in connection.execute("SELECT job_id,run_id,job_type,status,priority,profile,namespace_epoch,queued_at,started_at,completed_at,updated_at,error_fingerprint,terminal_reason FROM jobs ORDER BY updated_at DESC,job_id DESC LIMIT ?", (limit,)).fetchall():
            item = dict(row)
            schedule_row = connection.execute("SELECT schedule_key,trigger_kind,occurrence_id FROM jobs WHERE job_id=?", (item["job_id"],)).fetchone()
            schedule_key = str(schedule_row["schedule_key"] or "") if schedule_row is not None else ""
            trigger_kind = str(schedule_row["trigger_kind"] or "runtime") if schedule_row is not None else "runtime"
            item.update({
                "work_item_id": item["job_id"],
                "plan_id": f"schedule:{schedule_key}" if schedule_key else None,
                "schedule_key": schedule_key or None,
                "occurrence_id": schedule_row["occurrence_id"] if schedule_row is not None else None,
                "template_id": item.pop("job_type"),
                "trigger": trigger_kind,
                "dependencies": [],
                "canonical_status": canonical_status(item.get("status"), failure_class=item.get("terminal_reason")),
                "display_status": self._display_status(item.get("status")),
                "gate_state": "observed" if schedule_key else "not_applicable",
                "next_action": "查看 Run 详情" if item["status"] in {"failed", "dead_letter", "interrupted"} else "无",
            })
            item["source_status"] = "observed"
            values.append(item)
        return values

    @staticmethod
    def _display_status(value: Any) -> str:
        status = canonical_status(value)
        if status in {"queued", "accepted", "retry_wait"}:
            return "inbox"
        if status in {"assigned"}:
            return "assigned"
        if status in {"running", "processing"}:
            return "in_progress"
        if status == "completed":
            return "done"
        if status in {"failed", "dead_letter", "quarantine", "interrupted"}:
            return "failed"
        return "unknown"

    def _plans(self, connection: Any, schedules: Mapping[str, Any]) -> Dict[str, Any]:
        """Expose actual registry entries as the canonical PM plan view.

        A schedule registry is already the system's plan of record.  This
        projection adds no planning state; it merely joins each entry with
        its latest occurrence/Job/Run evidence from the same snapshot.
        """
        if self._table_exists(connection, "plans"):
            plan_rows = connection.execute(
                "SELECT * FROM plans ORDER BY updated_at DESC,plan_id DESC LIMIT ?",
                (max(1, min(int(len(schedules.get("tasks", [])) or 100), 500)),),
            ).fetchall()
            if plan_rows:
                items: List[Dict[str, Any]] = []
                for row in plan_rows:
                    plan = dict(row)
                    plan["dependencies"] = self._json_field(plan.pop("dependencies_json"), [])
                    plan["watermarks"] = self._json_field(plan.pop("watermarks_json"), {})
                    plan["feature_gate"] = plan.get("feature_gate") or "runtime_read_model_gate"
                    plan["canonical_status"] = plan.get("status") or "unknown"
                    plan["display_status"] = self._display_status(plan["canonical_status"])
                    plan["source_status"] = "observed"
                    plan["read_only"] = True
                    item_rows = connection.execute(
                        "SELECT * FROM plan_items WHERE plan_id=? ORDER BY sequence,item_key",
                        (plan["plan_id"],),
                    ).fetchall() if self._table_exists(connection, "plan_items") else []
                    plan["items"] = []
                    for item_row in item_rows:
                        item = dict(item_row)
                        item["dependencies"] = self._json_field(item.pop("dependencies_json"), [])
                        plan["items"].append(item)
                    items.append(plan)
                return {"items": items, "source_status": "observed", "status": "observed", "read_only": True, "metric_source": "plans+plan_items"}

        states = {
            str(item.get("schedule_key")): item
            for item in schedules.get("task_states", [])
            if isinstance(item, Mapping) and item.get("schedule_key")
        }
        items: List[Dict[str, Any]] = []
        for task in schedules.get("tasks", []):
            if not isinstance(task, Mapping):
                continue
            key = str(task.get("schedule_key") or "")
            if not key:
                continue
            state = states.get(key, {})
            observed_status = str(state.get("status") or state.get("occurrence_state") or "never_run")
            items.append(
                {
                    "plan_id": f"schedule:{key}",
                    "plan_type": "schedule_registry",
                    "schedule_key": key,
                    "title": key,
                    "calendar": task.get("calendar"),
                    "timezone": task.get("timezone"),
                    "handler": task.get("handler"),
                    "deadline": task.get("deadline"),
                    "concurrency_key": task.get("concurrency_key"),
                    "retry": task.get("retry"),
                    "dependencies": [],
                    "gate_state": "observed" if schedules.get("status") == "observed" else "unknown",
                    "canonical_status": observed_status,
                    "display_status": self._display_status(observed_status),
                    "occurrence_id": state.get("occurrence_id"),
                    "job_id": state.get("job_id"),
                    "run_id": state.get("run_id"),
                    "freshness": state.get("freshness", schedules.get("freshness", "unknown")),
                    "source_status": "observed" if schedules.get("status") == "observed" else "unknown",
                }
            )
        source_status = "observed" if schedules.get("status") == "observed" else str(schedules.get("source_status") or "unknown")
        return {"items": items, "source_status": source_status, "status": source_status, "read_only": True, "metric_source": "schedule_registry_state+schedule_occurrences"}

    @staticmethod
    def _review_failure_text(value: Any) -> Optional[str]:
        """Keep human-readable failure evidence separate from fingerprints."""
        if value is None:
            return None
        text = str(value).strip()
        return text if text and not _looks_like_error_fingerprint(text) else None

    def _read_review_package(self, item: Mapping[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[Path]]:
        """Read one bounded task package referenced by a review, if present.

        Review evidence is durable but may predate the task-package contract.
        Only a package inside the local PM Loop run archive is trusted for
        controlled-test classification; absent or malformed evidence stays
        unclassified rather than suppressing an operational failure.
        """
        archive_root = (self.runtime_home / ".codex" / "pm-loop" / "runs").resolve()
        refs = list(item.get("artifact_uris") or []) + list(item.get("evidence_refs") or [])
        for reference in refs:
            raw = str(reference or "")
            if raw.startswith("artifact:"):
                raw = raw[len("artifact:"):]
            raw = raw.split("#sha256:", 1)[0]
            path = Path(raw).expanduser()
            if path.name != "task-package.v1.json":
                continue
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(archive_root)
                if resolved.stat().st_size > 1024 * 1024:
                    continue
                value = json.loads(resolved.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(value, dict) and value.get("schema_version") == "pm-task-package.v1":
                return value, resolved
        return None, None

    def _review_diagnosis(self, connection: Any, item: Mapping[str, Any]) -> Dict[str, Any]:
        """Classify a review without changing its underlying terminal audit row."""
        run_id = str(item.get("run_id") or "")
        run_row = connection.execute(
            "SELECT status,error,terminal_reason FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone() if run_id else None
        run = dict(run_row) if run_row is not None else {}
        canonical = canonical_status(item.get("canonical_status") or run.get("status"), failure_class=run.get("terminal_reason"))
        failed = canonical in {"failed", "dead_letter", "quarantine", "interrupted"} or str(item.get("review_state")) == "failed"
        if not failed:
            return {
                "classification": "not_failed",
                "classification_label": "非失败记录",
                "tone": "blue",
                "summary": "终态结果待按证据核验",
                "facts": "该记录没有失败终态；评审中心仅展示已有 Run、Checkpoint 和 Artifact 证据。",
                "repair": "不适用。评审中心不批准、不发布、不执行或重试任务。",
                "needs_repair": "not_applicable",
                "evidence_status": "observed",
                "codex_advice": {"actionable": False, "title": "", "prompt": ""},
            }

        events = [
            dict(row) for row in connection.execute(
                "SELECT event_type,actor,payload_json,occurred_at FROM run_events WHERE run_id=? ORDER BY seq",
                (run_id,),
            ).fetchall()
        ] if run_id else []
        for event in events:
            event["payload"] = self._json_field(event.pop("payload_json"), {})
        accepted = next((event for event in events if event.get("event_type") == "run/accepted"), None)
        package, package_path = self._read_review_package(item)
        request: Optional[Dict[str, Any]] = None
        if package_path is not None:
            try:
                candidate = json.loads((package_path.parent / "request.json").read_text(encoding="utf-8"))
                if isinstance(candidate, dict):
                    request = candidate
            except (OSError, ValueError, json.JSONDecodeError):
                request = None
        execution = package.get("execution") if isinstance(package, Mapping) else {}
        outcome = package.get("outcome") if isinstance(package, Mapping) else {}
        summary = package.get("business_summary") if isinstance(package, Mapping) else {}
        fixture = request.get("replay_fixture") if isinstance(request, Mapping) else {}
        dependency_event = summary.get("dependency_event") if isinstance(summary, Mapping) else {}
        external_calls = fixture.get("external_calls") if isinstance(fixture, Mapping) else {}
        controlled = bool(
            accepted
            and accepted.get("actor") == "pm-p9-dependency-replay"
            and isinstance(package, Mapping)
            and isinstance(request, Mapping)
            and isinstance(execution, Mapping)
            and isinstance(outcome, Mapping)
            and isinstance(fixture, Mapping)
            and isinstance(external_calls, Mapping)
            and isinstance(dependency_event, Mapping)
            and execution.get("run_id") == run_id
            and execution.get("trigger_kind") == "manual_replay"
            and request.get("run_id") == run_id
            and fixture.get("stage") == "P9.2"
            and fixture.get("fixture") == "fixed_local_upstream_completion"
            and external_calls.get("oneapi") == 0
            and external_calls.get("openviking") == 0
            and outcome.get("impact") == "handler_exit_7"
            and dependency_event.get("status") == "blocked_by_upstream"
            and any(
                event.get("event_type") == "scheduled_dependency_event/appended"
                and isinstance(event.get("payload"), Mapping)
                and event["payload"].get("status") == "blocked_by_upstream"
                for event in events
            )
        )
        if controlled:
            return {
                "classification": "controlled_negative_test",
                "classification_label": "受控负向测试",
                "tone": "blue",
                "summary": "受控 fixture 注入 handler exit 7，已按预期阻断下游依赖",
                "facts": "P9.2 本地回放记录了固定 fixture、零 OneAPI/OpenViking 调用和 blocked_by_upstream 依赖事件；原始 dead_letter 终态保留用于审计。",
                "repair": "无需修复生产任务。该记录验证的是上游失败时下游会被正确阻断。",
                "needs_repair": "no",
                "evidence_status": "observed",
                "evidence_refs": [f"run_events:{run_id}:run/accepted", str(package_path), str(package_path.parent / "request.json")],
                "codex_advice": {
                    "actionable": False,
                    "title": "受控负向测试核验建议",
                    "prompt": "请 Codex 只读核对 P9.2 受控负向测试的 task package、request fixture、handler evidence 和 dependency event；确认整体测试汇总为 PASS。不要重跑真实周同步，不要修复或修改生产任务。",
                },
            }

        evidence_values: List[Any] = [run.get("error"), run.get("terminal_reason"), item.get("conclusion"), item.get("reason")]
        for event in reversed(events):
            payload = event.get("payload")
            if isinstance(payload, Mapping):
                evidence_values.extend(payload.get(key) for key in ("error", "reason", "detail", "message", "failure_reason", "exception", "stderr"))
        readable = next((text for value in evidence_values if (text := self._review_failure_text(value))), None)
        exit_match = re.fullmatch(r"handler_exit_(\d+)", str(readable or ""))
        if exit_match:
            reason_summary = f"处理器异常退出（exit {exit_match.group(1)}），当前没有更具体的错误正文"
        elif readable:
            reason_summary = readable
        else:
            reason_summary = "已记录失败终态，但当前没有可读的错误原因"
        has_specific_reason = bool(readable and not exit_match)
        classification = "business_failure" if has_specific_reason else "unclassified_failure"
        label = "真实业务失败" if classification == "business_failure" else "失败待确认"
        facts = (
            f"Run 当前终态为 {canonical}；可见失败证据：{reason_summary}。"
            if readable else
            f"Run 当前终态为 {canonical}；只有终态/错误指纹，没有可读错误正文。"
        )
        prompt = (
            f"请 Codex 只读诊断 PM Loop Run {run_id}。先读取 Run 事件、task package、handler evidence/output 与同时间段 Scheduler/Worker 日志。"
            f"当前终态：{canonical}；当前可见原因：{reason_summary}。确认根因后提出最小修复、验证和受控重跑方案；不要直接重跑、发布或修改业务数据。"
        )
        return {
            "classification": classification,
            "classification_label": label,
            "tone": "red" if classification == "business_failure" else "orange",
            "summary": reason_summary,
            "facts": facts,
            "repair": "需要先由 Codex 读取现有证据确认根因，再决定最小修复与受控重跑；页面不会执行任何操作。",
            "needs_repair": "yes" if classification == "business_failure" else "confirm",
            "evidence_status": "observed" if readable else "insufficient",
            "codex_advice": {"actionable": True, "title": "Codex 诊断建议", "prompt": prompt},
        }

    def _reviews(self, connection: Any, limit: int) -> Dict[str, Any]:
        """Project real terminal Runs into verification packages.

        This intentionally does not manufacture an approval decision.  A
        completed Run with a checkpoint is ``result_ready``; a terminal Run
        without evidence is ``verification_pending``; failures remain failed.
        """
        if self._table_exists(connection, "reviews"):
            canonical_rows = connection.execute(
                "SELECT * FROM reviews ORDER BY updated_at DESC,review_id DESC LIMIT ?",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            if canonical_rows:
                items: List[Dict[str, Any]] = []
                for row in canonical_rows:
                    item = dict(row)
                    evidence_rows = connection.execute(
                        "SELECT evidence_id,evidence_ref,evidence_role,source_hash,status,observed_at FROM review_evidence WHERE review_id=? ORDER BY observed_at,evidence_id",
                        (item["review_id"],),
                    ).fetchall() if self._table_exists(connection, "review_evidence") else []
                    item["evidence"] = [dict(evidence) for evidence in evidence_rows]
                    item["evidence_refs"] = [str(evidence["evidence_ref"]) for evidence in evidence_rows]
                    item["artifact_uris"] = [str(item["artifact_id"])] if item.get("artifact_id") else []
                    item["observed_at"] = item.get("updated_at")
                    item["source_status"] = "observed"
                    item["read_only"] = True
                    diagnosis = self._review_diagnosis(connection, item)
                    item["review_classification"] = diagnosis["classification"]
                    item["review_diagnosis"] = diagnosis
                    item["codex_advice"] = diagnosis["codex_advice"]
                    package, _package_path = self._read_review_package(item)
                    item["artifact_open_url"] = (
                        f"/artifacts/reviews/{item['run_id']}"
                        if package is not None and str(item.get("run_id") or "")
                        else None
                    )
                    items.append(item)
                return {"items": items, "source_status": "observed", "status": "observed", "read_only": True, "metric_source": "reviews+review_evidence"}
        rows = connection.execute(
            "SELECT run_id,job_id,loop_id,status,updated_at,completed_at,error,terminal_reason,snapshot_id FROM runs WHERE status IN ('completed','failed','dead_letter','quarantine','interrupted') ORDER BY updated_at DESC,run_id DESC LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            run = dict(row)
            checkpoints = connection.execute("SELECT checkpoint_key,artifact_uri,updated_at FROM checkpoints WHERE run_id=? ORDER BY updated_at DESC", (run["run_id"],)).fetchall()
            evidence_refs = [f"checkpoint:{item['checkpoint_key']}" for item in checkpoints]
            artifacts = [str(item["artifact_uri"]) for item in checkpoints if item["artifact_uri"]]
            canonical = canonical_status(run.get("status"), failure_class=run.get("terminal_reason"))
            if canonical in {"failed", "dead_letter", "quarantine", "interrupted"}:
                review_state = "failed"
            elif artifacts:
                review_state = "result_ready"
            else:
                review_state = "verification_pending"
            item = {
                "review_id": f"review:{run['run_id']}",
                "run_id": run["run_id"],
                "job_id": run.get("job_id"),
                "loop_id": run.get("loop_id"),
                "canonical_status": canonical,
                "display_status": self._display_status(canonical),
                "review_state": review_state,
                "publish_state": "not_applicable",
                "artifact_uris": artifacts,
                "evidence_refs": evidence_refs,
                "snapshot_id": run.get("snapshot_id"),
                "completed_at": run.get("completed_at"),
                "observed_at": run.get("updated_at"),
                "freshness": _snapshot_freshness(run.get("updated_at")),
                "reason": run.get("error") or run.get("terminal_reason"),
                "source_status": "observed",
            }
            diagnosis = self._review_diagnosis(connection, item)
            item["review_classification"] = diagnosis["classification"]
            item["review_diagnosis"] = diagnosis
            item["codex_advice"] = diagnosis["codex_advice"]
            package, _package_path = self._read_review_package(item)
            item["artifact_open_url"] = (
                f"/artifacts/reviews/{item['run_id']}"
                if package is not None and str(item.get("run_id") or "")
                else None
            )
            items.append(item)
        return {"items": items, "source_status": "observed", "status": "observed", "read_only": True, "metric_source": "runs+checkpoints"}

    def _knowledge_sources(self) -> Dict[str, Any]:
        """Project configured PM knowledge inputs from their existing ledgers.

        These files are evidence produced by their owning sync jobs.  The
        cockpit deliberately does not crawl, probe OpenViking, or infer a
        successful sync from a configured path.
        """
        codex = self.runtime_home / ".codex"
        specs = (
            {
                "source_id": "shengsuan-internal",
                "name": "胜算内部产品资料",
                "kind": "Ku 知识库",
                "scope": "产品管理、Ontology、Pipeline/FDE、DataSearch、DataAgent、DataBuilder 内部资料和功能清单",
                "storage": "viking://resources/shengsuan/",
                "schedule_key": "weekly-sync-and-refresh",
                "path": codex / "skills" / "shengsuan-sync" / "state" / "ledger.json",
                "members_key": "source",
            },
            {
                "source_id": "databuilder-public-docs",
                "name": "DataBuilder 公开文档",
                "kind": "官网公开文档",
                "scope": "DataBuilder 产品介绍、指南、API、FAQ、SLA 和发布记录",
                "storage": "viking://resources/shengsuan/public-docs/",
                "schedule_key": "weekly-sync-and-refresh",
                "path": codex / "skills" / "databuilder-public-docs" / "state" / "ledger.json",
                "members_key": None,
            },
            {
                "source_id": "product-intelligence",
                "name": "产品情报双源",
                "kind": "竞品公告与交付目录",
                "scope": "Palantir Foundry 官方公告、胜算 FDE 团队项目目录及可见后代",
                "storage": "viking://resources/competitive/",
                "schedule_key": "product-intelligence-monitor",
                "path": codex / "skills" / "product-intelligence-monitor" / "state" / "weekly-baseline.json",
                "members_key": "sources",
            },
            {
                "source_id": "competitive-radar",
                "name": "竞品雷达公开信号",
                "kind": "公开信号源",
                "scope": "Palantir News、GitHub Agent、Product Hunt AI、Hacker News AI、Anthropic News",
                "storage": "PM Loop competitive-radar signal ledger",
                "schedule_key": "competitive-radar-ingest",
                "path": codex / "pm-loop" / "state" / "competitive-radar" / "source-watermarks.json",
                "members_key": "top_level",
            },
        )
        items: List[Dict[str, Any]] = []
        fingerprints: List[str] = []
        for spec in specs:
            path = Path(spec["path"])
            item = {key: value for key, value in spec.items() if key not in {"path", "members_key"}}
            item.update({"evidence_path": str(path), "record_count": None, "source_members": [], "observed_at": None, "status": "unknown", "freshness": "stale"})
            if not path.is_file():
                fingerprints.append(f"{path}:missing")
                items.append(item)
                continue
            try:
                observed_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                document = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(document, Mapping):
                    raise ValueError("ledger root must be an object")
                members_key = spec["members_key"]
                if members_key == "sources":
                    members = document.get("sources") if isinstance(document.get("sources"), Mapping) else {}
                    item["source_members"] = [
                        {"name": str(name), "record_count": len(value.get("items", {})) if isinstance(value, Mapping) and isinstance(value.get("items"), Mapping) else None}
                        for name, value in sorted(members.items())
                    ]
                    item["record_count"] = sum(member["record_count"] or 0 for member in item["source_members"])
                elif members_key == "top_level":
                    item["source_members"] = [{"name": str(name), "record_count": 1} for name in sorted(document)]
                    item["record_count"] = len(document)
                else:
                    records = list(document.values())
                    item["record_count"] = len(records)
                    if members_key:
                        grouped: Dict[str, int] = {}
                        for record in records:
                            if isinstance(record, Mapping) and record.get(members_key):
                                name = str(record[members_key])
                                grouped[name] = grouped.get(name, 0) + 1
                        item["source_members"] = [{"name": name, "record_count": grouped[name]} for name in sorted(grouped)]
                item.update({"observed_at": observed_at, "status": "observed", "freshness": _snapshot_freshness(observed_at)})
                fingerprints.append(f"{path}:{self._external_file_hash(path) or 'unavailable'}")
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                item["error"] = f"{type(exc).__name__}: {exc}"
                fingerprints.append(f"{path}:unreadable")
            items.append(item)
        return {
            "items": items,
            "source_status": "observed" if any(item["status"] == "observed" for item in items) else "unknown",
            "read_only": True,
            "source_fingerprint": "sha256:" + hashlib.sha256("\n".join(fingerprints).encode("utf-8")).hexdigest(),
        }

    @staticmethod
    def _schedule_timing(task: ScheduleTask, *, now: datetime, timezone_name: str) -> Dict[str, Any]:
        """Return the next configured window without creating an occurrence."""
        if task.is_calendar:
            latest = latest_scheduled_at(task, now, timezone_name=timezone_name)
            next_run = next_scheduled_at(task, latest, timezone_name=timezone_name)
            return {
                "trigger_kind": "calendar",
                "next_run_at": next_run.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "next_run_state": "scheduled",
                "next_run_reason": None,
            }
        upstream = str(task.trigger.get("upstream_schedule_key") or "")
        required_artifact = str(task.trigger.get("required_artifact") or "")
        return {
            "trigger_kind": "dependency",
            "next_run_at": None,
            "next_run_state": "waiting_dependency",
            "next_run_reason": f"等待上游 {upstream} 成功并产出 {required_artifact}" if upstream and required_artifact else "等待上游成功事件",
        }

    def _schedules(self, connection: Any, limit: int) -> Dict[str, Any]:
        """Read registry, launchd, occurrence, Run and artifact evidence.

        The filesystem portions are configuration evidence only.  A plist on
        disk is never reported as loaded unless a separate health producer has
        supplied that fact; this prevents a retained rollback plist from being
        mistaken for a second scheduler.
        """
        launch_agents = self._launch_agent_evidence()
        automations = self._automation_evidence()
        external_hashes = [str(item.get("config_hash")) for item in launch_agents + automations if item.get("config_hash")]
        external_fingerprint = "sha256:" + hashlib.sha256("\n".join(sorted(external_hashes)).encode("utf-8")).hexdigest() if external_hashes else None
        knowledge_sources = self._knowledge_sources()
        base: Dict[str, Any] = {
            "registry": None,
            "config_version": None,
            "tasks": [],
            "task_states": [],
            "occurrences": [],
            "jobs": [],
            "runs": [],
            "launch_agents": launch_agents,
            "automations": automations,
            "knowledge_sources": knowledge_sources,
            "source_fingerprint": external_fingerprint,
            "freshness": "stale",
            "source_status": "not_implemented",
            "status": "not_implemented",
            "read_only": True,
        }
        if not self._table_exists(connection, "schedule_registry_state"):
            base["reason"] = "调度事实表尚未接入"
            return base
        registry_row = connection.execute("SELECT * FROM schedule_registry_state WHERE registry_id=1").fetchone()
        if registry_row is None:
            base["reason"] = "统一 registry 尚未加载；不从静态配置虚构运行状态"
            return base
        try:
            canonical = json.loads(registry_row["canonical_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            canonical = {}
        bounded = max(1, min(int(limit), 100))
        occurrences = [dict(row) for row in connection.execute("SELECT * FROM schedule_occurrences ORDER BY scheduled_at DESC,occurrence_id DESC LIMIT ?", (bounded,)).fetchall()] if self._table_exists(connection, "schedule_occurrences") else []
        jobs = [dict(row) for row in connection.execute("SELECT * FROM jobs WHERE schedule_key IS NOT NULL ORDER BY updated_at DESC,job_id DESC LIMIT ?", (bounded,)).fetchall()]
        runs = [dict(row) for row in connection.execute("SELECT * FROM runs WHERE schedule_key IS NOT NULL ORDER BY updated_at DESC,run_id DESC LIMIT ?", (bounded,)).fetchall()]
        task_rows = canonical.get("tasks", []) if isinstance(canonical, dict) and isinstance(canonical.get("tasks", []), list) else []
        tasks = [dict(item, timezone=canonical.get("timezone")) if isinstance(item, Mapping) else item for item in task_rows]
        try:
            validated_registry = validate_document(canonical, source_path=Path(str(registry_row["source_path"] or "schedule-registry.json")))
        except RegistryError:
            validated_registry = None
        timing_by_key = {
            task.schedule_key: self._schedule_timing(task, now=datetime.now(timezone.utc), timezone_name=validated_registry.timezone_name)
            for task in (validated_registry.tasks if validated_registry else ())
        }
        run_ids = [str(item["run_id"]) for item in runs if item.get("run_id")]
        events_by_run: Dict[str, List[Dict[str, Any]]] = {}
        checkpoints_by_run: Dict[str, List[Dict[str, Any]]] = {}
        if run_ids:
            event_rows = connection.execute("SELECT * FROM run_events ORDER BY occurred_at DESC,event_id DESC").fetchall()
            for row in event_rows:
                if str(row["run_id"]) in run_ids:
                    events_by_run.setdefault(str(row["run_id"]), []).append(dict(row))
            checkpoint_rows = connection.execute("SELECT * FROM checkpoints ORDER BY updated_at DESC").fetchall()
            for row in checkpoint_rows:
                if str(row["run_id"]) in run_ids:
                    checkpoints_by_run.setdefault(str(row["run_id"]), []).append(dict(row))
        job_by_id = {str(item.get("job_id")): item for item in jobs if item.get("job_id")}
        run_by_id = {str(item.get("run_id")): item for item in runs if item.get("run_id")}
        occurrence_by_schedule: Dict[str, List[Dict[str, Any]]] = {}
        for item in occurrences:
            occurrence_by_schedule.setdefault(str(item.get("schedule_key")), []).append(item)

        def artifact_info(uri: Any) -> Optional[Dict[str, Any]]:
            if not uri:
                return None
            raw_uri = str(uri)
            path = Path(raw_uri).expanduser()
            info: Dict[str, Any] = {"uri": raw_uri, "path": str(path), "exists": path.is_file(), "source_status": "observed" if path.is_file() else "unavailable"}
            if path.is_file():
                try:
                    info["size_bytes"] = path.stat().st_size
                    info["updated_at"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                    info["sha256"] = self._external_file_hash(path)
                except OSError as exc:
                    info["source_status"] = "unavailable"
                    info["error"] = f"{type(exc).__name__}: {exc}"
            return info

        def task_state(task: Mapping[str, Any]) -> Dict[str, Any]:
            key = str(task.get("schedule_key") or "")
            task_occurrences = occurrence_by_schedule.get(key, [])
            occurrence = task_occurrences[0] if task_occurrences else None
            job = job_by_id.get(str(occurrence.get("job_id"))) if occurrence and occurrence.get("job_id") else None
            run = run_by_id.get(str((job or {}).get("run_id") or (occurrence or {}).get("run_id")))
            events = events_by_run.get(str(run.get("run_id")), []) if run else []
            checkpoints = checkpoints_by_run.get(str(run.get("run_id")), []) if run else []
            returncode = None
            failed_step = None
            artifact_uri = None
            last_event = None
            for event in events:
                payload = self._json_field(event.get("payload_json"), {})
                if last_event is None:
                    last_event = {"event_type": event.get("event_type"), "occurred_at": event.get("occurred_at"), "payload": payload}
                if returncode is None and isinstance(payload, Mapping) and payload.get("returncode") is not None:
                    try:
                        returncode = int(payload.get("returncode"))
                    except (TypeError, ValueError):
                        returncode = None
                artifact_uri = artifact_uri or (payload.get("artifact") if isinstance(payload, Mapping) else None) or (payload.get("artifact_uri") if isinstance(payload, Mapping) else None)
                if failed_step is None and ("failed" in str(event.get("event_type") or "") or "error" in str(event.get("event_type") or "")):
                    failed_step = str(event.get("event_type"))
            for checkpoint in checkpoints:
                artifact_uri = artifact_uri or checkpoint.get("artifact_uri")
                payload = self._json_field(checkpoint.get("payload_json"), {})
                if returncode is None and isinstance(payload, Mapping) and payload.get("returncode") is not None:
                    try:
                        returncode = int(payload.get("returncode"))
                    except (TypeError, ValueError):
                        returncode = None
                if failed_step is None and isinstance(payload, Mapping) and payload.get("failure_reason"):
                    failed_step = str(payload.get("failure_reason"))
            status = str((run or {}).get("status") or (job or {}).get("status") or (occurrence or {}).get("state") or "never_run")
            timing = timing_by_key.get(key, {"trigger_kind": str((task.get("trigger") or {}).get("kind") or "calendar"), "next_run_at": None, "next_run_state": "unknown", "next_run_reason": "计划注册表无法校验，未计算下次运行时间"})
            evidence = task.get("evidence") if isinstance(task.get("evidence"), Mapping) else {}
            marker = str(evidence.get("marker") or "") or None
            marker_path = Path(marker).expanduser() if marker else None
            marker_info = {"path": str(marker_path), "exists": marker_path.is_file(), "source_status": "observed" if marker_path.is_file() else "unavailable"} if marker_path else None
            observed_at = (run or {}).get("updated_at") or (job or {}).get("updated_at") or (occurrence or {}).get("updated_at") or registry_row["loaded_at"]
            artifact = artifact_info(artifact_uri)
            freshness = _snapshot_freshness(observed_at)
            launch_label = "com.zhujie14.pm-scheduler"
            launch = next((item for item in launch_agents if item.get("label") == launch_label), None)
            return {
                "schedule_key": key,
                "configured": True,
                "handler": task.get("handler"),
                "calendar": task.get("calendar"),
                "trigger": task.get("trigger"),
                **timing,
                "timezone": canonical.get("timezone") if isinstance(canonical, Mapping) else None,
                "deadline": task.get("deadline"),
                "lock": task.get("lock"),
                "retry": task.get("retry"),
                "launch_agent_label": launch_label,
                "launch_agent_config_hash": launch.get("config_hash") if launch else None,
                "launchd_state": launch.get("launchd_state", "unknown") if launch else "unavailable",
                "occurrence_id": (occurrence or {}).get("occurrence_id"),
                "occurrence_state": (occurrence or {}).get("state") or "never_run",
                "job_id": (job or {}).get("job_id"),
                "run_id": (run or {}).get("run_id"),
                "status": status,
                "actual_exit_code": returncode,
                "failed_step": failed_step,
                "last_event": last_event,
                "artifact": artifact,
                "marker": marker_info,
                "evidence": dict(evidence),
                "freshness": freshness,
                "observed_at": observed_at,
                "source_status": "observed" if occurrence or run or job else "unknown",
                "next_action": "查看最近 occurrence/Run" if status in {"failed", "dead_letter", "expired", "deferred", "interrupted"} else "无",
            }

        task_states = [task_state(task) for task in tasks if isinstance(task, Mapping)]
        state = str(registry_row["state"] or "unknown")
        latest_tick = None
        if self._table_exists(connection, "scheduler_ticks"):
            row = connection.execute("SELECT * FROM scheduler_ticks ORDER BY started_at DESC,tick_id DESC LIMIT 1").fetchone()
            latest_tick = dict(row) if row else None
        freshness_basis = (latest_tick or {}).get("completed_at") or (latest_tick or {}).get("started_at") or registry_row["loaded_at"]
        status = "observed" if state == "valid" else "unknown"
        base.update(
            {
                "registry": {"registry_version": registry_row["registry_version"], "registry_hash": registry_row["registry_hash"], "source_path": registry_row["source_path"], "state": state, "error": registry_row["error"], "loaded_at": registry_row["loaded_at"]},
                "config_version": registry_row["registry_version"],
                "tasks": tasks,
                "task_states": task_states,
                "occurrences": occurrences,
                "jobs": jobs,
                "runs": runs,
                "latest_tick": latest_tick,
                "freshness": _snapshot_freshness(freshness_basis),
                "source_status": "observed" if status == "observed" else "unknown",
                "status": status,
            }
        )
        return base

    def _operations(
        self,
        connection: Any,
        modules: List[Dict[str, Any]],
        attention: Optional[Dict[str, Any]] = None,
        schedules: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        alerts = (attention or {}).get("items", []) if isinstance(attention, Mapping) else []
        by_module: Dict[str, List[str]] = {}
        for alert in alerts:
            if str(alert.get("state")) == "open" and alert.get("alert_id"):
                by_module.setdefault(str(alert.get("module") or "unknown"), []).append(str(alert["alert_id"]))
        result = []
        for item in modules:
            details = item.get("details") if isinstance(item.get("details"), Mapping) else {}
            incident_ids = by_module.get(str(item.get("module")), [])
            status = str(item.get("status") or "unknown")
            reconcile_state = "required" if incident_ids else "clear" if status == "healthy" and item.get("freshness") == "fresh" else "unknown"
            result.append(
                {
                    "operation_id": f"operation:{item.get('module')}",
                    "module_id": item.get("module"),
                    "process": details.get("process"),
                    "heartbeat": details.get("heartbeat_at"),
                    "lease": details.get("lease_id"),
                    "automation": details.get("automation"),
                    "current_run": details.get("current_run") or details.get("run_id"),
                    "last_exit_code": details.get("last_exit_code"),
                    "version": details.get("version") or details.get("source_version"),
                    "incident_ids": incident_ids,
                    "reconcile_state": reconcile_state,
                    "freshness": item.get("freshness", "unknown"),
                    "status": status,
                    "source_status": item.get("source_status") if item.get("source_status") in {"observed", "unknown", "unavailable"} else "unknown",
                    "evidence_refs": [f"module_health_snapshots:{item.get('module')}"],
                }
            )
        # A schedule is an independently operable runtime unit.  Project it
        # alongside module health so Operations has direct occurrence/Run
        # evidence instead of forcing the user to infer it from Scheduler.
        for task in (schedules or {}).get("task_states", []):
            if not isinstance(task, Mapping):
                continue
            schedule_key = str(task.get("schedule_key") or "")
            if not schedule_key:
                continue
            occurrence_state = str(task.get("occurrence_state") or "never_run")
            task_status = str(task.get("status") or occurrence_state)
            incident_ids = [str(alert.get("alert_id")) for alert in alerts if str(alert.get("state")) == "open" and str(alert.get("schedule_key") or "") == schedule_key and alert.get("alert_id")]
            result.append(
                {
                    "operation_id": f"operation:schedule:{schedule_key}",
                    "module_id": "Scheduler",
                    "schedule_key": schedule_key,
                    "handler": task.get("handler"),
                    "process": "pm-system-worker",
                    "heartbeat": (schedules or {}).get("latest_tick", {}).get("completed_at") if isinstance((schedules or {}).get("latest_tick"), Mapping) else None,
                    "lease": task.get("lock", {}).get("key") if isinstance(task.get("lock"), Mapping) else None,
                    "automation": None,
                    "current_run": task.get("run_id"),
                    "occurrence_id": task.get("occurrence_id"),
                    "last_exit_code": task.get("actual_exit_code"),
                    "version": (schedules or {}).get("registry", {}).get("registry_hash") if isinstance((schedules or {}).get("registry"), Mapping) else None,
                    "incident_ids": incident_ids,
                    "reconcile_state": "required" if incident_ids or occurrence_state in {"failed", "dead_letter", "expired"} else "clear" if task_status in {"completed", "never_run"} else "unknown",
                    "freshness": task.get("freshness", "unknown"),
                    "status": task_status,
                    "source_status": task.get("source_status", "unknown"),
                    "evidence_refs": [f"schedule_occurrences:{task.get('occurrence_id')}"] if task.get("occurrence_id") else [f"schedule_registry_state:{schedule_key}"],
                }
            )
        if self._table_exists(connection, "operations"):
            operation_limit = max(1, min(len(result) or 100, 1000))
            canonical_rows = connection.execute(
                "SELECT * FROM operations ORDER BY updated_at DESC,operation_id DESC LIMIT ?",
                (operation_limit,),
            ).fetchall()
            if canonical_rows:
                by_key = {str(item.get("operation_id")): item for item in result}
                for row in canonical_rows:
                    canonical = dict(row)
                    canonical["incident_ids"] = self._json_field(canonical.pop("incident_ids_json"), [])
                    canonical["evidence_refs"] = self._json_field(canonical.pop("evidence_refs_json"), [])
                    canonical["read_only"] = True
                    existing_key = canonical.get("operation_id")
                    if existing_key in by_key:
                        by_key[existing_key].update(canonical, source_status="observed")
                    else:
                        result.append({**canonical, "source_status": "observed"})
        return result

    def _role_output_candidates(self) -> list[tuple[Mapping[str, Any], Path]]:
        """Return only allowlisted local HTML outputs below the project root."""
        if self.project_root is None or not self.project_root.is_dir():
            return []
        root = self.project_root.resolve()
        seen: set[Path] = set()
        candidates: list[tuple[Mapping[str, Any], Path]] = []
        for spec in ROLE_OUTPUT_SPECS:
            for pattern in spec["patterns"]:
                try:
                    matches = root.glob(str(pattern))
                except OSError:
                    continue
                for path in matches:
                    try:
                        resolved = path.resolve()
                        resolved.relative_to(root)
                    except (OSError, ValueError):
                        continue
                    # Keep the opaque-ID allowlist independent of any link
                    # target.  A report symlink is never browser-openable,
                    # even if it happens to resolve below the project root.
                    if path.is_symlink() or resolved in seen or not resolved.is_file() or resolved.suffix.lower() != ".html":
                        continue
                    seen.add(resolved)
                    candidates.append((spec, resolved))
        return candidates

    def _role_output_id(self, path: Path) -> str:
        root = self.project_root.resolve() if self.project_root is not None else path.parent.resolve()
        relative = path.resolve().relative_to(root).as_posix()
        return hashlib.sha256(relative.encode("utf-8")).hexdigest()

    def _role_output_history(self) -> Dict[str, Any]:
        """Project historical browser artifacts without exposing arbitrary paths."""
        items: List[Dict[str, Any]] = []
        fingerprints: list[str] = []
        root = self.project_root.resolve() if self.project_root is not None else None
        for spec, path in self._role_output_candidates():
            try:
                stat = path.stat()
                relative = path.relative_to(root).as_posix() if root is not None else path.name
            except OSError:
                continue
            output_id = self._role_output_id(path)
            updated_at = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            items.append(
                {
                    "output_id": output_id,
                    "name": path.name,
                    "title": spec["title"],
                    "kind": spec["kind"],
                    "role_id": spec["role_id"],
                    "schedule_key": spec["schedule_key"],
                    "updated_at": updated_at,
                    "size_bytes": stat.st_size,
                    "relative_path": relative,
                    "open_url": f"/artifacts/role-outputs/{output_id}",
                    "source_status": "observed",
                }
            )
            fingerprints.append(f"{relative}:{stat.st_mtime_ns}:{stat.st_size}")
        items.sort(key=lambda item: (str(item["updated_at"]), str(item["name"])), reverse=True)
        fingerprint = "sha256:" + hashlib.sha256("\n".join(sorted(fingerprints)).encode("utf-8")).hexdigest()
        return {
            "items": items,
            "source_status": "observed" if root is not None and root.is_dir() else "unknown",
            "source_fingerprint": fingerprint,
        }

    def role_output_path(self, output_id: str) -> Optional[Path]:
        """Resolve an opaque historical-output ID through the same allowlist."""
        if not re.fullmatch(r"[0-9a-f]{64}", str(output_id)):
            return None
        for _spec, path in self._role_output_candidates():
            if self._role_output_id(path) == output_id:
                return path
        return None

    @staticmethod
    def _future_role_outputs(schedules: Mapping[str, Any]) -> List[Dict[str, Any]]:
        role_by_schedule = {
            str(spec["schedule_key"]): str(spec["role_id"])
            for spec in ROLE_OUTPUT_SPECS
            if spec.get("schedule_key")
        }
        future: List[Dict[str, Any]] = []
        for item in schedules.get("task_states", []) if isinstance(schedules, Mapping) else []:
            if not isinstance(item, Mapping):
                continue
            schedule_key = str(item.get("schedule_key") or "")
            if not schedule_key:
                continue
            trigger_kind = str(item.get("trigger_kind") or "calendar")
            future.append(
                {
                    "schedule_key": schedule_key,
                    "title": SCHEDULE_OUTPUT_LABELS.get(schedule_key, f"计划产物：{schedule_key}"),
                    "role_id": role_by_schedule.get(schedule_key),
                    "trigger_kind": trigger_kind,
                    "next_run_at": item.get("next_run_at"),
                    "next_run_reason": item.get("next_run_reason"),
                    "timezone": item.get("timezone"),
                    "schedule_status": item.get("status") or "unknown",
                    "source_status": item.get("source_status") or "unknown",
                }
            )
        return future

    def _roles(self, connection: Any, limit: int, schedules: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        """Join the frozen role manifest with locally installed Skill evidence.

        This is deliberately a local-private, read-only mapping.  It does not
        claim multi-user authorization or create a second role scheduler.
        """
        skills_root = self.runtime_home / ".codex" / "skills"
        recent_runs = [
            dict(row)
            for row in connection.execute(
                "SELECT run_id,job_id,loop_id,status,updated_at FROM runs ORDER BY updated_at DESC,run_id DESC LIMIT ?",
                (max(1, min(int(limit), 100)),),
            ).fetchall()
        ]
        history = self._role_output_history()
        historical_outputs = history["items"]
        future_outputs = self._future_role_outputs(schedules or {})
        items: List[Dict[str, Any]] = []
        for role in ROLE_PROFILES:
            skill_refs = []
            all_present = True
            for skill in role["skills"]:
                manifest = skills_root / skill / "SKILL.md"
                present = manifest.is_file()
                all_present = all_present and present
                skill_refs.append({"skill_id": skill, "manifest_path": str(manifest), "manifest_hash": self._external_file_hash(manifest) if present else None, "source_status": "observed" if present else "unavailable"})
            # Only explicit runtime role IDs qualify as a role's own run. A
            # general PM run is not silently attributed to a named role.
            role_runs = [run for run in recent_runs if str(run.get("loop_id") or "") == role["role_id"]]
            items.append(
                {
                    **dict(role),
                    "actions": ["read", "draft_only"],
                    "availability": "read_only" if all_present else "degraded",
                    "access_scope": "local_private_only",
                    "authorization_status": "single_user_local",
                    "skill_manifest_refs": skill_refs,
                    "recent_runs": role_runs[:3],
                    "historical_output_count": sum(1 for item in historical_outputs if item["role_id"] == role["role_id"]),
                    "future_output_count": sum(1 for item in future_outputs if item.get("role_id") == role["role_id"]),
                    "source_status": "observed" if all_present else "degraded",
                }
            )
        return {
            "items": items,
            "historical_outputs": historical_outputs,
            "future_outputs": future_outputs,
            "historical_source_status": history["source_status"],
            "source_fingerprint": history["source_fingerprint"],
            "source_status": "observed" if skills_root.is_dir() else "unknown",
            "read_only": True,
            "access_scope": "local_private_only",
            "authorization_status": "single_user_local",
            "metric_source": "ROLE_PROFILES+local_skill_manifests+allowlisted_project_reports+schedule_registry",
        }

    def _concepts(self, connection: Any, limit: int) -> Dict[str, Any]:
        source_coverage = self._concept_source_coverage()
        concept_tables = ("concept_candidates", "concept_versions", "concept_hot_projection", "concept_publish_ledger", "concept_source_map", "concept_admissions")
        present = [table for table in concept_tables if self._table_exists(connection, table)]
        if len(present) != len(concept_tables):
            missing = [table for table in concept_tables if table not in present]
            return {
                "candidates": [],
                "active": [],
                "hot": [],
                "publish_ledger": [],
                "source_map": [],
                "quarantine_count": None,
                "admission": None,
                "summary": {"sample_limit": limit},
                "generation": {"source_status": "not_implemented", "active": None, "status_counts": {}},
                "profile": None,
                "policy": None,
                "model_resolutions": [],
                "admission_events": [],
                "source_coverage": source_coverage,
                "blockers": [],
                "source_status": "not_implemented",
                "status": "not_implemented",
                "read_only": True,
                "reason": "概念域表尚未完整接入当前协调库；缺失：" + ", ".join(missing),
            }

        def count_rows(table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
            sql = f'SELECT COUNT(*) FROM "{table}"'
            if where:
                sql += " WHERE " + where
            return int(connection.execute(sql, params).fetchone()[0])

        def status_counts(table: str, *, domain: Optional[str] = None) -> Dict[str, int]:
            if not self._table_exists(connection, table):
                return {}
            if domain is None:
                rows = connection.execute(f'SELECT status,COUNT(*) FROM "{table}" GROUP BY status').fetchall()
            else:
                rows = connection.execute(
                    f'SELECT status,COUNT(*) FROM "{table}" WHERE domain=? GROUP BY status',
                    (domain,),
                ).fetchall()
            return {str(row[0] or "unknown"): int(row[1]) for row in rows}

        candidates = self._optional_rows(
            connection,
            "concept_candidates",
            ["candidate_id", "concept_id", "namespace_epoch", "proposed_version", "content_hash", "quality_score", "policy_decision", "status", "model_requested", "model_resolved", "created_at", "updated_at"],
            order_by=["updated_at", "created_at"],
            descending=True,
            limit=limit,
        )
        active = self._optional_rows(
            connection,
            "concept_versions",
            ["version_id", "concept_id", "namespace_epoch", "version", "generation_id", "content_hash", "status", "created_at", "provenance"],
            where="status='active'",
            order_by=["created_at"],
            descending=True,
            limit=limit,
        )
        hot = self._optional_rows(
            connection,
            "concept_hot_projection",
            ["concept_id", "namespace_epoch", "generation_id", "projection_state", "observed_content_hash", "observed_at", "provenance", "updated_at"],
            order_by=["updated_at", "observed_at"],
            descending=True,
            limit=limit,
        )
        publish_ledger = self._optional_rows(
            connection,
            "concept_publish_ledger",
            ["publish_id", "concept_id", "namespace_epoch", "version_id", "previous_generation", "current_generation", "current_hot_generation", "desired_hot_generation", "projection_state", "projection_outbox_id", "operator", "evidence_hash", "created_at", "updated_at", "provenance"],
            order_by=["updated_at", "created_at"],
            descending=True,
            limit=limit,
        )
        alignment = []
        for version in active:
            concept_id = str(version.get("concept_id") or "")
            epoch = str(version.get("namespace_epoch") or "")
            hot_item = self._optional_rows(
                connection,
                "concept_hot_projection",
                ["concept_id", "namespace_epoch", "generation_id", "projection_state", "observed_content_hash", "observed_at", "provenance", "updated_at"],
                where="concept_id=? AND namespace_epoch=?",
                params=(concept_id, epoch),
                order_by=["updated_at", "observed_at"],
                descending=True,
                limit=1,
            )
            ledger_item = self._optional_rows(
                connection,
                "concept_publish_ledger",
                ["publish_id", "concept_id", "namespace_epoch", "version_id", "previous_generation", "current_generation", "current_hot_generation", "desired_hot_generation", "projection_state", "projection_outbox_id", "operator", "evidence_hash", "created_at", "updated_at", "provenance"],
                where="concept_id=? AND namespace_epoch=?",
                params=(concept_id, epoch),
                order_by=["updated_at", "created_at"],
                descending=True,
                limit=1,
            )
            hot_value = hot_item[0] if hot_item else None
            ledger_value = ledger_item[0] if ledger_item else None
            provenance = {str(version.get("provenance") or "runtime")}
            if hot_value is not None:
                provenance.add(str(hot_value.get("provenance") or "runtime"))
            if ledger_value is not None:
                provenance.add(str(ledger_value.get("provenance") or "runtime"))
            expected_generation = version.get("generation_id")
            if hot_value is None or ledger_value is None:
                alignment_status = "not_recorded"
            elif "legacy_import" in provenance:
                alignment_status = "legacy_import"
            elif expected_generation == hot_value.get("generation_id") == ledger_value.get("current_generation"):
                alignment_status = "aligned"
            else:
                alignment_status = "mismatch"
            alignment.append(
                {
                    "concept_id": concept_id,
                    "namespace_epoch": epoch,
                    "version": version.get("version"),
                    "version_id": version.get("version_id"),
                    "version_generation": expected_generation,
                    "version_created_at": version.get("created_at"),
                    "version_provenance": version.get("provenance"),
                    "hot_generation": hot_value.get("generation_id") if hot_value else None,
                    "hot_projection_state": hot_value.get("projection_state") if hot_value else None,
                    "hot_updated_at": hot_value.get("updated_at") if hot_value else None,
                    "hot_provenance": hot_value.get("provenance") if hot_value else None,
                    "ledger_generation": ledger_value.get("current_generation") if ledger_value else None,
                    "ledger_projection_state": ledger_value.get("projection_state") if ledger_value else None,
                    "ledger_updated_at": ledger_value.get("updated_at") if ledger_value else None,
                    "ledger_provenance": ledger_value.get("provenance") if ledger_value else None,
                    "ledger_publish_id": ledger_value.get("publish_id") if ledger_value else None,
                    "ledger_operator": ledger_value.get("operator") if ledger_value else None,
                    "ledger_evidence_hash": ledger_value.get("evidence_hash") if ledger_value else None,
                    "ledger_projection_outbox_id": ledger_value.get("projection_outbox_id") if ledger_value else None,
                    "hot_observed_content_hash": hot_value.get("observed_content_hash") if hot_value else None,
                    "alignment_status": alignment_status,
                }
            )
        source_map = self._optional_rows(
            connection,
            "concept_source_map",
            ["map_id", "concept_id", "namespace_epoch", "source_id", "source_uri", "leaf_uri", "identity_method", "status", "confidence", "conflict_set_id", "owner", "evidence_refs_json", "evidence_set_hash", "next_action", "expires_at", "resolved_at", "resolved_by", "resolution_reason", "created_at", "updated_at"],
            where="status='quarantined'",
            order_by=["updated_at", "created_at"],
            descending=True,
            limit=limit,
        )
        for item in source_map:
            item["evidence_refs"] = self._json_field(item.pop("evidence_refs_json"), [])
        admission = self._optional_rows(
            connection,
            "concept_admissions",
            ["namespace_epoch", "admission_state", "version", "admission_snapshot_id", "policy_version", "operator", "reason", "observed_at", "expires_at", "renewal_policy", "evidence_hash", "updated_at"],
            order_by=["updated_at", "observed_at"],
            descending=True,
            limit=1,
        )
        admission_value = admission[0] if admission else None
        namespace_epoch = str((admission_value or {}).get("namespace_epoch") or "")
        profile_rows = self._optional_rows(
            connection,
            "concept_profile_admissions",
            ["workload", "profile", "namespace_epoch", "pending_count", "pending_soft_limit", "pending_high_water", "outbox_hard_cap", "pause_fence", "throttle_until", "provider_budget_remaining", "policy_hash", "updated_at"],
            order_by=["updated_at"],
            descending=True,
            limit=limit,
        )
        profile = next((item for item in profile_rows if item.get("namespace_epoch") == namespace_epoch), profile_rows[0] if profile_rows else None)
        generation_rows = self._optional_rows(
            connection,
            "generations",
            ["generation_id", "domain", "generation_hash", "status", "source_watermark", "knowledge_watermark", "created_at", "active_at"],
            where="domain=?",
            params=("concepts",),
            order_by=["active_at", "created_at"],
            descending=True,
            limit=limit,
        )
        active_generation_rows = self._optional_rows(
            connection,
            "generations",
            ["generation_id", "domain", "generation_hash", "status", "source_watermark", "knowledge_watermark", "created_at", "active_at"],
            where="domain=? AND status='active'",
            params=("concepts",),
            order_by=["active_at", "created_at"],
            descending=True,
            limit=1,
        )
        active_generation = active_generation_rows[0] if active_generation_rows else None
        policies = self._optional_rows(
            connection,
            "concept_model_policies",
            ["policy_version", "provider", "requested_model", "allowed_models_json", "capability_class", "privacy_scope", "cost_limit", "latency_limit_seconds", "policy_hash", "status", "created_at"],
            where="status='active'",
            order_by=["created_at"],
            descending=True,
            limit=1,
        )
        policy = policies[0] if policies else None
        if policy is not None:
            policy["allowed_models"] = self._json_field(policy.pop("allowed_models_json"), [])
        resolutions = self._optional_rows(
            connection,
            "concept_model_resolutions",
            ["resolution_id", "run_id", "call_id", "stage", "attempt", "model_requested", "model_resolved", "resolution_status", "policy_version", "provider", "resolution_changed", "model_input_hash", "evidence_hash", "created_at"],
            order_by=["created_at"],
            descending=True,
            limit=limit,
        )
        admission_events = self._optional_rows(
            connection,
            "concept_admission_events",
            ["event_id", "namespace_epoch", "from_state", "to_state", "expected_version", "new_version", "admission_snapshot_id", "policy_version", "operator", "evidence_hash", "reason", "observed_at"],
            order_by=["observed_at"],
            descending=True,
            limit=limit,
        )
        source_status_counts = status_counts("concept_source_map")
        candidate_status_counts = status_counts("concept_candidates")
        generation_status_counts = status_counts("generations", domain="concepts")
        source_map_count = count_rows("concept_source_map")
        quarantine_count = int(source_status_counts.get("quarantined", 0))
        coverage_counts = source_coverage.get("concept_status_counts") if isinstance(source_coverage.get("concept_status_counts"), Mapping) else {}
        coverage_is_closed = (
            str(source_coverage.get("status") or "") == "pass"
            and source_coverage.get("p3_closed") is True
            and int(coverage_counts.get("needs_repair") or 0) == 0
            and int(source_coverage.get("concept_count") or 0) == 45
        )
        # C7 deliberately keeps quarantined rows as immutable audit history.
        # Once the independently hashed coverage report proves that every
        # concept is refreshable or retired, those rows are no longer an
        # unresolved P1. Keep the raw count for audit/UI transparency.
        effective_quarantine_count = 0 if coverage_is_closed else quarantine_count
        active_count = count_rows("concept_versions", "status='active'")
        hot_count = count_rows("concept_hot_projection")
        publish_count = count_rows("concept_publish_ledger")
        candidate_count = count_rows("concept_candidates")
        blockers: List[Dict[str, Any]] = []
        if admission_value is None:
            blockers.append({"id": "admission_missing", "severity": "P0", "title": "Admission 未记录", "detail": "概念域没有可读取的 admission 状态，不能判断自动刷新权限。"})
        elif admission_value.get("admission_state") == "disabled":
            blockers.append(
                {
                    "id": "admission_owner_decision",
                    "severity": "P2",
                    "title": "Admission 灰度推进待负责人决策",
                    "detail": "来源闭包、正文预检、Baseline 与 45 条 Hot/Publish 投影已闭合；当前 disabled 只阻止未批准发布，不代表概念自动链路停用。",
                    "status": "requires_owner_decision",
                    "refresh_trigger": "pm_scheduler_dependency",
                }
            )
        elif admission_value.get("admission_state") not in {"canary", "incremental"}:
            blockers.append({"id": "admission_unexpected_state", "severity": "P0", "title": "Admission 状态异常", "detail": f"concept_admission={admission_value.get('admission_state') or 'unknown'} / version={admission_value.get('version') or 'unknown'}。"})
        if active_generation is None:
            blockers.append({"id": "active_generation_missing", "severity": "P0", "title": "生产 Generation 缺失", "detail": "generations(domain='concepts') 没有 active 记录；历史 legacy_import 不能替代可回滚生产 Generation。"})
        if effective_quarantine_count:
            blockers.append({"id": "source_map_quarantine", "severity": "P1", "title": "来源仍在隔离", "detail": f"{effective_quarantine_count} 条 source map 尚无有效 disposition；仅 mapped/已处置来源可参与后续编译。"})
        return {
            "candidates": candidates,
            "active": active,
            "alignment": alignment,
            "hot": hot,
            "publish_ledger": publish_ledger,
            "source_map": source_map,
            "quarantine_count": quarantine_count,
            "effective_quarantine_count": effective_quarantine_count,
            "quarantine_scope": "historical_exclusion" if quarantine_count and not effective_quarantine_count else "active_unresolved" if effective_quarantine_count else "none",
            "source_map_sample_scope": "quarantined",
            "admission": admission_value,
            "profile": profile,
            "policy": policy,
            "model_resolutions": resolutions,
            "admission_events": admission_events,
            "source_coverage": source_coverage,
            "generation": {
                "active": active_generation,
                "recent": generation_rows,
                "status_counts": generation_status_counts,
                "source_status": "observed" if self._table_exists(connection, "generations") else "not_implemented",
            },
            "summary": {
                "sample_limit": limit,
                "active_count": active_count,
                "hot_count": hot_count,
                "publish_ledger_count": publish_count,
                "candidate_count": candidate_count,
                "candidate_status_counts": candidate_status_counts,
                "source_map_count": source_map_count,
                "source_status_counts": source_status_counts,
                "model_resolution_count": count_rows("concept_model_resolutions") if self._table_exists(connection, "concept_model_resolutions") else None,
                "admission_event_count": count_rows("concept_admission_events") if self._table_exists(connection, "concept_admission_events") else None,
            },
            "blockers": blockers,
            "source_status": "observed",
            "status": "observed",
            "read_only": True,
        }

    def snapshot(self, *, limit: int = 20) -> Dict[str, Any]:
        read_at = _now()
        with self._snapshot_connection() as connection:
            counts = self._counts(connection)
            bounded = max(1, min(int(limit), 100))
            schedules = self._schedules(connection, bounded)
            attention = self._attention(connection, limit=bounded, read_at=read_at, schedules=schedules)
            concepts = self._concepts(connection, bounded)
            roles = self._roles(connection, bounded, schedules)
            version = self._source_version(connection, counts)
            if schedules.get("source_fingerprint"):
                version = "sha256:" + hashlib.sha256(f"{version}|{schedules['source_fingerprint']}".encode("utf-8")).hexdigest()
            if schedules.get("knowledge_sources", {}).get("source_fingerprint"):
                version = "sha256:" + hashlib.sha256(f"{version}|{schedules['knowledge_sources']['source_fingerprint']}".encode("utf-8")).hexdigest()
            if concepts.get("source_coverage", {}).get("source_fingerprint"):
                version = "sha256:" + hashlib.sha256(f"{version}|{concepts['source_coverage']['source_fingerprint']}".encode("utf-8")).hexdigest()
            if roles.get("source_fingerprint"):
                version = "sha256:" + hashlib.sha256(f"{version}|{roles['source_fingerprint']}".encode("utf-8")).hexdigest()
            errors = attention["items"]
            providers = [dict(row) for row in connection.execute("SELECT provider_key,provider,endpoint,model,throttle_until,circuit_state,consecutive_429,last_retry_after,updated_at FROM provider_buckets ORDER BY updated_at DESC LIMIT 20").fetchall()]
            provider_tokens = {
                "active": int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL AND expires_at>?", (read_at,)).fetchone()[0]),
                "leased": int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]),
                "expired_unreclaimed": int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL AND expires_at<=?", (read_at,)).fetchone()[0]),
            }
            runs = [dict(row) for row in connection.execute("SELECT * FROM runs ORDER BY updated_at DESC,run_id DESC LIMIT ?", (bounded,)).fetchall()]
            self._attach_run_event_summaries(connection, runs)
            modules = self.modules(connection=connection)
            watermarks = {name: self._watermark_value(connection, name) for name in ("source", "content", "knowledge", "active_generation")}
            incident_count = int(attention.get("p0_p1_open", 0))
            module_by_name = {item["module"]: item for item in modules}
            incident_module = any(item["status"] == "incident" for item in modules)
            degraded_module = any(item["status"] == "degraded" for item in modules)
            unknown_key_signal = any(module_by_name[name]["status"] == "unknown" or module_by_name[name]["freshness"] == "stale" for name in KEY_SIGNAL_MODULES)
            missing_watermark = any(watermarks[name] in (None, "") for name in ("source", "content", "knowledge", "active_generation"))
            if incident_count or incident_module:
                system_status = "incident"
            elif degraded_module:
                system_status = "degraded"
            elif unknown_key_signal or missing_watermark:
                system_status = "unknown"
            else:
                system_status = "healthy"
            acceptance_status = module_by_name["RunStore"]["status"] if module_by_name["RunStore"]["status"] in {"healthy", "degraded", "incident", "unknown"} else "unknown"
            execution_statuses = (module_by_name["Scheduler"]["status"], module_by_name["Worker"]["status"])
            knowledge_statuses = (module_by_name["OpenViking"]["status"], module_by_name["Source"]["status"], module_by_name["Evidence"]["status"])

            def capability_status(statuses: tuple[str, ...], *, missing: bool = False) -> str:
                if "incident" in statuses:
                    return "incident"
                if "degraded" in statuses:
                    return "degraded"
                if missing or "unknown" in statuses:
                    return "unknown"
                return "healthy"

            result = {
                "schema_version": COCKPIT_SCHEMA,
                "read_only": True,
                "read_at": read_at,
                "as_of": read_at,
                "source_version": version,
                "source_status": "observed",
                "source_cursor": version,
                "metric_source": "pm-system.db",
                "freshness": "fresh",
                "evidence_status": "observed",
                "status": system_status,
                "overall_state": system_status,
                "summary": {"acceptance": acceptance_status, "execution": capability_status(execution_statuses, missing=not counts["max_slots"]), "knowledge": capability_status(knowledge_statuses, missing=missing_watermark), "active_codex_slots": counts["active_slots"], "max_codex_slots": counts["max_slots"], "queued_runs": counts["queued_jobs"], "running_runs": counts["running_runs"], "outbox_pending": counts["outbox_pending"], "semantic_queue": counts["semantic_queued"], "terminal_failed": counts["failed"], "dead_letter": counts["dead_letter"], "historical_terminal_failed": counts["failed"], "historical_dead_letter": counts["dead_letter"], "incident_count": incident_count, "provider_tokens": provider_tokens},
                "modules": modules,
                "providers": providers,
                "provider_tokens": provider_tokens,
                "queues": {"jobs": {"queued": counts["queued_jobs"], "running": counts["running_jobs"]}, "outbox": {"pending": counts["outbox_pending"], "failed": counts["failed_outbox"], "dead_letter": counts["dead_letter_outbox"]}, "semantic": {"queued": counts["semantic_queued"], "accepted": counts["semantic_accepted"], "processing": counts["semantic_processing"], "degraded": counts["semantic_degraded"], "failed": counts["failed_semantic"], "dead_letter": counts["dead_letter_semantic"]}},
                "watermarks": watermarks,
                "incidents": errors,
                "ops_attention_view": attention,
                "runs": runs,
                "gates": self._gate_manifest(connection, version, read_at),
                "activity": self._activity(connection, bounded),
                "work_items": self._work_items(connection, bounded),
                "plans": self._plans(connection, schedules),
                "reviews": self._reviews(connection, bounded),
                "operations": self._operations(connection, modules, attention, schedules),
                "schedules": schedules,
                "roles": roles,
                "concepts": concepts,
            }
            return result

    def _attach_run_event_summaries(self, connection: Any, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def readable_error(payload: Any) -> Optional[str]:
            if not isinstance(payload, Mapping):
                return None
            # Keep the human-readable exception separate from the durable
            # fingerprint stored on the run row.  The latter is useful for
            # dedupe, but is not an explanation for operators.
            for key in ("error", "reason", "detail", "message", "failure_reason", "exception", "stderr"):
                value = payload.get(key)
                if value is None:
                    continue
                text = str(value).strip()
                if not text or _looks_like_error_fingerprint(text):
                    continue
                return text
            return None

        for row in rows:
            event_rows = connection.execute(
                """SELECT event_type,payload_json FROM run_events
                   WHERE run_id=? AND (
                     event_type IN ('run/failed','run/cancelled','run/rejected','gate/rejected')
                     OR event_type LIKE '%/failed'
                     OR event_type LIKE '%/error'
                   ) ORDER BY seq DESC""",
                (row.get("run_id"),),
            ).fetchall()
            event = event_rows[0] if event_rows else None
            readable_detail = None
            if event_rows:
                # Scheduler may append a compact fingerprint after the worker
                # exception. Prefer the event with readable detail.
                for candidate in event_rows:
                    payload = self._json_field(candidate["payload_json"], {})
                    detail = readable_error(payload)
                    if detail:
                        event = candidate
                        readable_detail = detail
                        break
            if event is not None:
                event_payload = self._json_field(event["payload_json"], {})
                row["last_event"] = {
                    "type": event["event_type"],
                    "payload": event_payload,
                }
                readable_detail = readable_detail or readable_error(event_payload)
            if readable_detail:
                row["error_detail"] = readable_detail
                row["failure_reason"] = readable_detail
        return rows

    def list_runs(self, *, limit: int = 100, after: int = 0) -> Dict[str, Any]:
        read_at = _now()
        with self._snapshot_connection() as connection:
            bounded = max(1, min(int(limit), 500))
            if int(after) > 0:
                rows = [dict(row) for row in connection.execute("SELECT rowid AS _rowid,* FROM runs WHERE rowid<? ORDER BY rowid DESC LIMIT ?", (int(after), bounded)).fetchall()]
            else:
                rows = [dict(row) for row in connection.execute("SELECT rowid AS _rowid,* FROM runs ORDER BY rowid DESC LIMIT ?", (bounded,)).fetchall()]
            next_cursor = rows[-1].get("_rowid") if rows else None
            self._attach_run_event_summaries(connection, rows)
            for row in rows:
                row.pop("_rowid", None)
            version = self._source_version(connection)
            return {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": read_at, "as_of": read_at, "source_status": "observed", "source_cursor": version, "source_version": version, "metric_source": "pm-system.db", "runs": rows, "next_cursor": next_cursor}

    def run_detail(self, run_id: str) -> Dict[str, Any]:
        read_at = _now()
        with self._snapshot_connection() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            events = [dict(item) for item in connection.execute("SELECT * FROM run_events WHERE run_id=? ORDER BY seq", (run_id,)).fetchall()]
            calls = [dict(item) for item in connection.execute("SELECT * FROM model_calls WHERE run_id=? ORDER BY stage,attempt", (run_id,)).fetchall()]
            checkpoints = [dict(item) for item in connection.execute("SELECT * FROM checkpoints WHERE run_id=? ORDER BY updated_at,stage,checkpoint_key", (run_id,)).fetchall()]
            for item in events:
                item["payload"] = self._json_field(item.pop("payload_json"), {})
            artifacts = []
            for item in checkpoints:
                uri = item.get("artifact_uri")
                if uri:
                    artifacts.append({"artifact_uri": uri, "source": "checkpoint", "updated_at": item.get("updated_at") or item.get("created_at")})
            for item in calls:
                uri = item.get("artifact_uri")
                if uri:
                    artifacts.append({"artifact_uri": uri, "source": "model_call", "updated_at": item.get("completed_at") or item.get("started_at")})
            version = self._source_version(connection)
            run = dict(row)
            disposition = {"status": canonical_status(run.get("status"), failure_class=run.get("terminal_reason")), "terminal_reason": run.get("terminal_reason"), "error": run.get("error")}
            evidence_items = []
            if self._table_exists(connection, "evidence_refs") and run.get("snapshot_id"):
                evidence_items = [dict(item) for item in connection.execute("SELECT * FROM evidence_refs WHERE snapshot_id=? ORDER BY created_at,evidence_id", (run["snapshot_id"],)).fetchall()]
            evidence = {"status": "observed", "items": evidence_items} if evidence_items else {"status": "not_recorded", "items": [], "reason": "run 没有关联 evidence_refs"}
            task_package = {"status": "not_recorded", "package": None}
            package_candidates = [Path(str(item.get("artifact_uri"))).expanduser() for item in checkpoints if str(item.get("artifact_uri") or "").endswith("task-package.v1.json")]
            if package_candidates:
                package_path = package_candidates[-1]
                try:
                    package_value = json.loads(package_path.read_text(encoding="utf-8"))
                    if isinstance(package_value, dict) and package_value.get("schema_version") == "pm-task-package.v1":
                        task_package = {
                            "status": "observed",
                            "package": {
                                "schema_version": package_value.get("schema_version"),
                                "task": package_value.get("task"),
                                "execution": package_value.get("execution"),
                                "outcome": package_value.get("outcome"),
                                "stages": package_value.get("stages"),
                                "sources": package_value.get("sources"),
                                "baseline": package_value.get("baseline"),
                                "artifacts": package_value.get("artifacts"),
                                "evidence_refs": package_value.get("evidence_refs"),
                                "checks": package_value.get("checks"),
                                "next_action": package_value.get("next_action"),
                            },
                        }
                        if not evidence_items:
                            package_refs = package_value.get("evidence_refs") or []
                            package_artifacts = package_value.get("artifacts") or []
                            evidence = {
                                "status": "observed" if package_refs else "not_recorded",
                                "items": [
                                    {"evidence_ref": str(ref), "evidence_role": "scheduled_artifact", "verified": True, "run_id": run_id}
                                    for ref in package_refs
                                ],
                                "reason": "scheduled package evidence" if package_refs else "run 没有关联 evidence_refs",
                            }
                except (OSError, ValueError, TypeError):
                    task_package = {"status": "unavailable", "package": None, "reason": "task package unreadable"}
            review_item = {
                "run_id": run_id,
                "canonical_status": disposition["status"],
                "review_state": "failed" if disposition["status"] in {"failed", "dead_letter", "quarantine", "interrupted"} else "result_ready",
                "artifact_uris": [str(item["artifact_uri"]) for item in artifacts],
                "evidence_refs": [str(item.get("evidence_ref")) for item in evidence_items if item.get("evidence_ref")],
                "reason": run.get("error") or run.get("terminal_reason"),
            }
            review_diagnosis = self._review_diagnosis(connection, review_item)
            return {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": read_at, "as_of": read_at, "source_status": "observed", "source_cursor": version, "source_version": version, "metric_source": "pm-system.db", "run": run, "events": events, "model_calls": calls, "checkpoints": checkpoints, "artifacts": artifacts, "task_package": task_package, "evidence": evidence, "disposition": disposition, "review_diagnosis": review_diagnosis, "codex_advice": review_diagnosis["codex_advice"]}


class CockpitHTTPServer(ThreadingHTTPServer):
    """Optional local adapter for the V4.4 cockpit read model."""

    def __init__(self, address: tuple[str, int], model: CockpitReadModel) -> None:
        super().__init__(address, CockpitHandler)
        self.model = model


class CockpitHandler(BaseHTTPRequestHandler):
    server: CockpitHTTPServer

    def _json(self, status: int, value: Dict[str, Any], *, etag: Optional[str] = None) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        tag = etag or ("sha256:" + hashlib.sha256(payload).hexdigest())
        tag = tag if tag.startswith('"') else f'"{tag}"'
        if status == 200 and self.headers.get("If-None-Match") == tag:
            self.send_response(304)
            self.send_header("ETag", tag)
            self.end_headers()
            return
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "private, max-age=0, must-revalidate")
        self.send_header("ETag", tag)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        model = self.server.model
        try:
            if path in {
                "/api/control-plane/v4/summary",
                "/api/control-plane/v4/modules",
                "/api/control-plane/v4/incidents",
                "/api/control-plane/v4/queues",
                "/api/control-plane/v4/activity",
                "/api/control-plane/v4/work-items",
                "/api/control-plane/v4/plans",
                "/api/control-plane/v4/reviews",
                "/api/control-plane/v4/operations",
                "/api/control-plane/v4/roles",
                "/api/control-plane/v4/concepts",
                "/api/control-plane/v4/schedules",
            }:
                snapshot = model.snapshot()
                if path.endswith("/modules"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "modules": snapshot["modules"]}
                elif path.endswith("/incidents"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "incidents": snapshot["incidents"], "ops_attention_view": snapshot["ops_attention_view"]}
                elif path.endswith("/queues"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "queues": snapshot["queues"], "providers": snapshot["providers"]}
                elif path.endswith("/activity"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "activity": snapshot["activity"]}
                elif path.endswith("/work-items"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "work_items": snapshot["work_items"]}
                elif path.endswith("/plans"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "plans": snapshot["plans"]}
                elif path.endswith("/reviews"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "reviews": snapshot["reviews"]}
                elif path.endswith("/operations"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "operations": snapshot["operations"], "freshness": snapshot["freshness"], "evidence_status": snapshot["evidence_status"]}
                elif path.endswith("/roles"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "roles": snapshot["roles"]}
                elif path.endswith("/concepts"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "concepts": snapshot["concepts"], "gate": snapshot["gates"]["concept_view_gate"]}
                elif path.endswith("/schedules"):
                    value = {"schema_version": COCKPIT_SCHEMA, "read_only": True, "read_at": snapshot["read_at"], "source_version": snapshot["source_version"], "schedules": snapshot["schedules"], "freshness": snapshot["freshness"], "evidence_status": snapshot["evidence_status"]}
                else:
                    value = snapshot
                self._json(200, value, etag=snapshot["source_version"])
                return
            if path == "/api/control-plane/v4/runs":
                self._json(200, model.list_runs())
                return
            if path.startswith("/api/control-plane/v4/runs/"):
                run_id = path.rsplit("/", 1)[-1]
                self._json(200, model.run_detail(run_id))
                return
            self._json(404, {"error": "not_found", "read_only": True})
        except KeyError as exc:
            self._json(404, {"error": str(exc), "read_only": True})
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc), "read_only": True})
        except Exception as exc:
            self._json(500, {"error": f"{type(exc).__name__}: {exc}", "read_only": True})

    def do_POST(self) -> None:  # noqa: N802
        self._json(405, {"error": "read_only_cockpit", "read_only": True})

    def do_PUT(self) -> None:  # noqa: N802
        self._json(405, {"error": "read_only_cockpit", "read_only": True})

    def do_DELETE(self) -> None:  # noqa: N802
        self._json(405, {"error": "read_only_cockpit", "read_only": True})

    def log_message(self, _format: str, *_args: Any) -> None:
        return


__all__ = ["COCKPIT_SCHEMA", "CockpitHandler", "CockpitHTTPServer", "CockpitReadModel", "MODULES"]
