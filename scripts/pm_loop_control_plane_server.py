#!/usr/bin/env python3
"""Small local HTTP/SSE control plane for the Codex-only PM Loop runner."""

from __future__ import annotations

import argparse
import copy
import difflib
import fcntl
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

from pm_loop_runtime import RunStore, TERMINAL_STATES
from pm_loop_control_plane import collect_launchd_jobs
from concept_learning import ConceptLearningStore, content_hash, discover_from_uris
from concept_recheck import configured_concepts
from pm_system_cockpit import CockpitReadModel
from pm_system_scheduler import ADMISSION_ENABLED, Scheduler
from pm_system_store import PMSystemStore, StoreUnavailable
from competitive_radar_read_model import CompetitiveRadarReadModel
from retention_read_model import RetentionReadModel
from artifact_registry_read_model import ArtifactRegistryReadModel


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INVENTORY_ROOTS = ["data-agent", "datasearch", "feature-list", "ontology", "pipeline-logic-fde", "product-management", "public-docs"]
RUNNER = PROJECT_ROOT / "scripts" / "pm_loop_runner.py"
ACTION_RUNNER = PROJECT_ROOT / "scripts" / "pm_loop_action_runner.py"
WEB_ROOT = PROJECT_ROOT / "web" / "pm-loop-control-plane"
CONCEPT_REVIEW_PAGE = PROJECT_ROOT / "docs" / "03-产品架构" / "concept-review-control-plane-v2.html"
V2_DEMO_PAGE = WEB_ROOT / "v2-demo.html"
# The current V11 refresh chain is owned by the PM Scheduler and PM Worker.
# Its read model must not be conflated with the old Control Plane mutation
# endpoints.  Those endpoints stay hard-rejected so a stale UI cannot bypass
# Admission or publish directly, while the recovery gate remains observable.
CONCEPT_WORKFLOW_DISABLED = True
CONCEPT_LOOP_IDS = frozenset({"concept-review", "concept-recheck"})
CONCEPT_LEGACY_WRITE_REASON = "旧 Control Plane 概念写接口已禁用；概念刷新仅可由 PM Scheduler 依赖事件交给 PM Worker，并受 Admission 门禁约束。"
CONCEPT_WORKFLOW_REASON = CONCEPT_LEGACY_WRITE_REASON
CONCEPT_RECOVERY_REASON = "V11 来源闭包、正文预检、Baseline 与 45 条 Hot/Publish 投影通过后，概念刷新由 PM Scheduler 在 weekly-sync-and-refresh 成功并落账 source_manifest 后依赖触发；Admission=disabled 仅阻止未批准写入和发布。"
CONCEPT_WORKFLOW_SCHEMA = "pm-loop.concept-workflow-status.v2"
CONCEPT_EXPECTED_COUNT = 45
FULL_INVENTORY_ONCE_LABEL = "com.zhujie14.shengsuan-concepts-full-inventory-once"
LOOPS = [
    {
        "id": "daily-radar",
        "title": "每日 PM 雷达",
        "permission_mode": "draft",
        "description": "读取最近事实，识别新事件、异常与未跟进事项",
        "executor": "codex-analysis-v2",
        "input_schema": [
            {"id": "time_range", "label": "时间范围", "type": "select", "options": ["最近 24 小时", "最近 7 天"], "default": "最近 24 小时"},
            {"id": "focus", "label": "关注范围", "type": "text", "placeholder": "全部客户、项目或指定主题"},
        ],
        "snapshot_sources": ["pm_timeline", "launchd", "skills", "openviking"],
        "analysis_instruction": "按新鲜度和影响排序，区分事实、风险信号与待跟进项。",
        "write_allowlist": ["draft/report.md"],
    },
    {
        "id": "requirement-fit",
        "title": "需求满足度",
        "permission_mode": "report",
        "description": "判断需求是否被现有能力与可追溯证据覆盖",
        "executor": "codex-analysis-v2",
        "input_schema": [
            {"id": "subject", "label": "客户 / 项目", "type": "text", "required": True, "placeholder": "输入客户或项目"},
            {"id": "requirement", "label": "需求文本", "type": "textarea", "required": True, "placeholder": "输入需要判断的一条需求"},
        ],
        "snapshot_sources": ["pm_timeline", "openviking"],
        "analysis_instruction": "逐项给出已覆盖、部分覆盖或未覆盖，并显式列出证据缺口。",
        "write_allowlist": ["draft/report.md"],
    },
    {
        "id": "delivery-risk",
        "title": "交付风险",
        "permission_mode": "draft",
        "description": "聚合延期、承诺、依赖与责任边界风险",
        "executor": "codex-analysis-v2",
        "input_schema": [
            {"id": "project", "label": "项目", "type": "text", "required": True, "placeholder": "输入交付项目"},
            {"id": "horizon_days", "label": "观察周期", "type": "number", "default": 14, "min": 1, "max": 90},
        ],
        "snapshot_sources": ["pm_timeline", "openviking"],
        "analysis_instruction": "按高、中、低风险分级，给出触发证据和仅限草稿的缓解建议。",
        "write_allowlist": ["draft/report.md"],
    },
    {
        "id": "weekly-review",
        "title": "每周复盘",
        "permission_mode": "draft",
        "description": "汇总本周运行、决定、反馈与未闭环事项",
        "executor": "codex-analysis-v2",
        "input_schema": [
            {"id": "week", "label": "复盘周期", "type": "select", "options": ["本周", "最近 7 天"], "default": "本周"},
            {"id": "topic", "label": "关注主题", "type": "text", "placeholder": "决策、交付、客户或全部"},
        ],
        "snapshot_sources": ["pm_timeline", "openviking", "runs"],
        "analysis_instruction": "区分已完成、未闭环、需要本人决定和下周草稿计划。",
        "write_allowlist": ["draft/report.md"],
    },
    {
        "id": "concept-review",
        "title": "概念历史（只读）",
        "permission_mode": "report",
        "description": "展示概念 Active、Candidate、证据和历史审核结果；自动刷新与发布已停用",
        "executor": "concept-status-observer",
        "action_url": "/concept-review",
        "disabled": True,
        "read_only": True,
        "history_only": True,
        "actionable": False,
        "status": "disabled",
        "input_schema": [],
        "snapshot_sources": ["concept_ledger", "candidates", "usage", "discovery"],
        "write_allowlist": [],
    },
    {
        "id": "concept-recheck",
        "title": "概念重检历史（只读）",
        "permission_mode": "report",
        "description": "展示既有概念重检与全量盘点历史；新的重检与盘点已停用",
        "executor": "concept-status-observer",
        "disabled": True,
        "read_only": True,
        "history_only": True,
        "actionable": False,
        "status": "disabled",
        # Retain the loop id for historical RunStore rows, but expose no
        # executable inputs.  New recheck/inventory requests are rejected at
        # the API boundary and can only be viewed through the history model.
        "input_schema": [],
        "snapshot_sources": ["concept_ledger", "sync_ledgers", "usage", "discovery"],
        "write_allowlist": [],
    },
]
REVIEW_ACTIONS = {"pause": "gate/paused", "changes": "gate/changes_requested", "approve": "gate/approved"}
QUEUE_TERMINAL = {"completed", "failed", "cancelled"}
# A terminal candidate is no longer actionable from the review queue.  Failed
# and stale proposals remain visible so the reviewer can understand or retry
# them instead of losing the only evidence trail.
CANDIDATE_REVIEW_TERMINAL = {"published", "rejected", "superseded"}
# Keep the stage contract stable for both current and legacy inventory runs.
# Older manifests predate stage-level telemetry and therefore need an explicit
# ``not_recorded`` projection instead of synthetic zeroes.
INVENTORY_STAGE_NAMES = (
    "document_read",
    "term_aggregation",
    "llm_reduce",
    "candidate_write",
)
# Metadata-only change signals are deliberately kept separate from the
# content/evidence baseline.  The weekly runner has used both names over its
# migration, so the read-only projection accepts either sidecar name.
NAME_HASH_RULE = "source+path+name:v1"
NAME_HASH_PREFIX = "namepath-v1:"
LEGACY_NAME_HASH_PREFIX = "sha256:"
NAME_HASH_FORMAT = "namepath-v1"
NAME_BASELINE_FILENAMES = (
    "weekly-source-revisions.json",
    "weekly-name-baseline.json",
    "name-baseline.json",
)
SOURCE_MANIFEST_FILENAMES = (
    "source-manifest.meta.json",
    "source-manifest.json",
    "source_manifest.json",
)
AUDIT_QUEUE_FILENAMES = (
    "content-audit-queue.meta.json",
    "content-audit-queue.json",
    "content_audit_queue.json",
)
# ``heartbeat_at`` is the queue item's lease heartbeat (with ``updated_at`` as
# a backward-compatible fallback for rows written before the field existed).
# Refresh workers are bounded to 15 minutes; a 30-minute lease leaves
# startup/write headroom while still recovering an orphaned worker after a
# service restart.
QUEUE_LEASE_SECONDS = 30 * 60
QUEUE_MAX_ATTEMPTS = 3
# Read paths are polled by the review UI.  A short process-local cache keeps
# concurrent requests from re-parsing hundreds of immutable Candidate files.
# The cache is source-signature aware: a runner write invalidates it
# immediately, so the TTL is only a burst coalescing bound and never a stale
# data window.
READ_CACHE_TTL_SECONDS = 1.0
# The overview page polls several read endpoints at once.  Serialize and
# reuse one complete snapshot for the short polling window so concurrent
# requests do not rebuild the same multi-megabyte projection in parallel.
SNAPSHOT_CACHE_TTL_SECONDS = 1.0
CONTROL_PLANE_SNAPSHOT_SCHEMA = "pm-loop.control-plane-snapshot.v3"
CONTROL_PLANE_SUMMARY_SCHEMA = "pm-loop.control-plane-summary.v3"
# The overview may ask Codex to refresh a stale source.  These requests are
# durable *intent* records only: they are deliberately kept outside RunStore
# so posting one can never start a runner or mutate the concept Active ledger.
CONTROL_PLANE_JOB_SCHEMA = "pm-loop.control-plane-job.v1"
CONTROL_PLANE_JOBS_SCHEMA = "pm-loop.control-plane-jobs.v1"
CONTROL_PLANE_JOB_LIMIT_DEFAULT = 50
CONTROL_PLANE_JOB_LIMIT_MAX = 200
CONTROL_PLANE_JOB_TITLE_MAX = 200
CONTROL_PLANE_JOB_INSTRUCTIONS_MAX = 12000
CONTROL_PLANE_JOB_SCOPE_MAX = 32_000
# A Control Plane job is only an intent record, but retired concept intents
# must not even be queued for a later worker/Codex turn.  Usage telemetry is
# deliberately excluded: it is a bounded append-only feedback stream and does
# not create Candidates, mutate Active, or start refresh work.
RETIRED_CONCEPT_JOB_MARKERS = (
    "shengsuan-concepts",
    "weekly-sync-and-refresh",
    "concept-review",
    "concept-recheck",
    "full_inventory",
    "full-inventory",
    "approved queue",
    "approved_queue",
    "candidate",
    "concept refresh",
    "概念刷新",
    "概念盘点",
    "概念重检",
    "需求评估",
    "requirement-fit",
    "requirement_fit",
)
# These jobs are runtime infrastructure rather than PM work.  They remain in
# the system-health-check inventory, but the PM scheduling view omits them so
# its plan and failure reasons stay focused on user-facing work.
SCHEDULE_DISPLAY_EXCLUDED_LABELS = frozenset(
    {
        "com.zhujie14.catchup",
        "com.zhujie14.openviking-server",
        "com.zhujie14.ov-memory-sync",
        "com.zhujie14.pm-loop-control-plane",
    }
)
# Candidate list responses are intentionally bounded when callers opt into the
# summary/pagination contract.  The legacy no-query endpoint remains backward
# compatible and can still request the full projection explicitly.
CANDIDATE_PAGE_SIZE_DEFAULT = 50
CANDIDATE_PAGE_SIZE_MAX = 500
CANDIDATE_SUMMARY_FIELDS = {
    "schema_version",
    "candidate_id",
    "concept",
    "kind",
    "base_version",
    "proposed_version",
    "base_page_sha256",
    "content_hash",
    "confidence",
    "status",
    "created_at",
    "updated_at",
    "source_refs",
    "reason",
    "content_available",
    "approved_by",
    "approved_at",
    "approved_content_hash",
    "approval_note",
    "approval_run_id",
    "reviewed_by",
    "reviewed_at",
    "review_note",
    "error",
    "failed_at",
    "publish_failed_at",
}


class ControlPlane:
    def __init__(
        self,
        state_dir: Path,
        adapter_script: Path,
        project_root: Path,
        codex_root: Path,
        web_root: Path,
        snapshot_path: Optional[Path] = None,
        evidence_project_root: Optional[Path] = None,
    ) -> None:
        self.store = RunStore(state_dir)
        self.project_root = project_root
        # The service executes from the immutable runtime mirror, while P3
        # review packages remain project-owned evidence. Keep those roots
        # explicit so the read model never silently substitutes one for the
        # other.
        self.evidence_project_root = evidence_project_root or project_root
        self.codex_root = codex_root
        self.web_root = web_root
        # V4.4 cockpit is an optional read model.  Keep the historical
        # Control Plane fully functional when the new coordination DB is not
        # deployed yet or is temporarily unreadable.
        self.v44_cockpit = CockpitReadModel.open_existing(
            self.store.state_dir / "state" / "pm-system.db",
            runtime_home=self.codex_root.parent,
            project_root=self.evidence_project_root,
        )
        retention_registry = self.evidence_project_root / "scripts" / "retention-source-registry.json"
        if not retention_registry.is_file():
            retention_registry = self.project_root / "config" / "retention-source-registry.json"
        if not retention_registry.is_file():
            retention_registry = Path(__file__).with_name("retention-source-registry.json")
        retention_schedule = self.evidence_project_root / "scripts" / "schedule-registry.json"
        if not retention_schedule.is_file():
            retention_schedule = self.project_root / "config" / "schedule-registry.json"
        if not retention_schedule.is_file():
            retention_schedule = Path(__file__).with_name("schedule-registry.json")
        self.retention_read_model = RetentionReadModel(
            state_root=self.store.state_dir / "state" / "retention",
            registry_path=retention_registry,
            schedule_registry_path=retention_schedule,
            db_path=self.store.state_dir / "state" / "pm-system.db",
        )
        self.artifact_registry_read_model = ArtifactRegistryReadModel(
            project_root=self.evidence_project_root,
            inventory_root=self.evidence_project_root / "state" / "pm-loop" / "artifact-inventory",
        )
        self.coordination_store: Optional[PMSystemStore] = None
        self.coordination_read_store: Optional[PMSystemStore] = None
        coordination_path = self.store.state_dir / "state" / "pm-system.db"
        if coordination_path.is_file():
            try:
                # Keep the compatibility writer available for explicit Codex
                # commands, but make all Control Plane read paths use a
                # separate SQLite read-only connection.  In particular, a
                # dashboard GET must never request journal_mode=WAL.
                self.coordination_store = PMSystemStore(coordination_path, auto_migrate=False)
                self.coordination_read_store = PMSystemStore(coordination_path, auto_migrate=False, read_only=True)
            except StoreUnavailable:
                self.coordination_store = None
                self.coordination_read_store = None
        self.adapter_script = adapter_script
        self.snapshot_path = snapshot_path
        self.processes: Dict[str, subprocess.Popen[str]] = {}
        self.lock = threading.Lock()
        self.review_lock = threading.RLock()
        self.recheck_lock = threading.Lock()
        self.queue_lock = threading.RLock()
        self.learning = ConceptLearningStore(self.concepts_root)
        self.queue_path = self.store.state_dir / "concept-review" / "approved-queue.json"
        self.queue_file_lock_path = self.store.state_dir / "concept-review" / ".approved-queue.lock"
        self.review_file_lock_path = self.store.state_dir / "concept-review" / ".staged.lock"
        self.queue_wakeup = threading.Event()
        # The queue object is retained as an audit projection only.  No worker
        # thread is ever started after the concept workflow retirement.
        self.queue_thread = None
        self._read_cache_lock = threading.RLock()
        self._candidate_read_cache: tuple[float, Any, list[Dict[str, Any]]] | None = None
        self._projection_cache: Dict[tuple[Any, ...], tuple[float, Dict[str, Any]]] = {}
        self._snapshot_cache_lock = threading.RLock()
        self._snapshot_cache: tuple[float, Optional[str], Dict[str, Any]] | None = None
        # Concept state is owned by the shengsuan-concepts runner.  The
        # Control Plane only projects its files and never repairs/requeues them.
        self._recover_action_runs()

    @staticmethod
    def _truthy_env(name: str) -> bool:
        return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "on", "enabled", "yes"}

    def coordination_enabled(self) -> bool:
        """Return whether new Run writes use the V4.4 coordination store.

        The explicit flag keeps isolated legacy tests and rollback copies
        compatible while the deployed LaunchAgent opts into the new writer.
        """
        return self.coordination_store is not None and (
            self._truthy_env("PM_V44_COORDINATION_ACTIVE")
            or str(os.environ.get("PM_V44_COORDINATION_ACTIVE", "")).strip().lower() == "canary"
        )

    def coordination_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        reader = self.coordination_read_store
        if not self.coordination_enabled() or reader is None:
            return None
        return reader.get_run(str(run_id))

    def coordination_artifact_root(self) -> Path:
        return self.store.state_dir / "runs"

    def coordination_run_detail(self, run_id: str) -> Dict[str, Any]:
        if self.coordination_read_store is None:
            raise KeyError(run_id)
        cockpit = self.v44_cockpit or CockpitReadModel(
            self.coordination_read_store,
            runtime_home=self.codex_root.parent,
            project_root=self.evidence_project_root,
        )
        return cockpit.run_detail(str(run_id))

    def coordination_events(self, run_id: str) -> list[Dict[str, Any]]:
        """Project coordination events into the legacy SSE event contract."""
        reader = self.coordination_read_store
        if not self.coordination_enabled() or reader is None:
            raise KeyError(run_id)
        if reader.get_run(str(run_id)) is None:
            raise KeyError(run_id)
        values = []
        for event in reader.list_events(str(run_id), after_seq=0, limit=5000):
            values.append(
                {
                    "schema_version": "pm-loop.event.v1",
                    "run_id": str(run_id),
                    "seq": int(event["seq"]),
                    "at": event["occurred_at"],
                    "type": event["event_type"],
                    "actor": event["actor"],
                    "data": event.get("payload") or {},
                    "visibility": "user",
                }
            )
        return values

    @staticmethod
    def _as_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _read_json_mapping(path: Path) -> tuple[Dict[str, Any], Optional[str]]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {}, f"{type(exc).__name__}: {exc}"
        if not isinstance(value, dict):
            return {}, "JSON root must be an object"
        return value, None

    def _concept_recovery_evidence(self) -> Dict[str, Any]:
        """Read the V11 recovery gate without opening a writable DB handle."""
        state_root = self.store.state_dir / "state" / "concept-v11"
        coverage_path = state_root / "source-coverage-current.json"
        preflight_path = state_root / "content-source-preflight-current.json"
        baseline_path = state_root / "baseline-roll-20260903.json"
        database_path = self.store.state_dir / "state" / "pm-system.db"
        coverage, coverage_error = self._read_json_mapping(coverage_path)
        preflight, preflight_error = self._read_json_mapping(preflight_path)
        baseline, baseline_error = self._read_json_mapping(baseline_path)

        expected = self._as_int(coverage.get("expected_concept_count")) or CONCEPT_EXPECTED_COUNT
        coverage_counts = coverage.get("concept_status_counts") if isinstance(coverage.get("concept_status_counts"), dict) else {}
        preflight_summary = preflight.get("summary") if isinstance(preflight.get("summary"), dict) else {}
        member_count = len(baseline.get("members") or []) if isinstance(baseline.get("members"), list) else 0
        generation_id = str(baseline.get("generation_id") or "")
        coverage_ok = (
            coverage.get("status") == "PASS"
            and self._as_int(coverage.get("concept_count")) == expected
            and self._as_int(coverage_counts.get("refreshable")) == expected - 1
            and self._as_int(coverage_counts.get("retired_with_evidence")) == 1
            and self._as_int(coverage_counts.get("needs_repair")) == 0
            and not coverage.get("validation_errors")
        )
        preflight_ok = (
            preflight.get("status") == "PASS"
            and self._as_int(preflight.get("expected_concept_count")) == expected
            and self._as_int(preflight_summary.get("ready")) == expected - 1
            and self._as_int(preflight_summary.get("retired_excluded")) == 1
            and self._as_int(preflight_summary.get("needs_source_rebuild")) == 0
            and self._as_int(preflight_summary.get("blocked")) == 0
        )
        baseline_ok = baseline.get("status") == "APPLIED" and member_count == expected and bool(generation_id)
        projection: Dict[str, Any] = {"status": "missing", "database_path": str(database_path), "generation_id": generation_id or None}
        dependency: Dict[str, Any] = {"status": "missing"}
        projection_ok = False
        if database_path.is_file() and generation_id:
            try:
                connection = sqlite3.connect(f"file:{database_path.resolve()}?mode=ro", uri=True, timeout=1)
                connection.row_factory = sqlite3.Row
                tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
                required = {"concept_admissions", "generations", "concept_hot_projection", "concept_publish_ledger"}
                if not required <= tables:
                    projection.update({"status": "schema_missing", "missing_tables": sorted(required - tables)})
                else:
                    admission = connection.execute(
                        "SELECT admission_state,version,policy_version,evidence_hash,observed_at FROM concept_admissions ORDER BY version DESC,updated_at DESC LIMIT 1"
                    ).fetchone()
                    generation = connection.execute(
                        "SELECT generation_id,status,active_at FROM generations WHERE generation_id=? AND domain='concepts'",
                        (generation_id,),
                    ).fetchone()
                    hot = connection.execute(
                        "SELECT count(DISTINCT concept_id) AS concepts,count(*) AS rows FROM concept_hot_projection WHERE generation_id=? AND projection_state='active'",
                        (generation_id,),
                    ).fetchone()
                    publish = connection.execute(
                        "SELECT count(DISTINCT concept_id) AS concepts,count(*) AS rows FROM concept_publish_ledger WHERE current_generation=? AND current_hot_generation=? AND projection_state='active'",
                        (generation_id, generation_id),
                    ).fetchone()
                    projection.update(
                        {
                            "status": "checked",
                            "admission": dict(admission) if admission else None,
                            "generation": dict(generation) if generation else None,
                            "hot_projection": dict(hot) if hot else {"concepts": 0, "rows": 0},
                            "publish_projection": dict(publish) if publish else {"concepts": 0, "rows": 0},
                        }
                    )
                    if "scheduled_dependency_events" in tables:
                        latest = connection.execute(
                            "SELECT event_id,dependent_schedule_key,upstream_schedule_key,upstream_run_id,status,occurrence_id,created_at,consumed_at,updated_at "
                            "FROM scheduled_dependency_events WHERE dependent_schedule_key='concept-refresh-planner' AND status='consumed' "
                            "ORDER BY consumed_at DESC,updated_at DESC LIMIT 1"
                        ).fetchone()
                        dependency = {"status": "consumed" if latest else "not_observed", "latest_consumed": dict(latest) if latest else None}
                    admission_state = str(admission["admission_state"] if admission else "")
                    projection_ok = (
                        admission is not None
                        and admission_state in {"disabled", "canary", "incremental"}
                        and generation is not None
                        and str(generation["status"]) == "active"
                        and self._as_int(hot["concepts"] if hot else 0) == expected
                        and self._as_int(hot["rows"] if hot else 0) == expected
                        and self._as_int(publish["concepts"] if publish else 0) == expected
                        and self._as_int(publish["rows"] if publish else 0) == expected
                    )
                connection.close()
            except sqlite3.Error as exc:
                projection.update({"status": "query_failed", "error": f"{type(exc).__name__}: {exc}"})

        read_errors = [
            name
            for name, error in (("coverage", coverage_error), ("content_preflight", preflight_error), ("baseline", baseline_error))
            if error
        ]
        all_ready = coverage_ok and preflight_ok and baseline_ok and projection_ok and not read_errors
        admission = projection.get("admission") if isinstance(projection.get("admission"), dict) else {}
        admission_state = str(admission.get("admission_state") or "")
        return {
            "expected_concept_count": expected,
            "ready": all_ready,
            "admission_state": admission_state or None,
            "coverage": {
                "status": coverage.get("status"),
                "report_hash": coverage.get("report_hash"),
                "concept_count": coverage.get("concept_count"),
                "concept_status_counts": dict(coverage_counts),
                "validation_errors": coverage.get("validation_errors", []),
            },
            "content_preflight": {
                "status": preflight.get("status"),
                "coverage_report_hash": preflight.get("coverage_report_hash"),
                "expected_concept_count": preflight.get("expected_concept_count"),
                "summary": dict(preflight_summary),
            },
            "baseline": {
                "status": baseline.get("status"),
                "generation_id": generation_id or None,
                "member_count": member_count,
                "coverage_report_hash": baseline.get("coverage_report_hash"),
                "applied_at": baseline.get("applied_at"),
            },
            "projection": projection,
            "dependency": dependency,
            "read_errors": read_errors,
        }

    def _concept_write_rejected(self) -> None:
        raise PermissionError(CONCEPT_LEGACY_WRITE_REASON)

    def concept_write_response(self, *, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """Describe a rejected legacy mutation without misreporting system state."""
        value = self.concept_workflow_status()
        value.update(
            {
                "error": "concept_owned_runner_only",
                "code": "legacy_concept_control_plane_write_api_disabled",
                "message": CONCEPT_LEGACY_WRITE_REASON,
                "endpoint": endpoint,
            }
        )
        return value

    def concept_workflow_status(self) -> Dict[str, Any]:
        """Project the V11 recovery gate; legacy Control Plane writes stay blocked."""
        evidence = self._concept_recovery_evidence()
        admission_state = evidence.get("admission_state")
        ready = bool(evidence.get("ready"))
        status = "recovery_gated" if ready and admission_state == "disabled" else "ready" if ready else "attention"
        reason = CONCEPT_RECOVERY_REASON if status == "recovery_gated" else (
            "概念恢复证据不完整或不一致；PM Scheduler 仍保持 fail-closed，直到来源、正文、Baseline 和投影门禁恢复一致。"
        )
        return {
            "schema_version": CONCEPT_WORKFLOW_SCHEMA,
            "disabled": False,
            "read_only": True,
            "history_only": False,
            "status": status,
            "reason": reason,
            "execution": "pm_scheduler_dependency",
            "refresh_trigger": "pm_scheduler_dependency",
            "schedule_chain": ["weekly-sync-and-refresh", "concept-refresh-planner"],
            "admission": {
                "state": admission_state,
                "blocks_unapproved_writes": admission_state == "disabled",
                "blocks_unapproved_publish": admission_state == "disabled",
                "does_not_mean_workflow_retired": True,
            },
            "evidence": evidence,
            "legacy_control_plane_write_apis": {
                "disabled": True,
                "code": "legacy_concept_control_plane_write_api_disabled",
                "reason": CONCEPT_LEGACY_WRITE_REASON,
            },
            "candidate_writes": False,
            "active_writes": False,
            "publish_writes": False,
            "inventory_writes": False,
            "usage_telemetry": True,
            "usage_writes": True,
        }

    @staticmethod
    def _is_retired_concept_job(payload: Mapping[str, Any]) -> bool:
        """Return whether a handoff intent belongs to a retired path."""
        try:
            scope = json.dumps(payload.get("scope") or {}, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            scope = str(payload.get("scope") or "")
        haystack = " ".join(
            str(payload.get(key) or "")
            for key in ("title", "instructions", "kind", "source")
        ) + " " + scope
        haystack = haystack.casefold()
        return any(marker.casefold() in haystack for marker in RETIRED_CONCEPT_JOB_MARKERS)

    def _recover_action_runs(self) -> None:
        """Resume approved generic safe-draft actions after a service restart."""
        for state in self.store.list_states_read_only():
            if state.get("loop_id") in CONCEPT_LOOP_IDS:
                # Concept actions belong to shengsuan-concepts.  A stale
                # Control Plane process must never resume their writes.
                continue
            if state.get("status") in {"action_queued", "executing"}:
                try:
                    self.start_action_runner(str(state.get("run_id")))
                except (OSError, ValueError):
                    continue

    def _recover_orphaned_rechecks(self) -> None:
        """Fail non-terminal rechecks whose one-shot process owner was lost."""
        if CONCEPT_WORKFLOW_DISABLED:
            # Historical non-terminal rows must remain untouched.  They are
            # displayed as historical records, never repaired or failed here.
            return
        active_statuses = {"queued", "running", "collecting", "analyzing", "verifying", "awaiting_human"}
        for state in self.store.list_states_read_only():
            if state.get("loop_id") != "concept-recheck" or state.get("status") not in active_statuses:
                continue
            run_id = str(state.get("run_id") or "")
            if not run_id:
                continue
            self.store.append(run_id, "agent/failed", {"error": "Control Plane restarted and lost the one-shot recheck process owner", "recoverable": True}, actor="startup-recovery")
            self.store.append(run_id, "run/failed", {"error": "orphaned concept recheck after Control Plane restart", "recoverable": True}, actor="startup-recovery")

    @property
    def concepts_root(self) -> Path:
        return self.codex_root / "skills" / "shengsuan-concepts"

    @property
    def concepts_ledger_path(self) -> Path:
        return self.concepts_root / "state" / "concepts-ledger.json"

    @property
    def launch_agents_root(self) -> Path:
        return self.codex_root.parent / "Library" / "LaunchAgents"

    def health_report_path(self) -> Optional[Path]:
        html_report = self._latest_file(
            self.evidence_project_root / "docs",
            self.evidence_project_root / "docs" / "系统健康巡检报告-*.html",
        )
        if html_report:
            return html_report
        return self._latest_file(
            self.codex_root / "skills" / "system-health-check" / "state",
            self.codex_root / "skills" / "system-health-check" / "state" / "health-check-*.md"
        )

    def health_report_projection(self) -> Dict[str, Any]:
        """Expose the latest private health report without starting a check."""
        report_path = self.health_report_path()
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        if report_path is None:
            return {
                "available": False,
                "url": None,
                "name": None,
                "updated_at": None,
                "generated_date": None,
                "generated_today": False,
                "status": "missing",
                "time_basis": "报告文件名或正文中的显式生成日期",
                "reason": "今日尚未产生巡检报告，当前没有可打开的历史报告。",
            }

        updated_at = self._file_updated_at(report_path)
        generated_date, time_basis = self._report_generated_date(report_path)
        generated_today = generated_date == today
        return {
            "available": True,
            "url": "/health-report",
            "name": report_path.name,
            "updated_at": updated_at,
            "generated_date": generated_date,
            "generated_today": generated_today,
            "status": "fresh_today" if generated_today else "outdated" if generated_date else "unknown_date",
            "time_basis": time_basis,
            "reason": (
                "已找到今日巡检报告。"
                if generated_today
                else (
                    f"今日尚未产生巡检报告；最新报告生成日期为 {generated_date}。"
                    if generated_date
                    else "已找到历史报告，但未记录可验证的生成日期，无法判断是否为今日报告。"
                )
            ),
        }

    @staticmethod
    def _report_generated_date(path: Path) -> tuple[Optional[str], str]:
        """Read an explicit report date without treating ``mtime`` as fact."""
        match = re.search(r"(?<!\d)(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)(?!\d)", path.name)
        if match:
            try:
                return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date().isoformat(), "报告文件名日期"
            except ValueError:
                pass
        try:
            # A report's date is normally in the title/context block.  This
            # bounded read never interprets document text as instructions.
            prefix = path.read_bytes()[:64 * 1024].decode("utf-8", errors="ignore")
        except OSError:
            return None, "未记录"
        match = re.search(r"(?:生成(?:时间|于)?|日期|报告日期)\s*[:：]?\s*(20\d{2})[-/.年]([01]?\d)[-/.月]([0-3]?\d)", prefix)
        if match:
            try:
                return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date().isoformat(), "报告正文显式日期"
            except ValueError:
                pass
        return None, "未记录"

    def pm_timeline_review_path(self) -> Optional[Path]:
        """Return the newest weekly (ISO ``YYYY-Www``) review artifact.

        Monthly reviews share the directory but are not the output of the
        ``pm-timeline-weekly`` LaunchAgent, so keep them out of this route.
        """
        root = self.evidence_project_root / "docs" / "reviews"
        return self._latest_file(root, root / "????-W??-review.html")

    @staticmethod
    def _file_updated_at(path: Optional[Path]) -> Optional[str]:
        if path is None:
            return None
        try:
            return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(
                microsecond=0
            ).isoformat().replace("+00:00", "Z")
        except OSError:
            return None

    def pm_timeline_review_output(self) -> Dict[str, Any]:
        """Expose the latest weekly review as a same-origin read-only artifact."""
        path = self.pm_timeline_review_path()
        available = bool(path and path.is_file())
        return {
            "available": available,
            "url": "/reports/pm-timeline/latest" if available else None,
            "path": str(path) if available else None,
            "updated_at": self._file_updated_at(path) if available else None,
            "name": path.name if available and path else None,
        }

    def role_output_path(self, output_id: str) -> Optional[Path]:
        """Resolve one allowlisted historical role artifact for same-origin viewing."""
        if self.v44_cockpit is None:
            return None
        return self.v44_cockpit.role_output_path(output_id)

    @staticmethod
    def _safe_regular_file(path: Path, root: Path) -> Optional[Path]:
        """Return a regular file inside ``root`` without traversing symlinks.

        Browser-facing routes must reject a symlink even when its current
        target remains inside the allowlisted root.  That keeps the contract
        stable if the link target is later replaced, and prevents an opaque
        identifier or a persisted pointer from becoming a path escape.
        """
        try:
            lexical_root = root.absolute()
            lexical_path = path.absolute()
            relative = lexical_path.relative_to(lexical_root)
            current = lexical_root
            if current.is_symlink():
                return None
            resolved_root = root.resolve(strict=True)
            for part in relative.parts:
                current = current / part
                if current.is_symlink():
                    return None
            if not lexical_path.is_file():
                return None
            resolved = lexical_path.resolve(strict=True)
            resolved.relative_to(resolved_root)
            # Retain the lexical path after validation.  A later serving
            # boundary rechecks it, so a replaced symlink remains detectable.
            return lexical_path
        except (OSError, RuntimeError, ValueError):
            return None

    def review_artifact_path(self, run_id: str) -> Optional[Path]:
        """Resolve the task package for one review without exposing arbitrary paths."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", str(run_id or "")):
            return None
        root = self.coordination_artifact_root()
        return self._safe_regular_file(root / run_id / "task-package.v1.json", root)

    def concept_artifact_path(self, kind: str) -> Optional[Path]:
        """Resolve one fixed P3 concept evidence artifact."""
        root = self.codex_root
        candidates = {
            "coverage": root / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json",
            "candidates": root / "pm-loop" / "runs" / "concept-v11" / "p3-source-candidates-current-coverage.json",
        }
        if kind in {"review-package", "review-package-json"}:
            package_root = self.evidence_project_root / "docs" / "03-产品架构"
            try:
                extension = "json" if kind == "review-package-json" else "html"
                paths = sorted(package_root.glob(f"概念自动刷新-P3来源处置决策工作包-*.{extension}"), key=lambda item: item.stat().st_mtime, reverse=True)
                if not paths and kind == "review-package":
                    paths = sorted(package_root.glob("概念自动刷新-P3来源处置决策工作包-*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
            except OSError:
                paths = []
            return self._safe_regular_file(paths[0], package_root) if paths else None
        path = candidates.get(kind)
        return self._safe_regular_file(path, root) if path else None

    def retention_artifact_path(self, kind: str) -> Optional[Path]:
        """Resolve a Retention observer artifact through its signed pointer."""
        root = self.store.state_dir / "state" / "retention"
        pointer_path = self._safe_regular_file(root / "latest-observer.json", root)
        if pointer_path is None:
            return None
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            result_ref = pointer.get("result")
            result_path = self._safe_regular_file(root / str(result_ref), root)
            if result_path is None:
                return None
            result = json.loads(result_path.read_text(encoding="utf-8"))
            refs = {"observer": result_ref, **(result.get("artifacts") or {})}
            ref = refs.get(kind)
            if not isinstance(ref, str) or not ref.endswith(".json"):
                return None
            return self._safe_regular_file(root / ref, root)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def retention_artifact_projection(self) -> Dict[str, Any]:
        """Return only browser-facing URLs for the latest observer artifacts."""
        labels = {
            "observer": "观察结果",
            "inventory": "来源盘点",
            "unknowns": "无法处理项",
            "plan": "回收计划",
            "result": "观察结果",
        }
        output: Dict[str, Any] = {}
        for kind in ("observer", "inventory", "unknowns", "plan"):
            path = self.retention_artifact_path(kind)
            if path is not None:
                output[kind] = {"available": True, "url": f"/reports/retention/{kind}", "name": path.name, "label": labels[kind]}
        return output

    def domain_report_path(self, domain: str) -> Optional[Path]:
        """Return the newest HTML artifact for a report domain.

        The browser-facing report routes are intentionally HTML-only. Markdown
        remains an observed source fallback for freshness and audit details,
        but it must never be advertised as an openable report.
        """
        return self._domain_report_path(domain, html_only=True)

    def domain_report_source_path(self, domain: str) -> Optional[Path]:
        """Return the newest observed report source, preferring HTML."""
        return self._domain_report_path(domain, html_only=False)

    def domain_report_projection(self, domain: str) -> Dict[str, Any]:
        """Expose one report's source and browser-facing HTML availability."""
        source = self.domain_report_source_path(domain)
        html = self.domain_report_path(domain)
        return {
            "source": str(source) if source else None,
            "available": bool(html and html.is_file()),
            "url": f"/reports/{domain}/latest" if html else None,
            "html_path": str(html) if html else None,
            "updated_at": self._file_updated_at(source) if source else None,
        }

    def _domain_report_path(self, domain: str, *, html_only: bool) -> Optional[Path]:
        patterns = self._domain_report_patterns(domain)
        extensions = ("html",) if html_only else ("html", "md")
        for extension in extensions:
            for pattern in patterns.get(extension, ()):
                path = self._latest_file(self.evidence_project_root / "docs", pattern)
                if path:
                    return path
        return None

    def _domain_report_patterns(self, domain: str) -> Dict[str, tuple[Path, ...]]:
        if domain == "gaps":
            return {
                "html": (
                    self.evidence_project_root / "docs" / "产品缺口周报" / "产品缺口与安排建议-*.html",
                    self.evidence_project_root / "docs" / "DataBuilder产品缺口与安排建议-*.html",
                ),
                "md": (
                    self.evidence_project_root / "docs" / "产品缺口周报" / "产品缺口与安排建议-*.md",
                    self.evidence_project_root / "docs" / "DataBuilder产品缺口与安排建议-*.md",
                ),
            }
        if domain == "materials":
            return {
                "html": (
                    self.evidence_project_root / "docs" / "04-产品设计" / "资料缺失周报" / "胜算产品资料缺失周报-*.html",
                    self.evidence_project_root / "docs" / "04-产品设计" / "胜算产品资料缺失分析与建议*.html",
                    self.evidence_project_root / "docs" / "04-产品设计" / "基本概念-资料评审意见-*.html",
                ),
                "md": (
                    self.evidence_project_root / "docs" / "04-产品设计" / "资料缺失周报" / "胜算产品资料缺失周报-*.md",
                    self.evidence_project_root / "docs" / "04-产品设计" / "基本概念-资料评审意见-*.md",
                ),
            }
        return {"html": (), "md": ()}

    def _domain_report_freshness_spec(self, domain: str, path: Optional[Path]) -> tuple[str, str]:
        """Return a stable display label and signature glob for a report source."""
        patterns = self._domain_report_patterns(domain)
        suffix = path.suffix.lower().lstrip(".") if path else "html"
        candidates = patterns.get(suffix, ())
        label = str(candidates[0].relative_to(self.evidence_project_root)) if candidates else f"{domain} report"
        return label, f"*.{suffix}"

    def competitive_radar_read_model(self) -> CompetitiveRadarReadModel:
        """Return the radar projection backed by the PM system singleton."""
        return CompetitiveRadarReadModel(
            db_path=self.store.state_dir / "state" / "pm-system.db",
            state_root=self.store.state_dir / "state" / "competitive-radar",
            project_root=self.evidence_project_root,
        )

    def competitive_radar_report_path(self) -> Optional[Path]:
        pointer = self.competitive_radar_read_model().latest()
        if not pointer:
            return None
        path = Path(str(pointer.get("html_uri") or "")).expanduser()
        # The database pointer is evidence, not authorization to serve an
        # arbitrary local file.  Competitive reports are generated only below
        # this project-owned root and must still be regular non-symlink files.
        return self._safe_regular_file(
            path,
            self.evidence_project_root / "docs" / "产品情报监控" / "竞品雷达",
        )

    @property
    def review_state_path(self) -> Path:
        return self.store.state_dir / "concept-review" / "staged.json"

    @property
    def control_plane_jobs_path(self) -> Path:
        """Append-only intent log used by the display layer's Codex handoff.

        This is intentionally separate from ``RunStore``.  A job request is a
        durable handoff for a later Codex turn, not an executable PM Loop run;
        keeping it in its own file makes that boundary auditable and prevents
        a Control Plane restart from accidentally starting business work.
        """
        return self.store.state_dir / "control-plane" / "jobs.jsonl"

    @property
    def control_plane_jobs_lock_path(self) -> Path:
        return self.control_plane_jobs_path.with_name(".jobs.lock")

    @staticmethod
    def _control_plane_job_now() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _read_control_plane_jobs(self, *, limit: int = CONTROL_PLANE_JOB_LIMIT_MAX) -> list[Dict[str, Any]]:
        """Read the newest intent records without repairing or executing them."""
        path = self.control_plane_jobs_path
        if not path.is_file():
            return []
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return []
        rows: list[Dict[str, Any]] = []
        # The log is append-only and newest records are at the end.  Read only
        # the bounded tail so an old, large history cannot inflate every GET.
        for line in reversed(lines[-max(1, min(int(limit), CONTROL_PLANE_JOB_LIMIT_MAX)):]):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
        return rows

    def control_plane_jobs(self, *, limit: int = CONTROL_PLANE_JOB_LIMIT_DEFAULT, status: str = "") -> Dict[str, Any]:
        """Return recent Codex handoff intents and the source version observed."""
        try:
            bounded_limit = max(1, min(int(limit), CONTROL_PLANE_JOB_LIMIT_MAX))
        except (TypeError, ValueError):
            bounded_limit = CONTROL_PLANE_JOB_LIMIT_DEFAULT
        source_signatures_before = self._control_plane_source_signatures()
        source_version_before = self._source_version(source_signatures_before)
        rows = self._read_control_plane_jobs(limit=CONTROL_PLANE_JOB_LIMIT_MAX)
        source_signatures_after = self._control_plane_source_signatures()
        source_version_after = self._source_version(source_signatures_after)
        # A concurrent POST may append while this GET is reading.  Re-read the
        # bounded tail once so the returned rows correspond to the version we
        # advertise; if a producer is still writing, expose that fact instead
        # of pretending the projection is an atomic snapshot.
        read_consistency = "consistent"
        if source_version_before != source_version_after:
            retry_version_before = source_version_after
            rows = self._read_control_plane_jobs(limit=CONTROL_PLANE_JOB_LIMIT_MAX)
            source_signatures_after = self._control_plane_source_signatures()
            source_version_after = self._source_version(source_signatures_after)
            read_consistency = "consistent" if retry_version_before == source_version_after else "changed_during_read"
        status_filter = str(status or "").strip().casefold()
        if status_filter and status_filter not in {"all", "*"}:
            rows = [row for row in rows if str(row.get("status") or "").casefold() == status_filter]
        projected_rows: list[Dict[str, Any]] = []
        retired_count = 0
        for row in rows:
            if not self._is_retired_concept_job(row):
                projected_rows.append(row)
                continue
            retired_count += 1
            value = dict(row)
            value["raw_status"] = row.get("status")
            value["status"] = "history_only"
            value["display_status"] = "历史保留"
            value["disabled"] = True
            value["history_only"] = True
            value["actionable"] = False
            value["execution_started"] = False
            value["active_mutation"] = False
            value["worker"] = None
            projected_rows.append(value)
        rows = projected_rows
        total = len(rows)
        rows = rows[:bounded_limit]
        signatures = source_signatures_after
        version = source_version_after
        read_at = self._control_plane_job_now()
        return {
            "schema_version": CONTROL_PLANE_JOBS_SCHEMA,
            "read_only": True,
            "intent_only": True,
            "disabled": retired_count > 0,
            "history_only": retired_count > 0,
            "retired_count": retired_count,
            "actionable": sum(1 for row in rows if row.get("actionable", True) and row.get("status") not in {"history_only", "disabled"}),
            "read_at": read_at,
            "read_consistency": read_consistency,
            "version": version,
            "source_version": version,
            "source_signatures": signatures,
            "requests": rows,
            # ``jobs`` is the frontend-friendly alias; ``requests`` names the
            # persisted contract explicitly for API consumers.
            "jobs": rows,
            "pagination": {
                "limit": bounded_limit,
                "total": total,
                "has_more": total > bounded_limit,
                "status": status_filter or "all",
            },
        }

    def create_control_plane_job(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Persist a Codex intent without starting a worker or changing Active.

        Only the small, explicit handoff fields are retained.  In particular,
        arbitrary request keys are not passed to a runner, and no ``RunStore``
        request/event is created here.
        """
        payload = payload if isinstance(payload, dict) else {}
        title = str(payload.get("title") or "").strip()
        instructions = str(payload.get("instructions") or "").strip()
        if not title:
            raise ValueError("title is required")
        if not instructions:
            raise ValueError("instructions is required")
        if len(title) > CONTROL_PLANE_JOB_TITLE_MAX:
            raise ValueError(f"title exceeds {CONTROL_PLANE_JOB_TITLE_MAX} characters")
        if len(instructions) > CONTROL_PLANE_JOB_INSTRUCTIONS_MAX:
            raise ValueError(f"instructions exceeds {CONTROL_PLANE_JOB_INSTRUCTIONS_MAX} characters")

        if self._is_retired_concept_job(payload):
            response = self.concept_write_response(endpoint="/api/control-plane/jobs")
            response.update(
                {
                    "status": "rejected",
                    "history_only": True,
                    "intent_only": False,
                    "actionable": False,
                    "pending": 0,
                    "execution_started": False,
                    "active_mutation": False,
                    "worker": None,
                    "job": None,
                    "requested_title": title,
                    "requested_instructions": instructions,
                    "message": "旧 Control Plane 概念写入口不接收此意图；请由 PM Scheduler 的依赖链路和 Admission 门禁处理。",
                }
            )
            return response

        scope = payload.get("scope")
        if scope is None:
            scope = {}
        try:
            encoded_scope = json.dumps(scope, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError("scope must be JSON-serializable") from exc
        if len(encoded_scope.encode("utf-8")) > CONTROL_PLANE_JOB_SCOPE_MAX:
            raise ValueError(f"scope exceeds {CONTROL_PLANE_JOB_SCOPE_MAX} bytes")

        created_at = self._control_plane_job_now()
        job_id = f"job-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
        job: Dict[str, Any] = {
            "schema_version": CONTROL_PLANE_JOB_SCHEMA,
            "job_id": job_id,
            "status": "waiting_codex",
            "title": title,
            "instructions": instructions,
            "scope": scope,
            "source": str(payload.get("source") or "pm-loop-control-plane-v3")[:200],
            "kind": str(payload.get("kind") or "control_plane_request")[:100],
            "requested_at": str(payload.get("requested_at") or created_at)[:100],
            "created_at": created_at,
            "actor": "zhujie14",
            "intent_only": True,
            "execution_started": False,
            "active_mutation": False,
            "worker": None,
        }
        path = self.control_plane_jobs_path
        lock_path = self.control_plane_jobs_lock_path
        path.parent.mkdir(parents=True, exist_ok=True)
        # fcntl protects concurrent Control Plane processes; append+fsync makes
        # the handoff visible to the next Codex turn even after a crash.
        with lock_path.open("a+", encoding="utf-8") as lock_stream:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
            try:
                with path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(job, ensure_ascii=False, separators=(",", ":")) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            finally:
                fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)

        response = self.control_plane_jobs(limit=CONTROL_PLANE_JOB_LIMIT_DEFAULT)
        response.update({
            "job_id": job_id,
            "status": job["status"],
            "job": job,
            "message": "意图已记录，等待 Codex 领取；未启动 worker，未修改 Active 概念。",
        })
        return response

    def _read_staged(self) -> Dict[str, Any]:
        if not self.review_state_path.is_file():
            return {}
        try:
            value = json.loads(self.review_state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _write_staged(self, value: Dict[str, Any]) -> None:
        self.review_state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.review_state_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        temp_path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp_path.replace(self.review_state_path)

    def _read_candidates_cached(self) -> list[Dict[str, Any]]:
        """Return Candidate manifests from one short-lived read snapshot.

        The files are still the source of truth.  This is only a bounded read
        cache for the display layer, so an external runner update is visible
        on the next one-second poll without making the UI wait for 457 JSON
        parses on every parallel request.
        """
        now = time.monotonic()
        # Candidate manifests are runner-owned files.  Check their cheap
        # metadata signature on every request so an atomic runner replace is
        # visible immediately, even when it happens inside the one-second
        # burst-coalescing window.
        source_signature = self._path_signature(self.learning.candidates_root)
        with self._read_cache_lock:
            cached = self._candidate_read_cache
            if (
                cached is not None
                and now - cached[0] < READ_CACHE_TTL_SECONDS
                and cached[1] == source_signature
            ):
                return cached[2]
            rows = self.learning.list_candidates()
            self._candidate_read_cache = (now, source_signature, rows)
            # Candidate status/content changes should never reuse an old
            # derived projection after the read cache expires.
            self._projection_cache.clear()
            return rows

    @staticmethod
    def _active_projection_key(active_rows: Iterable[Dict[str, Any]]) -> tuple[Any, ...]:
        return tuple(
            sorted(
                (
                    str(row.get("name") or ""),
                    tuple(sorted(str(alias) for alias in (row.get("aliases") or []))),
                )
                for row in active_rows
            )
        )

    @contextmanager
    def _review_transaction(self) -> Iterator[None]:
        """Serialize staged-review transactions across threads and processes."""
        with self.review_lock:
            self.review_file_lock_path.parent.mkdir(parents=True, exist_ok=True)
            with self.review_file_lock_path.open("a+", encoding="utf-8") as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def _review_candidate(self, name: str) -> Optional[Dict[str, Any]]:
        """Return the newest actionable proposal for a concept.

        ``ConceptLearningStore.candidate_for_concept`` intentionally treats a
        failed candidate as terminal for some callers.  The review surface
        needs a slightly different policy: publish failures and stale
        proposals must remain inspectable and retryable, while an already
        published/rejected/superseded proposal must not resurrect an old
        review row.
        """
        for candidate in self.learning.list_candidates(concept=name):
            if str(candidate.get("status") or "") not in CANDIDATE_REVIEW_TERMINAL:
                return candidate
        return None

    @staticmethod
    def _candidate_source_uris(candidate: Dict[str, Any]) -> list[str]:
        """Collect verifiable source URIs without counting duplicate evidence."""
        values: list[str] = []
        source_refs = candidate.get("source_refs")
        if isinstance(source_refs, (list, tuple)):
            values.extend(str(value) for value in source_refs if value)
        evidence = candidate.get("evidence")
        if isinstance(evidence, (list, tuple)):
            for item in evidence:
                if isinstance(item, str) and item:
                    values.append(item)
                elif isinstance(item, dict):
                    for key in ("uri", "viking_uri", "source_uri", "ref"):
                        value = item.get(key)
                        if value:
                            values.append(str(value))
                            break
        return list(dict.fromkeys(values))

    @staticmethod
    def _normalise_match_term(value: Any) -> str:
        """Normalise a concept name for conservative, display-only matching."""
        # Keep CJK and alphanumeric characters while ignoring the separators
        # people commonly vary between a concept name and an alias.
        text = str(value or "").casefold()
        return re.sub(r"[\s_\-./·:：,，()（）\[\]【】]+", "", text)

    _ACTIVE_OVERLAP_MARKERS = {"资源", "权限", "本体", "数据表", "数据集", "数据卷", "目录", "非结构化"}
    _ACTIVE_GENERIC_OVERLAPS = {"数据", "管理", "功能", "任务", "信息", "系统", "应用", "配置", "支持"}

    @staticmethod
    def _longest_common_substring(left: str, right: str) -> str:
        previous = [0] * (len(right) + 1)
        best = ""
        for left_index, left_char in enumerate(left, 1):
            current = [0] * (len(right) + 1)
            for right_index, right_char in enumerate(right, 1):
                if left_char == right_char:
                    current[right_index] = previous[right_index - 1] + 1
                    length = current[right_index]
                    if length > len(best):
                        best = left[left_index - length : left_index]
            previous = current
        return best

    @staticmethod
    def _proposal_kind(candidate: Dict[str, Any]) -> tuple[Optional[str], str]:
        """Return a stable machine kind and the Chinese review label.

        Candidate producers have historically used both ``new_concept`` and
        ``new-concept``.  The projection deliberately normalises those values
        without changing the manifest on disk.
        """
        raw = str(candidate.get("kind") or "").strip().casefold().replace("-", "_")
        if raw in {"new", "new_concept"}:
            return "new_concept", "新概念"
        if raw == "alias":
            return "alias", "别名"
        if raw == "merge":
            return "merge", "合并"
        if raw in {"refresh", "correction", "assessment_evidence", "assessment", "update", "updated"}:
            return "update", "更新"
        if raw:
            # Unknown proposal kinds still represent an existing refresh path
            # unless their base explicitly says this is a new concept.
            base = str(candidate.get("base_version") or "").casefold()
            if base in {"new", "new_concept", "new-concept"} and not candidate.get("base_page_sha256"):
                return "new_concept", "新概念"
            return "update", "更新"
        base = str(candidate.get("base_version") or "").casefold()
        if base in {"new", "new_concept", "new-concept"} and not candidate.get("base_page_sha256"):
            return "new_concept", "新概念"
        return None, "—"

    @staticmethod
    def _diff_line_stats(value: Any) -> Dict[str, int]:
        """Count changed lines in the persisted unified diff representation."""
        if isinstance(value, str):
            chunks = [value]
        elif isinstance(value, (list, tuple)):
            chunks = []
            for item in value:
                if isinstance(item, str):
                    chunks.append(item)
                elif isinstance(item, dict):
                    # ``change`` is the field emitted by concept_learning;
                    # accept a few legacy names for old candidates.
                    for key in ("change", "diff", "text", "patch"):
                        if item.get(key) is not None:
                            chunks.append(str(item[key]))
                            break
        elif isinstance(value, dict):
            chunks = [str(value.get(key)) for key in ("change", "diff", "text", "patch") if value.get(key) is not None]
        else:
            chunks = []

        added = 0
        removed = 0
        for chunk in chunks:
            for line in str(chunk).replace("\r\n", "\n").replace("\r", "\n").split("\n"):
                if line.startswith(("+++", "---")):
                    continue
                if line.startswith("+"):
                    added += 1
                elif line.startswith("-"):
                    removed += 1
        return {"added": added, "removed": removed, "total": added + removed}

    def _active_concept_index(self, ledger: Optional[Dict[str, Any]] = None) -> list[Dict[str, Any]]:
        """Read the current Active names/aliases for duplicate hints only."""
        if ledger is None:
            try:
                value = json.loads(self.concepts_ledger_path.read_text(encoding="utf-8"))
                ledger = value if isinstance(value, dict) else {}
            except (OSError, json.JSONDecodeError):
                ledger = {}
        if not isinstance(ledger, dict):
            return []

        rows_by_name: Dict[str, Dict[str, Any]] = {}
        for key, value in ledger.items():
            if not isinstance(value, dict):
                continue
            status = str(value.get("status") or "active").casefold()
            if status not in {"active", "published"}:
                continue
            name = str(value.get("name") or key).strip()
            if not name:
                continue
            aliases = value.get("aliases")
            if isinstance(aliases, str):
                aliases = [aliases]
            if not isinstance(aliases, (list, tuple)):
                aliases = []
            rows_by_name[name] = {
                "name": name,
                "aliases": [str(alias).strip() for alias in aliases if str(alias).strip()],
                "category": value.get("category"),
            }

        # The Active ledger is authoritative for membership, but older ledger
        # rows do not persist aliases.  Merge aliases from config when PyYAML is
        # available; a missing optional parser simply leaves name matching in
        # place and does not make the API fail.
        try:
            import yaml

            config = yaml.safe_load((self.concepts_root / "config.yaml").read_text(encoding="utf-8")) or {}
            config_rows = config.get("concepts") if isinstance(config, dict) else []
            for item in config_rows if isinstance(config_rows, list) else []:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                row = rows_by_name.get(name)
                if row is None:
                    continue
                aliases = item.get("aliases")
                if isinstance(aliases, str):
                    aliases = [aliases]
                for alias in aliases if isinstance(aliases, (list, tuple)) else []:
                    text = str(alias).strip()
                    if text and text not in row["aliases"]:
                        row["aliases"].append(text)
        except Exception:
            pass
        return list(rows_by_name.values())

    @classmethod
    def _suspected_active_matches(
        cls,
        candidate: Dict[str, Any],
        active_rows: Iterable[Dict[str, Any]],
        proposal_kind: Optional[str],
    ) -> tuple[bool, list[str], list[Dict[str, Any]], str]:
        """Find likely Active duplicates without making a merge decision."""
        if proposal_kind != "new_concept":
            return False, [], [], "候选对应已有概念的更新流程"

        candidate_aliases = candidate.get("aliases")
        if isinstance(candidate_aliases, str):
            candidate_aliases = [candidate_aliases]
        if not isinstance(candidate_aliases, (list, tuple)):
            candidate_aliases = []
        candidate_terms: list[tuple[str, str]] = []
        for value in [candidate.get("concept"), *candidate_aliases]:
            text = str(value or "").strip()
            normalized = cls._normalise_match_term(text)
            if normalized and len(normalized) >= 2 and (text, normalized) not in candidate_terms:
                candidate_terms.append((text, normalized))

        details: list[Dict[str, Any]] = []
        for row in active_rows:
            active_name = str(row.get("name") or "").strip()
            active_terms: list[tuple[str, str]] = []
            for value in [active_name, *(row.get("aliases") or [])]:
                text = str(value or "").strip()
                normalized = cls._normalise_match_term(text)
                if normalized and len(normalized) >= 2:
                    active_terms.append((text, normalized))
            best: Optional[Dict[str, Any]] = None
            for candidate_text, candidate_normalized in candidate_terms:
                for active_text, active_normalized in active_terms:
                    if candidate_normalized == active_normalized:
                        score = 1.0
                        reason = "名称或别名与 Active 概念完全一致"
                    elif candidate_normalized in active_normalized or active_normalized in candidate_normalized:
                        # Avoid flagging a one-character substring (for
                        # example, a generic Chinese classifier) as a match.
                        common = cls._longest_common_substring(candidate_normalized, active_normalized)
                        if len(common) < 3 and common not in cls._ACTIVE_OVERLAP_MARKERS:
                            continue
                        if common in cls._ACTIVE_GENERIC_OVERLAPS:
                            continue
                        score = 0.9
                        reason = "名称或别名包含 Active 概念"
                    else:
                        common = cls._longest_common_substring(candidate_normalized, active_normalized)
                        score = difflib.SequenceMatcher(None, candidate_normalized, active_normalized).ratio()
                        if len(common) < 3 or common in cls._ACTIVE_GENERIC_OVERLAPS or score < 0.84:
                            continue
                        reason = "名称或别名与 Active 概念高度相似"
                    detail = {
                        "name": active_name,
                        "active_name": active_name,
                        "matched_term": candidate_text,
                        "active_term": active_text,
                        "score": round(score, 4),
                        "reason": reason,
                    }
                    if best is None or detail["score"] > best["score"]:
                        best = detail
            if best is not None:
                details.append(best)

        details.sort(key=lambda item: (-float(item.get("score") or 0), str(item.get("name") or "")))
        # Keep the API compact while retaining enough context for a reviewer.
        details = details[:10]
        names = list(dict.fromkeys(str(item.get("name")) for item in details if item.get("name")))
        if names:
            reason = "疑似与 Active 概念重复：" + "、".join(names[:3])
        else:
            reason = "未发现名称或别名匹配的 Active 概念"
        return bool(names), names, details, reason

    def candidate_projection(
        self,
        candidate: Dict[str, Any],
        *,
        active_rows: Optional[Iterable[Dict[str, Any]]] = None,
        include_details: bool = True,
    ) -> Dict[str, Any]:
        """Return a cached, read-only projection for one Candidate."""
        active = list(active_rows) if active_rows is not None else self._active_concept_index()
        key = (
            str(candidate.get("candidate_id") or ""),
            str(candidate.get("updated_at") or candidate.get("created_at") or ""),
            str(candidate.get("status") or ""),
            str(candidate.get("content_hash") or ""),
            str(candidate.get("base_page_sha256") or ""),
            bool(include_details),
            self._active_projection_key(active),
        )
        now = time.monotonic()
        with self._read_cache_lock:
            cached = self._projection_cache.get(key)
            if cached is not None and now - cached[0] < READ_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached[1])
        projected = self._candidate_projection_uncached(
            candidate,
            active_rows=active,
            include_details=include_details,
        )
        with self._read_cache_lock:
            self._projection_cache[key] = (now, projected)
            # Keep the cache bounded when a large historical Candidate store is
            # reviewed for a long-lived Control Plane process.
            if len(self._projection_cache) > 1000:
                self._projection_cache.clear()
        return copy.deepcopy(projected)

    def _candidate_projection_uncached(
        self,
        candidate: Dict[str, Any],
        *,
        active_rows: Optional[Iterable[Dict[str, Any]]] = None,
        include_details: bool = True,
    ) -> Dict[str, Any]:
        """Return a read-only review projection over one Candidate manifest.

        This function intentionally deep-copies the manifest.  The runner and
        review write paths continue to read the original Candidate file and
        never consume any of the derived fields below.
        """
        # Evidence excerpts and unified diffs can dominate a list response.
        # Keep list projections allow-listed and load those fields only from
        # the single-candidate detail endpoint.
        projected = (
            copy.deepcopy(candidate)
            if include_details
            else {
                key: copy.deepcopy(candidate[key])
                for key in CANDIDATE_SUMMARY_FIELDS
                if key in candidate
            }
        )
        proposal_kind, kind_label = self._proposal_kind(candidate)

        stats = self._diff_line_stats(candidate.get("diff"))
        # A few early candidates did not persist ``diff``.  Derive the same
        # statistics from local page/content snapshots when possible, without
        # writing either source.
        if stats["total"] == 0 and proposal_kind == "update" and include_details:
            try:
                before_path = self.concepts_root / "state" / "pages" / f"{str(candidate.get('concept') or '')}.md"
                content_path = Path(str(candidate.get("content_path") or self.learning.content_path(str(candidate.get("candidate_id") or ""))))
                if not content_path.is_absolute():
                    content_path = self.learning.skill_root / content_path
                before = before_path.read_text(encoding="utf-8") if before_path.is_file() else ""
                after = content_path.read_text(encoding="utf-8") if content_path.is_file() else ""
                if before != after and (before or after):
                    derived = list(difflib.unified_diff(before.splitlines(), after.splitlines(), lineterm=""))
                    stats = self._diff_line_stats("\n".join(derived))
            except (OSError, UnicodeError):
                pass

        active = list(active_rows) if active_rows is not None else self._active_concept_index()
        explicit_suspected = candidate.get("suspected_existing")
        explicit_matches = candidate.get("suspected_existing_matches")
        explicit_reason = candidate.get("suspected_existing_reason")
        explicit_details = candidate.get("suspected_existing_match_details")
        # Newer discovery producers may persist deterministic active-match
        # metadata on the Candidate.  Treat it as authoritative when present;
        # this projection must not replace a stronger upstream decision with a
        # weaker lexical fallback.
        active_match = candidate.get("active_match")
        if isinstance(explicit_matches, str):
            explicit_matches = [explicit_matches]
        if isinstance(explicit_matches, (list, tuple)) or isinstance(explicit_suspected, bool) or isinstance(active_match, dict):
            matches = []
            if isinstance(explicit_matches, (list, tuple)):
                for item in explicit_matches:
                    if isinstance(item, dict):
                        value = item.get("name") or item.get("target") or item.get("active_name")
                    else:
                        value = item
                    if value and str(value) not in matches:
                        matches.append(str(value))
            if isinstance(active_match, dict):
                value = active_match.get("name") or active_match.get("target") or active_match.get("active_name")
                if value and str(value) not in matches:
                    matches.append(str(value))
            suspected = bool(explicit_suspected) if isinstance(explicit_suspected, bool) else bool(matches)
            if isinstance(explicit_details, (list, tuple)):
                match_details = [copy.deepcopy(item) for item in explicit_details if isinstance(item, dict)]
            elif isinstance(active_match, dict):
                match_details = [copy.deepcopy(active_match)]
            else:
                match_details = []
            suspect_reason = str(explicit_reason or "").strip()
            if not suspect_reason:
                suspect_reason = "疑似与 Active 概念重复：" + "、".join(matches[:3]) if matches else "未发现名称或别名匹配的 Active 概念"
        else:
            suspected, matches, match_details, suspect_reason = self._suspected_active_matches(candidate, active, proposal_kind)
        existing_name = matches[0] if matches else None
        if proposal_kind == "update":
            concept_name = str(candidate.get("concept") or "").strip()
            active_names = {str(row.get("name") or "") for row in active}
            if concept_name in active_names:
                existing_name = concept_name

        # Keep both naming styles because the review page is also consumed by
        # older local clients that do not transform API keys.
        projected.update(
            {
                "read_only_projection": True,
                # Candidate files remain an append-only audit source after the
                # concept workflow retirement.  Keep the persisted status in
                # ``raw_status`` for history consumers, but make the display
                # contract unambiguously non-actionable so an old client
                # cannot render an approved/queued row as executable work.
                "raw_status": candidate.get("status"),
                "actionable": False,
                "history_only": True,
                "workflow_status": "disabled",
                "display_status": "历史保留",
                "projection_schema_version": "concept-review.candidate-projection.v1",
                "raw_kind": candidate.get("kind"),
                "proposal_kind": proposal_kind,
                "proposal_kind_code": proposal_kind,
                "proposal_kind_label": kind_label,
                "kind_label": kind_label,
                "proposalKind": proposal_kind,
                "proposalKindCode": proposal_kind,
                "proposalKindLabel": kind_label,
                "kindLabel": kind_label,
                "diff_added_lines": stats["added"],
                "diff_removed_lines": stats["removed"],
                "diff_line_count": stats["total"],
                "diff_total_lines": stats["total"],
                "diff_added": stats["added"],
                "diff_removed": stats["removed"],
                "diff_total": stats["total"],
                "diffAddedLines": stats["added"],
                "diffRemovedLines": stats["removed"],
                "diffLineCount": stats["total"],
                "diffTotalLines": stats["total"],
                "diffAdded": stats["added"],
                "diffRemoved": stats["removed"],
                "diffTotal": stats["total"],
                "diff_stats": {"added": stats["added"], "removed": stats["removed"], "total": stats["total"]},
                "diffStats": {"added": stats["added"], "removed": stats["removed"], "total": stats["total"]},
                "suspected_existing": suspected,
                "suspected_active": suspected,
                "suspected_existing_active": suspected,
                "suspected_existing_matches": matches,
                "suspected_existing_active_matches": matches,
                "suspected_existing_match_details": match_details,
                "suspected_existing_reason": suspect_reason,
                "suspected_existing_active_reason": suspect_reason,
                "suspected_existing_match_reason": suspect_reason,
                "suspected_existing_reasons": [suspect_reason],
                "suspected_existing_concept": existing_name,
                "suspected_active_concept": existing_name,
                "suspected_existing_active_concept": existing_name,
                "active_matches": matches,
                "existing_active_concept": existing_name,
                "suspectedExisting": suspected,
                "suspectedActive": suspected,
                "suspectedExistingActive": suspected,
                "suspectedExistingMatches": matches,
                "suspectedExistingActiveMatches": matches,
                "suspectedExistingMatchDetails": match_details,
                "suspectedExistingReason": suspect_reason,
                "suspectedExistingActiveReason": suspect_reason,
                "suspectedExistingMatchReason": suspect_reason,
                "suspectedExistingReasons": [suspect_reason],
                "suspectedExistingConcept": existing_name,
                "suspectedActiveConcept": existing_name,
                "suspectedExistingActiveConcept": existing_name,
                "activeMatches": matches,
                "existingActiveConcept": existing_name,
                "details_available": True,
                "details_loaded": bool(include_details),
                "evidence_count": len(candidate.get("evidence") or []) if isinstance(candidate.get("evidence"), (list, tuple)) else 0,
                "diff_available": bool(candidate.get("diff")),
            }
        )
        return projected

    def candidate_projections(
        self,
        candidates: Optional[Iterable[Dict[str, Any]]] = None,
        *,
        include_details: bool = True,
    ) -> list[Dict[str, Any]]:
        """Project a Candidate collection without touching its persisted rows."""
        rows = list(candidates) if candidates is not None else self._read_candidates_cached()
        active = self._active_concept_index()
        return [
            self.candidate_projection(row, active_rows=active, include_details=include_details)
            for row in rows
        ]

    @staticmethod
    def _candidate_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        values = list(rows)
        counts = {
            "total": len(values),
            "ready_for_review": 0,
            "paused": 0,
            "changes_requested": 0,
            "approved": 0,
            "queued": 0,
            "publishing": 0,
            "published": 0,
            "rejected": 0,
            "superseded": 0,
            "stale": 0,
            "failed": 0,
            "publish_failed": 0,
        }
        for row in values:
            status = str(row.get("status") or "")
            if status in counts:
                counts[status] += 1
        return counts

    @staticmethod
    def _filter_candidate_statuses(
        rows: Iterable[Dict[str, Any]],
        status_query: str,
    ) -> list[Dict[str, Any]]:
        """Apply a compact status filter without changing persisted rows."""
        rows = list(rows)
        raw = str(status_query or "").strip().casefold()
        if not raw or raw in {"all", "*"}:
            return rows
        if raw in {"reviewable", "actionable", "pending"}:
            allowed = set(CANDIDATE_REVIEW_TERMINAL)
            return [row for row in rows if str(row.get("status") or "") not in allowed]
        if raw in {"terminal", "closed"}:
            return [row for row in rows if str(row.get("status") or "") in CANDIDATE_REVIEW_TERMINAL]
        allowed = {item.strip() for item in raw.split(",") if item.strip()}
        return [row for row in rows if str(row.get("status") or "").casefold() in allowed]

    @staticmethod
    def _parse_page(query: Dict[str, list[str]]) -> tuple[int, int, bool]:
        explicit = any(key in query for key in ("page", "page_size", "limit"))
        try:
            page = max(1, int((query.get("page") or ["1"])[0]))
        except (TypeError, ValueError):
            page = 1
        raw_size = (query.get("page_size") or query.get("limit") or [""])[0]
        if raw_size == "":
            size = CANDIDATE_PAGE_SIZE_DEFAULT
        else:
            try:
                size = max(1, min(int(raw_size), CANDIDATE_PAGE_SIZE_MAX))
            except (TypeError, ValueError):
                size = CANDIDATE_PAGE_SIZE_DEFAULT
        return page, size, explicit

    @staticmethod
    def _query_true(query: Dict[str, list[str]], *keys: str) -> bool:
        values = [str((query.get(key) or [""])[0]).strip().casefold() for key in keys]
        return any(value in {"1", "true", "yes", "detail", "full"} for value in values)

    def candidates_read_model(self, query: Optional[Dict[str, list[str]]] = None) -> Dict[str, Any]:
        """Return a paged, summary-first Candidate projection for the UI.

        The no-query shape remains the historical full list for compatibility.
        New clients should send ``status=reviewable&page_size=50`` and receive
        lightweight rows; opening one row uses the detail endpoint.
        """
        query = query or {}
        raw_rows = self._read_candidates_cached()
        counts = self._candidate_counts(raw_rows)
        status_query = (query.get("status") or [""])[0]
        filtered = self._filter_candidate_statuses(raw_rows, status_query)
        concept_query = str((query.get("concept") or [""])[0]).strip()
        if concept_query:
            filtered = [row for row in filtered if str(row.get("concept") or "") == concept_query]

        page, page_size, explicit_page = self._parse_page(query)
        # Preserve the old no-query contract.  Any filtered/paged request is
        # bounded and summary-first, which is the path used by the Control Plane
        # browser.
        legacy_full = not query and not explicit_page
        include_details = legacy_full or self._query_true(query, "details", "include_details")
        if explicit_page:
            start = (page - 1) * page_size
            selected = filtered[start : start + page_size]
        else:
            page = 1
            page_size = len(filtered) or 1
            selected = filtered
        projected = self.candidate_projections(selected, include_details=include_details)
        total = len(filtered)
        pages = math.ceil(total / page_size) if page_size else 0
        return {
            "schema_version": "concept-review.candidates.v2",
            "read_only": True,
            "disabled": CONCEPT_WORKFLOW_DISABLED,
            "history_only": CONCEPT_WORKFLOW_DISABLED,
            "actionable": 0,
            "reason": CONCEPT_WORKFLOW_REASON if CONCEPT_WORKFLOW_DISABLED else None,
            "candidates": projected,
            "counts": counts,
            "actionable_counts": {
                "total": 0,
                "ready_for_review": 0,
                "paused": 0,
                "changes_requested": 0,
                "approved": 0,
                "queued": 0,
                "publishing": 0,
                "publish_failed": 0,
            },
            "history_counts": counts,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "pages": pages,
                "has_next": page < pages,
                "has_previous": page > 1,
                "status": status_query or "all",
                "concept": concept_query or None,
                "details": include_details,
            },
        }

    @staticmethod
    def _path_signature(path: Path, *, directory_glob: str = "*.json") -> Any:
        """Return a cheap, deterministic mutation signature for read polling."""
        try:
            if path.is_file():
                stat = path.stat()
                return [path.name, stat.st_mtime_ns, stat.st_size]
            if path.is_dir():
                rows = []
                for item in sorted(path.glob(directory_glob), key=lambda value: value.name):
                    try:
                        stat = item.stat()
                    except OSError:
                        continue
                    rows.append([item.name, stat.st_mtime_ns, stat.st_size])
                return rows
        except OSError:
            pass
        return None

    @classmethod
    def _source_signature_value(cls, path: Optional[Path], *, directory_glob: str = "*.json") -> Dict[str, Any]:
        """Return a bounded, debuggable signature for one observed source.

        The signature deliberately contains metadata rather than the source
        body.  It is sufficient to invalidate a read projection after an
        atomic runner write and keeps six-second summary polling cheap.  A
        missing or unreadable source is represented explicitly instead of
        being silently treated as an unchanged empty value.
        """
        if path is None:
            return {"exists": False, "digest": None, "path": None}
        raw = cls._path_signature(path, directory_glob=directory_glob)
        encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        value: Dict[str, Any] = {
            "path": str(path),
            "exists": raw is not None,
            "digest": content_hash(encoded) if raw is not None else None,
        }
        if isinstance(raw, list) and len(raw) == 3 and isinstance(raw[1], (int, float)):
            value.update({"mtime_ns": raw[1], "bytes": raw[2]})
        elif isinstance(raw, list):
            value["entries"] = len(raw)
        return value

    def _control_plane_source_paths(self) -> Dict[str, tuple[Optional[Path], str]]:
        """Return all state files represented by the V3 read model.

        Keeping this map in one place makes the freshness projection and the
        cache invalidation contract use the same source set.  Values are
        `(path, directory_glob)` pairs; `None` is used for signal sources that
        are not wired to a local artifact yet.
        """
        inventory_root = self.concepts_root / "state" / "full-inventory"
        name_sidecars = self._name_fingerprint_sidecar_paths()
        timeline_root = self.codex_root / "skills" / "pm-timeline" / "state" / "timeline"
        team_path = self._latest_file(timeline_root, timeline_root / "*.jsonl")
        product_gap_path = self.domain_report_source_path("gaps")
        material_path = self.domain_report_source_path("materials")
        health_report_path = self.health_report_path()
        timeline_review_path = self.pm_timeline_review_path()
        paths: Dict[str, tuple[Optional[Path], str]] = {
            "concept_ledger": (self.concepts_ledger_path, "*.json"),
            "candidates": (self.learning.candidates_root, "*.json"),
            "candidate_content": (self.learning.content_root, "*.md"),
            "usage": (self.learning.usage_root / "events.jsonl", "*.json"),
            "discovery": (self.learning.discovery_root, "*.json"),
            "staged_reviews": (self.review_state_path, "*.json"),
            "publish_queue": (self.queue_path, "*.json"),
            "run_states": (self.store.state_dir, "**/*.json"),
            "health": (self.codex_root / "skills" / "system-health-check" / "state" / "latest.json", "*.json"),
            "health_report": (health_report_path, "*.html"),
            "timeline_review": (timeline_review_path, "*.html"),
            "launch_agents": (self.launch_agents_root, "com.zhujie14.*.plist"),
            "automations": (self.codex_root / "automations", "*/automation.toml"),
            "weekly": (self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.done", "*.json"),
            "requirements": (self.codex_root / "skills" / "requirement-fit-assessment" / "state" / "index.jsonl", "*.json"),
            "team": (team_path, "*.jsonl"),
            "gaps": (product_gap_path, f"*.{product_gap_path.suffix.lstrip('.')}") if product_gap_path else (None, "*.html"),
            "materials": (material_path, f"*.{material_path.suffix.lstrip('.')}") if material_path else (None, "*.html"),
            "inventory_result": (inventory_root / "latest-result.json", "*.json"),
            "inventory_baseline": (inventory_root / "incremental-baseline.json", "*.json"),
            "inventory_cache_meta": (inventory_root / "evidence-cache.meta.json", "*.json"),
            "inventory_content_dedup": (inventory_root / "content-dedup.json.gz", "*.json"),
            "name_baseline": (name_sidecars["baseline"][0] if name_sidecars["baseline"] else None, "*.json"),
            "source_manifest": (name_sidecars["manifest"][0] if name_sidecars["manifest"] else None, "*.json"),
            "content_audit_queue": (name_sidecars["audit_queue"][0] if name_sidecars["audit_queue"] else None, "*.json"),
            # Codex handoff intents are part of the observed Control Plane
            # state.  A newly submitted intent therefore advances the summary
            # and snapshot version immediately, without becoming an executable
            # runner state.
            "control_plane_jobs": (self.control_plane_jobs_path, "*.jsonl"),
        }
        # Keep migration aliases in the mutation set.  A producer may still
        # write an older sidecar name while another process reads the newer
        # one; either change must invalidate the displayed snapshot.
        for group, aliases in name_sidecars.items():
            for index, alias in enumerate(aliases):
                paths[f"{group}_alias_{index}"] = (alias, "*.json")
        return paths

    def _control_plane_source_signatures(self) -> Dict[str, Any]:
        """Return compact source signatures used for V3 freshness and cache."""
        return {
            name: self._source_signature_value(path, directory_glob=directory_glob)
            for name, (path, directory_glob) in self._control_plane_source_paths().items()
        }

    @staticmethod
    def _source_version(signatures: Mapping[str, Any]) -> str:
        """Derive a deterministic version from source metadata only."""
        encoded = json.dumps(signatures, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return content_hash(encoded)

    @staticmethod
    def _compact_signatures(signatures: Dict[str, Any]) -> Dict[str, Any]:
        """Keep the polling response small while retaining debuggable digests."""
        compact: Dict[str, Any] = {}
        for name, value in signatures.items():
            if value is None:
                compact[name] = None
                continue
            encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            digest = content_hash(encoded)
            if (
                isinstance(value, list)
                and len(value) == 3
                and isinstance(value[0], str)
                and isinstance(value[1], (int, float))
                and isinstance(value[2], (int, float))
            ):
                compact[name] = {
                    "digest": digest,
                    "mtime_ns": value[1],
                    "bytes": value[2],
                }
            elif isinstance(value, list):
                # Directory signatures can contain thousands of [name, mtime,
                # size] rows.  Never send those rows to a six-second poller.
                compact[name] = {"digest": digest, "count": len(value)}
            elif isinstance(value, dict):
                compact[name] = {"digest": digest, "keys": len(value)}
            else:
                compact[name] = {"digest": digest, "value": value}
        return compact

    def _name_fingerprint_sidecar_paths(self) -> Dict[str, list[Path]]:
        """Return the compatible metadata sidecar locations, in preference order."""
        state_root = self.concepts_root / "state"
        inventory_root = state_root / "full-inventory"

        def unique(paths: Iterable[Path]) -> list[Path]:
            result: list[Path] = []
            seen: set[str] = set()
            for path in paths:
                key = str(path)
                if key not in seen:
                    seen.add(key)
                    result.append(path)
            return result

        return {
            "baseline": unique(state_root / name for name in NAME_BASELINE_FILENAMES),
            "manifest": unique(
                [state_root / name for name in SOURCE_MANIFEST_FILENAMES]
                + [inventory_root / name for name in SOURCE_MANIFEST_FILENAMES]
            ),
            "audit_queue": unique(
                [state_root / name for name in AUDIT_QUEUE_FILENAMES]
                + [inventory_root / name for name in AUDIT_QUEUE_FILENAMES]
            ),
        }

    @staticmethod
    def _read_first_sidecar(paths: Iterable[Path]) -> tuple[Optional[Path], Dict[str, Any]]:
        """Read the first valid JSON object without touching large evidence artifacts."""
        for path in paths:
            if not path.is_file():
                continue
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                # A partially written sidecar is not a reason to fail the
                # whole snapshot.  Try a migration alias before reporting it.
                continue
            if isinstance(value, dict):
                return path, value
        return None, {}

    @staticmethod
    def _sidecar_signature(paths: Iterable[Path]) -> Any:
        """Keep all aliases in the cheap version signature, without their body."""
        rows: list[Any] = []
        for path in paths:
            signature = ControlPlane._path_signature(path)
            if signature is not None:
                rows.append([str(path), signature])
        return rows or None

    @staticmethod
    def _first_value(*values: Any) -> Any:
        for value in values:
            if value is not None and value != "":
                return value
        return None

    @staticmethod
    def _count_value(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return max(0, int(value))
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _ratio_value(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return round(float(value), 6)
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _sidecar_metrics(value: Mapping[str, Any]) -> Mapping[str, Any]:
        for key in ("metrics", "stats", "summary"):
            candidate = value.get(key)
            if isinstance(candidate, Mapping):
                return candidate
        return {}

    def _name_fingerprint_status(self) -> Dict[str, Any]:
        """Project name-proxy, strict-content and audit status for the UI.

        This method intentionally reads only compact sidecars.  In particular,
        it never opens ``evidence-cache.json[.gz]`` or content-dedup bodies.
        Missing metrics remain ``null`` so the Control Plane cannot imply that
        an unmapped/conflict/audit count is zero merely because a producer has
        not written that field yet.
        """
        paths = self._name_fingerprint_sidecar_paths()
        baseline_path, baseline = self._read_first_sidecar(paths["baseline"])
        manifest_path, manifest = self._read_first_sidecar(paths["manifest"])
        audit_path, audit = self._read_first_sidecar(paths["audit_queue"])
        baseline_metrics = self._sidecar_metrics(baseline)
        manifest_metrics = self._sidecar_metrics(manifest)

        baseline_rows: Any = baseline.get("rows")
        if not baseline_rows:
            baseline_rows = baseline.get("documents")
        revisions = baseline.get("revisions")
        if not baseline_rows and isinstance(revisions, Mapping):
            baseline_rows = revisions

        def row_values(value: Any) -> list[Any]:
            if isinstance(value, Mapping):
                return list(value.values())
            if isinstance(value, list):
                return value
            return []

        observed_count: Optional[int] = None
        inferred_total: Optional[int] = None
        row_unknown_count: Optional[int] = None
        if baseline_rows:
            values = row_values(baseline_rows)
            inferred_total = len(values)
            observed_count = sum(
                1
                for row in values
                if (
                    isinstance(row, str)
                    and row.strip()
                    and row.strip().lower() not in {"unknown", ""}
                )
                or (
                    isinstance(row, Mapping)
                    and str(
                        row.get("name_hash")
                        or row.get("revision")
                        or row.get("source_revision")
                        or ""
                    ).strip().lower()
                    not in {"", "unknown", "none", "null"}
                )
            )
            row_unknown_count = max(0, inferred_total - observed_count)

        mode = self._first_value(
            baseline.get("revision_mode"),
            baseline.get("revision_kind"),
            manifest.get("revision_mode"),
            manifest.get("revision_kind"),
        )
        mode = str(mode) if mode is not None else None
        rule = self._first_value(
            baseline.get("name_hash_rule"),
            manifest.get("name_hash_rule"),
            NAME_HASH_RULE if mode == "name_hash" else None,
        )
        rule = str(rule) if rule is not None else None
        name_hash_prefix = self._first_value(
            baseline.get("name_hash_prefix"),
            manifest.get("name_hash_prefix"),
            NAME_HASH_PREFIX if mode == "name_hash" else None,
        )
        name_hash_format = self._first_value(
            baseline.get("name_hash_format"),
            manifest.get("name_hash_format"),
            NAME_HASH_FORMAT if mode == "name_hash" else None,
        )

        # A source manifest also contains inventory-only URI rows.  Those rows
        # are useful for mapping diagnostics, but their fallback names are not
        # trusted sync metadata.  Prefer an explicit name baseline, then the
        # sync-ledger counters, and only fall back to the full observed set for
        # legacy producers that have no ledger scope at all.
        baseline_coverage_count = self._count_value(
            self._first_value(
                baseline.get("name_hash_observed"),
                baseline.get("name_hash_count"),
                baseline_metrics.get("name_hash_observed"),
                baseline_metrics.get("name_hash_count"),
                manifest_metrics.get("name_hash_observed"),
                manifest_metrics.get("name_hash_count"),
                observed_count,
            )
        )
        baseline_coverage_total = self._count_value(
            self._first_value(
                baseline.get("resource_count"),
                baseline.get("document_count"),
                baseline_metrics.get("resource_count"),
                baseline_metrics.get("document_count"),
                manifest_metrics.get("document_count"),
                inferred_total,
            )
        )
        ledger_coverage_count = self._count_value(
            self._first_value(
                manifest_metrics.get("ledger_name_hash_count"),
                manifest_metrics.get("ledger_name_hash_observed"),
            )
        )
        ledger_coverage_total = self._count_value(manifest_metrics.get("ledger_document_count"))
        # The manifest's ledger counters describe the trusted sync scope even
        # after a weekly baseline has been materialized.  A separate baseline
        # file is used only when those scope counters are unavailable (legacy
        # or isolated test producers); otherwise inventory rows must not be
        # mislabeled as the operational sync ledger.
        if ledger_coverage_total is not None:
            coverage_count = ledger_coverage_count
            coverage_total = ledger_coverage_total
            coverage_scope = "sync_ledger"
        elif baseline_path:
            coverage_count = baseline_coverage_count
            coverage_total = baseline_coverage_total
            coverage_scope = "name_baseline"
        else:
            coverage_count = self._count_value(
                self._first_value(manifest_metrics.get("name_hash_observed"), observed_count)
            )
            coverage_total = self._count_value(
                self._first_value(manifest_metrics.get("document_count"), inferred_total)
            )
            coverage_scope = "observed_documents"
        coverage_ratio = self._ratio_value(
            self._first_value(
                manifest_metrics.get("ledger_name_hash_coverage") if coverage_scope == "sync_ledger" else None,
                baseline.get("name_hash_coverage") if coverage_scope == "name_baseline" else None,
                baseline_metrics.get("name_hash_coverage") if coverage_scope == "name_baseline" else None,
                manifest_metrics.get("name_hash_coverage") if coverage_scope == "observed_documents" else None,
                (coverage_count / coverage_total if coverage_count is not None and coverage_total else None),
            )
        )
        comparison_coverage = {
            "count": baseline_coverage_count if baseline_path else None,
            "total": baseline_coverage_total if baseline_path else None,
            "ratio": self._ratio_value(
                self._first_value(
                    baseline.get("name_hash_coverage") if baseline_path else None,
                    baseline_metrics.get("name_hash_coverage") if baseline_path else None,
                    (
                        baseline_coverage_count / baseline_coverage_total
                        if baseline_path
                        and baseline_coverage_count is not None
                        and baseline_coverage_total
                        else None
                    ),
                )
            ),
        }
        inventory_coverage_count = self._count_value(
            self._first_value(manifest_metrics.get("name_hash_observed"), observed_count)
        )
        inventory_coverage_total = self._count_value(
            self._first_value(manifest_metrics.get("document_count"), inferred_total)
        )
        inventory_coverage = {
            "count": inventory_coverage_count,
            "total": inventory_coverage_total,
            "ratio": self._ratio_value(
                self._first_value(
                    manifest_metrics.get("name_hash_coverage"),
                    (inventory_coverage_count / inventory_coverage_total
                     if inventory_coverage_count is not None and inventory_coverage_total else None),
                )
            ),
        }
        unknown_count = self._count_value(
            self._first_value(
                baseline.get("unknown_count"),
                baseline.get("unknown_revision_count"),
                baseline_metrics.get("unknown_count"),
                baseline_metrics.get("unknown_revision_count"),
                row_unknown_count,
                (coverage_total - coverage_count if coverage_total is not None and coverage_count is not None else None),
            )
        )
        conflict_count = self._count_value(
            self._first_value(
                baseline.get("name_hash_conflict"),
                baseline.get("conflict_count"),
                baseline_metrics.get("name_hash_conflict"),
                baseline_metrics.get("conflict_count"),
                manifest_metrics.get("name_hash_conflict"),
                manifest_metrics.get("conflict_document_count"),
                manifest_metrics.get("metadata_conflict_uri_count"),
                manifest_metrics.get("conflict_count"),
                manifest_metrics.get("conflict_active_source_count"),
            )
        )
        if conflict_count is None and baseline_rows:
            conflict_count = sum(
                1
                for row in row_values(baseline_rows)
                if isinstance(row, Mapping) and (row.get("metadata_conflict") or row.get("conflicts"))
            )

        unmapped_count = self._count_value(
            self._first_value(
                baseline.get("name_hash_unmapped"),
                baseline.get("unmapped_count"),
                baseline_metrics.get("name_hash_unmapped"),
                baseline_metrics.get("unmapped_count"),
                manifest_metrics.get("name_hash_unmapped"),
                manifest_metrics.get("unmapped_document_count"),
                manifest_metrics.get("unmapped_active_source_count"),
                manifest_metrics.get("unmapped_count"),
            )
        )
        if unmapped_count is None and isinstance(manifest.get("unmapped_active_sources"), list):
            unmapped_count = len(manifest["unmapped_active_sources"])

        unmapped_document_count = self._count_value(
            self._first_value(
                baseline.get("unmapped_document_count"),
                baseline_metrics.get("unmapped_document_count"),
                manifest_metrics.get("unmapped_document_count"),
            )
        )
        unmapped_active_source_count = self._count_value(
            self._first_value(
                baseline.get("unmapped_active_source_count"),
                baseline_metrics.get("unmapped_active_source_count"),
                manifest_metrics.get("unmapped_active_source_count"),
                len(manifest.get("unmapped_active_sources") or []) if isinstance(manifest.get("unmapped_active_sources"), list) else None,
            )
        )
        conflict_document_count = self._count_value(
            self._first_value(
                baseline.get("conflict_document_count"),
                baseline_metrics.get("conflict_document_count"),
                manifest_metrics.get("conflict_document_count"),
                manifest_metrics.get("metadata_conflict_uri_count"),
            )
        )
        conflict_active_source_count = self._count_value(
            self._first_value(
                baseline.get("conflict_active_source_count"),
                baseline_metrics.get("conflict_active_source_count"),
                manifest_metrics.get("conflict_active_source_count"),
            )
        )
        active_source_reference_count = self._count_value(
            self._first_value(
                baseline.get("active_source_reference_count"),
                baseline_metrics.get("active_source_reference_count"),
                manifest_metrics.get("active_source_reference_count"),
                manifest_metrics.get("active_source_count"),
            )
        )
        active_source_unique_count = self._count_value(
            self._first_value(
                baseline.get("active_source_unique_count"),
                baseline_metrics.get("active_source_unique_count"),
                manifest_metrics.get("active_source_unique_count"),
            )
        )
        mapped_active_source_unique_count = self._count_value(
            self._first_value(
                baseline.get("mapped_active_source_unique_count"),
                baseline_metrics.get("mapped_active_source_unique_count"),
                manifest_metrics.get("mapped_active_source_unique_count"),
            )
        )
        unmapped_active_source_unique_count = self._count_value(
            self._first_value(
                baseline.get("unmapped_active_source_unique_count"),
                baseline_metrics.get("unmapped_active_source_unique_count"),
                manifest_metrics.get("unmapped_active_source_unique_count"),
            )
        )
        conflict_active_source_unique_count = self._count_value(
            self._first_value(
                baseline.get("conflict_active_source_unique_count"),
                baseline_metrics.get("conflict_active_source_unique_count"),
                manifest_metrics.get("conflict_active_source_unique_count"),
            )
        )

        explicit_operational_ready = baseline.get("baseline_ready")
        if not isinstance(explicit_operational_ready, bool):
            explicit_operational_ready = manifest.get("baseline_ready")
        if not isinstance(explicit_operational_ready, bool):
            explicit_operational_ready = (
                mode == "name_hash" and coverage_ratio is not None and coverage_ratio >= 1.0 and unknown_count == 0
            )
        operational_recorded = bool(baseline_path or manifest_path)
        operational_status = self._first_value(
            baseline.get("status"),
            manifest.get("status"),
            "ready" if explicit_operational_ready else ("incomplete" if operational_recorded else "not_recorded"),
        )
        if not operational_recorded:
            operational_status = "not_recorded"
            explicit_operational_ready = False
        operational_usable = bool(
            operational_recorded
            and mode == "name_hash"
            and coverage_ratio is not None
            and coverage_ratio > 0
        )

        inventory = self.deep_inventory_status()
        strict = inventory.get("incremental_baseline") if isinstance(inventory, dict) else None
        strict = strict if isinstance(strict, Mapping) else {}
        strict_total = self._count_value(
            self._first_value(strict.get("resource_count"), inventory.get("resource_count") if isinstance(inventory, dict) else None)
        )
        strict_count = self._count_value(
            self._first_value(strict.get("source_hash_count"), inventory.get("evidence_cache", {}).get("source_hash_rows") if isinstance(inventory.get("evidence_cache"), dict) else None)
        )
        strict_ratio = self._ratio_value(
            self._first_value(strict.get("source_hash_coverage"), (strict_count / strict_total if strict_count is not None and strict_total else None))
        )
        strict_ready = strict.get("baseline_ready") if isinstance(strict.get("baseline_ready"), bool) else False
        strict_recorded = bool(strict)
        content_baseline = {
            "ready": bool(strict_ready),
            "status": str(strict.get("status") or ("ready" if strict_ready else ("incomplete" if strict_recorded else "not_recorded"))),
            "trusted_hash_count": strict_count,
            "total": strict_total,
            "coverage": strict_ratio,
            "updated_at": self._first_value(strict.get("materialized_at"), strict.get("updated_at")),
        }

        audit_metrics = self._sidecar_metrics(audit)
        items = audit.get("items") if isinstance(audit.get("items"), list) else []
        item_statuses = [str(item.get("status") or "") for item in items if isinstance(item, Mapping)]
        pending = self._count_value(self._first_value(
            audit.get("pending"), audit.get("pending_count"), audit_metrics.get("pending"), audit_metrics.get("pending_count"), audit_metrics.get("selected_count"),
            sum(1 for value in item_statuses if value in {"pending", "queued", "retry", "running"}) if items else None,
        ))
        processed = self._count_value(self._first_value(
            audit.get("processed"), audit.get("processed_count"), audit.get("completed_count"), audit_metrics.get("processed"), audit_metrics.get("processed_count"), audit_metrics.get("completed_count"),
        ))
        audit_total = self._count_value(self._first_value(
            audit.get("total"), audit.get("total_count"), audit_metrics.get("total"), audit_metrics.get("total_count"), audit_metrics.get("document_count"),
        ))
        audit_planned = self._count_value(self._first_value(
            audit.get("planned"), audit.get("planned_count"), audit_metrics.get("planned"), audit_metrics.get("planned_count"),
        ))
        audit_selected = self._count_value(self._first_value(
            audit.get("selected"), audit.get("selected_count"), audit_metrics.get("selected"), audit_metrics.get("selected_count"),
        ))
        mismatch = self._count_value(self._first_value(
            audit.get("mismatch"), audit.get("mismatch_count"), audit_metrics.get("mismatch"), audit_metrics.get("mismatch_count"), audit_metrics.get("content_mismatch_count"), audit_metrics.get("content_audit_mismatch"),
        ))
        failed = self._count_value(self._first_value(
            audit.get("failed"), audit.get("failed_count"), audit.get("error_count"), audit_metrics.get("failed"), audit_metrics.get("failed_count"), audit_metrics.get("error_count"),
            sum(1 for value in item_statuses if value in {"failed", "error"}) if items else None,
        ))
        audit_status = self._first_value(audit.get("status"), audit_metrics.get("status"))
        if audit_status is None and audit_path:
            if failed:
                audit_status = "failed"
            elif pending:
                audit_status = "pending"
            elif processed is not None and audit_total is not None and processed < audit_total:
                audit_status = "pending"
            else:
                audit_status = "completed"
        audit_available = bool(audit_path)
        audit_queue = {
            "available": audit_available,
            "status": str(audit_status or "not_recorded"),
            "pending": pending,
            "processed": processed,
            "total": audit_total,
            "planned": audit_planned,
            "selected": audit_selected,
            "mismatch": mismatch,
            "failed": failed,
            "updated_at": self._first_value(audit.get("updated_at"), audit.get("generated_at"), audit.get("finished_at")),
            "path": str(audit_path) if audit_path else None,
        }

        return {
            "available": bool(baseline_path or manifest_path or audit_path),
            "status": str(operational_status),
            "mode": mode or "not_recorded",
            "revision_kind": mode or "not_recorded",
            "name_hash_rule": rule,
            "name_hash_prefix": str(name_hash_prefix) if name_hash_prefix is not None else None,
            "name_hash_format": str(name_hash_format) if name_hash_format is not None else None,
            "legacy_name_hash_prefix": LEGACY_NAME_HASH_PREFIX if mode == "name_hash" else None,
            "heuristic": True if mode == "name_hash" else None,
            "coverage": {"count": coverage_count, "total": coverage_total, "ratio": coverage_ratio},
            "coverage_scope": coverage_scope,
            "inventory_coverage": inventory_coverage,
            "unmapped_count": unmapped_count,
            "conflict_count": conflict_count,
            "unmapped_document_count": unmapped_document_count,
            "unmapped_active_source_count": unmapped_active_source_count,
            "conflict_document_count": conflict_document_count,
            "conflict_active_source_count": conflict_active_source_count,
            "active_source_reference_count": active_source_reference_count,
            "active_source_unique_count": active_source_unique_count,
            "mapped_active_source_unique_count": mapped_active_source_unique_count,
            "unmapped_active_source_unique_count": unmapped_active_source_unique_count,
            "conflict_active_source_unique_count": conflict_active_source_unique_count,
            "unknown_count": unknown_count,
            "operational_baseline": {
                "ready": bool(explicit_operational_ready),
                "usable": operational_usable,
                "status": str(operational_status),
                "coverage": comparison_coverage,
                "exception_count": unknown_count,
                "updated_at": self._first_value(baseline.get("updated_at"), baseline.get("materialized_at"), manifest.get("generated_at")),
                "path": str(baseline_path or manifest_path) if (baseline_path or manifest_path) else None,
            },
            "content_baseline": content_baseline,
            "audit_queue": audit_queue,
            "sources": {
                "baseline": str(baseline_path) if baseline_path else None,
                "manifest": str(manifest_path) if manifest_path else None,
                "audit_queue": str(audit_path) if audit_path else None,
            },
        }

    def control_plane_summary(self) -> Dict[str, Any]:
        """Build the cheap version/summary contract used by browser polling."""
        candidates = self._read_candidates_cached()
        ledger = self._read_json_file(self.concepts_ledger_path, {})
        ledger = ledger if isinstance(ledger, dict) else {}
        candidate_signature = self._path_signature(self.learning.candidates_root)
        inventory_state_root = self.concepts_root / "state" / "full-inventory"
        inventory_result_path = inventory_state_root / "latest-result.json"
        inventory_manifest_path: Optional[Path] = None
        try:
            manifest_paths = [
                item
                for item in (inventory_state_root / "runs").glob("*/manifest.json")
                if item.is_file()
            ]
            inventory_manifest_path = max(
                manifest_paths,
                key=lambda item: item.stat().st_mtime_ns,
                default=None,
            )
        except OSError:
            inventory_manifest_path = None
        product_gap_path = self.domain_report_source_path("gaps")
        material_path = self.domain_report_source_path("materials")
        health_report_path = self.health_report_path()
        timeline_review_path = self.pm_timeline_review_path()
        signatures = {
            "ledger": self._path_signature(self.concepts_ledger_path),
            "candidates": candidate_signature,
            "candidate_content": self._path_signature(self.learning.content_root, directory_glob="*.md"),
            "usage": self._path_signature(self.learning.usage_root / "events.jsonl"),
            "discovery": self._path_signature(self.learning.discovery_root),
            "staged": self._path_signature(self.review_state_path),
            "queue": self._path_signature(self.queue_path),
            "runs": self._path_signature(self.store.state_dir, directory_glob="**/*.json"),
            "health": self._path_signature(
                self.codex_root / "skills" / "system-health-check" / "state" / "latest.json"
            ),
            "health_report": self._path_signature(health_report_path) if health_report_path else None,
            "timeline_review": self._path_signature(timeline_review_path) if timeline_review_path else None,
            "launch_agents": self._path_signature(self.launch_agents_root, directory_glob="com.zhujie14.*.plist"),
            "automations": self._path_signature(self.codex_root / "automations", directory_glob="*/automation.toml"),
            "weekly": self._path_signature(self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.done"),
            "requirements": self._path_signature(
                self.codex_root / "skills" / "requirement-fit-assessment" / "state" / "index.jsonl"
            ),
            "team": self._path_signature(self.codex_root / "skills" / "pm-timeline" / "state", directory_glob="**/*.jsonl"),
            "gaps": self._path_signature(product_gap_path) if product_gap_path else None,
            "materials": self._path_signature(material_path) if material_path else None,
            "inventory_baseline": self._path_signature(
                inventory_state_root / "incremental-baseline.json"
            ),
            "inventory_cache_meta": self._path_signature(
                inventory_state_root / "evidence-cache.meta.json"
            ),
            "inventory_result": self._path_signature(inventory_result_path),
            "inventory_manifest": self._path_signature(inventory_manifest_path) if inventory_manifest_path else None,
            "control_plane_jobs": self._path_signature(self.control_plane_jobs_path),
        }
        name_sidecars = self._name_fingerprint_sidecar_paths()
        signatures.update(
            {
                # Include all migration aliases so creating/removing a sidecar
                # always invalidates the six-second summary version.
                "name_baseline": self._sidecar_signature(name_sidecars["baseline"]),
                "source_manifest": self._sidecar_signature(name_sidecars["manifest"]),
                "content_audit_queue": self._sidecar_signature(name_sidecars["audit_queue"]),
            }
        )
        # V3 keeps a single source-of-truth signature map for both summary
        # versions and full snapshots.  The older ``signatures`` map remains
        # in the response for clients that know the v1 contract.
        source_signatures = self._control_plane_source_signatures()
        source_version = self._source_version(source_signatures)
        version = source_version
        candidate_version = content_hash(
            json.dumps(
                {"manifests": candidate_signature, "content": signatures.get("candidate_content")},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        counts = self._candidate_counts(candidates)
        queue = self.queue_status()
        concept_workflow = self.concept_workflow_status()
        active = sum(
            1
            for value in ledger.values()
            if isinstance(value, dict) and str(value.get("status") or "active").casefold() in {"active", "published"}
        )
        checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "schema_version": CONTROL_PLANE_SUMMARY_SCHEMA,
            "legacy_schema_version": "pm-loop.control-plane-summary.v1",
            "read_only": True,
            "concept_workflow": concept_workflow,
            "checked_at": checked_at,
            "read_at": checked_at,
            "version": version,
            "source_version": source_version,
            "snapshot_id": "cp-" + source_version.removeprefix("sha256:")[:16],
            "pending": 0,
            "actionable": False,
            "candidate_version": candidate_version,
            "summary": {
                "active_concepts": active,
                "ledger_concepts": len(ledger),
                "candidate_counts": counts,
                "queue": {
                    "queued": queue.get("queued", 0),
                    "running": queue.get("running", 0),
                    "completed": queue.get("completed", 0),
                    "failed": queue.get("failed", 0),
                    "disabled": bool(queue.get("disabled")),
                    "history_only": bool(queue.get("history_only")),
                    "historical_counts": queue.get("historical_counts", {}),
                },
                "staged": 0,
            },
            "sources": self._compact_signatures(signatures),
            # Migration alias signatures are needed for cache invalidation but
            # are internal implementation detail.  Excluding them from the
            # compact summary keeps polling responses bounded; the full V3
            # snapshot still exposes the complete source map when diagnostics
            # need it.
            "source_signatures": {
                key: value
                for key, value in source_signatures.items()
                if "_alias_" not in str(key)
            },
        }

    def _candidate_content_snapshot(self, candidate: Dict[str, Any]) -> str:
        actual = self.learning.candidate_content_hash(candidate)
        expected = str(candidate.get("content_hash") or "")
        if not expected or actual != expected:
            raise ValueError("candidate content no longer matches its proposal")
        return actual

    @staticmethod
    def _validate_reviewable_status(candidate: Dict[str, Any], action: str) -> None:
        status = str(candidate.get("status") or "")
        if status in {"approved", "publishing", "queued", "publish_failed"}:
            raise ValueError(f"candidate is already in publish flow: {status}")
        if status in CANDIDATE_REVIEW_TERMINAL:
            raise ValueError(f"candidate is already {status}")
        if action == "approve" and status not in {"ready_for_review", "paused"}:
            raise ValueError(f"candidate cannot be approved from status: {status}")
        if action in {"pause", "changes"} and status not in {
            "ready_for_review",
            "paused",
            "changes_requested",
            "stale",
            "failed",
        }:
            raise ValueError(f"candidate cannot be reviewed from status: {status}")

    def concepts(
        self,
        candidates: Optional[Iterable[Dict[str, Any]]] = None,
        usage: Optional[Dict[str, Any]] = None,
        *,
        include_candidate_details: bool = False,
    ) -> list[Dict[str, Any]]:
        ledger: Dict[str, Any] = {}
        if self.concepts_ledger_path.is_file():
            try:
                value = json.loads(self.concepts_ledger_path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    ledger = value
            except (OSError, json.JSONDecodeError):
                ledger = {}
        staged = self._read_staged()
        result: list[Dict[str, Any]] = []
        ledger_names: set[str] = set()
        active_rows = self._active_concept_index(ledger)
        # Read the two append-only stores once per API request.  Scanning all
        # Candidate files once is materially cheaper than doing it once per
        # Active concept (and keeps the six-second UI refresh from piling up).
        candidates = list(candidates) if candidates is not None else self._read_candidates_cached()
        candidate_by_name: Dict[str, Dict[str, Any]] = {}
        for candidate_row in candidates:
            candidate_name = str(candidate_row.get("concept") or "").strip()
            if not candidate_name or candidate_name in candidate_by_name:
                continue
            if str(candidate_row.get("status") or "") in CANDIDATE_REVIEW_TERMINAL:
                continue
            candidate_by_name[candidate_name] = candidate_row
        usage_payload = usage if usage is not None else self.learning.usage_summary()
        usage_by_name = usage_payload.get("concepts", {}) if isinstance(usage_payload, dict) else {}
        for name, item in ledger.items():
            if not isinstance(item, dict):
                continue
            name = str(name)
            ledger_names.add(name)
            sources = item.get("sources") if isinstance(item.get("sources"), list) else []
            sources = list(dict.fromkeys(str(source) for source in sources if source))
            candidate = candidate_by_name.get(name)
            record = {
                "name": name,
                "category": item.get("category") or "未分类",
                "updated": item.get("last_updated"),
                "sourceCount": len(sources),
                "placeholder": len(sources) == 0,
                "uri": item.get("viking_uri") or f"viking://resources/shengsuan/concepts/{name}.md",
                "staged": staged.get(name),
                "candidate_only": False,
                "candidateOnly": False,
                "candidate": candidate,
            }
            result.append(
                self._enrich_review_record(
                    name,
                    record,
                    candidate,
                    active_rows,
                    usage_by_name,
                    include_candidate_details=include_candidate_details,
                )
            )

        # A newly discovered concept exists only as a Candidate until a human
        # publishes it.  Expose it in the same API so the review UI can act on
        # real proposals without fabricating an Active page or Viking URI.
        seen_names = set(ledger_names)
        for candidate in candidates:
            name = str(candidate.get("concept") or "").strip()
            status = str(candidate.get("status") or "")
            if not name or name in seen_names or status in CANDIDATE_REVIEW_TERMINAL:
                continue
            source_uris = self._candidate_source_uris(candidate)
            record = {
                "name": name,
                "category": candidate.get("category") or "待归类",
                "updated": candidate.get("updated_at") or candidate.get("created_at"),
                "sourceCount": len(source_uris),
                "placeholder": not source_uris,
                "uri": candidate.get("viking_uri") or candidate.get("published_uri"),
                "staged": staged.get(name),
                "candidate_only": True,
                "candidateOnly": True,
                "candidate": candidate,
            }
            result.append(
                self._enrich_review_record(
                    name,
                    record,
                    candidate,
                    active_rows,
                    usage_by_name,
                    include_candidate_details=include_candidate_details,
                )
            )
            seen_names.add(name)
        return result

    def _enrich_review_record(
        self,
        name: str,
        record: Dict[str, Any],
        candidate: Optional[Dict[str, Any]],
        active_rows: Optional[Iterable[Dict[str, Any]]] = None,
        usage_by_name: Optional[Dict[str, Dict[str, Any]]] = None,
        *,
        include_candidate_details: bool = False,
    ) -> Dict[str, Any]:
        enriched = self.learning.enrich_concept(
            name,
            record,
            candidate=candidate,
            usage_summary=usage_by_name,
        )
        # Keep the exact candidate selected above.  This matters for failed or
        # stale proposals which the generic store helper deliberately skips.
        enriched["candidate"] = (
            self.candidate_projection(
                candidate,
                active_rows=active_rows,
                include_details=include_candidate_details,
            )
            if isinstance(candidate, dict)
            else None
        )
        return enriched

    def concept(self, name: str) -> Dict[str, Any]:
        record = next((item for item in self.concepts() if item["name"] == name), None)
        if record is None:
            raise FileNotFoundError(f"unknown concept: {name}")
        page_path = self.concepts_root / "state" / "pages" / f"{name}.md"
        if page_path.is_file():
            try:
                record["page_excerpt"] = page_path.read_text(encoding="utf-8")[:12000]
            except OSError:
                record["page_excerpt"] = ""
        else:
            # Candidate-only concepts have no Active page yet.  Show the
            # proposed markdown from the candidate store instead of returning
            # a misleading empty page.
            candidate = record.get("candidate") if isinstance(record.get("candidate"), dict) else {}
            content_path = candidate.get("content_path")
            if not content_path and candidate.get("candidate_id"):
                content_path = self.learning.content_path(str(candidate["candidate_id"]))
            try:
                candidate_path = Path(str(content_path)) if content_path else None
                if candidate_path is not None and not candidate_path.is_absolute():
                    candidate_path = self.learning.skill_root / candidate_path
                record["page_excerpt"] = candidate_path.read_text(encoding="utf-8")[:12000] if candidate_path and candidate_path.is_file() else ""
            except OSError:
                record["page_excerpt"] = ""
        return record

    @contextmanager
    def _queue_transaction(self) -> Iterator[None]:
        """Serialize queue read-modify-write across threads and processes."""
        with self.queue_lock:
            self.queue_file_lock_path.parent.mkdir(parents=True, exist_ok=True)
            with self.queue_file_lock_path.open("a+", encoding="utf-8") as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def _read_queue_unlocked(self) -> list[Dict[str, Any]]:
        value = self._read_json_file(self.queue_path, [])
        return value if isinstance(value, list) else []

    def _read_queue(self) -> list[Dict[str, Any]]:
        with self._queue_transaction():
            return self._read_queue_unlocked()

    @staticmethod
    def _read_json_file(path: Path, default: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return default

    def _write_queue_unlocked(self, rows: list[Dict[str, Any]]) -> None:
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.queue_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        temp_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp_path.replace(self.queue_path)

    def _write_queue(self, rows: list[Dict[str, Any]]) -> None:
        with self._queue_transaction():
            self._write_queue_unlocked(rows)

    @staticmethod
    def _queue_timestamp(value: Any) -> Optional[float]:
        if not value:
            return None
        try:
            stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return stamp.timestamp()
        except (TypeError, ValueError, OverflowError):
            return None

    def _recover_running_queue_items(self) -> None:
        if CONCEPT_WORKFLOW_DISABLED:
            return
        with self._queue_transaction():
            self._recover_running_queue_items_locked()

    def _recover_running_queue_items_locked(self) -> None:
        """Recover only stale leases left by a crashed control-plane process.

        A fresh ``running`` row may belong to an older process that is still
        publishing, so it is left alone.  Once its lease expires, requeue it
        (up to the bounded attempt count) and emit a retry event.  This keeps
        restart recovery auditable and avoids blindly duplicating a live
        publish action.
        """
        if CONCEPT_WORKFLOW_DISABLED:
            return
        rows = self._read_queue_unlocked()
        if not rows:
            return
        now = time.time()
        changed = False
        for item in rows:
            if item.get("status") != "running":
                continue
            run_id = str(item.get("run_id") or "")
            run_exists = bool(run_id and self.store.exists(run_id))
            try:
                run_state = self.store.state(run_id) if run_exists else {}
            except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
                run_state = {}

            if not run_exists:
                item["status"] = "failed"
                item["error"] = "orphaned publish has no RunStore record"
                self._mark_candidate_publish_failed(item, item["error"])
                item["recovered_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                item["updated_at"] = item["recovered_at"]
                item["heartbeat_at"] = None
                item["recovery_reason"] = "run_missing"
                changed = True
                continue

            # If the worker completed the run before dying while updating the
            # queue projection, reconcile the queue instead of publishing a
            # second time.
            if run_state.get("status") in {"completed", "cancelled", "failed", "rejected"}:
                item["status"] = "completed" if run_state.get("status") == "completed" else run_state.get("status")
                if item["status"] != "completed":
                    self._mark_candidate_publish_failed(
                        item,
                        f"publish Run is already terminal: {run_state.get('status')}",
                    )
                item["recovered_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                item["updated_at"] = item["recovered_at"]
                item["heartbeat_at"] = None
                item["recovery_reason"] = "run_already_terminal"
                changed = True
                continue

            heartbeat = self._queue_timestamp(item.get("heartbeat_at") or item.get("updated_at"))
            if heartbeat is not None and now - heartbeat < QUEUE_LEASE_SECONDS:
                continue

            try:
                attempts = int(item.get("attempts", 0))
            except (TypeError, ValueError):
                attempts = 0
            recovered_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            item["recovered_at"] = recovered_at
            item["updated_at"] = recovered_at
            item["recovery_reason"] = "stale_running_lease"
            if attempts >= QUEUE_MAX_ATTEMPTS:
                item["status"] = "failed"
                item["error"] = "orphaned publish exceeded retry limit"
                self._mark_candidate_publish_failed(item, item["error"])
                item["heartbeat_at"] = None
                if run_id and run_state.get("status") not in QUEUE_TERMINAL:
                    self.store.append(run_id, "run/failed", {"error": item["error"]}, actor="control-plane")
            else:
                item["status"] = "queued"
                item["error"] = None
                item["heartbeat_at"] = None
                if run_id and run_state.get("status") not in QUEUE_TERMINAL:
                    self.store.append(run_id, "run/retrying", {"reason": "stale_running_lease", "attempt": attempts + 1}, actor="control-plane")
            changed = True
        if changed:
            self._write_queue_unlocked(rows)

    def _mark_candidate_publish_failed(self, item: Dict[str, Any], error: str) -> None:
        """Best-effort Candidate projection for a terminal queue recovery."""
        if CONCEPT_WORKFLOW_DISABLED:
            return
        candidate_id = str(item.get("candidate_id") or "")
        if not candidate_id:
            return
        try:
            self.learning.update_candidate(
                candidate_id,
                expected_statuses={"approved", "publishing"},
                status="publish_failed",
                error=error,
                failed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )
        except (FileNotFoundError, ValueError):
            pass

    def queue_status(self) -> Dict[str, Any]:
        # Display-only projection: do not create or acquire the queue lock.
        # The concept runner writes the queue atomically via replace(), so a
        # lock-free read cannot turn a page refresh into a mutation.
        rows = self._read_queue_unlocked()
        historical_counts = {
            "queued": sum(1 for row in rows if row.get("status") == "queued"),
            "running": sum(1 for row in rows if row.get("status") == "running"),
            "completed": sum(1 for row in rows if row.get("status") == "completed"),
            "failed": sum(1 for row in rows if row.get("status") == "failed"),
            "cancelled": sum(1 for row in rows if row.get("status") == "cancelled"),
        }
        projected_rows = []
        for row in rows:
            projected = dict(row)
            projected.setdefault("raw_status", row.get("status"))
            # Preserve the raw queue status for audit/debugging, while making
            # the public row impossible to mistake for a live queue item.
            projected["status"] = "history_only"
            projected["actionable"] = False
            projected["disabled"] = True
            projected["history_only"] = True
            projected["display_status"] = "历史保留"
            projected_rows.append(projected)
        if CONCEPT_WORKFLOW_DISABLED:
            return {
                "schema_version": "pm-loop.concept-publish-queue.v2",
                "disabled": True,
                "read_only": True,
                "history_only": True,
                "status": "disabled",
                "message": CONCEPT_WORKFLOW_REASON,
                "items": projected_rows,
                "history_items": projected_rows,
                "actionable": 0,
                "queued": 0,
                "running": 0,
                "completed": historical_counts["completed"],
                "failed": historical_counts["failed"],
                "cancelled": historical_counts["cancelled"],
                "historical_counts": historical_counts,
                "pending": 0,
                "actionable_items": [],
                "next_action": None,
                "concurrency": 0,
                "worker": False,
            }
        return {
            "items": projected_rows,
            "queued": sum(1 for row in rows if row.get("status") == "queued"),
            "running": sum(1 for row in rows if row.get("status") == "running"),
            "completed": sum(1 for row in rows if row.get("status") == "completed"),
            "failed": sum(1 for row in rows if row.get("status") == "failed"),
            "cancelled": sum(1 for row in rows if row.get("status") == "cancelled"),
            "concurrency": 1,
            # Concept publishing is owned by the concept runner.  The
            # display-only Control Plane has no worker thread, but still
            # exposes queue history without turning that absence into 500.
            "worker": bool(self.queue_thread and self.queue_thread.is_alive()),
        }

    def retry_publish(self, run_id: str) -> Dict[str, Any]:
        self._concept_write_rejected()
        with self._queue_transaction():
            snapshot = next((dict(row) for row in self._read_queue_unlocked() if row.get("run_id") == run_id), None)
        if not snapshot:
            raise FileNotFoundError(f"publish queue item not found for run: {run_id}")
        concept = str(snapshot.get("concept") or "")
        candidate_id = str(snapshot.get("candidate_id") or "")
        with self.learning.concept_lock(concept):
            candidate = self.learning.read_candidate(candidate_id)
            if candidate.get("status") != "publish_failed":
                raise ValueError(f"candidate is not retryable: {candidate.get('status')}")
            if candidate.get("approved_by") != "zhujie14":
                raise ValueError("candidate has no valid human approval")
            if self._candidate_content_snapshot(candidate) != candidate.get("approved_content_hash"):
                raise ValueError("candidate content no longer matches the approved snapshot")
            updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self.learning.update_candidate(
                candidate_id,
                expected_statuses={"publish_failed"},
                status="approved",
                retry_requested_at=updated_at,
            )
            try:
                self.store.append(
                    run_id,
                    "run/retrying",
                    {"candidate_id": candidate_id, "reason": "manual_retry"},
                    actor="reviewer",
                )
                with self._queue_transaction():
                    rows = self._read_queue_unlocked()
                    item = next((row for row in rows if row.get("run_id") == run_id), None)
                    if not item:
                        raise FileNotFoundError(f"publish queue item not found for run: {run_id}")
                    if item.get("status") != "failed":
                        raise ValueError(f"queue item is not failed: {item.get('status')}")
                    item["status"] = "queued"
                    item["error"] = None
                    item["updated_at"] = updated_at
                    item["heartbeat_at"] = None
                    self._write_queue_unlocked(rows)
            except Exception:
                self.learning.update_candidate(
                    candidate_id,
                    expected_statuses={"approved"},
                    status="publish_failed",
                )
                try:
                    self.store.append(run_id, "run/failed", {"error": "retry queue persistence failed"}, actor="control-plane")
                except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
                    pass
                raise
        self.queue_wakeup.set()
        return item

    def _enqueue_publish(self, run_id: str, candidate_id: str, concept: str) -> Dict[str, Any]:
        self._concept_write_rejected()
        with self._queue_transaction():
            rows = self._read_queue_unlocked()
            existing = next((row for row in rows if row.get("run_id") == run_id), None)
            if existing:
                return existing
            item = {
                "queue_id": f"publish-{run_id}",
                "run_id": run_id,
                "candidate_id": candidate_id,
                "concept": concept,
                "status": "queued",
                "attempts": 0,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "heartbeat_at": None,
            }
            rows.append(item)
            self._write_queue_unlocked(rows)
        self.queue_wakeup.set()
        return item

    def _reconcile_approved_candidates(self) -> None:
        if CONCEPT_WORKFLOW_DISABLED:
            return
        with self._review_transaction():
            self._reconcile_approved_candidates_locked()

    def _reconcile_approved_candidates_locked(self) -> None:
        """Recover approval records durably written before their queue item."""
        if CONCEPT_WORKFLOW_DISABLED:
            return
        for candidate in self.learning.list_candidates():
            status = str(candidate.get("status") or "")
            if status not in {"approved", "publishing"}:
                continue
            run_id = str(candidate.get("approval_run_id") or "")
            concept = str(candidate.get("concept") or "")
            candidate_id = str(candidate.get("candidate_id") or "")
            if not run_id or not concept or not candidate_id or not self.store.exists(run_id):
                continue
            state = self.store.state(run_id)
            if state.get("status") in TERMINAL_STATES:
                if status == "approved":
                    try:
                        with self.learning.concept_lock(concept):
                            self.learning.update_candidate(
                                candidate_id,
                                expected_statuses={"approved"},
                                status="ready_for_review",
                                approved_by=None,
                                approved_at=None,
                                approved_content_hash=None,
                                approval_note=None,
                                approval_run_id=None,
                                approval_recovered_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                error="approval Run ended before queue persistence",
                            )
                    except (FileNotFoundError, ValueError):
                        pass
                elif status == "publishing":
                    error = "approval Run ended while Candidate was publishing"
                    try:
                        with self.learning.concept_lock(concept):
                            self.learning.update_candidate(
                                candidate_id,
                                expected_statuses={"publishing"},
                                status="publish_failed",
                                error=error,
                                failed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            )
                            self._record_failed_publish(run_id, candidate_id, concept, error)
                    except (FileNotFoundError, ValueError):
                        pass
                continue
            event_types = {event.get("type") for event in self.store.events_for(run_id)}
            if "run/started" not in event_types:
                self.store.append(run_id, "run/started", {"concept": concept, "candidate_id": candidate_id, "mode": "approved_action_recovery"}, actor="control-plane")
            if "action/queued" not in event_types:
                self.store.append(run_id, "action/queued", {"candidate_id": candidate_id, "concept": concept, "queue": "approved_action", "recovered": True}, actor="control-plane")
            self._enqueue_publish(run_id, candidate_id, concept)

    def _record_failed_publish(self, run_id: str, candidate_id: str, concept: str, error: str) -> Dict[str, Any]:
        """Ensure a terminal orphan remains reachable through manual retry."""
        self._concept_write_rejected()
        with self._queue_transaction():
            rows = self._read_queue_unlocked()
            item = next((row for row in rows if row.get("run_id") == run_id), None)
            if item is None:
                now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                item = {
                    "queue_id": f"publish-{run_id}",
                    "run_id": run_id,
                    "candidate_id": candidate_id,
                    "concept": concept,
                    "status": "failed",
                    "attempts": 1,
                    "created_at": now,
                    "updated_at": now,
                    "heartbeat_at": None,
                    "error": error,
                    "recovery_reason": "terminal_run_with_publishing_candidate",
                }
                rows.append(item)
                self._write_queue_unlocked(rows)
            return dict(item)
    def _cancel_queued_publish(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._queue_transaction():
            snapshot = next((dict(row) for row in self._read_queue_unlocked() if row.get("run_id") == run_id), None)
            if snapshot is not None and snapshot.get("status") == "cancelled":
                if self.store.state(run_id).get("status") not in TERMINAL_STATES:
                    self.store.append(
                        run_id,
                        "run/cancelled",
                        {"reason": "approved_publish_cancelled_before_start"},
                        actor="control-plane",
                    )
                return snapshot
        if snapshot is None:
            return None
        if CONCEPT_WORKFLOW_DISABLED:
            self._concept_write_rejected()
        status = str(snapshot.get("status") or "")
        if status in QUEUE_TERMINAL:
            return snapshot
        if status == "running":
            raise ValueError("publish action has started and passed the safe cancellation point")
        if status != "queued":
            raise ValueError(f"publish action cannot be cancelled from status: {status}")

        concept = str(snapshot.get("concept") or "")
        candidate_id = str(snapshot.get("candidate_id") or "")
        cancelled_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self.learning.concept_lock(concept):
            candidate_before = self.learning.read_candidate(candidate_id)
            candidate_updated = False
            with self._queue_transaction():
                rows = self._read_queue_unlocked()
                item = next((row for row in rows if row.get("run_id") == run_id), None)
                if item is None:
                    raise FileNotFoundError(f"publish queue item not found for run: {run_id}")
                current_status = str(item.get("status") or "")
                if current_status != "queued":
                    if current_status == "running":
                        raise ValueError("publish action has started and passed the safe cancellation point")
                    raise ValueError(f"publish action cannot be cancelled from status: {current_status}")
                try:
                    self.learning.update_candidate(
                        candidate_id,
                        expected_statuses={"approved"},
                        status="ready_for_review",
                        approved_by=None,
                        approved_at=None,
                        approved_content_hash=None,
                        approval_note=None,
                        approval_run_id=None,
                        publish_cancelled_at=cancelled_at,
                    )
                    candidate_updated = True
                    item["status"] = "cancelled"
                    item["updated_at"] = cancelled_at
                    item["heartbeat_at"] = None
                    item["cancelled_at"] = cancelled_at
                    self._write_queue_unlocked(rows)
                    result = dict(item)
                except Exception:
                    if candidate_updated:
                        self.learning.update_candidate(
                            candidate_id,
                            expected_statuses={"ready_for_review"},
                            status="approved",
                            approved_by=candidate_before.get("approved_by"),
                            approved_at=candidate_before.get("approved_at"),
                            approved_content_hash=candidate_before.get("approved_content_hash"),
                            approval_note=candidate_before.get("approval_note"),
                            approval_run_id=candidate_before.get("approval_run_id"),
                            publish_cancelled_at=candidate_before.get("publish_cancelled_at"),
                        )
                    raise
        self.store.append(run_id, "run/cancelled", {"reason": "approved_publish_cancelled_before_start"}, actor="control-plane")
        self.queue_wakeup.set()
        return result

    def _verify_published_candidate(self, item: Dict[str, Any]) -> Dict[str, Any]:
        candidate_id = str(item.get("candidate_id") or "")
        concept = str(item.get("concept") or "")
        candidate = self.learning.read_candidate(candidate_id)
        if candidate.get("status") != "published":
            raise RuntimeError(f"publish adapter exited without publishing Candidate: {candidate.get('status')}")
        ledger = self.learning.load_ledger()
        record = ledger.get(concept) if isinstance(ledger.get(concept), dict) else {}
        if record.get("last_candidate_id") != candidate_id:
            raise RuntimeError("Active ledger does not reference the published Candidate")
        if record.get("current_version") != candidate.get("proposed_version"):
            raise RuntimeError("Active ledger version does not match the published Candidate")
        page_path = self.concepts_root / "state" / "pages" / f"{concept}.md"
        if not page_path.is_file():
            raise RuntimeError("published Active page is missing")
        if content_hash(page_path.read_text(encoding="utf-8")) != candidate.get("approved_content_hash"):
            raise RuntimeError("published Active page differs from the approved Candidate snapshot")
        return candidate

    def _publish_worker(self) -> None:
        """Resume approved actions after a service restart, one concept at a time."""
        if CONCEPT_WORKFLOW_DISABLED:
            # This guard is deliberately before the loop and before any queue
            # read/claim/subprocess call.  It protects direct callers and stale
            # launch code that may still reference this method.
            return
        last_recovery_check = 0.0
        while True:
            item: Optional[Dict[str, Any]] = None
            with self._queue_transaction():
                rows = self._read_queue_unlocked()
                # A fresh running lease may still belong to the previous
                # process during a rolling restart.  Global concurrency remains
                # one, so this worker must not claim another item yet.
                if not any(candidate.get("status") == "running" for candidate in rows):
                    for candidate in rows:
                        if candidate.get("status") == "queued":
                            item = dict(candidate)
                            candidate["status"] = "running"
                            candidate["attempts"] = int(candidate.get("attempts", 0)) + 1
                            candidate["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                            candidate["heartbeat_at"] = candidate["updated_at"]
                            item.update(candidate)
                            self._write_queue_unlocked(rows)
                            break
            if item is None:
                if time.time() - last_recovery_check >= 60:
                    self._recover_running_queue_items()
                    last_recovery_check = time.time()
                self.queue_wakeup.wait(1.0)
                self.queue_wakeup.clear()
                continue
            run_id = str(item["run_id"])
            try:
                self.store.append(run_id, "action/started", {"candidate_id": item["candidate_id"], "concept": item["concept"], "attempt": item["attempts"]}, actor="control-plane")
                log_path = self.store.paths(run_id).runner_log
                log_path.parent.mkdir(parents=True, exist_ok=True)
                command = [sys.executable, str(self.concepts_root / "scripts" / "refresh.py"), "--publish", str(item["candidate_id"])]
                with log_path.open("a", encoding="utf-8") as log_stream:
                    result = subprocess.run(command, cwd=str(self.concepts_root), stdout=log_stream, stderr=subprocess.STDOUT, text=True, timeout=900, check=False)
                if result.returncode != 0:
                    raise RuntimeError(f"publish worker exited with code {result.returncode}")
                self._verify_published_candidate(item)
                self.learning.update_candidate(
                    str(item["candidate_id"]),
                    expected_statuses={"published"},
                    publish_run_id=run_id,
                )
                self.store.append(run_id, "action/completed", {"candidate_id": item["candidate_id"], "concept": item["concept"]}, actor="control-plane")
                self.store.append(run_id, "verification/completed", {"checks": ["candidate_published", "ledger_candidate", "ledger_version", "approved_content_hash"], "ok": True}, actor="control-plane")
                self.store.append(run_id, "run/completed", {"candidate_id": item["candidate_id"], "concept": item["concept"]}, actor="control-plane")
                item["status"] = "completed"
            except Exception as exc:
                item["status"] = "failed"
                item["error"] = f"{type(exc).__name__}: {exc}"
                try:
                    candidate_id = str(item.get("candidate_id"))
                    concept = str(item.get("concept") or "")
                    with self.learning.concept_lock(concept):
                        self.learning.update_candidate(
                            candidate_id,
                            expected_statuses={"approved", "publishing"},
                            status="publish_failed",
                            error=item["error"],
                            failed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        )
                except (FileNotFoundError, ValueError):
                    pass
                self.store.append(run_id, "action/failed", {"candidate_id": item.get("candidate_id"), "error": item["error"]}, actor="control-plane")
                self.store.append(run_id, "run/failed", {"error": item["error"]}, actor="control-plane")
            item["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            item["heartbeat_at"] = None
            with self._queue_transaction():
                current_rows = self._read_queue_unlocked()
                current_item = next((row for row in current_rows if row.get("queue_id") == item.get("queue_id")), None)
                if current_item is not None and current_item.get("status") == "running":
                    current_item.update(
                        {
                            "status": item.get("status"),
                            "attempts": item.get("attempts"),
                            "error": item.get("error"),
                            "updated_at": item.get("updated_at"),
                            "heartbeat_at": None,
                        }
                    )
                    self._write_queue_unlocked(current_rows)

    def stage_review(self, name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._concept_write_rejected()
        with self._review_transaction():
            # Validation and persistence are one linearizable review action.
            # Otherwise commit can approve the Candidate after this request
            # validates but before it writes a now-impossible staged decision.
            record = self.concept(name)
            action = str(payload.get("action") or "")
            note = str(payload.get("note") or "").strip()
            candidate_id = str(payload.get("candidate_id") or (record.get("candidate") or {}).get("candidate_id") or "")
            if action not in REVIEW_ACTIONS:
                raise ValueError("action must be pause, changes, or approve")
            if action != "approve" and not note:
                raise ValueError("note is required for pause or changes")
            if not candidate_id:
                raise ValueError("review requires a Candidate revision")
            candidate: Dict[str, Any] = {}
            candidate_content_hash: Optional[str] = None
            if candidate_id:
                candidate = self.learning.read_candidate(candidate_id)
                if candidate.get("concept") != name:
                    raise ValueError("candidate does not belong to concept")
                self._validate_reviewable_status(candidate, action)
            if action == "approve" and not self._candidate_source_uris(candidate):
                raise ValueError("candidate cannot be approved without evidence sources")
            if action == "approve":
                candidate_content_hash = self._candidate_content_snapshot(candidate)
            staged = self._read_staged()
            staged[name] = {
                "action": action,
                "note": note,
                "candidate_id": candidate_id or None,
                "candidate_base_sha256": candidate.get("base_page_sha256") if candidate_id else None,
                "candidate_base_version": candidate.get("base_version") if candidate_id else None,
                "candidate_content_hash": candidate_content_hash,
                "actor": "zhujie14",
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            self._write_staged(staged)
            return staged[name]

    def remove_review(self, name: str) -> Dict[str, Any]:
        self._concept_write_rejected()
        self.concept(name)
        with self._review_transaction():
            staged = self._read_staged()
            staged.pop(name, None)
            self._write_staged(staged)
            return {"staged": staged}

    def commit_reviews(self) -> Dict[str, Any]:
        self._concept_write_rejected()
        # A double-click or two browser tabs must materialize one batch only.
        with self._review_transaction():
            return self._commit_reviews_locked()

    def _validate_staged_review(self, name: str, item: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[Dict[str, Any]], str, str]:
        """Re-read all mutable review inputs immediately before materializing a Run."""
        record = self.concept(name)
        action = str(item.get("action") or "")
        candidate_id = str(item.get("candidate_id") or "")
        if action not in REVIEW_ACTIONS:
            raise ValueError("staged action is invalid")
        if action != "approve" and not str(item.get("note") or "").strip():
            raise ValueError("note is required for pause or changes")
        if not candidate_id:
            raise ValueError("review requires a Candidate revision")
        candidate: Optional[Dict[str, Any]] = None
        if candidate_id:
            candidate = self.learning.read_candidate(candidate_id)
            if candidate.get("concept") != name:
                raise ValueError("candidate does not belong to concept")
            self._validate_reviewable_status(candidate, action)
        if action == "approve" and candidate is not None:
            if not self._candidate_source_uris(candidate):
                raise ValueError("candidate cannot be approved without evidence sources")
            expected_sha = item.get("candidate_base_sha256")
            if expected_sha and candidate.get("base_page_sha256") != expected_sha:
                raise ValueError("candidate base changed after staging")
            expected_version = item.get("candidate_base_version")
            if expected_version and candidate.get("base_version") != expected_version:
                raise ValueError("candidate base version changed after staging")
            expected_content_hash = str(item.get("candidate_content_hash") or "")
            if not expected_content_hash or self._candidate_content_snapshot(candidate) != expected_content_hash:
                raise ValueError("candidate content changed after staging")
        return record, candidate, action, candidate_id

    def _commit_reviews_locked(self) -> Dict[str, Any]:
        if CONCEPT_WORKFLOW_DISABLED:
            self._concept_write_rejected()
        staged = self._read_staged()
        submitted = []
        failed = []
        batch_id = f"batch-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
        for name, item in staged.items():
            run_id: Optional[str] = None
            try:
                # Proposal generation uses the same per-concept lock. This
                # makes the final staged-state recheck and Candidate transition
                # one atomic decision relative to a concurrent weekly refresh.
                with self.learning.concept_lock(name):
                    record, candidate, action, candidate_id = self._validate_staged_review(name, item)
                    request = self.store.create(
                        {
                            "loop_id": "concept-review",
                            "trigger": {"kind": "manual_review", "actor": "zhujie14", "batch_id": batch_id},
                            "scope": {"concept": name, "action": action, "note": item.get("note"), "candidate_id": candidate_id or None},
                            "permission_mode": "approved_action",
                            "record": True,
                        }
                    )
                    run_id = request["run_id"]
                    self.store.append(run_id, "gate/requested", {"concept": name, "action": action, "candidate_id": candidate_id or None, "batch_id": batch_id}, actor="reviewer")
                    event_type = REVIEW_ACTIONS[action]
                    self.store.append(run_id, event_type, {"concept": name, "note": item.get("note"), "candidate_id": candidate_id or None}, actor="reviewer")
                    if action == "approve":
                        previous_status = str((candidate or {}).get("status") or "ready_for_review")
                        self.learning.update_candidate(
                            candidate_id,
                            expected_statuses={previous_status},
                            status="approved",
                            approved_by="zhujie14",
                            approved_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            approved_content_hash=item.get("candidate_content_hash"),
                            approval_note=item.get("note"),
                            approval_run_id=run_id,
                        )
                        try:
                            self.store.append(run_id, "run/started", {"concept": name, "candidate_id": candidate_id, "mode": "approved_action"}, actor="control-plane")
                            self.store.append(run_id, "action/queued", {"candidate_id": candidate_id, "concept": name, "queue": "approved_action"}, actor="control-plane")
                            queue_item = self._enqueue_publish(run_id, candidate_id, name)
                        except Exception:
                            # A normal I/O failure is compensated immediately;
                            # a hard crash is recovered from approval_run_id on
                            # the next Control Plane start.
                            self.learning.update_candidate(
                                candidate_id,
                                expected_statuses={"approved"},
                                status=previous_status,
                                approved_by=None,
                                approved_at=None,
                                approved_content_hash=None,
                                approval_note=None,
                                approval_run_id=None,
                                error="approval queue persistence failed",
                            )
                            raise
                    else:
                        if candidate_id:
                            self.learning.update_candidate(
                                candidate_id,
                                expected_statuses={str((candidate or {}).get("status") or "")},
                                status="paused" if action == "pause" else "changes_requested",
                                reviewed_by="zhujie14",
                                reviewed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                review_note=item.get("note"),
                            )
                        queue_item = None
                    submitted.append({"concept": record, "run": self.store.state(run_id), "queue": queue_item})
            except Exception as exc:
                if run_id:
                    try:
                        if self.store.state(run_id).get("status") not in TERMINAL_STATES:
                            self.store.append(run_id, "run/failed", {"error": f"{type(exc).__name__}: {exc}"}, actor="control-plane")
                    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
                        pass
                failed.append({"concept": name, "error": f"{type(exc).__name__}: {exc}"})
                continue
        # Clear only entries that were successfully materialized.  A failed item
        # remains visible so the reviewer can correct its candidate or note.
        remaining = {name: item for name, item in staged.items() if not any(row.get("concept", {}).get("name") == name for row in submitted)}
        self._write_staged(remaining)
        return {"batch_id": batch_id, "submitted": submitted, "failed": failed, "staged": remaining, "queue": self.queue_status()}

    def request_agent_refresh(self, name: str) -> Dict[str, Any]:
        self._concept_write_rejected()
        record = self.concept(name)
        request = self.store.create(
            {
                "loop_id": "concept-review",
                "trigger": {"kind": "manual_agent_request", "actor": "zhujie14"},
                "scope": {"concept": name, "intent": "evidence_refresh"},
                "permission_mode": "approved_action",
                "record": True,
            }
        )
        run_id = request["run_id"]
        self.store.append(run_id, "run/started", {"runtime": "codex", "concept": name, "mode": "propose-only"}, actor="control-plane")
        self.store.append(run_id, "agent/requested", {"concept": name, "mode": "one-shot-proposal"}, actor="reviewer")
        refresh_script = self.concepts_root / "scripts" / "refresh.py"
        if not refresh_script.is_file():
            self.store.append(run_id, "run/failed", {"error": f"refresh script missing: {refresh_script}"}, actor="control-plane")
            return self.store.state(run_id)
        log_path = self.store.paths(run_id).runner_log
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_stream = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, str(refresh_script), "--propose", name, "--run-id", run_id],
            cwd=str(self.concepts_root),
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            text=True,
        )
        with self.lock:
            self.processes[run_id] = process
        threading.Thread(target=self._wait_agent_refresh, args=(run_id, name, process, log_stream), daemon=True).start()
        return self.store.state(run_id)

    def refresh_status(
        self,
        candidates: Optional[Iterable[Dict[str, Any]]] = None,
        usage: Optional[Dict[str, Any]] = None,
        discovery_runs: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        marker_path = self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.done"
        running_path = self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.running"
        plist_path = self.codex_root.parent / "Library" / "LaunchAgents" / "com.zhujie14.weekly-sync-and-refresh.plist"
        marker: Dict[str, Any] = {}
        if marker_path.is_file():
            try:
                value = json.loads(marker_path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    marker = value
            except (OSError, json.JSONDecodeError):
                marker = {}
        schedule = None
        if plist_path.is_file():
            try:
                import plistlib

                plist = plistlib.loads(plist_path.read_bytes())
                interval = plist.get("StartCalendarInterval") or {}
                weekday = {1: "周一", 2: "周二", 3: "周三", 4: "周四", 5: "周五", 6: "周六", 7: "周日"}.get(interval.get("Weekday"), "每周")
                schedule = f"{weekday} {int(interval.get('Hour', 0)):02d}:{int(interval.get('Minute', 0)):02d}"
            except (OSError, ValueError, TypeError):
                schedule = None
        legacy_label = "com.zhujie14.weekly-sync-and-refresh"
        loaded = False
        try:
            loaded = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{legacy_label}"], capture_output=True, check=False, text=True, timeout=2).returncode == 0
        except (OSError, subprocess.SubprocessError):
            loaded = False
        scheduler_label = "com.zhujie14.pm-scheduler"
        scheduler_loaded = False
        try:
            scheduler_loaded = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{scheduler_label}"], capture_output=True, check=False, text=True, timeout=2).returncode == 0
        except (OSError, subprocess.SubprocessError):
            scheduler_loaded = False
        control_label = "com.zhujie14.pm-loop-control-plane"
        control_loaded = False
        try:
            control_loaded = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{control_label}"], capture_output=True, check=False, text=True, timeout=2).returncode == 0
        except (OSError, subprocess.SubprocessError):
            control_loaded = False
        candidates = list(candidates) if candidates is not None else self._read_candidates_cached()
        usage_payload = usage if usage is not None else self.learning.usage_summary()
        discovery_rows = list(discovery_runs) if discovery_runs is not None else self.learning.discovery_runs()
        usage_producer = self.concepts_root / "scripts" / "record-usage.py"
        triage_runner = self.project_root / "scripts" / "concept_discovery_triage.py"
        signal_runner = self.project_root / "scripts" / "concept_signal_discovery.py"
        proposal_mode = False
        try:
            weekly_text = (self.concepts_root / "scripts" / "weekly-refresh.sh").read_text(encoding="utf-8")
            proposal_mode = "--propose" in weekly_text or "propose" in weekly_text.lower()
        except OSError:
            pass
        inventory = self.deep_inventory_status()
        concept_workflow = self.concept_workflow_status()
        concept_freshness = self._freshness_item(
            "concepts",
            "概念卡（V11 恢复门禁）",
            "shengsuan-concepts/state/concepts-ledger.json",
            self.concepts_ledger_path,
            14 * 24,
            "概念刷新由 PM Scheduler 依赖事件触发；旧 Control Plane 写接口保持禁用。",
            observed_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )
        concept_freshness.update(
            {
                "status": concept_workflow["status"],
                "display_status": "恢复门禁已就绪" if concept_workflow["status"] == "recovery_gated" else "需要核查",
                "raw_status": concept_freshness.get("status"),
                "disabled": False,
                "history_only": False,
                "request": None,
                "reason": concept_workflow["reason"],
            }
        )
        return {
            "job": scheduler_label,
            "schedule_key": "weekly-sync-and-refresh",
            "loaded": scheduler_loaded,
            "disabled": False,
            "read_only": True,
            "history_only": False,
            "reason": concept_workflow["reason"],
            "schedule": "PM Scheduler canonical registry",
            "running": False,
            "pipeline": ["weekly-sync-and-refresh", "concept-refresh-planner"],
            "legacy_launchagent": {
                "label": legacy_label,
                "loaded": loaded,
                "plist_exists": plist_path.is_file(),
                "schedule": schedule,
                "running": running_path.exists(),
                "last_run": marker or None,
                "disabled": True,
                "reason": "已由统一 PM Scheduler 替代；仅保留回滚文件。",
            },
            "concept_refresh": {**concept_workflow, "freshness": concept_freshness},
            "writes_active_directly": False,
            "human_gate": concept_workflow.get("admission", {}).get("blocks_unapproved_publish", True),
            "usage_telemetry": {
                "events": usage_payload.get("events", 0) if isinstance(usage_payload, dict) else 0,
                "producer_wired": usage_producer.is_file(),
                "producer_path": str(usage_producer),
                "ingest_endpoint": "/api/usage",
                "note": "shengsuan-concepts 在命中、未命中和人工纠正时写入；历史事件从接入后开始累计",
            },
            "new_concept_discovery": {
                "runs": len(discovery_rows),
                "triaged_candidates": sum(len(item.get("candidate_ids") or []) for item in discovery_rows if isinstance(item, dict)),
                "disabled": True,
                "history_only": True,
                "triage_agent_wired": False,
                "triage_runner_path": str(triage_runner),
                "note": "历史 Candidate discovery 仅供审计；当前刷新由 concept-refresh-planner 按依赖事件规划。",
            },
            # Keep persisted counts for history/audit, but expose a separate
            # actionable view that is always empty while the workflow is
            # retired.  This prevents stale approved/queued rows from being
            # interpreted as work to execute.
            "candidate_counts": {
                "total": len(candidates),
                "ready_for_review": sum(1 for item in candidates if item.get("status") == "ready_for_review"),
                "approved": sum(1 for item in candidates if item.get("status") == "approved"),
                "published": sum(1 for item in candidates if item.get("status") == "published"),
                "failed": sum(1 for item in candidates if item.get("status") == "failed"),
                "publish_failed": sum(1 for item in candidates if item.get("status") == "publish_failed"),
            },
            "candidate_history_counts": self._candidate_counts(candidates),
            "candidate_actionable": 0,
            "pending": 0,
            "actionable": False,
            "control_plane": {"job": control_label, "loaded": control_loaded, "resident_expected": True},
            "deep_inventory": inventory,
            "queue": self.queue_status(),
            "discovery_sources": [
                {"id": "document_delta", "label": "同步文档新增/更新", "source_exists": True, "wired": False, "disabled": True},
                {"id": "requirement_term", "label": "需求评估中的术语待澄清", "source_exists": True, "wired": False, "disabled": True},
                {"id": "pm_timeline", "label": "PM 时间轴高频术语与能力缺口", "source_exists": True, "wired": False, "disabled": True},
                {"id": "ontology_change", "label": "Ontology 类型、字段和能力变化", "source_exists": True, "wired": False, "disabled": True},
                {"id": "agent_usage", "label": "Agent 未命中、低置信度和人工改写", "source_exists": usage_producer.is_file(), "wired": False, "disabled": True},
                {"id": "manual_seed", "label": "本人手工提议新概念", "source_exists": True, "wired": False, "disabled": True},
            ],
            "legacy_proposal_mode": proposal_mode,
        }

    @staticmethod
    def _legacy_inventory_stage_progress(
        result: Mapping[str, Any],
        manifest: Mapping[str, Any],
        resource_count: int,
    ) -> Dict[str, Dict[str, Any]]:
        """Derive useful stage counters for manifests written before v2 telemetry.

        The first deep-inventory run predates ``stage_progress``.  Its
        manifest still contains the durable aggregate counters (``progress``,
        ``evidence``, ``llm`` and the nested result), so the read-only API can
        show what actually completed without inventing timings.  Every value
        returned here is marked as ``legacy_derived``; a missing counter stays
        ``not_recorded`` rather than becoming a misleading zero.
        """

        def first_value(*values: Any) -> Any:
            for value in values:
                if value is not None:
                    return value
            return None

        def as_count(value: Any) -> Optional[int]:
            if value is None or isinstance(value, bool):
                return None
            try:
                return max(0, int(value))
            except (TypeError, ValueError, OverflowError):
                return None

        def section(value: Any) -> Mapping[str, Any]:
            return value if isinstance(value, Mapping) else {}

        # Historical manifests keep the result both as a nested object and,
        # in newer sidecars, as the top-level latest-result object.  Prefer
        # the sidecar when a field exists, while retaining legacy-only fields.
        nested_result = section(manifest.get("result"))
        merged_result: Dict[str, Any] = dict(nested_result)
        merged_result.update({key: value for key, value in result.items() if value is not None})

        progress = section(manifest.get("progress"))
        evidence = section(manifest.get("evidence"))
        llm = section(manifest.get("llm"))
        triage_selection = section(manifest.get("triage_selection"))
        triage_result = section(merged_result.get("triage"))
        run_status = str(
            first_value(merged_result.get("status"), manifest.get("status"), "unknown")
        ).lower()
        terminal_success = run_status in {"completed", "complete", "done", "success"}
        terminal_failure = run_status in {"failed", "error", "cancelled", "canceled", "rejected"}

        def status_for(processed: Optional[int], total: Optional[int], available: bool) -> str:
            if not available:
                return "not_recorded"
            if terminal_failure:
                # A failed run may still have completed an earlier stage.
                # Preserve that evidence instead of painting every stage red.
                if processed is not None and total is not None and processed >= total:
                    return "completed"
                return "failed"
            if terminal_success:
                return "completed"
            if processed is not None and total is not None and total > 0 and processed >= total:
                return "completed"
            if processed is not None:
                return "running"
            return "not_recorded"

        def record(
            processed: Optional[int],
            total: Optional[int],
            *,
            errors: Optional[int] = None,
            cache_hits: Optional[int] = None,
            cache_misses: Optional[int] = None,
            skip_reason: Optional[str] = None,
        ) -> Dict[str, Any]:
            available = processed is not None or total is not None
            value: Dict[str, Any] = {
                "status": status_for(processed, total, available),
                "telemetry_source": "legacy_derived",
            }
            if processed is not None:
                value["processed"] = processed
            if total is not None:
                value["total"] = total
            if errors is not None:
                value["errors"] = errors
            if cache_hits is not None:
                value["cache_hits"] = cache_hits
            if cache_misses is not None:
                value["cache_misses"] = cache_misses
            if skip_reason:
                value["skip_reason"] = skip_reason
            return value

        total_documents = as_count(
            first_value(progress.get("total"), manifest.get("resource_count"), resource_count)
        )
        read_documents = as_count(
            first_value(
                merged_result.get("read_count"),
                progress.get("read"),
                progress.get("processed"),
            )
        )
        unreadable = as_count(first_value(merged_result.get("unreadable_count"), progress.get("unreadable")))
        stages: Dict[str, Dict[str, Any]] = {
            "document_read": record(
                read_documents,
                total_documents,
                errors=unreadable,
                cache_hits=as_count(evidence.get("cache_hits")),
                cache_misses=as_count(evidence.get("cache_misses")),
            )
        }

        term_count = as_count(
            first_value(
                merged_result.get("term_count"),
                triage_result.get("observed_term_count"),
                triage_selection.get("observed_term_count"),
            )
        )
        stages["term_aggregation"] = record(
            term_count,
            term_count,
            skip_reason=None if term_count is not None else "legacy_term_count_unavailable",
        )

        llm_batches = as_count(llm.get("batch_count"))
        llm_completed = as_count(llm.get("completed_batches"))
        decision_count = as_count(first_value(merged_result.get("decision_count"), len(merged_result.get("decisions") or []) if isinstance(merged_result.get("decisions"), list) else None))
        if llm_batches is not None and llm_batches > 0:
            stages["llm_reduce"] = record(
                llm_completed if llm_completed is not None else 0,
                llm_batches,
            )
        elif decision_count is not None and decision_count > 0:
            # Older runs did not persist LLM batch counters, but a non-zero
            # decision count proves that the reduce stage produced output.
            stages["llm_reduce"] = record(
                decision_count,
                decision_count,
                skip_reason="legacy_decision_count_fallback",
            )
        else:
            stages["llm_reduce"] = record(
                None,
                None,
                skip_reason="legacy_llm_telemetry_unavailable",
            )

        candidate_ids = first_value(merged_result.get("candidate_ids"), manifest.get("candidate_ids"))
        candidate_id_count = len(candidate_ids) if isinstance(candidate_ids, list) else None
        candidate_count = as_count(first_value(merged_result.get("candidate_count"), candidate_id_count))
        stages["candidate_write"] = record(
            candidate_count,
            candidate_count,
            skip_reason=None if candidate_count is not None else "legacy_candidate_count_unavailable",
        )
        return stages

    def deep_inventory_status(self) -> Dict[str, Any]:
        """Expose the latest deep-inventory cost and cache facts read-only."""
        state_root = self.concepts_root / "state" / "full-inventory"
        result_path = state_root / "latest-result.json"
        result = self._read_json_file(result_path, {})
        if not isinstance(result, dict):
            result = {}
        run_id = str(result.get("run_id") or "")
        manifest = self._read_json_file(state_root / "runs" / run_id / "manifest.json", {}) if run_id else {}
        if not isinstance(manifest, dict):
            manifest = {}
        # ``latest-result.json`` is written only after a run reaches a result
        # boundary.  A newer manifest can therefore describe a live run while
        # latest-result still points to an older completion.  Prefer that live
        # manifest for the status projection and let it override stale result
        # fields, while retaining result-only counters.
        try:
            manifest_paths = [p for p in (state_root / "runs").glob("*/manifest.json") if p.is_file()]
        except OSError:
            manifest_paths = []

        def manifest_stamp(path: Path) -> float:
            value = self._read_json_file(path, {})
            stamp = self._queue_timestamp(value.get("updated_at") or value.get("completed_at") or value.get("created_at")) if isinstance(value, dict) else None
            if stamp is not None:
                return stamp
            try:
                return path.stat().st_mtime
            except OSError:
                return 0.0

        newest_manifest = max(manifest_paths, key=manifest_stamp, default=None)
        if newest_manifest is not None:
            newest = self._read_json_file(newest_manifest, {})
            newest = newest if isinstance(newest, dict) else {}
            newest_id = str(newest.get("run_id") or newest_manifest.parent.name or "")
            result_stamp = self._queue_timestamp(result.get("updated_at") or result.get("finished_at") or result.get("completed_at"))
            newest_status = str(newest.get("status") or "").casefold()
            if (
                newest_id != run_id and manifest_stamp(newest_manifest) >= (result_stamp or 0)
                or newest_status in {"queued", "running", "collecting", "analyzing", "verifying", "interrupted"}
            ):
                run_id = newest_id
                manifest = newest
        live_result = dict(result)
        live_result.update({key: value for key, value in manifest.items() if value is not None})
        result = live_result
        if not result and not manifest:
            return {
                "available": False,
                "state_root": str(state_root),
                "disabled": True,
                "read_only": True,
                "history_only": True,
                "status": "disabled",
                "display_status": "历史只读",
                "historical_status": "not_recorded",
                "reason": CONCEPT_WORKFLOW_REASON,
                "actionable": False,
                "pending": 0,
                "recoverable": False,
            }
        resource_count = int(result.get("resource_count") or manifest.get("resource_count") or 0)
        evidence = result.get("evidence_cache") if isinstance(result.get("evidence_cache"), dict) else {}
        if not evidence and isinstance(manifest.get("evidence"), dict):
            evidence = manifest["evidence"]
        cache_path = state_root / "evidence-cache.json.gz"
        if not cache_path.is_file():
            cache_path = state_root / "evidence-cache.json"
        content_dedup_path = state_root / "content-dedup.json.gz"
        if not content_dedup_path.is_file():
            content_dedup_path = state_root / "content-dedup.json"
        # Read only the tiny sidecar.  Parsing evidence-cache.json here would
        # make a status poll proportional to the entire deep-inventory cache.
        cache_meta = self._read_json_file(
            state_root / "evidence-cache.meta.json", {}
        )
        cache_meta_available = isinstance(cache_meta, dict) and "entry_count" in cache_meta
        entry_count = int(cache_meta.get("entry_count") or 0) if cache_meta_available else 0
        # The incremental-baseline sidecar is intentionally small and avoids
        # parsing the full evidence cache during a status poll.  Keep only
        # aggregate counters here; source revision maps stay runner-owned.
        baseline_raw = self._read_json_file(state_root / "incremental-baseline.json", {})
        baseline: Dict[str, Any] = {}
        baseline_source = baseline_raw if isinstance(baseline_raw, dict) and baseline_raw else {}
        if not baseline_source:
            for value in (result.get("baseline"), manifest.get("baseline")):
                if isinstance(value, dict) and value:
                    baseline_source = value
                    break
        if baseline_source:
            baseline["available"] = bool(baseline_source)
            baseline["path"] = str(state_root / "incremental-baseline.json")
            for key in (
                "status",
                "baseline_ready",
                "run_id",
                "materialized_at",
                "snapshot_hash",
                "resource_count",
                "deep_read_count",
                "deep_read_coverage",
                "source_hash_count",
                "source_hash_coverage",
                "content_hash_count",
                "evidence_cache_count",
                "evidence_cache_coverage",
            ):
                if key in baseline_source:
                    baseline[key] = baseline_source[key]
            for section in ("changed_documents", "content_dedup"):
                value = baseline_source.get(section)
                if isinstance(value, dict):
                    baseline[section] = {
                        str(key): value[key]
                        for key in value
                        if str(key).endswith(("count", "ratio", "coverage"))
                        or str(key) in {"changed_count", "unchanged_count", "unknown_revision_count", "new_count", "removed_count", "document_count", "unique_content_count", "duplicate_document_count", "duplicate_ratio"}
                    }
        # Stage progress is persisted in the manifest/result as a compact
        # sidecar.  Project only bounded scalar fields so a status poll never
        # opens evidence batches, term groups, or source revision maps.
        result_stage_source = result.get("stage_progress")
        manifest_stage_source = manifest.get("stage_progress")
        # A completed result may have been written before its manifest flush.
        # Merge both bounded sidecars so a useful manifest projection is not
        # discarded merely because the result contains an empty object.
        stage_source: Dict[str, Any] = {}
        if isinstance(manifest_stage_source, dict):
            stage_source.update(manifest_stage_source)
        if isinstance(result_stage_source, dict):
            stage_source.update(result_stage_source)
        stage_progress: Dict[str, Dict[str, Any]] = {}
        for name, value in stage_source.items():
            if not isinstance(value, dict):
                continue
            projected_stage: Dict[str, Any] = {}
            for key in (
                "status",
                "processed",
                "total",
                "elapsed_seconds",
                "cache_hits",
                "cache_misses",
                "errors",
                "cursor",
                "eta_seconds",
                "skip_reason",
                "telemetry_source",
            ):
                if key in value:
                    projected_stage[key] = value[key]
            stage_progress[str(name)] = projected_stage

        # Older runs have no stage telemetry, but their aggregate counters are
        # still sufficient to show completed work.  Fill only missing stages;
        # explicit v2 telemetry always wins.  This keeps the projection useful
        # for historical runs while never fabricating elapsed time.
        legacy_stage_progress = self._legacy_inventory_stage_progress(
            result,
            manifest,
            resource_count,
        )
        if stage_source:
            stage_progress_source = "native"
        else:
            stage_progress_source = "legacy_derived"
        for name in INVENTORY_STAGE_NAMES:
            if name not in stage_progress:
                derived = legacy_stage_progress.get(name)
                if derived:
                    stage_progress[name] = derived
                    if stage_source:
                        stage_progress_source = "mixed"
                else:
                    stage_progress[name] = {
                        "status": "not_recorded",
                        "skip_reason": "stage_not_recorded",
                    }

        def compact_stats(value: Any) -> Dict[str, Any]:
            if not isinstance(value, dict):
                return {}
            allowed = {
                "document_count",
                "unique_content_count",
                "duplicate_document_count",
                "duplicate_ratio",
                "changed_count",
                "unchanged_count",
                "unknown_revision_count",
                "new_count",
                "removed_count",
                "source_hash_count",
                "source_hash_coverage",
                "content_hash_count",
                "content_hash_coverage",
                "evidence_cache_count",
                "evidence_cache_coverage",
            }
            return {str(key): value[key] for key in value if str(key) in allowed}

        changed_documents = compact_stats(
            result.get("changed_documents")
            if isinstance(result.get("changed_documents"), dict)
            else manifest.get("changed_documents")
        )
        content_dedup = compact_stats(
            result.get("content_dedup")
            if isinstance(result.get("content_dedup"), dict)
            else manifest.get("content_dedup")
        )
        if not content_dedup and isinstance(baseline.get("content_dedup"), dict):
            content_dedup = dict(baseline["content_dedup"])
        if not changed_documents and isinstance(baseline.get("changed_documents"), dict):
            changed_documents = dict(baseline["changed_documents"])
        if not cache_meta_available:
            baseline_entry_count = baseline.get("evidence_cache_count")
            if baseline_entry_count is not None:
                entry_count = int(baseline_entry_count or 0)
                cache_meta_available = True
        source_hash_rows = int(
            evidence.get("source_hash_rows")
            or baseline.get("source_hash_count")
            or 0
        )
        duration_seconds = None
        start = self._queue_timestamp(manifest.get("created_at") or result.get("created_at"))
        finish = self._queue_timestamp(manifest.get("completed_at") or result.get("finished_at"))
        if start is not None and finish is not None and finish >= start:
            duration_seconds = round(finish - start, 1)
        historical_status = str(result.get("status") or manifest.get("status") or "unknown")
        return {
            "available": True,
            "run_id": run_id,
            # Keep the raw terminal state for audit consumers, while exposing
            # an explicit disabled display state so a historical interrupted
            # run cannot be mistaken for work that should be resumed.
            "status": "disabled",
            "display_status": "历史只读",
            "raw_status": historical_status,
            "historical_status": historical_status,
            "disabled": True,
            "read_only": True,
            "history_only": True,
            "reason": CONCEPT_WORKFLOW_REASON,
            "actionable": False,
            "pending": 0,
            "resource_count": resource_count,
            "read_count": int(result.get("read_count") or 0),
            "term_count": int(result.get("term_count") or 0),
            "candidate_count": int(result.get("candidate_count") or 0),
            "duration_seconds": duration_seconds,
            "evidence_cache": {
                "entries": entry_count,
                "coverage": (
                    round(entry_count / resource_count, 4)
                    if cache_meta_available and resource_count
                    else (1.0 if cache_meta_available else None)
                ),
                "coverage_known": cache_meta_available,
                "cache_hits": int(evidence.get("cache_hits") or 0),
                "cache_misses": int(evidence.get("cache_misses") or 0),
                "source_hash_rows": source_hash_rows,
                "path": str(cache_path),
            },
            "incremental_baseline": baseline or {"available": False, "path": str(state_root / "incremental-baseline.json")},
            "changed_documents": changed_documents,
            "content_dedup": content_dedup,
            "content_dedup_path": str(content_dedup_path),
            "stage_progress_source": stage_progress_source,
            "stage_progress": stage_progress,
        }

    @classmethod
    def _latest_file(cls, root: Path, pattern: Path) -> Optional[Path]:
        """Return the newest allowed file under a fixed root.

        ``mtime`` is discovery metadata only.  Callers must not use it as the
        report's business generation time or as proof that a report is fresh.
        """
        safe_exact = cls._safe_regular_file(pattern, root)
        if safe_exact is not None:
            return safe_exact
        try:
            candidates = [
                safe
                for item in pattern.parent.glob(pattern.name)
                if (safe := cls._safe_regular_file(item, root)) is not None
            ]
        except OSError:
            return None
        return max(candidates, key=lambda item: item.stat().st_mtime_ns, default=None)

    @classmethod
    def _freshness_item(
        cls,
        item_id: str,
        label: str,
        source: str,
        path: Optional[Path],
        threshold_hours: float,
        request: str,
        *,
        directory_glob: str = "*.json",
        observed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        observed = observed_at or now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        signature = cls._source_signature_value(path, directory_glob=directory_glob)
        if path is None or not path.is_file():
            return {
                "id": item_id,
                "label": label,
                "source": source,
                "path": str(path) if path else None,
                "updated_at": None,
                "age_hours": None,
                "threshold_hours": threshold_hours,
                "status": "missing",
                "request": request,
                "observed_at": observed,
                "source_signature": signature,
                "read_status": "missing",
            }
        try:
            updated = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return {
                "id": item_id,
                "label": label,
                "source": source,
                "path": str(path),
                "updated_at": None,
                "age_hours": None,
                "threshold_hours": threshold_hours,
                "status": "missing",
                "request": request,
                "observed_at": observed,
                "source_signature": signature,
                "read_status": "unreadable",
            }
        age_hours = max(0.0, (now - updated).total_seconds() / 3600)
        return {
            "id": item_id,
            "label": label,
            "source": source,
            "path": str(path),
            "updated_at": updated.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "age_hours": round(age_hours, 1),
            "threshold_hours": threshold_hours,
            "status": "fresh" if age_hours <= threshold_hours else "stale",
            "request": request,
            "observed_at": observed,
            "source_signature": signature,
            "read_status": "ok",
        }

    def _inventory_projection(self, inventory: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Project the latest inventory run without opening large artifacts."""
        state_root = self.concepts_root / "state" / "full-inventory"
        result_path = state_root / "latest-result.json"
        result = self._read_json_file(result_path, {})
        result = result if isinstance(result, dict) else {}
        run_id = str(result.get("run_id") or "")
        manifest_path = state_root / "runs" / run_id / "manifest.json" if run_id else None
        manifest = self._read_json_file(manifest_path, {}) if manifest_path else {}
        manifest = manifest if isinstance(manifest, dict) else {}
        # A process may write a new manifest while latest-result still points
        # at the previous completed run.  Select the newest manifest by its
        # persisted update timestamp (mtime is the fallback) so the display
        # cannot hide a currently running or interrupted inventory.
        try:
            manifest_paths = [p for p in (state_root / "runs").glob("*/manifest.json") if p.is_file()]
        except OSError:
            manifest_paths = []

        def manifest_stamp(path: Path) -> float:
            value = self._read_json_file(path, {})
            stamp = self._queue_timestamp(value.get("updated_at") or value.get("completed_at") or value.get("created_at")) if isinstance(value, dict) else None
            if stamp is not None:
                return stamp
            try:
                return path.stat().st_mtime
            except OSError:
                return 0.0

        newest_manifest = max(manifest_paths, key=manifest_stamp, default=None)
        if newest_manifest is not None:
            newest = self._read_json_file(newest_manifest, {})
            newest = newest if isinstance(newest, dict) else {}
            newest_id = str(newest.get("run_id") or newest_manifest.parent.name or "")
            result_stamp = self._queue_timestamp(
                result.get("updated_at") or result.get("finished_at") or result.get("completed_at")
            )
            newest_stamp = manifest_stamp(newest_manifest)
            # Prefer the manifest on ties when it explicitly describes an
            # active/non-terminal run; otherwise retain latest-result's richer
            # completed artifact and merge the manifest below.
            newest_status = str(newest.get("status") or "").casefold()
            prefer_manifest = (
                not result
                or newest_id != run_id and newest_stamp >= (result_stamp or 0)
                or newest_status in {"queued", "running", "collecting", "analyzing", "verifying", "interrupted"}
            )
            if prefer_manifest:
                manifest_path = newest_manifest
                manifest = newest
                run_id = newest_id
                nested_result = manifest.get("result")
                if isinstance(nested_result, dict) and nested_result:
                    result = nested_result
        # Manifest is the live run control record; latest-result is the richer
        # completed artifact.  Let manifest status/cursor fields win while
        # retaining counters that only the result persisted.
        merged: Dict[str, Any] = dict(result)
        merged.update({key: value for key, value in manifest.items() if value is not None})
        progress = merged.get("progress") if isinstance(merged.get("progress"), dict) else {}
        stage_progress = merged.get("stage_progress") if isinstance(merged.get("stage_progress"), dict) else {}
        document_stage = stage_progress.get("document_read") if isinstance(stage_progress.get("document_read"), dict) else {}
        total = self._count_value(
            self._first_value(
                merged.get("resource_count"),
                progress.get("total"),
                document_stage.get("total"),
            )
        )
        cursor = self._count_value(
            self._first_value(
                merged.get("scan_cursor"),
                progress.get("processed"),
                progress.get("read"),
                document_stage.get("cursor"),
                document_stage.get("processed"),
            )
        )
        status = str(merged.get("status") or "not_recorded")
        error = self._first_value(merged.get("error"), merged.get("last_error"))
        recoverable = bool(merged.get("recoverable") or merged.get("resume_available"))
        # Older inventory manifests used SIGTERM as a plain error.  Preserve
        # the actionable interrupted semantics without relabeling a logical
        # failure as recoverable.
        if not recoverable and status in {"failed", "error", "interrupted"}:
            error_text = str(error or "").casefold()
            recoverable = "sigterm" in error_text or "exited -15" in error_text or "signal 15" in error_text
        if recoverable and status in {"failed", "error"}:
            display_status = "interrupted"
        else:
            display_status = status
        updated_at = self._first_value(
            merged.get("updated_at"),
            merged.get("finished_at"),
            merged.get("completed_at"),
            merged.get("created_at"),
        )
        if not updated_at:
            try:
                updated_at = datetime.fromtimestamp(
                    (manifest_path or result_path).stat().st_mtime,
                    tz=timezone.utc,
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except OSError:
                updated_at = None
        triage = merged.get("triage") if isinstance(merged.get("triage"), dict) else {}
        candidates_from_inventory = self._count_value(
            self._first_value(merged.get("candidate_count"), triage.get("candidate_count"))
        )
        new_names = self._count_value(
            self._first_value(
                merged.get("new_name_count"),
                merged.get("new_names"),
                merged.get("discovered_new_concept_count"),
                triage.get("new_name_count"),
                triage.get("new_concept_count"),
            )
        )
        triaged_terms = self._count_value(
            self._first_value(
                merged.get("triaged_term_count"),
                triage.get("triaged_term_count"),
                merged.get("decision_count"),
            )
        )
        value = {
            "available": bool(result or manifest),
            "run_id": run_id or None,
            "status": display_status,
            "raw_status": status,
            "error": str(error) if error else None,
            "recoverable": False,
            "historical_recoverable": recoverable,
            "cursor": cursor,
            "total": total,
            "progress_ratio": round(cursor / total, 6) if cursor is not None and total else None,
            "new_names": new_names,
            "triaged_terms": triaged_terms,
            "candidates_from_inventory": candidates_from_inventory,
            "updated_at": updated_at,
            "manifest_path": str(manifest_path) if manifest_path else None,
        }
        if inventory:
            # Keep the bounded cost/cache/stage projection under the run row
            # while exposing the concise cursor fields at the top level.
            value["detail"] = inventory
            if value["status"] == "not_recorded":
                value["status"] = str(inventory.get("status") or value["status"])
            if inventory.get("disabled"):
                value["historical_status"] = (
                    inventory.get("historical_status")
                    or value.get("raw_status")
                    or value.get("status")
                )
                value["status"] = "disabled"
                value["display_status"] = "历史只读"
                value["disabled"] = True
                value["read_only"] = True
                value["history_only"] = True
                value["reason"] = CONCEPT_WORKFLOW_REASON
                value["actionable"] = False
                value["pending"] = 0
                value["historical_recoverable"] = bool(
                    inventory.get("historical_recoverable", value.get("historical_recoverable"))
                )
                value["recoverable"] = False
        if CONCEPT_WORKFLOW_DISABLED:
            # The projection is also called directly by a few health/reporting
            # paths without passing the detailed inventory object.  Normalize
            # that route too, so an old live/interrupted manifest can never
            # advertise a resumable run.
            value.setdefault("historical_status", value.get("raw_status") or value.get("status"))
            value["status"] = "disabled"
            value["display_status"] = "历史只读"
            value["disabled"] = True
            value["read_only"] = True
            value["history_only"] = True
            value["actionable"] = False
            value["pending"] = 0
            value["reason"] = CONCEPT_WORKFLOW_REASON
            value["recoverable"] = False
        return value

    def _discovery_source_projection(
        self,
        discovery_rows: Iterable[Dict[str, Any]],
        usage: Optional[Dict[str, Any]] = None,
    ) -> list[Dict[str, Any]]:
        """Expose the six V3 discovery signals from current local artifacts."""
        rows = [row for row in discovery_rows if isinstance(row, dict)]
        usage_events = int((usage or {}).get("events") or 0) if isinstance(usage, dict) else 0
        state_root = self.concepts_root / "state"
        manual_seed_path = self.store.state_dir / "concept-review" / "manual-seeds.jsonl"
        requirement_path = self.codex_root / "skills" / "requirement-fit-assessment" / "state" / "index.jsonl"
        timeline_root = self.codex_root / "skills" / "pm-timeline" / "state" / "timeline"
        timeline_path = self._latest_file(timeline_root, timeline_root / "*.jsonl")
        weekly_path = self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.done"
        usage_path = self.learning.usage_root / "events.jsonl"
        signal_runner = self.project_root / "scripts" / "concept_signal_discovery.py"
        weekly_script = self.concepts_root / "scripts" / "weekly-refresh.sh"

        def source_rows(names: set[str]) -> list[Dict[str, Any]]:
            return [row for row in rows if str(row.get("source") or "").casefold() in names]

        def row_time(row: Dict[str, Any]) -> Optional[str]:
            return self._first_value(row.get("updated_at"), row.get("created_at"), row.get("finished_at"))

        def build(
            item_id: str,
            label: str,
            path: Optional[Path],
            wired: bool,
            aliases: set[str],
            note: str,
            *,
            produced_count: Optional[int] = None,
        ) -> Dict[str, Any]:
            matched = source_rows(aliases)
            latest = max(matched, key=lambda row: self._queue_timestamp(row_time(row)) or 0, default=None)
            if produced_count is None and latest is not None:
                produced_count = self._count_value(latest.get("candidate_count"))
                if not produced_count:
                    candidate_ids = latest.get("candidate_ids")
                    updated_uris = latest.get("updated_uris")
                    candidate_count = len(candidate_ids) if isinstance(candidate_ids, list) else 0
                    updated_count = len(updated_uris) if isinstance(updated_uris, list) else 0
                    produced_count = self._count_value(candidate_count or updated_count or None)
            exists = bool(path and path.is_file())
            status = "wired" if wired else ("available" if exists else "missing")
            if CONCEPT_WORKFLOW_DISABLED:
                status = "disabled"
            return {
                "id": item_id,
                "label": label,
                "wired": False if CONCEPT_WORKFLOW_DISABLED else wired,
                "status": status,
                "disabled": CONCEPT_WORKFLOW_DISABLED,
                "history_only": CONCEPT_WORKFLOW_DISABLED,
                "actionable": False,
                "source_exists": exists,
                "source": str(path) if path else None,
                "source_signature": self._source_signature_value(path),
                "last_run_id": latest.get("run_id") if latest else None,
                "last_run_at": row_time(latest) if latest else None,
                "produced_count": produced_count,
                "run_count": len(matched),
                "note": CONCEPT_WORKFLOW_REASON if CONCEPT_WORKFLOW_DISABLED else note,
            }

        return [
            build(
                "document_delta",
                "同步文档新增/更新",
                weekly_path,
                weekly_script.is_file(),
                {"weekly-document-delta", "weekly_document_delta", "full_inventory"},
                "weekly-refresh 的文档增量入口",
            ),
            build(
                "requirement_term",
                "需求评估未支持/高频术语",
                requirement_path,
                signal_runner.is_file() and requirement_path.is_file(),
                {"requirement_term", "requirement-fit", "requirement_fit"},
                "从需求评估索引提取待澄清能力词",
            ),
            build(
                "pm_timeline",
                "PM 时间轴高频术语",
                timeline_path,
                signal_runner.is_file() and timeline_path is not None,
                {"pm_usage_signals", "pm-timeline", "pm_timeline"},
                "从时间轴事件提取反复出现但未入账的术语",
            ),
            build(
                "agent_usage",
                "Agent 未命中/低置信/人工纠正",
                usage_path,
                usage_path.is_file(),
                {"pm_usage_signals", "agent_usage", "usage"},
                "usage 事件反馈会提升下一轮刷新优先级",
                produced_count=usage_events,
            ),
            build(
                "ontology_change",
                "Ontology 类型/字段变化",
                None,
                False,
                {"ontology_change", "ontology"},
                "尚未接入 Ontology schema 变更源",
            ),
            build(
                "manual_seed",
                "本人手工提名新概念",
                manual_seed_path,
                manual_seed_path.is_file(),
                {"manual_seed", "manual-seed"},
                "审核页提名写入 Codex-owned manual-seeds",
            ),
        ]

    @staticmethod
    def _health_issue_reason(name: str, item: Mapping[str, Any]) -> str:
        if item.get("passed") is True:
            return "检查通过"
        data = item.get("data")
        if item.get("checker_error"):
            detail = data.get("detail") if isinstance(data, Mapping) else None
            return f"检查器异常：{detail or '请打开完整报告查看日志'}"
        if isinstance(data, Mapping):
            issues = data.get("issues")
            if isinstance(issues, list) and issues:
                return "；".join(str(value) for value in issues[:3])
            jobs = data.get("jobs")
            if isinstance(jobs, list):
                failures = []
                for row in jobs:
                    if not isinstance(row, Mapping):
                        continue
                    status = str(row.get("status") or "").lower()
                    if status in {"ok", "healthy", "success", "completed"}:
                        continue
                    label = row.get("label") or row.get("task") or name
                    detail = row.get("detail") or row.get("reason") or status or "异常"
                    failures.append(f"{label}：{detail}")
                if failures:
                    return "；".join(failures[:3])
            for key in ("detail", "error", "reason"):
                if data.get(key):
                    return str(data[key])
            if data.get("status"):
                return f"状态：{data['status']}"
        if isinstance(data, list):
            failures = []
            for row in data:
                if not isinstance(row, Mapping) or str(row.get("status") or "").lower() in {
                    "ok", "healthy", "success", "completed"
                }:
                    continue
                label = row.get("task") or row.get("source") or row.get("label") or name
                detail = [f"{label}：{row.get('status') or '异常'}"]
                if row.get("expected_since"):
                    detail.append(f"计划截止 {row['expected_since']}")
                if row.get("last_output_mtime"):
                    detail.append(f"最近产出 {row['last_output_mtime']}")
                failures.append("，".join(detail))
            if failures:
                return "；".join(failures[:3])
        return "检查未通过，请打开完整报告查看证据"

    @staticmethod
    def _checker_error_reason(health: Mapping[str, Any]) -> Optional[str]:
        """Return actionable checker-level diagnostics from latest.json.

        Older reports did not have the top-level ``checker_errors`` field, so
        derive the same information from per-check records when possible.
        """
        rows = health.get("checker_errors")
        if not isinstance(rows, list):
            rows = []
            checks = health.get("checks") if isinstance(health.get("checks"), Mapping) else {}
            for label, result in checks.items():
                if not isinstance(result, Mapping) or not result.get("checker_error"):
                    continue
                data = result.get("data") if isinstance(result.get("data"), Mapping) else {}
                detail = data.get("detail") or data.get("stderr") or data.get("error") or "未记录详情"
                rows.append({"label": label, "detail": detail})
        if not rows:
            return None
        details = []
        for row in rows[:3]:
            if isinstance(row, Mapping):
                label = row.get("label") or row.get("script") or "未知检查器"
                detail = row.get("detail") or row.get("stderr") or row.get("error") or "未记录详情"
                details.append(f"{label}：{detail}")
            else:
                details.append(str(row))
        return "检查器异常（巡检主流程已完成）：" + "；".join(details)

    @classmethod
    def _health_checks_projection(cls, health: Mapping[str, Any]) -> list[Dict[str, Any]]:
        checks = health.get("checks") if isinstance(health.get("checks"), Mapping) else {}
        rows: list[Dict[str, Any]] = []
        for name, item in checks.items():
            if not isinstance(item, Mapping):
                continue
            label = str(name)
            retired = any(token in label.casefold() for token in ("概念卡", "概念刷新", "概念盘点", "概念重检", "shengsuan-concepts"))
            rows.append(
                {
                    "name": label,
                    "passed": True if retired else item.get("passed") is True,
                    "checker_error": False if retired else bool(item.get("checker_error")),
                    "status": "disabled" if retired else ("healthy" if item.get("passed") is True else "failed"),
                    "disabled": retired,
                    "history_only": retired,
                    "reason": CONCEPT_WORKFLOW_REASON if retired else cls._health_issue_reason(label, item),
                }
            )
        return rows

    @staticmethod
    def _rrule_schedule(rrule: str) -> str:
        values = {}
        for token in str(rrule or "").split(";"):
            if "=" in token:
                key, value = token.split("=", 1)
                values[key.upper()] = value
        hour = int(values.get("BYHOUR", 0))
        minute = int(values.get("BYMINUTE", 0))
        time_label = f"{hour:02d}:{minute:02d}"
        days = {
            "MO": "周一", "TU": "周二", "WE": "周三", "TH": "周四",
            "FR": "周五", "SA": "周六", "SU": "周日",
        }
        if values.get("FREQ") == "WEEKLY":
            day_label = "、".join(days.get(value, value) for value in values.get("BYDAY", "").split(",") if value)
            return f"{day_label or '每周'} {time_label}"
        if values.get("FREQ") == "DAILY":
            return f"每天 {time_label}"
        return str(rrule or "未配置")

    def _automation_jobs_projection(self) -> list[Dict[str, Any]]:
        rows = []
        for path in sorted((self.codex_root / "automations").glob("*/automation.toml")):
            values: Dict[str, Any] = {}
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                match = re.match(r"^\s*(id|name|kind|status|rrule)\s*=\s*(.+?)\s*$", line)
                if not match:
                    continue
                raw = match.group(2)
                try:
                    values[match.group(1)] = json.loads(raw)
                except (TypeError, ValueError):
                    values[match.group(1)] = raw.strip("\"")
            status_value = str(values.get("status") or "UNKNOWN").upper()
            enabled = status_value == "ACTIVE"
            rrule = str(values.get("rrule") or "")
            rows.append({
                "id": str(values.get("id") or path.parent.name),
                "label": str(values.get("name") or values.get("id") or path.parent.name),
                "kind": "automation",
                "schedule": self._rrule_schedule(rrule),
                "loaded": enabled,
                "state": status_value.lower(),
                "runs": None,
                "last": None,
                "last_exit_code": None,
                "status": "scheduled" if enabled else "disabled",
                "reason": "Codex automation 已启用" if enabled else f"status={status_value}",
                "source": "Codex automation",
                "source_path": str(path),
                "rrule": rrule,
            })
        return rows

    def _schedule_jobs_projection(
        self,
        refresh: Dict[str, Any],
        health: Mapping[str, Any],
    ) -> list[Dict[str, Any]]:
        """Project schedules from the coordination Read Model.

        The V4 model is the runtime source of truth once the registry has been
        loaded.  The historical LaunchAgent/automation projection remains
        below as a compatibility path for isolated fixtures and pre-migration
        databases that do not have the schedule tables yet.
        """
        if self.v44_cockpit is not None:
            try:
                snapshot = self.v44_cockpit.snapshot(limit=100)
                schedules = snapshot.get("schedules") if isinstance(snapshot, Mapping) else None
                if isinstance(schedules, Mapping) and schedules.get("source_status") == "observed":
                    registry = schedules.get("registry") if isinstance(schedules.get("registry"), Mapping) else {}
                    task_states = schedules.get("task_states") if isinstance(schedules.get("task_states"), list) else []
                    projected: list[Dict[str, Any]] = []
                    for state in task_states:
                        if not isinstance(state, Mapping):
                            continue
                        key = str(state.get("schedule_key") or "")
                        if not key:
                            continue
                        occurrence_state = str(state.get("occurrence_state") or "never_run")
                        run_id = state.get("run_id")
                        status = str(state.get("status") or occurrence_state)
                        if not run_id and status == "never_run":
                            status = "scheduled"
                        if status in {"completed", "succeeded", "success"}:
                            reason = "最近一次 Run 已完成"
                        elif status in {"failed", "permanent_failed", "dead_letter", "expired", "interrupted"}:
                            reason = str(state.get("next_action") or state.get("failed_step") or "需要处理最近一次运行")
                        elif status in {"accepted", "queued", "running", "retry_wait", "deferred"}:
                            reason = f"当前 occurrence 状态：{status}"
                        else:
                            reason = "尚无运行记录，等待下一次计划"
                        launchd_state = str(state.get("launchd_state") or "unknown")
                        loaded = None if launchd_state == "unknown" else launchd_state not in {"not_loaded", "unavailable"}
                        projected.append(
                            {
                                "id": key,
                                "label": key,
                                "kind": "registry",
                                "schedule": state.get("calendar") or "按 registry",
                                "loaded": loaded,
                                "state": launchd_state,
                                "runs": 1 if run_id else 0,
                                "last": state.get("observed_at"),
                                "last_exit_code": state.get("actual_exit_code"),
                                "status": status,
                                "reason": reason,
                                "source": "pm-system.db",
                                "source_path": registry.get("source_path"),
                                "schedule_key": key,
                                "occurrence_id": state.get("occurrence_id"),
                                "job_id": state.get("job_id"),
                                "run_id": run_id,
                                "registry_hash": (schedules.get("registry") or {}).get("registry_hash"),
                                "artifact": state.get("artifact"),
                                "marker": state.get("marker"),
                                "evidence": state.get("evidence"),
                                "freshness": state.get("freshness"),
                            }
                        )
                    if projected:
                        return projected
            except (KeyError, OSError, sqlite3.Error, TypeError, ValueError):
                # Compatibility fallback below is intentionally bounded to
                # read/projection errors; it never mutates the coordination DB.
                pass
        weekly = refresh if isinstance(refresh, dict) else {}
        checks = health.get("checks") if isinstance(health.get("checks"), Mapping) else {}
        launchd_check = checks.get("launchd 作业状态") if isinstance(checks.get("launchd 作业状态"), Mapping) else {}
        launchd_data = launchd_check.get("data") if isinstance(launchd_check.get("data"), Mapping) else {}
        observed_jobs = launchd_data.get("jobs") if isinstance(launchd_data.get("jobs"), list) else []
        observed_by_label = {
            str(row.get("label")): row for row in observed_jobs if isinstance(row, Mapping) and row.get("label")
        }
        liveness_check = checks.get("定时任务执行留痕") if isinstance(checks.get("定时任务执行留痕"), Mapping) else {}
        liveness_rows = liveness_check.get("data") if isinstance(liveness_check.get("data"), list) else []
        task_labels = {
            "pm-timeline 周回顾": "com.zhujie14.pm-timeline-weekly",
            "pm-timeline 停滞跟进检查": "com.zhujie14.pm-timeline-daily",
            "shengsuan-concepts 周增量刷新": "com.zhujie14.weekly-sync-and-refresh",
            "产品情报双源周度监控": "com.zhujie14.product-intelligence-monitor",
        }
        liveness_by_label = {
            task_labels[str(row.get("task"))]: row
            for row in liveness_rows
            if isinstance(row, Mapping) and str(row.get("task")) in task_labels
        }
        timeline_review_output = self.pm_timeline_review_output()
        collected = collect_launchd_jobs(self.launch_agents_root)
        jobs = []
        job_concept_status: Optional[Dict[str, Any]] = None
        for raw in collected.get("jobs", []):
            if not isinstance(raw, Mapping):
                continue
            job_concept_status = None
            label = str(raw.get("label") or "未命名 LaunchAgent")
            if label in SCHEDULE_DISPLAY_EXCLUDED_LABELS:
                continue
            observed = observed_by_label.get(label, {})
            probe = raw.get("launchctl") if isinstance(raw.get("launchctl"), Mapping) else {}
            state = str(probe.get("state") or "")
            if not state or state == "probe_inconclusive":
                state = str(observed.get("state") or state or "unknown")
            loaded = state not in {"not_loaded", "probe_inconclusive", "unavailable", "missing", "unknown"}
            runs_raw = probe.get("runs") if probe.get("runs") is not None else observed.get("runs")
            try:
                runs = int(runs_raw) if runs_raw is not None else None
            except (TypeError, ValueError):
                runs = None
            last_exit = probe.get("last_exit_code") if probe.get("last_exit_code") is not None else observed.get("last_exit_code")
            logs = raw.get("logs") if isinstance(raw.get("logs"), list) else []
            modified = sorted(
                (str(item.get("modified_at")) for item in logs if isinstance(item, Mapping) and item.get("modified_at")),
                reverse=True,
            )
            schedule = raw.get("schedule")
            if not schedule and raw.get("interval_seconds"):
                schedule = f"每 {int(raw['interval_seconds'])} 秒"
            if not schedule and raw.get("keep_alive"):
                schedule = "常驻"
            if not schedule and raw.get("run_at_load"):
                schedule = "登录 / 加载时"
            schedule = schedule or "手动 / 一次性"
            if not loaded:
                status = "idle" if label.endswith("-once") else "missing"
                reason = "一次性作业当前未加载" if status == "idle" else "LaunchAgent 未加载"
            elif state == "running":
                status, reason = "running", "当前运行中"
            elif str(observed.get("status") or "ok") != "ok":
                status, reason = "failed", str(observed.get("detail") or "launchd 健康检查未通过")
            elif str(last_exit) not in {"", "None", "0", "(never exited)"}:
                status, reason = "failed", f"上次退出码 {last_exit}"
            elif runs == 0 and raw.get("schedule"):
                if modified:
                    status, reason = "completed", "最近有运行日志，等待下次计划"
                else:
                    status, reason = "scheduled", "已加载，等待首次计划运行"
            else:
                status, reason = "completed", "最近一次退出正常" if runs else "已加载"

            liveness = liveness_by_label.get(label)
            last = modified[0] if modified else None
            if isinstance(liveness, Mapping):
                last = liveness.get("last_output_mtime") or last
                liveness_status = str(liveness.get("status") or "unknown")
                if liveness_status == "disabled":
                    # The concept sub-step is intentionally retired.  Do not
                    # turn that expected state into a failed/not_run weekly
                    # job; the aggregate is evaluated below from Step 1/2.
                    pass
                elif liveness_status == "ok" and status not in {"running", "failed", "missing"}:
                    status, reason = "completed", "本周期已有有效产出"
                elif liveness_status != "ok":
                    status = "not_run" if liveness_status in {"not_run", "missing_output"} else "failed"
                    reason_parts = [f"{liveness.get('task') or label}：{liveness_status}"]
                    if liveness.get("expected_since"):
                        reason_parts.append(f"计划截止 {liveness['expected_since']}")
                    if liveness.get("last_output_mtime"):
                        reason_parts.append(f"最近产出 {liveness['last_output_mtime']}")
                    reason = "；".join(reason_parts)

            if label == str(weekly.get("job") or "com.zhujie14.weekly-sync-and-refresh"):
                marker = weekly.get("last_run") if isinstance(weekly.get("last_run"), Mapping) else {}
                # Step 3 is retired and may be recorded as the string
                # ``disabled``.  Only the remaining source-sync steps can
                # make the weekly aggregate fail; never coerce the retired
                # marker to ``int``.
                failed_steps = []
                for key in ("step1_sync", "step2_public_docs"):
                    raw_step = marker.get(key)
                    if raw_step is None or raw_step is False:
                        continue
                    if isinstance(raw_step, (int, float)):
                        failed = raw_step != 0
                    else:
                        value = str(raw_step).strip().casefold()
                        if value in {"", "0", "ok", "success", "completed", "complete", "disabled", "skipped"}:
                            failed = False
                        else:
                            try:
                                failed = int(value) != 0
                            except (TypeError, ValueError):
                                failed = True
                    if failed:
                        failed_steps.append(key)
                marker_status = str(marker.get("status") or "").lower()
                if (
                    not failed_steps
                    and str(marker.get("step3_refresh") or "").strip().casefold() == "disabled"
                    and marker_status in {"partial", "partial_failure"}
                ):
                    # Legacy markers were marked partial solely because Step 3
                    # ran.  Once Step 3 is retired, that historical value must
                    # not surface as a current weekly failure.
                    marker_status = "completed"
                last = marker.get("finished_at") or last
                if weekly.get("running"):
                    status, reason = "running", "每周流水线正在运行"
                elif marker and (failed_steps or marker_status in {"partial", "partial_failure", "aborted", "cancelled", "canceled", "error"}):
                    status = "partial"
                    if marker_status in {"aborted", "cancelled", "canceled"}:
                        reason = f"本周流水线已中止：{marker.get('reason') or marker.get('signal') or '未记录原因'}"
                    else:
                        reason = "部分步骤失败" + ("：" + "、".join(failed_steps) if failed_steps else "")
                elif marker and marker_status in {"failed", "error"}:
                    status, reason = "failed", f"本周运行状态：{marker_status}"
                elif marker:
                    status, reason = "completed", "本周流水线全部步骤退出码为 0"
                job_concept_status = {
                    "status": "disabled",
                    "disabled": True,
                    "history_only": True,
                    "actionable": False,
                    "reason": CONCEPT_WORKFLOW_REASON,
                    "last_run": marker.get("step3_refresh"),
                }

            if label == "com.zhujie14.product-intelligence-monitor":
                product_check = checks.get("产品情报周度比较门禁") if isinstance(checks.get("产品情报周度比较门禁"), Mapping) else {}
                if product_check and product_check.get("passed") is not True:
                    product_data = product_check.get("data") if isinstance(product_check.get("data"), Mapping) else {}
                    status = "partial" if product_data.get("usable_for_assessment") else "failed"
                    reason = self._health_issue_reason("产品情报周度比较门禁", product_check)

            if label == "com.zhujie14.system-health-check" and state != "running":
                checker_reason = self._checker_error_reason(health)
                if checker_reason:
                    status, reason = "failed", checker_reason
                else:
                    # Exit code 1 is the documented "inspection completed,
                    # business issues found" result.  Prefer the latest
                    # structured report over a historical launchd exit code,
                    # which may still be 2 from an earlier checker failure.
                    issue_count = sum(
                        1 for item in self._health_checks_projection(health)
                        if item.get("status") == "failed"
                    )
                    if issue_count:
                        status, reason = "partial", f"巡检已完成，发现 {issue_count} 项业务问题；请打开最新巡检报告"
                    elif checks:
                        status, reason = "completed", "巡检已完成，未发现业务问题"

            job = {
                "id": label.removeprefix("com.zhujie14."),
                "label": label,
                "kind": "launchd",
                "schedule": schedule,
                "loaded": loaded,
                "state": state,
                "runs": runs,
                "last": last,
                "last_exit_code": last_exit,
                "status": status,
                "reason": reason,
                "source": "LaunchAgent",
                "source_path": raw.get("plist"),
            }
            if label == str(weekly.get("job") or "com.zhujie14.weekly-sync-and-refresh"):
                job["concept_refresh"] = job_concept_status or {
                    "status": "disabled",
                    "disabled": True,
                    "history_only": True,
                    "actionable": False,
                    "reason": CONCEPT_WORKFLOW_REASON,
                }
            if label == "com.zhujie14.pm-timeline-weekly":
                job.update(
                    {
                        "output_url": timeline_review_output["url"],
                        "output_path": timeline_review_output["path"],
                        "output_updated_at": timeline_review_output["updated_at"],
                        "latest_output": timeline_review_output,
                    }
                )
            jobs.append(job)
        jobs.extend(self._automation_jobs_projection())
        return jobs

    def control_plane_snapshot(self, *, force: bool = False) -> Dict[str, Any]:
        """Return a V3 read projection with source-aware burst coalescing.

        ``force=True`` is used by an explicit browser refresh (and the
        ``?fresh=1`` HTTP contract).  Normal polling may reuse a projection
        only when all observed source signatures are unchanged.  This keeps
        concurrent requests cheap without presenting a stale state after a
        runner atomically replaces a source file.
        """
        now = time.monotonic()
        current_version: Optional[str] = None
        try:
            current_version = self._source_version(self._control_plane_source_signatures())
        except Exception:
            # A transient permissions/filesystem error should not make the
            # read endpoint fail; the uncached projection will expose the
            # resulting missing/read_error state instead.
            current_version = None
        with self._snapshot_cache_lock:
            cached = self._snapshot_cache
            if not force and cached is not None:
                cached_at, cached_version, cached_value = cached
                if (
                    now - cached_at < SNAPSHOT_CACHE_TTL_SECONDS
                    # A cache is safe only when the complete source signature
                    # set was read successfully and still matches.  If the
                    # signature probe is inconclusive, rebuild the projection
                    # so a transient filesystem/permission error can never
                    # make the UI present an older snapshot as current.
                    and current_version is not None
                    and cached_version is not None
                    and cached_version == current_version
                ):
                    return copy.deepcopy(cached_value)
            value = self._control_plane_snapshot_uncached()
            value_version = str(value.get("source_version") or current_version or "") or None
            # If a source changed while the projection was being built, do not
            # retain the inconsistent read model.  The next poll will retry.
            if value.get("read_consistency", "consistent") == "consistent":
                self._snapshot_cache = (time.monotonic(), value_version, value)
            else:
                self._snapshot_cache = None
            return copy.deepcopy(value)

    def _control_plane_snapshot_uncached(self) -> Dict[str, Any]:
        """Read the latest local facts for the display-only Control Plane.

        This projection intentionally has no mutation side effects. It makes
        freshness explicit so a UI refresh never implies that Codex has run.
        """
        checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        source_signatures_before = self._control_plane_source_signatures()
        source_version_before = self._source_version(source_signatures_before)
        # Build one read model for the whole snapshot.  The previous path
        # reparsed the Candidate directory in concepts(), refresh_status() and
        # candidate_projections(), which multiplied latency under UI polling.
        raw_candidates = self._read_candidates_cached()
        usage = self.learning.usage_summary()
        discovery_rows = self.learning.discovery_runs()
        concept_rows = self.concepts(
            candidates=raw_candidates,
            usage=usage,
            include_candidate_details=False,
        )
        # The snapshot is an overview read model.  Keep only the first page
        # of actionable Candidate summaries here; the dedicated
        # `/api/candidates` endpoint owns pagination and detail loading.
        reviewable_rows = [
            row for row in raw_candidates
            if str(row.get("status") or "") not in CANDIDATE_REVIEW_TERMINAL
        ]
        candidate_page_rows = reviewable_rows[:CANDIDATE_PAGE_SIZE_DEFAULT]
        candidates = self.candidate_projections(
            candidates=candidate_page_rows,
            include_details=False,
        )
        recheck = self.concept_recheck_status()
        refresh = self.refresh_status(candidates=raw_candidates, usage=usage, discovery_runs=discovery_rows)
        inventory = self._inventory_projection(refresh.get("deep_inventory") if isinstance(refresh, dict) else None)
        discovery_sources = self._discovery_source_projection(discovery_rows, usage)
        states = self.store.list_states_read_only()
        health_path = self.codex_root / "skills" / "system-health-check" / "state" / "latest.json"
        health: Dict[str, Any] = {}
        health_read_error: Optional[str] = None
        try:
            value = json.loads(health_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                health = value
        except (OSError, json.JSONDecodeError):
            health_read_error = "health_latest_unreadable"
            health = {}
        checks = health.get("checks") if isinstance(health.get("checks"), dict) else {}
        health_checks_projection = self._health_checks_projection(health)
        health_issues = sum(1 for item in health_checks_projection if item.get("status") == "failed")
        health_report_path = self.health_report_path()
        health_report_updated_at = None
        if health_report_path:
            try:
                health_report_updated_at = datetime.fromtimestamp(
                    health_report_path.stat().st_mtime, timezone.utc
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except OSError:
                health_report_updated_at = None
        name_fingerprint = self._name_fingerprint_status()
        requirement_path = self.codex_root / "skills" / "requirement-fit-assessment" / "state" / "index.jsonl"
        timeline_root = self.codex_root / "skills" / "pm-timeline" / "state" / "timeline"
        team_path = self._latest_file(timeline_root, timeline_root / "*.jsonl")
        requirement_projection = self._jsonl_projection(requirement_path, 200)
        team_projection = self._jsonl_projection(team_path, 8) if team_path else {"rows": [], "total": 0, "counts": {}}
        team_updated_at = None
        if team_path:
            try:
                team_updated_at = datetime.fromtimestamp(
                    team_path.stat().st_mtime, timezone.utc
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except OSError:
                team_updated_at = None
        product_gap_path = self.domain_report_source_path("gaps")
        material_path = self.domain_report_source_path("materials")
        product_gap_html_path = self.domain_report_path("gaps")
        material_html_path = self.domain_report_path("materials")
        domain_updated_at: Dict[str, Optional[str]] = {"gaps": None, "materials": None}
        for domain, source_path in (("gaps", product_gap_path), ("materials", material_path)):
            if not source_path:
                continue
            try:
                domain_updated_at[domain] = datetime.fromtimestamp(
                    source_path.stat().st_mtime, timezone.utc
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except OSError:
                pass
        requirement_freshness = self._freshness_item(
            "requirements",
            "需求评估索引（历史只读）",
            "requirement-fit-assessment/state/index.jsonl",
            requirement_path,
            30 * 24,
            "需求评估历史保留；不再回写概念 Candidate 或触发刷新",
            observed_at=checked_at,
        )
        requirement_freshness.update(
            {
                "status": "disabled",
                "display_status": "历史只读",
                "disabled": True,
                "history_only": True,
                "actionable": False,
                "request": None,
                "reason": "需求评估历史保留；不再回写概念 Candidate 或触发刷新",
            }
        )
        gap_source_label, gap_source_glob = self._domain_report_freshness_spec("gaps", product_gap_path)
        material_source_label, material_source_glob = self._domain_report_freshness_spec("materials", material_path)
        freshness = [
            (refresh.get("concept_refresh", {}).get("freshness") if isinstance(refresh.get("concept_refresh"), dict) else None)
            or self._freshness_item("concepts", "概念卡（历史只读）", "shengsuan-concepts/state/concepts-ledger.json", self.concepts_ledger_path, 14 * 24, "概念刷新已停用；仅查看 Active 与历史 Candidate", observed_at=checked_at),
            self._freshness_item("health", "系统健康检查", "system-health-check/state/latest.json", health_path, 7 * 24, "请 Codex 执行 system-health-check，并回写 latest.json", observed_at=checked_at),
            self._freshness_item("schedule", "每周同步与刷新", "scripts/state/weekly-sync-and-refresh.done", self.codex_root / "scripts" / "state" / "weekly-sync-and-refresh.done", 10 * 24, "请 Codex 执行 weekly-sync-and-refresh，检查同步失败项", observed_at=checked_at),
            requirement_freshness,
            self._freshness_item("team", "团队状态时间轴", "pm-timeline/state/timeline/*.jsonl", team_path, 7 * 24, "请 Codex 执行 pm-timeline 周回顾并刷新团队状态", directory_glob="*.jsonl", observed_at=checked_at),
            self._freshness_item("gaps", "产品缺口评估", gap_source_label, product_gap_path, 14 * 24, "请 Codex 执行 capability-gap-aggregator，生成最新缺口评估", directory_glob=gap_source_glob, observed_at=checked_at),
            self._freshness_item("materials", "资料评估", material_source_label, material_path, 14 * 24, "请 Codex 执行资料缺失评估，检查新增与失效来源", directory_glob=material_source_glob, observed_at=checked_at),
        ]
        latest_runs = [item for item in states if item.get("loop_id") not in CONCEPT_LOOP_IDS][:40]
        source_errors = ["health_latest_unreadable"] if health_read_error else []
        source_signatures_after = self._control_plane_source_signatures()
        source_version_after = self._source_version(source_signatures_after)
        read_consistency = "consistent" if source_version_before == source_version_after else "changed_during_read"
        queue = self.queue_status()
        interrupted = sum(1 for row in latest_runs if str(row.get("status") or "") == "interrupted")
        if inventory.get("recoverable"):
            interrupted = max(interrupted, 1)
        snapshot_id = "cp-" + source_version_after.removeprefix("sha256:")[:16]
        candidate_counts = self._candidate_counts(raw_candidates)
        return {
            "schema_version": CONTROL_PLANE_SNAPSHOT_SCHEMA,
            "legacy_schema_version": "pm-loop.control-plane-snapshot.v1",
            "read_only": True,
            "concept_workflow": self.concept_workflow_status(),
            "disabled": False,
            "history_only": False,
            "actionable": False,
            "pending": 0,
            "snapshot_id": snapshot_id,
            "checked_at": checked_at,
            "read_at": checked_at,
            "version": source_version_after,
            "source_version": source_version_after,
            "read_consistency": read_consistency,
            "source_errors": source_errors,
            "source_signatures": source_signatures_after,
            "freshness": freshness,
            "summary": {
                "fresh": sum(1 for item in freshness if item["status"] == "fresh"),
                "stale": sum(1 for item in freshness if item["status"] == "stale"),
                "missing": sum(1 for item in freshness if item["status"] == "missing"),
                "interrupted": interrupted,
                "health_issues": health_issues,
                "concepts": len([item for item in concept_rows if item.get("status") in {None, "active"}]),
                "candidate_ready": 0,
                "candidate_total": candidate_counts.get("total", 0),
                "candidate_history_counts": candidate_counts,
                "candidate_actionable": 0,
                "candidate_published": candidate_counts.get("published", 0),
                "candidate_changes_requested": candidate_counts.get("changes_requested", 0),
                "candidate_superseded": candidate_counts.get("superseded", 0),
                "usage_events": usage.get("events", 0) if isinstance(usage, dict) else 0,
                "queue": {key: queue.get(key, 0) for key in ("queued", "running", "completed", "failed", "cancelled")},
            },
            "concepts": {
                "rows": concept_rows,
                "candidates": candidates,
                "active": len([item for item in concept_rows if item.get("status") in {None, "active"}]),
                "candidate_total": candidate_counts.get("total", 0),
                "candidate_actionable": 0,
                "history_only": True,
                "published": candidate_counts.get("published", 0),
                "usage": usage,
                "recheck": recheck,
                "refresh": refresh,
                "name_fingerprint": name_fingerprint,
                "inventory": inventory,
                "discovery_sources": discovery_sources,
                "candidate_pagination": {
                    "page": 1,
                    "page_size": CANDIDATE_PAGE_SIZE_DEFAULT,
                    "total": len(reviewable_rows),
                    "pages": math.ceil(len(reviewable_rows) / CANDIDATE_PAGE_SIZE_DEFAULT) if reviewable_rows else 0,
                    "has_next": len(reviewable_rows) > CANDIDATE_PAGE_SIZE_DEFAULT,
                    "status": "reviewable",
                },
            },
            "health": {
                "latest": health,
                "checks": health_checks_projection,
                "issues": health_issues,
                "refresh": refresh,
                "name_fingerprint": name_fingerprint,
                "report": {
                    "available": health_report_path is not None,
                    "url": "/health-report" if health_report_path else None,
                    "path": str(health_report_path) if health_report_path else None,
                    "updated_at": health_report_updated_at,
                },
            },
            "schedules": {
                "weekly": refresh,
                "runs": latest_runs,
                "jobs": self._schedule_jobs_projection(refresh, health),
                "visibility": {
                    "hidden_infrastructure_labels": sorted(SCHEDULE_DISPLAY_EXCLUDED_LABELS),
                },
            },
            "execution": {
                "queue": queue,
                "worker": {
                    "resident": False,
                    "status": "disabled",
                    "source": CONCEPT_WORKFLOW_REASON,
                },
            },
            "domains": {
                # Requirements can contain hundreds of large evidence rows.
                # The overview snapshot carries only a bounded recent window;
                # the source path/freshness item tells the page when it needs
                # a dedicated deeper read.
                "requirements": {
                    **requirement_projection,
                    "source": str(requirement_path),
                    "bounded": True,
                    "limit": 200,
                },
                "team": {
                    "source": str(team_path) if team_path else None,
                    "stale_followups": str(self.codex_root / "skills" / "pm-timeline" / "state" / "stale-followups.md"),
                    "timeline_events": team_projection["total"],
                    "updated_at": team_updated_at,
                },
                "gaps": {
                    "source": str(product_gap_path) if product_gap_path else None,
                    "available": bool(product_gap_html_path and product_gap_html_path.is_file()),
                    "url": "/reports/gaps/latest" if product_gap_html_path else None,
                    "html_path": str(product_gap_html_path) if product_gap_html_path else None,
                    "updated_at": domain_updated_at["gaps"],
                },
                "materials": {
                    "source": str(material_path) if material_path else None,
                    "available": bool(material_html_path and material_html_path.is_file()),
                    "url": "/reports/materials/latest" if material_html_path else None,
                    "html_path": str(material_html_path) if material_html_path else None,
                    "updated_at": domain_updated_at["materials"],
                },
            },
        }

    @staticmethod
    def _jsonl_projection(path: Path, limit: int = 200) -> Dict[str, Any]:
        if not path.is_file():
            return {"rows": [], "total": 0, "counts": {}}
        rows: list[Dict[str, Any]] = []
        total = 0
        counts: Dict[str, int] = {}
        try:
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    rows.append(value)
                    total += 1
                    verdict = str(value.get("verdict") or "unknown")
                    counts[verdict] = counts.get(verdict, 0) + 1
        except OSError:
            return {"rows": [], "total": 0, "counts": {}}
        return {"rows": rows[-limit:], "total": total, "counts": counts}

    def _wait_agent_refresh(self, run_id: str, name: str, process: subprocess.Popen[str], log_stream: Any) -> None:
        if CONCEPT_WORKFLOW_DISABLED:
            # A process created before the retirement flag was deployed may
            # still notify this callback.  Drop it without reading its output,
            # writing Candidate/RunStore events, or attempting recovery.
            try:
                log_stream.close()
            finally:
                with self.lock:
                    self.processes.pop(run_id, None)
            return
        try:
            code = process.wait()
            if code == 0:
                candidate = self.learning.candidate_for_concept(name)
                if candidate:
                    self.store.append(run_id, "candidate/created", {"candidate_id": candidate.get("candidate_id"), "concept": name, "confidence": candidate.get("confidence")}, actor="control-plane")
                self.store.append(run_id, "agent/completed", {"concept": name}, actor="control-plane")
                self.store.append(run_id, "verification/completed", {"checks": ["candidate_written", "active_unchanged"], "ok": bool(candidate)}, actor="control-plane")
                self.store.append(run_id, "run/completed", {"concept": name}, actor="control-plane")
            else:
                self.store.append(run_id, "agent/failed", {"concept": name, "returncode": code}, actor="control-plane")
                self.store.append(run_id, "run/failed", {"error": f"concept refresh exited with code {code}"}, actor="control-plane")
        finally:
            log_stream.close()
            with self.lock:
                self.processes.pop(run_id, None)

    def request_full_recheck(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._concept_write_rejected()
        with self.recheck_lock:
            return self._request_full_recheck_locked(payload)

    def _request_full_recheck_locked(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if CONCEPT_WORKFLOW_DISABLED:
            self._concept_write_rejected()
        payload = payload or {}
        mode = str(payload.get("mode") or "recheck_existing")
        if mode not in {"recheck_existing", "full_inventory"}:
            raise ValueError("mode must be recheck_existing or full_inventory")
        scope = payload.get("concepts") if isinstance(payload.get("concepts"), list) else []
        status = self.concept_recheck_status()
        active = status.get("active")
        if isinstance(active, dict):
            value = dict(active)
            value["deduplicated"] = True
            value["message"] = f"已有全量重检正在运行：{active.get('run_id')}"
            return value
        before_active = self._active_fingerprint()
        request = self.store.create(
            {
                "loop_id": "concept-recheck",
                "trigger": {"kind": "manual_full_inventory" if mode == "full_inventory" else "manual_full_recheck", "actor": "zhujie14"},
                "scope": {"concepts": scope if mode == "recheck_existing" else [], "mode": mode, "roots": INVENTORY_ROOTS if mode == "full_inventory" else []},
                "permission_mode": "approved_action",
                "record": True,
            }
        )
        run_id = request["run_id"]
        result_path = self.store.paths(run_id).root / "recheck" / "result.json"
        self.store.append(run_id, "run/started", {"runtime": "codex", "scope": request["scope"]}, actor="control-plane")
        self.store.append(run_id, "agent/requested", {"mode": mode, "concepts": scope or "all", "roots": INVENTORY_ROOTS if mode == "full_inventory" else []}, actor="reviewer")
        log_path = self.store.paths(run_id).runner_log
        log_path.parent.mkdir(parents=True, exist_ok=True)
        runner_script = "concept_inventory.py" if mode == "full_inventory" else "concept_recheck.py"
        command = [sys.executable, str(self.project_root / "scripts" / runner_script), "--codex-root", str(self.codex_root), "--state-dir", str(self.store.state_dir), "--result-path", str(result_path)]
        if mode == "full_inventory":
            command.append("--resume-latest")
        if scope and mode == "recheck_existing":
            command.extend(["--concepts", *[str(item) for item in scope]])
        log_stream = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(command, cwd=str(self.project_root), stdout=log_stream, stderr=subprocess.STDOUT, text=True)
        with self.lock:
            self.processes[run_id] = process
        threading.Thread(target=self._wait_full_recheck, args=(run_id, process, log_stream, before_active, result_path, mode), daemon=True).start()
        return self.store.state(run_id)

    def concept_recheck_status(self) -> Dict[str, Any]:
        active_statuses = {"queued", "running", "collecting", "analyzing", "verifying", "awaiting_human"}
        rows = [item for item in self.store.list_states_read_only() if item.get("loop_id") == "concept-recheck"]
        historical_active_raw = next((item for item in rows if item.get("status") in active_statuses), None)
        latest_raw = rows[0] if rows else None

        def historical_row(item: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if not isinstance(item, dict):
                return None
            value = dict(item)
            value["raw_status"] = item.get("status")
            value["status"] = "history_only"
            value["display_status"] = "历史只读"
            value["disabled"] = True
            value["read_only"] = True
            value["history_only"] = True
            value["actionable"] = False
            return value

        historical_active = historical_row(historical_active_raw)
        latest = historical_row(latest_raw)
        history = [historical_row(item) for item in rows[:20]]
        history = [item for item in history if item is not None]
        raw_scope = (historical_active_raw or latest_raw or {}).get("scope")
        raw_scope = raw_scope if isinstance(raw_scope, dict) else {}
        configured = configured_concepts(self.concepts_root)
        return {
            **self.concept_workflow_status(),
            "running": False,
            "can_start": False,
            "active": None,
            "historical_active": historical_active,
            "latest": latest,
            "history": history,
            "pending": 0,
            "actionable": False,
            "historical_count": len(rows),
            "scope": {
                "concept_count": len(configured),
                "mode": "disabled",
                "historical_mode": raw_scope.get("mode") or "recheck_existing",
                "document_mode": "history_only",
                "historical_document_mode": "sync_ledgers_for_evidence_and_discovery",
                "inventory_roots": INVENTORY_ROOTS,
            },
        }

    def _active_fingerprint(self) -> str:
        try:
            value = json.loads(self.concepts_ledger_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            value = {}
        active = {str(name): record for name, record in value.items() if isinstance(record, dict) and str(record.get("status") or "active") == "active"} if isinstance(value, dict) else {}
        return content_hash(json.dumps(active, ensure_ascii=False, sort_keys=True))

    def _wait_full_recheck(self, run_id: str, process: subprocess.Popen[str], log_stream: Any, before_active: str, result_path: Path, mode: str = "recheck_existing") -> None:
        if CONCEPT_WORKFLOW_DISABLED:
            try:
                log_stream.close()
            finally:
                with self.lock:
                    self.processes.pop(run_id, None)
            return
        try:
            code = process.wait()
            if code == 0:
                try:
                    value = json.loads(result_path.read_text(encoding="utf-8"))
                    result = value if isinstance(value, dict) else {}
                except (OSError, json.JSONDecodeError):
                    result = {}
                after_active = self._active_fingerprint()
                discovery_ids = [str(item) for item in result.get("discovery_run_ids") or [] if str(item)]
                if not discovery_ids and result.get("discovery_run_id"):
                    discovery_ids = [str(result.get("discovery_run_id"))]
                discovery_rows = {str(item.get("run_id") or ""): item for item in self.learning.discovery_runs() if isinstance(item, dict)}
                discovery_ok = bool(discovery_ids) and all(item_id in discovery_rows for item_id in discovery_ids)
                triage_rows = [discovery_rows[item_id] for item_id in discovery_ids if item_id in discovery_rows]
                triage_ok = all(
                    str(item.get("triage_status") or "") == "complete"
                    or str(item.get("status") or "") in {"triaged", "triage_no_candidate"}
                    for item in triage_rows
                ) if triage_rows else False
                if mode == "full_inventory":
                    snapshot = result.get("snapshot") if isinstance(result.get("snapshot"), dict) else {}
                    triage = result.get("triage") if isinstance(result.get("triage"), dict) else {}
                    checks = {
                        "result_artifact": result.get("schema_version") == "concept-learning.inventory.v1",
                        "resource_snapshot": snapshot.get("status") == "ok" and int(snapshot.get("file_count") or 0) > 0,
                        "discovery_recorded": discovery_ok,
                        "inventory_triage_complete": triage.get("status") in {"complete", "completed", "no_candidate"} or triage_ok,
                        "active_unchanged": before_active == after_active,
                    }
                else:
                    checks = {
                        "result_artifact": bool(result),
                        "all_concepts_proposed": result.get("refresh", {}).get("status") == "ok",
                        "discovery_recorded": discovery_ok,
                        "discovery_triage_complete": triage_ok,
                        "active_unchanged": before_active == after_active,
                    }
                ok = all(checks.values())
                self.store.append(run_id, "agent/completed", {"mode": mode, "result": result}, actor="control-plane")
                self.store.append(run_id, "verification/completed", {"checks": checks, "ok": ok}, actor="control-plane")
                if ok:
                    self.store.append(run_id, "run/completed", {"mode": mode, "result": result}, actor="control-plane")
                else:
                    self.store.append(run_id, "run/failed", {"error": "full recheck verification failed", "checks": checks, "result": result}, actor="control-plane")
            else:
                self.store.append(run_id, "agent/failed", {"mode": mode, "returncode": code}, actor="control-plane")
                self.store.append(run_id, "run/failed", {"error": f"{mode} exited with code {code}"}, actor="control-plane")
        finally:
            log_stream.close()
            with self.lock:
                self.processes.pop(run_id, None)

    def record_usage(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Usage telemetry is intentionally retained after refresh retirement.
        # It is append-only feedback and never starts a proposer, writes a
        # Candidate/Active page, or enters the approved queue.
        event = dict(payload)
        if not event.get("concept"):
            event["concept"] = event.get("term") or "__not_found__"
        return self.learning.append_usage(event)

    def record_manual_seed(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._concept_write_rejected()
        term = str(payload.get("term") or "").strip()
        if not term:
            raise ValueError("term is required")
        value = {"term": term[:200], "context": str(payload.get("context") or "")[:4000], "source_refs": [str(item) for item in payload.get("source_refs") or [] if str(item)], "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"), "actor": "zhujie14"}
        path = self.store.state_dir / "concept-review" / "manual-seeds.jsonl"
        lock_path = path.with_suffix(".lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_stream:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(value, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)
        return value

    @staticmethod
    def _normalize_loop_scope(loop: Dict[str, Any], raw_scope: Any) -> Dict[str, Any]:
        if not isinstance(raw_scope, dict):
            raise ValueError("scope must be an object")
        schema = loop.get("input_schema") or []
        allowed = {str(field.get("id")): field for field in schema if isinstance(field, dict) and field.get("id")}
        legacy_aliases = {"customer", "project", "note", "time_range", "focus"}
        unknown = sorted(set(raw_scope) - set(allowed) - legacy_aliases)
        if unknown:
            raise ValueError("unknown scope fields: " + ", ".join(unknown))
        normalized: Dict[str, Any] = {}
        for field_id in sorted(set(raw_scope) & legacy_aliases):
            value = raw_scope.get(field_id)
            if value is not None and str(value).strip():
                normalized[field_id] = str(value).strip()[:8000]
        for field_id, field in allowed.items():
            value = raw_scope.get(field_id, field.get("default"))
            if field.get("required") and (value is None or (isinstance(value, str) and not value.strip())):
                raise ValueError(f"scope.{field_id} is required")
            if value is None or value == "":
                continue
            field_type = str(field.get("type") or "text")
            if field_type == "number":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    raise ValueError(f"scope.{field_id} must be an integer")
                if field.get("min") is not None and value < int(field["min"]):
                    raise ValueError(f"scope.{field_id} must be >= {field['min']}")
                if field.get("max") is not None and value > int(field["max"]):
                    raise ValueError(f"scope.{field_id} must be <= {field['max']}")
            elif field_type == "select":
                options = [str(item) for item in field.get("options") or []]
                if options and str(value) not in options:
                    raise ValueError(f"scope.{field_id} must be one of: {', '.join(options)}")
                value = str(value)
            else:
                value = str(value).strip()[:8000]
            normalized[field_id] = value
        return normalized

    def create_coordination_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Accept a V4.4 Run without starting a child process in the API."""
        if self.coordination_store is None:
            raise RuntimeError("coordination store is unavailable")
        admission = str(os.environ.get("PM_V44_ADMISSION", "freeze") or "freeze").strip().lower()
        frozen = str(os.environ.get("PM_V44_AUTOMATION_FREEZE", "on") or "on").strip().lower()
        if admission not in ADMISSION_ENABLED or frozen in {"1", "true", "on", "enabled", "yes"}:
            raise PermissionError("V4.4 admission is frozen")
        loop_id = str(payload.get("loop_id") or "daily-radar")
        loop = next((item for item in LOOPS if item["id"] == loop_id), None)
        if loop is None:
            raise ValueError(f"unknown loop_id: {loop_id}")
        if loop_id in CONCEPT_LOOP_IDS or loop.get("executor") in {"concept-review-control-plane", "concept-status-observer", "concept-recheck-runner"}:
            self._concept_write_rejected()
        permission_mode = str(payload.get("permission_mode") or loop["permission_mode"])
        if permission_mode != loop["permission_mode"]:
            raise ValueError(f"permission_mode is fixed by Loop Registry: {loop['permission_mode']}")
        scope = self._normalize_loop_scope(loop, payload.get("scope") or {})
        loop_contract = {
            key: loop.get(key)
            for key in ("id", "title", "executor", "input_schema", "snapshot_sources", "analysis_instruction", "write_allowlist")
        }
        trigger = payload.get("trigger") if isinstance(payload.get("trigger"), dict) else {"kind": "manual", "actor": "local-web"}
        input_hash = content_hash(json.dumps({"loop_id": loop_id, "scope": scope, "loop_contract": loop_contract}, ensure_ascii=False, sort_keys=True))
        trigger_suffix = str(trigger.get("rerun_of") or "")
        idempotency_key = str(payload.get("idempotency_key") or f"{loop_id}:{input_hash}:{trigger_suffix}")
        budget = payload.get("budget") if isinstance(payload.get("budget"), dict) else {}
        execution_payload = {
            "loop_id": loop_id,
            "scope": scope,
            "permission_mode": permission_mode,
            "loop_contract": loop_contract,
            "trigger": trigger,
            "record": bool(payload.get("record", False)),
            "runtime": {"kind": "codex", "executor": loop.get("executor"), "input_hash": input_hash},
            "analysis_mode": "snapshot-only" if self.snapshot_path else "codex",
            "snapshot_path": str(self.snapshot_path) if self.snapshot_path else None,
            "project_root": str(self.project_root),
            "codex_root": str(self.codex_root),
            "adapter_script": str(self.adapter_script),
            "budget": budget,
            "provider": str(payload.get("provider") or "oneapi"),
        }
        max_seconds = int(budget.get("max_seconds") or 900)
        deadline = datetime.now(timezone.utc).timestamp() + max(1, max_seconds)
        accepted = self.coordination_store.accept(
            {
                "job_type": "pm-loop",
                "loop_id": loop_id,
                "idempotency_key": idempotency_key,
                "profile": str(payload.get("profile") or ("report" if permission_mode == "report" else "interactive")),
                "priority": int(payload.get("priority", 50)),
                "deadline_at": datetime.fromtimestamp(deadline, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "payload": execution_payload,
                "actor": str((trigger or {}).get("actor") or "local-web"),
            }
        )
        accepted.update(
            {
                "coordination": True,
                "state_root": str(self.store.state_dir),
                "status": accepted.get("job_status", "queued"),
                "input_hash": input_hash,
            }
        )
        return accepted

    def create_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.coordination_enabled():
            return self.create_coordination_run(payload)
        loop_id = str(payload.get("loop_id") or "daily-radar")
        loop = next((item for item in LOOPS if item["id"] == loop_id), None)
        if loop is None:
            raise ValueError(f"unknown loop_id: {loop_id}")
        if loop_id in CONCEPT_LOOP_IDS or loop.get("executor") in {"concept-review-control-plane", "concept-status-observer", "concept-recheck-runner"}:
            self._concept_write_rejected()
        permission_mode = str(payload.get("permission_mode") or loop["permission_mode"])
        if permission_mode != loop["permission_mode"]:
            raise ValueError(f"permission_mode is fixed by Loop Registry: {loop['permission_mode']}")
        scope = self._normalize_loop_scope(loop, payload.get("scope") or {})
        loop_contract = {
            key: loop.get(key)
            for key in ("id", "title", "executor", "input_schema", "snapshot_sources", "analysis_instruction", "write_allowlist")
        }
        input_hash = content_hash(
            json.dumps({"loop_id": loop_id, "scope": scope, "loop_contract": loop_contract}, ensure_ascii=False, sort_keys=True)
        )
        request = self.store.create(
            {
                "loop_id": loop_id,
                "trigger": payload.get("trigger") or {"kind": "manual", "actor": "local-web"},
                "scope": scope,
                "permission_mode": permission_mode,
                "loop_contract": loop_contract,
                "input_hash": input_hash,
                "runtime": {"kind": "codex", "executor": loop.get("executor"), "input_hash": input_hash},
                "record": bool(payload.get("record", False)),
            }
        )
        self.start_runner(request["run_id"])
        return {
            **request,
            "state_root": str(self.store.state_dir),
            "status": self.store.state(request["run_id"]).get("status"),
            "input_hash": input_hash,
        }

    def run_analysis(self, run_id: str) -> Dict[str, Any]:
        """Return a structured analysis artifact when an executor produced one.

        The current safe runner writes a deterministic draft, not an invented
        LLM conclusion.  Returning ``available: false`` keeps the v2 UI honest
        until a loop-specific executor emits analysis/analysis.json.
        """
        coordination = self.coordination_run(run_id)
        if coordination is not None:
            path = self.coordination_artifact_root() / str(run_id) / "analysis" / "analysis.json"
            if not path.is_file():
                return {"available": False, "run_id": run_id, "schema_version": "pm-loop.analysis.v2"}
            value = self._read_json_file(path, {})
            if not isinstance(value, dict):
                raise ValueError(f"analysis artifact must be an object: {path}")
            value.setdefault("available", True)
            value.setdefault("run_id", run_id)
            return value
        # A known run with no artifact is a valid, expected state.  Validate
        # the run first so an unknown id cannot be mistaken for that state.
        self.store.request(run_id)
        path = self.store.paths(run_id).root / "analysis" / "analysis.json"
        if not path.is_file():
            return {"available": False, "run_id": run_id, "schema_version": "pm-loop.analysis.v2"}
        value = self._read_json_file(path, {})
        if not isinstance(value, dict):
            raise ValueError(f"analysis artifact must be an object: {path}")
        value.setdefault("available", True)
        value.setdefault("run_id", run_id)
        return value

    def run_decision(self, run_id: str) -> Dict[str, Any]:
        coordination = self.coordination_run(run_id)
        if coordination is not None:
            path = self.coordination_artifact_root() / str(run_id) / "decision" / "decision.json"
            if not path.is_file():
                return {"available": False, "run_id": run_id, "schema_version": "pm-loop.decision.v2"}
            value = self._read_json_file(path, {})
            if not isinstance(value, dict):
                raise ValueError(f"decision artifact must be an object: {path}")
            value.setdefault("available", True)
            value.setdefault("run_id", run_id)
            return value
        self.store.request(run_id)
        path = self.store.paths(run_id).root / "decision" / "decision.json"
        if not path.is_file():
            return {"available": False, "run_id": run_id, "schema_version": "pm-loop.decision.v2"}
        value = self._read_json_file(path, {})
        if not isinstance(value, dict):
            raise ValueError(f"decision artifact must be an object: {path}")
        value.setdefault("available", True)
        value.setdefault("run_id", run_id)
        return value

    def run_log(self, run_id: str) -> Dict[str, Any]:
        coordination = self.coordination_run(run_id)
        if coordination is not None:
            path = self.coordination_artifact_root() / str(run_id) / "worker.log"
            if not path.is_file():
                return {"available": False, "run_id": run_id, "text": ""}
            return {"available": True, "run_id": run_id, "path": str(path), "text": path.read_text(encoding="utf-8", errors="replace")[-20000:]}
        self.store.request(run_id)
        path = self.store.paths(run_id).runner_log
        if not path.is_file():
            return {"available": False, "run_id": run_id, "text": ""}
        try:
            text = path.read_text(encoding="utf-8", errors="replace")[-20000:]
        except OSError as exc:
            raise FileNotFoundError(str(exc))
        return {"available": True, "run_id": run_id, "path": str(path), "text": text}

    def run_report(self, run_id: str) -> Dict[str, Any]:
        coordination = self.coordination_run(run_id)
        if coordination is not None:
            path = self.coordination_artifact_root() / str(run_id) / "draft" / "report.md"
            if not path.is_file():
                return {"available": False, "run_id": run_id, "text": ""}
            return {"available": True, "run_id": run_id, "path": str(path), "text": path.read_text(encoding="utf-8", errors="replace")[-50000:]}
        self.store.request(run_id)
        path = self.store.paths(run_id).draft
        if not path.is_file():
            return {"available": False, "run_id": run_id, "text": ""}
        return {"available": True, "run_id": run_id, "path": str(path), "text": path.read_text(encoding="utf-8", errors="replace")[-50000:]}

    def diagnostics(self) -> Dict[str, Any]:
        cli_candidates = [
            self.codex_root.parent / ".baidu-cx" / "baidu-cx" / "bin" / "codex",
            Path("/Applications/ChatGPT.app/Contents/Resources/codex"),
        ]
        cli_path = next((str(path) for path in cli_candidates if path.is_file()), None)
        return {
            "schema_version": "pm-loop.diagnostics.v2",
            "checked_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "service": {"status": "ok", "pid": os.getpid(), "resident": True, "state_root": str(self.store.state_dir)},
            "runner": {"path": str(RUNNER), "available": RUNNER.is_file(), "mode": "one-shot"},
            "action_runner": {"path": str(ACTION_RUNNER), "available": ACTION_RUNNER.is_file(), "mode": "one-shot"},
            "adapter": {"path": str(self.adapter_script), "available": self.adapter_script.is_file()},
            "codex_cli": {"path": cli_path, "available": bool(cli_path)},
            "openviking": {"config_path": str(Path.home() / ".openviking" / "ovcli.conf"), "configured": (Path.home() / ".openviking" / "ovcli.conf").is_file(), "status": "checked_by_snapshot"},
            "queue": self.queue_status(),
            "concept_learning": self.refresh_status(),
        }

    def last_run_at(self) -> Optional[str]:
        """Return the newest persisted event timestamp for health reporting."""
        latest: Optional[tuple[float, str]] = None
        for state in self.store.list_states_read_only():
            last_event = state.get("last_event")
            value = last_event.get("at") if isinstance(last_event, dict) else None
            if not value:
                continue
            try:
                stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                timestamp = stamp.timestamp()
            except (TypeError, ValueError, OverflowError):
                continue
            if latest is None or timestamp > latest[0]:
                latest = (timestamp, str(value))
        return latest[1] if latest else None

    def rerun(self, run_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        coordination = self.coordination_run(run_id)
        if coordination is not None and self.coordination_store is not None:
            with self.coordination_store.connect() as connection:
                row = connection.execute("SELECT profile,payload_json FROM jobs WHERE run_id=?", (str(run_id),)).fetchone()
            if row is None:
                raise KeyError(run_id)
            original = json.loads(row[1] or "{}")
            body = payload or {}
            original["scope"] = body.get("scope") if isinstance(body.get("scope"), dict) else original.get("scope") or {}
            original["trigger"] = {"kind": "rerun", "actor": "local-web", "rerun_of": str(run_id)}
            original["idempotency_key"] = f"rerun:{run_id}:{content_hash(json.dumps(original['scope'], ensure_ascii=False, sort_keys=True))}"
            original["profile"] = row[0]
            return self.create_coordination_run(original)
        request = self.store.request(run_id)
        body = payload or {}
        new_request = {
            "loop_id": request.get("loop_id"),
            "scope": body.get("scope") if isinstance(body.get("scope"), dict) else request.get("scope") or {},
            "permission_mode": request.get("permission_mode"),
            "record": request.get("record", False),
            "trigger": {"kind": "rerun", "actor": "local-web", "rerun_of": run_id},
        }
        return self.create_run(new_request)

    def gate_decision(self, run_id: str, action: str, note: str = "") -> Dict[str, Any]:
        return self._gate_decision(run_id, action, note, "")

    def _gate_decision(self, run_id: str, action: str, note: str = "", gate_token: str = "") -> Dict[str, Any]:
        state = self.store.state_read_only(run_id)
        if state.get("loop_id") in {"concept-review", "concept-recheck"}:
            self._concept_write_rejected()
        if state.get("status") != "awaiting_human":
            raise ValueError(f"run is not awaiting human decision: {state.get('status')}")
        action = str(action)
        if action == "approve":
            decision = self.run_decision(run_id)
            gate = decision.get("gate") if isinstance(decision.get("gate"), dict) else {}
            if gate.get("required"):
                if not gate_token or gate_token != gate.get("token"):
                    raise ValueError("gate_token is required and must match the approved decision")
                self.store.append(
                    run_id,
                    "gate/approved",
                    {"note": note, "actor": "zhujie14", "gate_token": gate_token, "snapshot_id": decision.get("snapshot_id"), "action_hashes": [item.get("action_hash") for item in decision.get("proposed_actions") or [] if item.get("requires_gate")]},
                    actor="reviewer",
                )
                self.store.append(run_id, "action/queued", {"run_id": run_id, "queue": "safe-draft", "gate_token": gate_token}, actor="control-plane")
                self.start_action_runner(run_id)
            else:
                # Backward-compatible path for old manually-created test runs.
                self.store.append(run_id, "gate/approved", {"note": note, "actor": "zhujie14"}, actor="reviewer")
                request = self.store.request(run_id)
                action_id = str((request.get("scope") or {}).get("action_id") or "")
                if action_id:
                    self.store.append(run_id, "action/queued", {"action_id": action_id}, actor="control-plane")
                    self.store.append(run_id, "action/started", {"action_id": action_id, "mode": "safe-draft"}, actor="control-plane")
                    path = self.store.paths(run_id).root / "draft" / f"action-{action_id}.md"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(f"# Approved action {action_id}\n\n- reviewer: zhujie14\n- note: {note}\n\nThis is a local draft action; no external source was modified.\n", encoding="utf-8")
                    self.store.append(run_id, "action/completed", {"action_id": action_id, "path": str(path), "writes": []}, actor="control-plane")
                self.store.append(run_id, "run/completed", {"gate": "approved", "note": note}, actor="control-plane")
        elif action in {"changes", "changes_requested"}:
            self.store.append(run_id, "gate/changes_requested", {"note": note, "actor": "zhujie14"}, actor="reviewer")
        elif action == "pause":
            self.store.append(run_id, "gate/paused", {"note": note, "actor": "zhujie14"}, actor="reviewer")
        else:
            raise ValueError(f"unknown gate action: {action}")
        return self.store.state(run_id)

    def start_action_runner(self, run_id: str) -> None:
        if self.store.state_read_only(run_id).get("loop_id") in {"concept-review", "concept-recheck"}:
            self._concept_write_rejected()
        command = [sys.executable, str(ACTION_RUNNER), "--run-id", run_id, "--state-dir", str(self.store.state_dir)]
        log_path = self.store.paths(run_id).root / "action-runner.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_stream = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(command, cwd=str(self.project_root), stdout=log_stream, stderr=subprocess.STDOUT, text=True)
        with self.lock:
            self.processes[f"action:{run_id}"] = process
        threading.Thread(target=self._wait_action_runner, args=(run_id, process, log_stream), daemon=True).start()

    def _wait_action_runner(self, run_id: str, process: subprocess.Popen[str], log_stream: Any) -> None:
        try:
            process.wait()
        finally:
            log_stream.close()
            with self.lock:
                self.processes.pop(f"action:{run_id}", None)

    def start_runner(self, run_id: str) -> None:
        state = self.store.state_read_only(run_id)
        if state.get("loop_id") in CONCEPT_LOOP_IDS:
            self._concept_write_rejected()
        command = [
            sys.executable,
            str(RUNNER),
            "run",
            "--run-id",
            run_id,
            "--state-dir",
            str(self.store.state_dir),
            "--adapter",
            str(self.adapter_script),
            "--project-root",
            str(self.project_root),
            "--codex-root",
            str(self.codex_root),
        ]
        if self.snapshot_path:
            command.extend(["--snapshot", str(self.snapshot_path)])
            # A supplied snapshot is an explicit deterministic fixture/replay
            # boundary. Production runs without this flag invoke Codex.
            command.extend(["--analysis-mode", "snapshot-only"])
        log_path = self.store.paths(run_id).runner_log
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_stream = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(command, stdout=log_stream, stderr=subprocess.STDOUT, text=True)
        with self.lock:
            self.processes[run_id] = process
        threading.Thread(target=self._wait_runner, args=(run_id, process, log_stream), daemon=True).start()

    def _wait_runner(self, run_id: str, process: subprocess.Popen[str], log_stream: Any) -> None:
        try:
            process.wait()
        finally:
            log_stream.close()
            with self.lock:
                self.processes.pop(run_id, None)

    def cancel(self, run_id: str) -> Dict[str, Any]:
        coordination = self.coordination_run(run_id)
        if coordination is not None and self.coordination_store is not None:
            Scheduler(self.coordination_store, max_slots=0).cancel(str(run_id), reason="api_cancel")
            return self.coordination_store.get_run(str(run_id)) or {"run_id": run_id, "status": "unknown"}
        if self.store.state_read_only(run_id).get("loop_id") in {"concept-review", "concept-recheck"}:
            self._concept_write_rejected()
        publish_item = self._cancel_queued_publish(run_id)
        if publish_item is not None:
            return self.store.state(run_id)
        paths = self.store.paths(run_id)
        paths.root.mkdir(parents=True, exist_ok=True)
        paths.cancel_marker.write_text("cancel requested\n", encoding="utf-8")
        with self.lock:
            process = self.processes.get(run_id)
        if process and process.poll() is None:
            process.terminate()
        if self.store.state(run_id).get("status") not in TERMINAL_STATES:
            self.store.append(run_id, "run/cancelled", {"reason": "api_cancel"}, actor="control-plane")
        return self.store.state(run_id)


class Handler(BaseHTTPRequestHandler):
    server: "ControlPlaneHTTPServer"

    @staticmethod
    def _v4_resource(value: Dict[str, Any], field: str, *, source_status: Optional[str] = None) -> Dict[str, Any]:
        """Build the stable metadata envelope shared by every V4 resource."""
        section = value.get(field)
        section_map = section if isinstance(section, dict) else {}
        effective_source = str(source_status or section_map.get("source_status") or value.get("source_status") or "unknown")
        not_implemented = effective_source in {"not_implemented", "unknown", "design_manifest"}
        return {
            "schema_version": value.get("schema_version"),
            "read_only": True,
            "read_at": value.get("read_at"),
            "as_of": value.get("as_of") or value.get("read_at"),
            "source_status": effective_source,
            "source_cursor": value.get("source_cursor") or value.get("source_version"),
            "source_version": value.get("source_version"),
            "metric_source": value.get("metric_source") or "pm-system.db",
            "freshness": "unknown" if not_implemented else section_map.get("freshness") or value.get("freshness") or "unknown",
            "evidence_status": "unknown" if not_implemented else section_map.get("evidence_status") or value.get("evidence_status") or "observed",
            field: section,
        }

    def _json(
        self,
        status: int,
        value: Dict[str, Any],
        *,
        etag: Optional[str] = None,
        compress: bool = True,
        cache_control: str = "private, max-age=0, must-revalidate",
    ) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        # ETag is content-addressed, so a polling client can avoid downloading
        # an unchanged summary/list.  The optional explicit tag lets the cheap
        # summary endpoint use its mutation version without re-hashing JSON.
        tag_value = str(etag or ("sha256:" + hashlib.sha256(payload).hexdigest()))
        if not tag_value.startswith("\""):
            tag_value = '"' + tag_value + '"'
        if self.headers.get("If-None-Match", "") == tag_value and status == 200:
            self.send_response(304)
            self.send_header("ETag", tag_value)
            self.send_header("Cache-Control", cache_control)
            self.end_headers()
            return
        encoded = payload
        compressed = False
        accepts_gzip = "gzip" in self.headers.get("Accept-Encoding", "").lower()
        if compress and accepts_gzip and len(payload) >= 512:
            encoded = gzip.compress(payload, compresslevel=6, mtime=0)
            compressed = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", cache_control)
        self.send_header("ETag", tag_value)
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            # A polling client may cancel an obsolete snapshot after the
            # summary version changes.  The client closed the connection; it
            # is not an application error and should not trigger a second
            # 500 response/log traceback.
            self.close_connection = True

    def _body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        value = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def _reject_concept_write(self) -> None:
        self._json(
            405,
            self.server.controller.concept_write_response(endpoint=urlparse(self.path).path),
        )

    def _concept_write_blocked(self, path: str, body: Optional[Dict[str, Any]] = None) -> bool:
        if path in {"/api/concept-recheck", "/api/concept-discovery/seeds", "/api/concept-review/commit"}:
            return True
        if path.startswith("/api/concepts/"):
            return path.endswith("/review") or path.endswith("/agent-refresh")
        if path == "/api/runs" and isinstance(body, dict):
            return body.get("loop_id") in {"concept-review", "concept-recheck"}
        if path.startswith("/api/gates/"):
            run_id = path.split("/")[3] if len(path.split("/")) > 3 else ""
            try:
                return self.server.controller.store.state_read_only(run_id).get("loop_id") in {"concept-review", "concept-recheck"}
            except (FileNotFoundError, ValueError):
                return False
        if path.startswith("/api/runs/"):
            parts = path.split("/")
            if len(parts) < 5 or parts[4] not in {"cancel", "retry", "rerun"}:
                return False
            try:
                return self.server.controller.store.state_read_only(parts[3]).get("loop_id") in {"concept-review", "concept-recheck"}
            except (FileNotFoundError, ValueError):
                return False
        return False

    def _reject_v4_method(self, *, head_only: bool = False) -> None:
        value = {"error": "method_not_allowed", "read_only": True, "allow": ["GET"]}
        if not head_only:
            self._json(405, value, cache_control="no-store")
            return
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(405)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Allow", "GET")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        controller = self.server.controller
        try:
            if path == "/":
                self._serve_index()
            elif path == "/v3":
                # Stable explicit alias for the live V3 surface.  It serves
                # the same API-backed page as `/`, so bookmarks do not fall
                # back to the historical demo.
                self._serve_index()
            elif path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            elif path == "/v2-demo":
                self._serve_file(V2_DEMO_PAGE)
            elif path == "/concept-review":
                self._serve_file(CONCEPT_REVIEW_PAGE)
            elif path == "/health-report":
                report_path = controller.health_report_path()
                if not report_path:
                    self._json(404, {"error": "health_report_not_found"})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path == "/reports/pm-timeline/latest":
                report_path = controller.pm_timeline_review_path()
                if not report_path:
                    self._json(404, {"error": "pm_timeline_review_not_found"})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path == "/reports/competitive/latest":
                report_path = controller.competitive_radar_report_path()
                if not report_path:
                    self._json(404, {"error": "competitive_radar_report_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path.startswith("/artifacts/registry/"):
                parts = path.split("/")
                if len(parts) != 5:
                    self._json(404, {"error": "artifact_representation_not_found", "read_only": True})
                    return
                artifact_id, kind = unquote(parts[3]), unquote(parts[4])
                report_path = controller.artifact_registry_read_model.open_path(artifact_id, kind)
                if report_path is None:
                    self._json(404, {"error": "artifact_representation_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path.startswith("/artifacts/role-outputs/"):
                output_id = unquote(path.rsplit("/", 1)[-1])
                report_path = controller.role_output_path(output_id)
                if not report_path:
                    self._json(404, {"error": "role_output_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path.startswith("/artifacts/reviews/"):
                run_id = unquote(path.rsplit("/", 1)[-1])
                report_path = controller.review_artifact_path(run_id)
                if not report_path:
                    self._json(404, {"error": "review_artifact_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path.startswith("/reports/concepts/"):
                kind = path.rsplit("/", 1)[-1]
                report_path = controller.concept_artifact_path(kind)
                if not report_path:
                    self._json(404, {"error": "concept_artifact_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path.startswith("/reports/retention/"):
                kind = path.rsplit("/", 1)[-1]
                report_path = controller.retention_artifact_path(kind)
                if not report_path:
                    self._json(404, {"error": "retention_artifact_not_found", "read_only": True})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path in {"/reports/gaps/latest", "/reports/materials/latest"}:
                domain = "gaps" if path == "/reports/gaps/latest" else "materials"
                report_path = controller.domain_report_path(domain)
                if not report_path:
                    self._json(404, {"error": f"{domain}_report_not_found"})
                else:
                    self._serve_file(report_path, cache_control="private, max-age=0, must-revalidate")
            elif path == "/api/health":
                self._json(200, {"status": "ok", "runtime": "codex", "state_dir": str(controller.store.state_dir), "state_root": str(controller.store.state_dir), "mode": "local", "last_run_at": controller.last_run_at(), "service": "resident-capable", "queue": controller.queue_status(), "name_fingerprint": controller._name_fingerprint_status()})
            elif path in {"/api/control-plane/summary", "/api/control-plane/v3/summary"}:
                summary = controller.control_plane_summary()
                self._json(200, summary, etag=summary.get("version"))
            elif path == "/api/control-plane/v4/artifacts" or path.startswith("/api/control-plane/v4/artifacts/") or path == "/api/control-plane/v4/artifact-facets":
                query = parse_qs(parsed.query)
                registry = controller.artifact_registry_read_model
                if path == "/api/control-plane/v4/artifacts":
                    try:
                        cursor = int((query.get("cursor") or [0])[0])
                        limit = int((query.get("limit") or [50])[0])
                    except (TypeError, ValueError):
                        self._json(400, {"error": "invalid_pagination", "read_only": True}, cache_control="no-store")
                        return
                    value = registry.list_artifacts(
                        cursor=cursor,
                        limit=limit,
                        search=str((query.get("search") or [""])[0]),
                        artifact_domain=str((query.get("artifact_domain") or [""])[0]),
                        artifact_type=str((query.get("artifact_type") or [""])[0]),
                        status=str((query.get("status") or [""])[0]),
                        source_kind=str((query.get("source_kind") or [""])[0]),
                        time_scope=str((query.get("time_scope") or [""])[0]),
                    )
                elif path == "/api/control-plane/v4/artifact-facets":
                    value = registry.facets()
                else:
                    artifact_id = unquote(path.rsplit("/", 1)[-1])
                    try:
                        value = registry.detail(artifact_id)
                    except KeyError:
                        self._json(404, {"error": "artifact_not_found", "read_only": True}, cache_control="no-store")
                        return
                self._json(200, value, etag=value.get("source_version"), cache_control="no-store")
            elif path == "/api/control-plane/v4/retention" or path.startswith("/api/control-plane/v4/retention/"):
                if path == "/api/control-plane/v4/retention":
                    value = controller.retention_read_model.snapshot()
                    value["artifacts"] = controller.retention_artifact_projection()
                elif path == "/api/control-plane/v4/retention/summary":
                    value = controller.retention_read_model.summary()
                elif path in {"/api/control-plane/v4/retention/sources", "/api/control-plane/v4/retention/actions", "/api/control-plane/v4/retention/unknowns"}:
                    value = controller.retention_read_model.resource(path.rsplit("/", 1)[-1], parse_qs(parsed.query))
                elif path.startswith("/api/control-plane/v4/retention/plans/"):
                    plan_id = unquote(path.rsplit("/", 1)[-1])
                    try:
                        value = controller.retention_read_model.plan(plan_id)
                    except KeyError:
                        self._json(404, {"error": "retention_plan_not_found", "read_only": True}, cache_control="no-store")
                        return
                else:
                    self._json(404, {"error": "not_found", "read_only": True}, cache_control="no-store")
                    return
                self._json(200, value, etag=value.get("source_version"), cache_control="no-store")
            elif path in {
                "/api/control-plane/v4/summary",
                "/api/control-plane/v4/modules",
                "/api/control-plane/v4/incidents",
                "/api/control-plane/v4/queues",
                "/api/control-plane/v4/runs",
                "/api/control-plane/v4/activity",
                "/api/control-plane/v4/work-items",
                "/api/control-plane/v4/plans",
                "/api/control-plane/v4/reviews",
                "/api/control-plane/v4/operations",
                "/api/control-plane/v4/roles",
                "/api/control-plane/v4/concepts",
                "/api/control-plane/v4/schedules",
                "/api/control-plane/v4/competitive-radar",
            } or (path.startswith("/api/control-plane/v4/runs/") and path.count("/") == 5):
                cockpit = controller.v44_cockpit
                if cockpit is None:
                    self._json(404, {"error": "v4_cockpit_unavailable", "read_only": True})
                elif path.endswith("/summary"):
                    value = cockpit.snapshot()
                    value["domains"] = {
                        "gaps": controller.domain_report_projection("gaps"),
                        "materials": controller.domain_report_projection("materials"),
                    }
                    value["health_report"] = controller.health_report_projection()
                    value["artifact_registry"] = controller.artifact_registry_read_model.summary()
                    # Report files live outside PM system DB; include their
                    # compact signature so browser ETags and polling notice a
                    # newly generated HTML artifact.
                    report_signature = content_hash(
                        json.dumps(
                            {
                                "domains": value["domains"],
                                "health_report": value["health_report"],
                                "artifact_registry": value["artifact_registry"],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    )
                    base_source_version = str(value.get("source_version") or "unknown")
                    value["source_version"] = f"{base_source_version}:reports-{report_signature[:16]}"
                    value["source_cursor"] = value["source_version"]
                    self._json(200, value, etag=value.get("source_version"))
                elif path.endswith("/modules"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "modules"), etag=value.get("source_version"))
                elif path.endswith("/incidents"):
                    value = cockpit.snapshot()
                    payload = self._v4_resource(value, "incidents")
                    payload["ops_attention_view"] = value.get("ops_attention_view")
                    self._json(200, payload, etag=value.get("source_version"))
                elif path.endswith("/queues"):
                    value = cockpit.snapshot()
                    payload = self._v4_resource(value, "queues")
                    payload["providers"] = value["providers"]
                    self._json(200, payload, etag=value.get("source_version"))
                elif path.endswith("/runs"):
                    value = cockpit.list_runs()
                    self._json(200, value, etag=value.get("source_version"))
                elif path.endswith("/activity"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "activity"), etag=value.get("source_version"))
                elif path.endswith("/work-items"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "work_items"), etag=value.get("source_version"))
                elif path.endswith("/plans"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "plans"), etag=value.get("source_version"))
                elif path.endswith("/reviews"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "reviews"), etag=value.get("source_version"))
                elif path.endswith("/operations"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "operations"), etag=value.get("source_version"))
                elif path.endswith("/schedules"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "schedules"), etag=value.get("source_version"))
                elif path.endswith("/roles"):
                    value = cockpit.snapshot()
                    self._json(200, self._v4_resource(value, "roles"), etag=value.get("source_version"))
                elif path.endswith("/concepts"):
                    value = cockpit.snapshot()
                    payload = self._v4_resource(value, "concepts")
                    payload["gate"] = value["gates"]["concept_view_gate"]
                    self._json(200, payload, etag=value.get("source_version"))
                elif path.endswith("/competitive-radar"):
                    value = controller.competitive_radar_read_model().snapshot()
                    self._json(200, value, etag=value.get("source_version"))
                else:
                    run_id = unquote(path.rsplit("/", 1)[-1])
                    try:
                        detail = cockpit.run_detail(run_id)
                    except KeyError:
                        self._json(
                            404,
                            {"error": "run_not_found", "run_id": run_id, "read_only": True},
                        )
                    else:
                        self._json(200, detail, etag=detail.get("source_version"))
            elif path in {"/api/control-plane/jobs", "/api/control-plane/v3/jobs"}:
                query = parse_qs(parsed.query)
                jobs = controller.control_plane_jobs(
                    limit=(query.get("limit") or query.get("page_size") or [CONTROL_PLANE_JOB_LIMIT_DEFAULT])[0],
                    status=(query.get("status") or [""])[0],
                )
                self._json(200, jobs, etag=jobs.get("source_version"))
            elif path in {"/api/control-plane/snapshot", "/api/control-plane/v3/snapshot"}:
                query = parse_qs(parsed.query)
                force = controller._query_true(query, "fresh", "force", "refresh")
                snapshot = controller.control_plane_snapshot(force=force)
                self._json(200, snapshot, etag=snapshot.get("source_version") or snapshot.get("version"))
            elif path == "/api/loops":
                self._json(200, {"loops": LOOPS})
            elif path == "/api/concepts":
                query = parse_qs(parsed.query)
                include_details = controller._query_true(query, "details", "include_details")
                self._json(
                    200,
                    {
                        "concepts": controller.concepts(include_candidate_details=include_details),
                        "staged": controller._read_staged(),
                        "workflow": controller.concept_workflow_status(),
                        "staged_actionable": 0,
                    },
                )
            elif path == "/api/concept-review/staged":
                self._json(
                    200,
                    {
                        "staged": controller._read_staged(),
                        "disabled": True,
                        "read_only": True,
                        "history_only": True,
                        "actionable": 0,
                        "status": "disabled",
                        "reason": CONCEPT_WORKFLOW_REASON,
                    },
                )
            elif path == "/api/candidates":
                model = controller.candidates_read_model(parse_qs(parsed.query))
                self._json(200, model)
            elif path.startswith("/api/candidates/"):
                candidate_id = unquote(path.split("/", 3)[3])
                self._json(200, controller.candidate_projection(controller.learning.read_candidate(candidate_id)))
            elif path == "/api/usage":
                self._json(200, controller.learning.usage_summary())
            elif path.startswith("/api/usage/"):
                concept_name = unquote(path.split("/", 3)[3])
                self._json(200, controller.learning.usage_summary(concept_name))
            elif path == "/api/concept-discovery":
                self._json(200, {"runs": controller.learning.discovery_runs()})
            elif path == "/api/concept-recheck/status":
                self._json(200, controller.concept_recheck_status())
            elif path == "/api/queue/status":
                self._json(200, controller.queue_status())
            elif path == "/api/refresh/status":
                self._json(200, controller.refresh_status())
            elif path.startswith("/api/concepts/"):
                name = unquote(path.split("/", 3)[3])
                self._json(200, controller.concept(name))
            elif path == "/api/runs":
                if controller.coordination_enabled() and controller.coordination_read_store is not None:
                    values = controller.coordination_read_store.list_runs(limit=500)
                    self._json(200, {"runs": values, "coordination": True, "read_only": True})
                    return
                values = controller.store.list_states_read_only()
                query = parse_qs(parsed.query)
                loop_filter = (query.get("loop_id") or [""])[0]
                status_filter = (query.get("status") or [""])[0]
                if loop_filter:
                    values = [item for item in values if item.get("loop_id") == loop_filter]
                if status_filter:
                    values = [item for item in values if item.get("status") == status_filter]
                try:
                    limit = max(1, min(int((query.get("limit") or ["100"])[0]), 500))
                except (TypeError, ValueError):
                    limit = 100
                self._json(200, {"runs": values[:limit]})
            elif path.startswith("/api/runs/") and path.endswith("/analysis"):
                self._json(200, controller.run_analysis(path.split("/")[3]))
            elif path.startswith("/api/runs/") and path.endswith("/decision"):
                self._json(200, controller.run_decision(path.split("/")[3]))
            elif path.startswith("/api/runs/") and path.endswith("/log"):
                self._json(200, controller.run_log(path.split("/")[3]))
            elif path.startswith("/api/runs/") and path.endswith("/report"):
                self._json(200, controller.run_report(path.split("/")[3]))
            elif path == "/api/state/diagnostics":
                self._json(200, controller.diagnostics())
            elif path.startswith("/api/runs/") and path.endswith("/events"):
                run_id = path.split("/")[3]
                self._stream_events(run_id)
            elif path.startswith("/api/runs/") and path.endswith("/snapshot"):
                run_id = unquote(path.split("/")[3])
                if controller.coordination_enabled() and controller.coordination_read_store is not None:
                    if controller.coordination_read_store.get_run(run_id) is None:
                        raise FileNotFoundError(f"unknown run: {run_id}")
                    snapshot_path = controller.coordination_artifact_root() / run_id / "snapshot.json"
                else:
                    snapshot_path = controller.store.paths(run_id).snapshot
                if not snapshot_path.is_file():
                    raise FileNotFoundError(f"snapshot not available for run: {run_id}")
                self._json(200, json.loads(snapshot_path.read_text(encoding="utf-8")))
            elif path.startswith("/api/runs/") and path.count("/") == 3:
                run_id = unquote(path.rsplit("/", 1)[-1])
                if controller.coordination_enabled() and controller.coordination_store is not None:
                    try:
                        self._json(200, controller.coordination_run_detail(run_id))
                    except KeyError:
                        self._json(404, {"error": "run_not_found", "run_id": run_id, "read_only": True})
                else:
                    self._json(200, controller.store.state_read_only(run_id))
            elif path.startswith("/api/runs/"):
                run_id = path.split("/")[3]
                self._json(200, controller.store.state_read_only(run_id))
            else:
                self._json(404, {"error": "not_found"})
        except FileNotFoundError as exc:
            self._json(404, {"error": str(exc)})
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc)})
        except Exception as exc:  # preserve a JSON response for local debugging
            self._json(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        controller = self.server.controller
        try:
            if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
                self._reject_v4_method()
                return
            body = self._body()
            if self._concept_write_blocked(path, body):
                self._reject_concept_write()
                return
            # During a migration freeze the public write surface must fail
            # explicitly, so clients do not retry a 500 and accidentally
            # create work after the drain fence. GET/read-model routes remain
            # side-effect free and continue to be served by do_GET.
            if path in {"/api/runs", "/api/control-plane/jobs", "/api/control-plane/v3/jobs"} and controller.coordination_store is not None:
                freeze = controller.coordination_store.migration_freeze()
                if freeze is not None and str(freeze.get("state", "")).lower() in {"freeze", "draining", "read_only", "maintenance"}:
                    self._json(405, {"error": "migration_freeze", "migration_id": freeze.get("migration_id"), "stage_id": freeze.get("stage_id"), "read_only": True, "allow": ["GET"]})
                    return
            if path in {"/api/control-plane/jobs", "/api/control-plane/v3/jobs"}:
                # The cockpit is observation-only. Historical intent records
                # remain readable, but the old POST handoff is permanently
                # fenced so execution cannot be smuggled through the UI.
                self._json(405, {"error": "legacy_control_plane_handoff_fenced", "read_only": True, "message": "请回到 Codex 在受控 Runtime 中执行", "allow": ["GET"]})
            elif path == "/api/runs":
                created = controller.create_run(body)
                self._json(202 if created.get("coordination") else 201, created)
            elif path == "/api/concept-recheck":
                self._json(202, controller.request_full_recheck(body))
            elif path == "/api/usage":
                self._json(201, {"event": controller.record_usage(body), "summary": controller.learning.usage_summary()})
            elif path == "/api/concept-discovery/seeds":
                self._json(201, {"seed": controller.record_manual_seed(body), "next": "POST /api/concept-recheck"})
            elif path == "/api/concept-review/commit":
                self._json(200, controller.commit_reviews())
            elif path.startswith("/api/concepts/") and path.endswith("/review"):
                name = unquote(path.split("/", 3)[3][:-len("/review")])
                self._json(200, {"concept": controller.concept(name), "staged": controller.stage_review(name, body)})
            elif path.startswith("/api/concepts/") and path.endswith("/agent-refresh"):
                name = unquote(path.split("/", 3)[3][:-len("/agent-refresh")])
                self._json(202, controller.request_agent_refresh(name))
            elif path.startswith("/api/runs/") and path.endswith("/cancel"):
                run_id = path.split("/")[3]
                self._json(200, controller.cancel(run_id))
            elif path.startswith("/api/runs/") and path.endswith("/retry"):
                run_id = path.split("/")[3]
                if controller.coordination_run(run_id) is not None:
                    self._json(405, {"error": "coordination_retry_not_supported", "message": "协调库 Run 由 Worker/Scheduler 管理，请使用 rerun 创建新 Run", "read_only": True})
                    return
                self._json(202, {"queue": controller.retry_publish(run_id), "run": controller.store.state(run_id)})
            elif path.startswith("/api/runs/") and path.endswith("/rerun"):
                run_id = path.split("/")[3]
                self._json(202, controller.rerun(run_id, body))
            elif path.startswith("/api/runs/") and path.endswith("/replay"):
                run_id = path.split("/")[3]
                coordination = controller.coordination_run(run_id)
                if coordination is not None:
                    self._json(200, {"read_only": True, "coordination": True, **controller.coordination_run_detail(run_id)})
                else:
                    state = controller.store.state_read_only(run_id)
                    self._json(200, {"read_only": True, "coordination": False, "state": state, "events": controller.store.events_for(run_id)})
            elif path.startswith("/api/gates/"):
                parts = [unquote(item) for item in path.split("/") if item]
                if len(parts) != 4 or parts[0] != "api" or parts[1] != "gates":
                    self._json(404, {"error": "not_found"})
                else:
                    action = {"approve": "approve", "changes-requested": "changes", "pause": "pause"}.get(parts[3])
                    if not action:
                        self._json(404, {"error": "unknown_gate_action"})
                    else:
                        self._json(200, controller._gate_decision(parts[2], action, str(body.get("note") or ""), str(body.get("gate_token") or "")))
            else:
                self._json(404, {"error": "not_found"})
        except FileExistsError as exc:
            self._json(409, {"error": str(exc)})
        except FileNotFoundError as exc:
            self._json(404, {"error": str(exc)})
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc)})
        except PermissionError as exc:
            self._json(423, {"error": "admission_frozen", "message": str(exc), "read_only": True})
        except Exception as exc:
            self._json(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        controller = self.server.controller
        try:
            if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
                self._reject_v4_method()
                return
            if self._concept_write_blocked(path):
                self._reject_concept_write()
                return
            if path.startswith("/api/concepts/") and path.endswith("/review"):
                name = unquote(path.split("/", 3)[3][:-len("/review")])
                self._json(200, controller.remove_review(name))
            else:
                self._json(404, {"error": "not_found"})
        except FileNotFoundError as exc:
            self._json(404, {"error": str(exc)})
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc)})
        except Exception as exc:
            self._json(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_PUT(self) -> None:  # noqa: N802 - stdlib handler contract
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
            self._reject_v4_method()
            return
        self._json(405, {"error": "method_not_allowed", "allow": ["GET", "POST", "DELETE"]})

    def do_PATCH(self) -> None:  # noqa: N802 - stdlib handler contract
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
            self._reject_v4_method()
            return
        self._json(405, {"error": "method_not_allowed", "allow": ["GET", "POST", "DELETE"]})

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
            self._reject_v4_method(head_only=True)
            return
        self.send_error(501, "Unsupported method ('HEAD')")

    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib handler contract
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/control-plane/v4" or path.startswith("/api/control-plane/v4/"):
            self._reject_v4_method()
            return
        self.send_error(501, "Unsupported method ('OPTIONS')")

    def _serve_index(self) -> None:
        # The control plane is a live read model. Revalidate the shell on each
        # visit so a redeploy at the stable /v3 URL cannot leave stale copy or
        # client-side code in the browser cache.
        self._serve_file(
            self.server.controller.web_root / "index.html",
            cache_control="private, max-age=0, must-revalidate",
        )

    def _serve_file(self, path: Path, *, cache_control: Optional[str] = None) -> None:
        # Every browser-facing producer already resolves its path through a
        # fixed root.  Recheck the leaf here so a file replaced by a symlink
        # between discovery and response cannot be served.
        try:
            if path.is_symlink() or not path.is_file():
                self._json(404, {"error": "artifact_not_available", "read_only": True}, cache_control="no-store")
                return
            payload = path.read_bytes()
        except OSError:
            self._json(404, {"error": "artifact_not_available", "read_only": True}, cache_control="no-store")
            return
        suffix = path.suffix.lower()
        if suffix == ".json":
            content_type = "application/json; charset=utf-8"
        elif suffix == ".jsonl":
            content_type = "application/x-ndjson; charset=utf-8"
        elif suffix == ".md":
            content_type = "text/markdown; charset=utf-8"
        elif suffix == ".pdf":
            content_type = "application/pdf"
        elif suffix == ".txt":
            content_type = "text/plain; charset=utf-8"
        else:
            content_type = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        if cache_control:
            self.send_header("Cache-Control", cache_control)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _stream_events(self, run_id: str) -> None:
        controller = self.server.controller
        coordination = controller.coordination_enabled() and controller.coordination_read_store is not None and controller.coordination_read_store.get_run(run_id) is not None
        if coordination:
            event_reader = controller.coordination_events
        else:
            controller.store.state_read_only(run_id)  # validate without repairing files
            event_reader = controller.store.events_for
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        sent = 0
        deadline = time.time() + 30
        while time.time() < deadline:
            events = event_reader(run_id)
            for event in events[sent:]:
                payload = json.dumps(event, ensure_ascii=False)
                try:
                    self.wfile.write(f"id: {event.get('seq')}\ndata: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    self.close_connection = True
                    return
            sent = len(events)
            if events and events[-1].get("type") in {"run/completed", "run/failed", "run/cancelled", "run/rejected", "gate/rejected", "gate/paused", "gate/changes_requested"}:
                self.close_connection = True
                return
            time.sleep(0.2)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class ControlPlaneHTTPServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], controller: ControlPlane) -> None:
        super().__init__(address, Handler)
        self.controller = controller


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local Codex PM Loop Control Plane")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".codex" / "pm-loop")
    parser.add_argument("--adapter", type=Path, default=PROJECT_ROOT / "scripts" / "pm_loop_control_plane.py")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--evidence-project-root",
        type=Path,
        default=Path(os.environ["PM_CANONICAL_PROJECT_ROOT"]) if os.environ.get("PM_CANONICAL_PROJECT_ROOT") else None,
        help="canonical project root containing read-only P3 evidence packages",
    )
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--web-root", type=Path, default=WEB_ROOT)
    parser.add_argument("--snapshot", type=Path, help="optional fixed snapshot for deterministic acceptance tests")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    controller = ControlPlane(
        args.state_dir,
        args.adapter,
        args.project_root,
        args.codex_root,
        args.web_root,
        args.snapshot,
        args.evidence_project_root,
    )
    server = ControlPlaneHTTPServer((args.host, args.port), controller)
    print(json.dumps({"status": "ready", "url": f"http://{args.host}:{args.port}", "runtime": "codex"}, ensure_ascii=False), flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
