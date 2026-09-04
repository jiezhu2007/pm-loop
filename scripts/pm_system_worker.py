#!/usr/bin/env python3
"""Persistent V4.4 worker for coordination-store backed PM Loop runs.

The Control Plane only accepts work into ``pm-system.db``.  This process owns
slot leases and executes one claimed job at a time per worker thread.  Source
snapshots and model responses are artifacts; the SQLite store is the only
authoritative run state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from pm_loop_analysis import (
    PROMPT_VERSION,
    build_decision,
    build_prompt,
    canonical_hash,
    invoke_codex,
    normalize_analysis,
    parse_json_object,
)
from pm_loop_runner import parse_last_json, validate_snapshot
from pm_loop_runtime import atomic_json_write, now_iso
from artifact_manifest import write_worker_artifact_manifest
from concept_source_manifest import build_manifest, load_metadata_rows, write_manifest
from process_utils import run_process_group
from pm_scheduled_handlers import default_invoker as default_scheduled_invoker
from pm_scheduled_handlers import resolve_handler, scheduled_environment
from pm_ops_attention import refresh_ops_attention
from pm_resource_dispatcher import PMResourceDispatcher
from pm_memory_dispatcher import MemorySkillWriter
from pm_system_scheduler import ProviderThrottled, Scheduler
from pm_system_store import PMSystemStore


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
DEFAULT_STATE_DIR = CODEX_ROOT / "pm-loop"
DEFAULT_DB_PATH = DEFAULT_STATE_DIR / "state" / "pm-system.db"
DEFAULT_ARTIFACT_ROOT = DEFAULT_STATE_DIR / "runs"
TRANSIENT_MODEL_FAILURES = {"connection", "timeout", "504", "502", "503", "result_unknown"}


def _error_fingerprint(category: str, detail: str) -> str:
    return hashlib.sha256(f"{category}:{detail}".encode("utf-8")).hexdigest()[:20]


def _safe_text(value: Any, limit: int = 3000) -> str:
    return str(value or "").strip()[:limit]


def _model_failure(value: str) -> Tuple[str, Optional[str]]:
    text = str(value or "")
    if re.search(r"(?:\bHTTP\s*)?\b429\b|too many requests|rate[_ -]?limit", text, flags=re.IGNORECASE):
        matched = re.search(r"retry[-_ ]?after(?:\s*[:=]\s*|[\"']?\s*:\s*)([A-Za-z0-9,:+ -]+)", text, flags=re.IGNORECASE)
        return "429", matched.group(1).strip().strip("\"'") if matched else None
    return "result_unknown", None


def _remaining_seconds(deadline_at: Any) -> int:
    raw = str(deadline_at or "").strip()
    if not raw:
        return 0
    try:
        deadline = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return 0
    deadline = deadline.replace(tzinfo=deadline.tzinfo or timezone.utc).astimezone(timezone.utc)
    return max(0, int((deadline - datetime.now(timezone.utc)).total_seconds()))


def _positive_timeout(value: Any, *, default: int = 900) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(1, parsed)


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _safe_regular_file_under(path: Path, root: Path) -> Optional[Path]:
    """Return a regular, non-symlink file under one controlled root."""
    try:
        lexical_root = root.expanduser().absolute()
        lexical_path = path.expanduser().absolute()
        resolved_root = lexical_root.resolve(strict=True)
        try:
            relative = lexical_path.relative_to(lexical_root)
        except ValueError:
            # macOS exposes /var through /private/var. Accept that canonical
            # system alias only after resolving it back under the controlled
            # root; links inside the controlled root are still rejected below.
            lexical_path = lexical_path.resolve(strict=True)
            lexical_root = resolved_root
            relative = lexical_path.relative_to(lexical_root)
        current = lexical_root
        if current.is_symlink():
            return None
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                return None
        if not lexical_path.is_file():
            return None
        lexical_path.resolve(strict=True).relative_to(resolved_root)
        return lexical_path
    except (OSError, RuntimeError, ValueError):
        return None


class PMSystemWorker:
    """Claim and execute coordination-store jobs with bounded model retries."""

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        *,
        artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
        project_root: Path = PROJECT_ROOT,
        runtime_root: Optional[Path] = None,
        codex_root: Path = CODEX_ROOT,
        adapter_script: Optional[Path] = None,
        max_slots: Optional[int] = None,
        slot_ttl_seconds: int = 60,
        poll_interval: float = 0.25,
        max_model_attempts: int = 2,
        invoker: Optional[Callable[[str, int, Path], Tuple[int, str, str, str]]] = None,
        scheduled_invoker: Optional[Callable[..., Any]] = None,
    ) -> None:
        if max_model_attempts <= 0:
            raise ValueError("max_model_attempts must be positive")
        # Schema changes are owned by the migration runner.  Worker startup
        # must fail closed instead of silently changing the coordination DB.
        self.store = PMSystemStore(Path(db_path), auto_migrate=False)
        self.scheduler = Scheduler(self.store, max_slots=max_slots, slot_ttl_seconds=slot_ttl_seconds)
        self.artifact_root = Path(artifact_root).expanduser().resolve()
        self.project_root = Path(project_root).expanduser().resolve()
        self.runtime_root = Path(runtime_root or self.project_root).expanduser().resolve()
        self.codex_root = Path(codex_root).expanduser().resolve()
        self.adapter_script = Path(adapter_script or self.runtime_root / "scripts" / "pm_loop_control_plane.py").resolve()
        self.poll_interval = max(0.05, float(poll_interval))
        self.max_model_attempts = int(max_model_attempts)
        self.invoker = invoker or invoke_codex
        self.scheduled_invoker = scheduled_invoker or default_scheduled_invoker
        self.stop_event = threading.Event()
        # Resource dispatch is I/O-bound and does not consume Codex model
        # slots. Keep it in one lightweight loop so all PM writers share the
        # same Outbox, throttle bucket, and retry budget.
        self.resource_dispatcher = PMResourceDispatcher(self.store)
        # Memory Markdown mirroring has its own handler and projection. It
        # never enters the Resource API or semantic-task lane.
        self.memory_writer = MemorySkillWriter(self.store)

    def _artifact_dir(self, run_id: str) -> Path:
        path = self.artifact_root / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _request(self, claim: Dict[str, Any]) -> Dict[str, Any]:
        payload = claim.get("payload") if isinstance(claim.get("payload"), dict) else {}
        request = dict(payload)
        request.setdefault("run_id", claim["run_id"])
        request.setdefault("job_id", claim["job_id"])
        request.setdefault("loop_id", payload.get("loop_id") or "daily-radar")
        request.setdefault("permission_mode", payload.get("permission_mode") or "report")
        request.setdefault("scope", payload.get("scope") or {})
        request.setdefault("loop_contract", payload.get("loop_contract") or {})
        request.setdefault("runtime", {"kind": "codex"})
        for key in ("occurrence_id", "schedule_key", "trigger_kind", "registry_hash", "lock_key", "deadline_at", "scheduled_at", "local_scheduled_at"):
            if claim.get(key) is not None:
                # Job columns are authoritative; payload cannot rewrite the
                # schedule identity after the dispatcher accepted it.
                request[key] = claim[key]
        dependency = payload.get("dependency")
        if isinstance(dependency, dict):
            # The Scheduler stored this object while accepting the occurrence;
            # no caller-supplied environment value can replace it.
            request["dependency"] = dict(dependency)
        return request

    @staticmethod
    def _dependency_context_failure(request: Mapping[str, Any], envelope_path: Path) -> Optional[str]:
        """Reject malformed dependency jobs before starting their handler.

        Dependency planner inputs are Scheduler-owned identity.  Checking the
        required fields here prevents a malformed replay from becoming a
        child-process ``retry_wait``/handler failure and makes the rejection
        visible in the parent Run evidence.
        """
        if str(request.get("schedule_key") or "") != "concept-refresh-planner":
            return None
        if str(request.get("trigger_kind") or "") != "dependency":
            return "dependency_trigger_kind_required"
        dependency = request.get("dependency")
        if not isinstance(dependency, Mapping):
            return "dependency_context_missing:dependency"
        missing = [
            key
            for key in ("event_id", "source_manifest_path", "source_manifest_hash", "planner_version")
            if not str(dependency.get(key) or "").strip()
        ]
        if not str(request.get("db_path") or "").strip():
            missing.append("PM_SCHEDULE_DB_PATH")
        if not envelope_path.is_file():
            missing.append("PM_SCHEDULE_RUN_ENVELOPE")
        if missing:
            return "dependency_context_missing:" + ",".join(missing)
        return None

    def _append_weekly_dependency_event(
        self,
        request: Mapping[str, Any],
        *,
        run_id: str,
        finished_at: str,
        handler_status: str,
        failure_reason: Optional[str],
        handler_evidence_path: Path,
        artifact_dir: Path,
    ) -> Dict[str, Any]:
        """Append source-sync completion evidence for Scheduler consumption.

        The Worker can append evidence only. It never creates the dependent
        occurrence itself. A still-running enclosing Run is acceptable here:
        the Scheduler leaves the event pending until that Run is terminally
        completed, avoiding a crash window that would silently lose refresh.
        """
        planner_version = "concept-refresh-planner.v2"
        manifest_path: Optional[Path] = None
        manifest_hash = ""
        status = "blocked_by_upstream"
        reason = failure_reason or "upstream_handler_failed"
        if handler_status == "completed":
            ledgers = (
                self.codex_root / "skills" / "shengsuan-sync" / "state" / "ledger.json",
                self.codex_root / "skills" / "databuilder-public-docs" / "state" / "ledger.json",
            )
            concepts_ledger = self.codex_root / "skills" / "shengsuan-concepts" / "state" / "concepts-ledger.json"
            missing = [str(path) for path in (*ledgers, concepts_ledger) if not path.is_file()]
            if missing:
                reason = "source_manifest_input_missing"
            else:
                try:
                    raw_concepts = json.loads(concepts_ledger.read_text(encoding="utf-8"))
                    if not isinstance(raw_concepts, dict):
                        raise ValueError("concepts_ledger_not_object")
                    manifest = build_manifest(load_metadata_rows(ledgers), raw_concepts)
                    manifest["producer"] = "weekly-sync-and-refresh"
                    manifest["upstream_run_id"] = run_id
                    manifest["source_ledgers"] = [str(path) for path in (*ledgers, concepts_ledger)]
                    manifest_path = artifact_dir / "source-manifest.json"
                    write_manifest(manifest_path, manifest)
                    manifest_hash = _sha256_path(manifest_path)
                    status = "pending"
                    reason = ""
                except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                    reason = f"source_manifest_failed:{type(exc).__name__}"
        event_key = ":".join(
            (
                "concept-refresh-planner",
                run_id,
                manifest_hash or "unavailable",
                planner_version,
            )
        )
        return self.store.append_scheduled_dependency_event(
            {
                "event_key": event_key,
                "dependent_schedule_key": "concept-refresh-planner",
                "upstream_schedule_key": "weekly-sync-and-refresh",
                "upstream_occurrence_id": str(request.get("occurrence_id") or ""),
                "upstream_run_id": run_id,
                "upstream_completed_at": finished_at,
                "source_manifest_path": str(manifest_path) if manifest_path else "",
                "source_manifest_hash": manifest_hash,
                "handler_evidence_path": str(handler_evidence_path),
                "handler_evidence_hash": _sha256_path(handler_evidence_path),
                "planner_version": planner_version,
                "status": status,
                "reason": reason,
            }
        )

    def _run_scheduled_handler(self, claim: Dict[str, Any], request: Dict[str, Any], artifact_dir: Path) -> str:
        """Execute one fixed scheduled command and persist its evidence."""
        spec, command = resolve_handler(request, self.codex_root)
        run_id = str(request["run_id"])
        envelope = {
            "schema_version": "pm-run-envelope.v1",
            "run_id": run_id,
            "job_id": str(request.get("job_id") or claim.get("job_id") or ""),
            "occurrence_id": request.get("occurrence_id"),
            "schedule_key": spec.schedule_key,
            "trigger_kind": request.get("trigger_kind", "calendar"),
            "scheduled_at": request.get("scheduled_at"),
            "deadline_at": request.get("deadline_at"),
            "registry_hash": request.get("registry_hash"),
            "handler_version": "pm-loop.scheduled-handler.v1",
            "artifact_root": str(artifact_dir),
        }
        envelope_path = artifact_dir / "run-envelope.v1.json"
        candidate_path = artifact_dir / "task-package.candidate.json"
        atomic_json_write(envelope_path, envelope)
        request.update({"artifact_dir": str(artifact_dir), "run_envelope": str(envelope_path), "task_package_candidate": str(candidate_path), "db_path": str(self.store.db_path)})
        context_failure = self._dependency_context_failure(request, envelope_path)
        if context_failure:
            self.store.append_run_event(
                run_id,
                "scheduled/rejected",
                {"schedule_key": spec.schedule_key, "reason": context_failure, "handler_started": False},
                actor="coordination-worker",
            )
            request["_scheduled_failure_reason"] = context_failure
            return "failed"
        request["delivery_policy"] = request.get("delivery_policy") or (request.get("payload") or {}).get("delivery_policy")
        policy_failure = None
        if spec.schedule_key == "weekly-report-reminder":
            policy = str(request.get("delivery_policy") or "").strip()
            if policy not in {"dry_run", "scheduled"}:
                policy_failure = "delivery_policy_invalid"
            if policy == "scheduled" and not bool(request.get("delivery_authorized")):
                policy_failure = "delivery_policy_not_authorized"
            if policy == "scheduled" and policy_failure is None:
                command = [item for item in command if item != "--dry-run"]
        started_at = now_iso()
        timeout = min(spec.timeout_seconds, _remaining_seconds(request.get("deadline_at")) or spec.timeout_seconds)
        if request.get("deadline_at") and _remaining_seconds(request.get("deadline_at")) <= 0:
            timeout = 0
        evidence = request.get("evidence")
        if not isinstance(evidence, dict):
            payload = request.get("payload")
            evidence = payload.get("evidence") if isinstance(payload, dict) and isinstance(payload.get("evidence"), dict) else {}
        self.store.append_run_event(run_id, "scheduled/started", {"schedule_key": spec.schedule_key, "handler": spec.handler, "command": command, "timeout_seconds": timeout}, actor="coordination-worker")
        if policy_failure:
            result = type("Result", (), {"returncode": 78, "stdout": "", "stderr": policy_failure})()
        elif timeout <= 0:
            result = type("Result", (), {"returncode": 124, "stdout": "", "stderr": "scheduled deadline exhausted"})()
        else:
            env = scheduled_environment(request)
            heartbeat_stop = threading.Event()
            lease_lost = threading.Event()

            def beat() -> None:
                while not heartbeat_stop.wait(max(1.0, self.scheduler.slot_ttl_seconds / 3)):
                    if not self.scheduler.heartbeat(str(claim["lease_id"])):
                        lease_lost.set()
                        return

            heartbeat_thread = threading.Thread(target=beat, name=f"pm-scheduled-heartbeat-{run_id[:8]}", daemon=True)
            heartbeat_thread.start()
            try:
                result = self.scheduled_invoker(command, timeout, env=env)
            except TypeError:
                # Small test doubles may only accept (command, timeout).
                result = self.scheduled_invoker(command, timeout)
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join(timeout=2)
            if lease_lost.is_set():
                result = type("Result", (), {"returncode": 124, "stdout": getattr(result, "stdout", ""), "stderr": "scheduled lease lost"})()
        finished_at = now_iso()
        output_path = artifact_dir / "scheduled" / "output.txt"
        _write_text(output_path, (getattr(result, "stdout", "") or "") + ("\n" + getattr(result, "stderr", "") if getattr(result, "stderr", "") else ""))
        rc = int(getattr(result, "returncode", 1))
        status = "completed" if rc == 0 else "failed"
        failure = None if rc == 0 else (policy_failure or ("deadline_exceeded" if rc == 124 else f"handler_exit_{rc}"))
        if failure:
            # Keep the concrete handler reason available to the enclosing
            # scheduler release, which atomically projects it to the
            # occurrence instead of replacing it with a generic wrapper.
            request["_scheduled_failure_reason"] = failure
        evidence_path = artifact_dir / "scheduled" / "handler.json"
        atomic_json_write(evidence_path, {
            "schema_version": "pm-loop.scheduled-handler.v1",
            "run_id": run_id,
            "occurrence_id": request.get("occurrence_id"),
            "schedule_key": spec.schedule_key,
            "handler": spec.handler,
            "command": command,
            "started_at": started_at,
            "finished_at": finished_at,
            "returncode": rc,
            "status": status,
            "failure_reason": failure,
            "evidence": evidence,
            "output_path": str(output_path),
        })
        # The Worker owns the immutable package commit.  Handler stdout is
        # treated as an index only; arbitrary paths are accepted only when
        # they resolve under the project or this Run's artifact root.
        handler_result: Dict[str, Any] = {}
        try:
            parsed = parse_last_json(getattr(result, "stdout", "") or "")
            if isinstance(parsed, dict):
                handler_result = parsed
        except Exception:
            # Some process wrappers expose stdout only through the persisted
            # output file.  Parsing that file keeps the package deterministic
            # without trusting arbitrary child paths or rerunning the handler.
            try:
                parsed = parse_last_json(output_path.read_text(encoding="utf-8"))
                if isinstance(parsed, dict):
                    handler_result = parsed
            except Exception:
                handler_result = {}
        send_result = handler_result.get("send") if isinstance(handler_result, dict) else None
        if isinstance(send_result, dict) and send_result.get("delivery_disposition") == "no_retry_after_effect":
            failure = "no_retry_after_effect"
            request["_scheduled_failure_reason"] = failure
        # Competitive radar publishes through the coordination DB singleton.
        # A degraded/draft report may never replace the last reviewed pointer;
        # a reviewed report must be durably recorded before the Run completes.
        if spec.schedule_key == "competitive-radar-brief" and handler_result.get("latest_updated"):
            try:
                latest_path = Path(str(handler_result.get("latest") or "")).expanduser().resolve()
                latest_value = json.loads(latest_path.read_text(encoding="utf-8"))
                if not isinstance(latest_value, dict):
                    raise ValueError("latest pointer is not an object")
                self.store.upsert_competitive_radar_latest(latest_value)
                self.store.append_run_event(run_id, "competitive_radar/latest_published", {"report_hash": latest_value.get("report_hash"), "report_uri": latest_value.get("report_uri"), "review_run_id": latest_value.get("review_run_id")}, actor="coordination-worker")
            except Exception as exc:
                failure = "latest_pointer_failed"
                request["_scheduled_failure_reason"] = failure
                handler_result["latest_pointer_error"] = f"{type(exc).__name__}: {exc}"
                rc = 1
        status = "completed" if rc == 0 else "failed"
        if failure and rc == 0:
            status = "failed"
        if rc != 0:
            atomic_json_write(evidence_path, {
                "schema_version": "pm-loop.scheduled-handler.v1",
                "run_id": run_id,
                "occurrence_id": request.get("occurrence_id"),
                "schedule_key": spec.schedule_key,
                "handler": spec.handler,
                "command": command,
                "started_at": started_at,
                "finished_at": finished_at,
                "returncode": rc,
                "status": status,
                "failure_reason": failure,
                "evidence": evidence,
                "output_path": str(output_path),
            })
        if spec.schedule_key == "weekly-sync-and-refresh":
            try:
                dependency_event = self._append_weekly_dependency_event(
                    request,
                    run_id=run_id,
                    finished_at=finished_at,
                    handler_status=status,
                    failure_reason=failure,
                    handler_evidence_path=evidence_path,
                    artifact_dir=artifact_dir,
                )
                handler_result["dependency_event"] = dependency_event
                if dependency_event.get("status") == "pending":
                    manifest_path = artifact_dir / "source-manifest.json"
                    if manifest_path.is_file():
                        handler_result["source_manifest"] = str(manifest_path)
                self.store.append_run_event(
                    run_id,
                    "scheduled_dependency_event/appended",
                    {
                        "event_id": dependency_event.get("event_id"),
                        "status": dependency_event.get("status"),
                        "deduplicated": dependency_event.get("deduplicated"),
                    },
                    actor="coordination-worker",
                )
            except Exception as exc:
                # A successful source sync without its durable dependency
                # evidence must not look healthy: otherwise the planner could
                # silently stop forever after a Worker restart.
                failure = "dependency_event_write_failed"
                request["_scheduled_failure_reason"] = failure
                handler_result["dependency_event_error"] = f"{type(exc).__name__}: {exc}"
                rc = 1
                status = "failed"
                atomic_json_write(evidence_path, {
                    "schema_version": "pm-loop.scheduled-handler.v1",
                    "run_id": run_id,
                    "occurrence_id": request.get("occurrence_id"),
                    "schedule_key": spec.schedule_key,
                    "handler": spec.handler,
                    "command": command,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "returncode": rc,
                    "status": status,
                    "failure_reason": failure,
                    "evidence": evidence,
                    "output_path": str(output_path),
                })
        artifacts = []

        def add_artifact(role: str, path: Path) -> None:
            resolved = _safe_regular_file_under(path, self.project_root)
            if resolved is None:
                resolved = _safe_regular_file_under(path, artifact_dir)
            if resolved is not None and not any(item.get("uri") == str(resolved) for item in artifacts):
                artifacts.append({"role": role, "uri": str(resolved), "sha256": _sha256_path(resolved), "visibility": "local_private"})

        add_artifact("run_envelope", envelope_path)
        add_artifact("handler_evidence", evidence_path)
        add_artifact("handler_output", output_path)
        for role, key in (("primary_markdown", "markdown"), ("primary_html", "html"), ("status", "status_file"), ("snapshot", "snapshot"), ("source_manifest", "source_manifest")):
            raw_path = handler_result.get(key)
            if not raw_path:
                continue
            add_artifact(role, Path(str(raw_path)).expanduser())
        evidence_refs = [f"artifact:{item['uri']}#{item['sha256']}" for item in artifacts]
        marker = str(evidence.get("marker") or "").strip()
        if marker:
            evidence_refs.append(f"configured_marker:{marker}")
        package_status = "completed" if rc == 0 or failure == "no_retry_after_effect" else "failed"
        package = {
            "schema_version": "pm-task-package.v1",
            "task": {"schedule_key": spec.schedule_key, "display_name": spec.schedule_key, "task_kind": "reminder" if "reminder" in spec.schedule_key else "report", "data_classification": "internal_private"},
            "execution": {"occurrence_id": request.get("occurrence_id"), "job_id": str(request.get("job_id") or claim.get("job_id") or ""), "run_id": run_id, "schedule_key": spec.schedule_key, "trigger_kind": request.get("trigger_kind", "calendar"), "replay_id": request.get("replay_id"), "replay_of_occurrence_id": request.get("replay_of_occurrence_id"), "display_role": request.get("display_role", "current_calendar"), "scheduled_at": request.get("scheduled_at"), "deadline_at": request.get("deadline_at"), "registry_hash": request.get("registry_hash")},
            "outcome": {"execution_status": package_status, "business_result": "available" if rc == 0 else "partial" if failure == "no_retry_after_effect" else "not_available", "failure_class": "delivery_uncertain" if failure == "no_retry_after_effect" else "none" if rc == 0 else ("worker_interrupted" if rc == 124 else "validation_failed"), "delivery_disposition": ((send_result or {}).get("delivery_disposition") or "preflight_only") if isinstance(send_result, dict) and spec.schedule_key == "weekly-report-reminder" else ("preflight_only" if spec.schedule_key == "weekly-report-reminder" else "none"), "safe_statement": "处理器已返回" if rc == 0 else "回执未确认，禁止自动重发" if failure == "no_retry_after_effect" else "处理器未完成", "impact": failure or ""},
            "stages": [{"name": "handler", "status": "ok" if rc == 0 else "failed", "started_at": started_at, "completed_at": finished_at, "reason": failure or ""}],
            "sources": [],
            "baseline": {"status": "unknown", "kind": "none", "reference_uri": "", "reason": "由任务专用 status 文件提供"},
            "business_summary": handler_result,
            "artifacts": artifacts,
            "evidence_refs": evidence_refs,
            "checks": [{"name": "handler_exit", "status": "pass" if rc == 0 else "fail", "reason": failure or ""}],
            "next_action": {"kind": "inspect" if rc != 0 else "none", "summary": failure or ""},
        }
        required_identity = {"run_id": run_id, "job_id": str(request.get("job_id") or claim.get("job_id") or ""), "occurrence_id": request.get("occurrence_id"), "schedule_key": spec.schedule_key, "registry_hash": request.get("registry_hash")}
        execution_identity = package["execution"]
        if any(execution_identity.get(key) != value for key, value in required_identity.items()):
            raise RuntimeError("task package identity mismatch")
        atomic_json_write(candidate_path, package)
        try:
            artifact_manifest_path = write_worker_artifact_manifest(project_root=self.project_root, package=package)
            package["artifact_manifest"] = {
                "schema_version": "pm-loop.artifact-manifest.v1",
                "path": str(artifact_manifest_path),
                "visibility": "local_private",
            }
        except Exception as exc:
            # A generated report without a durable Registry-facing Manifest is
            # usable evidence, but it is not a complete scheduled delivery.
            failure = "artifact_manifest_write_failed"
            request["_scheduled_failure_reason"] = failure
            status = "failed"
            package["outcome"].update({
                "execution_status": "partial",
                "business_result": "partial",
                "failure_class": failure,
                "safe_statement": "处理器已返回，但产物索引未登记。",
                "impact": f"{type(exc).__name__}: {exc}",
            })
            package["checks"].append({"name": "artifact_manifest", "status": "fail", "reason": failure})
            package["next_action"] = {"kind": "inspect", "summary": failure}
        final_package = artifact_dir / "task-package.v1.json"
        atomic_json_write(final_package, package)
        self.store.upsert_checkpoint(run_id, "scheduled", "handler", input_hash=str(request.get("registry_hash") or ""), artifact_uri=str(final_package), payload={"status": status, "handler": spec.handler, "marker": evidence.get("marker"), "task_package": str(final_package), "task_package_sha256": _sha256_path(final_package), "artifact_manifest": package.get("artifact_manifest")})
        self.store.append_run_event(run_id, f"scheduled/{status}", {"schedule_key": spec.schedule_key, "handler": spec.handler, "returncode": rc, "artifact": str(evidence_path), "failure_reason": failure}, actor="coordination-worker")
        return status

    def _snapshot(self, request: Dict[str, Any], artifact_dir: Path) -> Dict[str, Any]:
        # A model retry must reuse the source checkpoint. Re-collecting the
        # snapshot can observe a different world and turns one bounded retry
        # into duplicate source work.
        existing = self.store.get_checkpoint(str(request["run_id"]), "source", "snapshot")
        if existing:
            artifact_uri = existing.get("artifact_uri")
            if artifact_uri:
                checkpoint_path = Path(str(artifact_uri))
                if checkpoint_path.is_file():
                    return validate_snapshot(checkpoint_path)
        source_dir = artifact_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        configured = request.get("snapshot_path")
        if configured:
            source = Path(str(configured)).expanduser().resolve()
            destination = artifact_dir / "snapshot.json"
            shutil.copy2(source, destination)
            return validate_snapshot(destination)

        command = [sys.executable, str(self.adapter_script), "snapshot", "--out", str(source_dir)]
        command.extend(["--project-root", str(request.get("project_root") or self.project_root)])
        command.extend(["--codex-root", str(request.get("codex_root") or self.codex_root)])
        # Snapshot adapters may spawn Ku/OpenViking helpers.  Run them in an
        # isolated process group so a timeout or worker stop cannot leave an
        # untracked child behind.
        result = run_process_group(command, timeout=900, stdin=subprocess.DEVNULL, capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(_safe_text(result.stderr or result.stdout or "snapshot adapter failed"))
        response = parse_last_json(result.stdout)
        source_path = Path(str(response.get("snapshot_path") or ""))
        if not source_path.is_file():
            raise FileNotFoundError(f"snapshot adapter output missing: {source_path}")
        destination = artifact_dir / "snapshot.json"
        shutil.copy2(source_path, destination)
        return validate_snapshot(destination)

    def _draft(self, request: Dict[str, Any], snapshot: Dict[str, Any], artifact_dir: Path) -> Path:
        summary = snapshot.get("summary") or {}
        lines = [
            f"# {request.get('loop_id', 'PM Loop')} 运行草稿",
            "",
            f"- run_id：`{request.get('run_id')}`",
            f"- snapshot_id：`{snapshot.get('snapshot_id')}`",
            f"- 采集时间：`{snapshot.get('collected_at')}`",
            "",
            "## 本次快照",
            "",
            f"- LaunchAgent：{summary.get('launchd_jobs', 0)} 个",
            f"- Skill：{summary.get('skills', 0)} 个",
            f"- OpenViking：`{summary.get('openviking_status', 'unknown')}`",
            f"- 时间轴事件：{summary.get('timeline_events', 0)} 条",
            "",
            "这是由 V4.4 coordination worker 生成的只读草稿。",
            "",
        ]
        path = artifact_dir / "draft" / "report.md"
        _write_text(path, "\n".join(lines))
        return path

    def _model_timeout(self, run_id: str, request: Dict[str, Any], call: Dict[str, Any]) -> int:
        """Bound one provider attempt by every persisted wall-clock deadline."""
        candidates = [
            900,
            _positive_timeout((request.get("budget") or {}).get("max_seconds")),
        ]
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT r.deadline_at,j.deadline_at FROM runs AS r JOIN jobs AS j ON j.job_id=r.job_id WHERE r.run_id=?",
                (run_id,),
            ).fetchone()
        for deadline_at in (
            row[0] if row is not None else None,
            row[1] if row is not None else None,
            call.get("retry_deadline_at"),
        ):
            if deadline_at:
                candidates.append(_remaining_seconds(deadline_at))
        return min(candidates)

    def _run_analysis(self, request: Dict[str, Any], snapshot: Dict[str, Any], artifact_dir: Path, lease_id: str) -> str:
        run_id = str(request["run_id"])
        current = self.store.get_run(run_id)
        if current is None or current.get("status") == "cancelled":
            return "cancelled"
        self.store.upsert_checkpoint(
            run_id,
            "source",
            "snapshot",
            input_hash=str(snapshot.get("snapshot_id") or ""),
            artifact_uri=str(artifact_dir / "snapshot.json"),
            payload={"status": "completed", "snapshot_id": snapshot.get("snapshot_id")},
        )
        self.store.append_run_event(run_id, "source/completed", {"snapshot_id": snapshot.get("snapshot_id")}, actor="coordination-worker")
        current = self.store.get_run(run_id)
        if current is None or current.get("status") == "cancelled":
            return "cancelled"
        if request.get("analysis_mode") == "snapshot-only":
            draft = self._draft(request, snapshot, artifact_dir)
            self.store.upsert_checkpoint(run_id, "draft", "report", artifact_uri=str(draft), payload={"status": "completed"})
            self.store.append_run_event(run_id, "assistant/draft", {"path": str(draft), "mode": "snapshot-only"}, actor="coordination-worker")
            return "completed"

        prompt = build_prompt(request, snapshot)
        input_value = {
            "run_id": run_id,
            "loop_id": request.get("loop_id"),
            "scope": request.get("scope") or {},
            "snapshot_id": snapshot.get("snapshot_id"),
            "evidence_refs": sorted((snapshot.get("sources") or {}).keys()),
        }
        input_hash = canonical_hash(input_value)
        analysis_dir = artifact_dir / "analysis"
        analysis_dir.mkdir(parents=True, exist_ok=True)
        atomic_json_write(analysis_dir / "input.json", input_value)
        manifest = {
            "schema_version": "pm-loop.analysis-manifest.v1",
            "run_id": run_id,
            "loop_id": request.get("loop_id"),
            "prompt_version": PROMPT_VERSION,
            "snapshot_id": snapshot.get("snapshot_id"),
            "permission_mode": request.get("permission_mode"),
            "executor_mode": "coordination-worker",
            "input_hash": input_hash,
            "started_at": now_iso(),
        }
        atomic_json_write(analysis_dir / "manifest.json", manifest)
        try:
            call = self.scheduler.begin_model_call(
                run_id,
                stage="analysis",
                model_input_hash=input_hash,
                prompt_version=PROMPT_VERSION,
                provider=str(request.get("provider") or "oneapi"),
                endpoint=str(request.get("provider_endpoint") or "default"),
                model=str(request.get("model") or "default"),
            )
        except ProviderThrottled as exc:
            self.store.append_run_event(
                run_id,
                "model_call/throttled",
                {
                    "provider_key": exc.provider_key,
                    "throttle_until": exc.throttle_until,
                    "retry_after_seconds": exc.retry_after_seconds,
                },
                actor="coordination-worker",
            )
            self.scheduler.release(
                lease_id,
                status="retry_wait",
                error="provider_throttled",
                retry_after_seconds=exc.retry_after_seconds,
                increment_attempt=False,
            )
            return "retry_wait"
        self.store.append_run_event(run_id, "model_call/started", {"call_id": call["call_id"], "attempt": call["attempt"], "model_input_hash": input_hash}, actor="coordination-worker")
        heartbeat_stop = threading.Event()

        def beat() -> None:
            while not heartbeat_stop.wait(max(1.0, self.scheduler.slot_ttl_seconds / 3)):
                self.scheduler.heartbeat(lease_id)

        heartbeat_thread = threading.Thread(target=beat, name=f"pm-v44-heartbeat-{run_id[:8]}", daemon=True)
        heartbeat_thread.start()
        raw_path = analysis_dir / f"response-{call['attempt']}.txt"
        try:
            timeout = self._model_timeout(run_id, request, call)
            if timeout <= 0:
                fingerprint = _error_fingerprint("deadline_exceeded", "model invocation deadline exhausted")
                _write_text(raw_path, "model invocation skipped: deadline exhausted\n")
                self.scheduler.finish_model_call(call["call_id"], status="failed", artifact_uri=str(raw_path), error_fingerprint=fingerprint)
                self.store.append_run_event(
                    run_id,
                    "model_call/deadline_exceeded",
                    {"call_id": call["call_id"], "attempt": call["attempt"], "retry_deadline_at": call.get("retry_deadline_at")},
                    actor="coordination-worker",
                )
                self.scheduler.release(lease_id, status="failed", error=fingerprint)
                return "failed"
            try:
                returncode, output, stderr, cli_path = self.invoker(prompt, timeout, self.codex_root)
            except Exception as exc:
                # An adapter/network exception is indistinguishable from a
                # lost provider response at this boundary.  Persist it as a
                # non-zero attempt so the bounded model-stage retry path can
                # reuse the source checkpoint instead of failing the whole Run.
                returncode, output, stderr, cli_path = 1, "", f"{type(exc).__name__}: {exc}", "invoker-exception"
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=2)
        attempt = {
            "schema_version": "pm-loop.analysis-attempt.v1",
            "run_id": run_id,
            "returncode": int(returncode),
            "cli_path": cli_path,
            "prompt_version": PROMPT_VERSION,
            "completed_at": now_iso(),
            "stderr_tail": _safe_text(stderr, 2000),
        }
        atomic_json_write(analysis_dir / f"attempt-{call['attempt']}.json", attempt)
        manifest.update({"cli_path": cli_path, "returncode": int(returncode), "completed_at": now_iso(), "attempt": call["attempt"]})
        atomic_json_write(analysis_dir / "manifest.json", manifest)
        _write_text(raw_path, output or stderr or "")

        if self.store.get_run(run_id) and self.store.get_run(run_id).get("status") == "cancelled":
            self.scheduler.finish_model_call(call["call_id"], status="cancelled", artifact_uri=str(raw_path))
            return "cancelled"
        if returncode != 0:
            failure_text = _safe_text(stderr or output or f"exit={returncode}")
            category, retry_after = _model_failure(failure_text)
            fingerprint = _error_fingerprint(category, failure_text)
            remaining = _remaining_seconds(call.get("retry_deadline_at"))
            if category == "429":
                rate_limit = self.resource_dispatcher.gateway.record_429(
                    str(call["provider_bucket_key"]),
                    retry_after=retry_after,
                )
                retry_seconds = min(int(rate_limit["retry_after_seconds"]), remaining) if remaining > 0 else 0
                terminal = remaining <= 0
                self.scheduler.finish_model_call(call["call_id"], status="failed" if terminal else "retry_wait", artifact_uri=str(raw_path), error_fingerprint=fingerprint)
                self.store.append_run_event(
                    run_id,
                    "model_call/rate_limited",
                    {"call_id": call["call_id"], "attempt": call["attempt"], "retry_after_seconds": rate_limit["retry_after_seconds"], "retry_deadline_at": call.get("retry_deadline_at"), "terminal": terminal},
                    actor="coordination-worker",
                )
                if not terminal:
                    self.scheduler.release(
                        lease_id,
                        status="retry_wait",
                        error=fingerprint,
                        retry_after_seconds=retry_seconds,
                        increment_attempt=False,
                    )
                    return "retry_wait"
                self.scheduler.release(lease_id, status="failed", error=fingerprint)
                return "failed"
            unknown_count = int(call.get("prior_result_unknown_count") or 0) + 1
            terminal = unknown_count >= self.max_model_attempts or remaining <= 0
            self.scheduler.finish_model_call(
                call["call_id"],
                status="failed" if terminal else "result_unknown",
                artifact_uri=str(raw_path),
                error_fingerprint=fingerprint,
            )
            if not terminal:
                self.store.append_run_event(run_id, "model_call/retry_wait", {"call_id": call["call_id"], "attempt": call["attempt"], "error_fingerprint": fingerprint, "retry_deadline_at": call.get("retry_deadline_at")}, actor="coordination-worker")
                self.scheduler.release(lease_id, status="retry_wait", error=fingerprint, retry_after_seconds=min(2, remaining))
                return "retry_wait"
            self.store.append_run_event(run_id, "model_call/failed", {"call_id": call["call_id"], "attempt": call["attempt"], "error_fingerprint": fingerprint}, actor="coordination-worker")
            self.scheduler.release(lease_id, status="failed", error=fingerprint)
            return "failed"

        try:
            analysis = normalize_analysis(parse_json_object(output), request, snapshot)
            decision = build_decision(analysis, request)
        except Exception as exc:
            fingerprint = _error_fingerprint("invalid_response", _safe_text(str(exc)))
            self.scheduler.finish_model_call(call["call_id"], status="failed", artifact_uri=str(raw_path), error_fingerprint=fingerprint)
            self.scheduler.release(lease_id, status="failed", error=fingerprint)
            return "failed"
        atomic_json_write(analysis_dir / "analysis.json", analysis)
        atomic_json_write(artifact_dir / "decision" / "decision.json", decision)
        report = artifact_dir / "draft" / "report.md"
        conclusion = analysis.get("conclusion") or {}
        _write_text(report, f"# {request.get('loop_id', 'PM Loop')} 分析报告\n\n{conclusion.get('headline') or '—'}\n")
        self.scheduler.finish_model_call(call["call_id"], status="completed", artifact_uri=str(analysis_dir / "analysis.json"))
        self.store.append_run_event(run_id, "analysis/completed", {"analysis_path": str(analysis_dir / "analysis.json"), "decision_path": str(artifact_dir / "decision" / "decision.json")}, actor="coordination-worker")
        self.store.upsert_checkpoint(run_id, "analysis", "result", input_hash=input_hash, artifact_uri=str(analysis_dir / "analysis.json"), payload={"status": "completed", "call_id": call["call_id"]})
        return "completed"

    def process_claim(self, claim: Dict[str, Any]) -> str:
        run_id = str(claim["run_id"])
        request = self._request(claim)
        artifact_dir = self._artifact_dir(run_id)
        atomic_json_write(artifact_dir / "request.json", request)
        current = self.store.get_run(run_id)
        if current is None:
            self.scheduler.release(str(claim["lease_id"]), status="failed", error="run_not_found")
            return "failed"
        if current.get("status") == "cancelled":
            self.scheduler.release(str(claim["lease_id"]), status="cancelled")
            return "cancelled"
        self.store.append_run_event(run_id, "run/started", {"worker": "coordination-worker", "artifact_root": str(artifact_dir)}, actor="coordination-worker")
        try:
            if request.get("schedule_key"):
                status = self._run_scheduled_handler(claim, request, artifact_dir)
                failure_reason = request.pop("_scheduled_failure_reason", None)
                released = self.scheduler.release(
                    str(claim["lease_id"]),
                    status=status,
                    error=None if status == "completed" else (failure_reason or "scheduled_handler_failed"),
                )
                if not released:
                    self.store.append_run_event(run_id, "run/terminal_state_uncertain", {"reason": "lease_release_rejected", "handler_status": status}, actor="coordination-worker")
                    return "interrupted"
                return status
            self.store.append_run_event(run_id, "source/started", {"worker": "coordination-worker"}, actor="coordination-worker")
            # Keep the lease alive while the source adapter is running too;
            # otherwise a slow Ku/OpenViking read can be reconciled as stale
            # while this worker is still legitimately active.
            source_heartbeat_stop = threading.Event()

            def source_beat() -> None:
                while not source_heartbeat_stop.wait(max(1.0, self.scheduler.slot_ttl_seconds / 3)):
                    if not self.scheduler.heartbeat(str(claim["lease_id"])):
                        source_heartbeat_stop.set()
                        return

            source_heartbeat = threading.Thread(target=source_beat, name=f"pm-v44-source-heartbeat-{run_id[:8]}", daemon=True)
            source_heartbeat.start()
            try:
                snapshot = self._snapshot(request, artifact_dir)
            finally:
                source_heartbeat_stop.set()
                source_heartbeat.join(timeout=2)
            current = self.store.get_run(run_id)
            if current is None or current.get("status") == "cancelled":
                return "cancelled"
            status = self._run_analysis(request, snapshot, artifact_dir, str(claim["lease_id"]))
            if status in {"retry_wait", "cancelled", "failed"}:
                if status == "cancelled":
                    self.scheduler.release(str(claim["lease_id"]), status="cancelled")
                elif status == "failed":
                    # _run_analysis owns the terminal release for model errors.
                    pass
                return status
            self.store.append_run_event(run_id, "verification/completed", {"ok": True, "worker": "coordination-worker"}, actor="coordination-worker")
            self.scheduler.release(str(claim["lease_id"]), status="completed")
            return "completed"
        except Exception as exc:
            detail = _safe_text(f"{type(exc).__name__}: {exc}")
            fingerprint = _error_fingerprint("worker", detail)
            current = self.store.get_run(run_id)
            if current and current.get("status") == "cancelled":
                # Cancellation wins over a late adapter/model exception. Keep
                # the run terminally cancelled and avoid a post-cancel failed
                # event that would make the projection regress.
                self.scheduler.release(str(claim["lease_id"]), status="cancelled")
                return "cancelled"
            self.store.append_run_event(run_id, "run/failed", {"error": detail, "error_fingerprint": fingerprint}, actor="coordination-worker")
            self.scheduler.release(str(claim["lease_id"]), status="failed", error=fingerprint)
            return "failed"

    def _refresh_attention(self) -> None:
        """Keep alert projection and notification outside the Job transaction."""
        try:
            refresh_ops_attention(self.store)
        except Exception:
            pass

    def run_once(self) -> Optional[str]:
        memory_results = self.memory_writer.dispatch_pending(limit=4)
        memory_results.extend(self.memory_writer.reconcile_tasks(limit=8))
        resource_results = self.resource_dispatcher.dispatch_pending(limit=4)
        claim = self.scheduler.claim_next(worker_id=f"worker-{os.getpid()}", pid=os.getpid(), process_group_id=os.getpgrp())
        if claim is None:
            self._refresh_attention()
            if memory_results:
                return "memory-dispatched"
            return "resource-dispatched" if resource_results else None
        try:
            return self.process_claim(claim)
        finally:
            self._refresh_attention()

    def serve(self, *, max_jobs: Optional[int] = None) -> Dict[str, Any]:
        self.scheduler.startup_reconcile(active_lease_ids=[])
        self._refresh_attention()
        completed = 0
        futures: Dict[Future[str], Dict[str, Any]] = {}
        resource_thread = threading.Thread(target=self._serve_resources, name="pm-v44-resource-dispatcher", daemon=True)
        resource_thread.start()
        try:
            with ThreadPoolExecutor(max_workers=max(1, self.scheduler.max_slots), thread_name_prefix="pm-v44-worker") as pool:
                while not self.stop_event.is_set() and (max_jobs is None or completed < max_jobs):
                    while not self.stop_event.is_set() and len(futures) < max(1, self.scheduler.max_slots):
                        claim = self.scheduler.claim_next(worker_id=f"worker-{os.getpid()}", pid=os.getpid(), process_group_id=os.getpgrp())
                        if claim is None:
                            break
                        futures[pool.submit(self.process_claim, claim)] = claim
                    done = [future for future in futures if future.done()]
                    for future in done:
                        future.result()
                        futures.pop(future, None)
                        completed += 1
                        self._refresh_attention()
                    if not done:
                        time.sleep(self.poll_interval)
                self.stop_event.set()
                for future in list(futures):
                    future.result()
                    completed += 1
        finally:
            self.stop_event.set()
            resource_thread.join(timeout=max(2.0, self.poll_interval * 4))
            self._refresh_attention()
        return {"status": "stopped", "processed": completed, "schema_version": "pm-system.worker.v1"}

    def _serve_resources(self) -> None:
        """Drain Resource and Memory Outboxes without entering model slots."""
        while not self.stop_event.is_set():
            try:
                self.memory_writer.dispatch_pending(limit=4)
                self.memory_writer.reconcile_tasks(limit=8)
                self.resource_dispatcher.dispatch_pending(limit=4)
                self.resource_dispatcher.reconcile_content(limit=8)
                self.resource_dispatcher.reconcile_tasks(limit=8)
            except Exception as exc:  # pragma: no cover - defensive worker boundary
                print(f"pm-resource-dispatcher error: {type(exc).__name__}: {exc}", file=sys.stderr)
            self.stop_event.wait(max(0.1, self.poll_interval))

    def stop(self, *_args: Any) -> None:
        self.stop_event.set()


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT, help="runtime mirror root used for Worker code")
    parser.add_argument("--canonical-project-root", type=Path, default=PROJECT_ROOT, help="canonical project root for business artifacts and manifests")
    parser.add_argument("--codex-root", type=Path, default=CODEX_ROOT)
    parser.add_argument("--adapter", type=Path)
    parser.add_argument("--max-slots", type=int)
    parser.add_argument("--max-model-attempts", type=int, default=2)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-jobs", type=int)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    worker = PMSystemWorker(
        args.db_path,
        artifact_root=args.artifact_root,
        project_root=args.canonical_project_root,
        runtime_root=args.project_root,
        codex_root=args.codex_root,
        adapter_script=args.adapter,
        max_slots=args.max_slots,
        max_model_attempts=args.max_model_attempts,
        poll_interval=args.poll_interval,
    )
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    if args.once:
        result = worker.run_once()
        print(json.dumps({"status": result or "idle", "schema_version": "pm-system.worker.v1"}, ensure_ascii=False))
        return 0
    print(json.dumps(worker.serve(max_jobs=args.max_jobs), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
