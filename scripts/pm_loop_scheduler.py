#!/usr/bin/env python3
"""Calendar dispatcher for the PM Loop unified scheduler.

This process is intentionally short-lived: a launchd tick loads the canonical
registry, records one scheduler tick, accepts due occurrences, and exits.  It
never runs a task or invokes OpenViking/OneAPI.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional
from zoneinfo import ZoneInfo

from pm_schedule_registry import DEFAULT_REGISTRY_PATH, RegistryError, ScheduleRegistry, ScheduleTask, is_business_window_open, latest_scheduled_at, load_registry, next_scheduled_at, occurrence_key
from pm_ops_attention import refresh_ops_attention
from pm_system_store import PMSystemStore, StoreUnavailable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
DEFAULT_DB_PATH = CODEX_ROOT / "pm-loop" / "state" / "pm-system.db"
DEFAULT_RUNTIME_REGISTRY = CODEX_ROOT / "pm-loop" / "runtime" / "config" / "schedule-registry.json"
DEFAULT_CANONICAL_REGISTRY = Path(
    os.environ.get(
        "PM_LOOP_CANONICAL_REGISTRY",
        str(PROJECT_ROOT / "scripts" / "schedule-registry.json"),
    )
)
DEFAULT_LOCK_PATH = CODEX_ROOT / "pm-loop" / "locks" / "dispatcher.lock"


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _occurrence_id(key: str, registry_hash: str) -> str:
    return "occ-" + hashlib.sha256(f"{registry_hash}:{key}".encode("utf-8")).hexdigest()[:32]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _runtime_document(path: Path) -> Optional[Mapping[str, Any]]:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read runtime registry {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise RegistryError("runtime registry must be an object")
    return value


@contextmanager
def _single_instance(path: Path) -> Iterator[None]:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("duplicate_scheduler") from exc
    try:
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class PMLoopDispatcher:
    def __init__(self, db_path: Path, *, registry_path: Path = DEFAULT_REGISTRY_PATH, runtime_registry_path: Optional[Path] = DEFAULT_RUNTIME_REGISTRY, canonical_registry_path: Optional[Path] = None, lock_path: Path = DEFAULT_LOCK_PATH, scheduler_id: Optional[str] = None) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.registry_path = Path(registry_path).expanduser().resolve()
        self.runtime_registry_path = Path(runtime_registry_path).expanduser().resolve() if runtime_registry_path else None
        self.canonical_registry_path = Path(canonical_registry_path).expanduser().resolve() if canonical_registry_path else None
        self.lock_path = Path(lock_path).expanduser().resolve()
        self.scheduler_id = scheduler_id or f"scheduler-{os.getpid()}"

    def load_verified_registry(self) -> tuple[ScheduleRegistry, str]:
        """Load the runtime input only after checking its canonical source.

        ``registry_path`` is the runtime mirror used to make dispatch
        decisions.  A live Scheduler must also compare it with the project
        canonical file on every tick.  Keeping the paths distinct prevents a
        copied runtime file from silently becoming its own source of truth.
        ``runtime_registry_path`` remains a compatibility verifier for older
        callers that pass the canonical file as ``registry_path``.
        """
        registry = load_registry(self.registry_path)
        canonical_path = self.canonical_registry_path or self.registry_path
        canonical = load_registry(canonical_path)
        if registry.registry_hash != canonical.registry_hash:
            raise RegistryError(
                "canonical/runtime registry hash mismatch: "
                f"canonical={canonical.source_path} runtime={registry.source_path}"
            )
        if self.runtime_registry_path and self.runtime_registry_path != self.registry_path:
            runtime = load_registry(self.runtime_registry_path)
            if runtime.registry_hash != canonical.registry_hash:
                raise RegistryError(
                    "canonical/runtime registry hash mismatch: "
                    f"canonical={canonical.source_path} runtime={runtime.source_path}"
                )
        canonical_document = json.loads(canonical.source_path.read_text(encoding="utf-8"))
        return registry, json.dumps(canonical_document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    def _record_ops_alert(
        self,
        alert_type: str,
        message: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
        store: Optional[PMSystemStore] = None,
    ) -> bool:
        """Best-effort P0 evidence for failures before a scheduler tick exists.

        Registry validation and the process-wide instance lock happen before a
        tick row can be created.  Write directly to the existing attention
        ledger when the database is usable.  If SQLite itself is unavailable,
        no in-database record is possible; the original failure still exits
        non-zero for launchd/health evidence instead of masking it.
        """
        fingerprint = "sha256:" + hashlib.sha256(
            f"scheduler|{alert_type}|{self.registry_path}".encode("utf-8")
        ).hexdigest()[:32]
        try:
            target = store or PMSystemStore(self.db_path, auto_migrate=False)
            target.upsert_ops_alert(
                fingerprint=fingerprint,
                severity="P0",
                alert_type=alert_type,
                module="Scheduler",
                message=message,
                details={
                    "scheduler_id": self.scheduler_id,
                    "registry_path": str(self.registry_path),
                    "runtime_registry_path": str(self.runtime_registry_path) if self.runtime_registry_path else None,
                    "canonical_registry_path": str(self.canonical_registry_path) if self.canonical_registry_path else str(self.registry_path),
                    **dict(details or {}),
                },
            )
            return True
        except (OSError, sqlite3.Error, StoreUnavailable):
            return False

    @staticmethod
    def _refresh_attention(store: PMSystemStore) -> None:
        """Alert delivery is best effort and never changes tick outcome."""
        try:
            refresh_ops_attention(store)
        except Exception:
            pass

    @staticmethod
    def _request(
        task: ScheduleTask,
        registry: ScheduleRegistry,
        scheduled_at: datetime,
        *,
        now: datetime,
        trigger_kind: str = "calendar",
        occurrence_key_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = occurrence_key_override or occurrence_key(task, scheduled_at)
        local = scheduled_at.astimezone(ZoneInfo(registry.timezone_name))
        deadline = scheduled_at + task.deadline
        return {
            "schedule_key": task.schedule_key,
            "occurrence_id": _occurrence_id(key, registry.registry_hash),
            "occurrence_key": key,
            "scheduled_at": _iso(scheduled_at),
            "local_scheduled_at": local.isoformat(timespec="seconds"),
            "deadline_at": _iso(deadline),
            "registry_hash": registry.registry_hash,
            "lock_key": str(task.lock.get("key") or task.schedule_key),
            "job_type": task.job_type,
            "loop_id": task.schedule_key,
            "profile": task.profile,
            "priority": task.priority,
            "concurrency_key": task.concurrency_key,
            "retry": dict(task.retry),
            "delivery_policy": task.delivery_policy,
            "trigger_kind": trigger_kind,
            "owner": "pm-scheduler",
            "payload": {
                "schedule_key": task.schedule_key,
                "handler": task.handler,
                "scheduled_at": _iso(scheduled_at),
                "deadline_at": _iso(deadline),
                "registry_hash": registry.registry_hash,
                "concurrency_key": task.concurrency_key,
                "retry": dict(task.retry),
                "delivery_policy": task.delivery_policy,
                "evidence": task.evidence,
                "handler_version": "pm-loop.scheduled-handler.v1",
            },
        }

    @staticmethod
    def _dependency_occurrence_key(task: ScheduleTask, event: Mapping[str, Any]) -> str:
        """Return the stable occurrence identity for one upstream artifact."""
        material = "\0".join(
            (
                task.schedule_key,
                str(event.get("upstream_run_id") or ""),
                str(event.get("source_manifest_hash") or ""),
                str(event.get("planner_version") or ""),
            )
        )
        return f"{task.schedule_key}:dependency:{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"

    def _dependency_request(
        self,
        task: ScheduleTask,
        registry: ScheduleRegistry,
        event: Mapping[str, Any],
        *,
        now: datetime,
    ) -> Dict[str, Any]:
        completed_at = _parse_utc(event.get("upstream_completed_at"))
        if completed_at is None:
            raise ValueError("dependency_event_invalid_completed_at")
        key = self._dependency_occurrence_key(task, event)
        request = self._request(
            task,
            registry,
            completed_at,
            now=now,
            trigger_kind="dependency",
            occurrence_key_override=key,
        )
        dependency = {
            "event_id": str(event.get("event_id") or ""),
            "event_key": str(event.get("event_key") or ""),
            "upstream_schedule_key": str(event.get("upstream_schedule_key") or ""),
            "upstream_occurrence_id": str(event.get("upstream_occurrence_id") or ""),
            "upstream_run_id": str(event.get("upstream_run_id") or ""),
            "upstream_completed_at": str(event.get("upstream_completed_at") or ""),
            "source_manifest_path": str(event.get("source_manifest_path") or ""),
            "source_manifest_hash": str(event.get("source_manifest_hash") or ""),
            "handler_evidence_path": str(event.get("handler_evidence_path") or ""),
            "handler_evidence_hash": str(event.get("handler_evidence_hash") or ""),
            "planner_version": str(event.get("planner_version") or ""),
        }
        request["idempotency_key"] = "dependency:" + ":".join(
            (
                task.schedule_key,
                dependency["upstream_run_id"],
                dependency["source_manifest_hash"],
                dependency["planner_version"],
            )
        )
        request["payload"]["dependency"] = dependency
        return request

    @staticmethod
    def _dependency_validation(
        store: PMSystemStore,
        registry: ScheduleRegistry,
        event: Mapping[str, Any],
    ) -> tuple[str, str]:
        """Classify a pending event without inferring a missing upstream run.

        ``waiting`` is deliberately non-terminal: a Worker appends the event
        after its handler evidence is durable, while its enclosing Run may
        still be changing from ``running`` to ``completed``.  All malformed
        or terminally failed upstream evidence is blocked instead.
        """
        dependent = str(event.get("dependent_schedule_key") or "")
        upstream = str(event.get("upstream_schedule_key") or "")
        try:
            task = registry.task(dependent)
        except (KeyError, RegistryError):
            return "blocked", "dependent_task_not_registered"
        trigger = task.trigger
        if task.trigger_kind != "dependency":
            return "blocked", "dependent_task_not_dependency"
        if str(trigger.get("upstream_schedule_key") or "") != upstream:
            return "blocked", "upstream_schedule_key_mismatch"
        if str(trigger.get("terminal_status") or "") != "completed":
            return "blocked", "dependency_terminal_policy_invalid"
        if str(trigger.get("planner_version") or "") != str(event.get("planner_version") or ""):
            return "blocked", "planner_version_mismatch"
        run = store.get_run(str(event.get("upstream_run_id") or ""))
        if run is None:
            return "blocked", "upstream_run_missing"
        status = str(run.get("status") or "")
        if status in {"queued", "running", "retry_wait"}:
            return "waiting", f"upstream_run_{status}"
        if status != "completed":
            return "blocked", f"upstream_run_not_completed:{status or 'unknown'}"
        if str(run.get("schedule_key") or "") != upstream:
            return "blocked", "upstream_run_schedule_key_mismatch"
        if str(run.get("occurrence_id") or "") != str(event.get("upstream_occurrence_id") or ""):
            return "blocked", "upstream_occurrence_id_mismatch"
        for field in ("source_manifest_path", "source_manifest_hash", "handler_evidence_path", "handler_evidence_hash"):
            if not str(event.get(field) or "").strip():
                return "blocked", f"missing_{field}"
        try:
            manifest = Path(str(event["source_manifest_path"])).expanduser().resolve()
            evidence = Path(str(event["handler_evidence_path"])).expanduser().resolve()
            if not manifest.is_file() or not evidence.is_file():
                return "blocked", "dependency_artifact_missing"
            if _sha256_file(manifest) != str(event["source_manifest_hash"]):
                return "blocked", "source_manifest_hash_mismatch"
            if _sha256_file(evidence) != str(event["handler_evidence_hash"]):
                return "blocked", "handler_evidence_hash_mismatch"
            document = json.loads(manifest.read_text(encoding="utf-8"))
            if not isinstance(document, Mapping) or str(document.get("schema_version") or "") != "concept-source-manifest.v1":
                return "blocked", "source_manifest_invalid"
        except (OSError, ValueError, json.JSONDecodeError):
            return "blocked", "dependency_artifact_unreadable"
        return "ready", "ok"

    def _consume_dependency_events(
        self,
        store: PMSystemStore,
        registry: ScheduleRegistry,
        *,
        now: datetime,
    ) -> tuple[Dict[str, int], list[Dict[str, Any]]]:
        counts = {"accepted": 0, "deduplicated": 0, "waiting": 0, "blocked": 0, "expired": 0}
        outcomes: list[Dict[str, Any]] = []
        for event in store.list_pending_scheduled_dependency_events(limit=100):
            state, reason = self._dependency_validation(store, registry, event)
            event_id = str(event["event_id"])
            if state == "waiting":
                counts["waiting"] += 1
                outcomes.append({"event_id": event_id, "dependency_state": state, "reason": reason})
                continue
            if state == "blocked":
                changed = store.mark_scheduled_dependency_event_blocked(
                    event_id,
                    reason=reason,
                    outcome={"dependency_state": state, "reason": reason},
                )
                if changed:
                    counts["blocked"] += 1
                outcomes.append({"event_id": event_id, "dependency_state": state, "reason": reason, "changed": changed})
                continue
            task = registry.task(str(event["dependent_schedule_key"]))
            request = self._dependency_request(task, registry, event, now=now)
            scheduled_at = _parse_utc(request["scheduled_at"])
            if scheduled_at is None:
                raise ValueError("dependency_request_invalid_scheduled_at")
            if now > scheduled_at + task.deadline:
                accepted = store.record_schedule_occurrence(request, state="expired", reason="deadline_exceeded")
                outcome = {"dependency_state": "expired", "reason": "deadline_exceeded", **accepted}
                if store.mark_scheduled_dependency_event_consumed(event_id, occurrence_id=str(accepted["occurrence_id"]), outcome=outcome):
                    counts["expired"] += 1
                outcomes.append({"event_id": event_id, **outcome})
                continue
            accepted = store.accept_scheduled_occurrence(request)
            outcome = {"dependency_state": "accepted", **accepted}
            consumed = store.mark_scheduled_dependency_event_consumed(
                event_id,
                occurrence_id=str(accepted["occurrence_id"]),
                outcome=outcome,
            )
            if accepted.get("deduplicated"):
                counts["deduplicated"] += 1
            elif consumed:
                counts["accepted"] += 1
            outcomes.append({"event_id": event_id, **outcome, "consumed": consumed})
        return counts, outcomes

    def plan(self, *, now: Optional[datetime] = None, registry: Optional[ScheduleRegistry] = None) -> list[Dict[str, Any]]:
        registry = registry or self.load_verified_registry()[0]
        current = _utc(now or datetime.now(timezone.utc))
        result = []
        for task in registry.tasks:
            if not task.is_calendar:
                continue
            scheduled_at = latest_scheduled_at(task, current, timezone_name=registry.timezone_name)
            request = self._request(task, registry, scheduled_at, now=current)
            request["decision"] = "expired" if current > scheduled_at + task.deadline else "due"
            result.append(request)
        return result

    def _record_catchup_gap_evidence(
        self,
        store: PMSystemStore,
        registry: ScheduleRegistry,
        planned: Iterable[Mapping[str, Any]],
        *,
        now: datetime,
    ) -> tuple[int, int, list[Dict[str, Any]]]:
        """Record only old windows provable from prior occurrence evidence.

        With ``coalesce_latest/max_backfill=1`` the newest outstanding window
        is the only candidate that may become a Job.  Every known window
        between the prior occurrence watermark and that newest window is
        retained as ``suppressed``; if the newest window is already past its
        own deadline, those older windows are terminal ``expired`` instead.
        A new database has no lower watermark, so this deliberately records
        no invented historical intervals.
        """
        suppressed = 0
        expired = 0
        outcomes: list[Dict[str, Any]] = []
        for latest in planned:
            task = registry.task(str(latest["schedule_key"]))
            latest_at = _parse_utc(latest.get("scheduled_at"))
            last_at = _parse_utc(store.latest_schedule_occurrence_at(task.schedule_key))
            if latest_at is None or last_at is None or last_at >= latest_at:
                continue
            candidate = next_scheduled_at(task, last_at, timezone_name=registry.timezone_name)
            # A corrupted future row must not create an unbounded loop.
            for _ in range(512):
                if candidate >= latest_at:
                    break
                request = self._request(task, registry, candidate, now=now)
                state = "suppressed" if latest.get("decision") == "due" else "expired"
                reason = (
                    f"coalesced_by:{latest['occurrence_key']}"
                    if state == "suppressed"
                    else "deadline_exceeded"
                )
                outcome = store.record_schedule_occurrence(request, state=state, reason=reason)
                outcomes.append({"schedule_key": task.schedule_key, **outcome})
                if not outcome.get("deduplicated"):
                    if state == "suppressed":
                        suppressed += 1
                    else:
                        expired += 1
                candidate = next_scheduled_at(task, candidate, timezone_name=registry.timezone_name)
        return suppressed, expired, outcomes

    def tick(
        self,
        *,
        now: Optional[datetime] = None,
        mode: str = "calendar",
        dry_run: bool = False,
        observations_dir: Optional[Path] = None,
        dependency_only: bool = False,
    ) -> Dict[str, Any]:
        if mode not in {"shadow", "calendar", "catchup", "manual_replay"}:
            raise ValueError("invalid scheduler mode")
        current = _utc(now or datetime.now(timezone.utc))
        try:
            with _single_instance(self.lock_path):
                try:
                    store = PMSystemStore(self.db_path, auto_migrate=False)
                except (StoreUnavailable, sqlite3.Error) as exc:
                    self._record_ops_alert(
                        "database_unavailable",
                        "PM Scheduler 无法打开协调数据库",
                        details={"error": f"{type(exc).__name__}: {exc}"},
                    )
                    raise
                try:
                    registry, canonical_json = self.load_verified_registry()
                except RegistryError as exc:
                    self._record_ops_alert(
                        "registry_invalid",
                        "PM Scheduler registry 校验失败",
                        details={"error": f"{type(exc).__name__}: {exc}"},
                        store=store,
                    )
                    self._refresh_attention(store)
                    raise
                # A dependency-only replay retains the schema-supported
                # ``manual_replay`` audit mode, while exercising the real
                # Scheduler consumer without accepting calendar work.
                planned = [] if dependency_only else self.plan(now=current, registry=registry)
                if dry_run or mode == "shadow":
                    result = {"status": "shadow", "mode": mode, "registry_hash": registry.registry_hash, "planned": planned, "read_only": True}
                    if observations_dir:
                        path = Path(observations_dir).expanduser().resolve()
                        path.mkdir(parents=True, exist_ok=True)
                        (path / f"{current.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                    return result
                try:
                    store.set_schedule_registry_state(
                        registry_version=registry.registry_version,
                        registry_hash=registry.registry_hash,
                        source_path=str(self.canonical_registry_path or self.registry_path),
                        canonical_json=canonical_json,
                        state="valid",
                    )
                    tick_id = store.start_scheduler_tick(scheduler_id=self.scheduler_id, mode=mode, registry_hash=registry.registry_hash, started_at=_iso(current))
                except (StoreUnavailable, sqlite3.Error) as exc:
                    self._record_ops_alert(
                        "database_unavailable",
                        "PM Scheduler 无法写入协调数据库",
                        details={"error": f"{type(exc).__name__}: {exc}"},
                        store=store,
                    )
                    raise
                counts = {"accepted": 0, "deduplicated": 0, "deferred": 0, "suppressed": 0, "expired": 0}
                dependency_counts = {"accepted": 0, "deduplicated": 0, "waiting": 0, "blocked": 0, "expired": 0}
                reconcile: Dict[str, int] = {}
                outcomes = []
                business_window_open = is_business_window_open(current, timezone_name=registry.timezone_name)
                try:
                    if not dependency_only:
                        reconcile = store.reconcile_schedule_occurrences()
                        suppressed, old_expired, catchup_outcomes = self._record_catchup_gap_evidence(
                            store, registry, planned, now=current
                        )
                        counts["suppressed"] += suppressed
                        counts["expired"] += old_expired
                        outcomes.extend(catchup_outcomes)
                    for item in planned:
                        task = registry.task(str(item["schedule_key"]))
                        if item["decision"] == "expired":
                            outcome = store.record_schedule_occurrence(item, state="expired", reason="deadline_exceeded")
                            counts["expired"] += 1
                        elif not business_window_open:
                            outcome = store.record_schedule_occurrence(item, state="deferred", reason="outside_business_window")
                            counts["deferred"] += 1
                        else:
                            outcome = store.accept_scheduled_occurrence(item)
                            if outcome.get("deduplicated"):
                                counts["deduplicated"] += 1
                            elif outcome.get("occurrence_state") == "deferred":
                                counts["deferred"] += 1
                            else:
                                counts["accepted"] += 1
                        outcomes.append({"schedule_key": task.schedule_key, **outcome})
                    if mode == "calendar" or dependency_only:
                        dependency_counts, dependency_outcomes = self._consume_dependency_events(
                            store, registry, now=current
                        )
                        outcomes.extend(dependency_outcomes)
                    store.finish_scheduler_tick(
                        tick_id,
                        accepted=counts["accepted"] + dependency_counts["accepted"],
                        deduplicated=counts["deduplicated"] + dependency_counts["deduplicated"],
                        deferred=counts["deferred"],
                        expired=counts["expired"] + dependency_counts["expired"],
                    )
                except Exception as exc:
                    store.finish_scheduler_tick(tick_id, status="failed", accepted=counts["accepted"], deduplicated=counts["deduplicated"], deferred=counts["deferred"], expired=counts["expired"], error=f"{type(exc).__name__}: {exc}")
                    self._record_ops_alert(
                        "scheduler_tick_failed",
                        "PM Scheduler tick 执行失败",
                        details={"tick_id": tick_id, "error": f"{type(exc).__name__}: {exc}"},
                        store=store,
                    )
                    self._refresh_attention(store)
                    raise
                self._refresh_attention(store)
                return {"status": "completed", "mode": mode, "dependency_only": dependency_only, "tick_id": tick_id, "registry_hash": registry.registry_hash, "business_window_open": business_window_open, **counts, "dependency": dependency_counts, "reconcile": reconcile, "outcomes": outcomes, "read_only": False}
        except RuntimeError as exc:
            if str(exc) == "duplicate_scheduler":
                self._record_ops_alert(
                    "duplicate_scheduler",
                    "PM Scheduler 检测到并发实例",
                    details={"error": str(exc)},
                )
                try:
                    self._refresh_attention(PMSystemStore(self.db_path, auto_migrate=False))
                except (StoreUnavailable, sqlite3.Error):
                    pass
            raise
        except (RegistryError, StoreUnavailable, sqlite3.Error):
            raise


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY, help="runtime registry mirror used for dispatch")
    parser.add_argument("--canonical-registry", type=Path, default=DEFAULT_CANONICAL_REGISTRY, help="project canonical registry that must match the runtime mirror")
    parser.add_argument("--runtime-registry", type=Path, help="deprecated compatibility verifier for callers that use --registry as canonical")
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--mode", choices=["shadow", "calendar", "catchup", "manual_replay"], default="calendar")
    parser.add_argument("--now")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dependency-only", action="store_true", help="consume dependency events without accepting calendar occurrences")
    parser.add_argument("--observations-dir", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else None
    result = PMLoopDispatcher(
        args.db_path,
        registry_path=args.registry,
        runtime_registry_path=args.runtime_registry,
        canonical_registry_path=args.canonical_registry,
        lock_path=args.lock_path,
    ).tick(
        now=now,
        mode=args.mode,
        dry_run=args.dry_run,
        observations_dir=args.observations_dir,
        dependency_only=bool(args.dependency_only),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
