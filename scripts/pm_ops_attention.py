#!/usr/bin/env python3
"""Project durable PM Loop failures into the operator attention ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from pm_system_store import PMSystemStore


def _fingerprint(kind: str, entity_id: str, detail: str = "") -> str:
    raw = f"{kind}|{entity_id}|{detail}".encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()[:32]


def _severity(kind: str, state: str = "") -> str:
    if kind in {"scheduler_tick_failed", "registry_invalid", "duplicate_scheduler", "database_unavailable"}:
        return "P0"
    if kind in {"occurrence_failed", "occurrence_expired", "job_failed", "run_failed", "dead_letter", "heartbeat_stale"}:
        return "P1"
    return "P2"


CANONICAL_ALERT_TYPES = {
    "scheduler_tick_failed",
    "registry_invalid",
    "duplicate_scheduler",
    "database_unavailable",
    "heartbeat_stale",
    "occurrence_failed",
    "occurrence_expired",
    "dead_letter",
    "job_failed",
    "run_failed",
    "health_check",
}


def _parse_time(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _is_stale(value: Any, *, now: datetime, seconds: int) -> bool:
    observed = _parse_time(value)
    return observed is not None and (now - observed).total_seconds() > seconds


def project_ops_attention(
    store: PMSystemStore,
    *,
    limit: int = 200,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Project current canonical faults and resolve only verified recoveries."""
    current = now or datetime.now(timezone.utc)
    current = current.replace(tzinfo=current.tzinfo or timezone.utc).astimezone(timezone.utc)
    bounded = max(1, min(int(limit), 1000))
    created = 0
    refreshed = 0
    alerts = []
    with store.connect() as connection:
        tick_rows = connection.execute("SELECT tick_id,status,error,started_at FROM scheduler_ticks WHERE status='failed' ORDER BY started_at DESC LIMIT ?", (bounded,)).fetchall()
        occurrence_rows = connection.execute("SELECT occurrence_id,schedule_key,state,failure_reason,job_id,run_id,updated_at FROM schedule_occurrences WHERE state IN ('failed','dead_letter','expired') ORDER BY updated_at DESC LIMIT ?", (bounded,)).fetchall()
        job_rows = connection.execute("SELECT job_id,run_id,status,error_fingerprint,terminal_reason,updated_at FROM jobs WHERE status IN ('failed','dead_letter') ORDER BY updated_at DESC LIMIT ?", (bounded,)).fetchall()
        run_rows = connection.execute("SELECT run_id,job_id,status,error,terminal_reason,updated_at FROM runs WHERE status IN ('failed','dead_letter','interrupted') ORDER BY updated_at DESC LIMIT ?", (bounded,)).fetchall()
        error_rows = connection.execute("SELECT error_event_id,fingerprint,severity,module,run_id,message,details_json,occurred_at FROM error_events ORDER BY occurred_at DESC,error_event_id DESC LIMIT ?", (bounded,)).fetchall()
        latest_tick = connection.execute("SELECT tick_id,status,started_at,completed_at FROM scheduler_ticks ORDER BY started_at DESC,tick_id DESC LIMIT 1").fetchone()
        registry_row = connection.execute("SELECT state FROM schedule_registry_state WHERE registry_id=1").fetchone()
        health_rows = connection.execute("SELECT module,status,observed_at,details_json FROM module_health_snapshots WHERE module IN ('Scheduler','Worker') ORDER BY observed_at DESC,rowid DESC").fetchall()
        source_counts = {
            "tick": int(connection.execute("SELECT COUNT(*) FROM scheduler_ticks WHERE status='failed'").fetchone()[0]),
            "occurrence": int(connection.execute("SELECT COUNT(*) FROM schedule_occurrences WHERE state IN ('failed','dead_letter','expired')").fetchone()[0]),
            "job": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('failed','dead_letter')").fetchone()[0]),
            "run": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('failed','dead_letter','interrupted')").fetchone()[0]),
        }
    candidates = []
    for row in tick_rows:
        candidates.append(("scheduler_tick_failed", row[0], "Scheduler tick 失败", "Scheduler", None, None, None, row[2] or "tick failed", row[3]))
    for row in occurrence_rows:
        kind = "dead_letter" if row[2] == "dead_letter" else "occurrence_expired" if row[2] == "expired" else "occurrence_failed"
        candidates.append((kind, row[0], f"计划 occurrence {row[1]} 进入 {row[2]}", "Scheduler", row[0], row[4], row[5], row[3] or row[2], row[6]))
    for row in job_rows:
        kind = "dead_letter" if row[2] == "dead_letter" else "job_failed"
        candidates.append((kind, row[0], f"Job {row[0]} 进入 {row[2]}", "Worker", None, row[0], row[1], row[4] or row[3] or row[2], row[5]))
    for row in run_rows:
        kind = "dead_letter" if row[2] == "dead_letter" else "run_failed"
        candidates.append((kind, row[0], f"Run {row[0]} 进入 {row[2]}", "Worker", None, row[1], row[0], row[4] or row[3] or row[2], row[5]))
    for row in error_rows:
        candidates.append(("error_event", row[1] or row[0], row[5], row[3], None, None, row[4], row[6], row[7], str(row[2] or "P2").upper()))

    latest_health: Dict[str, Any] = {}
    for row in health_rows:
        latest_health.setdefault(str(row[0]), row)
    for module, row in latest_health.items():
        status = str(row[1] or "unknown")
        stale = _is_stale(row[2], now=current, seconds=15 * 60)
        if status in {"incident", "degraded", "failed", "unknown"} or stale:
            freshness = "stale" if stale else "fresh"
            detail = f"status={status};freshness={freshness}"
            candidates.append(("health_check", module, f"模块 {module} 健康状态 {status}/{freshness}", module, None, None, None, detail, row[2], "P1"))
    if latest_tick is not None and _is_stale(latest_tick[3] or latest_tick[2], now=current, seconds=120):
        candidates.append(("heartbeat_stale", "Scheduler", "Scheduler tick 已超出 120 秒新鲜度", "Scheduler", None, None, None, f"last_tick={latest_tick[0]};status={latest_tick[1]}", latest_tick[3] or latest_tick[2], "P1"))

    active_fingerprints: set[str] = set()
    suppressed_fingerprints = store.list_ops_alert_fingerprints(state="suppressed", limit=bounded * 5)
    suppressed = 0
    for item in candidates:
        kind, entity_id, message, module, occurrence_id, job_id, run_id, detail, observed_at, *severity_override = item
        severity = severity_override[0] if severity_override else _severity(kind)
        fingerprint = _fingerprint(kind, str(entity_id), str(detail or ""))
        if kind in CANONICAL_ALERT_TYPES and fingerprint in suppressed_fingerprints:
            # A controlled reconciliation preserves the source failure but
            # prevents the same historical entity from reopening forever.
            suppressed += 1
            continue
        result = store.upsert_ops_alert(
            fingerprint=fingerprint,
            severity=severity,
            alert_type=kind,
            module=module,
            message=message,
            occurrence_id=occurrence_id,
            job_id=job_id,
            run_id=run_id,
            details={"detail": detail, "observed_at": observed_at},
        )
        if kind in CANONICAL_ALERT_TYPES:
            active_fingerprints.add(fingerprint)
        alerts.append(result)
        if result.get("deduplicated"):
            refreshed += 1
        else:
            created += 1

    resolvable_types = {"health_check", "heartbeat_stale"}
    if source_counts["tick"] <= bounded:
        resolvable_types.add("scheduler_tick_failed")
    if source_counts["occurrence"] <= bounded:
        resolvable_types.update({"occurrence_failed", "occurrence_expired", "dead_letter"})
    if source_counts["job"] <= bounded:
        resolvable_types.update({"job_failed", "dead_letter"})
    if source_counts["run"] <= bounded:
        resolvable_types.update({"run_failed", "dead_letter"})
    scheduler_healthy = bool(
        latest_tick is not None
        and str(latest_tick[1]) == "completed"
        and not _is_stale(latest_tick[3] or latest_tick[2], now=current, seconds=120)
        and (registry_row is None or str(registry_row[0]) == "valid")
    )
    if scheduler_healthy:
        resolvable_types.update({"registry_invalid", "duplicate_scheduler", "database_unavailable"})
    resolved = store.resolve_ops_alerts(alert_types=resolvable_types, active_fingerprints=active_fingerprints)
    return {"created": created, "refreshed": refreshed, "suppressed": suppressed, "resolved": len(resolved), "resolved_alerts": resolved, "alerts": alerts, "count": len(alerts), "source_status": "observed"}


def _notification_script(alert: Mapping[str, Any]) -> str:
    """Build an AppleScript string with values escaped as AppleScript text."""
    def quote(value: Any) -> str:
        return json.dumps(str(value or ""), ensure_ascii=False)

    title = f"PM Loop {alert.get('severity', 'P1')} 告警"
    body = f"{alert.get('module', 'unknown')}: {alert.get('message', 'unknown')} (alert_id={alert.get('alert_id', '')})"
    return f"display notification {quote(body)} with title {quote(title)}"


def deliver_macos_notifications(
    store: PMSystemStore,
    *,
    limit: int = 20,
    runner: Optional[Any] = None,
) -> Dict[str, Any]:
    """Deliver open P0/P1 alerts to macOS and persist every attempt.

    ``runner`` is injectable for tests.  No network or external messaging
    channel is used; a previously sent alert/fingerprint is skipped.
    """
    run = runner or subprocess.run
    results = []
    for alert in store.list_ops_alerts(limit=max(1, min(int(limit), 100)), state="open"):
        if str(alert.get("severity") or "").upper() not in {"P0", "P1"}:
            continue
        fingerprint = str(alert.get("fingerprint") or "")
        previous = [item for item in store.list_notification_deliveries(limit=1000) if item.get("alert_id") == alert.get("alert_id") and item.get("fingerprint") == fingerprint and item.get("state") in {"sent", "failed"}]
        if previous:
            results.append({"alert_id": alert.get("alert_id"), "state": "deduplicated"})
            continue
        command = ["/usr/bin/osascript", "-e", _notification_script(alert)]
        try:
            completed = run(command, capture_output=True, text=True, timeout=10, check=False)
            returncode = int(getattr(completed, "returncode", 1))
            state = "sent" if returncode == 0 else "failed"
            error = None if returncode == 0 else (getattr(completed, "stderr", "") or f"osascript exit={returncode}").strip()[-500:]
        except Exception as exc:  # pragma: no cover - host-level defensive path
            state = "failed"
            error = f"{type(exc).__name__}: {exc}"
        delivery = store.record_notification_delivery(alert_id=str(alert["alert_id"]), fingerprint=fingerprint, state=state, delivered_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if state == "sent" else None, error=error)
        results.append(delivery)
    return {"count": len(results), "sent": sum(item.get("state") == "sent" for item in results), "failed": sum(item.get("state") == "failed" for item in results), "deduplicated": sum(item.get("state") == "deduplicated" or item.get("deduplicated") for item in results), "notifications": results}


def refresh_ops_attention(store: PMSystemStore, *, limit: int = 200) -> Dict[str, Any]:
    """Project canonical failures and make one bounded local notification pass."""
    result = project_ops_attention(store, limit=limit)
    result["notification_delivery"] = deliver_macos_notifications(store, limit=limit)
    return result


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--notify", action="store_true", help="投递 P0/P1 macOS 本机通知")
    args = parser.parse_args(list(argv) if argv is not None else None)
    store = PMSystemStore(args.db_path, auto_migrate=False)
    result = project_ops_attention(store, limit=args.limit)
    if args.notify:
        result["notification_delivery"] = deliver_macos_notifications(store, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
