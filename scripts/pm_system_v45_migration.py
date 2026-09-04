#!/usr/bin/env python3
"""V4.5 R2 migration stage runner.

The runner is intentionally conservative: one stage lease, one manifest, and
one terminal decision per invocation.  It never auto-advances or retries a
failed stage.  Without ``--apply`` it performs read-only checks; mutation of
the persistent freeze is explicit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from pm_system_store import MigrationFrozen, MigrationLeaseConflict, PMSystemStore, now_iso
from pm_system_v45_g4 import classify_g4, probe_openapi


STAGES = ("G0", "G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9")
CANONICAL_PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_REPORT_DIR = Path.cwd() / "output" / "pm-system-v45-migration"
DEFAULT_OWNER = f"codex:{os.getpid()}"

BUSINESS_LAUNCH_LABELS = (
    "com.zhujie14.pm-loop-control-plane",
    "com.zhujie14.pm-system-worker",
    "com.zhujie14.ov-memory-sync",
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.catchup",
    "com.zhujie14.shengsuan-concepts-full-inventory-once",
)

PROCESS_MARKERS = (
    "pm_loop_control_plane_server.py",
    "pm_system_worker.py",
    "ov_memory_sync.py",
    "weekly-sync-and-refresh.sh",
    "pm-timeline/scripts/daily.sh",
    "pm-timeline/scripts/weekly-review.sh",
    "product-intelligence-monitor/scripts/sync.py",
    "catchup.py",
    "concept_inventory.py",
)


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _matching_processes() -> list[dict[str, Any]]:
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,pgid=,stat=,command="],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return [{"pid": None, "command": "ps unavailable"}]
    current_pid = os.getpid()
    result: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        if not any(marker in text for marker in PROCESS_MARKERS):
            continue
        fields = text.split(None, 4)
        try:
            pid = int(fields[0])
        except (ValueError, IndexError):
            continue
        if pid == current_pid:
            continue
        result.append(
            {
                "pid": pid,
                "ppid": int(fields[1]) if len(fields) > 1 and fields[1].isdigit() else None,
                "pgid": int(fields[2]) if len(fields) > 2 and fields[2].isdigit() else None,
                "stat": fields[3] if len(fields) > 3 else "",
                "command": fields[4] if len(fields) > 4 else text,
            }
        )
    return result


def automation_statuses() -> Dict[str, str]:
    root = Path.home() / ".codex" / "automations"
    result: Dict[str, str] = {}
    for automation_id in ("databuilder", "automation", "v4-4-s10"):
        path = root / automation_id / "automation.toml"
        try:
            value = tomllib.loads(path.read_text(encoding="utf-8"))
            result[automation_id] = str(value.get("status") or "missing")
        except (OSError, ValueError):
            result[automation_id] = "missing"
    return result


def stop_business_services(*, timeout_seconds: int = 30) -> Dict[str, Any]:
    """Unload business LaunchAgents and drain any direct descendants."""
    domain = _launchctl_domain()
    actions: list[dict[str, Any]] = []
    for label in BUSINESS_LAUNCH_LABELS:
        try:
            proc = subprocess.run(
                ["launchctl", "bootout", f"{domain}/{label}"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            actions.append(
                {
                    "label": label,
                    "returncode": proc.returncode,
                    "stderr": (proc.stderr or "").strip()[-240:],
                    "status": "unloaded" if proc.returncode == 0 else "not_loaded_or_failed",
                }
            )
        except (OSError, subprocess.SubprocessError) as exc:
            actions.append({"label": label, "status": "error", "error": f"{type(exc).__name__}: {exc}"})

    deadline = time.monotonic() + max(1, int(timeout_seconds))
    seen_term: set[int] = set()
    while time.monotonic() < deadline:
        remaining = _matching_processes()
        if not remaining:
            return {"actions": actions, "residual_processes": [], "drained": True}
        for item in remaining:
            pid = item.get("pid")
            if isinstance(pid, int) and pid not in seen_term:
                try:
                    os.kill(pid, 15)
                except (ProcessLookupError, PermissionError):
                    pass
                seen_term.add(pid)
        time.sleep(0.25)

    remaining = _matching_processes()
    for item in remaining:
        pid = item.get("pid")
        if isinstance(pid, int):
            try:
                os.kill(pid, 9)
            except (ProcessLookupError, PermissionError):
                pass
    time.sleep(0.5)
    residual = _matching_processes()
    return {"actions": actions, "residual_processes": residual, "drained": not residual}


def backup_database(db_path: Path, backup_dir: Path, *, migration_id: str, epoch: str) -> Dict[str, Any]:
    """Create and verify a consistent SQLite online backup."""
    backup_dir = Path(backup_dir).expanduser().resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)
    destination = backup_dir / f"{migration_id}-{epoch}-g0.sqlite3"
    temporary = destination.with_suffix(".sqlite3.tmp")
    temporary.unlink(missing_ok=True)
    source = sqlite3.connect(str(Path(db_path).expanduser().resolve()), timeout=10)
    target = sqlite3.connect(str(temporary), timeout=10)
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
        source.close()
    os.replace(temporary, destination)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    verify = sqlite3.connect(str(destination), timeout=10)
    try:
        integrity = str(verify.execute("PRAGMA integrity_check").fetchone()[0])
        schema_version = int(
            verify.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0]
        )
        counts = {
            table: int(verify.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("jobs", "runs", "outbox_items", "semantic_tasks", "migration_freeze")
        }
    finally:
        verify.close()
    return {
        "path": str(destination),
        "sha256": digest,
        "size_bytes": destination.stat().st_size,
        "integrity_check": integrity,
        "schema_version": schema_version,
        "counts": counts,
        "verified": integrity == "ok",
    }


def apply_g1_contracts(store: PMSystemStore) -> Dict[str, Any]:
    """Canonicalize legacy terminal rows and install provider capacity."""
    at = now_iso()
    classified: list[dict[str, Any]] = []
    with store.transaction() as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS provider_capacity (
                provider TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                model TEXT NOT NULL,
                max_concurrency INTEGER NOT NULL CHECK (max_concurrency > 0),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(provider, endpoint, model)
            )"""
        )
        configured = max(1, int(os.environ.get("PM_V45_PROVIDER_MAX_CONCURRENCY", "4") or "4"))
        buckets = connection.execute("SELECT provider,endpoint,model FROM provider_buckets").fetchall()
        if not buckets:
            buckets = [("oneapi", "default", "default")]
        for provider, endpoint, model in buckets:
            connection.execute(
                "INSERT INTO provider_capacity(provider,endpoint,model,max_concurrency,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(provider,endpoint,model) DO UPDATE SET max_concurrency=excluded.max_concurrency,updated_at=excluded.updated_at",
                (str(provider), str(endpoint), str(model), configured, at),
            )

        # The operation ledger did not exist before v7.  That absence is
        # itself insufficient evidence for replay, so all legacy failures are
        # classified/quarantined rather than retried during migration.
        required = ("artifact_uri", "model_input_hash", "provider", "error_fingerprint", "owner", "revision_id")
        for table, id_column in (("outbox_items", "outbox_id"), ("semantic_tasks", "semantic_task_id")):
            selected = "payload_json" if table == "outbox_items" else "NULL AS payload_json"
            rows = connection.execute(
                f"SELECT {id_column},status,revision_id,provider,owner,error_fingerprint,{selected} FROM {table} WHERE status IN ('failed','permanent_failed')"
            ).fetchall()
            for row in rows:
                entity_id, original_status, revision_id, provider, owner, error_fingerprint, payload_json = row
                try:
                    payload = json.loads(payload_json or "{}")
                except (TypeError, ValueError):
                    payload = {}
                artifact = payload.get("artifact_uri") or payload.get("file_path")
                evidence = {
                    "artifact_uri": artifact if artifact and Path(str(artifact)).is_file() else "",
                    "model_input_hash": payload.get("model_input_hash") or "",
                    "provider": provider,
                    "error_fingerprint": error_fingerprint or "",
                    "owner": owner,
                    "revision_id": revision_id,
                    "operation_ledger": False,
                }
                has_evidence = all(str(evidence.get(key) or "").strip() for key in required) and bool(evidence["operation_ledger"])
                failure_class = "replayable" if has_evidence else "quarantine"
                evidence_hash = hashlib.sha256(json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                connection.execute(
                    "INSERT OR IGNORE INTO historical_failure_classifications(entity_type,entity_id,original_status,failure_class,evidence_hash,classified_at,details_json) VALUES(?,?,?,?,?,?,?)",
                    (table, str(entity_id), str(original_status).lower(), failure_class, evidence_hash, at, json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
                )
                canonical = "failed" if str(original_status).lower() == "permanent_failed" else str(original_status).lower()
                if failure_class == "quarantine":
                    canonical = "quarantine"
                elif canonical == "permanent_failed":
                    canonical = "failed"
                connection.execute(
                    f"UPDATE {table} SET status=?,terminal_reason=COALESCE(terminal_reason,?) WHERE {id_column}=?",
                    (canonical, "historical_failure_evidence_missing" if failure_class == "quarantine" else None, entity_id),
                )
                classified.append({"table": table, "entity_id": str(entity_id), "original_status": str(original_status), "failure_class": failure_class, "canonical_status": canonical})
    return {
        "classified": classified,
        "classified_count": len(classified),
        "quarantine_count": sum(1 for item in classified if item["canonical_status"] == "quarantine"),
        "provider_capacity": True,
        "provider_limit": configured,
    }


def bootstrap_g0_fence(
    db_path: Path,
    *,
    migration_id: str,
    migration_epoch: str,
    stage_id: str,
    owner: str,
    lease_seconds: int,
) -> Dict[str, Any]:
    """Install the durable G0 fence before any schema upgrade is opened.

    The production database may still be on v6, while the v7 schema owns the
    full migration tables.  Creating only these two tables in v6 lets G0
    block new entrants before G1 performs the table rebuild.  The v7 DDL is
    deliberately compatible and keeps the rows during the later upgrade.
    """
    db_path = Path(db_path).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    at = now_iso()
    expires = _future_iso(lease_seconds)
    connection = sqlite3.connect(str(db_path), timeout=10, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            raise RuntimeError(f"SQLite WAL unavailable (journal_mode={mode})")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS migration_freeze (
                freeze_id INTEGER PRIMARY KEY CHECK (freeze_id = 1),
                migration_id TEXT NOT NULL UNIQUE,
                migration_epoch TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                state TEXT NOT NULL,
                deadline_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS migration_leases (
                migration_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                migration_epoch TEXT NOT NULL,
                owner TEXT NOT NULL,
                lease_id TEXT NOT NULL UNIQUE,
                acquired_at TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'active',
                PRIMARY KEY(migration_id, stage_id)
            )"""
        )
        freeze = connection.execute(
            "SELECT migration_id,migration_epoch,state FROM migration_freeze WHERE freeze_id=1"
        ).fetchone()
        if freeze and (str(freeze[0]) != migration_id or str(freeze[1]) != migration_epoch):
            raise RuntimeError(
                f"existing migration freeze conflicts: {freeze[0]}/{freeze[1]} ({freeze[2]})"
            )
        if freeze is None:
            connection.execute(
                """INSERT INTO migration_freeze(
                    freeze_id,migration_id,migration_epoch,stage_id,owner,state,
                    deadline_at,created_at,updated_at
                ) VALUES(1,?,?,?,?,?,?,?,?)""",
                (migration_id, migration_epoch, stage_id, owner, "freeze", expires, at, at),
            )
        else:
            connection.execute(
                "UPDATE migration_freeze SET stage_id=?,owner=?,state='freeze',deadline_at=?,updated_at=? WHERE freeze_id=1",
                (stage_id, owner, expires, at),
            )
        lease = connection.execute(
            "SELECT lease_id,owner,state,acquired_at,lease_expires_at FROM migration_leases WHERE migration_id=? AND stage_id=?",
            (migration_id, stage_id),
        ).fetchone()
        if lease and str(lease[2]) == "active" and str(lease[1]) != owner:
            raise RuntimeError(f"G0 migration lease held by {lease[1]}")
        lease_acquired_at = str(lease[3]) if lease and str(lease[2]) == "active" else at
        lease_expires_at = str(lease[4]) if lease and str(lease[2]) == "active" else expires
        if lease is None or str(lease[2]) != "active":
            lease_id = f"migration-{hashlib.sha256(f'{migration_id}:{stage_id}:{migration_epoch}:{owner}:{at}'.encode()).hexdigest()[:24]}"
            connection.execute(
                """INSERT OR REPLACE INTO migration_leases(
                    migration_id,stage_id,migration_epoch,owner,lease_id,acquired_at,lease_expires_at,state
                ) VALUES(?,?,?,?,?,?,?,'active')""",
                (migration_id, stage_id, migration_epoch, owner, lease_id, at, expires),
            )
        connection.execute("COMMIT")
        return {
            "migration_id": migration_id,
            "migration_epoch": migration_epoch,
            "stage_id": stage_id,
            "owner": owner,
            "state": "freeze",
            "deadline_at": expires,
            "lease_id": lease[0] if lease else lease_id,
            "acquired_at": lease_acquired_at,
            "lease_expires_at": lease_expires_at,
            "bootstrapped_before_schema_v7": True,
        }
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.DatabaseError:
            pass
        raise
    finally:
        connection.close()


def _future_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(seconds)))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def snapshot(store: PMSystemStore) -> Dict[str, Any]:
    with store.connect() as connection:
        def count(table: str, where: str = "") -> int:
            try:
                return int(connection.execute(f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")).fetchone()[0])
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    return 0
                raise

        return {
            "captured_at": now_iso(),
            "schema_version": store.schema_version(),
            "pragmas": store.pragmas(),
            "tables": {
                "jobs": count("jobs"),
                "runs": count("runs"),
                "outbox": count("outbox_items"),
                "semantic": count("semantic_tasks"),
                "memory_events": count("memory_change_events"),
                "operations": count("operation_ledger"),
                "watermarks": count("watermarks"),
                "failed": count("outbox_items", "status IN ('failed','permanent_failed')") + count("semantic_tasks", "status IN ('failed','permanent_failed')"),
                "dead_letter": count("outbox_items", "status='dead_letter'") + count("semantic_tasks", "status='dead_letter'"),
                "quarantine": count("outbox_items", "status='quarantine'") + count("semantic_tasks", "status='quarantine'"),
            },
            "active": {
                "jobs": count("jobs", "status IN ('queued','running','retry_wait')"),
                "outbox": count("outbox_items", "status IN ('pending','in_flight','retry_wait')"),
                "semantic": count("semantic_tasks", "status IN ('queued','running','retry_wait','in_flight')"),
                "slots": count("execution_slots", "status='leased'"),
                "dispatch_leases": count("outbox_dispatch_leases"),
                "provider_probe_leases": count("provider_probe_leases"),
                "provider_tokens": count("provider_tokens", "released_at IS NULL"),
            },
            "services": {
                "residual_business_processes": _matching_processes(),
                "automations": automation_statuses(),
            },
        }


def _report_path(report_dir: Path, stage: str) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"{stage.lower()}-检查报告.json"


def write_report(report_dir: Path, report: Dict[str, Any]) -> Path:
    path = _report_path(report_dir, str(report["stage_id"]))
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown = path.with_suffix(".md")
    markdown.write_text(
        "# V4.5 R2 " + str(report["stage_id"]) + " 检查报告\n\n" +
        f"- 判定：`{report['decision']}`\n- 模式：`{report['mode']}`\n- migration_id：`{report['migration_id']}`\n- migration_epoch：`{report['migration_epoch']}`\n- owner：`{report['owner']}`\n- 采集时间：`{report['finished_at']}`\n\n" +
        "## 检查项\n\n" + "\n".join(f"- {item['name']}：`{item['status']}`，{item.get('detail','')}" for item in report["checks"]) +
        "\n\n## 快照\n\n```json\n" + json.dumps(report["snapshot"], ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    html = markdown.with_suffix(".html")
    converter = Path(__file__).with_name("markdown_to_architecture_html.py")
    try:
        subprocess.run([sys.executable, str(converter), str(markdown), str(html)], check=True, capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError):
        # The Markdown report remains authoritative if the optional renderer
        # is unavailable in a minimal runtime.
        pass
    return path


def _check(
    stage: str,
    store: PMSystemStore,
    *,
    apply: bool,
    migration_id: str,
    epoch: str,
    require_services_drained: bool = False,
    backup_evidence: Optional[Dict[str, Any]] = None,
    g6_manifest_path: Optional[Path] = None,
    g7_manifest_path: Optional[Path] = None,
    g8_manifest_path: Optional[Path] = None,
    g9_manifest_path: Optional[Path] = None,
) -> list[Dict[str, Any]]:
    current = snapshot(store)
    checks: list[Dict[str, Any]] = [{"name": "schema/integrity", "status": "PASS", "detail": f"schema={current['schema_version']} journal={current['pragmas'].get('journal_mode')}"}]
    if stage == "G0":
        freeze = store.migration_freeze()
        checks.append({"name": "persistent migration freeze", "status": "PASS" if freeze and freeze.get("migration_id") == migration_id else "HOLD", "detail": "已写入持久 fence" if freeze else "需要先执行 --apply G0"})
        checks.append({"name": "drain leases", "status": "PASS" if not any(current["active"].get(key) for key in ("slots", "dispatch_leases", "provider_probe_leases", "provider_tokens")) else "HOLD", "detail": json.dumps(current["active"], ensure_ascii=False)})
        if require_services_drained:
            residual = current.get("services", {}).get("residual_business_processes", [])
            checks.append({"name": "business services drained", "status": "PASS" if not residual else "HOLD", "detail": json.dumps(residual, ensure_ascii=False)})
            automations = current.get("services", {}).get("automations", {})
            checks.append({"name": "Codex Automations paused", "status": "PASS" if automations and all(value == "PAUSED" for value in automations.values()) else "HOLD", "detail": json.dumps(automations, ensure_ascii=False)})
            checks.append({"name": "online backup restore rehearsal", "status": "PASS" if backup_evidence and backup_evidence.get("verified") else "HOLD", "detail": json.dumps(backup_evidence or {}, ensure_ascii=False)})
    elif stage == "G1":
        with store.connect() as connection:
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            index_rows = connection.execute("PRAGMA index_list(outbox_items)").fetchall()
            index_names = {str(row[1]) for row in index_rows}
            old_unique = False
            for row in index_rows:
                if int(row[2] or 0) != 1:
                    continue
                columns = [str(item[2]) for item in connection.execute(f"PRAGMA index_info('{str(row[1]).replace(chr(39), chr(39)*2)}')").fetchall()]
                if columns == ["idempotency_key"]:
                    old_unique = True
            capacity_count = int(connection.execute("SELECT COUNT(*) FROM provider_capacity").fetchone()[0])
            classified_count = int(connection.execute("SELECT COUNT(*) FROM historical_failure_classifications").fetchone()[0])
            legacy_failures = int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status='permanent_failed'").fetchone()[0]) + int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status='permanent_failed'").fetchone()[0])
        checks.extend([
            {"name": "v7 tables", "status": "PASS" if current["schema_version"] >= 7 else "HOLD", "detail": "runtime governance schema"},
            {"name": "SQLite integrity", "status": "PASS" if integrity == "ok" else "HOLD", "detail": integrity},
            {"name": "composite idempotency indexes", "status": "PASS" if "uq_outbox_logical_v7" in index_names and not old_unique else "HOLD", "detail": sorted(index_names)},
            {"name": "canonical terminal mapping", "status": "PASS" if legacy_failures == 0 and classified_count >= current["tables"]["quarantine"] else "HOLD", "detail": f"classifications={classified_count}, permanent_failed={legacy_failures}"},
            {"name": "provider global capacity", "status": "PASS" if capacity_count > 0 else "HOLD", "detail": f"buckets={capacity_count}"},
        ])
    elif stage == "G2":
        required_names = {"source", "content", "knowledge", "active_generation"}
        with store.connect() as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT source_domain,watermark_name,captured_at,sequence,value_hash,value,producer,state "
                "FROM watermarks WHERE source_domain='pm-runtime' ORDER BY watermark_name"
            ).fetchall()]
            events = int(connection.execute("SELECT COUNT(*) FROM watermark_events WHERE source_domain='pm-runtime'").fetchone()[0])
            modules = {str(row[0]) for row in connection.execute("SELECT DISTINCT module FROM module_health_snapshots").fetchall()}
            metrics = int(connection.execute("SELECT COUNT(*) FROM metric_rollups WHERE metric_name LIKE 'g2.%'").fetchone()[0])
        by_name = {str(row.get("watermark_name")): row for row in rows}
        malformed: list[str] = []
        for name in required_names:
            row = by_name.get(name)
            if row is None or not row.get("producer") or not row.get("value_hash") or int(row.get("captured_at") or 0) <= 0:
                malformed.append(name)
                continue
            state = str(row.get("state") or "")
            if state not in {"accepted", "missing", "unknown", "quarantine", "replay_rejected"}:
                malformed.append(name)
            if state in {"missing", "unknown"}:
                try:
                    value = json.loads(str(row.get("value") or "{}"))
                except (TypeError, ValueError):
                    value = {}
                if not isinstance(value, dict) or str(value.get("status") or "") not in {"missing", "unknown"}:
                    malformed.append(name)
        checks.extend([
            {"name": "structured watermarks", "status": "PASS" if not malformed and required_names.issubset(by_name) else "HOLD", "detail": f"pm-runtime={len(by_name)}/4 malformed={sorted(set(malformed))}"},
            {"name": "watermark event ledger", "status": "PASS" if events >= len(required_names) else "HOLD", "detail": f"events={events}"},
            {"name": "module snapshot producer", "status": "PASS" if len(modules) >= 9 else "HOLD", "detail": f"modules={len(modules)}"},
            {"name": "metric rollup producer", "status": "PASS" if metrics >= 3 else "HOLD", "detail": f"g2 metrics={metrics}"},
        ])
    elif stage == "G3":
        with store.connect() as connection:
            event_rows = [dict(row) for row in connection.execute("SELECT event_id,name,content_hash,namespace_epoch,state FROM memory_change_events ORDER BY observed_at,event_id").fetchall()]
            memory_rows = [dict(row) for row in connection.execute("SELECT outbox_id,kind,profile,status,resource_id,revision_id,namespace_epoch,terminal_reason,payload_json FROM outbox_items WHERE kind='memory'").fetchall()]
            non_memory_claimable = int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE kind='memory' AND status IN ('pending','in_flight','retry_wait')").fetchone()[0])
        event_ids = {str(row["event_id"]) for row in event_rows}
        linked_event_ids: set[str] = set()
        for row in memory_rows:
            try:
                payload = json.loads(row.get("payload_json") or "{}")
            except (TypeError, ValueError):
                payload = {}
            if payload.get("event_id"):
                linked_event_ids.add(str(payload["event_id"]))
        checks.extend([
            {"name": "durable memory events", "status": "PASS" if event_rows and all(row.get("content_hash") and row.get("namespace_epoch") for row in event_rows) else "HOLD", "detail": f"events={len(event_rows)}"},
            {"name": "memory outbox linkage", "status": "PASS" if memory_rows and linked_event_ids.issubset(event_ids) and all(row.get("profile") == "memory-skill" for row in memory_rows) else "HOLD", "detail": f"memory_outbox={len(memory_rows)} linked_events={len(linked_event_ids)}"},
            {"name": "memory queue isolation", "status": "PASS" if non_memory_claimable == 0 else "HOLD", "detail": f"claimable_memory_rows={non_memory_claimable}; freeze/direct writer remains off"},
        ])
    elif stage == "G4":
        adapter = os.environ.get("PM_V45_MEMORY_LINK_ADAPTER", "").strip() or None
        smoke = os.environ.get("PM_V45_MEMORY_LINK_SMOKE", "").strip() or None
        try:
            configured_timeout = float(os.environ.get("PM_V45_OPENAPI_TIMEOUT", "30"))
        except ValueError:
            configured_timeout = 30.0
        # The local OpenViking OpenAPI document can take several seconds while
        # the service enumerates its routes.  Bound the override so a stalled
        # endpoint cannot hold the migration lease indefinitely.
        openapi_timeout = min(60.0, max(5.0, configured_timeout))
        probe = probe_openapi(os.environ.get("OPENVIKING_URL", "http://127.0.0.1:1933"), timeout=openapi_timeout)
        classified = classify_g4(probe, adapter=adapter, smoke=smoke)
        decision = str(classified.get("decision"))
        check_status = {
            "PASS": "PASS",
            "PASS_WITH_SKIP": "SKIPPED/HOLD",
            "HOLD": "HOLD",
        }.get(decision, "HOLD")
        checks.append({
            "name": "independent MemoryLink API",
            "status": check_status,
            "detail": json.dumps({"decision": decision, "reason": classified.get("reason"), "probe": probe, "adapter": adapter or ""}, ensure_ascii=False, sort_keys=True),
        })
    elif stage == "G5":
        manifest_path = Path(os.environ.get("PM_V45_G5_MANIFEST", "") or (Path.cwd() / "docs/03-产品架构/v4.5实施报告/g5-skill-migration-manifest.json"))
        manifest: Dict[str, Any] = {}
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        expected = set(str(item) for item in (manifest.get("expected_names") or []))
        actual = set(str(item) for item in ((manifest.get("native_after") or {}).get("names") or []))
        validations = manifest.get("validations") or []
        checks.append({"name": "skill source freeze", "status": "PASS" if manifest.get("source_freeze") and manifest.get("namespace_epoch") == epoch else "HOLD", "detail": str(manifest_path)})
        checks.append({"name": "native Skill strict validation", "status": "PASS" if validations and all(bool(item.get("valid")) for item in validations) else "HOLD", "detail": f"valid={sum(1 for item in validations if item.get('valid'))}/{len(validations)}"})
        checks.append({"name": "native Skill shadow/read-back", "status": "PASS" if manifest.get("shadow_complete") and expected == actual else "HOLD", "detail": f"expected={len(expected)} actual={len(actual)}"})
        checks.append({"name": "native canary CRUD", "status": "PASS" if (manifest.get("canary") or {}).get("passed") else "HOLD", "detail": "add/get/update/find/delete read-back"})
        checks.append({"name": "legacy Skill namespace fence", "status": "PASS" if (manifest.get("legacy_namespace") or {}).get("enqueue_fenced") else "HOLD", "detail": "old resource enqueue is denied"})
        checks.append({"name": "legacy namespace retained for rollback", "status": "PASS" if (manifest.get("legacy_namespace") or {}).get("state") == "retained_for_rollback" and not (manifest.get("legacy_namespace") or {}).get("physical_delete_performed") else "HOLD", "detail": "physical deletion deferred pending explicit confirmation"})
        operations = manifest.get("operation_ledger") or {}
        checks.append({"name": "Skill operation ledger", "status": "PASS" if int(operations.get("unknown") or 0) == 0 and int(operations.get("quarantine") or 0) == 0 else "HOLD", "detail": json.dumps(operations, ensure_ascii=False)})
        residual = current.get("services", {}).get("residual_business_processes", [])
        checks.append({"name": "business services drained", "status": "PASS" if not residual else "HOLD", "detail": json.dumps(residual, ensure_ascii=False)})
    elif stage == "G6":
        manifest_path = g6_manifest_path or (Path.cwd() / "docs/03-产品架构/v4.5实施报告/g6-runtime-manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        freeze = store.migration_freeze() or {}
        cardinality = manifest.get("process_cardinality") or {}
        automations = manifest.get("automations") or {}
        hashes = manifest.get("after_sha256") or {}
        checks.extend([
            {"name": "persistent G6 freeze", "status": "PASS" if freeze.get("migration_id") == migration_id and freeze.get("migration_epoch") == epoch and freeze.get("stage_id") == "G6" and freeze.get("state") == "freeze" else "HOLD", "detail": json.dumps(freeze, ensure_ascii=False)},
            {"name": "canonical/runtime hash convergence", "status": "PASS" if manifest.get("decision") == "PASS" and len(hashes) >= 15 and not manifest.get("issues") else "HOLD", "detail": f"targets={len(hashes)} issues={len(manifest.get('issues') or [])}"},
            {"name": "absolute Python and LaunchAgent contract", "status": "PASS" if manifest.get("canonical_python") == CANONICAL_PYTHON and len(manifest.get("maintenance_plists") or []) == 3 and not manifest.get("legacy_scan", {}).get("issues") else "HOLD", "detail": json.dumps(manifest.get("legacy_scan") or {}, ensure_ascii=False)},
            {"name": "single maintenance process each", "status": "PASS" if cardinality and all(int(cardinality.get(label, 0)) == 1 for label in ("com.zhujie14.pm-loop-control-plane", "com.zhujie14.pm-system-worker", "com.zhujie14.ov-memory-sync")) else "HOLD", "detail": json.dumps(cardinality, ensure_ascii=False)},
            {"name": "watcher durable memory events", "status": "PASS" if manifest.get("decision") == "PASS" and any("ov-memory-sync" in name for name in (manifest.get("maintenance_plists") or [])) else "HOLD", "detail": "plist includes --durable-events and outbox mode"},
            {"name": "Control Plane POST remains frozen", "status": "PASS" if (manifest.get("control_plane_post_freeze") or {}).get("status") == "pass" and (manifest.get("control_plane_post_freeze") or {}).get("http_status") == 405 else "HOLD", "detail": json.dumps(manifest.get("control_plane_post_freeze") or {}, ensure_ascii=False)},
            {"name": "Codex Automations remain paused", "status": "PASS" if automations and all(str(value) == "PAUSED" for value in automations.values()) else "HOLD", "detail": json.dumps(automations, ensure_ascii=False)},
        ])
    elif stage == "G7":
        manifest_path = g7_manifest_path or (Path.cwd() / "docs/03-产品架构/v4.5实施报告/g7-performance-manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        gate_checks = manifest.get("checks") or []
        strict_pass = manifest.get("decision") == "PASS" and gate_checks and all(item.get("status") == "PASS" or (item.get("name") == "shadow profile memory-link" and item.get("status") == "SKIPPED/HOLD") for item in gate_checks)
        baseline_pass = manifest.get("decision") == "PASS_WITH_BASELINE" and (manifest.get("baseline_acceptance") or {}).get("status") == "PASS"
        checks.append({"name": "host OpenViking performance gate", "status": "PASS" if strict_pass or baseline_pass else "HOLD", "detail": f"manifest={manifest_path}; checks={len(gate_checks)}; mode={'strict' if strict_pass else 'accepted-baseline' if baseline_pass else 'hold'}"})
        checks.append({"name": "performance thresholds and baseline registry", "status": "PASS" if (strict_pass or baseline_pass) and manifest.get("thresholds") and manifest.get("minimums") else "HOLD", "detail": json.dumps({"thresholds": manifest.get("thresholds"), "minimums": manifest.get("minimums"), "baseline_acceptance": manifest.get("baseline_acceptance")}, ensure_ascii=False)})
    elif stage == "G8":
        manifest_path = g8_manifest_path or (Path.cwd() / "docs/03-产品架构/v4.5实施报告/g8-recovery-manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        scenarios = manifest.get("scenarios") if isinstance(manifest.get("scenarios"), list) else []
        expected = {
            "model disconnect and bounded retry",
            "OpenViking 504 profile isolation",
            "duplicate revision idempotency",
            "cancel and late callback terminal fence",
            "restart lease reconciliation",
            "Resource 429 total-wall-clock terminal",
            "model 429 Retry-After and deadline",
            "response-unknown one controlled resend",
            "missed-period catch-up idempotency",
        }
        names = {str(item.get("name")) for item in scenarios if isinstance(item, dict)}
        all_pass = bool(
            manifest.get("decision") == "PASS"
            and names == expected
            and len(scenarios) == len(expected)
            and all(isinstance(item, dict) and item.get("status") == "PASS" for item in scenarios)
        )
        checks.extend([
            {
                "name": "fault/recovery/catch-up",
                "status": "PASS" if all_pass else "HOLD",
                "detail": f"manifest={manifest_path}; scenarios={len(scenarios)}/{len(expected)}; decision={manifest.get('decision')}",
            },
            {
                "name": "G8 production/provider isolation",
                "status": "PASS" if all_pass and manifest.get("production_state_touched") is False and int(manifest.get("external_provider_calls") or 0) == 0 else "HOLD",
                "detail": json.dumps({"production_state_touched": manifest.get("production_state_touched"), "external_provider_calls": manifest.get("external_provider_calls")}, ensure_ascii=False),
            },
        ])
        with store.connect() as connection:
            watermark_rows = [dict(row) for row in connection.execute(
                "SELECT watermark_name,captured_at,sequence,value_hash,value,producer,state "
                "FROM watermarks WHERE source_domain='pm-runtime'"
            ).fetchall()]
            active_generation_rows = int(connection.execute(
                "SELECT COUNT(*) FROM generations WHERE status='active'"
            ).fetchone()[0])
            # Memory watcher events are durable change notifications, not
            # Resource work. During the persistent freeze, pending Memory
            # rows remain deferred for post-migration catch-up. Every other
            # Outbox kind (including Skill and future MemoryLink work) is
            # claimable and must block a successful drain. Probe leases are
            # included because they can keep provider capacity occupied even
            # when no task row is currently active.
            active = {
                "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
                "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE kind <> 'memory' AND status IN ('pending','in_flight','retry_wait')").fetchone()[0]),
                "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status IN ('queued','running','retry_wait','in_flight')").fetchone()[0]),
                "slots": int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased'").fetchone()[0]),
                "dispatch_leases": int(connection.execute("SELECT COUNT(*) FROM outbox_dispatch_leases").fetchone()[0]),
                "provider_probe_leases": int(connection.execute("SELECT COUNT(*) FROM provider_probe_leases").fetchone()[0]),
                "provider_tokens": int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]),
                "model_calls": int(connection.execute("SELECT COUNT(*) FROM model_calls WHERE status='running' OR response_state='waiting'").fetchone()[0]),
            }
            memory_deferred = int(connection.execute(
                "SELECT COUNT(*) FROM outbox_items WHERE kind='memory' AND status='pending'"
            ).fetchone()[0])
            memory_non_deferred = int(connection.execute(
                "SELECT COUNT(*) FROM outbox_items WHERE kind='memory' AND status IN ('in_flight','retry_wait')"
            ).fetchone()[0])
            pending_memory_events = int(connection.execute(
                "SELECT COUNT(*) FROM memory_change_events WHERE state='pending'"
            ).fetchone()[0])
            memory_event_orphans = int(connection.execute(
                """SELECT COUNT(*) FROM memory_change_events AS e
                   WHERE e.state='pending' AND NOT EXISTS (
                     SELECT 1 FROM outbox_items AS o
                     WHERE o.kind='memory'
                       AND o.profile='memory-skill'
                       AND json_extract(o.payload_json,'$.event_id')=e.event_id
                       AND json_extract(o.payload_json,'$.content_hash')=e.content_hash
                       AND o.revision_id=e.content_hash
                       AND o.namespace_epoch=e.namespace_epoch
                       AND o.status='pending'
                   )"""
            ).fetchone()[0])
            invalid_terminal = int(connection.execute(
                "SELECT COUNT(*) FROM outbox_items WHERE status NOT IN ('pending','in_flight','retry_wait','completed','failed','dead_letter','quarantine')"
            ).fetchone()[0]) + int(connection.execute(
                "SELECT COUNT(*) FROM semantic_tasks WHERE status NOT IN ('accepted','processing','queued','running','retry_wait','in_flight','completed','failed','dead_letter','quarantine')"
            ).fetchone()[0])
            permanent_failed = int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status='permanent_failed'").fetchone()[0]) + int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status='permanent_failed'").fetchone()[0])
        required_watermarks = {"source", "content", "knowledge", "active_generation"}
        watermark_by_name = {str(row.get("watermark_name")): row for row in watermark_rows}
        watermark_well_formed = required_watermarks.issubset(watermark_by_name) and all(
            int(watermark_by_name[name].get("captured_at") or 0) > 0
            and watermark_by_name[name].get("value_hash")
            and watermark_by_name[name].get("producer")
            for name in required_watermarks
        )
        core_watermark_ok = all(
            str(watermark_by_name.get(name, {}).get("state") or "") == "accepted"
            for name in ("source", "content", "knowledge")
        )
        active_row = watermark_by_name.get("active_generation", {})
        try:
            active_value = json.loads(str(active_row.get("value") or "{}"))
        except (TypeError, ValueError):
            active_value = {}
        active_generation_ok = str(active_row.get("state") or "") == "accepted"
        active_generation_disabled = bool(
            str(active_row.get("state") or "") == "missing"
            and isinstance(active_value, dict)
            and active_value.get("status") == "missing"
            and active_value.get("refresh_disabled") is True
            and str(active_value.get("reason") or "").strip()
            and active_generation_rows == 0
        )
        watermark_ok = watermark_well_formed and core_watermark_ok and active_generation_ok
        watermark_skip = watermark_well_formed and core_watermark_ok and active_generation_disabled
        watermark_states = ", ".join(
            "%s:%s" % (name, watermark_by_name.get(name, {}).get("state", "missing"))
            for name in sorted(required_watermarks)
        )
        checks.extend([
            {
                "name": "final structured watermarks",
                "status": "PASS" if watermark_ok else "SKIPPED/HOLD" if watermark_skip else "HOLD",
                "detail": (
                    f"pm-runtime={len(watermark_by_name)}/4; states={{{watermark_states}}}; "
                    f"active_generation_rows={active_generation_rows}; "
                    f"concept_refresh_disabled={bool(active_value.get('refresh_disabled'))}; "
                    "missing Active Generation is accepted only as a disabled capability"
                ),
            },
            {
                "name": "final drain and orphan fence",
                "status": "PASS" if not any(active.values()) and memory_non_deferred == 0 and memory_event_orphans == 0 else "HOLD",
                "detail": json.dumps({
                    **active,
                    "memory_deferred": memory_deferred,
                    "pending_memory_events": pending_memory_events,
                    "memory_non_deferred": memory_non_deferred,
                    "memory_event_orphans": memory_event_orphans,
                    "memory_deferred_policy": "pending durable Memory events are held for post-freeze catch-up",
                }, ensure_ascii=False),
            },
            {
                "name": "canonical terminal statuses",
                "status": "PASS" if invalid_terminal == 0 and permanent_failed == 0 else "HOLD",
                "detail": json.dumps({"invalid_terminal": invalid_terminal, "permanent_failed": permanent_failed}, ensure_ascii=False),
            },
        ])
    elif stage == "G9":
        manifest_path = g9_manifest_path or (Path.cwd() / "docs/03-产品架构/v4.5实施报告/g9-independent-review-manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        rounds = manifest.get("rounds") if isinstance(manifest.get("rounds"), list) else []
        final_round = rounds[-1] if rounds and isinstance(rounds[-1], dict) else {}
        findings = [
            finding
            for round_item in rounds
            if isinstance(round_item, dict)
            for finding in (round_item.get("findings") or [])
            if isinstance(finding, dict)
        ]
        unresolved_blockers = [
            finding
            for finding in findings
            if str(finding.get("severity") or "").upper() in {"P0", "P1"}
            and str(finding.get("status") or "").lower() not in {"resolved", "closed", "not_applicable"}
        ]
        reviewed_artifacts = manifest.get("reviewed_artifacts") if isinstance(manifest.get("reviewed_artifacts"), list) else []
        artifact_errors: list[str] = []
        for item in reviewed_artifacts:
            if not isinstance(item, dict) or not item.get("path") or not item.get("sha256"):
                artifact_errors.append("malformed reviewed artifact")
                continue
            path = Path(str(item["path"])).expanduser()
            if not path.is_file():
                artifact_errors.append(f"missing:{path}")
                continue
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != str(item["sha256"]):
                artifact_errors.append(f"hash_mismatch:{path}")
        runtime_hashes = manifest.get("runtime_hashes") if isinstance(manifest.get("runtime_hashes"), list) else []
        runtime_errors: list[str] = []
        for item in runtime_hashes:
            if not isinstance(item, dict):
                runtime_errors.append("malformed:unknown")
                continue
            name = str(item.get("name") or "unknown")
            canonical_path_raw = item.get("canonical_path")
            runtime_path_raw = item.get("runtime_path")
            canonical_declared = str(item.get("canonical_sha256") or "")
            runtime_declared = str(item.get("runtime_sha256") or "")
            if not canonical_path_raw or not runtime_path_raw or not canonical_declared or not runtime_declared:
                runtime_errors.append(f"malformed:{name}")
                continue
            canonical_path = Path(str(canonical_path_raw)).expanduser()
            runtime_path = Path(str(runtime_path_raw)).expanduser()
            if not canonical_path.is_file():
                runtime_errors.append(f"missing_canonical:{name}:{canonical_path}")
                continue
            if not runtime_path.is_file():
                runtime_errors.append(f"missing_runtime:{name}:{runtime_path}")
                continue
            canonical_actual = hashlib.sha256(canonical_path.read_bytes()).hexdigest()
            runtime_actual = hashlib.sha256(runtime_path.read_bytes()).hexdigest()
            if canonical_actual != canonical_declared:
                runtime_errors.append(f"canonical_hash_mismatch:{name}")
            if runtime_actual != runtime_declared:
                runtime_errors.append(f"runtime_hash_mismatch:{name}")
            if canonical_actual != runtime_actual:
                runtime_errors.append(f"runtime_drift:{name}")
        freeze = store.migration_freeze() or {}
        review_ok = bool(
            manifest.get("decision") == "PASS"
            and manifest.get("read_only") is True
            and manifest.get("production_state_touched") is False
            and int(manifest.get("external_provider_calls") or 0) == 0
            and 1 <= len(rounds) <= 2
            and int(final_round.get("p0") or 0) == 0
            and int(final_round.get("p1") or 0) == 0
            and not unresolved_blockers
        )
        checks.extend([
            {
                "name": "independent review",
                "status": "PASS" if review_ok else "HOLD",
                "detail": f"manifest={manifest_path}; rounds={len(rounds)}; final_p0={final_round.get('p0')}; final_p1={final_round.get('p1')}; unresolved={len(unresolved_blockers)}",
            },
            {
                "name": "reviewed artifact hashes",
                "status": "PASS" if reviewed_artifacts and not artifact_errors else "HOLD",
                "detail": json.dumps({"artifacts": len(reviewed_artifacts), "errors": artifact_errors}, ensure_ascii=False),
            },
            {
                "name": "canonical/runtime hash convergence",
                "status": "PASS" if runtime_hashes and not runtime_errors else "HOLD",
                "detail": json.dumps({"targets": len(runtime_hashes), "errors": runtime_errors}, ensure_ascii=False),
            },
            {
                "name": "persistent G9 freeze",
                "status": "PASS" if freeze.get("migration_id") == migration_id and freeze.get("migration_epoch") == epoch and freeze.get("stage_id") == "G9" and freeze.get("state") == "freeze" else "HOLD",
                "detail": json.dumps(freeze, ensure_ascii=False),
            },
        ])
    return checks


def run_stage(
    stage: str,
    *,
    db_path: Path,
    report_dir: Path,
    migration_id: str,
    epoch: str,
    owner: str,
    apply: bool,
    lease_seconds: int,
    revalidate: bool = False,
    stop_services: bool = False,
    backup_dir: Optional[Path] = None,
    g7_baseline_manifest: Optional[Path] = None,
    g9_review_manifest: Optional[Path] = None,
) -> Dict[str, Any]:
    stage_id = str(stage).upper()
    if stage_id not in STAGES:
        raise ValueError(f"invalid stage: {stage}")
    # G0 is the only stage that may open the v6 database without running the
    # v7 schema migration.  The bootstrap fence is persisted first, then all
    # services are stopped and drained before G1 opens the auto-migrating
    # store.
    bootstrap = None
    g1_result = None
    g6_result = None
    g7_result = None
    g8_result = None
    if stage_id == "G0" and apply:
        bootstrap = bootstrap_g0_fence(
            db_path,
            migration_id=migration_id,
            migration_epoch=epoch,
            stage_id=stage_id,
            owner=owner,
            lease_seconds=lease_seconds,
        )
        drain = (
            stop_business_services(timeout_seconds=min(60, max(5, lease_seconds // 10)))
            if stop_services
            else None
        )
    else:
        drain = None
    store = PMSystemStore(
        db_path,
        max_schema_version=6 if (stage_id == "G0" and apply) else None,
        auto_migrate=True,
    )
    if stage_id == "G1" and apply:
        g1_result = apply_g1_contracts(store)
    backup = (
        backup_database(
            db_path,
            backup_dir or (Path.home() / ".codex" / "backups" / "v4.5-r2"),
            migration_id=migration_id,
            epoch=epoch,
        )
        if stage_id == "G0" and apply and stop_services
        else None
    )
    existing_path = _report_path(report_dir, stage_id)
    existing_report: Optional[Dict[str, Any]] = None
    revalidation_input: Optional[Dict[str, Any]] = None
    if existing_path.is_file():
        try:
            existing_report = json.loads(existing_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            existing_report = None
        if isinstance(existing_report, dict) and existing_report.get("decision") in {"PASS", "PASS_WITH_SKIP", "PASS_WITH_BASELINE"} and existing_report.get("migration_epoch") == epoch:
            if revalidate:
                freeze = store.migration_freeze() or {}
                if not (
                    freeze.get("migration_id") == migration_id
                    and freeze.get("migration_epoch") == epoch
                    and freeze.get("stage_id") == stage_id
                    and freeze.get("state") == "freeze"
                ):
                    raise MigrationFrozen("revalidation requires the persistent freeze to remain on the same stage and epoch")
                revalidation_input = {
                    "previous_decision": existing_report.get("decision"),
                    "previous_manifest_hash": existing_report.get("manifest_hash"),
                    "previous_finished_at": existing_report.get("finished_at"),
                }
            else:
                report = {
                    "schema": "pm-system.v45-r2-migration-report.v1",
                    "stage_id": stage_id,
                    "migration_id": migration_id,
                    "migration_epoch": epoch,
                    "owner": owner,
                    "mode": "apply" if apply else "check",
                    "lease": None,
                    "started_at": now_iso(),
                    "finished_at": now_iso(),
                    "decision": "HOLD",
                    "checks": [{"name": "duplicate stage execution", "status": "HOLD", "detail": "该 stage 已在同一 epoch PASS；使用显式 --revalidate 只复验当前 freeze 阶段，或创建新 epoch"}],
                    "snapshot": snapshot(store),
                    "canonical_report_preserved": True,
                }
                report["manifest_hash"] = _hash(report)
                report["report_path"] = str(existing_path)
                return report
        elif revalidate:
            freeze = store.migration_freeze() or {}
            if not (
                isinstance(existing_report, dict)
                and existing_report.get("migration_epoch") == epoch
                and freeze.get("migration_id") == migration_id
                and freeze.get("migration_epoch") == epoch
                and freeze.get("stage_id") == stage_id
                and freeze.get("state") == "freeze"
            ):
                raise MigrationFrozen("revalidation requires an existing same-epoch report and a persistent freeze on the same stage")
            revalidation_input = {
                "previous_decision": existing_report.get("decision"),
                "previous_manifest_hash": existing_report.get("manifest_hash"),
                "previous_finished_at": existing_report.get("finished_at"),
            }
    index = STAGES.index(stage_id)
    if index:
        previous = STAGES[index - 1]
        previous_path = _report_path(report_dir, previous)
        previous_report: Optional[Dict[str, Any]] = None
        if previous_path.is_file():
            try:
                previous_report = json.loads(previous_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                previous_report = None
        allowed_previous = {"PASS"}
        if previous in {"G4", "G8"}:
            allowed_previous.add("PASS_WITH_SKIP")
        if previous == "G7":
            allowed_previous.add("PASS_WITH_BASELINE")
        if not previous_report or previous_report.get("decision") not in allowed_previous or previous_report.get("migration_epoch") != epoch:
            report = {
                "schema": "pm-system.v45-r2-migration-report.v1",
                "stage_id": stage_id,
                "migration_id": migration_id,
                "migration_epoch": epoch,
                "owner": owner,
                "mode": "apply" if apply else "check",
                "lease": None,
                "started_at": now_iso(),
                "finished_at": now_iso(),
                "decision": "HOLD",
                "checks": [{"name": "previous stage gate", "status": "HOLD", "detail": f"{previous} 必须以同一 epoch PASS；禁止跳阶段"}],
                "snapshot": snapshot(store),
            }
            report["manifest_hash"] = _hash(report)
            report_path = write_report(report_dir, report)
            report["report_path"] = str(report_path)
            return report
    lease = bootstrap or store.acquire_migration_lease(
        migration_id=migration_id,
        stage_id=stage_id,
        migration_epoch=epoch,
        owner=owner,
        lease_seconds=lease_seconds,
    )
    mode = ("revalidate-apply" if apply else "revalidate-check") if revalidate else ("apply" if apply else "check")
    if stage_id == "G0" and apply and bootstrap is not None:
        # The raw bootstrap already wrote the fence.  Do not reopen a second
        # transaction or extend the lease after the services are drained.
        pass
    if stage_id == "G6" and apply:
        from pm_system_v45_g6 import apply_g6

        g6_result = apply_g6(
            db_path=db_path,
            backup_root=backup_dir or (Path.home() / ".codex" / "backups" / "v4.5-r2" / "G6-runtime-before"),
            manifest_path=report_dir / "g6-runtime-manifest.json",
            execute_launchd=True,
        )
    if stage_id == "G7" and apply:
        from pm_system_v45_g7 import run_g7

        g7_result = run_g7(
            db_path=db_path,
            manifest_path=report_dir / "g7-performance-manifest.json",
            baseline_manifest=g7_baseline_manifest,
            execute=True,
        )
    if stage_id == "G8" and apply:
        from pm_system_v45_g8 import run_g8

        g8_result = run_g8()
        g8_manifest_path = report_dir / "g8-recovery-manifest.json"
        g8_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        g8_manifest_path.write_text(json.dumps(g8_result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    else:
        g8_manifest_path = report_dir / "g8-recovery-manifest.json"
    checks = _check(
        stage_id,
        store,
        apply=apply,
        migration_id=migration_id,
        epoch=epoch,
        require_services_drained=bool(stage_id == "G0" and stop_services),
        backup_evidence=backup,
        g6_manifest_path=report_dir / "g6-runtime-manifest.json",
        g7_manifest_path=report_dir / "g7-performance-manifest.json",
        g8_manifest_path=g8_manifest_path,
        g9_manifest_path=g9_review_manifest or (report_dir / "g9-independent-review-manifest.json"),
    )
    if all(item["status"] == "PASS" for item in checks):
        decision = "PASS_WITH_BASELINE" if stage_id == "G7" and g7_result and g7_result.get("decision") == "PASS_WITH_BASELINE" else "PASS"
    elif stage_id == "G4" and checks and all(item["status"] in {"PASS", "SKIPPED/HOLD"} for item in checks) and any(item["status"] == "SKIPPED/HOLD" for item in checks):
        decision = "PASS_WITH_SKIP"
    elif (
        stage_id == "G8"
        and checks
        and all(item["status"] in {"PASS", "SKIPPED/HOLD"} for item in checks)
        and {item["name"] for item in checks if item["status"] == "SKIPPED/HOLD"}
        == {"final structured watermarks"}
    ):
        decision = "PASS_WITH_SKIP"
    else:
        decision = "HOLD"
    report = {
        "schema": "pm-system.v45-r2-migration-report.v1",
        "stage_id": stage_id,
        "migration_id": migration_id,
        "migration_epoch": epoch,
        "owner": owner,
        "mode": mode,
        "lease": lease,
        "started_at": lease["acquired_at"],
        "finished_at": now_iso(),
        "decision": decision,
        "checks": checks,
        "snapshot": snapshot(store),
    }
    if drain is not None:
        report["g0_service_actions"] = drain
    if backup is not None:
        report["g0_backup"] = backup
    if g1_result is not None:
        report["g1_contracts"] = g1_result
    if g6_result is not None:
        report["g6_runtime"] = g6_result
    if g7_result is not None:
        report["g7_performance"] = g7_result
    if g8_result is not None:
        report["g8_recovery"] = g8_result
    if revalidation_input is not None:
        report["revalidation"] = revalidation_input
    report["manifest_hash"] = _hash(report)
    report_path = write_report(report_dir, report)
    # A failed stage remains HOLD and keeps its report/lease evidence. Release
    # only after the report is durable; a caller that wants to retry must make
    # that choice explicitly in a later invocation.
    store.release_migration_lease(lease_id=lease["lease_id"], state="released" if decision in {"PASS", "PASS_WITH_SKIP", "PASS_WITH_BASELINE"} else "hold")
    report["report_path"] = str(report_path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=STAGES)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--migration-id", default="v45-r2")
    parser.add_argument("--migration-epoch", default="v45-r2-20260830")
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--apply", action="store_true", help="perform the explicit stage mutation (G0 writes the persistent freeze)")
    parser.add_argument("--revalidate", action="store_true", help="explicitly re-run checks for the same stage while its persistent freeze remains active")
    parser.add_argument("--stop-services", action="store_true", help="G0 only: unload business LaunchAgents after the durable freeze")
    parser.add_argument("--backup-dir", type=Path, help="G0 online backup directory (defaults to ~/.codex/backups/v4.5-r2)")
    parser.add_argument("--g7-baseline-manifest", type=Path, help="G7 only: explicit accepted performance baseline manifest")
    parser.add_argument("--g9-review-manifest", type=Path, help="G9 only: independent review manifest with artifact/runtime hashes")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        report = run_stage(
            args.stage,
            db_path=args.db_path,
            report_dir=args.report_dir,
            migration_id=args.migration_id,
            epoch=args.migration_epoch,
            owner=args.owner,
            apply=args.apply,
            revalidate=args.revalidate,
            lease_seconds=args.lease_seconds,
            stop_services=args.stop_services,
            backup_dir=args.backup_dir,
            g7_baseline_manifest=args.g7_baseline_manifest,
            g9_review_manifest=args.g9_review_manifest,
        )
    except (MigrationLeaseConflict, MigrationFrozen, OSError, ValueError) as exc:
        print(json.dumps({"decision": "HOLD", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0 if report["decision"] in {"PASS", "PASS_WITH_SKIP", "PASS_WITH_BASELINE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
