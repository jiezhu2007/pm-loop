#!/usr/bin/env python3
"""V1.1 concept-domain migration and read-only admission checks.

This runner is deliberately separate from the V4.5 G0-G9 runner.  It owns
only additive concept-domain state in the existing PM SQLite database.  A
future shared-runtime change must use a new V4.5 migration/epoch and is not
performed by this module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_schema import (  # noqa: E402
    CONCEPT_SCHEMA_VERSION,
    CONCEPT_SCHEMA_ID,
    concept_tables,
    ddl_hash,
    get_admission,
    migrate_schema,
    record_model_policy,
    schema_state,
)
from concept_v11_schema_v2 import (  # noqa: E402
    TARGET_SCHEMA_ID,
    TARGET_SCHEMA_VERSION,
    migrate_schema_v2,
    schema_v2_state,
)
from pm_system_store import PMSystemStore, now_iso  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
DEFAULT_MANIFEST = DEFAULT_CONCEPT_ROOT / "state" / "source-manifest.json"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "migrations" / "concept-v11"
DEFAULT_EPOCH = "v45-r2-20260830"
DEFAULT_OWNER = f"codex-concept-v11:{os.getpid()}"
CONCEPT_POLICY_VERSION = "concept-v11-oneapi-auto-v1"
DEFAULT_RUNTIME_ROOT = Path.home() / ".codex" / "pm-loop" / "runtime"
DEFAULT_HEALTH_LATEST = Path.home() / ".codex" / "skills" / "system-health-check" / "state" / "latest.json"
HEALTH_MAX_STALE_SECONDS = 8 * 24 * 60 * 60
RUNTIME_HASH_FILES = (
    "pm_system_gateway.py",
    "pm_resource_dispatcher.py",
    "pm_system_worker.py",
)
HEALTH_LABELS = (
    "com.zhujie14.system-health-check",
    "com.zhujie14.system-health-heartbeat",
)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _safe_component(value: str) -> str:
    """Keep migration metadata in filenames without allowing path traversal."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip())
    return cleaned.strip("._") or "unknown"


def _copy_backup(
    db_path: Path,
    backup_root: Path,
    migration_id: str,
    *,
    stage_id: str,
    migration_epoch: str,
) -> Dict[str, Any]:
    """Create an immutable, independently addressable stage backup.

    A stage must never overwrite another stage's rollback point.  The
    migration and stage are part of the directory/path, while the UTC
    timestamp and nonce make repeated validations independently recoverable.
    """
    safe_migration = _safe_component(migration_id)
    safe_stage = _safe_component(stage_id)
    stage_root = backup_root / safe_migration
    stage_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = stage_root / f"{safe_stage}-{timestamp}-{uuid.uuid4().hex[:12]}.sqlite3"
    while destination.exists():
        destination = stage_root / f"{safe_stage}-{timestamp}-{uuid.uuid4().hex[:12]}.sqlite3"
    with tempfile.NamedTemporaryFile(
        prefix=f".{destination.name}.", suffix=".tmp", dir=stage_root, delete=False
    ) as temporary_file:
        temporary = Path(temporary_file.name)
    source: Optional[sqlite3.Connection] = None
    target: Optional[sqlite3.Connection] = None
    try:
        source = sqlite3.connect(str(db_path), timeout=10)
        target = sqlite3.connect(str(temporary), timeout=10)
        source.backup(target)
        target.commit()
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()
    try:
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    verify = sqlite3.connect(str(destination), timeout=10)
    try:
        integrity = str(verify.execute("PRAGMA integrity_check").fetchone()[0])
        schema = int(verify.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
    finally:
        verify.close()
    return {
        "path": str(destination),
        "migration_id": migration_id,
        "stage_id": stage_id,
        "migration_epoch": migration_epoch,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sha256": _file_hash(destination),
        "size_bytes": destination.stat().st_size,
        "integrity_check": integrity,
        "core_schema_version": schema,
        "verified": integrity == "ok",
    }


def _runtime_process_snapshot() -> Dict[str, Any]:
    """Collect process and orphan evidence without changing runtime state."""
    markers = {
        "control_plane": "pm_loop_control_plane_server.py",
        "worker": "pm_system_worker.py",
        "memory_watcher": "ov_memory_sync.py",
    }
    output = ""
    error = None
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,command="],
            capture_output=True,
            text=True,
            check=False,
        )
        output = completed.stdout or ""
        if completed.returncode != 0:
            error = (completed.stderr or "").strip() or f"ps exited {completed.returncode}"
    except OSError as exc:
        error = f"{type(exc).__name__}: {exc}"
    result: Dict[str, Any] = {}
    parsed = []
    for line in output.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            parsed.append({"pid": int(parts[0]), "ppid": int(parts[1]), "command": parts[2]})
        except ValueError:
            continue
    for name, marker in markers.items():
        rows = [item for item in parsed if marker in item["command"] and item["pid"] != os.getpid()]
        result[name] = {
            "count": len(rows),
            "commands": [f"{item['pid']} {item['command']}" for item in rows[:4]],
            "source": "ps",
            "error": error,
        }
    worker_pids = {item["pid"] for item in parsed if "pm_system_worker.py" in item["command"] and item["pid"] != os.getpid()}
    codex_exec = [
        item for item in parsed
        if re.search(r"(?:^|/)(?:codex|baidu-cx)(?:\s+exec|$)", item["command"])
        and item["pid"] != os.getpid()
    ]
    result["orphan_processes"] = [
        {"pid": item["pid"], "ppid": item["ppid"], "command": item["command"]}
        for item in codex_exec if item["ppid"] not in worker_pids
    ]
    result["codex_exec"] = {"count": len(codex_exec), "source": "ps", "error": error}
    return result


def _runtime_hash_snapshot(
    *, canonical_root: Path = PROJECT_ROOT, runtime_root: Path = DEFAULT_RUNTIME_ROOT
) -> Dict[str, Any]:
    """Verify the exact shared-runtime files used by the live services."""
    files: Dict[str, Any] = {}
    errors: list[str] = []
    for name in RUNTIME_HASH_FILES:
        canonical = canonical_root / "scripts" / name
        runtime = runtime_root / "scripts" / name
        canonical_hash = _file_hash(canonical) if canonical.is_file() else None
        runtime_hash = _file_hash(runtime) if runtime.is_file() else None
        match = bool(canonical_hash and runtime_hash and canonical_hash == runtime_hash)
        if not canonical.is_file():
            errors.append(f"missing_canonical:{name}")
        if not runtime.is_file():
            errors.append(f"missing_runtime:{name}")
        elif canonical_hash != runtime_hash:
            errors.append(f"runtime_drift:{name}")
        files[name] = {
            "canonical": str(canonical),
            "runtime": str(runtime),
            "canonical_sha256": canonical_hash,
            "runtime_sha256": runtime_hash,
            "match": match,
        }
    return {"status": "PASS" if not errors else "HOLD", "files": files, "errors": errors}


def _launchd_loaded(label: str) -> Optional[bool]:
    try:
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.returncode == 0


def _heartbeat_snapshot(path: Path = DEFAULT_HEALTH_LATEST) -> Dict[str, Any]:
    """Read the independent liveness marker; never runs the checker."""
    errors: list[str] = []
    exists = path.is_file()
    mtime = path.stat().st_mtime if exists else None
    age_seconds = max(0.0, time.time() - mtime) if mtime is not None else None
    payload = _read_json(path, None) if exists else None
    if not exists:
        errors.append("health_marker_missing")
    elif not isinstance(payload, dict):
        errors.append("health_marker_invalid")
    elif age_seconds is not None and age_seconds > HEALTH_MAX_STALE_SECONDS:
        errors.append("health_marker_stale")
    return {
        "status": "PASS" if not errors else "HOLD",
        "path": str(path),
        "exists": exists,
        "mtime": datetime.fromtimestamp(mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if mtime is not None else None,
        "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "max_stale_seconds": HEALTH_MAX_STALE_SECONDS,
        "run_at": payload.get("run_at") if isinstance(payload, dict) else None,
        "checker_errors": payload.get("checker_errors") if isinstance(payload, dict) else None,
        "launchd": {label: _launchd_loaded(label) for label in HEALTH_LABELS},
        "errors": errors,
    }


def _orphan_snapshot(connection: sqlite3.Connection) -> Dict[str, Any]:
    """Find active records whose owning row/process is missing."""
    result: Dict[str, Any] = {}
    result["execution_slots"] = int(connection.execute(
        "SELECT COUNT(*) FROM execution_slots WHERE status='leased' AND (run_id IS NULL OR lease_id IS NULL)"
    ).fetchone()[0])
    result["semantic_tasks"] = int(connection.execute(
        "SELECT COUNT(*) FROM semantic_tasks s LEFT JOIN outbox_items o ON o.outbox_id=s.outbox_id "
        "WHERE s.status IN ('queued','in_flight','accepted','processing','retry_wait') AND o.outbox_id IS NULL"
    ).fetchone()[0])
    result["dispatch_leases"] = int(connection.execute(
        "SELECT COUNT(*) FROM outbox_dispatch_leases l LEFT JOIN outbox_items o ON o.outbox_id=l.outbox_id "
        "WHERE o.outbox_id IS NULL"
    ).fetchone()[0])
    result["model_calls"] = int(connection.execute(
        "SELECT COUNT(*) FROM model_calls m LEFT JOIN runs r ON r.run_id=m.run_id "
        "WHERE r.run_id IS NULL"
    ).fetchone()[0])
    result["total"] = sum(int(value) for value in result.values())
    return result


def foundation_check(
    store: PMSystemStore,
    *,
    expected_epoch: str,
    runtime_root: Path = DEFAULT_RUNTIME_ROOT,
    canonical_root: Path = PROJECT_ROOT,
) -> Dict[str, Any]:
    """Read the current runtime and concept schema without changing state."""
    freeze = store.migration_freeze() or {}
    with store.connect() as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        core_schema = store.schema_version()
        active = {}
        for table, statuses in {
            "jobs": ("queued", "running", "retry_wait"),
            "runs": ("queued", "running", "retry_wait"),
            "outbox_items": ("pending", "in_flight", "retry_wait"),
            "semantic_tasks": ("queued", "in_flight", "accepted", "processing", "retry_wait"),
        }.items():
            placeholders = ",".join("?" for _ in statuses)
            active[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE status IN ({placeholders})", statuses).fetchone()[0])
        leases = {
            "migration": int(connection.execute("SELECT COUNT(*) FROM migration_leases WHERE state='active'").fetchone()[0]),
            "dispatch": int(connection.execute("SELECT COUNT(*) FROM outbox_dispatch_leases").fetchone()[0]),
            "provider_probe": int(connection.execute("SELECT COUNT(*) FROM provider_probe_leases").fetchone()[0]),
            "provider_token": int(connection.execute("SELECT COUNT(*) FROM provider_tokens WHERE released_at IS NULL").fetchone()[0]),
        }
        orphan = _orphan_snapshot(connection)
    schema = schema_state(store)
    with store.connect() as connection:
        concept_meta_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='concept_schema_meta'"
        ).fetchone() is not None
        concept_admissions_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='concept_admissions'"
        ).fetchone() is not None
        concept_namespace_epochs = sorted({
            str(row[0]) for row in connection.execute("SELECT namespace_epoch FROM concept_admissions").fetchall()
        }) if concept_admissions_exists else []
    schema_v2 = schema_v2_state(store) if concept_meta_exists else {
        "schema_version": 0,
        "schema_id": None,
        "migration_id": None,
        "migration_epoch": None,
        "ddl_sha256": None,
        "hot_projection_composite_key": False,
        "admission_events": 0,
        "legacy_provenance_rows": {},
        "tables": {},
    }
    runtime_hashes = _runtime_hash_snapshot(canonical_root=canonical_root, runtime_root=runtime_root)
    processes = _runtime_process_snapshot()
    heartbeat = _heartbeat_snapshot()
    errors = []
    if integrity != "ok":
        errors.append("core_integrity_not_ok")
    if core_schema < 7:
        errors.append(f"core_schema<{7}")
    if str(freeze.get("state") or "") != "released":
        errors.append("v4_runtime_not_released")
    if str(freeze.get("migration_epoch") or "") != expected_epoch:
        errors.append("runtime_epoch_mismatch")
    if leases["migration"] or leases["dispatch"] or leases["provider_probe"] or leases["provider_token"]:
        errors.append("active_lease_or_provider_token")
    if runtime_hashes["errors"]:
        errors.append("runtime_hash_mismatch")
    if orphan["total"]:
        errors.append("orphan_record_detected")
    if processes.get("orphan_processes"):
        errors.append("orphan_process_detected")
    if heartbeat["errors"]:
        errors.append("heartbeat_not_fresh")
    return {
        "status": "PASS" if not errors else "HOLD",
        "observed_at": now_iso(),
        "expected_runtime_epoch": expected_epoch,
        "core_schema_version": core_schema,
        "integrity_check": integrity,
        "migration_freeze": freeze,
        "active": active,
        "leases": leases,
        "runtime_processes": processes,
        "runtime_hashes": runtime_hashes,
        "orphan": orphan,
        "heartbeat": heartbeat,
        "concept_schema": schema,
        "concept_schema_v2": schema_v2,
        "concept_namespace_epochs": concept_namespace_epochs,
        "errors": errors,
    }


def _concept_id(name: str) -> str:
    return "concept-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:24]


def _map_status(row: Mapping[str, Any]) -> str:
    status = str(row.get("status") or "").lower()
    if status == "mapped":
        return "mapped"
    if status == "conflict":
        return "quarantined"
    return "quarantined" if status else "unknown"


def import_legacy(
    store: PMSystemStore,
    *,
    concept_root: Path,
    manifest_path: Path,
    namespace_epoch: str,
    migration_id: str,
    owner: str,
) -> Dict[str, Any]:
    """Import existing file-backed concepts as immutable historical versions."""
    ledger_path = concept_root / "state" / "concepts-ledger.json"
    ledger = _read_json(ledger_path, {})
    manifest = _read_json(manifest_path, {})
    if not isinstance(ledger, dict):
        raise RuntimeError(f"invalid concepts ledger: {ledger_path}")
    if not isinstance(manifest, dict):
        raise RuntimeError(f"invalid source manifest: {manifest_path}")
    checks = [row for row in manifest.get("active_source_checks", []) if isinstance(row, dict)]
    now = now_iso()
    generation_id = f"legacy-import-{namespace_epoch}"
    imported = 0
    missing_pages = []
    map_counts = {"mapped": 0, "quarantined": 0, "unknown": 0}
    with store.transaction() as connection:
        admission = connection.execute("SELECT admission_state FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)).fetchone()
        if admission is None or admission[0] != "disabled":
            raise RuntimeError("legacy import requires concept_admission=disabled")
        version_columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(concept_versions)").fetchall()}
        publish_columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(concept_publish_ledger)").fetchall()}
        hot_columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(concept_hot_projection)").fetchall()}
        has_provenance = "provenance" in version_columns and "provenance" in publish_columns and "provenance" in hot_columns
        hot_conflict = "ON CONFLICT(concept_id,namespace_epoch)" if len(hot_columns) and any(
            int(row[5]) == 2 and str(row[1]) == "namespace_epoch"
            for row in connection.execute("PRAGMA table_info(concept_hot_projection)").fetchall()
        ) else "ON CONFLICT(concept_id)"
        for name, record in sorted(ledger.items(), key=lambda item: str(item[0])):
            if not isinstance(record, dict):
                continue
            concept_id = _concept_id(str(name))
            page_path = concept_root / "state" / "pages" / f"{name}.md"
            if not page_path.is_file():
                missing_pages.append(str(name))
                continue
            content = page_path.read_text(encoding="utf-8")
            content_hash = "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
            version = str(record.get("current_version") or record.get("latest_version") or "legacy")
            version_id = f"version-{hashlib.sha256((concept_id + version + content_hash).encode('utf-8')).hexdigest()[:24]}"
            evidence_hash = _hash({"concept": name, "sources": record.get("sources") or []})
            if has_provenance:
                connection.execute(
                    "INSERT OR IGNORE INTO concept_versions(version_id,concept_id,namespace_epoch,version,generation_id,content,content_hash,source_snapshot_hash,evidence_set_hash,compiler_version,policy_version,status,provenance,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (version_id, concept_id, namespace_epoch, version, generation_id, content, content_hash, None, evidence_hash, "legacy-import", None, "active", "legacy_import", now),
                )
            else:
                connection.execute(
                    "INSERT OR IGNORE INTO concept_versions(version_id,concept_id,namespace_epoch,version,generation_id,content,content_hash,source_snapshot_hash,evidence_set_hash,compiler_version,policy_version,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (version_id, concept_id, namespace_epoch, version, generation_id, content, content_hash, None, evidence_hash, "legacy-import", None, "active", now),
                )
            publish_id = f"publish-{hashlib.sha256((concept_id + version_id).encode('utf-8')).hexdigest()[:24]}"
            if has_provenance:
                connection.execute(
                    "INSERT OR IGNORE INTO concept_publish_ledger(publish_id,concept_id,namespace_epoch,version_id,previous_generation,current_generation,current_hot_generation,desired_hot_generation,projection_state,projection_outbox_id,operator,evidence_hash,provenance,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (publish_id, concept_id, namespace_epoch, version_id, None, generation_id, generation_id, generation_id, "legacy_imported", None, owner, evidence_hash, "legacy_import", now, now),
                )
                connection.execute(
                    f"INSERT INTO concept_hot_projection(concept_id,namespace_epoch,generation_id,projection_state,outbox_item_id,observed_content_hash,observed_at,provenance,updated_at) VALUES(?,?,?,?,?,?,?,?,?) {hot_conflict} DO UPDATE SET namespace_epoch=excluded.namespace_epoch,generation_id=excluded.generation_id,projection_state=excluded.projection_state,observed_content_hash=excluded.observed_content_hash,observed_at=excluded.observed_at,provenance=excluded.provenance,updated_at=excluded.updated_at",
                    (concept_id, namespace_epoch, generation_id, "legacy_imported", None, content_hash, now, "legacy_import", now),
                )
            else:
                connection.execute(
                    "INSERT OR IGNORE INTO concept_publish_ledger(publish_id,concept_id,namespace_epoch,version_id,previous_generation,current_generation,current_hot_generation,desired_hot_generation,projection_state,projection_outbox_id,operator,evidence_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (publish_id, concept_id, namespace_epoch, version_id, None, generation_id, generation_id, generation_id, "applied", None, owner, evidence_hash, now, now),
                )
                connection.execute(
                    f"INSERT INTO concept_hot_projection(concept_id,namespace_epoch,generation_id,projection_state,outbox_item_id,observed_content_hash,observed_at,updated_at) VALUES(?,?,?,?,?,?,?,?) {hot_conflict} DO UPDATE SET namespace_epoch=excluded.namespace_epoch,generation_id=excluded.generation_id,projection_state=excluded.projection_state,observed_content_hash=excluded.observed_content_hash,observed_at=excluded.observed_at,updated_at=excluded.updated_at",
                    (concept_id, namespace_epoch, generation_id, "applied", None, content_hash, now, now),
                )
            imported += 1
        for index, row in enumerate(checks):
            concept = str(row.get("concept") or "")
            source_uri = str(row.get("source_uri") or "")
            if not concept or not source_uri:
                continue
            status = _map_status(row)
            map_counts[status] = map_counts.get(status, 0) + 1
            map_id = "map-" + hashlib.sha256((namespace_epoch + concept + source_uri).encode("utf-8")).hexdigest()[:24]
            source_ids = list(row.get("matched_source_ids") or [])
            evidence_hash = _hash({"source_uri": source_uri, "matched_source_ids": source_ids, "matched_paths": row.get("matched_paths") or []})
            connection.execute(
                "INSERT OR IGNORE INTO concept_source_map(map_id,concept_id,namespace_epoch,source_id,source_uri,leaf_uri,identity_method,status,confidence,conflict_set_id,owner,evidence_refs_json,evidence_set_hash,next_action,expires_at,lineage_json,resolved_at,resolved_by,resolution_reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (map_id, _concept_id(concept), namespace_epoch, source_ids[0] if source_ids else "unknown", source_uri, None, str(row.get("match_mode") or "evidence_match"), status, None, None, owner if status != "mapped" else None, json.dumps(source_ids, ensure_ascii=False), evidence_hash, "review_source_map" if status != "mapped" else None, None, json.dumps({"migration_id": migration_id, "source_index": index}, ensure_ascii=False), now if status == "mapped" else None, owner if status == "mapped" else None, "legacy_source_manifest", now, now),
            )
    return {
        "status": "PASS" if not missing_pages else "PASS_WITH_WARNINGS",
        "migration_id": migration_id,
        "namespace_epoch": namespace_epoch,
        "generation_id": generation_id,
        "imported_concepts": imported,
        "missing_pages": missing_pages,
        "source_map_counts": map_counts,
        "source_manifest_hash": _file_hash(manifest_path) if manifest_path.is_file() else None,
    }


def write_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("foundation-check", "schema", "schema-v2", "legacy-import"))
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--migration-id", default="concept-v11-20260831")
    parser.add_argument("--migration-epoch", default=DEFAULT_EPOCH)
    parser.add_argument("--runtime-epoch", help="expected current shared-runtime epoch; defaults to the active fence")
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--apply", action="store_true", help="required for schema/import mutations")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    db_path = args.db_path.expanduser().resolve()
    store = PMSystemStore(db_path)
    active_runtime_epoch = str(args.runtime_epoch or ((store.migration_freeze() or {}).get("migration_epoch") or args.migration_epoch))
    if args.command == "foundation-check":
        result = foundation_check(store, expected_epoch=active_runtime_epoch)
        output = {"schema": "concept-v11.foundation-check.v1", "result": result}
        if args.report:
            write_manifest(args.report, output)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0 if result["status"] == "PASS" else 1
    if not args.apply:
        raise SystemExit("schema, schema-v2 and legacy-import require --apply")
    before = foundation_check(store, expected_epoch=active_runtime_epoch)
    if before["status"] != "PASS":
        output = {"schema": "concept-v11.migration.v1", "status": "HOLD", "before": before, "errors": ["foundation_check_failed"]}
        if args.report:
            write_manifest(args.report, output)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 1
    stage_id = {
        "schema": "C-SCHEMA",
        "schema-v2": "C-SCHEMA-V2",
        "legacy-import": "C-LEGACY-IMPORT",
    }[args.command]
    lease = store.acquire_migration_lease(
        migration_id=str(args.migration_id),
        stage_id=stage_id,
        migration_epoch=str(args.migration_epoch),
        owner=str(args.owner),
        lease_seconds=int(args.lease_seconds),
    )
    try:
        backup = _copy_backup(
            db_path,
            args.backup_root,
            str(args.migration_id),
            stage_id=stage_id,
            migration_epoch=str(args.migration_epoch),
        )
        if args.command == "schema":
            result = migrate_schema(
                store,
                migration_id=str(args.migration_id),
                migration_epoch=str(args.migration_epoch),
                owner=str(args.owner),
                lease_id=str(lease["lease_id"]),
            )
            result["policy"] = record_model_policy(
                store,
                {
                    "policy_version": CONCEPT_POLICY_VERSION,
                    "provider": "oneapi",
                    "requested_model": "auto",
                    "allowed_models": [],
                    "capability_class": "concept-compiler-and-semantic",
                    "privacy_scope": "local-private",
                    "latency_limit_seconds": 900,
                },
            )
        elif args.command == "schema-v2":
            result = migrate_schema_v2(
                store,
                migration_id=str(args.migration_id),
                migration_epoch=str(args.migration_epoch),
                owner=str(args.owner),
                lease_id=str(lease["lease_id"]),
            )
        else:
            result = import_legacy(
                store,
                concept_root=args.concept_root.expanduser().resolve(),
                manifest_path=args.manifest.expanduser().resolve(),
                namespace_epoch=str(args.migration_epoch),
                migration_id=str(args.migration_id),
                owner=str(args.owner),
            )
    finally:
        released = store.release_migration_lease(lease_id=str(lease["lease_id"]))
        lease["state"] = "released" if released else "release_unknown"
        if released:
            lease["released_at"] = now_iso()
    after = foundation_check(store, expected_epoch=active_runtime_epoch)
    output = {
        "schema": "concept-v11.migration.v1",
        "status": "PASS" if after["status"] == "PASS" else "HOLD",
        "command": args.command,
        "migration_id": args.migration_id,
        "migration_epoch": args.migration_epoch,
        "runtime_epoch": active_runtime_epoch,
        "stage_lease": lease,
        "backup": backup,
        "before": before,
        "result": result,
        "after": after,
        "concept_schema_version": CONCEPT_SCHEMA_VERSION,
        "concept_schema_id": CONCEPT_SCHEMA_ID,
        "concept_target_schema_version": TARGET_SCHEMA_VERSION,
        "concept_target_schema_id": TARGET_SCHEMA_ID,
        "concept_schema_v2": schema_v2_state(store),
        "ddl_sha256": ddl_hash(),
        "admission": get_admission(store, str(args.migration_epoch)),
    }
    if args.report:
        write_manifest(args.report, output)
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
