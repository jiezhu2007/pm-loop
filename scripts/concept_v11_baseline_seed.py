#!/usr/bin/env python3
"""Seed or roll an auditable Active Generation for the concept domain.

The runner is intentionally conservative.  A normal invocation is a dry-run
and performs no database write.  ``--apply`` is the explicit production-write
boundary: it creates an independently verifiable SQLite backup, then updates
the existing legacy concept rows and their current projection pointers in one
short transaction.  It never calls OpenViking or a model provider.

The normal mode creates the first baseline.  The explicit ``--replace-active``
mode performs a baseline roll after an approved, deterministic page rebuild:
it never overwrites a Version, and it requires the same disabled Admission,
full coverage, empty runtime and backup checks as the initial seed.  The input
coverage report must be the current, human-reviewed P3 report with 45/45
coverage closed.  A partial catalog is rejected; it cannot become a baseline
generation by accident.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from concept_v11_admission import backup_database  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


SCHEMA = "concept-v11.baseline-seed.v1"
REPORT_SCHEMA = "concept-v11.source-coverage-report.v1"
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
DEFAULT_COVERAGE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "concept-backups"
DEFAULT_NAMESPACE = "v45-r2-20260830"
STAGE_ID = "P5.5-BASELINE-SEED"
EXPECTED_STATUSES = {"refreshable", "substituted", "retired_with_evidence"}
ACTIVE_WORK_STATUSES = {
    "jobs": ("queued", "running", "processing", "active", "retry_wait"),
    "runs": ("queued", "running", "processing", "active", "retry_wait"),
    "outbox_items": ("pending", "in_flight", "dispatching", "processing", "active", "retry_wait"),
    "semantic_tasks": ("queued", "in_flight", "accepted", "processing", "active", "retry_wait"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _concept_id(name: str) -> str:
    return "concept-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:24]


def _is_legacy_version(row: Mapping[str, Any]) -> bool:
    """A legacy import may legitimately predate the current concept page."""
    provenance = str(row.get("provenance") or "")
    generation_id = str(row.get("generation_id") or "")
    compiler_version = str(row.get("compiler_version") or "")
    return (
        provenance == "legacy_import"
        or generation_id.startswith("legacy-import-")
        or compiler_version == "legacy-import"
    )


def _next_version_label(
    connection: sqlite3.Connection,
    concept_id: str,
    namespace_epoch: str,
) -> str:
    """Choose a deterministic free vN label after the historical versions."""
    rows = connection.execute(
        "SELECT version FROM concept_versions WHERE concept_id=? AND namespace_epoch=?",
        (concept_id, namespace_epoch),
    ).fetchall()
    used = {str(row[0] or "") for row in rows}
    numeric = [
        int(match.group(1))
        for value in used
        if (match := re.fullmatch(r"v(\d+)", value)) is not None
    ]
    candidate = max(numeric or [0]) + 1
    while f"v{candidate}" in used:
        candidate += 1
    return f"v{candidate}"


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON: {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"JSON object required: {path}")
    return value


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _record_active_generation_watermark(
    connection: sqlite3.Connection,
    *,
    plan: Mapping[str, Any],
    captured_at: int,
) -> dict[str, Any]:
    """Atomically bind the PM runtime watermark to the newly active Generation."""
    if not _table_exists(connection, "watermarks") or not _table_exists(connection, "watermark_events"):
        raise RuntimeError("generation_watermark_tables_missing")
    value = {
        "domain": "concepts",
        "generation_id": str(plan["generation_id"]),
        "generation_hash": str(plan["generation_hash"]),
        "source_watermark": str(plan.get("source_manifest_hash") or ""),
        "knowledge_watermark": str(plan.get("coverage_report_hash") or ""),
    }
    value_text = _canonical(value)
    value_hash = hashlib.sha256(value_text.encode("utf-8")).hexdigest()
    source_domain = "pm-runtime"
    watermark_name = "active_generation"
    producer = "concept-v11-baseline-seed"
    sequence = 0
    current = connection.execute(
        "SELECT captured_at,sequence,value_hash FROM watermarks WHERE source_domain=? AND watermark_name=?",
        (source_domain, watermark_name),
    ).fetchone()
    current_cursor = (int(current[0]), int(current[1])) if current is not None else None
    cursor = (captured_at, sequence)
    if current_cursor is not None and cursor <= current_cursor:
        raise RuntimeError("active_generation_watermark_cursor_not_advanced")
    connection.execute(
        "INSERT INTO watermarks(source_domain,watermark_name,captured_at,sequence,value_hash,value,producer,state) VALUES(?,?,?,?,?,?,?,?) "
        "ON CONFLICT(source_domain,watermark_name) DO UPDATE SET captured_at=excluded.captured_at,sequence=excluded.sequence,value_hash=excluded.value_hash,value=excluded.value,producer=excluded.producer,state=excluded.state",
        (source_domain, watermark_name, captured_at, sequence, value_hash, value_text, producer, "accepted"),
    )
    connection.execute(
        "INSERT INTO watermark_events(source_domain,watermark_name,captured_at,sequence,value_hash,state,observed_at,details_json) VALUES(?,?,?,?,?,?,?,?)",
        (source_domain, watermark_name, captured_at, sequence, value_hash, "accepted", _now(), _canonical({"producer": producer, "value": value_text})),
    )
    return value


def _validate_coverage(report: Mapping[str, Any], expected_count: int) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    if str(report.get("schema") or "") != REPORT_SCHEMA:
        errors.append("coverage_schema_invalid")
    if str(report.get("status") or "") != "PASS":
        errors.append(f"coverage_not_pass:{report.get('status') or 'missing'}")
    gate = report.get("gate")
    if not isinstance(gate, Mapping) or gate.get("p3_closed") is not True:
        errors.append("coverage_gate_not_closed")
    if not str(report.get("report_hash") or "").startswith("sha256:"):
        errors.append("coverage_report_hash_missing")
    if not str(report.get("source_manifest_hash") or "").startswith("sha256:"):
        errors.append("coverage_source_manifest_hash_missing")
    if int(report.get("expected_concept_count") or 0) != expected_count:
        errors.append("coverage_expected_count_mismatch")
    concepts = report.get("concepts")
    if not isinstance(concepts, list):
        return [], errors + ["coverage_concepts_missing"]
    if len(concepts) != expected_count or int(report.get("concept_count") or 0) != expected_count:
        errors.append(f"coverage_concept_count_mismatch:{len(concepts)}!={expected_count}")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(concepts):
        if not isinstance(raw, Mapping):
            errors.append(f"coverage_concept_invalid:{index}")
            continue
        name = str(raw.get("concept") or "").strip()
        concept_id = str(raw.get("concept_id") or "")
        status = str(raw.get("coverage_status") or "")
        if not name:
            errors.append(f"coverage_concept_name_missing:{index}")
            continue
        if name in seen:
            errors.append(f"coverage_concept_duplicate:{name}")
        seen.add(name)
        if concept_id != _concept_id(name):
            errors.append(f"coverage_concept_id_mismatch:{name}")
        if status not in EXPECTED_STATUSES:
            errors.append(f"coverage_concept_not_closed:{name}:{status or 'missing'}")
        refs = raw.get("references")
        if not isinstance(refs, list) or not refs:
            errors.append(f"coverage_references_missing:{name}")
        else:
            retirement = raw.get("retirement")
            if status == "retired_with_evidence":
                if not isinstance(retirement, Mapping):
                    errors.append(f"coverage_retirement_evidence_missing:{name}")
                elif str(retirement.get("decision") or "") != "retired_with_evidence":
                    errors.append(f"coverage_retirement_decision_invalid:{name}")
                elif not str(retirement.get("retirement_content_sha256") or "").startswith("sha256:"):
                    errors.append(f"coverage_retirement_hash_missing:{name}")
            current_source_present = False
            for ref_index, ref in enumerate(refs):
                if not isinstance(ref, Mapping):
                    errors.append(f"coverage_reference_invalid:{name}:{ref_index}")
                    continue
                disposition = str(ref.get("disposition") or "")
                if disposition in {"mapped", "substituted"}:
                    current_source_present = True
                elif disposition == "historical_exclusion":
                    pass
                elif disposition != "retired_with_evidence":
                    errors.append(f"coverage_reference_not_closed:{name}:{ref_index}")
                if not str(ref.get("evidence_set_hash") or "").startswith("sha256:"):
                    errors.append(f"coverage_reference_evidence_missing:{name}:{ref_index}")
            # A historical exclusion is a retained audit record, not current
            # coverage. Refreshable concepts need at least one independently
            # verified current source; retired concepts are closed by their
            # tombstone above and may retain only historical references.
            if status in {"refreshable", "substituted"} and not current_source_present:
                errors.append(f"coverage_current_source_missing:{name}")
        normalized.append(dict(raw))
    if len(seen) != expected_count:
        errors.append(f"coverage_unique_concept_count_mismatch:{len(seen)}!={expected_count}")
    return sorted(normalized, key=lambda item: str(item.get("concept") or "")), errors


def _active_work(connection: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, statuses in ACTIVE_WORK_STATUSES.items():
        marks = ",".join("?" for _ in statuses)
        counts[table] = int(connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE status IN ({marks})", statuses
        ).fetchone()[0]) if _table_exists(connection, table) else 0
    for key, table, where in (
        ("slots", "execution_slots", "status <> 'free'"),
        ("tokens", "provider_tokens", "released_at IS NULL"),
        ("migration_leases", "migration_leases", "state='active'"),
        ("dispatch_leases", "outbox_dispatch_leases", "1=1"),
        ("probe_leases", "provider_probe_leases", "1=1"),
    ):
        counts[key] = int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]) if _table_exists(connection, table) else 0
    return counts


def _build_plan(
    db_path: Path,
    concept_root: Path,
    coverage_path: Path,
    namespace_epoch: str,
    expected_count: int,
    allow_active_generation_replacement: bool = False,
) -> dict[str, Any]:
    report = _read_json(coverage_path)
    concepts, errors = _validate_coverage(report, expected_count)
    ledger_path = concept_root / "state" / "concepts-ledger.json"
    ledger: Mapping[str, Any] = {}
    if not ledger_path.is_file():
        errors.append("concept_ledger_missing")
    else:
        value = _read_json(ledger_path)
        ledger = value
    page_records: list[dict[str, Any]] = []
    for item in concepts:
        name = str(item.get("concept") or "")
        page = (concept_root / "state" / "pages" / f"{name}.md").resolve()
        if not page.is_file():
            errors.append(f"concept_page_missing:{name}")
            continue
        record = ledger.get(name) if isinstance(ledger, Mapping) else None
        if not isinstance(record, Mapping) or str(record.get("status") or "active") != "active":
            errors.append(f"concept_ledger_not_active:{name}")
        content = page.read_text(encoding="utf-8")
        page_hash = "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
        refs = item.get("references") if isinstance(item.get("references"), list) else []
        evidence = _hash({
            "concept": name,
            "coverage_report_hash": report.get("report_hash"),
            "references": [
                {
                    "map_id": ref.get("map_id"),
                    "source_uri": ref.get("source_uri"),
                    "disposition": ref.get("disposition"),
                    "evidence_set_hash": ref.get("evidence_set_hash"),
                    "ledger_entry_id": ref.get("ledger_entry_id"),
                }
                for ref in refs if isinstance(ref, Mapping)
            ],
        })
        page_records.append({
            "concept": name,
            "concept_id": _concept_id(name),
            "page_hash": page_hash,
            "coverage_status": item.get("coverage_status"),
            "evidence_set_hash": evidence,
        })
    if len(page_records) != expected_count:
        errors.append(f"page_record_count_mismatch:{len(page_records)}!={expected_count}")

    generation_hash = _hash({
        "schema": SCHEMA,
        "namespace_epoch": namespace_epoch,
        "coverage_report_hash": report.get("report_hash"),
        "source_manifest_hash": report.get("source_manifest_hash"),
        "members": page_records,
    })
    generation_id = "generation-concept-baseline-" + generation_hash.split(":", 1)[1][:24]
    db_state: dict[str, Any] = {"integrity": None, "admission": None, "active_work": {}, "active_generation": None, "members": []}
    if not db_path.is_file():
        errors.append("database_missing")
    else:
        uri = f"file:{db_path.resolve()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=3) as connection:
            connection.row_factory = sqlite3.Row
            db_state["integrity"] = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            if db_state["integrity"] != "ok":
                errors.append("database_integrity_not_ok")
            required = ("concept_admissions", "concept_versions", "concept_hot_projection", "concept_publish_ledger", "generations")
            for table in required:
                if not _table_exists(connection, table):
                    errors.append(f"table_missing:{table}")
            if _table_exists(connection, "concept_admissions"):
                row = connection.execute("SELECT * FROM concept_admissions WHERE namespace_epoch=?", (namespace_epoch,)).fetchone()
                db_state["admission"] = dict(row) if row else None
                if row is None:
                    errors.append("admission_missing")
                elif str(row[1]) != "disabled":
                    errors.append(f"admission_not_disabled:{row[1]}")
            db_state["active_work"] = _active_work(connection)
            for key, value in db_state["active_work"].items():
                if int(value) != 0:
                    errors.append(f"active_work_present:{key}:{value}")
            if _table_exists(connection, "migration_freeze"):
                freeze = connection.execute("SELECT state FROM migration_freeze WHERE freeze_id=1").fetchone()
                if freeze is not None and str(freeze[0]).lower() != "released":
                    errors.append(f"runtime_fence_not_released:{freeze[0]}")
            if _table_exists(connection, "generations"):
                active = connection.execute("SELECT generation_id,generation_hash,status FROM generations WHERE domain='concepts' AND status='active'").fetchall()
                db_state["active_generation"] = [dict(row) for row in active]
                if active:
                    if not any(str(row[0]) == generation_id and str(row[1]) == generation_hash for row in active):
                        if not allow_active_generation_replacement:
                            errors.append("active_generation_already_exists")
                        elif len(active) != 1:
                            errors.append(f"active_generation_not_unique:{len(active)}")
            if not _table_exists(connection, "watermarks") or not _table_exists(connection, "watermark_events"):
                errors.append("generation_watermark_tables_missing")
            if all(_table_exists(connection, table) for table in ("concept_versions", "concept_hot_projection", "concept_publish_ledger")):
                rows = connection.execute(
                    "SELECT v.concept_id,v.version_id,v.version,v.content,v.content_hash,v.generation_id,v.evidence_set_hash,v.source_snapshot_hash,v.compiler_version,v.provenance,v.status, h.generation_id AS hot_generation, p.current_generation AS ledger_generation "
                    "FROM concept_versions v LEFT JOIN concept_hot_projection h ON h.concept_id=v.concept_id AND h.namespace_epoch=v.namespace_epoch "
                    "LEFT JOIN concept_publish_ledger p ON p.concept_id=v.concept_id AND p.namespace_epoch=v.namespace_epoch "
                    "AND p.rowid=(SELECT p2.rowid FROM concept_publish_ledger p2 WHERE p2.concept_id=v.concept_id AND p2.namespace_epoch=v.namespace_epoch ORDER BY p2.updated_at DESC,p2.created_at DESC,p2.rowid DESC LIMIT 1) "
                    "WHERE v.namespace_epoch=? AND v.status='active' ORDER BY v.concept_id", (namespace_epoch,)
                ).fetchall()
                db_state["members"] = [dict(row) for row in rows]
                if len(rows) != expected_count:
                    errors.append(f"active_version_count_mismatch:{len(rows)}!={expected_count}")
                rows_by_id: dict[str, list[sqlite3.Row]] = {}
                for row in rows:
                    rows_by_id.setdefault(str(row["concept_id"]), []).append(row)
                for concept_id, concept_rows in rows_by_id.items():
                    if len(concept_rows) != 1:
                        errors.append(f"active_version_duplicate:{concept_id}:{len(concept_rows)}")
                hot_count = int(connection.execute(
                    "SELECT COUNT(*) FROM concept_hot_projection WHERE namespace_epoch=?", (namespace_epoch,)
                ).fetchone()[0])
                if hot_count != expected_count:
                    errors.append(f"concept_hot_projection_count_mismatch:{hot_count}!={expected_count}")
                publish_count = int(connection.execute(
                    "SELECT COUNT(DISTINCT concept_id) FROM concept_publish_ledger WHERE namespace_epoch=?", (namespace_epoch,)
                ).fetchone()[0])
                if publish_count != expected_count:
                    errors.append(f"concept_publish_ledger_concept_count_mismatch:{publish_count}!={expected_count}")
                expected_ids = {item["concept_id"] for item in page_records}
                actual_ids = {str(row[0]) for row in rows}
                for missing in sorted(expected_ids - actual_ids):
                    errors.append(f"active_version_missing:{missing}")
                for extra in sorted(actual_ids - expected_ids):
                    errors.append(f"active_version_unexpected:{extra}")
                expected_by_id = {item["concept_id"]: item for item in page_records}
                for concept_id, expected in expected_by_id.items():
                    concept_rows = rows_by_id.get(concept_id, [])
                    if len(concept_rows) != 1:
                        continue
                    row = concept_rows[0]
                    expected["current_version_id"] = str(row["version_id"])
                    expected["current_version"] = str(row["version"] or "")
                    expected["current_content_hash"] = str(row["content_hash"] or "")
                    expected["version_action"] = "reuse_current"
                    expected["target_version_id"] = str(row["version_id"])
                    expected["target_version"] = str(row["version"] or "")
                    if str(row["content_hash"] or "") != str(expected["page_hash"]):
                        if not _is_legacy_version(dict(row)):
                            errors.append(f"page_hash_mismatch:{concept_id}")
                        else:
                            matching = connection.execute(
                                "SELECT version_id,version,status FROM concept_versions "
                                "WHERE concept_id=? AND namespace_epoch=? AND content_hash=? "
                                "ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END,rowid DESC LIMIT 1",
                                (concept_id, namespace_epoch, str(expected["page_hash"])),
                            ).fetchone()
                            expected["legacy_content_hash"] = str(row["content_hash"] or "")
                            if matching is not None:
                                expected["version_action"] = "activate_existing"
                                expected["target_version_id"] = str(matching[0])
                                expected["target_version"] = str(matching[1] or "")
                            else:
                                expected["version_action"] = "append_current"
                                expected["target_version"] = _next_version_label(connection, concept_id, namespace_epoch)
                                expected["target_version_id"] = "version-" + hashlib.sha256(
                                    (concept_id + str(expected["target_version"]) + str(expected["page_hash"])).encode("utf-8")
                                ).hexdigest()[:24]
                    if not str(row["hot_generation"] or ""):
                        errors.append(f"hot_projection_missing:{concept_id}")
                    if not str(row["ledger_generation"] or ""):
                        errors.append(f"publish_ledger_missing:{concept_id}")
    plan = {
        "schema": SCHEMA,
        "status": "PASS" if not errors else "HOLD",
        "generated_at": _now(),
        "apply": False,
        "db_path": str(db_path),
        "concept_root": str(concept_root),
        "coverage_path": str(coverage_path),
        "coverage_report_hash": report.get("report_hash"),
        "source_manifest_hash": report.get("source_manifest_hash"),
        "namespace_epoch": namespace_epoch,
        "expected_concept_count": expected_count,
        "allow_active_generation_replacement": bool(allow_active_generation_replacement),
        "generation_id": generation_id,
        "generation_hash": generation_hash,
        "members": page_records,
        "database": db_state,
        "errors": sorted(set(errors)),
        "rollback": {"required": True, "previous_generation": None, "backup": None},
    }
    return plan


def _apply(
    plan: Mapping[str, Any],
    *,
    db_path: Path,
    backup_root: Path,
    owner: str,
    migration_id: str = "concept-v11-baseline-seed",
    lease_id: Optional[str] = None,
    allow_active_generation_replacement: bool = False,
) -> dict[str, Any]:
    if str(plan.get("status")) != "PASS":
        raise RuntimeError("cannot apply a failed baseline seed plan")
    backup = backup_database(db_path, backup_root)
    if backup.get("verified") is not True:
        raise RuntimeError("database backup restore verification failed")
    generation_id = str(plan["generation_id"])
    generation_hash = str(plan["generation_hash"])
    now = _now()
    watermark_captured_at = int(datetime.now(timezone.utc).timestamp() * 1000)
    members = list(plan.get("members") or [])
    rollback_members: list[dict[str, Any]] = []
    with sqlite3.connect(str(db_path), timeout=10, isolation_level=None) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        try:
            if lease_id:
                lease = connection.execute(
                    "SELECT migration_id,stage_id,migration_epoch,owner,state,lease_expires_at FROM migration_leases WHERE lease_id=?",
                    (lease_id,),
                ).fetchone()
                if lease is None or str(lease[0]) != migration_id or str(lease[1]) != STAGE_ID or str(lease[2]) != str(plan["namespace_epoch"]) or str(lease[3]) != owner or str(lease[4]) != "active":
                    raise RuntimeError("baseline_stage_lease_missing_or_mismatched")
            admission = connection.execute(
                "SELECT admission_state FROM concept_admissions WHERE namespace_epoch=?", (str(plan["namespace_epoch"]),)
            ).fetchone()
            if admission is None or str(admission[0]) != "disabled":
                raise RuntimeError("admission_changed_before_apply")
            active = connection.execute("SELECT generation_id,generation_hash FROM generations WHERE domain='concepts' AND status='active'").fetchall()
            if active and not any(str(row[0]) == generation_id and str(row[1]) == generation_hash for row in active):
                if not allow_active_generation_replacement:
                    raise RuntimeError("active_generation_changed_before_apply")
                if len(active) != 1:
                    raise RuntimeError(f"active_generation_not_unique_before_apply:{len(active)}")
            already_active = any(str(row[0]) == generation_id and str(row[1]) == generation_hash for row in active)
            if already_active:
                watermark = _record_active_generation_watermark(
                    connection,
                    plan=plan,
                    captured_at=watermark_captured_at,
                )
                connection.execute("COMMIT")
                result = dict(plan)
                result["status"] = "APPLIED"
                result["apply"] = True
                result["applied_at"] = now
                result["operator"] = owner
                result["idempotent"] = True
                result["stage_lease"] = {"migration_id": migration_id, "stage_id": STAGE_ID, "lease_id": lease_id, "state": "active_during_apply"}
                result["active_generation_watermark"] = watermark
                result["rollback"] = {
                    "required": True,
                    "previous_generation": [generation_id],
                    "backup": backup,
                    "members": [],
                }
                return result
            existing = connection.execute("SELECT status FROM generations WHERE generation_id=?", (generation_id,)).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO generations(generation_id,domain,generation_hash,status,source_watermark,knowledge_watermark,created_at) VALUES(?,?,?,?,?,?,?)",
                    (generation_id, "concepts", generation_hash, "staged", str(plan.get("source_manifest_hash") or ""), str(plan.get("coverage_report_hash") or ""), now),
                )
            elif str(existing[0]) not in {"staged", "active"}:
                raise RuntimeError("baseline_generation_in_terminal_state")
            for member in members:
                concept_id = str(member["concept_id"])
                namespace_epoch = str(plan["namespace_epoch"])
                page_path = Path(str(plan["concept_root"])) / "state" / "pages" / f"{member['concept']}.md"
                if not page_path.is_file():
                    raise RuntimeError(f"concept_page_missing_before_apply:{member['concept']}")
                page_content = page_path.read_text(encoding="utf-8")
                page_hash = _file_hash(page_path)
                if page_hash != str(member.get("page_hash") or ""):
                    raise RuntimeError(f"concept_page_changed_before_apply:{concept_id}")
                active_rows = connection.execute(
                    "SELECT version_id,version,content,content_hash,generation_id,evidence_set_hash,source_snapshot_hash,status,provenance "
                    "FROM concept_versions WHERE concept_id=? AND namespace_epoch=? AND status='active'",
                    (concept_id, namespace_epoch),
                ).fetchall()
                if len(active_rows) != 1:
                    raise RuntimeError(f"active_version_count_changed_before_apply:{concept_id}:{len(active_rows)}")
                version = active_rows[0]
                action = str(member.get("version_action") or "reuse_current")
                target_id = str(member.get("target_version_id") or version["version_id"])
                target_label = str(member.get("target_version") or version["version"] or "")
                target = version
                if action in {"append_current", "activate_existing"}:
                    target = connection.execute(
                        "SELECT version_id,version,content,content_hash,generation_id,evidence_set_hash,source_snapshot_hash,status,provenance "
                        "FROM concept_versions WHERE version_id=? AND concept_id=? AND namespace_epoch=?",
                        (target_id, concept_id, namespace_epoch),
                    ).fetchone()
                    if target is None and action == "append_current":
                        target_id = "version-" + hashlib.sha256((concept_id + target_label + page_hash).encode("utf-8")).hexdigest()[:24]
                        target = connection.execute(
                            "SELECT version_id,version,content,content_hash,generation_id,evidence_set_hash,source_snapshot_hash,status,provenance "
                            "FROM concept_versions WHERE version_id=?",
                            (target_id,),
                        ).fetchone()
                        if target is None:
                            version_columns = _columns(connection, "concept_versions")
                            fields = [
                                "version_id", "concept_id", "namespace_epoch", "version", "generation_id",
                                "content", "content_hash", "source_snapshot_hash", "evidence_set_hash",
                                "compiler_version", "policy_version", "status", "created_at",
                            ]
                            values: list[Any] = [
                                target_id, concept_id, namespace_epoch, target_label, generation_id,
                                page_content, page_hash, str(plan.get("source_manifest_hash") or ""),
                                str(member["evidence_set_hash"]), "concept-v11-baseline-seed", None,
                                "active", now,
                            ]
                            if "provenance" in version_columns:
                                fields.append("provenance")
                                values.append("baseline_seed")
                            marks = ",".join("?" for _ in fields)
                            connection.execute(
                                f"INSERT INTO concept_versions({','.join(fields)}) VALUES({marks})",
                                values,
                            )
                            target = connection.execute(
                                "SELECT version_id,version,content,content_hash,generation_id,evidence_set_hash,source_snapshot_hash,status,provenance "
                                "FROM concept_versions WHERE version_id=?",
                                (target_id,),
                            ).fetchone()
                    if target is None:
                        raise RuntimeError(f"baseline_target_version_missing:{concept_id}:{target_id}")
                    if str(target["content_hash"] or "") != page_hash:
                        raise RuntimeError(f"baseline_target_content_hash_mismatch:{concept_id}")
                    if target_id != str(version["version_id"]):
                        connection.execute(
                            "UPDATE concept_versions SET status='superseded' WHERE version_id=? AND status='active'",
                            (version["version_id"],),
                        )
                        connection.execute(
                            "UPDATE concept_versions SET status='active' WHERE version_id=?",
                            (target_id,),
                        )
                elif action != "reuse_current":
                    raise RuntimeError(f"unknown_baseline_version_action:{concept_id}:{action}")
                rollback_members.append({
                    "concept_id": concept_id,
                    "version_id": version["version_id"],
                    "generation_id": version["generation_id"],
                    "evidence_set_hash": version["evidence_set_hash"],
                    "source_snapshot_hash": version["source_snapshot_hash"],
                    "status": version["status"],
                    "provenance": version["provenance"],
                    "version_action": action,
                    "new_version_id": target_id,
                    "new_version": target["version"],
                })
                connection.execute(
                    "UPDATE concept_versions SET generation_id=?,source_snapshot_hash=?,evidence_set_hash=? WHERE version_id=?",
                    (generation_id, str(plan.get("source_manifest_hash") or ""), str(member["evidence_set_hash"]), target_id),
                )
                hot_before = connection.execute(
                    "SELECT * FROM concept_hot_projection WHERE concept_id=? AND namespace_epoch=?",
                    (concept_id, namespace_epoch),
                ).fetchone()
                if hot_before is None:
                    raise RuntimeError(f"active_version_missing_before_apply:{concept_id}")
                hot_columns = _columns(connection, "concept_hot_projection")
                hot_values = [generation_id, "active", page_hash, now, now]
                hot_set = "generation_id=?,projection_state=?,observed_content_hash=?,observed_at=?,updated_at=?"
                if "provenance" in hot_columns:
                    hot_set += ",provenance=?"
                    hot_values.append("baseline_seed")
                changed = connection.execute(
                    f"UPDATE concept_hot_projection SET {hot_set} WHERE concept_id=? AND namespace_epoch=?",
                    (*hot_values, concept_id, namespace_epoch),
                )
                if changed.rowcount != 1:
                    raise RuntimeError(f"hot_projection_missing_before_apply:{concept_id}")
                ledger_before = connection.execute(
                    "SELECT * FROM concept_publish_ledger WHERE concept_id=? AND namespace_epoch=? "
                    "ORDER BY updated_at DESC,created_at DESC,rowid DESC LIMIT 1",
                    (concept_id, namespace_epoch),
                ).fetchone()
                if ledger_before is None:
                    raise RuntimeError(f"publish_ledger_missing_before_apply:{concept_id}")
                if str(ledger_before["version_id"] or "") == target_id:
                    publish_columns = _columns(connection, "concept_publish_ledger")
                    publish_set = "current_generation=?,current_hot_generation=?,desired_hot_generation=?,projection_state=?,evidence_hash=?,updated_at=?"
                    publish_values: list[Any] = [generation_id, generation_id, generation_id, "active", str(member["evidence_set_hash"]), now]
                    if "provenance" in publish_columns:
                        publish_set += ",provenance=?"
                        publish_values.append("baseline_seed")
                    changed = connection.execute(
                        f"UPDATE concept_publish_ledger SET {publish_set} WHERE publish_id=?",
                        (*publish_values, ledger_before["publish_id"]),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError(f"publish_ledger_update_failed:{concept_id}")
                else:
                    publish_columns = _columns(connection, "concept_publish_ledger")
                    fields = [
                        "publish_id", "concept_id", "namespace_epoch", "version_id", "previous_generation",
                        "current_generation", "current_hot_generation", "desired_hot_generation", "projection_state",
                        "projection_outbox_id", "operator", "evidence_hash", "created_at", "updated_at",
                    ]
                    values = [
                        "publish-" + hashlib.sha256((concept_id + target_id + generation_id).encode("utf-8")).hexdigest()[:24],
                        concept_id, namespace_epoch, target_id, ledger_before["current_generation"], generation_id,
                        generation_id, generation_id, "active", None, owner, str(member["evidence_set_hash"]), now, now,
                    ]
                    if "provenance" in publish_columns:
                        fields.append("provenance")
                        values.append("baseline_seed")
                    marks = ",".join("?" for _ in fields)
                    connection.execute(
                        f"INSERT INTO concept_publish_ledger({','.join(fields)}) VALUES({marks})",
                        values,
                    )
            connection.execute("UPDATE generations SET status='superseded' WHERE domain='concepts' AND status='active' AND generation_id<>?", (generation_id,))
            connection.execute("UPDATE generations SET status='active',active_at=? WHERE generation_id=?", (now, generation_id))
            watermark = _record_active_generation_watermark(
                connection,
                plan=plan,
                captured_at=watermark_captured_at,
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    result = dict(plan)
    result["status"] = "APPLIED"
    result["apply"] = True
    result["applied_at"] = now
    result["operator"] = owner
    result["active_generation_watermark"] = watermark
    result["stage_lease"] = {"migration_id": migration_id, "stage_id": STAGE_ID, "lease_id": lease_id, "state": "released_after_apply" if lease_id else "not_supplied"}
    result["rollback"] = {
        "required": True,
        "previous_generation": sorted({str(item.get("generation_id") or "") for item in rollback_members if item.get("generation_id")}),
        "backup": backup,
        "members": rollback_members,
    }
    return result


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--coverage", "--manifest", dest="coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--namespace-epoch", default=DEFAULT_NAMESPACE)
    parser.add_argument("--expected-concept-count", type=int, default=45)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--migration-id", default="concept-v11-baseline-seed")
    parser.add_argument("--owner", default="zhujie14")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--replace-active", action="store_true", help="allow one existing active Generation to be atomically superseded after a deterministic page rebuild")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    db_path = args.db_path.expanduser().resolve()
    concept_root = args.concept_root.expanduser().resolve()
    coverage = args.coverage.expanduser().resolve()
    try:
        plan = _build_plan(
            db_path,
            concept_root,
            coverage,
            args.namespace_epoch,
            args.expected_concept_count,
            allow_active_generation_replacement=args.replace_active,
        )
        plan["migration_id"] = args.migration_id
        if args.apply:
            store = PMSystemStore(db_path, auto_migrate=False)
            lease = store.acquire_migration_lease(
                migration_id=args.migration_id,
                stage_id=STAGE_ID,
                migration_epoch=args.namespace_epoch,
                owner=args.owner,
            )
            try:
                plan = _apply(
                    plan,
                    db_path=db_path,
                    backup_root=args.backup_root.expanduser().resolve(),
                    owner=args.owner,
                migration_id=args.migration_id,
                lease_id=str(lease["lease_id"]),
                allow_active_generation_replacement=args.replace_active,
            )
            finally:
                store.release_migration_lease(lease_id=str(lease["lease_id"]))
        _write_json(args.report.expanduser().resolve(), plan)
    except Exception as exc:
        plan = {"schema": SCHEMA, "status": "HOLD", "apply": bool(args.apply), "error": f"{type(exc).__name__}:{exc}"}
        _write_json(args.report.expanduser().resolve(), plan)
        print(json.dumps(plan, ensure_ascii=False))
        return 1
    print(json.dumps({key: plan.get(key) for key in ("schema", "status", "generation_id", "generation_hash", "errors", "rollback")}, ensure_ascii=False))
    return 0 if plan.get("status") in {"PASS", "APPLIED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
