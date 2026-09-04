#!/usr/bin/env python3
"""Apply evidence-backed suppression for historical PM Loop alerts.

This is intentionally an operator command, not an HTTP endpoint.  It never
changes canonical occurrence, Job, Run, or health snapshot states.  It only
suppresses an open alert after checking an S5 cutover boundary, a completed
replacement run with readable artifacts, or an exact historical health
baseline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from pm_system_store import PMSystemStore


PLAN_SCHEMA = "pm-loop.alert-reconciliation-plan.v1"
RESULT_SCHEMA = "pm-loop.alert-reconciliation-result.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _parse_time(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("timestamp is required")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_mapping(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"JSON object required: {path}")
    return value


def _completed_artifact_evidence(path: Path) -> tuple[dict[str, Any], datetime]:
    status = _read_mapping(path)
    if str(status.get("status") or "") != "completed":
        raise ValueError(f"replacement status is not completed: {path}")
    completed_at = _parse_time(status.get("run_at"))
    raw_artifacts = status.get("artifacts")
    if not isinstance(raw_artifacts, Mapping) or not raw_artifacts:
        raise ValueError(f"replacement status has no artifacts: {path}")
    artifacts = []
    for name, raw_path in sorted(raw_artifacts.items()):
        artifact = Path(str(raw_path)).expanduser().resolve()
        if not artifact.is_file():
            raise ValueError(f"replacement artifact is missing: {artifact}")
        artifacts.append({"name": str(name), "path": str(artifact), "sha256": _sha256(artifact)})
    return {
        "kind": "completed_replacement_run",
        "status_file": str(path),
        "status_sha256": _sha256(path),
        "completed_at": _iso(completed_at),
        "artifacts": artifacts,
    }, completed_at


def _completed_handler_evidence(path: Path) -> tuple[dict[str, Any], datetime]:
    """Validate a scheduled handler's own completed evidence file."""
    status = _read_mapping(path)
    if str(status.get("status") or "") != "completed":
        raise ValueError(f"handler evidence is not completed: {path}")
    if int(status.get("returncode", 1)) != 0:
        raise ValueError(f"handler evidence has non-zero returncode: {path}")
    run_id = str(status.get("run_id") or "").strip()
    schedule_key = str(status.get("schedule_key") or "").strip()
    occurrence_id = str(status.get("occurrence_id") or "").strip()
    finished_at = _parse_time(status.get("finished_at"))
    if not run_id or not schedule_key or not occurrence_id or finished_at is None:
        raise ValueError(f"handler evidence is missing identity or finished_at: {path}")
    output_path = Path(str(status.get("output_path") or "")).expanduser().resolve()
    if not output_path.is_file():
        raise ValueError(f"handler output is missing: {output_path}")
    return {
        "kind": "completed_handler_run",
        "handler_file": str(path),
        "handler_sha256": _sha256(path),
        "run_id": run_id,
        "schedule_key": schedule_key,
        "occurrence_id": occurrence_id,
        "finished_at": _iso(finished_at),
        "output_path": str(output_path),
        "output_sha256": _sha256(output_path),
    }, finished_at


def _pre_cutover_expired_alerts(store: PMSystemStore, *, cutover_at: datetime) -> list[dict[str, Any]]:
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT a.alert_id,a.fingerprint,a.alert_type,o.occurrence_id,o.schedule_key,o.scheduled_at,o.updated_at
            FROM ops_alerts AS a
            JOIN schedule_occurrences AS o ON o.occurrence_id=a.occurrence_id
            WHERE a.state='open'
              AND a.alert_type='occurrence_expired'
              AND o.state='expired'
              AND o.job_id IS NULL
              AND o.run_id IS NULL
              AND o.scheduled_at < ?
            ORDER BY o.scheduled_at,a.alert_id
            """,
            (_iso(cutover_at),),
        ).fetchall()
    return [dict(row) for row in rows]


def _replacement_alerts(store: PMSystemStore, *, schedule_key: str, completed_at: datetime) -> list[dict[str, Any]]:
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT a.alert_id,a.fingerprint,a.alert_type,a.occurrence_id,a.job_id,a.run_id,
                   COALESCE(o.schedule_key,j.schedule_key,r.schedule_key) AS schedule_key,
                   COALESCE(o.updated_at,j.updated_at,r.updated_at) AS source_updated_at
            FROM ops_alerts AS a
            LEFT JOIN schedule_occurrences AS o ON o.occurrence_id=a.occurrence_id
            LEFT JOIN jobs AS j ON j.job_id=a.job_id
            LEFT JOIN runs AS r ON r.run_id=a.run_id
            WHERE a.state='open'
              AND a.alert_type IN ('occurrence_failed','job_failed','run_failed','dead_letter')
              AND COALESCE(o.schedule_key,j.schedule_key,r.schedule_key)=?
              AND CASE
                    WHEN a.alert_type='occurrence_failed'
                      THEN COALESCE(j.updated_at,r.updated_at,o.updated_at)
                    ELSE COALESCE(o.updated_at,j.updated_at,r.updated_at)
                  END < ?
            ORDER BY source_updated_at,a.alert_id
            """,
            (str(schedule_key), _iso(completed_at)),
        ).fetchall()
    return [dict(row) for row in rows]


def _handler_replacement_alerts(
    store: PMSystemStore,
    *,
    schedule_key: str,
    replaced_run_ids: list[str],
    completed_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Select only alerts linked to explicitly listed historical runs."""
    normalized_runs = sorted({str(value).strip() for value in replaced_run_ids if str(value).strip()})
    if not normalized_runs:
        raise ValueError("replaced_run_ids are required")
    placeholders = ",".join("?" for _ in normalized_runs)
    with store.connect() as connection:
        run_rows = connection.execute(
            f"SELECT run_id,job_id,occurrence_id,schedule_key,updated_at FROM runs WHERE run_id IN ({placeholders})",
            tuple(normalized_runs),
        ).fetchall()
        if len(run_rows) != len(normalized_runs):
            found = {str(row[0]) for row in run_rows}
            missing = sorted(set(normalized_runs) - found)
            raise ValueError(f"replacement runs are missing: {missing}")
        linked_jobs = sorted({str(row[1]) for row in run_rows if row[1]})
        linked_occurrences = sorted({str(row[2]) for row in run_rows if row[2]})
        for row in run_rows:
            if str(row[3] or "") != str(schedule_key):
                raise ValueError(f"replacement run has unexpected schedule_key: {row[0]}")
            updated_at = _parse_time(row[4])
            if updated_at is not None and updated_at > completed_at:
                raise ValueError(f"replacement evidence predates run update: {row[0]}")
        clauses = [f"run_id IN ({placeholders})"]
        params: list[str] = list(normalized_runs)
        if linked_jobs:
            job_placeholders = ",".join("?" for _ in linked_jobs)
            clauses.append(f"job_id IN ({job_placeholders})")
            params.extend(linked_jobs)
        if linked_occurrences:
            occurrence_placeholders = ",".join("?" for _ in linked_occurrences)
            clauses.append(f"occurrence_id IN ({occurrence_placeholders})")
            params.extend(linked_occurrences)
        rows = connection.execute(
            f"SELECT * FROM ops_alerts WHERE state='open' AND ({' OR '.join(clauses)}) ORDER BY last_seen_at,alert_id",
            tuple(params),
        ).fetchall()
    evidence = {
        "replaced_run_ids": normalized_runs,
        "linked_job_ids": linked_jobs,
        "linked_occurrence_ids": linked_occurrences,
        "completed_at": _iso(completed_at),
    }
    return [dict(row) for row in rows], evidence


def _schedule_expired_replacement_alerts(
    store: PMSystemStore,
    *,
    schedule_key: str,
    completed_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Select old expired windows replaced by a completed handler run.

    An expired occurrence has no Job/Run to link to the replacement, so this
    selector uses the schedule key and the replacement completion time.  It
    never touches newer windows or any canonical occurrence state.
    """
    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT a.*
            FROM ops_alerts AS a
            JOIN schedule_occurrences AS o ON o.occurrence_id=a.occurrence_id
            WHERE a.state='open'
              AND a.alert_type='occurrence_expired'
              AND o.schedule_key=?
              AND o.state='expired'
              AND o.job_id IS NULL
              AND o.run_id IS NULL
              AND o.scheduled_at < ?
            ORDER BY o.scheduled_at,a.alert_id
            """,
            (str(schedule_key), _iso(completed_at)),
        ).fetchall()
    return [dict(row) for row in rows], {
        "schedule_key": str(schedule_key),
        "completed_at": _iso(completed_at),
        "selection": "open occurrence_expired with scheduled_at before completed handler",
    }


def _historical_health_alerts(
    store: PMSystemStore,
    *,
    modules: list[str],
    source_version: str,
    observed_before: datetime,
) -> list[dict[str, Any]]:
    """Select health alerts tied to one exact pre-cutover snapshot."""
    normalized_modules = sorted({str(value).strip() for value in modules if str(value).strip()})
    if not normalized_modules:
        raise ValueError("health modules are required")
    placeholders = ",".join("?" for _ in normalized_modules)
    with store.connect() as connection:
        snapshots = connection.execute(
            f"SELECT module,observed_at FROM module_health_snapshots WHERE module IN ({placeholders}) AND status='maintenance' AND source_version=? AND observed_at < ?",
            tuple(normalized_modules) + (str(source_version), _iso(observed_before)),
        ).fetchall()
        if not snapshots:
            return []
        conditions = []
        params: list[str] = []
        for row in snapshots:
            conditions.append("(a.module=? AND a.details_json LIKE ?)")
            params.extend([str(row[0]), f'%"observed_at":"{row[1]}"%'])
        rows = connection.execute(
            f"SELECT a.* FROM ops_alerts AS a WHERE a.state='open' AND a.alert_type='health_check' AND ({' OR '.join(conditions)}) ORDER BY a.last_seen_at,a.alert_id",
            tuple(params),
        ).fetchall()
    return [dict(row) for row in rows]


def reconcile(*, db_path: Path, plan_path: Path, apply: bool = False) -> dict[str, Any]:
    plan_path = Path(plan_path).expanduser().resolve()
    plan = _read_mapping(plan_path)
    if str(plan.get("schema_version") or "") != PLAN_SCHEMA:
        raise ValueError(f"unsupported reconciliation plan: {plan_path}")
    entries = plan.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("reconciliation plan entries are required")
    store = PMSystemStore(Path(db_path).expanduser().resolve(), auto_migrate=False)
    result_entries = []
    all_alert_ids: list[str] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise ValueError(f"entry {index} must be an object")
        kind = str(entry.get("kind") or "")
        reason = str(entry.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"entry {index} reason is required")
        if kind == "pre_cutover_expired":
            manifest = Path(str(entry.get("cutover_manifest") or "")).expanduser().resolve()
            if not manifest.is_file():
                raise ValueError(f"cutover manifest is missing: {manifest}")
            cutover_at = _parse_time(entry.get("cutover_at"))
            selected = _pre_cutover_expired_alerts(store, cutover_at=cutover_at)
            evidence = {
                "kind": "s5_pre_cutover_expired_baseline",
                "cutover_at": _iso(cutover_at),
                "cutover_manifest": str(manifest),
                "cutover_manifest_sha256": _sha256(manifest),
            }
        elif kind == "successful_rerun":
            schedule_key = str(entry.get("schedule_key") or "").strip()
            if not schedule_key:
                raise ValueError(f"entry {index} schedule_key is required")
            status_path = Path(str(entry.get("status_file") or "")).expanduser().resolve()
            evidence, completed_at = _completed_artifact_evidence(status_path)
            evidence["schedule_key"] = schedule_key
            selected = _replacement_alerts(store, schedule_key=schedule_key, completed_at=completed_at)
        elif kind == "successful_handler_replacement":
            schedule_key = str(entry.get("schedule_key") or "").strip()
            if not schedule_key:
                raise ValueError(f"entry {index} schedule_key is required")
            handler_file = Path(str(entry.get("handler_file") or "")).expanduser().resolve()
            evidence, completed_at = _completed_handler_evidence(handler_file)
            if evidence["schedule_key"] != schedule_key:
                raise ValueError(f"handler schedule_key does not match entry: {handler_file}")
            replaced = entry.get("replaced_run_ids")
            if not isinstance(replaced, list):
                raise ValueError(f"entry {index} replaced_run_ids must be a list")
            selected, linked = _handler_replacement_alerts(
                store,
                schedule_key=schedule_key,
                replaced_run_ids=[str(value) for value in replaced],
                completed_at=completed_at,
            )
            evidence["replacement"] = linked
        elif kind == "successful_schedule_rerun":
            schedule_key = str(entry.get("schedule_key") or "").strip()
            if not schedule_key:
                raise ValueError(f"entry {index} schedule_key is required")
            handler_file = Path(str(entry.get("handler_file") or "")).expanduser().resolve()
            evidence, completed_at = _completed_handler_evidence(handler_file)
            if evidence["schedule_key"] != schedule_key:
                raise ValueError(f"handler schedule_key does not match entry: {handler_file}")
            selected, replacement = _schedule_expired_replacement_alerts(
                store,
                schedule_key=schedule_key,
                completed_at=completed_at,
            )
            evidence["replacement"] = replacement
        elif kind == "historical_health_baseline":
            manifest = Path(str(entry.get("cutover_manifest") or "")).expanduser().resolve()
            if not manifest.is_file():
                raise ValueError(f"cutover manifest is missing: {manifest}")
            source_version = str(entry.get("source_version") or "").strip()
            if not source_version:
                raise ValueError(f"entry {index} source_version is required")
            modules = entry.get("modules")
            if not isinstance(modules, list):
                raise ValueError(f"entry {index} modules must be a list")
            observed_before = _parse_time(entry.get("observed_before"))
            evidence = {
                "kind": "s5_pre_cutover_health_baseline",
                "observed_before": _iso(observed_before),
                "source_version": source_version,
                "modules": [str(value) for value in modules],
                "cutover_manifest": str(manifest),
                "cutover_manifest_sha256": _sha256(manifest),
            }
            selected = _historical_health_alerts(
                store,
                modules=[str(value) for value in modules],
                source_version=source_version,
                observed_before=observed_before,
            )
        else:
            raise ValueError(f"unsupported reconciliation entry kind: {kind}")
        alert_ids = [str(row["alert_id"]) for row in selected]
        all_alert_ids.extend(alert_ids)
        result_entries.append({
            "kind": kind,
            "reason": reason,
            "selected_alerts": selected,
            "selected_alert_ids": alert_ids,
            "evidence": evidence,
        })
    applied = []
    if apply:
        for entry in result_entries:
            applied.extend(
                store.suppress_ops_alerts(
                    alert_ids=entry["selected_alert_ids"],
                    reason=str(entry["reason"]),
                    evidence=entry["evidence"],
                )
            )
    return {
        "schema_version": RESULT_SCHEMA,
        "plan_path": str(plan_path),
        "plan_sha256": _sha256(plan_path),
        "db_path": str(Path(db_path).expanduser().resolve()),
        "apply": bool(apply),
        "selected_count": len(all_alert_ids),
        "selected_alert_ids": all_alert_ids,
        "applied_count": len(applied),
        "applied_alert_ids": [str(item["alert_id"]) for item in applied],
        "entries": result_entries,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="write suppression records; default is dry-run")
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = reconcile(db_path=args.db_path, plan_path=args.plan, apply=args.apply)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
