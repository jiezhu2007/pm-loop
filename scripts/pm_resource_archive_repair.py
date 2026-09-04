#!/usr/bin/env python3
"""Repair terminal PM archive submissions without rewriting their history.

The normal dispatcher intentionally deduplicates terminal Outbox rows.  This
maintenance command is the explicit recovery boundary for a real provider
repair: it reads the original durable artifact, sends one request with the
original idempotency key, and records the outcome without changing the
historical failed/dead-letter row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pm_resource_dispatcher import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_DB_PATH,
    OBSERVATION_ACTIVE,
    OBSERVATION_TERMINAL_FAILURE,
    OBSERVATION_TERMINAL_SUCCESS,
    OpenVikingTransport,
    SUCCESS_STATUSES,
    _task_id,
    _walk_status,
)
from pm_system_store import PMSystemStore, now_iso


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_path(root: Path, revision: str) -> Optional[Path]:
    directory = root / revision[:2]
    candidates = sorted(directory.glob(f"{revision}-*")) if directory.is_dir() else []
    return candidates[0] if len(candidates) == 1 else None


def _temp_file_id(value: Any) -> Optional[str]:
    if not isinstance(value, Mapping):
        return None
    result = value.get("result")
    if isinstance(result, Mapping) and result.get("temp_file_id"):
        return str(result["temp_file_id"])
    if value.get("temp_file_id"):
        return str(value["temp_file_id"])
    return None


def _response_status(response: Any) -> tuple[str, Optional[str]]:
    status, task_id = _walk_status(response)
    return str(status or "").strip().lower(), task_id or _task_id(response)


def _wait_task(transport: OpenVikingTransport, task_id: str, *, timeout: float) -> Dict[str, Any]:
    deadline = time.monotonic() + max(0.1, float(timeout))
    last_status = ""
    while time.monotonic() < deadline:
        response = transport.get_task(task_id, timeout=min(10.0, max(1.0, deadline - time.monotonic())))
        last_status, _ = _response_status(response)
        if last_status in OBSERVATION_TERMINAL_SUCCESS | OBSERVATION_TERMINAL_FAILURE:
            return {"status": last_status, "response": response}
        if last_status not in OBSERVATION_ACTIVE:
            return {"status": last_status or "unknown", "response": response}
        time.sleep(min(0.5, max(0.05, deadline - time.monotonic())))
    return {"status": "timeout", "response": None, "last_status": last_status}


def _rows(store: PMSystemStore, statuses: Iterable[str], limit: int) -> List[Mapping[str, Any]]:
    placeholders = ",".join("?" for _ in statuses)
    values = list(statuses)
    with store.connect() as connection:
        result = connection.execute(
            f"""
            SELECT outbox_id,idempotency_key,resource_id,revision_id,processing_mode,
                   provider,profile,payload_json,status,attempt,error_fingerprint,
                   created_at,updated_at
            FROM outbox_items
            WHERE status IN ({placeholders})
            ORDER BY created_at,outbox_id
            LIMIT ?
            """,
            (*values, int(limit)),
        ).fetchall()
    return [dict(row) for row in result]


def repair(
    *,
    db_path: Path = DEFAULT_DB_PATH,
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    limit: int = 100,
    timeout: float = 120.0,
    include_dead_letter: bool = True,
    transport: Optional[OpenVikingTransport] = None,
) -> Dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    statuses = ("failed", "dead_letter") if include_dead_letter else ("failed",)
    store = PMSystemStore(db_path)
    client = transport or OpenVikingTransport(timeout=timeout)
    results: List[Dict[str, Any]] = []
    for row in _rows(store, statuses, limit):
        item: Dict[str, Any] = {
            "outbox_id": row["outbox_id"],
            "idempotency_key": row["idempotency_key"],
            "resource_id": row["resource_id"],
            "revision_id": row["revision_id"],
            "historical_status": row["status"],
            "historical_attempt": int(row["attempt"] or 0),
            "historical_error_fingerprint": row["error_fingerprint"],
            "repair_started_at": now_iso(),
        }
        try:
            payload = json.loads(row["payload_json"] or "{}")
            payload = payload if isinstance(payload, Mapping) else {}
            source = Path(str(payload.get("file_path") or "")).expanduser()
            if not source.is_file():
                source = _artifact_path(Path(artifact_root), str(row["revision_id"])) or Path("")
            if not source.is_file():
                raise FileNotFoundError(f"durable artifact missing for {row['revision_id']}")
            digest = _sha256_file(source)
            if digest != str(row["revision_id"]):
                raise ValueError(f"artifact hash mismatch: expected {row['revision_id']}, got {digest}")
            item["artifact_path"] = str(source)
            item["artifact_sha256"] = digest

            uploaded = client.upload_file(source, timeout=timeout)
            temp_id = _temp_file_id(uploaded)
            if not temp_id:
                raise RuntimeError("temp_upload returned no temp_file_id")
            body: Dict[str, Any] = {
                "temp_file_id": temp_id,
                "to": row["resource_id"],
                "create_parent": True,
                "wait": True,
                "timeout": float(timeout),
                "processing_mode": row["processing_mode"],
            }
            if payload.get("instruction") not in (None, ""):
                body["instruction"] = payload["instruction"]
            response = client.add_resource(
                body,
                timeout=timeout,
                idempotency_key=str(row["idempotency_key"]),
            )
            status, task_id = _response_status(response)
            item["provider_status"] = status or "unknown"
            item["openviking_task_id"] = task_id
            item["response_summary"] = {
                "root_uri": response.get("result", {}).get("root_uri") if isinstance(response, Mapping) and isinstance(response.get("result"), Mapping) else None,
                "queue_status": response.get("result", {}).get("queue_status") if isinstance(response, Mapping) and isinstance(response.get("result"), Mapping) else None,
            }
            if status in OBSERVATION_ACTIVE and task_id:
                waited = _wait_task(client, task_id, timeout=timeout)
                item["provider_status"] = waited["status"]
                item["task_observation"] = waited
            item["status"] = "repaired" if item["provider_status"] in SUCCESS_STATUSES | OBSERVATION_TERMINAL_SUCCESS else "not_repaired"
        except Exception as exc:  # repair report must cover every row
            item["status"] = "not_repaired"
            item["error"] = f"{type(exc).__name__}: {exc}"
        item["repair_finished_at"] = now_iso()
        results.append(item)

    repaired = sum(item["status"] == "repaired" for item in results)
    return {
        "schema_version": "pm-resource-archive-repair.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "db_path": str(Path(db_path).expanduser().resolve()),
        "artifact_root": str(Path(artifact_root).expanduser().resolve()),
        "include_dead_letter": include_dead_letter,
        "selected": len(results),
        "repaired": repaired,
        "not_repaired": len(results) - repaired,
        "historical_rows_unchanged": True,
        "results": results,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--failed-only", action="store_true", help="exclude historical dead-letter rows")
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    report = repair(
        db_path=args.db_path,
        artifact_root=args.artifact_root,
        limit=args.limit,
        timeout=args.timeout,
        include_dead_letter=not args.failed_only,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("selected", "repaired", "not_repaired", "historical_rows_unchanged")}, ensure_ascii=False))
    return 0 if report["not_repaired"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
