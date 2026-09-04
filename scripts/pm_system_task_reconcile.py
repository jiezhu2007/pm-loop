#!/usr/bin/env python3
"""Read-only OpenViking task reconciliation and isolated snapshot staging.

The OpenViking task API is a bounded projection, so this module scans the
local task files without changing them.  Durable observations are optional and
must be directed at an explicit PMSystemStore (normally an isolated database
during migration).  No OpenViking API call or task mutation is performed.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pm_system_evidence import EvidenceGateway
from pm_system_store import PMSystemStore, now_iso


TERMINAL_STATUSES = {"completed", "complete", "success", "cancelled", "canceled"}
QUARANTINE_STATUSES = {"failed", "error", "dead_letter"}
NON_TERMINAL_STATUSES = {"running", "processing", "queued", "accepted", "pending", "retry_wait", "in_flight"}
CONTENT_HASH_VERIFIER = "content_sha256"


def _has_verified_content_hash(item: Mapping[str, Any]) -> bool:
    """Require both a valid digest and explicit live-read provenance."""
    digest = str(item.get("sha256") or item.get("content_sha256") or "").strip()
    return bool(
        re.fullmatch(r"(?:sha256:)?[0-9a-fA-F]{64}", digest)
        and item.get("sha256_verified_by") == CONTENT_HASH_VERIFIER
        and str(item.get("sha256_verified_at") or "").strip()
    )


def _task_time(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _synthetic_task_id(path: Path) -> str:
    return "file:" + hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:24]


def classify_task(
    record: Mapping[str, Any],
    *,
    now: Optional[float] = None,
    stale_after_seconds: int = 3600,
    resource_exists: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return a stable observation without modifying the external record."""
    current = time.time() if now is None else float(now)
    task_id = str(record.get("task_id") or record.get("id") or "").strip()
    status = str(record.get("status") or record.get("stage") or record.get("state") or "unknown").lower()
    created = _task_time(record.get("created_at") or record.get("createdAt"), current)
    age = max(0.0, current - created)
    resource_id = str(record.get("resource_id") or record.get("resource_uri") or "").strip()
    if not resource_id or resource_exists is False:
        classification, reason = "orphan", "missing_resource" if not resource_id else "resource_not_found"
    elif status in QUARANTINE_STATUSES:
        classification, reason = "quarantine", "terminal_failure"
    elif status in TERMINAL_STATUSES:
        classification, reason = "terminal", "terminal_status"
    elif status in NON_TERMINAL_STATUSES:
        if age >= max(1, int(stale_after_seconds)):
            classification, reason = "stale", f"non_terminal_age_seconds={int(age)}"
        else:
            classification, reason = "active", f"non_terminal_age_seconds={int(age)}"
    else:
        classification, reason = "unknown", "unrecognized_status"
    return {
        "task_id": task_id,
        "task_type": str(record.get("task_type") or record.get("type") or "unknown"),
        "external_status": status,
        "classification": classification,
        "resource_uri": resource_id or None,
        "created_at": record.get("created_at_iso") or record.get("created_at") or record.get("createdAt"),
        "reason": reason,
    }


def scan_task_directory(
    task_dir: Path,
    *,
    now: Optional[float] = None,
    stale_after_seconds: int = 3600,
    resource_root: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Scan JSON task files in deterministic order; malformed files are observed."""
    root = Path(task_dir).expanduser().resolve()
    observations: List[Dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(record, Mapping):
                raise ValueError("task record is not an object")
            resource_exists = None
            if resource_root is not None:
                resource_uri = str(record.get("resource_id") or record.get("resource_uri") or "")
                if resource_uri.startswith("viking://resources/"):
                    candidate = Path(resource_root) / resource_uri.removeprefix("viking://resources/")
                    # OpenViking may materialize a URI as a directory with
                    # chunk files, so a missing exact path is inconclusive.
                    resource_exists = True if candidate.exists() else None
            observation = classify_task(record, now=now, stale_after_seconds=stale_after_seconds, resource_exists=resource_exists)
            if not observation["task_id"]:
                observation["task_id"] = _synthetic_task_id(path)
                observation["classification"] = "orphan"
                observation["reason"] = "missing_task_id"
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            observation = {
                "task_id": _synthetic_task_id(path),
                "task_type": "unknown",
                "external_status": "unknown",
                "classification": "invalid",
                "resource_uri": None,
                "created_at": None,
                "reason": f"{type(exc).__name__}",
            }
        observation["payload_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        observations.append(observation)
    return observations


def observe_tasks(store: PMSystemStore, observations: Iterable[Mapping[str, Any]], *, observed_at: Optional[str] = None) -> Dict[str, int]:
    """Upsert task observations into an explicit local store."""
    timestamp = observed_at or now_iso()
    counts: Counter[str] = Counter()
    with store.transaction() as connection:
        for item in observations:
            task_id = str(item.get("task_id") or "").strip()
            if not task_id:
                continue
            connection.execute(
                """INSERT INTO external_task_observations(task_id,task_type,external_status,classification,resource_uri,created_at,observed_at,payload_sha256,reason)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(task_id) DO UPDATE SET task_type=excluded.task_type,
                     external_status=excluded.external_status, classification=excluded.classification,
                     resource_uri=excluded.resource_uri, created_at=excluded.created_at,
                     observed_at=excluded.observed_at, payload_sha256=excluded.payload_sha256,
                     reason=excluded.reason""",
                (task_id, str(item.get("task_type") or "unknown"), str(item.get("external_status") or "unknown"), str(item.get("classification") or "unknown"), item.get("resource_uri"), str(item.get("created_at")) if item.get("created_at") is not None else None, timestamp, item.get("payload_sha256"), str(item.get("reason") or "")),
            )
            counts[str(item.get("classification") or "unknown")] += 1
    return dict(counts)


def summarize(observations: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    items = list(observations)
    classes = Counter(str(item.get("classification") or "unknown") for item in items)
    return {
        "files": len(items),
        "by_classification": dict(sorted(classes.items())),
        "stale_task_ids": [str(item.get("task_id")) for item in items if item.get("classification") == "stale"],
        "orphan_task_ids": [str(item.get("task_id")) for item in items if item.get("classification") == "orphan"],
        "quarantine_task_ids": [str(item.get("task_id")) for item in items if item.get("classification") == "quarantine"],
        "invalid_task_ids": [str(item.get("task_id")) for item in items if item.get("classification") == "invalid"],
    }


def stage_first_batch_snapshot(
    store: PMSystemStore,
    ledger: Mapping[str, Any],
    *,
    source: str,
    limit: int = 20,
    captured_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Stage a non-publishable snapshot from existing ledger metadata.

    Ledger hashes without the explicit ``content_sha256`` provenance marker
    are intentionally represented as ``unknown`` source items; they are not
    promoted to verified evidence until a live content read confirms them.
    """
    entries = [value for value in ledger.values() if isinstance(value, Mapping) and value.get("source") == source and value.get("target_uri")]
    entries.sort(key=lambda value: (str(value.get("publishTime") or ""), str(value.get("doc_guid") or value.get("target_uri"))))
    selected = entries[: max(1, int(limit))]
    manifest_items = []
    for item in selected:
        resource_id = str(item.get("doc_guid") or hashlib.sha256(str(item.get("target_uri")).encode("utf-8")).hexdigest()[:24])
        content_hash = str(item.get("sha256") or "")
        revision = str(item.get("publishTime") or content_hash[:16] or "unknown")
        manifest_items.append({
            "resource_id": resource_id,
            "revision_id": revision,
            "uri": str(item["target_uri"]),
            "content_sha256": content_hash,
            "hash_mode": str(item.get("sha256_mode") or "missing"),
            "sha256_verified_at": str(item.get("sha256_verified_at") or ""),
            "sha256_verified_by": str(item.get("sha256_verified_by") or ""),
        })
    manifest = {"source": source, "items": manifest_items, "staging_only": True}
    manifest_hash = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    snapshot = EvidenceGateway(store).commit_snapshot(source_id=source, source_revision=f"staged-{manifest_hash[:16]}", content_sha256=manifest_hash, manifest=manifest, captured_at=captured_at)
    unknown = 0
    for item in manifest_items:
        status = "verified" if _has_verified_content_hash(item) else "unknown"
        if status != "verified":
            unknown += 1
        EvidenceGateway(store).add_source_item(snapshot_id=snapshot["snapshot_id"], resource_id=item["resource_id"], revision_id=item["revision_id"], uri=item["uri"], content_sha256=item["content_sha256"] or manifest_hash, status=status)
    return {"source": source, "selected": len(manifest_items), "unknown_items": unknown, "snapshot": snapshot, "manifest_hash": manifest_hash}


__all__ = ["classify_task", "observe_tasks", "scan_task_directory", "stage_first_batch_snapshot", "summarize"]
