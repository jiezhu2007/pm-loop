#!/usr/bin/env python3
"""One-time, lane-separated recovery for historical PM quarantine rows.

Memory rows are re-admitted through ``MemorySkillWriter`` and therefore use
``content/write``. Resource rows are re-admitted through
``PMResourceDispatcher`` and therefore use ``temp_upload -> /resources``.

The old quarantine rows are immutable historical evidence. A deterministic
replay epoch creates new Outbox rows, so re-running this script is idempotent.
Before enqueueing, every target is read back by content hash. A matching
remote copy is recorded as ``already_verified`` and is not uploaded again.
Use ``--force`` only when duplicate provider writes are explicitly acceptable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pm_memory_dispatcher import MemorySkillWriter
from pm_resource_dispatcher import (
    DispatchHTTPError,
    DispatchTransportError,
    OpenVikingTransport,
    PMResourceDispatcher,
    _content_hash_from_response,
)
from pm_system_store import PMSystemStore, now_iso


MEMORY_REASON = "legacy_pending_uncertain_remote_state"
RESOURCE_REASON = "historical_failure_evidence_missing"
MEMORY_KIND = ("memory", "memory-skill")
RESOURCE_KIND = ("resource", "pm-resource")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload(item: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        value = json.loads(str(item.get("payload_json") or "{}"))
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


def _load_rows(store: PMSystemStore, *, kind: str, profile: str, reason: str) -> List[Dict[str, Any]]:
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT * FROM outbox_items WHERE kind=? AND profile=? AND status='quarantine' AND terminal_reason=? ORDER BY created_at,outbox_id",
            (kind, profile, reason),
        ).fetchall()
    return [dict(row) for row in rows]


def _probe_remote(
    transport: OpenVikingTransport,
    resolver: PMResourceDispatcher,
    target_uri: str,
    expected_hash: str,
) -> Dict[str, Any]:
    """Read a target or its bounded directory leaves without changing state."""
    expected = str(expected_hash or "").lower().removeprefix("sha256:")
    read_timeout = min(max(float(getattr(transport, "timeout", 10.0)), 10.0), 30.0)
    try:
        response = transport.read_content(target_uri, timeout=read_timeout)
    except DispatchHTTPError as exc:
        if exc.status_code == 404:
            return {"status": "missing", "target_uri": target_uri}
        if exc.status_code != 400 or "directory uri" not in exc.body.lower():
            return {"status": "unknown", "target_uri": target_uri, "error": str(exc)}
        leaves = resolver._directory_leaves(target_uri, expected_hash=expected, timeout=read_timeout)
        for leaf in leaves:
            try:
                leaf_response = transport.read_content(leaf["uri"], timeout=read_timeout)
            except (DispatchHTTPError, DispatchTransportError, OSError):
                continue
            actual = _content_hash_from_response(leaf_response)
            if actual and actual.lower() == expected:
                return {"status": "verified", "target_uri": target_uri, "resolved_uri": leaf["uri"], "actual_hash": actual}
        return {"status": "missing", "target_uri": target_uri, "directory_leaves": len(leaves)}
    except (DispatchTransportError, OSError) as exc:
        return {"status": "unknown", "target_uri": target_uri, "error": str(exc)}
    actual = _content_hash_from_response(response)
    if actual and actual.lower() == expected:
        return {"status": "verified", "target_uri": target_uri, "actual_hash": actual}
    return {"status": "mismatch", "target_uri": target_uri, "actual_hash": actual}


def _validate_source(item: Mapping[str, Any], *, lane: str) -> Dict[str, Any]:
    payload = _payload(item)
    path = Path(str(payload.get("file_path") or "")).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"{lane} source is missing: {path}")
    current_hash = _sha256_file(path)
    recorded_hash = str(item.get("revision_id") or payload.get("content_hash") or "").lower().removeprefix("sha256:")
    if lane == "resource" and current_hash != recorded_hash:
        raise ValueError(f"resource artifact hash changed: {path} ({recorded_hash} != {current_hash})")
    return {
        "path": path,
        "payload": payload,
        "recorded_hash": recorded_hash,
        "current_hash": current_hash,
        "target_uri": str(payload.get("target_uri") or item.get("resource_id") or ""),
    }


def _active_ids(store: PMSystemStore, *, kind: str, profile: str) -> List[str]:
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT outbox_id FROM outbox_items WHERE kind=? AND profile=? AND status IN ('pending','retry_wait','in_flight','writing','awaiting_task','readback') ORDER BY outbox_id",
            (kind, profile),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _reactivate_failed_replay(store: PMSystemStore, *, outbox_id: str, namespace_epoch: str) -> bool:
    """Make only this replay batch's terminal row claimable on a rerun."""
    with store.transaction() as connection:
        cursor = connection.execute(
            "UPDATE outbox_items SET status='pending',attempt=0,next_attempt_at=NULL,error_fingerprint=NULL,terminal_reason=NULL,updated_at=datetime('now') "
            "WHERE outbox_id=? AND namespace_epoch=? AND status IN ('failed','dead_letter','quarantine')",
            (str(outbox_id), str(namespace_epoch)),
        )
    return cursor.rowcount == 1


def _finalize_originals(store: PMSystemStore, *, report: Mapping[str, Any], batch_id: str) -> List[str]:
    """Close the original quarantine rows after their replay evidence is terminal."""
    finalized: List[str] = []
    at = now_iso()
    for lane in ("memory", "resource"):
        epoch = f"v45-r2-quarantine-replay:{batch_id}:{lane}"
        for entry in report.get(lane, []):
            old_id = str(entry.get("old_outbox_id") or "")
            replay_id = str(entry.get("replay_outbox_id") or "")
            probe_status = str((entry.get("probe") or {}).get("status") or "")
            if not replay_id:
                with store.connect() as connection:
                    replay = connection.execute(
                        "SELECT r.outbox_id,r.status FROM outbox_items AS r JOIN outbox_items AS o ON o.resource_id=r.resource_id AND o.revision_id=r.revision_id WHERE o.outbox_id=? AND r.namespace_epoch=? AND r.kind=? AND r.profile=? ORDER BY r.created_at DESC LIMIT 1",
                        (old_id, f"v45-r2-quarantine-replay:{batch_id}:{lane}", MEMORY_KIND[0] if lane == "memory" else RESOURCE_KIND[0], MEMORY_KIND[1] if lane == "memory" else RESOURCE_KIND[1]),
                    ).fetchone()
                if replay is not None:
                    replay_id = str(replay[0])
                    entry["replay_outbox_id"] = replay_id
            result = "already_verified" if not replay_id and probe_status == "verified" else "replayed_completed"
            if replay_id:
                with store.connect() as connection:
                    replay = connection.execute(
                        "SELECT status FROM outbox_items WHERE outbox_id=? AND namespace_epoch=?",
                        (replay_id, epoch),
                    ).fetchone()
                if replay is None or str(replay[0]) != "completed":
                    raise RuntimeError(f"cannot finalize {old_id}: replay {replay_id} is not completed")
            elif result != "already_verified":
                raise RuntimeError(f"cannot finalize {old_id}: no terminal replay evidence")
            with store.transaction() as connection:
                row = connection.execute(
                    "SELECT payload_json FROM outbox_items WHERE outbox_id=? AND status='quarantine' AND ((kind='memory' AND profile='memory-skill') OR (kind='resource' AND profile='pm-resource'))",
                    (old_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"original quarantine row is no longer eligible: {old_id}")
                try:
                    payload = json.loads(str(row[0] or "{}"))
                except json.JSONDecodeError:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                payload["quarantine_replay"] = {
                    "batch_id": str(batch_id),
                    "namespace_epoch": epoch,
                    "replay_outbox_id": replay_id or None,
                    "result": result,
                    "finalized_at": at,
                }
                connection.execute(
                    "UPDATE outbox_items SET status='completed',terminal_reason='replayed',payload_json=?,updated_at=? WHERE outbox_id=? AND status='quarantine'",
                    (json.dumps(payload, ensure_ascii=False, separators=(",", ":")), at, old_id),
                )
                if lane == "memory":
                    event_id = str(payload.get("event_id") or "")
                    if event_id:
                        connection.execute(
                            "UPDATE memory_change_events SET state='consumed',consumed_at=? WHERE event_id=? AND state='quarantine'",
                            (at, event_id),
                        )
            finalized.append(old_id)
    return finalized


def _dispatch_lane(
    store: PMSystemStore,
    *,
    lane: str,
    count: int,
    artifact_root: Path,
    timeout: float,
    outbox_ids: List[str],
) -> List[Dict[str, Any]]:
    if count <= 0:
        return []
    if lane == "memory":
        writer = MemorySkillWriter(store)
        results = writer.dispatch_pending(limit=count, outbox_ids=outbox_ids)
        results.extend(writer.reconcile_tasks(limit=count, outbox_ids=outbox_ids))
        return results
    dispatcher = PMResourceDispatcher(store, artifact_root=artifact_root)
    results = dispatcher.dispatch_pending(limit=count, outbox_ids=outbox_ids)
    results.extend(dispatcher.reconcile_content(limit=count, min_age_seconds=0, outbox_ids=outbox_ids))
    results.extend(dispatcher.reconcile_tasks(limit=count, min_age_seconds=0, outbox_ids=outbox_ids))
    return results


def replay(
    *,
    db_path: Path,
    artifact_root: Path,
    batch_id: str,
    apply: bool,
    force: bool,
    timeout: float,
) -> Dict[str, Any]:
    store = PMSystemStore(db_path)
    memory_rows = _load_rows(store, kind=MEMORY_KIND[0], profile=MEMORY_KIND[1], reason=MEMORY_REASON)
    resource_rows = _load_rows(store, kind=RESOURCE_KIND[0], profile=RESOURCE_KIND[1], reason=RESOURCE_REASON)
    if len(memory_rows) != 19 or len(resource_rows) != 16:
        raise RuntimeError(f"quarantine count changed: memory={len(memory_rows)} resource={len(resource_rows)}; refusing partial replay")

    transport = OpenVikingTransport(timeout=timeout)
    probe_dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=artifact_root)
    report: Dict[str, Any] = {"batch_id": batch_id, "apply": apply, "force": force, "memory": [], "resource": []}

    prepared: Dict[str, List[Dict[str, Any]]] = {"memory": [], "resource": []}
    for lane, rows in (("memory", memory_rows), ("resource", resource_rows)):
        for item in rows:
            source = _validate_source(item, lane=lane)
            expected = source["current_hash"] if lane == "memory" else source["recorded_hash"]
            probe = _probe_remote(transport, probe_dispatcher, source["target_uri"], expected)
            entry = {
                "old_outbox_id": item["outbox_id"],
                "source": str(source["path"]),
                "target_uri": source["target_uri"],
                "old_revision_id": source["recorded_hash"],
                "replay_revision_id": expected,
                "probe": probe,
            }
            report[lane].append(entry)
            if probe["status"] == "unknown":
                raise RuntimeError(f"remote preflight unknown for {lane} {item['outbox_id']}: {probe}")
            if force or probe["status"] != "verified":
                prepared[lane].append({"item": item, "source": source, "revision": expected})

    if not apply:
        report["planned_writes"] = {lane: len(rows) for lane, rows in prepared.items()}
        return report

    replay_ids: Dict[str, List[str]] = {"memory": [], "resource": []}
    replay_epochs = {
        "memory": f"v45-r2-quarantine-replay:{batch_id}:memory",
        "resource": f"v45-r2-quarantine-replay:{batch_id}:resource",
    }
    memory_writer = MemorySkillWriter(store)
    resource_dispatcher = PMResourceDispatcher(store, artifact_root=artifact_root)
    for lane, entries in prepared.items():
        for entry in entries:
            item = entry["item"]
            source = entry["source"]
            if lane == "memory":
                admitted = store.enqueue_memory_change(
                    name=source["path"].name,
                    mtime=source["path"].stat().st_mtime_ns,
                    content_hash=entry["revision"],
                    snapshot_uri=source["target_uri"],
                    file_path=str(source["path"]),
                    namespace_epoch=replay_epochs[lane],
                )
            else:
                payload = source["payload"]
                admitted = resource_dispatcher.enqueue_file(
                    path=source["path"],
                    target_uri=source["target_uri"],
                    kind="resource",
                    processing_mode=str(item.get("processing_mode") or payload.get("processing_mode") or "vectors_only"),
                    provider=str(item.get("provider") or "openviking"),
                    profile="pm-resource",
                    instruction=str(payload.get("instruction") or ""),
                    wait=bool(payload.get("wait", True)),
                    timeout=float(payload.get("timeout") or timeout),
                    strict=bool(payload.get("strict", True)),
                    namespace_epoch=replay_epochs[lane],
                    owner="pm-quarantine-replay",
                )
            entry["replay_outbox_id"] = str(admitted["outbox_id"])
            entry["admission"] = admitted
            entry["reactivated"] = _reactivate_failed_replay(
                store,
                outbox_id=str(admitted["outbox_id"]),
                namespace_epoch=replay_epochs[lane],
            )
            # A completed row in this deterministic replay namespace is
            # already the local exactly-once result. Do not enqueue it again
            # merely because a later remote probe is still inconclusive.
            if not (
                bool(admitted.get("deduplicated"))
                and str(admitted.get("outbox_status") or "") == "completed"
                and not force
            ):
                replay_ids[lane].append(str(admitted["outbox_id"]))

    for lane, kind_profile in (("memory", MEMORY_KIND), ("resource", RESOURCE_KIND)):
        active = _active_ids(store, kind=kind_profile[0], profile=kind_profile[1])
        unexpected = [outbox_id for outbox_id in active if outbox_id not in replay_ids[lane]]
        if unexpected:
            raise RuntimeError(f"unexpected active {lane} rows would be mixed into dispatch: {unexpected}")

    report["dispatch"] = {
        "memory": _dispatch_lane(store, lane="memory", count=len(replay_ids["memory"]), artifact_root=artifact_root, timeout=timeout, outbox_ids=replay_ids["memory"]),
        "resource": _dispatch_lane(store, lane="resource", count=len(replay_ids["resource"]), artifact_root=artifact_root, timeout=timeout, outbox_ids=replay_ids["resource"]),
    }
    report["planned_writes"] = {lane: len(replay_ids[lane]) for lane in replay_ids}
    report["finalized_originals"] = _finalize_originals(store, report=report, batch_id=batch_id)
    return report


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path.home() / ".codex/pm-loop/state/pm-system.db")
    parser.add_argument("--artifact-root", type=Path, default=Path.home() / ".codex/pm-loop/resource-outbox")
    parser.add_argument("--batch-id", default="20260831")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--apply", action="store_true", help="perform admissions and dispatch; default is preflight only")
    parser.add_argument("--force", action="store_true", help="write even when preflight proves the target hash already exists")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        result = replay(
            db_path=args.db_path.expanduser().resolve(),
            artifact_root=args.artifact_root.expanduser().resolve(),
            batch_id=str(args.batch_id),
            apply=bool(args.apply),
            force=bool(args.force),
            timeout=max(10.0, float(args.timeout)),
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps({"status": "ok", **result}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
