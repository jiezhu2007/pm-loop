#!/usr/bin/env python3
"""Execute the V4.5 G3 durable Memory handoff and runtime sync."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ov_memory_sync import import_legacy_pending  # noqa: E402
from pm_system_gateway import SemanticGateway  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_watcher_runtime_sync import sync as sync_watcher  # noqa: E402


def run_g3(*, db_path: Path, mirror: Path, pending_path: Path, namespace_epoch: str, backup_root: Path, runtime_manifest: Path) -> dict:
    store = PMSystemStore(db_path)
    legacy = import_legacy_pending(store, mirror, namespace_epoch=namespace_epoch, pending_path=pending_path)
    runtime = sync_watcher(backup_root=backup_root)
    # The Resource semantic gateway must never claim kind=memory rows.  The
    # durable migration fence also prevents any remote dispatch here.
    dispatch_probe = SemanticGateway(store).dispatch_once(limit=20)
    with store.connect() as connection:
        events = [dict(row) for row in connection.execute("SELECT event_id,name,content_hash,namespace_epoch,state FROM memory_change_events ORDER BY observed_at,event_id").fetchall()]
        memory_outbox = [dict(row) for row in connection.execute("SELECT outbox_id,kind,profile,status,terminal_reason,payload_json FROM outbox_items WHERE kind='memory' ORDER BY created_at").fetchall()]
        claimable = int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE kind='memory' AND status IN ('pending','in_flight','retry_wait')").fetchone()[0])
    manifest = {
        "schema": "pm-system.v45-r2-g3-manifest.v1",
        "db_path": str(db_path),
        "namespace_epoch": namespace_epoch,
        "legacy_import": legacy,
        "runtime_sync": runtime,
        "memory_event_count": len(events),
        "memory_outbox_count": len(memory_outbox),
        "claimable_memory_rows": claimable,
        "resource_gateway_dispatch_probe": dispatch_probe,
        "all_memory_rows_quarantined": bool(memory_outbox) and all(row.get("status") == "quarantine" for row in memory_outbox),
    }
    manifest["manifest_hash"] = "sha256:" + hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
    runtime_manifest.parent.mkdir(parents=True, exist_ok=True)
    runtime_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--pending-path", type=Path, required=True)
    parser.add_argument("--namespace-epoch", required=True)
    parser.add_argument("--backup-root", type=Path, required=True)
    parser.add_argument("--runtime-manifest", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_g3(**vars(args)), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
