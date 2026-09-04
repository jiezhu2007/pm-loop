#!/usr/bin/env python3
"""Bounded read-back convergence for one already submitted concept Canary.

This utility never enqueues or dispatches a resource.  It only re-reads the
OpenViking root/leaf for a known outbox item and lets the shared dispatcher
promote its local content state after a matching hash is observed.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from pm_resource_dispatcher import OpenVikingTransport, PMResourceDispatcher  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"


def run(db_path: Path, outbox_id: str, *, attempts: int = 6, interval_seconds: float = 20.0) -> dict[str, Any]:
    db_path = db_path.expanduser().resolve()
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
    if row is None:
        return {"status": "HOLD", "outbox_id": outbox_id, "errors": ["outbox_missing"], "submissions": 0}
    if str(row["kind"] or "") != "concept":
        return {"status": "HOLD", "outbox_id": outbox_id, "errors": ["outbox_not_concept"], "submissions": 0}
    item = {key: row[key] for key in ("outbox_id", "idempotency_key", "kind", "resource_id", "revision_id", "processing_mode", "profile", "owner", "namespace_epoch", "attempt")}
    payload = json.loads(row["payload_json"] or "{}")
    dispatcher = PMResourceDispatcher(PMSystemStore(db_path), transport=OpenVikingTransport(), observation_deadline_seconds=180)
    observations: list[dict[str, Any]] = []
    for index in range(max(1, int(attempts))):
        result = dispatcher._read_back(item, payload if isinstance(payload, dict) else {})
        observations.append({"attempt": index + 1, "result": result})
        if result.get("verified") is True:
            return {"status": "PASS", "outbox_id": outbox_id, "submissions": 0, "observations": observations}
        if index + 1 < max(1, int(attempts)):
            time.sleep(max(0.0, float(interval_seconds)))
    return {"status": "HOLD", "outbox_id": outbox_id, "submissions": 0, "observations": observations, "errors": ["content_readback_not_verified"]}


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--outbox-id", required=True)
    parser.add_argument("--attempts", type=int, default=6)
    parser.add_argument("--interval-seconds", type=float, default=20.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = run(args.db_path, args.outbox_id, attempts=args.attempts, interval_seconds=args.interval_seconds)
    except (OSError, sqlite3.Error, ValueError, KeyError) as exc:
        result = {"status": "HOLD", "outbox_id": args.outbox_id, "submissions": 0, "errors": [f"{type(exc).__name__}:{exc}"]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
