#!/usr/bin/env python3
"""Produce a read-only S0 inventory for scheduler migration."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


DEFAULT_DB = Path.home() / ".codex/pm-loop/state/pm-system.db"
DEFAULT_OUTPUT = Path.home() / ".codex/pm-loop/scheduler-migration/S0-inventory.json"
BUSINESS_LABELS = {
    "com.zhujie14.weekly-sync-and-refresh",
    "com.zhujie14.product-intelligence-monitor",
    "com.zhujie14.pm-timeline-daily",
    "com.zhujie14.pm-timeline-weekly",
    "com.zhujie14.catchup",
}


def _sha(path: Path) -> Optional[str]:
    try:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _plist(path: Path) -> Dict[str, Any]:
    try:
        value = plistlib.loads(path.read_bytes())
        return value if isinstance(value, dict) else {"parse_error": "not_object"}
    except Exception as exc:
        return {"parse_error": f"{type(exc).__name__}: {exc}"}


def collect(*, launch_agents: Path, db_path: Path) -> Dict[str, Any]:
    plists = []
    for path in sorted(launch_agents.glob("*.plist")):
        label = str(_plist(path).get("Label") or path.stem)
        if label in BUSINESS_LABELS:
            plists.append({"label": label, "path": str(path), "sha256": _sha(path), "config": _plist(path)})
    schema = None
    tables = []
    try:
        connection = sqlite3.connect(str(db_path))
        schema = int(connection.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()[0])
        tables = sorted(str(row[0]) for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'"))
        connection.close()
    except Exception as exc:
        schema = {"error": f"{type(exc).__name__}: {exc}"}
    return {"schema_version": "pm-loop.scheduler-migration.inventory.v1", "collected_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"), "pid": os.getpid(), "db_path": str(db_path), "db_schema": schema, "tables": tables, "business_launchagents": plists, "target_scheduler": "com.zhujie14.pm-scheduler", "read_only": True}


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-agents", type=Path, default=Path.home() / "Library/LaunchAgents")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(list(argv) if argv is not None else None)
    value = collect(launch_agents=args.launch_agents.expanduser(), db_path=args.db_path.expanduser())
    args.output.expanduser().parent.mkdir(parents=True, exist_ok=True)
    args.output.expanduser().write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(value, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
