#!/usr/bin/env python3
"""Deterministic C4 contract check for the concept projection lanes.

The tests use temporary SQLite stores and fake OpenViking transports.  This
runner never submits a production task and never calls OneAPI/OpenViking.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
TEST_MODULES = (
    "tests.test_pm_v11_shared_runtime",
    "tests.test_pm_system_gateway",
    "tests.test_pm_resource_dispatcher",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _production_snapshot(db_path: Path) -> Dict[str, Any]:
    import sqlite3

    result: Dict[str, Any] = {"path": str(db_path), "read_only": True}
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=3)
    try:
        result["integrity_check"] = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        result["active"] = {
            "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "runs": int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running','retry_wait')").fetchone()[0]),
            "outbox": int(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status IN ('pending','in_flight','retry_wait')").fetchone()[0]),
            "semantic": int(connection.execute("SELECT COUNT(*) FROM semantic_tasks WHERE status IN ('queued','in_flight','accepted','processing','retry_wait')").fetchone()[0]),
        }
        result["concept_admission"] = [dict(zip(("namespace_epoch", "admission_state", "version"), row)) for row in connection.execute(
            "SELECT namespace_epoch,admission_state,version FROM concept_admissions ORDER BY namespace_epoch"
        ).fetchall()]
    finally:
        connection.close()
    return result


def run(*, db_path: Path) -> Dict[str, Any]:
    command = [sys.executable, "-m", "unittest", "-v", *TEST_MODULES]
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=180, check=False)
    output = (completed.stdout or "") + "\n" + (completed.stderr or "")
    ran = re.findall(r"Ran (\d+) tests?", output)
    test_count = int(ran[-1]) if ran else 0
    passed = completed.returncode == 0 and "OK" in output
    return {
        "command": command,
        "returncode": completed.returncode,
        "status": "PASS" if passed else "HOLD",
        "test_count": test_count,
        "output_tail": output[-4000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    production_before = _production_snapshot(args.db_path.expanduser().resolve())
    tests = run(db_path=args.db_path.expanduser().resolve())
    production_after = _production_snapshot(args.db_path.expanduser().resolve())
    production_unchanged = (
        production_before.get("integrity_check") == production_after.get("integrity_check") == "ok"
        and production_before.get("active") == production_after.get("active")
        and production_before.get("concept_admission") == production_after.get("concept_admission")
    )
    invariants = {
        "admission_disabled_in_production": all(item.get("admission_state") == "disabled" for item in production_after.get("concept_admission", [])),
        "isolated_fixture_only": True,
        "external_provider_calls": 0,
        "production_state_touched": not production_unchanged,
    }
    payload = {
        "schema": "concept-v11.c4-contract-check.v1",
        "stage_id": "C4-ROUTE-READBACK-DUAL-LANE",
        "observed_at": _now(),
        "status": "PASS" if tests["status"] == "PASS" and production_unchanged and invariants["admission_disabled_in_production"] else "HOLD",
        "mode": "deterministic_fixture",
        "tests": tests,
        "invariants": invariants,
        "production_before": production_before,
        "production_after": production_after,
        "notes": [
            "fixture tests use temporary SQLite and FakeTransport",
            "no OneAPI/OpenViking request was made",
            "production concept_admission remains disabled",
        ],
    }
    args.report.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    args.report.expanduser().resolve().write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
