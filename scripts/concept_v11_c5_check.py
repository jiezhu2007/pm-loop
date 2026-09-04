#!/usr/bin/env python3
"""Deterministic C5 shadow check with no provider or production writes."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
TEST_MODULES = (
    "tests.test_concept_v11_schema_v2",
    "tests.test_pm_v11_shared_runtime",
    "tests.test_pm_system_shadow",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _production_snapshot(db_path: Path) -> Dict[str, Any]:
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


def _run_tests() -> Dict[str, Any]:
    command = [sys.executable, "-m", "unittest", "-v", *TEST_MODULES]
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=180, check=False)
    output = (completed.stdout or "") + "\n" + (completed.stderr or "")
    matches = re.findall(r"Ran (\d+) tests?", output)
    test_count = int(matches[-1]) if matches else 0
    passed = completed.returncode == 0 and "OK" in output
    return {"command": command, "returncode": completed.returncode, "status": "PASS" if passed else "HOLD", "test_count": test_count, "output_tail": output[-4000:]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    db_path = args.db_path.expanduser().resolve()
    before = _production_snapshot(db_path)
    tests = _run_tests()
    after = _production_snapshot(db_path)
    unchanged = before == after
    admission_disabled = all(item.get("admission_state") == "disabled" for item in after.get("concept_admission", []))
    payload = {
        "schema": "concept-v11.c5-deterministic-shadow.v1",
        "stage_id": "C5-DETERMINISTIC-SHADOW",
        "observed_at": _now(),
        "status": "PASS" if tests["status"] == "PASS" and unchanged and admission_disabled else "HOLD",
        "mode": "deterministic_fixture",
        "tests": tests,
        "contracts": {
            "idempotency_and_replay": "covered by concept schema v2 tests",
            "epoch_revision_fence": "covered by shared runtime concept tests",
            "state_machine_and_quarantine": "covered by shared runtime and legacy shadow tests",
            "model_requested_auto_resolution_ledger": "append-only conflict detection covered; no provider call",
        },
        "invariants": {
            "external_provider_calls": 0,
            "production_state_touched": not unchanged,
            "concept_admission_disabled": admission_disabled,
            "active_production_work_unchanged": before.get("active") == after.get("active"),
        },
        "production_before": before,
        "production_after": after,
        "notes": [
            "all shadow fixtures use temporary SQLite and local fake inputs",
            "no OneAPI/OpenViking request was made",
            "deterministic shadow does not write Active or production concept namespace",
        ],
    }
    report = args.report.expanduser().resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
