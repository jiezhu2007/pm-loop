from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_catchup import main  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class CatchupTests(unittest.TestCase):
    def test_dry_run_reuses_registry_without_creating_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rc = main([
                "--db-path", str(root / "pm-system.db"),
                "--registry", str(ROOT / "scripts" / "schedule-registry.json"),
                "--runtime-registry", str(root / "missing-runtime.json"),
                "--lock-path", str(root / "catchup.lock"),
                "--now", "2026-09-08T03:00:00Z",
                "--dry-run",
            ])
            self.assertEqual(rc, 0)
            self.assertFalse((root / "pm-system.db").exists())

    def test_catchup_records_only_current_due_occurrences_and_expired_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm-system.db"
            PMSystemStore(db)
            rc = main([
                "--db-path", str(db),
                "--registry", str(ROOT / "scripts" / "schedule-registry.json"),
                "--runtime-registry", str(root / "missing-runtime.json"),
                "--lock-path", str(root / "catchup.lock"),
                "--now", "2026-09-07T08:00:00Z",
            ])
            self.assertEqual(rc, 0)
            with PMSystemStore(db, auto_migrate=False).connect() as connection:
                rows = connection.execute("SELECT schedule_key,state,job_id FROM schedule_occurrences ORDER BY schedule_key").fetchall()
                registry = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
                calendar_keys = {
                    str(task["schedule_key"])
                    for task in registry["tasks"]
                    if isinstance(task, dict) and isinstance(task.get("calendar"), dict)
                }
                self.assertEqual({row[0] for row in rows}, calendar_keys)
                self.assertEqual(sum(row[1] == "accepted" for row in rows), 2)
                self.assertEqual(sum(row[1] == "expired" for row in rows), len(calendar_keys) - 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs WHERE occurrence_id IS NOT NULL").fetchone()[0], 2)

    def test_entrypoint_has_no_independent_task_or_launchctl_trigger(self) -> None:
        source = (ROOT / "scripts" / "pm_loop_catchup.py").read_text(encoding="utf-8")
        self.assertNotIn("launchctl", source)
        self.assertNotIn("JOBS", source)
        self.assertIn('mode="catchup"', source)


if __name__ == "__main__":
    unittest.main()
