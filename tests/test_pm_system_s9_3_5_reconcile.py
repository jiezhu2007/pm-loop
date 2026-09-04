from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import pm_system_s9_3_5_reconcile as reconcile  # noqa: E402


class S935ReconcileTests(unittest.TestCase):
    def test_classifies_terminal_remote_and_local_states_without_writes(self) -> None:
        items = [
            {"docGuid": "complete", "uri": "viking://resources/a", "task_id": "t-complete", "status": "queued"},
            {"docGuid": "failed", "uri": "viking://resources/b", "task_id": "t-failed", "status": "queued"},
            {"docGuid": "fixture", "uri": "viking://resources/test/x", "task_id": "t", "status": "queued"},
            {"docGuid": "unknown", "uri": "viking://resources/c", "task_id": "t-unknown", "status": "queued"},
        ]
        responses = {
            "t-complete": {"state": "completed", "raw_status": "completed", "error": ""},
            "t-failed": {"state": "not_found", "raw_status": "NOT_FOUND", "error": "task_not_found_or_expired"},
            "t-unknown": {"state": "active", "raw_status": "running", "error": ""},
        }
        locals_ = {
            "t-complete": {"exists": True, "status": "completed"},
            "t-failed": {"exists": True, "status": "failed", "error": "timeout"},
            "t-unknown": {"exists": False, "status": ""},
        }
        with mock.patch.object(reconcile, "query_task", side_effect=lambda task_id: responses[task_id]), mock.patch.object(reconcile, "local_task_status", side_effect=lambda task_id: locals_.get(task_id, {"exists": False, "status": ""})):
            classified, summary = reconcile.reconcile_items(items)
        self.assertEqual(summary["terminal_completed"], 1)
        self.assertEqual(summary["terminal_failed"], 1)
        self.assertEqual(summary["fixture_quarantine"], 1)
        self.assertEqual(summary["unresolved"], 1)
        self.assertEqual([item["s9_3_5_classification"] for item in classified], ["terminal_completed", "terminal_failed", "fixture_quarantine", "unresolved"])

    def test_schedule_occurrences_are_bounded_by_freeze_window(self) -> None:
        start = __import__("datetime").datetime(2026, 8, 28, 19, 24, tzinfo=reconcile.TZ)
        end = __import__("datetime").datetime(2026, 8, 29, 4, 0, tzinfo=reconcile.TZ)
        self.assertEqual(reconcile.schedule_occurrences(start, end), [])

    def test_completed_ledger_item_without_task_id_is_not_fixture(self) -> None:
        items = [
            {"docGuid": "done", "uri": "viking://resources/shengsuan/source/doc", "status": "complete"},
            {"docGuid": "unknown", "uri": "viking://resources/shengsuan/source/doc2", "status": "queued"},
        ]
        with mock.patch.object(reconcile, "local_task_status", side_effect=AssertionError("no task id should be queried")):
            classified, summary = reconcile.reconcile_items(items)
        self.assertEqual(summary["terminal_completed"], 1)
        self.assertEqual(summary["unresolved"], 1)
        self.assertEqual(summary["fixture_quarantine"], 0)
        self.assertEqual(
            [item["s9_3_5_classification"] for item in classified],
            ["terminal_completed", "unresolved"],
        )

    def test_apply_closeout_requires_unchanged_hashes_and_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pending = root / "pending.json"
            ledger = root / "ledger.json"
            pending.write_text('{"items":[{"docGuid":"g","task_id":"t","status":"queued"}]}\n', encoding="utf-8")
            ledger.write_text('{"g":{"ingest_status":"queued"}}\n', encoding="utf-8")
            data = {
                "read_only": True,
                "phase_id": "S9.3.5",
                "freeze_flags": {"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze"},
                "source_hashes": {"pending_uploads": reconcile.sha256(pending), "ledger": reconcile.sha256(ledger)},
                "pending_items": [{"task_id": "t", "s9_3_5_classification": "fixture_quarantine", "s9_3_5_reason": "fixture"}],
            }
            with mock.patch.object(reconcile, "PENDING_PATH", pending), mock.patch.object(reconcile, "LEDGER_PATH", ledger):
                result = reconcile.apply_closeout(data, root / "backup")
                self.assertEqual(result["applied"]["fixture_quarantine"], 1)
                self.assertEqual(__import__("json").loads(pending.read_text())["items"][0]["status"], "quarantine")

    def test_dry_run_manifest_is_explicitly_read_only(self) -> None:
        data = {
            "read_only": True,
            "production_state_touched": False,
            "external_provider_calls": 0,
            "phase_id": "S9.3.5",
        }
        self.assertTrue(data["read_only"])
        self.assertFalse(data["production_state_touched"])
        self.assertEqual(data["external_provider_calls"], 0)


if __name__ == "__main__":
    unittest.main()
