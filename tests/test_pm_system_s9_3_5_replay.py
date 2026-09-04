from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import pm_system_s9_3_5_replay as replay  # noqa: E402


class S935ReplayTests(unittest.TestCase):
    def test_parse_totals_uses_last_totals_object(self) -> None:
        output = 'prefix {"success": 1, "upload_failed": 0}\n{"success": 3, "upload_failed": 1, "fetch_failed": 0, "query_failed": 0}\n'
        self.assertEqual(replay.parse_totals(output)["success"], 3)
        self.assertEqual(replay.parse_totals(output)["upload_failed"], 1)

    def test_source_counts_separates_pending_and_ledger_states(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pending = root / "pending.json"
            ledger = root / "ledger.json"
            pending.write_text(json.dumps({"items": [{"docGuid": "g", "uri": "viking://resources/shengsuan/demo/x", "status": "failed"}]}), encoding="utf-8")
            ledger.write_text(json.dumps({"g": {"source": "demo", "ingest_status": "failed"}}), encoding="utf-8")
            with mock.patch.object(replay, "PENDING", pending), mock.patch.object(replay, "LEDGER", ledger):
                self.assertEqual(replay.source_counts("demo"), {"pending_failed": 1, "ledger_failed": 1})

    def test_markdown_keeps_schedule_frozen_on_partial_source(self) -> None:
        text = replay.markdown_report(
            [{"source": "demo", "status": "HOLD_CONTINUE", "returncode": 1, "new_openviking_tasks": 0, "totals": {"success": 0, "upload_failed": 1, "fetch_failed": 0, "query_failed": 0}, "flags_during": {"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze"}}],
            status="HOLD_CONTINUE",
            started_at="x",
            finished_at="y",
        )
        self.assertIn("schedule/Codex Automation", text)
        self.assertIn("HOLD（补跑全部通过前不恢复）", text)


if __name__ == "__main__":
    unittest.main()
