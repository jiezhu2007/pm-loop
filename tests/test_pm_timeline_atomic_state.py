from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_STATE = (
    Path.home() / ".codex/skills/pm-timeline/scripts/runtime_state.py"
)


def load_module(path: Path):
    spec = importlib.util.spec_from_file_location("pm_timeline_runtime_state", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PMTimelineAtomicStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module(RUNTIME_STATE)

    def test_atomic_write_keeps_previous_file_when_replace_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "report.md"
            target.write_text("old", encoding="utf-8")
            with patch.object(self.module.os, "replace", side_effect=OSError("boom")):
                with self.assertRaises(OSError):
                    self.module.atomic_write_text(target, "new")
            self.assertEqual(target.read_text(encoding="utf-8"), "old")
            self.assertEqual(list(target.parent.glob(".report.md.*.tmp")), [])

    def test_terminal_marker_is_structured_and_terminal(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "latest.json"
            self.module.write_run_marker(
                target,
                task="pm-timeline-daily",
                status="ok",
                run_id="daily-test",
                started_at="2026-08-25T06:00:00+08:00",
                exit_code=0,
                reason="completed",
            )
            value = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(value["status"], "ok")
            self.assertEqual(value["exit_code"], 0)
            self.assertTrue(value["finished_at"])


if __name__ == "__main__":
    unittest.main()
