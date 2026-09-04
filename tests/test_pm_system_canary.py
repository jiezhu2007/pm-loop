from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_canary import run_canary  # noqa: E402


class CanaryTests(unittest.TestCase):
    def test_low_risk_canary_is_replayable_and_stable_over_logical_two_hour_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            result = run_canary(Path(temp), observation_seconds=7200)
            self.assertEqual(result.status, "pass")
            self.assertEqual(result.accepted, 3)
            self.assertEqual(result.completed, 3)
            self.assertEqual(result.duplicate_tasks, 0)
            self.assertEqual(result.orphan_slots, 0)
            self.assertEqual(result.post_cancel_commits, 0)
            self.assertEqual(result.observation["logical_window_seconds"], 7200)
            self.assertTrue(result.observation["source_version_stable"])


if __name__ == "__main__":
    unittest.main()

