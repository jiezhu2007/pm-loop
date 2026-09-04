from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_capacity import run_capacity  # noqa: E402


class CapacityGateTests(unittest.TestCase):
    def test_two_four_eight_lane_capacity_gate_is_isolated_and_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            result = run_capacity(Path(temp), levels=(2, 4, 8))

            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["levels_requested"], [2, 4, 8])
            self.assertFalse(result["production_state_touched"])
            self.assertEqual(result["external_provider_calls"], 0)
            self.assertEqual(len(list(Path(temp).glob("level-*/pm-system.db"))), 3)
            for level in result["levels"]:
                width = level["width"]
                self.assertEqual(level["status"], "pass")
                self.assertEqual(level["accepted_runs"], width * 2 + 1)
                self.assertEqual(level["accepted_errors"], 0)
                self.assertTrue(level["backpressure_pass"])
                self.assertEqual(level["first_claimed"], width)
                self.assertEqual(level["queue_after_first_claim"], width + 1)
                self.assertEqual(level["completed_runs"], width * 2 + 1)
                self.assertEqual(level["duplicate_semantic_tasks"], 0)
                self.assertEqual(level["rate_limit_attempts"], 0)
                self.assertTrue(level["provider_bucket_blocks_dispatch"])
                self.assertLessEqual(level["retry_amplification"], 1.0)
                self.assertEqual(level["orphan_slots"], 0)
                self.assertEqual(level["active_slots_after_release"], 0)
                self.assertEqual(level["sqlite"]["journal_mode"].lower(), "wal")
                self.assertGreaterEqual(level["sqlite"]["busy_timeout"], 5000)
                self.assertEqual(level["sqlite"]["foreign_keys"], 1)
                self.assertEqual(level["external_provider_calls"], 0)
                self.assertFalse(level["production_state_touched"])
                self.assertEqual(level["errors"], [])


if __name__ == "__main__":
    unittest.main()

