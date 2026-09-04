from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_s9_scheduler_gate import run_gate  # noqa: E402


class S924SchedulerGateTests(unittest.TestCase):
    def test_isolated_gate_covers_freeze_two_slots_and_checkpoint_reconcile(self) -> None:
        result = run_gate()
        self.assertTrue(result["isolated"])
        self.assertFalse(result["freeze"]["admission"]["claim_enabled"])
        self.assertIsNone(result["freeze"]["claim"])
        self.assertEqual(result["freeze"]["run_status"], "queued")
        self.assertEqual(len(result["canary"]["claimed_run_ids"]), 2)
        self.assertIsNone(result["canary"]["third_claim"])
        self.assertEqual(result["canary"]["queued_third_status"], "queued")
        self.assertEqual(result["canary"]["checkpoint"]["payload_status"], "completed")
        self.assertEqual(result["canary"]["checkpoint"]["model_call_attempt"], 1)
        self.assertEqual(result["canary"]["startup_reconcile"]["completed_from_checkpoint"], 1)
        self.assertTrue(result["canary"]["resumed_after_reconcile"])
        self.assertEqual(result["canary"]["orphan_slots"], 0)
        self.assertEqual(result["external_provider_calls"], 0)
        self.assertFalse(result["production_state_touched"])


if __name__ == "__main__":
    unittest.main()
