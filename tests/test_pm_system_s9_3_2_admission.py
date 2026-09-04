from __future__ import annotations

import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_s9_3_2_admission import run_isolated_gate  # noqa: E402


class S932AdmissionTests(unittest.TestCase):
    def test_isolated_gate_passes_freeze_canary_and_reconcile(self) -> None:
        result = run_isolated_gate()
        self.assertTrue(result["passed"])
        self.assertTrue(result["freeze"]["passed"])
        self.assertTrue(result["canary"]["passed"])
        self.assertEqual(result["canary"]["claimed_count"], 2)
        self.assertEqual(result["canary"]["third_claim_count"], 1)
        self.assertEqual(result["canary"]["orphan_slots"], 0)


if __name__ == "__main__":
    unittest.main()
