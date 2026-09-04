from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_v45_g8 import run_g8  # noqa: E402


class V45G8RecoveryGateTests(unittest.TestCase):
    def test_all_fault_recovery_and_catchup_scenarios_pass(self) -> None:
        result = run_g8()
        self.assertEqual(result["decision"], "PASS")
        self.assertEqual(result["scenario_counts"], {"total": 9, "passed": 9, "hold": 0})
        self.assertFalse(result["production_state_touched"])
        self.assertEqual(result["external_provider_calls"], 0)
        self.assertEqual(
            {item["name"] for item in result["scenarios"]},
            {
                "model disconnect and bounded retry",
                "OpenViking 504 profile isolation",
                "duplicate revision idempotency",
                "cancel and late callback terminal fence",
                "restart lease reconciliation",
                "Resource 429 total-wall-clock terminal",
                "model 429 Retry-After and deadline",
                "response-unknown one controlled resend",
                "missed-period catch-up idempotency",
            },
        )


if __name__ == "__main__":
    unittest.main()
