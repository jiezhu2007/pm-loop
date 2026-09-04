from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_v45_g7_model_shadow import run_model_shadow  # noqa: E402


class V45G7ModelShadowTests(unittest.TestCase):
    def test_fixture_records_model_calls_and_controlled_attempt_two(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            result = run_model_shadow(
                sample_count=8,
                duration_seconds=0.05,
                width=2,
                provider_limit=1,
                model_latency_ms=0,
                unknown_every=3,
                root=Path(temp),
            )
        self.assertEqual(result["sample_count"], 8)
        self.assertEqual(result["run_count"], 8)
        self.assertEqual(result["response_unknown_count"], 3)
        self.assertEqual(result["attempt_two_count"], 3)
        self.assertEqual(result["model_call_count"], 11)
        self.assertEqual(len(result["model_calls_ledger"]), 11)
        self.assertTrue(result["model_calls_ledger_sha256"].startswith("sha256:"))
        grouped = {}
        for row in result["model_calls_ledger"]:
            grouped.setdefault(row["run_id"], []).append(row)
        retried_run = next(rows for rows in grouped.values() if len(rows) == 2)
        self.assertEqual([row["attempt"] for row in retried_run], [1, 2])
        self.assertEqual(retried_run[0]["model_input_hash"], retried_run[1]["model_input_hash"])
        self.assertEqual(result["active_slots_after_release"], 0)
        self.assertEqual(result["active_provider_tokens_after_release"], 0)
        self.assertTrue(result["provider_calls_verified"])
        self.assertEqual(result["external_provider_calls"], 0)
        self.assertFalse(result["production_state_touched"])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["evidence_role"], "isolated_model_contract_fixture")
        self.assertEqual(result["decision"], "HOLD")
        self.assertIn("sample_count=8<1000", result["violations"])

    def test_concurrent_workers_process_the_run_they_claimed(self) -> None:
        for _ in range(5):
            with tempfile.TemporaryDirectory() as temp:
                result = run_model_shadow(
                    sample_count=16,
                    duration_seconds=0.02,
                    width=8,
                    provider_limit=2,
                    model_latency_ms=0,
                    unknown_every=3,
                    root=Path(temp),
                )
            self.assertEqual(result["sample_count"], 16)
            self.assertEqual(result["run_count"], 16)
            self.assertEqual(result["errors"], [])
            self.assertEqual(result["active_slots_after_release"], 0)
            self.assertEqual(result["active_provider_tokens_after_release"], 0)

    def test_invalid_window_is_rejected_before_creating_evidence(self) -> None:
        with self.assertRaises(ValueError):
            run_model_shadow(sample_count=0, duration_seconds=1, width=1, provider_limit=1)


if __name__ == "__main__":
    unittest.main()
