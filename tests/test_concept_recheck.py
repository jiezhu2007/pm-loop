from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_recheck import _triage_gate_failed, _triage_should_stop  # noqa: E402


class ConceptRecheckTriageContractTests(unittest.TestCase):
    def test_complete_with_unavailable_stops_paging_and_fails_gate(self) -> None:
        row = {
            "run_id": "discover-unavailable",
            "status": "triage_partial",
            "triage_status": "complete_with_unavailable",
            "triage_remaining": 0,
            "unavailable_uris": ["viking://resources/missing"],
        }

        self.assertTrue(_triage_should_stop(row))
        self.assertTrue(_triage_gate_failed([row]))

    def test_final_success_supersedes_intermediate_partial_page(self) -> None:
        rows = [
            {
                "run_id": "discover-paged",
                "status": "triage_partial",
                "triage_status": "in_progress",
                "triage_remaining": 20,
            },
            {
                "run_id": "discover-paged",
                "status": "triage_no_candidate",
                "triage_status": "complete",
                "triage_remaining": 0,
            },
        ]

        self.assertFalse(_triage_gate_failed(rows))


if __name__ == "__main__":
    unittest.main()
