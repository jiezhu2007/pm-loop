from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_p3_review_package import (  # noqa: E402
    CANDIDATE_SCHEMA,
    CLOSURE_SCHEMA,
    COVERAGE_SCHEMA,
    build_package,
    render_markdown,
    write_csv,
)


class P3ReviewPackageTests(unittest.TestCase):
    def _inputs(self) -> tuple[dict, dict, dict]:
        closure = {
            "schema": CLOSURE_SCHEMA,
            "closure_hash": "sha256:" + "a" * 64,
            "source_manifest": {"sha256": "sha256:" + "b" * 64},
            "rows": [
                {"map_id": "map-1", "concept": "概念A", "source_uri": "viking://old/a", "status": "quarantined", "resolution_reason": "source_not_in_current_ledger", "evidence_set_hash": "sha256:" + "c" * 64},
                {"map_id": "map-2", "concept": "概念B", "source_uri": "viking://old/b", "status": "quarantined", "resolution_reason": "content_readback_failed", "evidence_set_hash": "sha256:" + "d" * 64},
            ],
        }
        coverage = {
            "schema": COVERAGE_SCHEMA,
            "closure_hash": closure["closure_hash"],
            "source_manifest_hash": closure["source_manifest"]["sha256"],
            "report_hash": "sha256:" + "e" * 64,
            "concept_status_counts": {"refreshable": 0, "needs_repair": 2},
            "concepts": [
                {"concept": "概念A", "coverage_status": "needs_repair", "disposition_counts": {"mapped": 1, "needs_repair": 1}},
                {"concept": "概念B", "coverage_status": "needs_repair", "disposition_counts": {"needs_repair": 1}},
            ],
        }
        candidates = {
            "schema": CANDIDATE_SCHEMA,
            "coverage_report_hash": coverage["report_hash"],
            "report_hash": "sha256:" + "f" * 64,
            "candidates": [
                {"concept": "概念B", "candidate_uri": "viking://current/b", "source_id": "public-docs:b", "qualified_for_human_review": True, "readback": {"content_sha256": "sha256:" + "1" * 64}},
                {"concept": "概念A", "candidate_uri": "viking://ignore/a", "source_id": "current:a", "qualified_for_human_review": False, "readback": {"content_sha256": "sha256:" + "2" * 64}},
            ],
        }
        return closure, coverage, candidates

    def test_builds_non_ledger_worksheet_with_blank_decisions(self) -> None:
        closure, coverage, candidates = self._inputs()
        package = build_package(
            closure=closure,
            coverage=coverage,
            candidates=candidates,
            expected_concept_count=2,
            expected_quarantine_count=2,
        )
        self.assertEqual(package["artifact_kind"], "human_review_worksheet_not_ledger")
        self.assertFalse(package["write_authority"]["source_map"])
        self.assertEqual(len(package["worksheet_rows"]), 2)
        self.assertTrue(all(not row["review_decision"] for row in package["worksheet_rows"]))
        no_mapped = package["summary"]["no_mapped_concepts"]
        self.assertEqual(no_mapped, ["概念B"])
        row = next(item for item in package["worksheet_rows"] if item["concept"] == "概念B")
        self.assertEqual(row["qualified_candidates"][0]["candidate_uri"], "viking://current/b")
        self.assertIn("不能直接被 C7", render_markdown(package))

    def test_rejects_mixed_coverage_evidence(self) -> None:
        closure, coverage, candidates = self._inputs()
        coverage["closure_hash"] = "sha256:" + "z" * 64
        with self.assertRaisesRegex(RuntimeError, "coverage closure hash mismatch"):
            build_package(
                closure=closure,
                coverage=coverage,
                candidates=candidates,
                expected_concept_count=2,
                expected_quarantine_count=2,
            )

    def test_writes_csv_without_decisions(self) -> None:
        closure, coverage, candidates = self._inputs()
        package = build_package(
            closure=closure,
            coverage=coverage,
            candidates=candidates,
            expected_concept_count=2,
            expected_quarantine_count=2,
        )
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "worksheet.csv"
            write_csv(target, package["worksheet_rows"])
            text = target.read_text(encoding="utf-8-sig")
        self.assertIn("review_decision", text)
        self.assertNotIn("substituted\r\n", text)


if __name__ == "__main__":
    unittest.main()
