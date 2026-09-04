from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_source_coverage import (  # noqa: E402
    CLOSURE_SCHEMA,
    CONCEPT_RETIREMENT_SCHEMA,
    LEDGER_SCHEMA,
    _concept_id,
    build_report,
)


def closure() -> dict:
    mapped_hash = "sha256:" + "a" * 64
    return {
        "schema": CLOSURE_SCHEMA,
        "closure_hash": "sha256:" + "c" * 64,
        "source_manifest": {"sha256": "sha256:" + "e" * 64},
        "rows": [
            {
                "map_id": "map-a-current",
                "concept": "A",
                "source_uri": "viking://resources/a/current.md",
                "status": "mapped",
                "resolution_reason": "content_and_identity_verified",
                "evidence_set_hash": "sha256:" + "1" * 64,
                "evidence_refs": [{"kind": "content_readback", "content_sha256": mapped_hash}],
            },
            {
                "map_id": "map-a-history",
                "concept": "A",
                "source_uri": "viking://resources/a/history.md",
                "status": "quarantined",
                "resolution_reason": "source_not_in_current_ledger",
                "evidence_set_hash": "sha256:" + "2" * 64,
                "evidence_refs": [],
            },
            {
                "map_id": "map-b-current",
                "concept": "B",
                "source_uri": "viking://resources/b/current.md",
                "status": "mapped",
                "resolution_reason": "content_and_identity_verified",
                "evidence_set_hash": "sha256:" + "3" * 64,
                "evidence_refs": [{"kind": "content_readback", "content_sha256": "sha256:" + "b" * 64}],
            },
            {
                "map_id": "map-c-old",
                "concept": "C",
                "source_uri": "viking://resources/c/old.md",
                "status": "quarantined",
                "resolution_reason": "content_readback_failed",
                "evidence_set_hash": "sha256:" + "4" * 64,
                "evidence_refs": [],
            },
        ],
    }


def entry(map_id: str, concept: str, source_uri: str, disposition: str, **extra: str) -> dict:
    return {
        "schema": LEDGER_SCHEMA,
        "entry_id": f"entry-{map_id}",
        "closure_hash": closure()["closure_hash"],
        "map_id": map_id,
        "concept": concept,
        "source_uri": source_uri,
        "disposition": disposition,
        "operator": "reviewer",
        "observed_at": "2026-09-02T10:00:00Z",
        "evidence_refs": [{"kind": "review", "sha256": "sha256:" + "f" * 64}],
        **extra,
    }


class ConceptV11SourceCoverageTests(unittest.TestCase):
    def test_unreviewed_quarantine_remains_needs_repair(self) -> None:
        report = build_report(closure=closure(), expected_concept_count=3, generated_at="2026-09-02T10:00:00Z")
        self.assertEqual(report["status"], "HOLD")
        self.assertEqual(report["concept_status_counts"], {
            "refreshable": 1,
            "substituted": 0,
            "retired_with_evidence": 0,
            "needs_repair": 2,
        })

    def test_substitution_and_verified_retirement_close_coverage(self) -> None:
        mapped_hash = "sha256:" + "a" * 64
        dispositions = [
            entry(
                "map-a-history",
                "A",
                "viking://resources/a/history.md",
                "substituted",
                replacement_source_uri="viking://resources/a/current.md",
                replacement_content_sha256=mapped_hash,
            ),
            entry(
                "map-c-old",
                "C",
                "viking://resources/c/old.md",
                "retired_with_evidence",
                retirement_uri="viking://resources/tombstones/c.md",
                retirement_content_sha256="sha256:" + "d" * 64,
            ),
        ]
        supplemental = {
            "schema": "concept-v11.c7-content-readback.v1",
            "rows": [
                {
                    "uri": "viking://resources/tombstones/c.md",
                    "status": "verified",
                    "content_sha256": "sha256:" + "d" * 64,
                }
            ],
        }
        report = build_report(
            closure=closure(),
            dispositions=dispositions,
            supplemental_readback=supplemental,
            expected_concept_count=3,
            generated_at="2026-09-02T10:00:00Z",
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["concept_status_counts"], {
            "refreshable": 1,
            "substituted": 1,
            "retired_with_evidence": 1,
            "needs_repair": 0,
        })

    def test_substitution_cannot_reference_other_concept_or_wrong_hash(self) -> None:
        dispositions = [
            entry(
                "map-a-history",
                "A",
                "viking://resources/a/history.md",
                "substituted",
                replacement_source_uri="viking://resources/b/current.md",
                replacement_content_sha256="sha256:" + "b" * 64,
            )
        ]
        report = build_report(closure=closure(), dispositions=dispositions, expected_concept_count=3)
        self.assertEqual(report["status"], "HOLD")
        self.assertIn("entry[0]:replacement_not_current_mapped_source", report["validation_errors"])

    def test_historical_exclusion_does_not_create_a_source_for_empty_concept(self) -> None:
        dispositions = [
            entry(
                "map-c-old",
                "C",
                "viking://resources/c/old.md",
                "historical_exclusion",
                exclusion_reason="superseded historical note",
            )
        ]
        report = build_report(closure=closure(), dispositions=dispositions, expected_concept_count=3)
        row = next(item for item in report["concepts"] if item["concept"] == "C")
        self.assertEqual(row["coverage_status"], "needs_repair")

    def test_concept_retirement_closes_historical_only_concept(self) -> None:
        tombstone_hash = "sha256:" + "d" * 64
        retirements = [{
            "schema": CONCEPT_RETIREMENT_SCHEMA,
            "concept": "C",
            "concept_id": _concept_id("C"),
            "decision": "retired_with_evidence",
            "retirement_uri": "viking://resources/tombstones/c.md",
            "retirement_content_sha256": tombstone_hash,
            "operator": "reviewer",
            "observed_at": "2026-09-03T10:00:00Z",
            "evidence_refs": [{"kind": "tombstone", "sha256": tombstone_hash}],
        }]
        dispositions = [entry(
            "map-a-history", "A", "viking://resources/a/history.md", "historical_exclusion",
            exclusion_reason="historical reference retained outside current coverage",
        ), entry(
            "map-c-old", "C", "viking://resources/c/old.md", "historical_exclusion",
            exclusion_reason="concept retired with independent evidence",
        )]
        supplemental = {
            "schema": "concept-v11.c7-content-readback.v1",
            "rows": [{
                "uri": "viking://resources/tombstones/c.md",
                "status": "verified",
                "content_sha256": tombstone_hash,
            }],
        }
        report = build_report(
            closure=closure(),
            dispositions=dispositions,
            concept_retirements=retirements,
            supplemental_readback=supplemental,
            expected_concept_count=3,
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["concept_status_counts"]["retired_with_evidence"], 1)
        retired = next(item for item in report["concepts"] if item["concept"] == "C")
        self.assertEqual(retired["coverage_status"], "retired_with_evidence")
        self.assertEqual(retired["references"][0]["disposition"], "historical_exclusion")


if __name__ == "__main__":
    unittest.main()
