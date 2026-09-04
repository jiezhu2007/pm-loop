from __future__ import annotations

import unittest

from scripts.concept_v11_disposition_replay import replay


class ConceptV11DispositionReplayTests(unittest.TestCase):
    def test_rekeys_nfkc_uri_and_drops_only_rows_now_mapped(self) -> None:
        closure = {
            "schema": "concept-v11.c7-source-map-evidence.v1",
            "closure_hash": "sha256:" + "b" * 64,
            "rows": [
                {
                    "map_id": "new-quarantine",
                    "concept": "审批流",
                    "source_uri": "viking://docs/版本管理（Global-Branching）/review.md",
                    "status": "quarantined",
                },
                {
                    "map_id": "new-mapped",
                    "concept": "数据表",
                    "source_uri": "viking://docs/table.md",
                    "status": "mapped",
                },
            ],
        }
        old = [
            {
                "schema": "concept-v11.source-coverage-disposition.v1",
                "map_id": "old-quarantine",
                "concept": "审批流",
                "source_uri": "viking://docs/版本管理(Global-Branching)/review.md",
                "closure_hash": "sha256:" + "a" * 64,
                "evidence_refs": [{"kind": "c7_source_map_evidence", "sha256": "sha256:old"}],
            },
            {
                "schema": "concept-v11.source-coverage-disposition.v1",
                "map_id": "new-mapped",
                "concept": "数据表",
                "source_uri": "viking://docs/table.md",
                "closure_hash": "sha256:" + "a" * 64,
                "evidence_refs": [{"kind": "c7_source_map_evidence", "sha256": "sha256:old"}],
            },
        ]

        replayed, audit = replay(
            closure=closure,
            old_entries=old,
            c7_evidence_sha256="sha256:" + "c" * 64,
        )

        self.assertEqual(len(replayed), 1)
        self.assertEqual(replayed[0]["map_id"], "new-quarantine")
        self.assertEqual(replayed[0]["source_uri"], closure["rows"][0]["source_uri"])
        self.assertEqual(replayed[0]["closure_hash"], closure["closure_hash"])
        self.assertEqual(replayed[0]["evidence_refs"][0]["sha256"], "sha256:" + "c" * 64)
        self.assertEqual(audit["dropped_now_mapped"], 1)
        self.assertEqual(audit["matched_by"]["compat_uri"], 1)


if __name__ == "__main__":
    unittest.main()
