from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_content_source_preflight import REPORT_SCHEMA, build_preflight  # noqa: E402


def coverage() -> dict:
    return {
        "schema": "concept-v11.source-coverage-report.v1",
        "status": "PASS",
        "report_hash": "sha256:coverage",
        "source_manifest_hash": "sha256:manifest",
        "concepts": [
            {
                "concept": "Current",
                "concept_id": "concept-current",
                "coverage_status": "refreshable",
                "references": [
                    {"source_uri": "viking://resources/current.md", "disposition": "mapped"},
                    {"source_uri": "viking://resources/old.md", "disposition": "historical_exclusion"},
                ],
            },
            {
                "concept": "Retired",
                "concept_id": "concept-retired",
                "coverage_status": "retired_with_evidence",
                "references": [
                    {"source_uri": "viking://resources/retired.md", "disposition": "historical_exclusion"},
                ],
            },
        ],
    }


class ConceptContentSourcePreflightTests(unittest.TestCase):
    def _pages(self, root: Path, values: dict[str, str]) -> Path:
        pages = root / "state" / "pages"
        pages.mkdir(parents=True)
        for name, body in values.items():
            (pages / f"{name}.md").write_text(body, encoding="utf-8")
        return root

    def test_current_sources_and_retirement_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = self._pages(
                Path(temp),
                {
                    "Current": "---\nsources:\n  - viking://resources/current.md\n---\n# Current\n",
                    "Retired": "---\nconcept: Retired\n---\n# Retired\n",
                },
            )
            result = build_preflight(coverage=coverage(), concept_root=root, expected_concept_count=2, observed_at="2026-09-03T00:00:00Z")
            self.assertEqual(result["schema"], REPORT_SCHEMA)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["summary"], {"ready": 1, "needs_source_rebuild": 0, "retired_excluded": 1, "blocked": 0, "blocking_concepts": []})
            expected = {key: value for key, value in result.items() if key != "report_hash"}
            self.assertEqual(result["report_hash"], "sha256:" + hashlib.sha256(json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest())

    def test_historical_reference_is_audit_only_but_missing_current_source_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = self._pages(
                Path(temp),
                {
                    "Current": "# Current\n来源：viking://resources/old.md\n",
                    "Retired": "# Retired\n",
                },
            )
            result = build_preflight(coverage=coverage(), concept_root=root, expected_concept_count=2)
            row = next(item for item in result["concepts"] if item["concept"] == "Current")
            self.assertEqual(result["status"], "HOLD")
            self.assertEqual(row["content_status"], "needs_source_rebuild")
            self.assertEqual(row["historical_source_refs"], ["viking://resources/old.md"])
            self.assertIn("current_source_not_referenced", row["errors"])
            self.assertEqual(result["summary"]["blocking_concepts"], ["Current"])

    def test_historical_reference_does_not_block_when_current_source_is_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = self._pages(
                Path(temp),
                {
                    "Current": "# Current\n来源：viking://resources/current.md\n历史：viking://resources/old.md\n",
                    "Retired": "# Retired\n",
                },
            )
            result = build_preflight(coverage=coverage(), concept_root=root, expected_concept_count=2)
            row = next(item for item in result["concepts"] if item["concept"] == "Current")
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(row["content_status"], "ready")
            self.assertEqual(row["historical_source_refs"], ["viking://resources/old.md"])

    def test_missing_page_blocks_only_refreshable_concept(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = self._pages(Path(temp), {"Retired": "# Retired\n"})
            result = build_preflight(coverage=coverage(), concept_root=root, expected_concept_count=2)
            row = next(item for item in result["concepts"] if item["concept"] == "Current")
            self.assertEqual(row["content_status"], "needs_source_rebuild")
            self.assertEqual(row["errors"], ["page_missing"])

    def test_current_source_with_literal_space_is_exactly_detected(self) -> None:
        spaced = "viking://resources/current source.md"
        payload = coverage()
        payload["concepts"][0]["references"][0]["source_uri"] = spaced
        with tempfile.TemporaryDirectory() as temp:
            root = self._pages(
                Path(temp),
                {
                    "Current": f"---\nsources:\n  - {spaced}\n---\n# Current\n",
                    "Retired": "# Retired\n",
                },
            )
            result = build_preflight(coverage=payload, concept_root=root, expected_concept_count=2)
            row = next(item for item in result["concepts"] if item["concept"] == "Current")
            self.assertEqual(row["content_status"], "ready")


if __name__ == "__main__":
    unittest.main()
