from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_recompile_current import (  # noqa: E402
    RECOMPILE_SCHEMA,
    apply_plan,
    build_plan,
    collect_evidence,
)


class ConceptCurrentSourceRecompileTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path, Path, Path]:
        concept_root = root / "concept-root"
        pages = concept_root / "state" / "pages"
        pages.mkdir(parents=True)
        page = pages / "示例.md"
        current = "viking://resources/current/示例.md"
        historical = "viking://resources/history/示例.md"
        page.write_text(
            "---\n"
            "concept: 示例\n"
            "aliases: []\n"
            "category: 测试\n"
            "last_updated: '2026-01-01T00:00:00Z'\n"
            f"sources: [{historical}]\n"
            "related_concepts: [工作流]\n"
            "related_customers: [客户A]\n"
            "latest_version: v0\n"
            "---\n\n# 示例\n\n旧正文\n",
            encoding="utf-8",
        )
        coverage_path = root / "coverage.json"
        coverage_path.write_text(
            json.dumps(
                {
                    "schema": "concept-v11.source-coverage-report.v1",
                    "status": "PASS",
                    "report_hash": "sha256:coverage",
                    "source_manifest_hash": "sha256:manifest",
                    "concepts": [
                        {
                            "concept": "示例",
                            "coverage_status": "refreshable",
                            "references": [
                                {"source_uri": current, "source_map_status": "mapped", "disposition": "substituted"},
                                {"source_uri": historical, "source_map_status": "quarantined", "disposition": "historical_exclusion"},
                            ],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        preflight_path = root / "preflight.json"
        preflight_path.write_text(
            json.dumps(
                {
                    "schema": "concept-v11.content-source-preflight.v1",
                    "status": "PASS",
                    "concepts": [
                        {
                            "concept": "示例",
                            "content_status": "ready",
                            "historical_source_refs": [historical],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        evidence_path = root / "evidence.json"
        content = "当前来源明确支持创建和查看。\n\n当前来源未说明跨租户发布。"
        evidence_path.write_text(
            json.dumps(
                {
                    "schema": RECOMPILE_SCHEMA + ".evidence.v1",
                    "coverage_report_hash": "sha256:coverage",
                    "sources": [{"uri": current, "status": "ok", "content": content, "content_sha256": "sha256:" + hashlib.sha256(content.encode()).hexdigest()}],
                    "evidence_hash": "sha256:evidence",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return concept_root, coverage_path, preflight_path, evidence_path

    def test_current_and_historical_sources_are_separated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            concept_root, coverage, preflight, evidence = self._fixture(root)
            plan = build_plan(
                coverage_path=coverage,
                preflight_path=preflight,
                evidence_path=evidence,
                concept_root=concept_root,
                observed_at="2026-09-03T09:00:00Z",
                expected_count=1,
            )
            self.assertEqual(plan["status"], "PASS")
            page = plan["changes"][0]["content"]
            before, after = page.split("## 历史资料边界（不作为当前能力依据）", 1)
            self.assertIn("viking://resources/current/示例.md", before)
            self.assertNotIn("viking://resources/history/示例.md", before)
            self.assertIn("viking://resources/history/示例.md", after)
            applied = apply_plan(plan, backup_root=root / "backups")
            self.assertEqual(applied["status"], "PASS")
            self.assertTrue((concept_root / "state/pages/示例.md").read_text(encoding="utf-8").startswith("---\n"))

    def test_evidence_collection_is_fail_closed_for_target_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            concept_root, coverage, preflight, _ = self._fixture(root)
            with self.assertRaisesRegex(ValueError, "target_count_mismatch"):
                collect_evidence(
                    coverage_path=coverage,
                    preflight_path=preflight,
                    output=root / "out.json",
                    expected_count=34,
                    workers=1,
                )


if __name__ == "__main__":
    unittest.main()
