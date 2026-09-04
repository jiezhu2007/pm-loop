from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_content_source_preflight import build_preflight  # noqa: E402
from concept_v11_rebuild_pages import SOURCES, apply_plan, build_plan  # noqa: E402


def coverage() -> dict:
    concepts = []
    for name, sources in SOURCES.items():
        concepts.append(
            {
                "concept": name,
                "concept_id": "concept-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:12],
                "coverage_status": "refreshable",
                "references": [
                    {
                        "source_uri": source,
                        "source_map_status": "mapped",
                        "disposition": "mapped",
                    }
                    for source in sources
                ],
            }
        )
    return {
        "schema": "concept-v11.source-coverage-report.v1",
        "status": "PASS",
        "report_hash": "sha256:coverage",
        "source_manifest_hash": "sha256:manifest",
        "concepts": concepts,
    }


class ConceptCurrentSourceRebuildTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path]:
        concept_root = root / "concept-root"
        pages = concept_root / "state" / "pages"
        pages.mkdir(parents=True)
        for name in SOURCES:
            (pages / f"{name}.md").write_text(
                "---\n"
                f"concept: {name}\n"
                "aliases: []\n"
                "category: test\n"
                "last_updated: '2026-01-01T00:00:00Z'\n"
                "sources: []\n"
                "related_concepts: []\n"
                "related_customers: []\n"
                "latest_version: v0\n"
                "---\n\n"
                f"# {name}\n\nlegacy\n",
                encoding="utf-8",
            )
        coverage_path = root / "coverage.json"
        coverage_path.write_text(json.dumps(coverage(), ensure_ascii=False), encoding="utf-8")
        return concept_root, coverage_path

    def test_rebuilds_all_pages_and_closes_content_source_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            concept_root, coverage_path = self._fixture(root)
            plan = build_plan(
                coverage_path=coverage_path,
                concept_root=concept_root,
                observed_at="2026-09-03T06:00:00Z",
            )
            self.assertEqual(plan["status"], "PASS")
            self.assertEqual(plan["change_count"], len(SOURCES))
            self.assertEqual(plan["external_calls"], {"oneapi": 0, "openviking": 0})
            applied = apply_plan(plan, backup_root=root / "backups")
            self.assertEqual(applied["status"], "PASS")
            self.assertTrue(Path(applied["backup_dir"]).is_dir())
            preflight = build_preflight(
                coverage=coverage(),
                concept_root=concept_root,
                expected_concept_count=len(SOURCES),
                observed_at="2026-09-03T06:00:00Z",
            )
            self.assertEqual(preflight["status"], "PASS", preflight)
            self.assertEqual(preflight["summary"]["ready"], len(SOURCES))
            page = (concept_root / "state" / "pages" / "数据安全.md").read_text(encoding="utf-8")
            self.assertIn("动态脱敏", page)
            self.assertIn(SOURCES["数据安全"][0], page)

    def test_fails_closed_when_current_coverage_drifted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            concept_root, coverage_path = self._fixture(root)
            changed = coverage()
            changed["concepts"][0]["references"].pop()
            coverage_path.write_text(json.dumps(changed, ensure_ascii=False), encoding="utf-8")
            plan = build_plan(coverage_path=coverage_path, concept_root=concept_root, observed_at="2026-09-03T06:00:00Z")
            self.assertEqual(plan["status"], "HOLD")
            self.assertIn("ValueError:coverage_sources_drift:API生成", plan["errors"])

    def test_refuses_to_overwrite_concurrently_changed_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            concept_root, coverage_path = self._fixture(root)
            plan = build_plan(coverage_path=coverage_path, concept_root=concept_root, observed_at="2026-09-03T06:00:00Z")
            page = concept_root / "state" / "pages" / "API生成.md"
            page.write_text("concurrent change\n", encoding="utf-8")
            result = apply_plan(plan, backup_root=root / "backups")
            self.assertEqual(result["status"], "HOLD")
            self.assertIn("page_changed_since_plan:API生成.md", result["errors"])


if __name__ == "__main__":
    unittest.main()
