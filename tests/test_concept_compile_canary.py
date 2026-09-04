from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.concept_compile_canary import run_canary


class ConceptCompileCanaryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path, Path]:
        skill = root / "skill"
        pages = skill / "state" / "pages"
        pages.mkdir(parents=True)
        page = "---\nconcept: Demo\nsources:\n- viking://source/demo\n---\n# Demo\n\n## 定义\n旧内容\n"
        (pages / "Demo.md").write_text(page, encoding="utf-8")
        ledger = {"Demo": {"status": "active", "viking_uri": "viking://resources/demo"}}
        (skill / "state" / "concepts-ledger.json").write_text(json.dumps(ledger), encoding="utf-8")
        coverage = {
            "schema": "concept-v11.source-coverage-report.v1",
            "status": "PASS",
            "gate": {"p3_closed": True},
            "concepts": [{
                "concept": "Demo",
                "coverage_status": "refreshable",
                "references": [{
                    "map_id": "map-demo",
                    "source_uri": "viking://source/demo",
                    "source_map_status": "mapped",
                    "disposition": "mapped",
                    "evidence_set_hash": "sha256:demo",
                }],
            }],
        }
        coverage_path = root / "source-coverage-current.json"
        coverage_path.write_text(json.dumps(coverage), encoding="utf-8")
        return skill, coverage_path, pages / "Demo.md"

    def test_disconnect_recovery_atomic_publish_and_idempotent_replay_are_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill, coverage, page = self._fixture(Path(temp))
            ledger_before = (skill / "state" / "concepts-ledger.json").read_bytes()
            page_before = page.read_bytes()
            result = run_canary(
                skill_root=skill,
                coverage_path=coverage,
                concepts=("Demo",),
                work_dir=Path(temp) / "canary",
                idempotency_key="test-key",
            )

            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["model_recovery"]["calls"], 2)
            self.assertTrue(result["model_recovery"]["same_model_input_hash_on_retry"])
            self.assertTrue(result["atomic_generation"]["rollback_injection_pass"])
            self.assertTrue(result["idempotency"]["replay_same_candidate"])
            self.assertEqual(result["idempotency"]["replay_provider_calls"], 0)
            self.assertEqual((skill / "state" / "concepts-ledger.json").read_bytes(), ledger_before)
            self.assertEqual(page.read_bytes(), page_before)

            replay = run_canary(
                skill_root=skill,
                coverage_path=coverage,
                concepts=("Demo",),
                work_dir=Path(temp) / "canary",
                idempotency_key="test-key",
            )
            self.assertEqual(replay["status"], "PASS")
            self.assertEqual(replay["execution_mode"], "replay")
            self.assertEqual(replay["model_recovery"]["calls"], 0)
            self.assertTrue(replay["idempotency"]["replay_same_candidate"])

    def test_unmapped_source_is_blocked_before_any_canary_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill, coverage, _page = self._fixture(Path(temp))
            value = json.loads(coverage.read_text(encoding="utf-8"))
            value["concepts"][0]["references"][0]["disposition"] = "historical_exclusion"
            coverage.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "no current mapped source"):
                run_canary(skill_root=skill, coverage_path=coverage, concepts=("Demo",), work_dir=Path(temp) / "canary")


if __name__ == "__main__":
    unittest.main()
