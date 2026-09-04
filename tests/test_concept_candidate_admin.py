import json
import tempfile
import unittest
from pathlib import Path

from scripts import concept_candidate_admin as admin
from scripts.concept_learning import ConceptLearningStore, make_candidate


OLD_PAGE = """---\nconcept: 示例\ncategory: test\nsources:\n- viking://source/old\n---\n# 示例\n\n## 定义\n旧内容\n\n## 能力边界（能做什么）\n- 旧能力\n\n## 已知限制（不能做什么/需定制）\n- 旧限制\n\n## 版本演进\n- v1\n\n## 关联概念\n- 无\n\n## 出现过的客户/评估\n- 无\n"""


def make_root(root: Path) -> Path:
    skill = root / "skill"
    (skill / "state" / "candidates" / "content").mkdir(parents=True)
    (skill / "state" / "logs").mkdir(parents=True)
    (skill / "config.yaml").write_text("concepts: []\n", encoding="utf-8")
    (skill / "state" / "concepts-ledger.json").write_text("{}\n", encoding="utf-8")
    return skill


class CandidateAdminTests(unittest.TestCase):
    def _refresh(self, store: ConceptLearningStore, concept: str) -> dict:
        content = OLD_PAGE.replace("示例", concept)
        candidate = store.save_candidate(
            make_candidate(
                concept=concept,
                kind="refresh",
                content=content,
                before=OLD_PAGE,
                source_refs=[f"viking://source/{concept}"],
                evidence=[{"uri": f"viking://source/{concept}"}],
                status="ready_for_review",
                base_page_sha256="sha256:base",
            ),
            content,
        )
        return candidate

    def test_refresh_batch_requires_expected_count_and_preserves_content(self):
        with tempfile.TemporaryDirectory() as temp:
            skill = make_root(Path(temp))
            store = ConceptLearningStore(skill)
            candidates = [self._refresh(store, name) for name in ("甲", "乙")]
            plan = admin.build_refresh_supersede_plan(store, expected_count=2)
            result = admin.apply_refresh_supersede_plan(store, plan, actor="zhujie14")

            self.assertEqual(result["applied_count"], 2)
            self.assertEqual(result["conflict_count"], 0)
            for candidate in candidates:
                current = store.read_candidate(candidate["candidate_id"])
                self.assertEqual(current["status"], "superseded")
                self.assertEqual(current["review_decision"], "not_required")
                self.assertEqual(current["duplicate_decision"], "user_confirmed_duplicate_refresh")
                self.assertEqual(current["content_hash"], candidate["content_hash"])
                self.assertEqual(Path(current["content_path"]).read_text(encoding="utf-8"), Path(candidate["content_path"]).read_text(encoding="utf-8"))
            audit = (skill / "state" / "logs" / "concept-agent-audit.jsonl").read_text(encoding="utf-8")
            self.assertEqual(audit.count('"event": "candidate.superseded"'), 2)

    def test_refresh_batch_cas_conflict_does_not_overwrite_review_edit(self):
        with tempfile.TemporaryDirectory() as temp:
            skill = make_root(Path(temp))
            store = ConceptLearningStore(skill)
            candidate = self._refresh(store, "甲")
            plan = admin.build_refresh_supersede_plan(store, expected_count=1)
            edited = store.read_candidate(candidate["candidate_id"])
            edited["review_note"] = "本人新意见"
            store.save_candidate(edited)

            result = admin.apply_refresh_supersede_plan(store, plan)

            self.assertEqual(result["applied_count"], 0)
            self.assertEqual(result["conflict_count"], 1)
            current = store.read_candidate(candidate["candidate_id"])
            self.assertEqual(current["status"], "ready_for_review")
            self.assertEqual(current["review_note"], "本人新意见")

    def test_restore_creates_new_lineage_without_reviving_terminal_candidate(self):
        with tempfile.TemporaryDirectory() as temp:
            skill = make_root(Path(temp))
            store = ConceptLearningStore(skill)
            source_content = OLD_PAGE.replace("示例", "通用资源队列")
            source = store.save_candidate(
                make_candidate(
                    concept="通用资源队列",
                    kind="merge",
                    content=source_content,
                    source_refs=["viking://source/a", "viking://source/b"],
                    evidence=[{"uri": "viking://source/a"}, {"uri": "viking://source/b"}],
                    status="superseded",
                ),
                source_content,
            )
            reviewed = skill / "reviewed.md"
            reviewed.write_text(source_content, encoding="utf-8")

            restored = admin.restore_new_concept_candidate(
                store,
                source["candidate_id"],
                reviewed,
                actor="zhujie14",
                note="本人确认独立概念",
            )

            self.assertNotEqual(restored["candidate_id"], source["candidate_id"])
            self.assertEqual(restored["kind"], "new-concept")
            self.assertEqual(restored["status"], "ready_for_review")
            self.assertEqual(restored["restored_from_candidate_id"], source["candidate_id"])
            self.assertEqual(store.read_candidate(source["candidate_id"])["status"], "superseded")
            self.assertEqual(len(restored["source_refs"]), 2)
            audit = (skill / "state" / "logs" / "concept-agent-audit.jsonl").read_text(encoding="utf-8")
            self.assertIn('"event": "candidate.restored_for_active"', audit)

    def test_restore_rejects_existing_active_concept(self):
        with tempfile.TemporaryDirectory() as temp:
            skill = make_root(Path(temp))
            store = ConceptLearningStore(skill)
            ledger = {"通用资源队列": {"status": "active"}}
            store.save_ledger(ledger)
            content = OLD_PAGE.replace("示例", "通用资源队列")
            source = store.save_candidate(
                make_candidate(
                    concept="通用资源队列",
                    kind="merge",
                    content=content,
                    source_refs=["viking://source/a"],
                    status="superseded",
                ),
                content,
            )
            reviewed = skill / "reviewed.md"
            reviewed.write_text(content, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "already Active"):
                admin.restore_new_concept_candidate(store, source["candidate_id"], reviewed)


if __name__ == "__main__":
    unittest.main()
