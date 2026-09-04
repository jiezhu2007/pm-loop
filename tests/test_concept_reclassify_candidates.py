from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import concept_reclassify_candidates as reclassify  # noqa: E402
from concept_learning import ConceptLearningStore, make_candidate  # noqa: E402


class CandidateReclassificationTest(unittest.TestCase):
    def _store(self, root: Path) -> ConceptLearningStore:
        skill_root = root / "skill"
        skill_root.mkdir(parents=True)
        (skill_root / "config.yaml").write_text(
            "concepts:\n"
            "  - name: 计算资源\n"
            "    aliases: [compute, 资源管理]\n"
            "  - name: 数据授权\n"
            "    aliases: [行列权限]\n",
            encoding="utf-8",
        )
        store = ConceptLearningStore(skill_root)
        store.save_ledger(
            {
                "计算资源": {"status": "active", "aliases": ["compute", "资源管理"]},
                "数据授权": {"status": "active", "aliases": ["行列权限"]},
            }
        )
        return store

    def _candidate(
        self,
        store: ConceptLearningStore,
        concept: str,
        *,
        status: str = "ready_for_review",
        kind: str = "new_concept",
    ):
        content = f"# {concept}\n\n证据正文"
        candidate = make_candidate(
            concept=concept,
            kind=kind,
            content=content,
            status=status,
            source_refs=["viking://evidence/a"],
            evidence=[{"uri": "viking://evidence/a", "status": "available"}],
        )
        return store.save_candidate(candidate, content)

    def test_dry_run_finds_exact_alias_and_fuzzy_merge_without_writing(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            exact = self._candidate(store, "compute")
            fuzzy = self._candidate(store, "资源队列")
            before = {
                item["candidate_id"]: store.candidate_path(item["candidate_id"]).read_bytes()
                for item in (exact, fuzzy)
            }

            plan = reclassify.build_plan(store)

            self.assertEqual(plan["active_concept_count"], 2)
            self.assertEqual(plan["matched_count"], 2)
            decisions = {item["concept"]: item["to_kind"] for item in plan["items"]}
            self.assertEqual(decisions, {"compute": "alias", "资源队列": "merge"})
            match_types = {item["concept"]: item["match_type"] for item in plan["items"]}
            self.assertEqual(match_types, {"compute": "exact", "资源队列": "controlled_fuzzy"})
            reasons = {item["concept"]: item["reason"] for item in plan["items"]}
            self.assertIn("完全一致", reasons["compute"])
            self.assertIn("受控资源", reasons["资源队列"])
            for candidate_id, content in before.items():
                self.assertEqual(store.candidate_path(candidate_id).read_bytes(), content)

    def test_apply_preserves_content_and_evidence_and_writes_audit(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            candidate = self._candidate(store, "行权限")
            content_path = Path(candidate["content_path"])
            content_before = content_path.read_bytes()
            evidence_before = candidate["evidence"]
            refs_before = candidate["source_refs"]

            result = reclassify.apply_plan(store, reclassify.build_plan(store), actor="test")

            self.assertEqual(result["applied_count"], 1)
            self.assertEqual(result["conflict_count"], 0)
            current = store.read_candidate(candidate["candidate_id"])
            self.assertEqual(current["kind"], "merge")
            self.assertEqual(current["status"], "superseded")
            self.assertEqual(current["triage_decision"], "merge")
            self.assertEqual(current["merge_target"], "数据授权")
            self.assertEqual(current["matcher_version"], "active-match-v1")
            self.assertEqual(current["migration_version"], "candidate-reclassification-v1")
            self.assertEqual(current["reclassified_at"], current["superseded_at"])
            self.assertEqual(current["reclassification"]["target"], "数据授权")
            self.assertEqual(current["reclassification"]["actor"], "test")
            self.assertEqual(current["evidence"], evidence_before)
            self.assertEqual(current["source_refs"], refs_before)
            self.assertEqual(content_path.read_bytes(), content_before)
            audit = (store.state_root / "logs" / "concept-agent-audit.jsonl").read_text(encoding="utf-8")
            self.assertIn('"event": "candidate.reclassified"', audit)
            self.assertIn('"target": "数据授权"', audit)

    def test_protected_and_terminal_candidates_are_skipped(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            protected = self._candidate(store, "compute", status="approved")
            terminal = self._candidate(store, "资源队列", status="published")

            plan = reclassify.build_plan(store)

            self.assertEqual(plan["matched_count"], 0)
            self.assertEqual(store.read_candidate(protected["candidate_id"])["status"], "approved")
            self.assertEqual(store.read_candidate(terminal["candidate_id"])["status"], "published")

    def test_candidate_only_name_is_not_an_active_target(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            self._candidate(store, "未审核产品")
            duplicate = self._candidate(store, "未审核产品")

            plan = reclassify.build_plan(store)

            self.assertNotIn(duplicate["candidate_id"], {item["candidate_id"] for item in plan["items"]})

    def test_file_hash_cas_prevents_overwriting_concurrent_change(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            candidate = self._candidate(store, "compute")
            plan = reclassify.build_plan(store)
            current = store.read_candidate(candidate["candidate_id"])
            current["review_note"] = "本人刚写入的意见"
            store.save_candidate(current)

            result = reclassify.apply_plan(store, plan)

            self.assertEqual(result["applied_count"], 0)
            self.assertEqual(result["conflict_count"], 1)
            after = store.read_candidate(candidate["candidate_id"])
            self.assertEqual(after["kind"], "new_concept")
            self.assertEqual(after["status"], "ready_for_review")
            self.assertEqual(after["review_note"], "本人刚写入的意见")

    def test_apply_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            self._candidate(store, "compute")

            first = reclassify.apply_plan(store, reclassify.build_plan(store))
            second_plan = reclassify.build_plan(store)
            second = reclassify.apply_plan(store, second_plan)

            self.assertEqual(first["applied_count"], 1)
            self.assertEqual(second_plan["matched_count"], 0)
            self.assertEqual(second["applied_count"], 0)
            audit_lines = (store.state_root / "logs" / "concept-agent-audit.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(audit_lines), 1)

    def test_plan_file_hash_matches_raw_candidate_file(self):
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            candidate = self._candidate(store, "compute")

            item = reclassify.build_plan(store)["items"][0]

            expected = "sha256:" + hashlib.sha256(
                store.candidate_path(candidate["candidate_id"]).read_bytes()
            ).hexdigest()
            self.assertEqual(item["candidate_file_sha256"], expected)


if __name__ == "__main__":
    unittest.main()
