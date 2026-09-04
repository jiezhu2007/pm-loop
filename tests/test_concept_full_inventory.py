from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_full_inventory import enumerate_resources, execute, inventory_path, select_evidence_resources  # noqa: E402
from concept_learning import ConceptLearningStore  # noqa: E402


class FakeOpenViking:
    def __init__(self, uris, unreadable=()) -> None:
        self.uris = list(uris)
        self.unreadable = set(unreadable)
        self.reads = []

    def glob(self, root, pattern, node_limit):
        self.glob_args = (root, pattern, node_limit)
        return self.uris[:node_limit]

    def evidence_text(self, uri, limit):
        self.reads.append(uri)
        if uri in self.unreadable:
            raise OSError("unreadable")
        return (f"真实产品证据 {uri}。包含稳定定义、能力边界和限制。" * 4)[:limit]


def discovery_output(prompt):
    marker = "INPUT="
    payload = json.loads(prompt[prompt.index(marker) + len(marker) :])
    refs = [item["uri"] for item in payload]
    return 0, json.dumps(
        {
            "decisions": [
                {
                    "decision": "new_concept",
                    "name": "DataAgent",
                    "aliases": ["数据智能体"],
                    "category": "AI 数据应用",
                    "evidence_uris": refs,
                    "reason": ["多份产品证据具有独立边界"],
                    "confidence": 0.82,
                }
            ]
        },
        ensure_ascii=False,
    )


def consolidator(prompt):
    if "消除同义" not in prompt:
        return discovery_output(prompt)
    input_text = prompt.split("INPUT=", 1)[1]
    rows = json.loads(input_text)
    refs = list(dict.fromkeys(uri for row in rows for uri in row["evidence_uris"]))
    content = "# DataAgent\n\n## 定义\n基于真实资料的数据智能体能力。\n\n## 能力边界\n仅覆盖证据明确描述的能力。\n\n## 已知限制\n交付边界需要本人审核。\n\n## 关联概念\n与数据搜索有关。\n\n## 证据与待确认点\n以下来源需要逐项复核。" * 2
    return 0, json.dumps(
        {
            "decisions": [
                {
                    "decision": "new_concept",
                    "name": "DataAgent",
                    "aliases": ["数据智能体"],
                    "category": "AI 数据应用",
                    "content": content,
                    "evidence_uris": refs,
                    "reason": ["两份真实证据"],
                    "confidence": 0.86,
                },
                {
                    "decision": "alias",
                    "name": "DataSearch",
                    "target": "数据搜索",
                    "evidence_uris": refs[:1],
                    "confidence": 0.95,
                },
            ]
        },
        ensure_ascii=False,
    )


class FullInventoryTests(unittest.TestCase):
    def _store(self, root: Path) -> ConceptLearningStore:
        skill = root / "codex" / "skills" / "shengsuan-concepts"
        skill.mkdir(parents=True)
        (skill / "config.yaml").write_text("concepts:\n  - name: 数据搜索\n    aliases: [DataSearch]\n", encoding="utf-8")
        return ConceptLearningStore(skill)

    def test_recursive_enumeration_excludes_concept_pages(self) -> None:
        uris = [
            "viking://resources/shengsuan/data-agent/a.md",
            "viking://resources/shengsuan/concepts/数据搜索.md",
            "viking://resources/shengsuan/public-docs/b.md",
        ]
        client = FakeOpenViking(uris)
        found = enumerate_resources(client, ["viking://resources/shengsuan"], ["viking://resources/shengsuan/concepts"], 100)
        self.assertEqual(found, [uris[0], uris[2]])
        self.assertEqual(client.glob_args[1], "**/*.md")

    def test_enumeration_rejects_possible_truncation(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not provably complete"):
            enumerate_resources(FakeOpenViking(["viking://a.md", "viking://b.md"]), ["viking://resources"], [], 2)

    def test_bounded_selection_is_stratified_and_prioritizes_seed(self) -> None:
        roots = ["viking://resources/shengsuan/a", "viking://resources/shengsuan/b"]
        resources = [f"{roots[0]}/{index:03}.md" for index in range(100)] + [f"{roots[1]}/{index:03}.md" for index in range(10)]
        resources.append(f"{roots[0]}/AI-FDE-overview.md")
        selected = select_evidence_resources(resources, roots, 20, ["AI-FDE"])
        self.assertEqual(len(selected), 20)
        self.assertIn(f"{roots[0]}/AI-FDE-overview.md", selected)
        self.assertTrue(any(uri.startswith(roots[1] + "/") for uri in selected))

    def test_inventory_creates_only_new_concept_candidate_and_keeps_alias_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uris = ["viking://resources/shengsuan/data-agent/overview.md", "viking://resources/shengsuan/data-agent/prd.md"]
            result = execute(store, FakeOpenViking(uris), consolidator, batch_size=1, node_limit=20)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["summary"]["resources"], 2)
            self.assertEqual(result["result"]["schema_version"], "concept-learning.inventory.v1")
            self.assertEqual(result["result"]["snapshot"]["status"], "ok")
            self.assertEqual(result["result"]["snapshot"]["file_count"], 2)
            self.assertEqual(result["result"]["triage"]["status"], "complete")
            self.assertEqual(len(result["result"]["discovery_run_ids"]), 1)
            self.assertEqual(len(result["candidate_ids"]), 1)
            candidate = store.read_candidate(result["candidate_ids"][0])
            self.assertEqual(candidate["concept"], "DataAgent")
            self.assertEqual(candidate["kind"], "new_concept")
            self.assertEqual(candidate["source_refs"], uris)
            self.assertEqual(candidate["inventory_run_id"], result["run_id"])
            self.assertEqual([row["decision"] for row in result["decisions"]], ["new_concept", "alias"])

    def test_resume_starts_after_persisted_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uris = ["viking://resources/shengsuan/a.md", "viking://resources/shengsuan/b.md"]
            client = FakeOpenViking(uris)
            calls = 0

            def fail_second(prompt):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("interrupted")
                return 0, json.dumps({"decisions": []})

            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                execute(store, client, fail_second, batch_size=1, node_limit=20)
            run_file = next((store.state_root / "full-inventory" / "runs").glob("*.json"))
            failed = json.loads(run_file.read_text(encoding="utf-8"))
            self.assertEqual(failed["scan_cursor"], 1)
            client.reads.clear()

            completed = execute(store, client, lambda prompt: (0, json.dumps({"decisions": []})), batch_size=1, resume_run_id=failed["run_id"])
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(client.reads, [uris[1]])
            self.assertEqual(inventory_path(store, failed["run_id"]), run_file)

    def test_parallel_batches_complete_and_persist_contiguous_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uris = [f"viking://resources/shengsuan/{index}.md" for index in range(4)]
            barrier = threading.Barrier(2)

            def parallel_empty(prompt):
                barrier.wait(timeout=2)
                return 0, json.dumps({"decisions": []})

            result = execute(
                store,
                FakeOpenViking(uris),
                parallel_empty,
                batch_size=1,
                node_limit=20,
                workers=2,
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["scan_cursor"], 4)
            self.assertEqual(result["progress"], {"processed": 4, "total": 4, "workers": 2})

    def test_new_candidate_requires_two_real_refs_and_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uri = "viking://resources/shengsuan/only.md"

            def weak_consolidator(prompt):
                if "消除同义" not in prompt:
                    return discovery_output(prompt)
                content = "# 薄弱概念\n\n## 定义\n证据不足。\n\n## 能力边界\n待确认。\n\n## 已知限制\n待确认。" * 4
                return 0, json.dumps({"decisions": [{"decision": "new_concept", "name": "薄弱概念", "content": content, "evidence_uris": [uri], "confidence": 0.9}]}, ensure_ascii=False)

            result = execute(store, FakeOpenViking([uri]), weak_consolidator, node_limit=20)
            self.assertEqual(result["candidate_ids"], [])
            self.assertEqual(store.list_candidates(), [])

    def test_unreadable_evidence_is_explicit_partial_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uris = ["viking://resources/shengsuan/readable.md", "viking://resources/shengsuan/missing.md"]
            result = execute(
                store,
                FakeOpenViking(uris, unreadable=[uris[1]]),
                lambda prompt: (0, json.dumps({"decisions": []})),
                node_limit=20,
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["result"]["triage"]["status"], "partial-complete")
            self.assertEqual(result["result"]["triage"]["unreadable"], 1)

    def test_repeated_inventory_reuses_evidence_run_without_rewriting_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uris = ["viking://resources/shengsuan/a.md", "viking://resources/shengsuan/b.md"]
            empty = lambda prompt: (0, json.dumps({"decisions": []}))
            first = execute(store, FakeOpenViking(uris), empty, node_limit=20)
            second = execute(store, FakeOpenViking(uris), empty, node_limit=20)
            self.assertNotEqual(first["run_id"], second["run_id"])
            self.assertEqual(first["discovery_run_ids"], second["discovery_run_ids"])
            discovery = store.discovery_runs()[0]
            self.assertEqual(discovery["inventory_run_id"], first["run_id"])


if __name__ == "__main__":
    unittest.main()
