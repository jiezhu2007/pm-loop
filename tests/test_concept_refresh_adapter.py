from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import concept_refresh_adapter as adapter  # noqa: E402
from concept_learning import ConceptLearningStore, make_candidate  # noqa: E402


ACTIVE_CONCEPT = "现有概念"
NEW_CONCEPT = "新发现概念"
OLD_PAGE = "---\nconcept: 现有概念\nsources:\n  - viking://source/old\n---\n\n# 现有概念\n\n旧正文\n"
NEW_PAGE = "---\nconcept: 现有概念\nsources:\n  - viking://source/old\n  - viking://source/new\n---\n\n# 现有概念\n\n新正文\n"


def write_config(skill_root: Path, concepts: list[dict[str, object]]) -> None:
    value = {
        "settings": {
            "viking_namespace": "viking://resources/shengsuan/concepts",
            "search_targets": ["viking://resources/shengsuan"],
            "score_threshold": 0.55,
            "max_docs_per_concept": 10,
            "max_chars_per_doc": 8000,
        },
        "concepts": concepts,
    }
    (skill_root / "config.yaml").write_text(
        json.dumps(value, ensure_ascii=False),
        encoding="utf-8",
    )


def make_skill_root(root: Path, *, include_active_config: bool = True) -> Path:
    skill_root = root / "shengsuan-concepts"
    (skill_root / "state" / "pages").mkdir(parents=True)
    (skill_root / "prompts").mkdir(parents=True)
    (skill_root / "prompts" / "incremental-update.md").write_text(
        "概念：{concept_name}\n\n已有：\n{existing_page}\n\n证据：\n{new_documents}\n",
        encoding="utf-8",
    )
    concepts: list[dict[str, object]] = []
    if include_active_config:
        concepts.append(
            {
                "name": ACTIVE_CONCEPT,
                "category": "测试",
                "search_keywords": [ACTIVE_CONCEPT],
            }
        )
    write_config(skill_root, concepts)
    return skill_root


def write_ledger(skill_root: Path, value: dict[str, object]) -> Path:
    path = skill_root / "state" / "concepts-ledger.json"
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def active_record() -> dict[str, object]:
    return {
        "status": "active",
        "current_version": "v1",
        "viking_uri": f"viking://resources/shengsuan/concepts/{ACTIVE_CONCEPT}.md",
        "last_updated": "2026-08-15T00:00:00Z",
        "sources": ["viking://source/old"],
        "category": "测试",
    }


def fake_publish_modules(
    skill_root: Path,
    upload_result: str | None = None,
) -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace, Mock]:
    page_root = skill_root / "state" / "pages"
    namespace = "viking://resources/shengsuan/concepts"

    def upload_page(name: str, content: str, _namespace: str) -> str:
        # Mirror the real lib_pages ordering: local Active is written before
        # OpenViking reports success or failure.
        (page_root / f"{name}.md").write_text(content, encoding="utf-8")
        if upload_result is None:
            return f"{namespace}/{name}.md"
        return upload_result

    uploader = Mock(side_effect=upload_page)
    return (
        SimpleNamespace(),
        SimpleNamespace(upload_page=uploader),
        SimpleNamespace(),
        Mock(),
    )


def fake_proposal_modules(
    *,
    existing_page: str = OLD_PAGE,
    hits: list[dict[str, object]] | None = None,
    contents: dict[str, str] | None = None,
) -> tuple[tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace, Mock], dict[str, str], Mock]:
    source_hits = hits or [
        {
            "uri": "viking://source/new",
            "source": "internal-shengsuan",
            "score": 0.91,
            "query": ACTIVE_CONCEPT,
        }
    ]
    source_contents = contents or {"viking://source/new": "新证据正文"}
    lib_pages = SimpleNamespace(
        read_page=Mock(return_value=existing_page),
        upload_page=Mock(side_effect=AssertionError("proposal must not upload")),
    )
    ov_search = SimpleNamespace(
        search_concept=Mock(return_value=source_hits),
        read_content=Mock(side_effect=lambda uri: source_contents[uri]),
    )
    fm = SimpleNamespace(sanitize_llm_output=lambda value: value)
    llm_result = subprocess.CompletedProcess(args=["fake-llm"], returncode=0, stdout="", stderr="")
    run_prompt = Mock(return_value=(llm_result, NEW_PAGE))
    return (fm, lib_pages, ov_search, run_prompt), source_contents, run_prompt


class ConceptRefreshAdapterTests(unittest.TestCase):
    def test_incremental_timeout_retries_once_with_compact_evidence_and_low_effort(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            long_body = "证据开头\n" + ("中间证据。" * 1200) + "\n证据结尾"
            docs = [
                {
                    "uri": "viking://source/long",
                    "source": "test",
                    "content": long_body,
                }
            ]
            timed_out = subprocess.CompletedProcess(
                args=["fake-llm"], returncode=124, stdout="partial", stderr="TimeoutExpired"
            )
            completed = subprocess.CompletedProcess(args=["fake-llm"], returncode=0, stdout="", stderr="")
            run_prompt = Mock(side_effect=[(timed_out, ""), (completed, NEW_PAGE)])
            fm = SimpleNamespace(sanitize_llm_output=lambda value: value)

            with patch.dict(
                os.environ,
                {
                    "CONCEPTS_LLM_TIMEOUT": "150",
                    "CONCEPTS_LLM_RETRY_TIMEOUT": "120",
                    "CONCEPTS_LLM_RETRY_DOC_CHARS": "1800",
                },
                clear=False,
            ):
                content, mode = adapter.compile_content(
                    skill_root=skill_root,
                    config={},
                    concept={"name": ACTIVE_CONCEPT},
                    existing_page=OLD_PAGE,
                    docs=docs,
                    fm=fm,
                    run_prompt=run_prompt,
                )

            self.assertEqual(mode, "incremental")
            self.assertEqual(content, NEW_PAGE.strip())
            self.assertEqual(run_prompt.call_count, 2)
            first_call, second_call = run_prompt.call_args_list
            self.assertEqual(first_call.args[1], 150)
            self.assertEqual(second_call.args[1], 60)
            self.assertEqual(second_call.kwargs["reasoning_effort"], "low")
            self.assertLess(len(second_call.args[0]), len(first_call.args[0]))
            self.assertIn("evidence compacted for retry", second_call.args[0])
            self.assertIn("证据开头", second_call.args[0])
            self.assertIn("证据结尾", second_call.args[0])

    def test_incremental_non_timeout_failure_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            failed = subprocess.CompletedProcess(
                args=["fake-llm"], returncode=2, stdout="", stderr="provider error"
            )
            run_prompt = Mock(return_value=(failed, ""))
            with self.assertRaisesRegex(RuntimeError, "code 2"):
                adapter.compile_content(
                    skill_root=skill_root,
                    config={},
                    concept={"name": ACTIVE_CONCEPT},
                    existing_page=OLD_PAGE,
                    docs=[{"uri": "viking://source/new", "source": "test", "content": "证据"}],
                    fm=SimpleNamespace(sanitize_llm_output=lambda value: value),
                    run_prompt=run_prompt,
                )
            run_prompt.assert_called_once()

    def test_incremental_retry_has_total_budget_when_existing_page_is_large(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            large_page = (
                "---\nconcept: 现有概念\nsources:\n  - viking://source/old\n"
                "current_version: v99\n---\n\n"
                + "\n\n".join(
                    f"## 章节 {index}\n" + ("旧内容。" * 900) for index in range(1, 8)
                )
            )
            docs = [{
                "uri": "viking://source/new",
                "source": "test",
                "content": "新证据开头\n" + ("新证据。" * 3000) + "\n新证据结尾",
            }]
            timed_out = subprocess.CompletedProcess(
                args=["fake-llm"], returncode=124, stdout="", stderr="TimeoutExpired"
            )
            completed = subprocess.CompletedProcess(args=["fake-llm"], returncode=0, stdout="", stderr="")
            run_prompt = Mock(side_effect=[(timed_out, ""), (completed, NEW_PAGE)])
            with patch.dict(
                os.environ,
                {
                    "CONCEPTS_LLM_TIMEOUT": "150",
                    "CONCEPTS_LLM_RETRY_TIMEOUT": "60",
                    "CONCEPTS_LLM_RETRY_DOC_CHARS": "4000",
                    "CONCEPTS_LLM_RETRY_PROMPT_CHARS": "6000",
                },
                clear=False,
            ):
                content, _mode = adapter.compile_content(
                    skill_root=skill_root,
                    config={},
                    concept={"name": ACTIVE_CONCEPT},
                    existing_page=large_page,
                    docs=docs,
                    fm=SimpleNamespace(sanitize_llm_output=lambda value: value),
                    run_prompt=run_prompt,
                )

            self.assertEqual(content, NEW_PAGE.strip())
            retry_prompt = run_prompt.call_args_list[1].args[0]
            self.assertLessEqual(len(retry_prompt), 6000)
            self.assertIn("concept: 现有概念", retry_prompt)
            self.assertIn("current_version: v99", retry_prompt)
            self.assertIn("## 章节 1", retry_prompt)
            self.assertIn("## 章节 7", retry_prompt)
            self.assertIn("evidence compacted for retry", retry_prompt)
            self.assertLess(len(retry_prompt), len(run_prompt.call_args_list[0].args[0]) / 3)

    def test_propose_file_creates_correction_without_changing_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill_root = make_skill_root(root)
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            ledger_path = write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            reviewed = root / "reviewed.md"
            reviewed.write_text(
                "---\n"
                f"concept: {ACTIVE_CONCEPT}\n"
                "sources:\n  - viking://source/new\n"
                "---\n\n"
                f"# {ACTIVE_CONCEPT}\n\n"
                "## 定义\n修正版。\n\n"
                "## 能力边界（能做什么）\n- 有证据。\n\n"
                "## 已知限制（不能做什么/需定制）\n- 有边界。\n\n"
                "## 版本演进\n无。\n\n"
                "## 关联概念\n无。\n\n"
                "## 出现过的客户/评估\n无。\n",
                encoding="utf-8",
            )
            page_before = page_path.read_bytes()
            ledger_before = ledger_path.read_bytes()

            candidate = adapter.propose_file(
                skill_root,
                ACTIVE_CONCEPT,
                reviewed,
                actor="zhujie14",
                run_id="run-correction",
            )

            self.assertEqual(page_path.read_bytes(), page_before)
            self.assertEqual(ledger_path.read_bytes(), ledger_before)
            self.assertEqual(candidate["status"], "ready_for_review")
            self.assertEqual(candidate["kind"], "correction")
            self.assertEqual(candidate["source_strategy"], "replace")
            self.assertEqual(candidate["source_refs"], ["viking://source/new"])
            self.assertEqual(candidate["base_page_sha256"], adapter.sha256_text(OLD_PAGE))
            audit = (skill_root / "state" / "logs" / "concept-agent-audit.jsonl").read_text(encoding="utf-8")
            self.assertIn('"source_strategy": "replace"', audit)

    def test_propose_file_rejects_mismatched_frontmatter_concept(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill_root = make_skill_root(root)
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            reviewed = root / "reviewed.md"
            reviewed.write_text(
                "---\nconcept: 另一个概念\nsources:\n  - viking://source/new\n---\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "does not match"):
                adapter.propose_file(skill_root, ACTIVE_CONCEPT, reviewed)

    def test_candidate_only_concept_can_seed_another_evidence_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp), include_active_config=False)
            write_ledger(skill_root, {})
            store = ConceptLearningStore(skill_root)
            previous = store.save_candidate(
                make_candidate(
                    concept=NEW_CONCEPT,
                    kind="new-concept",
                    content="---\ntitle: 新发现概念\n---\n旧候选\n",
                    source_refs=["viking://source/old"],
                    status="changes_requested",
                ),
                "---\ntitle: 新发现概念\n---\n旧候选\n",
            )
            modules, _contents, _run_prompt = fake_proposal_modules(existing_page="")

            with patch.object(adapter, "_skill_modules", return_value=modules), patch.object(
                adapter,
                "compile_content",
                return_value=("---\ntitle: 新发现概念\n---\n新候选\n", "bootstrap"),
            ):
                current = adapter.propose_one(skill_root, NEW_CONCEPT, run_id="run-candidate-only")

            self.assertEqual(current["concept"], NEW_CONCEPT)
            self.assertEqual(current["run_id"], "run-candidate-only")
            self.assertEqual(current["status"], "ready_for_review")
            self.assertEqual(store.read_candidate(previous["candidate_id"])["status"], "superseded")

    def test_propose_does_not_change_active_page_or_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            ledger_path = write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            page_before = page_path.read_bytes()
            ledger_before = ledger_path.read_bytes()

            uploader = Mock(side_effect=AssertionError("proposal must not upload"))
            lib_pages = SimpleNamespace(
                read_page=Mock(return_value=OLD_PAGE),
                upload_page=uploader,
            )
            ov_search = SimpleNamespace(
                search_concept=Mock(
                    return_value=[
                        {
                            "uri": "viking://source/new",
                            "source": "internal-shengsuan",
                            "score": 0.91,
                            "query": ACTIVE_CONCEPT,
                        }
                    ]
                ),
                read_content=Mock(return_value="新证据正文"),
            )
            fm = SimpleNamespace(sanitize_llm_output=lambda value: value)
            llm_result = subprocess.CompletedProcess(args=["fake-llm"], returncode=0, stdout="", stderr="")
            run_prompt = Mock(return_value=(llm_result, NEW_PAGE))

            with patch.object(
                adapter,
                "_skill_modules",
                return_value=(fm, lib_pages, ov_search, run_prompt),
            ):
                candidate = adapter.propose_one(skill_root, ACTIVE_CONCEPT, run_id="run-test-propose")

            self.assertEqual(page_path.read_bytes(), page_before)
            self.assertEqual(ledger_path.read_bytes(), ledger_before)
            uploader.assert_not_called()
            self.assertEqual(candidate["status"], "ready_for_review")
            self.assertEqual(candidate["base_page_sha256"], adapter.sha256_text(OLD_PAGE))
            self.assertEqual(candidate["source_refs"], ["viking://source/new"])
            self.assertTrue(Path(candidate["content_path"]).is_file())
            self.assertEqual(Path(candidate["content_path"]).read_text(encoding="utf-8"), NEW_PAGE.strip())

    def test_repeated_propose_reuses_same_input_candidate_before_llm(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, _contents, run_prompt = fake_proposal_modules()

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.propose_one(skill_root, ACTIVE_CONCEPT, run_id="run-first")
                second = adapter.propose_one(skill_root, ACTIVE_CONCEPT, run_id="run-second")

            store = ConceptLearningStore(skill_root)
            self.assertEqual(first["candidate_id"], second["candidate_id"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(len(store.list_candidates(concept=ACTIVE_CONCEPT)), 1)
            run_prompt.assert_called_once()

    def test_concurrent_propose_serializes_to_one_candidate_and_one_llm_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, _contents, run_prompt = fake_proposal_modules()
            barrier = threading.Barrier(2)

            def propose(run_id: str) -> dict[str, object]:
                barrier.wait()
                return adapter.propose_one(skill_root, ACTIVE_CONCEPT, run_id=run_id)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    rows = list(executor.map(propose, ["run-a", "run-b"]))

            store = ConceptLearningStore(skill_root)
            self.assertEqual(len({str(row["candidate_id"]) for row in rows}), 1)
            self.assertEqual(len(store.list_candidates(concept=ACTIVE_CONCEPT)), 1)
            run_prompt.assert_called_once()

    def test_existing_candidate_fingerprint_ignores_source_order_and_fetched_at(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            record = active_record()
            write_ledger(skill_root, {ACTIVE_CONCEPT: record})
            hits = [
                {"uri": "viking://source/a", "source": "test", "score": 0.9, "query": ACTIVE_CONCEPT},
                {"uri": "viking://source/b", "source": "test", "score": 0.8, "query": ACTIVE_CONCEPT},
            ]
            contents = {
                "viking://source/a": "Evidence A",
                "viking://source/b": "Evidence B",
            }
            modules, _contents, run_prompt = fake_proposal_modules(hits=hits, contents=contents)
            store = ConceptLearningStore(skill_root)
            existing = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    base_ledger_last_updated=record["last_updated"],
                    source_snapshot=[
                        {
                            "uri": "viking://source/b",
                            "sha256": adapter.sha256_text("Evidence B"),
                            "fetched_at": "2026-01-01T00:00:00Z",
                        },
                        {
                            "uri": "viking://source/a",
                            "sha256": adapter.sha256_text("Evidence A"),
                            "fetched_at": "2026-02-01T00:00:00Z",
                        },
                    ],
                    status="paused",
                ),
                NEW_PAGE,
            )

            with patch.object(adapter, "_skill_modules", return_value=modules):
                reused = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            self.assertEqual(reused["candidate_id"], existing["candidate_id"])
            self.assertTrue(reused["deduplicated"])
            self.assertEqual(reused["status"], "paused")
            run_prompt.assert_not_called()

    def test_changed_source_hash_creates_new_candidate_and_supersedes_old(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, contents, run_prompt = fake_proposal_modules()

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.propose_one(skill_root, ACTIVE_CONCEPT)
                contents["viking://source/new"] = "发生变化的新证据"
                second = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            store = ConceptLearningStore(skill_root)
            self.assertNotEqual(first["candidate_id"], second["candidate_id"])
            self.assertEqual(store.read_candidate(first["candidate_id"])["status"], "superseded")
            self.assertEqual(store.read_candidate(second["candidate_id"])["status"], "ready_for_review")
            self.assertEqual(run_prompt.call_count, 2)

    def test_changed_active_base_creates_new_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, _contents, run_prompt = fake_proposal_modules()
            store = ConceptLearningStore(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.propose_one(skill_root, ACTIVE_CONCEPT)
                changed_page = OLD_PAGE + "\n人工更新\n"
                modules[1].read_page.return_value = changed_page
                changed_record = active_record()
                changed_record["current_version"] = "v2"
                changed_record["last_updated"] = "2026-08-17T00:00:00Z"
                write_ledger(skill_root, {ACTIVE_CONCEPT: changed_record})
                second = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            self.assertNotEqual(first["candidate_id"], second["candidate_id"])
            self.assertEqual(store.read_candidate(first["candidate_id"])["status"], "superseded")
            self.assertEqual(second["base_version"], "v2")
            self.assertEqual(second["base_page_sha256"], adapter.sha256_text(changed_page))
            self.assertEqual(run_prompt.call_count, 2)

    def test_changes_requested_allows_regeneration_with_same_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, _contents, run_prompt = fake_proposal_modules()
            store = ConceptLearningStore(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.propose_one(skill_root, ACTIVE_CONCEPT)
                store.update_candidate(first["candidate_id"], status="changes_requested")
                second = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            self.assertNotEqual(first["candidate_id"], second["candidate_id"])
            self.assertEqual(store.read_candidate(first["candidate_id"])["status"], "superseded")
            self.assertEqual(run_prompt.call_count, 2)

    def test_approved_candidate_defers_changed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            modules, contents, run_prompt = fake_proposal_modules()
            store = ConceptLearningStore(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.propose_one(skill_root, ACTIVE_CONCEPT)
                store.update_candidate(first["candidate_id"], status="approved", approved_by="zhujie14")
                contents["viking://source/new"] = "审批期间发生变化的证据"
                deferred = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            self.assertEqual(first["candidate_id"], deferred["candidate_id"])
            self.assertTrue(deferred["deferred"])
            self.assertFalse(deferred["deduplicated"])
            self.assertEqual(len(store.list_candidates(concept=ACTIVE_CONCEPT)), 1)
            run_prompt.assert_called_once()

    def test_older_approved_candidate_defers_even_when_newer_ready_row_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            (skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md").write_text(
                OLD_PAGE,
                encoding="utf-8",
            )
            record = active_record()
            write_ledger(skill_root, {ACTIVE_CONCEPT: record})
            modules, _contents, run_prompt = fake_proposal_modules()
            store = ConceptLearningStore(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                approved = adapter.propose_one(skill_root, ACTIVE_CONCEPT)
                store.update_candidate(approved["candidate_id"], status="approved", approved_by="zhujie14")
                store.save_candidate(
                    make_candidate(
                        concept=ACTIVE_CONCEPT,
                        kind="refresh",
                        content=NEW_PAGE,
                        before=OLD_PAGE,
                        base_version="v1",
                        base_page_sha256=adapter.sha256_text(OLD_PAGE),
                        base_ledger_last_updated=record["last_updated"],
                        source_snapshot=[
                            {"uri": "viking://source/new", "sha256": "sha256:legacy-race"}
                        ],
                        status="ready_for_review",
                    ),
                    NEW_PAGE,
                )
                deferred = adapter.propose_one(skill_root, ACTIVE_CONCEPT)

            self.assertEqual(deferred["candidate_id"], approved["candidate_id"])
            self.assertTrue(deferred["deferred"])
            run_prompt.assert_called_once()

    def test_approve_records_human_snapshot_and_concept_owned_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="ready_for_review",
                ),
                NEW_PAGE,
            )

            approved = adapter.approve_one(
                skill_root,
                candidate["candidate_id"],
                actor="zhujie14",
                note="用户明确要求修复概念新鲜度",
            )

            self.assertEqual(approved["status"], "approved")
            self.assertEqual(approved["approved_by"], "zhujie14")
            self.assertEqual(approved["approved_content_hash"], adapter.sha256_text(NEW_PAGE))
            self.assertEqual(approved["approval_note"], "用户明确要求修复概念新鲜度")
            self.assertEqual(approved["approval_source"], "shengsuan-concepts-cli")
            audit_path = skill_root / "state" / "logs" / adapter.AUDIT_LOG_NAME
            events = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(events[-1]["event"], "candidate.approved")
            self.assertEqual(events[-1]["candidate_id"], candidate["candidate_id"])

    def test_approve_rejects_candidate_without_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    status="ready_for_review",
                ),
                NEW_PAGE,
            )

            with self.assertRaisesRegex(ValueError, "without evidence"):
                adapter.approve_one(skill_root, candidate["candidate_id"])

            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "ready_for_review")

    def test_approve_rejects_content_changed_after_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="ready_for_review",
                ),
                NEW_PAGE,
            )
            Path(candidate["content_path"]).write_text(NEW_PAGE + "\n未审核修改\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "proposal hash"):
                adapter.approve_one(skill_root, candidate["candidate_id"])

            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "ready_for_review")

    def test_publish_rejects_ready_for_review_even_with_reviewer_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="ready_for_review",
                    approved_by="zhujie14",
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(ValueError, "not publishable"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            modules[1].upload_page.assert_not_called()
            self.assertEqual(page_path.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "ready_for_review")

    def test_publish_rejects_approved_candidate_without_reviewer_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(ValueError, "human approval"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            modules[1].upload_page.assert_not_called()
            self.assertEqual(page_path.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "approved")

    def test_publish_rejects_approval_from_another_reviewer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="someone-else",
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(ValueError, "zhujie14"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            modules[1].upload_page.assert_not_called()
            self.assertEqual(page_path.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "approved")

    def test_publish_success_updates_history_ledger_and_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new", "viking://source/old"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                published = adapter.publish_one(skill_root, candidate["candidate_id"], actor="zhujie14")

            history = skill_root / "state" / "history" / ACTIVE_CONCEPT / "v1.md"
            ledger = store.load_ledger()
            record = ledger[ACTIVE_CONCEPT]
            self.assertEqual(history.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(page_path.read_text(encoding="utf-8"), NEW_PAGE)
            self.assertEqual(record["status"], "active")
            self.assertEqual(record["current_version"], "v2")
            self.assertEqual(record["sources"], ["viking://source/old", "viking://source/new"])
            self.assertEqual(record["last_candidate_id"], candidate["candidate_id"])
            self.assertEqual(record["last_review_actor"], "zhujie14")
            self.assertEqual(published["status"], "published")
            self.assertEqual(published["proposed_version"], "v2")

    def test_publish_correction_replaces_ledger_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="correction",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/reviewed"],
                    source_strategy="replace",
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                adapter.publish_one(skill_root, candidate["candidate_id"], actor="zhujie14")

            self.assertEqual(
                store.load_ledger()[ACTIVE_CONCEPT]["sources"],
                ["viking://source/reviewed"],
            )

    def test_publish_upload_failure_restores_active_page_and_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            original_ledger = {ACTIVE_CONCEPT: active_record()}
            write_ledger(skill_root, original_ledger)
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root, upload_result="")

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(RuntimeError, "OpenViking upload failed"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            self.assertEqual(page_path.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(store.load_ledger(), original_ledger)
            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "publish_failed")

    def test_publish_rejects_content_changed_after_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            changed_content = NEW_PAGE + "\n未经审核的追加\n"
            Path(candidate["content_path"]).write_text(changed_content, encoding="utf-8")
            # Even if another writer also rewrites the mutable proposal hash,
            # the immutable approval snapshot must still block publication.
            store.update_candidate(candidate["candidate_id"], content_hash=adapter.sha256_text(changed_content))
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(RuntimeError, "approved snapshot"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            modules[1].upload_page.assert_not_called()
            self.assertEqual(page_path.read_text(encoding="utf-8"), OLD_PAGE)
            self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "stale")

    def test_publish_rejects_stale_base_hash_without_uploading(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            original_ledger = {ACTIVE_CONCEPT: active_record()}
            write_ledger(skill_root, original_ledger)
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            page_path.write_text(OLD_PAGE + "\n人工已修改\n", encoding="utf-8")
            modules = fake_publish_modules(skill_root)
            uploader = modules[1].upload_page

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(RuntimeError, "candidate base is stale"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            uploader.assert_not_called()
            self.assertEqual(store.load_ledger(), original_ledger)
            stale = store.read_candidate(candidate["candidate_id"])
            self.assertEqual(stale["status"], "stale")
            self.assertIn("active page changed", stale["error"])

    def test_repeated_publish_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)
            uploader = modules[1].upload_page

            with patch.object(adapter, "_skill_modules", return_value=modules):
                first = adapter.publish_one(skill_root, candidate["candidate_id"])
                second = adapter.publish_one(skill_root, candidate["candidate_id"])

            self.assertEqual(first["status"], "published")
            self.assertEqual(second["status"], "published")
            uploader.assert_called_once()
            self.assertEqual(store.load_ledger()[ACTIVE_CONCEPT]["current_version"], "v2")
            self.assertEqual(page_path.read_text(encoding="utf-8"), NEW_PAGE)
            history_files = list((skill_root / "state" / "history" / ACTIVE_CONCEPT).glob("*.md"))
            self.assertEqual([path.name for path in history_files], ["v1.md"])

    def test_publish_recovers_after_active_commit_without_duplicate_upload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp))
            page_path = skill_root / "state" / "pages" / f"{ACTIVE_CONCEPT}.md"
            page_path.write_text(OLD_PAGE, encoding="utf-8")
            write_ledger(skill_root, {ACTIVE_CONCEPT: active_record()})
            store = ConceptLearningStore(skill_root)
            candidate = store.save_candidate(
                make_candidate(
                    concept=ACTIVE_CONCEPT,
                    kind="refresh",
                    content=NEW_PAGE,
                    before=OLD_PAGE,
                    base_version="v1",
                    base_page_sha256=adapter.sha256_text(OLD_PAGE),
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(NEW_PAGE),
                ),
                NEW_PAGE,
            )
            modules = fake_publish_modules(skill_root)
            uploader = modules[1].upload_page
            original_update = ConceptLearningStore.update_candidate
            failed_once = False

            def fail_final_projection(
                target: ConceptLearningStore,
                candidate_id: str,
                *,
                expected_statuses: object = None,
                **updates: object,
            ) -> dict[str, object]:
                nonlocal failed_once
                if updates.get("status") == "published" and not failed_once:
                    failed_once = True
                    raise OSError("simulated Candidate projection failure")
                return original_update(
                    target,
                    candidate_id,
                    expected_statuses=expected_statuses,
                    **updates,
                )

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with patch.object(ConceptLearningStore, "update_candidate", new=fail_final_projection):
                    with self.assertRaisesRegex(OSError, "projection failure"):
                        adapter.publish_one(skill_root, candidate["candidate_id"])
                    self.assertEqual(store.read_candidate(candidate["candidate_id"])["status"], "publishing")
                    recovered = adapter.publish_one(skill_root, candidate["candidate_id"])

            self.assertEqual(recovered["status"], "published")
            self.assertTrue(recovered["recovered_from_active_commit"])
            uploader.assert_called_once()
            self.assertEqual(page_path.read_text(encoding="utf-8"), NEW_PAGE)
            self.assertEqual(store.load_ledger()[ACTIVE_CONCEPT]["current_version"], "v2")
            history_files = list((skill_root / "state" / "history" / ACTIVE_CONCEPT).glob("*.md"))
            self.assertEqual([path.name for path in history_files], ["v1.md"])

    def test_candidate_only_new_concept_without_config_record_publishes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp), include_active_config=False)
            write_ledger(skill_root, {})
            store = ConceptLearningStore(skill_root)
            content = "---\nconcept: 新发现概念\nsources:\n  - viking://source/new\n---\n\n# 新发现概念\n\n候选正文\n"
            candidate = store.save_candidate(
                make_candidate(
                    concept=NEW_CONCEPT,
                    kind="new-concept",
                    content=content,
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(content),
                ),
                content,
            )
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                published = adapter.publish_one(skill_root, candidate["candidate_id"])

            record = store.load_ledger()[NEW_CONCEPT]
            self.assertEqual(published["status"], "published")
            self.assertEqual(record["status"], "active")
            self.assertEqual(record["current_version"], "v1")
            self.assertEqual(record["category"], "")
            self.assertEqual(record["sources"], ["viking://source/new"])
            self.assertEqual(
                (skill_root / "state" / "pages" / f"{NEW_CONCEPT}.md").read_text(encoding="utf-8"),
                content,
            )

            proposal_modules, _contents, run_prompt = fake_proposal_modules(existing_page=content)
            with patch.object(adapter, "_skill_modules", return_value=proposal_modules):
                refreshed = adapter.propose_one(skill_root, NEW_CONCEPT)

            self.assertEqual(refreshed["kind"], "refresh")
            self.assertEqual(refreshed["base_version"], "v1")
            run_prompt.assert_called_once()

    def test_new_concept_without_base_hash_is_stale_if_page_appears(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            skill_root = make_skill_root(Path(temp), include_active_config=False)
            write_ledger(skill_root, {})
            store = ConceptLearningStore(skill_root)
            content = "---\nconcept: 新发现概念\n---\n\n# 新发现概念\n\n候选正文\n"
            candidate = store.save_candidate(
                make_candidate(
                    concept=NEW_CONCEPT,
                    kind="new-concept",
                    content=content,
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                    approved_content_hash=adapter.sha256_text(content),
                ),
                content,
            )
            page_path = skill_root / "state" / "pages" / f"{NEW_CONCEPT}.md"
            concurrent_page = "并发创建的 Active 页面\n"
            page_path.write_text(concurrent_page, encoding="utf-8")
            modules = fake_publish_modules(skill_root)

            with patch.object(adapter, "_skill_modules", return_value=modules):
                with self.assertRaisesRegex(RuntimeError, "candidate base is stale"):
                    adapter.publish_one(skill_root, candidate["candidate_id"])

            modules[1].upload_page.assert_not_called()
            self.assertEqual(page_path.read_text(encoding="utf-8"), concurrent_page)
            self.assertEqual(store.load_ledger(), {})
            stale = store.read_candidate(candidate["candidate_id"])
            self.assertEqual(stale["status"], "stale")
            self.assertIn("active page changed", stale["error"])


if __name__ == "__main__":
    unittest.main()
