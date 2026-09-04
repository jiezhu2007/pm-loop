from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_discovery_triage import _read_evidence_batch, propose, read_evidence  # noqa: E402
from concept_learning import ConceptLearningStore  # noqa: E402


class ConceptDiscoveryTriageTests(unittest.TestCase):
    def test_evidence_batch_timeout_does_not_join_stuck_worker(self) -> None:
        release = threading.Event()
        started = threading.Event()
        uris = ["viking://resources/fast", "viking://resources/stuck"]

        def evidence(uri: str) -> dict[str, str]:
            if uri.endswith("/stuck"):
                started.set()
                release.wait(2)
            return {"uri": uri, "status": "available", "text": "evidence"}

        try:
            with patch.dict(os.environ, {"CONCEPT_DISCOVERY_READ_BATCH_TIMEOUT": "0.1"}), patch(
                "concept_discovery_triage.read_evidence",
                side_effect=evidence,
            ):
                started_at = time.monotonic()
                outcomes = _read_evidence_batch(uris, {})
                elapsed = time.monotonic() - started_at
        finally:
            release.set()

        self.assertTrue(started.is_set())
        self.assertLess(elapsed, 0.5)
        self.assertEqual([item["uri"] for item in outcomes], uris)
        self.assertEqual(outcomes[0]["status"], "available")
        self.assertEqual(outcomes[1]["status"], "unavailable")
        self.assertEqual(outcomes[1]["error"], "batch_timeout")

    def test_directory_uri_resolves_to_readable_chunk(self) -> None:
        root_uri = "viking://resources/source/document"
        child_uri = f"{root_uri}/document"
        leaf_uri = f"{child_uri}/part-1.md"

        class Response:
            def __init__(self, value):
                self.value = value

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps(self.value).encode("utf-8")

        def urlopen(request, timeout=0):
            parsed = urllib.parse.urlparse(request.full_url)
            query = urllib.parse.parse_qs(parsed.query)
            uri = query.get("uri", [""])[0]
            if parsed.path.endswith("/content/read") and uri == root_uri:
                return Response({"result": None})
            if parsed.path.endswith("/fs/ls") and uri == root_uri:
                return Response({"result": [{"uri": child_uri, "isDir": True}]})
            if parsed.path.endswith("/fs/ls") and uri == child_uri:
                return Response({"result": [{"uri": leaf_uri, "isDir": False}]})
            if parsed.path.endswith("/content/read") and uri == leaf_uri:
                return Response({"result": "目录包装里的真实证据"})
            raise AssertionError(f"unexpected request: {request.full_url}")

        with patch("concept_discovery_triage._config", return_value={}), patch(
            "concept_discovery_triage.urllib.request.urlopen", side_effect=urlopen
        ):
            result = read_evidence(root_uri)

        self.assertEqual(result["status"], "available")
        self.assertEqual(result["text"], "目录包装里的真实证据")
        self.assertEqual(result["resolved_uri"], leaf_uri)

    def test_agent_triage_creates_reviewable_candidate_with_real_refs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            store.save_ledger({"已有概念": {"status": "active", "sources": []}})
            run = store.append_discovery_run(
                {
                    "run_id": "discover-fixture",
                    "source": "document_delta",
                    "updated_uris": ["viking://resources/new-capability"],
                    "unmatched_uris": ["viking://resources/new-capability"],
                    "candidate_ids": [],
                    "status": "needs_agent_triage",
                }
            )
            content = "# 新能力\n\n## 定义\n基于真实证据形成的候选概念。\n\n## 能力边界\n只描述证据中明确出现的能力。\n\n## 已知限制\n仍需本人审核产品边界与正式命名。" * 2
            output = json.dumps(
                {
                    "proposals": [
                        {
                            "name": "新能力",
                            "aliases": ["新别名"],
                            "category": "待归类",
                            "content": content,
                            "evidence_uris": ["viking://resources/new-capability", "viking://resources/invented"],
                            "reason": ["出现独立产品边界"],
                            "confidence": 0.81,
                        }
                    ]
                },
                ensure_ascii=False,
            )
            with patch("concept_discovery_triage.read_evidence", return_value={"uri": "viking://resources/new-capability", "status": "available", "text": "真实产品说明"}):
                updated = propose(store, run, root / "codex", invoker=lambda prompt: (0, output))
            self.assertEqual(updated["status"], "triaged")
            self.assertEqual(len(updated["candidate_ids"]), 1)
            candidate = store.read_candidate(updated["candidate_ids"][0])
            self.assertEqual(candidate["concept"], "新能力")
            self.assertEqual(candidate["kind"], "new_concept")
            self.assertEqual(candidate["source_refs"], ["viking://resources/new-capability"])
            self.assertEqual(candidate["status"], "ready_for_review")

    def test_triage_without_readable_evidence_stays_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            run = store.append_discovery_run(
                {
                    "run_id": "discover-blocked",
                    "source": "document_delta",
                    "updated_uris": ["viking://resources/missing"],
                    "unmatched_uris": ["viking://resources/missing"],
                    "candidate_ids": [],
                    "status": "needs_agent_triage",
                }
            )
            with patch("concept_discovery_triage.read_evidence", return_value={"uri": "viking://resources/missing", "status": "unavailable", "text": ""}):
                updated = propose(store, run, root / "codex", invoker=lambda prompt: self.fail("Codex must not run without evidence"))
            self.assertEqual(updated["status"], "triage_blocked")
            self.assertEqual(updated["triage_status"], "blocked")
            self.assertEqual(updated["triage_remaining"], 0)
            self.assertEqual(updated["candidate_ids"], [])
            self.assertEqual(updated["processed_uris"], ["viking://resources/missing"])
            self.assertEqual(updated["unavailable_uris"], ["viking://resources/missing"])
            self.assertEqual(updated["triage_evidence"]["viking://resources/missing"]["status"], "unavailable")
            self.assertEqual(updated["triage_evidence"]["viking://resources/missing"]["error"], "unreadable_or_empty")
            self.assertTrue(updated["triage_evidence"]["viking://resources/missing"]["attempted_at"])

    def test_mixed_unavailable_batch_advances_cursor_but_stays_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            missing = "viking://resources/missing"
            readable = "viking://resources/readable"
            run = store.append_discovery_run(
                {
                    "run_id": "discover-mixed-unavailable",
                    "source": "document_delta",
                    "unmatched_uris": [missing, readable],
                    "candidate_ids": [],
                    "status": "needs_agent_triage",
                }
            )

            repaired = False

            def evidence(uri: str) -> dict[str, str]:
                if uri == missing and not repaired:
                    return {"uri": uri, "status": "unavailable", "text": "", "error": "gone"}
                return {"uri": uri, "status": "available", "text": "真实证据"}

            with patch("concept_discovery_triage.read_evidence", side_effect=evidence):
                updated = propose(
                    store,
                    run,
                    root / "codex",
                    max_items=2,
                    invoker=lambda prompt: (0, json.dumps({"proposals": []})),
                )
                # Once the cursor is drained, a follow-up invocation must not
                # retry the unavailable URI or silently turn the run green.
                repeated = propose(
                    store,
                    updated,
                    root / "codex",
                    max_items=2,
                    invoker=lambda prompt: self.fail("drained unavailable URI must not invoke Codex"),
                )
                repaired = True
                retried = propose(
                    store,
                    repeated,
                    root / "codex",
                    max_items=1,
                    retry_unavailable=True,
                    invoker=lambda prompt: (0, json.dumps({"proposals": []})),
                )

            self.assertEqual(updated["status"], "triage_partial")
            self.assertEqual(updated["triage_status"], "complete_with_unavailable")
            self.assertEqual(updated["triage_remaining"], 0)
            self.assertEqual(updated["processed_uris"], [missing, readable])
            self.assertEqual(updated["unavailable_uris"], [missing])
            self.assertEqual(updated["triage_unavailable_count"], 1)
            self.assertEqual(updated["triage_evidence"][missing]["error"], "gone")
            self.assertEqual(updated["triage_evidence"][missing]["attempts"], 1)
            self.assertEqual(repeated["status"], "triage_partial")
            self.assertEqual(repeated["triage_status"], "complete_with_unavailable")
            self.assertEqual(retried["status"], "triage_no_candidate")
            self.assertEqual(retried["triage_status"], "complete")
            self.assertEqual(retried["unavailable_uris"], [])
            self.assertEqual(retried["triage_evidence"][missing]["status"], "available")
            self.assertEqual(retried["triage_evidence"][missing]["attempts"], 2)
            self.assertEqual(retried["triage_evidence"][missing]["errors"], ["gone"])

    def test_triage_persists_cursor_and_does_not_repeat_processed_uri(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            uris = [f"viking://resources/cap-{i}" for i in range(2)]
            run = store.append_discovery_run({"run_id": "discover-paged", "source": "document_delta", "unmatched_uris": uris, "candidate_ids": [], "status": "needs_agent_triage"})
            output = json.dumps({"proposals": []})
            seen = []
            def evidence(uri):
                seen.append(uri)
                return {"uri": uri, "status": "available", "text": "真实证据"}
            with patch("concept_discovery_triage.read_evidence", side_effect=evidence):
                first = propose(store, run, root / "codex", max_items=1, invoker=lambda prompt: (0, output))
                second = propose(store, first, root / "codex", max_items=1, invoker=lambda prompt: (0, output))
            self.assertEqual(seen, uris)
            self.assertEqual(second["triage_status"], "complete")
            self.assertEqual(second["processed_uris"], uris)

    def test_retry_unavailable_does_not_reopen_normal_pending_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            missing = "viking://resources/missing"
            pending = "viking://resources/pending"
            run = store.append_discovery_run(
                {
                    "run_id": "discover-retry-scope",
                    "source": "document_delta",
                    "unmatched_uris": [pending, missing],
                    "processed_uris": [pending],
                    "unavailable_uris": [missing],
                    "triage_evidence": {
                        missing: {
                            "status": "unavailable",
                            "error": "gone",
                            "errors": ["gone"],
                            "attempted_at": "2026-08-25T00:00:00Z",
                            "attempts": 1,
                        }
                    },
                    "candidate_ids": [],
                    "status": "triage_partial",
                    "triage_status": "in_progress",
                    "triage_remaining": 1,
                }
            )
            seen: list[str] = []

            def evidence(uri: str) -> dict[str, str]:
                seen.append(uri)
                return {"uri": uri, "status": "available", "text": "修复后的证据"}

            with patch("concept_discovery_triage.read_evidence", side_effect=evidence):
                updated = propose(
                    store,
                    run,
                    root / "codex",
                    max_items=1,
                    retry_unavailable=True,
                    invoker=lambda prompt: (0, json.dumps({"proposals": []})),
                )

            self.assertEqual(seen, [missing])
            self.assertEqual(updated["status"], "triage_no_candidate")
            self.assertEqual(updated["triage_remaining"], 0)
            self.assertEqual(updated["unavailable_uris"], [])
            self.assertEqual(updated["processed_uris"], [missing, pending])

    def test_active_fragment_is_recorded_as_merge_without_llm_or_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            store.save_ledger(
                {
                    "计算资源": {
                        "status": "active",
                        "category": "运维与商业",
                        "sources": [],
                    }
                }
            )
            (root / "skill" / "config.yaml").write_text(
                "concepts:\n  - name: 计算资源\n    aliases: [compute, 资源管理]\n",
                encoding="utf-8",
            )
            uri = "viking://resources/fragment-resource-queue"
            run = store.append_discovery_run(
                {
                    "run_id": "discover-active-fragment",
                    "source": "document_delta",
                    "unmatched_uris": [uri],
                    "candidate_ids": [],
                    "status": "needs_agent_triage",
                }
            )

            with patch(
                "concept_discovery_triage.read_evidence",
                return_value={
                    "uri": uri,
                    "status": "available",
                    "text": "通用资源队列支持独占和共享队列。",
                    "term": "资源队列",
                },
            ):
                updated = propose(
                    store,
                    run,
                    root / "codex",
                    invoker=lambda prompt: self.fail("Active fragment must not invoke new-concept LLM"),
                )

            self.assertEqual(updated["candidate_ids"], [])
            self.assertEqual(updated["triage_active_match_count"], 1)
            self.assertEqual(updated["triage_decisions"][0]["decision"], "merge")
            self.assertEqual(updated["triage_decisions"][0]["target"], "计算资源")

    def test_llm_cannot_override_active_match_with_new_concept(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = ConceptLearningStore(root / "skill")
            store.save_ledger({"数据授权": {"status": "active", "sources": []}})
            (root / "skill" / "config.yaml").write_text(
                "concepts:\n  - name: 数据授权\n    aliases: [行列权限]\n",
                encoding="utf-8",
            )
            uri = "viking://resources/unrelated"
            run = store.append_discovery_run(
                {
                    "run_id": "discover-llm-guard",
                    "source": "document_delta",
                    "unmatched_uris": [uri],
                    "candidate_ids": [],
                    "status": "needs_agent_triage",
                }
            )
            output = json.dumps(
                {
                    "proposals": [
                        {
                            "name": "行权限",
                            "content": "# 不应成为新概念\n" * 20,
                            "evidence_uris": [uri],
                            "confidence": 0.9,
                        }
                    ]
                },
                ensure_ascii=False,
            )
            with patch(
                "concept_discovery_triage.read_evidence",
                return_value={"uri": uri, "status": "available", "text": "一段未命名证据"},
            ):
                updated = propose(store, run, root / "codex", invoker=lambda prompt: (0, output))

            self.assertEqual(updated["candidate_ids"], [])
            self.assertEqual(updated["triage_decisions"][0]["decision"], "merge")
            self.assertEqual(updated["triage_decisions"][0]["target"], "数据授权")


if __name__ == "__main__":
    unittest.main()
