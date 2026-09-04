from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_learning import ConceptLearningStore, make_candidate  # noqa: E402
from pm_loop_control_plane_server import ControlPlane  # noqa: E402


def write_publish_stub(skill_root: Path, delay: float = 0.0) -> None:
    script = f'''import json
import sys
import time
from pathlib import Path

time.sleep({delay!r})
root = Path(__file__).resolve().parents[1]
candidate_id = sys.argv[sys.argv.index("--publish") + 1]
candidate_path = root / "state" / "candidates" / f"{{candidate_id}}.json"
candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
content = Path(candidate["content_path"]).read_text(encoding="utf-8")
concept = candidate["concept"]
page_path = root / "state" / "pages" / f"{{concept}}.md"
page_path.parent.mkdir(parents=True, exist_ok=True)
page_path.write_text(content, encoding="utf-8")
ledger_path = root / "state" / "concepts-ledger.json"
ledger = json.loads(ledger_path.read_text(encoding="utf-8")) if ledger_path.is_file() else {{}}
record = dict(ledger.get(concept) or {{}})
version = "v" + str(int(str(record.get("current_version") or "v0").lstrip("v")) + 1)
record.update({{"status": "active", "current_version": version, "last_candidate_id": candidate_id}})
ledger[concept] = record
ledger_path.write_text(json.dumps(ledger, ensure_ascii=False), encoding="utf-8")
candidate.update({{"status": "published", "proposed_version": version, "published_uri": f"viking://concepts/{{concept}}"}})
candidate_path.write_text(json.dumps(candidate, ensure_ascii=False), encoding="utf-8")
'''
    (skill_root / "scripts").mkdir(parents=True, exist_ok=True)
    (skill_root / "scripts" / "refresh.py").write_text(script, encoding="utf-8")


def make_review_fixture(
    root: Path,
    *,
    concept: str = "测试概念",
) -> tuple[Path, Path, ConceptLearningStore, dict[str, object]]:
    codex_root = root / "codex"
    skill_root = codex_root / "skills" / "shengsuan-concepts"
    (skill_root / "state" / "candidates").mkdir(parents=True)
    (skill_root / "scripts").mkdir(parents=True)
    (skill_root / "scripts" / "refresh.py").write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
    (skill_root / "state" / "concepts-ledger.json").write_text(
        json.dumps(
            {concept: {"sources": ["viking://source/base"], "category": "测试"}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    learning = ConceptLearningStore(skill_root)
    content = f"---\ntitle: {concept}\n---\n{concept} 候选正文\n"
    candidate = learning.save_candidate(
        make_candidate(
            concept=concept,
            kind="refresh",
            content=content,
            source_refs=["viking://source/new"],
        ),
        content,
    )
    return codex_root, skill_root, learning, candidate


@unittest.skip("legacy Control Plane concept writer removed; shengsuan-concepts owns this workflow")
class ConceptReviewBatchTests(unittest.TestCase):
    def test_approve_can_be_staged_without_a_review_note(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, skill_root, learning, candidate = make_review_fixture(root, concept="免批注批准")
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            staged = controller.stage_review("免批注批准", {"action": "approve", "note": "", "candidate_id": candidate["candidate_id"]})
            self.assertEqual(staged["action"], "approve")
            self.assertEqual(staged["note"], "")

    def test_candidate_only_concept_is_reviewable_and_reads_proposed_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "scripts").mkdir(parents=True)
            (skill_root / "scripts" / "refresh.py").write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="新发现概念",
                    kind="new-concept",
                    content="---\ntitle: 新发现概念\n---\n候选正文\n",
                    source_refs=["viking://source/1", "viking://source/1"],
                    evidence=[{"uri": "viking://source/2", "quote": "证据"}],
                    confidence=0.86,
                ),
                "---\ntitle: 新发现概念\n---\n候选正文\n",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            rows = controller.concepts()
            self.assertEqual([row["name"] for row in rows], ["新发现概念"])
            row = rows[0]
            self.assertTrue(row["candidate_only"])
            self.assertTrue(row["candidateOnly"])
            self.assertFalse(row["placeholder"])
            self.assertEqual(row["sourceCount"], 2)
            self.assertIsNone(row["uri"])
            self.assertEqual(row["candidate"]["candidate_id"], candidate["candidate_id"])
            self.assertIn("候选正文", controller.concept("新发现概念")["page_excerpt"])

            staged = controller.stage_review(
                "新发现概念",
                {"action": "approve", "note": "证据可核验", "candidate_id": candidate["candidate_id"]},
            )
            self.assertEqual(staged["action"], "approve")

    def test_candidate_only_without_evidence_cannot_be_approved(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="待补证据概念",
                    kind="new-concept",
                    content="---\ntitle: 待补证据概念\n---\n",
                    source_refs=[],
                    evidence=[],
                )
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            row = controller.concept("待补证据概念")
            self.assertTrue(row["candidate_only"])
            self.assertTrue(row["placeholder"])
            with self.assertRaisesRegex(ValueError, "without evidence"):
                controller.stage_review(
                    "待补证据概念",
                    {"action": "approve", "note": "批准", "candidate_id": candidate["candidate_id"]},
                )

    def test_review_without_candidate_is_rejected_for_every_action(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"占位概念": {"sources": [], "status": "active"}}, ensure_ascii=False),
                encoding="utf-8",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            for action in ("pause", "changes", "approve"):
                with self.subTest(action=action), self.assertRaisesRegex(ValueError, "requires a Candidate revision"):
                    controller.stage_review("占位概念", {"action": action, "note": "先生成候选"})

    def test_existing_placeholder_can_be_approved_after_candidate_adds_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"待补概念": {"sources": [], "status": "active"}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="待补概念",
                    kind="refresh",
                    content="---\ntitle: 待补概念\n---\n有证据的候选\n",
                    source_refs=["viking://source/new-evidence"],
                ),
                "---\ntitle: 待补概念\n---\n有证据的候选\n",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            staged = controller.stage_review(
                "待补概念",
                {"action": "approve", "note": "证据已补齐", "candidate_id": candidate["candidate_id"]},
            )

            self.assertEqual(staged["candidate_id"], candidate["candidate_id"])
            self.assertEqual(staged["candidate_content_hash"], candidate["content_hash"])

    def test_restart_reconciles_terminal_run_without_republishing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "scripts").mkdir(parents=True)
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="已完成概念",
                    kind="refresh",
                    content="---\ntitle: 已完成概念\n---\n",
                    source_refs=["viking://source/done"],
                )
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = controller.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            controller.store.append(run_id, "run/completed", {"candidate_id": candidate["candidate_id"]})
            controller.queue_path.parent.mkdir(parents=True, exist_ok=True)
            controller.queue_path.write_text(
                json.dumps(
                    [
                        {
                            "queue_id": "publish-" + run_id,
                            "run_id": run_id,
                            "candidate_id": candidate["candidate_id"],
                            "concept": "已完成概念",
                            "status": "running",
                            "attempts": 1,
                            "updated_at": "2020-01-01T00:00:00Z",
                            "heartbeat_at": "2020-01-01T00:00:00Z",
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            controller._recover_running_queue_items()
            item = controller.queue_status()["items"][0]
            self.assertEqual(item["status"], "completed")
            self.assertEqual(item["recovery_reason"], "run_already_terminal")
            self.assertEqual(controller.store.events_for(run_id)[-1]["type"], "run/completed")

    def test_commit_rechecks_candidate_after_staging(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "pages").mkdir(parents=True)
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "scripts").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"可变概念": {"sources": ["viking://source/base"]}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="可变概念",
                    kind="refresh",
                    content="---\ntitle: 可变概念\n---\n",
                    source_refs=["viking://source/new"],
                    base_page_sha256="sha256:before",
                    base_version="v1",
                ),
                "---\ntitle: 可变概念\n---\n",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "可变概念",
                {"action": "approve", "note": "先暂存", "candidate_id": candidate["candidate_id"]},
            )
            learning.update_candidate(candidate["candidate_id"], base_page_sha256="sha256:after")

            result = controller.commit_reviews()
            self.assertEqual(result["submitted"], [])
            self.assertEqual(len(result["failed"]), 1)
            self.assertEqual(result["staged"]["可变概念"]["candidate_id"], candidate["candidate_id"])
            self.assertEqual(controller.store.list_states(), [])

    def test_commit_rejects_candidate_content_changed_after_staging(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"内容可变概念": {"sources": ["viking://source/base"]}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="内容可变概念",
                    kind="refresh",
                    content="---\ntitle: 内容可变概念\n---\n原候选\n",
                    source_refs=["viking://source/new"],
                ),
                "---\ntitle: 内容可变概念\n---\n原候选\n",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "内容可变概念",
                {"action": "approve", "note": "先暂存", "candidate_id": candidate["candidate_id"]},
            )
            changed = "---\ntitle: 内容可变概念\n---\n未经审核的新正文\n"
            Path(candidate["content_path"]).write_text(changed, encoding="utf-8")
            learning.update_candidate(candidate["candidate_id"], content_hash="sha256:rewritten")

            result = controller.commit_reviews()

            self.assertEqual(result["submitted"], [])
            self.assertEqual(len(result["failed"]), 1)
            self.assertIn("content", result["failed"][0]["error"])
            self.assertEqual(learning.read_candidate(candidate["candidate_id"])["status"], "ready_for_review")

    def test_pause_and_changes_commit_update_candidate_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps(
                    {
                        "暂停概念": {"sources": ["viking://source/pause"]},
                        "退回概念": {"sources": ["viking://source/change"]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            paused = learning.save_candidate(
                make_candidate(
                    concept="暂停概念",
                    kind="refresh",
                    content="---\ntitle: 暂停概念\n---\n",
                    source_refs=["viking://source/pause"],
                )
            )
            changed = learning.save_candidate(
                make_candidate(
                    concept="退回概念",
                    kind="refresh",
                    content="---\ntitle: 退回概念\n---\n",
                    source_refs=["viking://source/change"],
                )
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "暂停概念",
                {
                    "action": "pause",
                    "note": "等待下一轮证据",
                    "candidate_id": paused["candidate_id"],
                },
            )
            controller.stage_review(
                "退回概念",
                {
                    "action": "changes",
                    "note": "补充边界证据",
                    "candidate_id": changed["candidate_id"],
                },
            )

            result = controller.commit_reviews()

            self.assertEqual(result["failed"], [])
            paused_after = learning.read_candidate(paused["candidate_id"])
            changed_after = learning.read_candidate(changed["candidate_id"])
            self.assertEqual(paused_after["status"], "paused")
            self.assertEqual(paused_after["review_note"], "等待下一轮证据")
            self.assertEqual(changed_after["status"], "changes_requested")
            self.assertEqual(changed_after["review_note"], "补充边界证据")
            self.assertEqual(paused_after["reviewed_by"], "zhujie14")
            self.assertEqual(changed_after["reviewed_by"], "zhujie14")

    def test_approved_candidate_cannot_be_paused_through_review_flow(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"已批准概念": {"sources": ["viking://source/base"]}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="已批准概念",
                    kind="refresh",
                    content="---\ntitle: 已批准概念\n---\n",
                    source_refs=["viking://source/new"],
                    status="approved",
                    approved_by="zhujie14",
                ),
                "---\ntitle: 已批准概念\n---\n",
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            with self.assertRaisesRegex(ValueError, "publish flow"):
                controller.stage_review(
                    "已批准概念",
                    {"action": "pause", "note": "试图撤回", "candidate_id": candidate["candidate_id"]},
                )

    def test_batch_materializes_independent_gate_runs_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            state_root = root / "pm-loop"
            (skill_root / "state" / "pages").mkdir(parents=True)
            (skill_root / "scripts").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps(
                    {
                        "概念A": {"sources": ["viking://source/a"], "category": "测试"},
                        "概念B": {"sources": ["viking://source/b"], "category": "测试"},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            write_publish_stub(skill_root)

            learning = ConceptLearningStore(skill_root)
            candidate = learning.save_candidate(
                make_candidate(
                    concept="概念A",
                    kind="refresh",
                    content="---\ntitle: A\n---\nnew\n",
                    before="",
                    source_refs=["viking://source/a"],
                    confidence=0.9,
                ),
                "---\ntitle: A\n---\nnew\n",
            )
            candidate_b = learning.save_candidate(
                make_candidate(
                    concept="概念B",
                    kind="refresh",
                    content="---\ntitle: B\n---\nnew\n",
                    source_refs=["viking://source/b"],
                ),
                "---\ntitle: B\n---\nnew\n",
            )
            controller = ControlPlane(state_root, root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "概念A",
                {"action": "approve", "note": "证据充分", "candidate_id": candidate["candidate_id"]},
            )
            controller.stage_review(
                "概念B",
                {"action": "pause", "note": "先保留观察", "candidate_id": candidate_b["candidate_id"]},
            )

            result = controller.commit_reviews()
            self.assertEqual(result["failed"], [])
            self.assertEqual(len(result["submitted"]), 2)
            self.assertEqual(result["staged"], {})

            rows = {item["concept"]["name"]: item for item in result["submitted"]}
            self.assertEqual(set(rows), {"概念A", "概念B"})
            self.assertNotEqual(rows["概念A"]["run"]["run_id"], rows["概念B"]["run"]["run_id"])
            batch_ids = {
                controller.store.request(item["run"]["run_id"])["trigger"]["batch_id"]
                for item in result["submitted"]
            }
            self.assertEqual(len(batch_ids), 1)

            paused_run = rows["概念B"]["run"]["run_id"]
            self.assertEqual(controller.store.state(paused_run)["status"], "paused")
            paused_events = [event["type"] for event in controller.store.events_for(paused_run)]
            self.assertEqual(paused_events, ["run/created", "gate/requested", "gate/paused"])

            approved_run = rows["概念A"]["run"]["run_id"]
            deadline = time.time() + 3
            while time.time() < deadline and controller.store.state(approved_run)["status"] not in {"completed", "failed"}:
                time.sleep(0.02)
            approved_events = [event["type"] for event in controller.store.events_for(approved_run)]
            self.assertEqual(controller.store.state(approved_run)["status"], "completed")
            self.assertEqual(
                approved_events[:5],
                ["run/created", "gate/requested", "gate/approved", "run/started", "action/queued"],
            )
            self.assertIn("action/started", approved_events)
            self.assertIn("run/completed", approved_events)
            self.assertEqual(learning.read_candidate(candidate["candidate_id"])["status"], "published")

            second = controller.commit_reviews()
            self.assertEqual(second["submitted"], [])
            self.assertEqual(second["failed"], [])

    def test_publish_worker_preserves_item_enqueued_while_another_publish_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps(
                    {
                        "概念A": {"sources": ["viking://source/a"], "category": "测试"},
                        "概念B": {"sources": ["viking://source/b"], "category": "测试"},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            write_publish_stub(skill_root, delay=0.3)
            learning = ConceptLearningStore(skill_root)
            candidates = {}
            for name, source in (("概念A", "viking://source/a"), ("概念B", "viking://source/b")):
                content = f"---\ntitle: {name}\n---\n{name} 新正文\n"
                candidates[name] = learning.save_candidate(
                    make_candidate(concept=name, kind="refresh", content=content, source_refs=[source]),
                    content,
                )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            controller.stage_review(
                "概念A",
                {"action": "approve", "note": "先发布 A", "candidate_id": candidates["概念A"]["candidate_id"]},
            )
            first = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            deadline = time.time() + 2
            while time.time() < deadline:
                if any(row.get("run_id") == first and row.get("status") == "running" for row in controller.queue_status()["items"]):
                    break
                time.sleep(0.01)

            controller.stage_review(
                "概念B",
                {"action": "approve", "note": "A 运行时加入 B", "candidate_id": candidates["概念B"]["candidate_id"]},
            )
            second = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            deadline = time.time() + 5
            while time.time() < deadline:
                states = {row.get("run_id"): row.get("status") for row in controller.queue_status()["items"]}
                if states.get(first) == "completed" and states.get(second) == "completed":
                    break
                time.sleep(0.02)

            states = {row.get("run_id"): row.get("status") for row in controller.queue_status()["items"]}
            self.assertEqual(states, {first: "completed", second: "completed"})
            self.assertEqual(learning.read_candidate(candidates["概念A"]["candidate_id"])["status"], "published")
            self.assertEqual(learning.read_candidate(candidates["概念B"]["candidate_id"])["status"], "published")

    def test_publish_worker_rejects_exit_zero_without_active_update(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "scripts").mkdir(parents=True)
            (skill_root / "scripts" / "refresh.py").write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"空成功概念": {"sources": ["viking://source/base"]}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            content = "---\ntitle: 空成功概念\n---\n候选正文\n"
            candidate = learning.save_candidate(
                make_candidate(
                    concept="空成功概念",
                    kind="refresh",
                    content=content,
                    source_refs=["viking://source/new"],
                ),
                content,
            )
            controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "空成功概念",
                {"action": "approve", "note": "测试验证门禁", "candidate_id": candidate["candidate_id"]},
            )
            run_id = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            deadline = time.time() + 3
            while time.time() < deadline:
                queue_item = next(
                    row for row in controller.queue_status()["items"] if row.get("run_id") == run_id
                )
                if queue_item.get("status") in {"completed", "failed"}:
                    break
                time.sleep(0.02)

            self.assertEqual(controller.store.state(run_id)["status"], "failed")
            self.assertEqual(learning.read_candidate(candidate["candidate_id"])["status"], "publish_failed")
            self.assertEqual(queue_item["status"], "failed")

    def test_retry_publish_requeues_the_frozen_approved_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = codex_root / "skills" / "shengsuan-concepts"
            (skill_root / "state" / "candidates").mkdir(parents=True)
            (skill_root / "state" / "concepts-ledger.json").write_text(
                json.dumps({"重试概念": {"sources": ["viking://source/base"]}}, ensure_ascii=False),
                encoding="utf-8",
            )
            learning = ConceptLearningStore(skill_root)
            content = "---\ntitle: 重试概念\n---\n候选正文\n"
            candidate = learning.save_candidate(
                make_candidate(
                    concept="重试概念",
                    kind="refresh",
                    content=content,
                    source_refs=["viking://source/new"],
                ),
                content,
            )
            learning.update_candidate(
                candidate["candidate_id"],
                status="publish_failed",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
            )

            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = controller.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            controller.store.append(run_id, "run/failed", {"error": "first publish failed"}, actor="control-plane")
            controller._write_queue(
                [
                    {
                        "queue_id": f"publish-{run_id}",
                        "run_id": run_id,
                        "candidate_id": candidate["candidate_id"],
                        "concept": "重试概念",
                        "status": "failed",
                        "attempts": 1,
                        "error": "first publish failed",
                    }
                ]
            )

            retried = controller.retry_publish(run_id)

            self.assertEqual(retried["status"], "queued")
            self.assertIsNone(retried["error"])
            after = learning.read_candidate(candidate["candidate_id"])
            self.assertEqual(after["status"], "approved")
            self.assertEqual(after["approved_content_hash"], candidate["content_hash"])
            self.assertIn("run/retrying", [event["type"] for event in controller.store.events_for(run_id)])

    def test_fresh_running_lease_blocks_claiming_next_queued_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root = root / "codex"
            (codex_root / "skills" / "shengsuan-concepts").mkdir(parents=True)
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            controller._write_queue(
                [
                    {
                        "queue_id": "publish-running",
                        "run_id": "run-running",
                        "candidate_id": "cand-running",
                        "concept": "运行中概念",
                        "status": "running",
                        "attempts": 1,
                        "updated_at": now,
                        "heartbeat_at": now,
                    },
                    {
                        "queue_id": "publish-queued",
                        "run_id": "run-queued",
                        "candidate_id": "cand-queued",
                        "concept": "排队概念",
                        "status": "queued",
                        "attempts": 0,
                    },
                ]
            )

            with mock.patch("pm_loop_control_plane_server.subprocess.run") as publish:
                with mock.patch.object(controller, "_recover_running_queue_items", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        controller._publish_worker()

            states = {row["run_id"]: row["status"] for row in controller.queue_status()["items"]}
            self.assertEqual(states["run-running"], "running")
            self.assertEqual(states["run-queued"], "queued")
            queued = next(row for row in controller.queue_status()["items"] if row["run_id"] == "run-queued")
            self.assertEqual(queued["attempts"], 0)
            publish.assert_not_called()

    def test_cancel_queued_publish_restores_candidate_without_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, skill_root, learning, candidate = make_review_fixture(root, concept="可取消概念")
            ledger_before = learning.load_ledger()
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "可取消概念",
                {"action": "approve", "note": "先排队", "candidate_id": candidate["candidate_id"]},
            )
            run_id = controller.commit_reviews()["submitted"][0]["run"]["run_id"]

            state = controller.cancel(run_id)

            self.assertEqual(state["status"], "cancelled")
            after = learning.read_candidate(str(candidate["candidate_id"]))
            self.assertEqual(after["status"], "ready_for_review")
            self.assertIsNone(after["approved_by"])
            self.assertIsNone(after["approved_content_hash"])
            self.assertIsNone(after["approval_run_id"])
            queue_item = next(row for row in controller.queue_status()["items"] if row["run_id"] == run_id)
            self.assertEqual(queue_item["status"], "cancelled")
            self.assertFalse((skill_root / "state" / "pages" / "可取消概念.md").exists())
            self.assertEqual(learning.load_ledger(), ledger_before)
            event_types = [event["type"] for event in controller.store.events_for(run_id)]
            self.assertIn("run/cancelled", event_types)
            self.assertNotIn("run/completed", event_types)

    def test_cancel_running_publish_is_rejected_without_false_cancel_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="已启动概念")
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "已启动概念",
                {"action": "approve", "note": "立即发布", "candidate_id": candidate["candidate_id"]},
            )
            run_id = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            rows = controller._read_queue()
            rows[0]["status"] = "running"
            rows[0]["heartbeat_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            controller._write_queue(rows)

            with self.assertRaisesRegex(ValueError, "safe cancellation point"):
                controller.cancel(run_id)

            self.assertEqual(learning.read_candidate(str(candidate["candidate_id"]))["status"], "approved")
            self.assertEqual(controller.queue_status()["items"][0]["status"], "running")
            event_types = [event["type"] for event in controller.store.events_for(run_id)]
            self.assertNotIn("run/cancelled", event_types)

    def test_cancel_completed_publish_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, _learning, candidate = make_review_fixture(root, concept="已完成概念")
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = controller.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            controller.store.append(run_id, "run/completed", {"candidate_id": candidate["candidate_id"]})
            controller._write_queue(
                [
                    {
                        "queue_id": f"publish-{run_id}",
                        "run_id": run_id,
                        "candidate_id": candidate["candidate_id"],
                        "concept": "已完成概念",
                        "status": "completed",
                    }
                ]
            )

            state = controller.cancel(run_id)

            self.assertEqual(state["status"], "completed")
            event_types = [event["type"] for event in controller.store.events_for(run_id)]
            self.assertNotIn("run/cancelled", event_types)

    def test_cancel_queue_write_failure_restores_approved_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="取消补偿概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "取消补偿概念",
                {"action": "approve", "note": "先排队", "candidate_id": candidate_id},
            )
            run_id = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            before = learning.read_candidate(candidate_id)

            with mock.patch.object(controller, "_write_queue_unlocked", side_effect=OSError("queue write failed")):
                with self.assertRaisesRegex(OSError, "queue write failed"):
                    controller.cancel(run_id)

            after = learning.read_candidate(candidate_id)
            self.assertEqual(after["status"], "approved")
            self.assertEqual(after["approved_by"], before["approved_by"])
            self.assertEqual(after["approved_content_hash"], before["approved_content_hash"])
            self.assertEqual(after["approval_run_id"], run_id)
            self.assertEqual(controller.queue_status()["items"][0]["status"], "queued")
            self.assertNotIn(
                "run/cancelled",
                [event["type"] for event in controller.store.events_for(run_id)],
            )

    def test_second_cancel_repairs_missing_run_cancelled_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="取消事件恢复概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "取消事件恢复概念",
                {"action": "approve", "note": "先排队", "candidate_id": candidate_id},
            )
            run_id = controller.commit_reviews()["submitted"][0]["run"]["run_id"]
            original_append = controller.store.append

            def fail_cancel_event(
                target_run_id: str,
                event_type: str,
                data: object = None,
                actor: str = "codex-runner",
            ) -> object:
                if event_type == "run/cancelled":
                    raise OSError("event write failed")
                return original_append(target_run_id, event_type, data, actor)

            with mock.patch.object(controller.store, "append", side_effect=fail_cancel_event):
                with self.assertRaisesRegex(OSError, "event write failed"):
                    controller.cancel(run_id)

            self.assertEqual(controller.queue_status()["items"][0]["status"], "cancelled")
            self.assertEqual(learning.read_candidate(candidate_id)["status"], "ready_for_review")
            self.assertNotEqual(controller.store.state(run_id)["status"], "cancelled")

            repaired = controller.cancel(run_id)

            self.assertEqual(repaired["status"], "cancelled")
            event_types = [event["type"] for event in controller.store.events_for(run_id)]
            self.assertEqual(event_types.count("run/cancelled"), 1)

    def test_failed_worker_cas_does_not_overwrite_concurrent_published_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="并发发布概念")
            candidate_id = str(candidate["candidate_id"])
            learning.update_candidate(
                candidate_id,
                expected_statuses={"ready_for_review"},
                status="approved",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
            )
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = controller.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            controller._enqueue_publish(run_id, candidate_id, "并发发布概念")

            def publish_elsewhere(*_args: object, **_kwargs: object) -> mock.Mock:
                learning.update_candidate(
                    candidate_id,
                    expected_statuses={"approved"},
                    status="published",
                    proposed_version="v2",
                )
                return mock.Mock(returncode=1)

            with mock.patch("pm_loop_control_plane_server.subprocess.run", side_effect=publish_elsewhere):
                with mock.patch.object(controller, "_recover_running_queue_items", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        controller._publish_worker()

            self.assertEqual(learning.read_candidate(candidate_id)["status"], "published")
            self.assertEqual(controller.queue_status()["items"][0]["status"], "failed")

    def test_enqueue_failure_rolls_back_candidate_and_second_commit_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="补偿概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            controller.stage_review(
                "补偿概念",
                {"action": "approve", "note": "验证队列补偿", "candidate_id": candidate_id},
            )

            with mock.patch.object(controller, "_enqueue_publish", side_effect=OSError("queue write failed")):
                first = controller.commit_reviews()

            self.assertEqual(first["submitted"], [])
            self.assertEqual(len(first["failed"]), 1)
            rolled_back = learning.read_candidate(candidate_id)
            self.assertEqual(rolled_back["status"], "ready_for_review")
            self.assertIsNone(rolled_back["approved_by"])
            self.assertIsNone(rolled_back["approval_run_id"])
            first_run_id = controller.store.list_states()[0]["run_id"]
            self.assertEqual(controller.store.state(first_run_id)["status"], "failed")

            second = controller.commit_reviews()

            self.assertEqual(second["failed"], [])
            self.assertEqual(len(second["submitted"]), 1)
            self.assertEqual(learning.read_candidate(candidate_id)["status"], "approved")
            self.assertEqual(controller.queue_status()["items"][0]["status"], "queued")

    def test_startup_reconciles_approved_candidate_missing_queue_item(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="恢复概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                first = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = first.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            first.store.append(run_id, "gate/approved", {"candidate_id": candidate_id}, actor="reviewer")
            learning.update_candidate(
                candidate_id,
                expected_statuses={"ready_for_review"},
                status="approved",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
                approval_run_id=run_id,
            )

            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                recovered = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
                repeated = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            queue = recovered.queue_status()["items"]
            self.assertEqual(len(queue), 1)
            self.assertEqual(queue[0]["status"], "queued")
            self.assertEqual(queue[0]["candidate_id"], candidate_id)
            self.assertEqual(len(repeated.queue_status()["items"]), 1)
            event_types = [event["type"] for event in recovered.store.events_for(run_id)]
            self.assertEqual(event_types.count("run/started"), 1)
            self.assertEqual(event_types.count("action/queued"), 1)

    def test_startup_rolls_back_orphan_approval_for_terminal_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="终态恢复概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                first = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = first.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            first.store.append(run_id, "run/failed", {"error": "queue persistence failed"})
            learning.update_candidate(
                candidate_id,
                expected_statuses={"ready_for_review"},
                status="approved",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
                approval_run_id=run_id,
            )

            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                recovered = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            after = learning.read_candidate(candidate_id)
            self.assertEqual(after["status"], "ready_for_review")
            self.assertIsNone(after["approved_by"])
            self.assertIsNone(after["approved_content_hash"])
            self.assertIsNone(after["approval_run_id"])
            self.assertEqual(recovered.queue_status()["items"], [])

    def test_stale_running_retry_limit_marks_candidate_publish_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="租约超限概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                first = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = first.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            first.store.append(run_id, "run/started", {"candidate_id": candidate_id})
            learning.update_candidate(
                candidate_id,
                expected_statuses={"ready_for_review"},
                status="publishing",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
                approval_run_id=run_id,
            )
            first._write_queue(
                [
                    {
                        "queue_id": f"publish-{run_id}",
                        "run_id": run_id,
                        "candidate_id": candidate_id,
                        "concept": "租约超限概念",
                        "status": "running",
                        "attempts": 3,
                        "updated_at": "2020-01-01T00:00:00Z",
                        "heartbeat_at": "2020-01-01T00:00:00Z",
                    }
                ]
            )

            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                recovered = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            self.assertEqual(learning.read_candidate(candidate_id)["status"], "publish_failed")
            self.assertEqual(recovered.queue_status()["items"][0]["status"], "failed")
            self.assertEqual(recovered.store.state(run_id)["status"], "failed")

    def test_terminal_publishing_orphan_is_recoverable_through_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="发布尾窗概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                first = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            request = first.store.create({"loop_id": "concept-review", "permission_mode": "approved_action"})
            run_id = request["run_id"]
            first.store.append(run_id, "run/failed", {"error": "process crashed after publish started"})
            learning.update_candidate(
                candidate_id,
                expected_statuses={"ready_for_review"},
                status="publishing",
                approved_by="zhujie14",
                approved_content_hash=candidate["content_hash"],
                approval_run_id=run_id,
            )

            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                recovered = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)

            self.assertEqual(learning.read_candidate(candidate_id)["status"], "publish_failed")
            failed_item = recovered.queue_status()["items"][0]
            self.assertEqual(failed_item["status"], "failed")
            self.assertEqual(failed_item["recovery_reason"], "terminal_run_with_publishing_candidate")

            retried = recovered.retry_publish(run_id)

            self.assertEqual(retried["status"], "queued")
            self.assertEqual(learning.read_candidate(candidate_id)["status"], "approved")
            self.assertEqual(recovered.store.state(run_id)["status"], "retrying")

    def test_stage_and_commit_are_linearized_by_review_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, learning, candidate = make_review_fixture(root, concept="线性化概念")
            candidate_id = str(candidate["candidate_id"])
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                controller = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            write_entered = threading.Event()
            allow_write = threading.Event()
            commit_done = threading.Event()
            results: dict[str, object] = {}
            errors: list[BaseException] = []
            original_write = controller._write_staged

            def blocked_write(value: dict[str, object]) -> None:
                write_entered.set()
                if not allow_write.wait(2):
                    raise TimeoutError("test did not release staged write")
                original_write(value)

            def stage() -> None:
                try:
                    results["stage"] = controller.stage_review(
                        "线性化概念",
                        {"action": "approve", "note": "等待提交", "candidate_id": candidate_id},
                    )
                except BaseException as exc:  # pragma: no cover - assertion reports thread failure
                    errors.append(exc)

            def commit() -> None:
                try:
                    results["commit"] = controller.commit_reviews()
                except BaseException as exc:  # pragma: no cover - assertion reports thread failure
                    errors.append(exc)
                finally:
                    commit_done.set()

            with mock.patch.object(controller, "_write_staged", side_effect=blocked_write):
                stage_thread = threading.Thread(target=stage)
                commit_thread = threading.Thread(target=commit)
                stage_thread.start()
                self.assertTrue(write_entered.wait(1))
                commit_thread.start()
                self.assertFalse(commit_done.wait(0.05))
                allow_write.set()
                stage_thread.join(2)
                commit_thread.join(2)

            self.assertEqual(errors, [])
            self.assertFalse(stage_thread.is_alive())
            self.assertFalse(commit_thread.is_alive())
            commit_result = results["commit"]
            self.assertIsInstance(commit_result, dict)
            self.assertEqual(len(commit_result["submitted"]), 1)
            self.assertEqual(learning.read_candidate(candidate_id)["status"], "approved")

    def test_review_transaction_blocks_second_control_plane_instance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex_root, _skill_root, _learning, candidate = make_review_fixture(root, concept="跨实例概念")
            with mock.patch.object(ControlPlane, "_publish_worker", return_value=None):
                first = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
                second = ControlPlane(root / "pm-loop", root / "adapter.py", root, codex_root, root)
            started = threading.Event()
            finished = threading.Event()
            errors: list[BaseException] = []

            def stage_on_second() -> None:
                started.set()
                try:
                    second.stage_review(
                        "跨实例概念",
                        {
                            "action": "approve",
                            "note": "跨实例锁验证",
                            "candidate_id": candidate["candidate_id"],
                        },
                    )
                except BaseException as exc:  # pragma: no cover - assertion reports thread failure
                    errors.append(exc)
                finally:
                    finished.set()

            with first._review_transaction():
                thread = threading.Thread(target=stage_on_second)
                thread.start()
                self.assertTrue(started.wait(1))
                self.assertFalse(finished.wait(0.05))

            thread.join(2)
            self.assertEqual(errors, [])
            self.assertFalse(thread.is_alive())
            self.assertEqual(second._read_staged()["跨实例概念"]["action"], "approve")


if __name__ == "__main__":
    unittest.main()
