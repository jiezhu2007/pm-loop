from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_inventory_compaction import build_plan, run  # noqa: E402


class ConceptInventoryCompactionTests(unittest.TestCase):
    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _state(self, root: Path) -> Path:
        state = root / "state"
        inventory = state / "full-inventory"
        self._write_json(inventory / "content-dedup.json", {"a": [1, 2], "b": "content"})
        with gzip.open(inventory / "content-dedup.json.gz", "wb") as stream:
            stream.write((inventory / "content-dedup.json").read_bytes())
        self._write_json(inventory / "evidence-cache.json", {"records": [{"uri": "viking://a", "revision": "1"}]})
        with gzip.open(inventory / "evidence-cache.json.gz", "wt", encoding="utf-8") as stream:
            json.dump({"records": [{"revision": "1", "uri": "viking://a"}]}, stream)
        deep = inventory / "runs" / "deep-inventory-20260820T120658Z-6257c2"
        self._write_json(deep / "resources.json", {"uris": ["viking://a"]})
        self._write_json(deep / "manifest.json", {"status": "completed", "resource_count": 1})
        self._write_json(deep / "evidence" / "batch.json", {"evidence": True})
        return state

    def _consumer_inputs(self, root: Path) -> tuple[tuple[Path, ...], Path, Path]:
        ledgers = (root / "sync-ledger.json", root / "public-ledger.json")
        self._write_json(ledgers[0], {"sync": {"target_uri": "viking://resources/shengsuan/a.md", "name": "a.md"}})
        self._write_json(ledgers[1], {})
        concepts = root / "concepts-ledger.json"
        self._write_json(concepts, {"A": {"status": "active", "sources": ["viking://resources/shengsuan/a.md"]}})
        manifest_tool = root / "source_manifest.py"
        manifest_tool.write_text(
            "import argparse,json\n"
            "p=argparse.ArgumentParser();p.add_argument('--ledger',action='append');p.add_argument('--inventory');p.add_argument('--concepts-ledger');p.add_argument('--output');a=p.parse_args()\n"
            "v=json.load(open(a.inventory));u=v.get('uris',[])\n"
            "json.dump({'document_mappings':[{'uri':x,'status':'mapped'} for x in u],'active_source_unique_checks':[],'metrics':{'document_count':len(u)},'conflicts':{}},open(a.output,'w'))\n",
            encoding="utf-8",
        )
        return ledgers, concepts, manifest_tool

    def test_only_verified_content_dedup_is_apply_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = self._state(Path(temp))
            plan = build_plan(state)
            targets = {item["target"]: item for item in plan["targets"]}
            self.assertEqual(targets["content-dedup.json"]["status"], "eligible")
            self.assertGreaterEqual(targets["content-dedup.json"]["estimated_reclaim_bytes"], 0)
            self.assertEqual(targets["evidence-cache.json"]["status"], "held")
            self.assertEqual(targets["evidence-cache.json"]["reason_code"], "not_in_allowlist")
            self.assertEqual(targets["runs/deep-inventory-20260820T120658Z-6257c2"]["reason_code"], "consumer_compatibility_not_implemented")

    def test_stage_keeps_deep_run_and_apply_only_replaces_content_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = self._state(root)
            ledgers, concepts, manifest_tool = self._consumer_inputs(root)
            staged = run(state, run_id="stage", stage_deep_archive=True, source_ledgers=ledgers, concepts_ledger=concepts, manifest_tool=manifest_tool)
            self.assertEqual(staged["status"], "staged_verified")
            self.assertTrue(Path(staged["deep_archive"]["archive"]).is_file())
            self.assertTrue(Path(staged["deep_archive"]["projection"]).is_file())
            self.assertEqual(staged["deep_archive"]["consumer_smoke"]["status"], "passed")
            self.assertTrue(Path(staged["latest_marker"]).is_file())
            self.assertTrue((state / "full-inventory" / "runs" / "deep-inventory-20260820T120658Z-6257c2").is_dir())

            verified_plan = build_plan(state)
            verified_deep = next(item for item in verified_plan["targets"] if item["target"].startswith("runs/"))
            self.assertEqual(verified_deep["status"], "eligible")
            self.assertEqual(verified_deep["reason_code"], "consumer_archive_verified")
            self.assertEqual(verified_deep["consumer_archive_verification"]["verification_run_id"], "stage")

            applied = run(state, run_id="apply", apply=True, confirmation=True)
            self.assertEqual(applied["status"], "applied")
            self.assertFalse((state / "full-inventory" / "content-dedup.json").exists())
            self.assertTrue((state / "full-inventory" / "content-dedup.json.gz").is_file())
            self.assertTrue((state / "full-inventory" / "evidence-cache.json").is_file())
            self.assertTrue((state / "full-inventory" / "runs" / "deep-inventory-20260820T120658Z-6257c2").is_dir())

            replacement = applied["content_replacement"]
            self.assertEqual(replacement["status"], "applied")
            self.assertEqual(replacement["canary"]["status"], "passed")
            self.assertFalse(Path(replacement["manifest"]).name.endswith("intent.json"))
            self.assertTrue(Path(replacement["manifest"]).is_file())
            self.assertTrue(Path(replacement["intent_manifest"]).is_file())
            self.assertFalse((state / "full-inventory" / "compaction" / "runs" / "apply" / "quarantine" / "content-dedup.json").exists())

    def test_deep_archive_replacement_uses_projection_and_archive_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = self._state(root)
            ledgers, concepts, manifest_tool = self._consumer_inputs(root)
            staged = run(state, run_id="stage-deep", stage_deep_archive=True, source_ledgers=ledgers, concepts_ledger=concepts, manifest_tool=manifest_tool)
            archive = Path(staged["deep_archive"]["archive"])
            applied = run(
                state,
                run_id="apply-deep",
                apply_deep_archive=True,
                deep_confirmation=True,
                source_ledgers=ledgers,
                concepts_ledger=concepts,
                manifest_tool=manifest_tool,
            )
            self.assertEqual(applied["status"], "applied")
            self.assertFalse((state / "full-inventory" / "runs" / "deep-inventory-20260820T120658Z-6257c2").exists())
            self.assertTrue(archive.is_file())
            self.assertTrue((state / "full-inventory" / "compatible-resources" / "current" / "resources.json").is_file())
            self.assertEqual(applied["deep_replacement"]["consumer_smoke"]["status"], "passed")
            closed = build_plan(state)
            deep = next(item for item in closed["targets"] if item["target"].startswith("runs/"))
            self.assertEqual(deep["status"], "archived")
            self.assertEqual(deep["reason_code"], "archive_replacement_verified")

    def test_canary_failure_restores_original_and_leaves_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = self._state(root)
            consumer = root / "failing_consumer.py"
            consumer.write_text(
                "def _read_json(path, default=None):\n"
                "    raise RuntimeError('intentional canary failure')\n",
                encoding="utf-8",
            )
            before = (state / "full-inventory" / "content-dedup.json").read_bytes()
            applied = run(
                state, run_id="failed-canary", apply=True, confirmation=True, content_consumer=consumer,
            )
            replacement = applied["content_replacement"]
            self.assertEqual(applied["status"], "reverted")
            self.assertEqual(replacement["status"], "reverted")
            self.assertEqual(replacement["recovery"]["status"], "restored")
            self.assertEqual((state / "full-inventory" / "content-dedup.json").read_bytes(), before)
            self.assertTrue((state / "full-inventory" / "content-dedup.json.gz").is_file())
            self.assertTrue(Path(replacement["manifest"]).is_file())
            self.assertTrue(Path(replacement["intent_manifest"]).is_file())
            self.assertFalse((state / "full-inventory" / "compaction" / "runs" / "failed-canary" / "quarantine" / "content-dedup.json").exists())

    def test_hash_drift_is_rejected_before_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = self._state(Path(temp))
            plan = build_plan(state)
            original = state / "full-inventory" / "content-dedup.json"
            original.write_text(original.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "changed after planning"):
                from concept_inventory_compaction import apply_content_replacement
                apply_content_replacement(plan, output_root=state / "full-inventory" / "compaction" / "runs" / "drift", confirmation=True)
            self.assertTrue(original.is_file())
            self.assertFalse((state / "full-inventory" / "compaction" / "runs" / "drift" / "quarantine").exists())


if __name__ == "__main__":
    unittest.main()
