from __future__ import annotations

import importlib
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))


class ConceptInventoryEntrypointTests(unittest.TestCase):
    def test_stable_entrypoint_delegates_to_deep_runner(self):
        wrapper = importlib.import_module("concept_inventory")
        deep = importlib.import_module("concept_deep_inventory")
        self.assertIs(wrapper.main, deep.main)

    def test_stable_entrypoint_does_not_reference_legacy_main_import(self):
        source = (SCRIPTS / "concept_inventory.py").read_text(encoding="utf-8")
        self.assertNotIn("from concept_full_inventory import main", source)
        self.assertIn("from concept_deep_inventory import main", source)

    def test_disabled_entrypoint_returns_without_network_or_state_writes(self):
        wrapper = importlib.import_module("concept_inventory")
        deep = importlib.import_module("concept_deep_inventory")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / "inventory.json"
            stdout = io.StringIO()
            with patch.object(deep, "OpenVikingClient") as client, patch.object(
                deep, "ConceptLearningStore"
            ) as store, redirect_stdout(stdout):
                result = wrapper.main(
                    ["--codex-root", str(root / "codex"), "--output", str(output)]
                )

            self.assertEqual(result, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "disabled")
            self.assertTrue(payload["read_only"])
            client.assert_not_called()
            store.assert_not_called()
            self.assertFalse(output.exists())
            self.assertEqual(list(root.rglob("*")), [])


if __name__ == "__main__":
    unittest.main()
