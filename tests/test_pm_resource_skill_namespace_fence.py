from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_resource_dispatcher import PMResourceDispatcher, is_legacy_skill_resource_uri  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class SkillNamespaceFenceTests(unittest.TestCase):
    def test_old_skill_resource_namespace_is_detected(self) -> None:
        self.assertTrue(is_legacy_skill_resource_uri("viking://resources/skills"))
        self.assertTrue(is_legacy_skill_resource_uri("viking://resources/skills/demo/SKILL.md"))
        self.assertFalse(is_legacy_skill_resource_uri("viking://resources/project-docs/demo.md"))

    def test_enqueue_rejects_old_skill_namespace_before_outbox_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "SKILL.md"
            source.write_text("---\nname: demo\ndescription: demo\n---\n", encoding="utf-8")
            store = PMSystemStore(root / "pm.db")
            dispatcher = PMResourceDispatcher(store, artifact_root=root / "artifacts")
            with self.assertRaisesRegex(ValueError, "native /api/v1/skills"):
                dispatcher.enqueue_file(
                    path=source,
                    target_uri="viking://resources/skills/demo/SKILL.md",
                )
            with store.connect() as connection:
                count = int(connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0])
            self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
