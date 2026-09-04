from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_v45_g5 import _canonical_skill, _hash_json, build_source_manifest  # noqa: E402


class V45G5Tests(unittest.TestCase):
    def test_canonical_hash_ignores_yaml_rendering(self) -> None:
        first = "---\nname: demo\ndescription: a long description\n---\n\nBody\n"
        second = "---\nname: demo\ndescription: >-\n  a long description\n---\nBody"
        self.assertEqual(_hash_json(_canonical_skill(first)), _hash_json(_canonical_skill(second)))

    def test_manifest_contains_only_minimal_package_and_stable_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill = root / "demo"
            (skill / "scripts").mkdir(parents=True)
            (skill / "state").mkdir()
            (skill / "SKILL.md").write_text(
                "---\nname: demo\ndescription: demo skill\n---\n\nBody\n",
                encoding="utf-8",
            )
            (skill / "scripts" / "run.py").write_text("print('ok')\n", encoding="utf-8")
            (skill / "state" / "latest.json").write_text("{}\n", encoding="utf-8")
            first = build_source_manifest(root, epoch="e", target_scope="viking://agent/skills")
            second = build_source_manifest(root, epoch="e", target_scope="viking://agent/skills")
            self.assertEqual(first["skill_count"], 1)
            self.assertEqual(first["skills"][0]["minimal_package_files"], ["SKILL.md"])
            self.assertEqual(first["skills"][0]["source_file_count"], 2)
            self.assertEqual(first["skills"][0]["canonical_hash"], second["skills"][0]["canonical_hash"])

    def test_manifest_records_directory_name_mismatch_without_losing_native_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            skill = root / "directory-name"
            skill.mkdir()
            (skill / "SKILL.md").write_text(
                "---\nname: other-name\ndescription: demo\n---\n",
                encoding="utf-8",
            )
            manifest = build_source_manifest(root, epoch="e", target_scope="viking://agent/skills")
            self.assertEqual(manifest["skills"][0]["name"], "other-name")
            self.assertFalse(manifest["skills"][0]["name_matches_directory"])


if __name__ == "__main__":
    unittest.main()
