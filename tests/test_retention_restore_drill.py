from __future__ import annotations

import gzip
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from retention_restore_drill import run, safe_extract_regular_files  # noqa: E402


class RetentionRestoreDrillTests(unittest.TestCase):
    def test_safe_extract_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "unsafe.tar"
            member = tarfile.TarInfo("../escape.txt")
            member.size = 4
            with tarfile.open(archive, "w") as bundle:
                bundle.addfile(member, io.BytesIO(b"oops"))
            destination = root / "destination"
            destination.mkdir()
            with tarfile.open(archive, "r") as bundle:
                with self.assertRaises(ValueError):
                    safe_extract_regular_files(bundle, destination)
            self.assertFalse((root / "escape.txt").exists())

    def test_run_restores_both_candidates_without_changing_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            content = state / "content-dedup.json"
            content.write_text('{"fixture":true}\n', encoding="utf-8")
            with gzip.open(state / "content-dedup.json.gz", "wb", compresslevel=1) as stream:
                stream.write(content.read_bytes())

            deep = state / "runs" / "deep-inventory-20260820T120658Z-6257c2"
            deep.mkdir(parents=True)
            (deep / "resources.json").write_text("[]\n", encoding="utf-8")
            (deep / "taxonomy.json").write_text("{}\n", encoding="utf-8")
            (deep / "manifest.json").write_text(json.dumps({
                "schema_version": "concept-learning.deep-inventory.v2",
                "status": "completed",
                "resource_count": 5735,
                "resources_artifact": "resources.json",
                "taxonomy_artifact": "taxonomy.json",
                "progress": {"processed": 5735, "read": 5735, "unreadable": 0},
            }), encoding="utf-8")
            before = {path.relative_to(state).as_posix(): path.read_bytes() for path in state.rglob("*") if path.is_file()}
            output = root / "result.json"
            result = run(state_root=state, deep_run=deep, output=output, temp_root=root)
            after = {path.relative_to(state).as_posix(): path.read_bytes() for path in state.rglob("*") if path.is_file()}

            self.assertEqual(result["status"], "passed")
            self.assertTrue(result["content_dedup"]["source_identity_unchanged"])
            self.assertTrue(result["deep_inventory"]["manifest_match"])
            self.assertFalse(result["originals_modified"])
            self.assertEqual(before, after)
            self.assertTrue(output.is_file())


if __name__ == "__main__":
    unittest.main()
