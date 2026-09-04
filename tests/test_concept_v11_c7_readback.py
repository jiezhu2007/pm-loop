from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_c7_readback import collect  # noqa: E402


class C7ReadbackTests(unittest.TestCase):
    def test_collect_deduplicates_uris_and_hashes_verified_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifest.json"
            output = root / "readback.json"
            manifest.write_text(json.dumps({
                "schema_version": "concept-source-manifest.v1",
                "active_source_checks": [
                    {"source_uri": "viking://resources/test/a", "status": "mapped"},
                    {"source_uri": "viking://resources/test/a", "status": "mapped"},
                    {"source_uri": "viking://resources/test/b", "status": "mapped"},
                ],
            }), encoding="utf-8")

            class Completed:
                returncode = 0
                stderr = ""
                stdout = json.dumps({"status": "ok", "result": "body"})

            with patch("concept_v11_c7_readback.subprocess.run", return_value=Completed()):
                result = collect(manifest_path=manifest, output_path=output, concurrency=2, ov_rest=root / "ov_rest.py")
            self.assertEqual(result["unique_uri_count"], 2)
            self.assertEqual(result["verified_count"], 2)
            self.assertEqual(result["failed_count"], 0)
            self.assertEqual(result["rows"][0]["content_sha256"], "sha256:" + "body".encode().hex()[:0] + __import__("hashlib").sha256(b"body").hexdigest())
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["external_writes"], {"openviking": 0, "database": 0})

    def test_collect_records_read_failure_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifest.json"
            output = root / "readback.json"
            manifest.write_text(json.dumps({"schema_version": "concept-source-manifest.v1", "active_source_checks": [{"source_uri": "viking://resources/test/a"}]}), encoding="utf-8")

            class Failed:
                returncode = 1
                stderr = "HTTP 404"
                stdout = ""

            with patch("concept_v11_c7_readback.subprocess.run", return_value=Failed()):
                result = collect(manifest_path=manifest, output_path=output, concurrency=1, ov_rest=root / "ov_rest.py")
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["failed_count"], 1)
            self.assertEqual(result["rows"][0]["error"], "HTTP 404")


if __name__ == "__main__":
    unittest.main()
