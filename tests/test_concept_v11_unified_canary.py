from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_unified_canary import preflight  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


def file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


class UnifiedCanaryPreflightTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[PMSystemStore, Path, Path, Path]:
        store = PMSystemStore(root / "pm-system.db")
        with store.transaction() as connection:
            connection.execute(
                "CREATE TABLE concept_admissions (namespace_epoch TEXT PRIMARY KEY, admission_state TEXT NOT NULL, expires_at TEXT, updated_at TEXT NOT NULL, version INTEGER NOT NULL)"
            )
            connection.execute(
                "INSERT INTO concept_admissions VALUES(?,?,?,?,?)",
                ("epoch-1", "canary", (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"), "2026-09-03T00:00:00Z", 1),
            )
        manifest = root / "source-manifest.json"
        manifest.write_text(json.dumps({"schema_version": "concept-source-manifest.v1"}), encoding="utf-8")
        coverage = root / "coverage.json"
        coverage.write_text(json.dumps({
            "schema": "concept-v11.source-coverage-report.v1",
            "status": "PASS",
            "gate": {"p3_closed": True},
            "concept_count": 45,
            "concept_status_counts": {"needs_repair": 0},
            "source_manifest_hash": file_hash(manifest),
            "report_hash": "sha256:coverage",
        }), encoding="utf-8")
        content = root / "content-source.json"
        content.write_text(json.dumps({
            "status": "PASS",
            "coverage_report_hash": "sha256:coverage",
            "coverage_source_manifest_hash": file_hash(manifest),
            "summary": {"ready": 44},
        }), encoding="utf-8")
        return store, coverage, content, manifest

    def test_requires_coverage_bound_current_source_witness(self):
        with tempfile.TemporaryDirectory() as temp:
            store, coverage, content, manifest = self._fixture(Path(temp))
            result = preflight(
                store=store,
                coverage_path=coverage,
                content_preflight_path=content,
                source_manifest_path=manifest,
                now=datetime.now(timezone.utc),
            )
            self.assertEqual(result["source_manifest_hash"], file_hash(manifest))
            payload = json.loads(coverage.read_text(encoding="utf-8"))
            payload["source_manifest_hash"] = "sha256:" + "0" * 64
            coverage.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "coverage_source_manifest_hash_mismatch"):
                preflight(
                    store=store,
                    coverage_path=coverage,
                    content_preflight_path=content,
                    source_manifest_path=manifest,
                    now=datetime.now(timezone.utc),
                )


if __name__ == "__main__":
    unittest.main()
