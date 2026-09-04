from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_source_candidate_discovery import build_report  # noqa: E402


class ConceptV11SourceCandidateDiscoveryTests(unittest.TestCase):
    def test_only_exact_current_ledger_candidates_are_read_and_reported(self) -> None:
        manifest = {
            "schema_version": "concept-source-manifest.v1",
            "generated_at": "2026-09-02T10:00:00Z",
            "document_mappings": [
                {"uri": "viking://resources/shengsuan/current.md", "status": "mapped", "source_id": "current:1", "match_mode": "exact"},
                {"uri": "viking://resources/shengsuan/conflict.md", "status": "conflict", "source_id": "", "match_mode": "exact"},
            ],
        }
        coverage = {
            "schema": "concept-v11.source-coverage-report.v1",
            "report_hash": "sha256:" + "a" * 64,
            "concepts": [{"concept": "概念A", "coverage_status": "needs_repair"}],
        }
        read_uris: list[str] = []

        def search(_: str) -> dict:
            return {
                "status": "ok",
                "result": {
                    "resources": [
                        {"uri": "viking://resources/shengsuan/current.md"},
                        {"uri": "viking://resources/shengsuan/conflict.md"},
                        {"uri": "viking://resources/shengsuan/semantic-only.md"},
                    ]
                },
            }

        def read(uri: str) -> dict:
            read_uris.append(uri)
            return {"status": "ok", "result": "verified body"}

        report = build_report(
            source_manifest=manifest,
            coverage_report=coverage,
            search=search,
            read=read,
            generated_at="2026-09-02T10:01:00Z",
        )
        self.assertEqual(read_uris, ["viking://resources/shengsuan/current.md"])
        self.assertEqual(report["candidate_count"], 1)
        candidate = report["candidates"][0]
        self.assertEqual(candidate["source_id"], "current:1")
        self.assertEqual(candidate["readback"]["status"], "verified")
        self.assertIsNone(candidate["automatic_disposition"])
        self.assertEqual(report["writes"], {"database": 0, "openviking": 0, "dispositions": 0})

    def test_find_failure_does_not_invent_candidates(self) -> None:
        manifest = {"schema_version": "concept-source-manifest.v1", "document_mappings": []}
        coverage = {
            "schema": "concept-v11.source-coverage-report.v1",
            "report_hash": "sha256:" + "a" * 64,
            "concepts": [{"concept": "概念A", "coverage_status": "needs_repair"}],
        }

        def search(_: str) -> dict:
            raise RuntimeError("unavailable")

        report = build_report(source_manifest=manifest, coverage_report=coverage, search=search, read=lambda _: {})
        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["candidate_count"], 0)
        self.assertEqual(report["failures"][0]["stage"], "find")

    def test_current_mapped_directory_expands_to_verified_leaf_candidates(self) -> None:
        manifest = {
            "schema_version": "concept-source-manifest.v1",
            "document_mappings": [
                {"uri": "viking://resources/shengsuan/directory", "status": "mapped", "source_id": "current:1", "match_mode": "exact"},
            ],
        }
        coverage = {
            "schema": "concept-v11.source-coverage-report.v1",
            "report_hash": "sha256:" + "a" * 64,
            "concepts": [{"concept": "目录", "coverage_status": "needs_repair"}],
        }

        def read(uri: str) -> dict:
            if uri == "viking://resources/shengsuan/directory":
                return {"status": "error", "error": {"message": "Directory URI is not readable as a file"}}
            return {"status": "ok", "result": uri}

        report = build_report(
            source_manifest=manifest,
            coverage_report=coverage,
            search=lambda _: {"status": "ok", "result": {"resources": [{"uri": "viking://resources/shengsuan/directory"}]}},
            read=read,
            glob=lambda _: [
                "viking://resources/shengsuan/directory/first.md",
                "viking://resources/shengsuan/directory/second.md",
            ],
        )
        self.assertEqual(report["candidate_count"], 2)
        self.assertEqual(report["qualified_candidate_count"], 2)
        self.assertTrue(all(row["source_directory_uri"] == "viking://resources/shengsuan/directory" for row in report["candidates"]))


if __name__ == "__main__":
    unittest.main()
