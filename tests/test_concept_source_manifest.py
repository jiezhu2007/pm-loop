from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


# The project keeps the implementation in a descriptive module while the
# installed skill exposes the same implementation from its runtime entrypoint.
PROJECT_MODULE_PATH = Path(__file__).parents[1] / "scripts" / "concept_source_manifest.py"
CANONICAL_MODULE_PATH = Path(__file__).parents[1] / "scripts" / "source_manifest.py"
MODULE_PATH = PROJECT_MODULE_PATH if PROJECT_MODULE_PATH.exists() else CANONICAL_MODULE_PATH
SPEC = importlib.util.spec_from_file_location("concept_source_manifest", MODULE_PATH)
assert SPEC and SPEC.loader
manifest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(manifest)


class ConceptSourceManifestTests(unittest.TestCase):
    def test_name_hash_is_stable_and_separate_from_content(self) -> None:
        first = manifest.name_hash("data-agent", "viking://docs/a", "需求说明")
        second = manifest.name_hash("data-agent", "viking://docs/a", "需求说明")
        renamed = manifest.name_hash("data-agent", "viking://docs/a", "新名称")

        self.assertEqual(first, second)
        self.assertNotEqual(first, renamed)
        self.assertTrue(first.startswith(manifest.NAME_HASH_PREFIX))
        self.assertEqual(manifest.NAME_HASH_FORMAT, "namepath-v1")
        self.assertEqual(manifest.NAME_HASH_RULE, "source+path+name:v1")

    def test_legacy_name_hash_prefix_is_canonicalized_only_in_name_fields(self) -> None:
        current = manifest.name_hash("repo", "viking://docs/a", "A")
        legacy = current.replace(manifest.NAME_HASH_PREFIX, manifest.LEGACY_NAME_HASH_PREFIX, 1)
        self.assertEqual(manifest.canonical_name_hash(legacy), current)
        self.assertTrue(manifest.name_hash_equal(legacy, current))

    def test_load_rows_uses_ledger_key_as_fallback_document_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "ledger.json"
            path.write_text(
                json.dumps(
                    {
                        "doc-key": {
                            "target_uri": "viking://resources/shengsuan/data-agent/a",
                            "name": "A",
                            "source": "data-agent",
                        }
                    }
                ),
                encoding="utf-8",
            )

            rows = manifest.load_metadata_rows([path])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_id"], "data-agent:doc-key")
        self.assertEqual(rows[0]["revision_kind"], "name_hash")
        self.assertTrue(rows[0]["heuristic"])
        self.assertEqual(rows[0]["name_hash_prefix"], manifest.NAME_HASH_PREFIX)
        self.assertEqual(rows[0]["name_hash_format"], manifest.NAME_HASH_FORMAT)

    def test_parent_mapping_is_unambiguous_and_inventory_only_is_not_a_source_match(self) -> None:
        rows = manifest.load_metadata_rows(
            [],
            inventory_path=None,
        )
        # A source ledger parent should map a leaf source, while a leaf that
        # exists only in the inventory must remain unmapped until its parent
        # metadata is available.
        rows = [
            {
                "source_id": "repo:parent",
                "source": "repo",
                "path": "viking://docs/page.html",
                "name": "Page",
                "name_hash": "sha256:p",
                "origin": "ledger",
            },
            {
                "source_id": "repo:leaf-inventory",
                "source": "repo",
                "path": "viking://docs/other.html/leaf.md",
                "name": "leaf.md",
                "name_hash": "sha256:i",
                "origin": "inventory",
            },
        ]
        concepts = {
            "mapped": {
                "status": "active",
                "sources": ["viking://docs/page.html/leaf.md"],
            },
            "inventory-only": {
                "status": "active",
                "sources": ["viking://docs/other.html/leaf.md"],
            },
        }

        payload = manifest.build_manifest(rows, concepts, generated_at="2026-01-01T00:00:00Z")
        checks = {row["concept"]: row for row in payload["active_source_checks"]}

        self.assertEqual(checks["mapped"]["status"], "mapped")
        self.assertEqual(checks["mapped"]["match_mode"], "tree")
        self.assertEqual(checks["inventory-only"]["status"], "unmapped")
        self.assertEqual(payload["generated_at"], "2026-01-01T00:00:00Z")

    def test_duplicate_uri_with_distinct_source_ids_is_reported_as_conflict(self) -> None:
        rows = [
            {
                "source_id": "repo:g1",
                "source": "repo",
                "doc_guid": "g1",
                "path": "viking://docs/same",
                "name": "Same",
                "name_hash": "sha256:1",
                "origin": "ledger",
            },
            {
                "source_id": "repo:g2",
                "source": "repo",
                "doc_guid": "g2",
                "path": "viking://docs/same",
                "name": "Same",
                "name_hash": "sha256:2",
                "origin": "ledger",
            },
        ]

        payload = manifest.build_manifest(rows, {}, generated_at="2026-01-01T00:00:00Z")

        self.assertEqual(payload["metrics"]["metadata_conflict_uri_count"], 1)
        self.assertEqual(payload["conflicts"], {"viking://docs/same": ["repo:g1", "repo:g2"]})
        self.assertEqual(
            {row["metadata_status"] for row in payload["documents"]},
            {"conflict"},
        )

    def test_active_source_metrics_expose_coverage_and_unmapped_rows(self) -> None:
        rows = [
            {
                "source_id": "repo:g1",
                "source": "repo",
                "path": "viking://docs/a",
                "name": "A",
                "name_hash": "sha256:a",
                "origin": "ledger",
            }
        ]
        concepts = {
            "one": {"status": "active", "sources": ["viking://docs/a"]},
            "two": {"status": "active", "sources": ["viking://docs/missing"]},
            "archived": {"status": "archived", "sources": ["viking://docs/missing"]},
        }

        payload = manifest.build_manifest(rows, concepts, generated_at="2026-01-01T00:00:00Z")
        metrics = payload["metrics"]

        self.assertEqual(metrics["active_source_count"], 2)
        self.assertEqual(metrics["mapped_active_source_count"], 1)
        self.assertEqual(metrics["unmapped_active_source_count"], 1)
        self.assertEqual(metrics["mapping_coverage"], 0.5)
        self.assertEqual([row["concept"] for row in payload["unmapped_active_sources"]], ["two"])

    def test_active_source_reference_and_unique_metrics_are_both_exposed(self) -> None:
        rows = [
            {
                "source_id": "repo:a",
                "source": "repo",
                "path": "viking://docs/a",
                "name": "A",
                "name_hash": "sha256:a",
                "origin": "ledger",
            },
            {
                "source_id": "repo:b",
                "source": "repo",
                "path": "viking://docs/b",
                "name": "B",
                "name_hash": "sha256:b",
                "origin": "ledger",
            },
            {
                "source_id": "repo:d1",
                "source": "repo",
                "path": "viking://docs/d",
                "name": "D",
                "name_hash": "sha256:d1",
                "origin": "ledger",
            },
            {
                "source_id": "repo:d2",
                "source": "repo",
                "path": "viking://docs/d",
                "name": "D",
                "name_hash": "sha256:d2",
                "origin": "ledger",
            },
        ]
        concepts = {
            # `a` is deliberately repeated in one concept and referenced by
            # another concept; the unique URI count must not inflate.
            "one": {
                "status": "active",
                "sources": [
                    "viking://docs/a",
                    "viking://docs/a",
                    "viking://docs/b",
                    "viking://docs/d",
                ],
            },
            "two": {
                "status": "active",
                "sources": ["viking://docs/a", "viking://docs/c", "viking://docs/d"],
            },
        }

        payload = manifest.build_manifest(rows, concepts, generated_at="2026-01-01T00:00:00Z")
        metrics = payload["metrics"]

        # Seven references: a x3, b x1, c x1, d x2.  Four unique URIs.
        self.assertEqual(metrics["active_source_reference_count"], 7)
        self.assertEqual(metrics["active_source_unique_count"], 4)
        self.assertEqual(metrics["active_source_count"], metrics["active_source_reference_count"])
        self.assertEqual(metrics["mapped_active_source_reference_count"], 4)
        self.assertEqual(metrics["unmapped_active_source_reference_count"], 1)
        self.assertEqual(metrics["conflict_active_source_reference_count"], 2)
        self.assertEqual(metrics["mapped_active_source_unique_count"], 2)
        self.assertEqual(metrics["unmapped_active_source_unique_count"], 1)
        self.assertEqual(metrics["conflict_active_source_unique_count"], 1)
        self.assertEqual(metrics["mapped_active_source_count"], 4)
        self.assertEqual(metrics["mapping_coverage"], round(4 / 7, 6))
        self.assertEqual(metrics["mapping_unique_coverage"], 0.5)

        unique_checks = {row["source_uri"]: row for row in payload["active_source_unique_checks"]}
        self.assertEqual(unique_checks["viking://docs/a"]["reference_count"], 3)
        self.assertEqual(unique_checks["viking://docs/d"]["status"], "conflict")

    def test_compact_manifest_keeps_new_mapping_metrics(self) -> None:
        payload = manifest.build_manifest(
            [
                {
                    "source_id": "repo:a",
                    "source": "repo",
                    "path": "viking://docs/a",
                    "name": "A",
                    "name_hash": "sha256:a",
                    "origin": "ledger",
                }
            ],
            {"概念": {"status": "active", "sources": ["viking://docs/a"]}},
            generated_at="2026-01-01T00:00:00Z",
        )
        compact = manifest.compact_manifest(payload)
        self.assertEqual(
            compact["active_source_reference_count"],
            payload["metrics"]["active_source_reference_count"],
        )
        self.assertEqual(
            compact["active_source_unique_count"],
            payload["metrics"]["active_source_unique_count"],
        )
        self.assertEqual(compact["mapping_unique_coverage"], 1.0)
        self.assertEqual(compact["name_hash_prefix"], manifest.NAME_HASH_PREFIX)
        self.assertEqual(compact["name_hash_format"], manifest.NAME_HASH_FORMAT)


if __name__ == "__main__":
    unittest.main()
