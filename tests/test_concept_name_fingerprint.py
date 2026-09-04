from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "concept_name_fingerprint.py"
SPEC = importlib.util.spec_from_file_location("concept_name_fingerprint", MODULE_PATH)
assert SPEC and SPEC.loader
fingerprint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fingerprint)


class ConceptNameFingerprintTests(unittest.TestCase):
    def test_same_name_is_stable_even_when_body_would_change(self) -> None:
        first = {"source": "data-agent", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "需求说明"}
        second = {**first, "content": "完全不同的正文"}
        first_hash = fingerprint.fingerprint_row(first)["name_hash"]
        second_hash = fingerprint.fingerprint_row(second)["name_hash"]
        self.assertEqual(first_hash, second_hash)
        self.assertTrue(first_hash.startswith(fingerprint.NAME_HASH_PREFIX))
        self.assertFalse(first_hash.startswith(fingerprint.LEGACY_NAME_HASH_PREFIX))

    def test_legacy_name_prefix_compares_equal_without_changing_content_hash_semantics(self) -> None:
        current = fingerprint.fingerprint_row(
            {"source": "repo", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "A"}
        )
        digest = current["name_hash"][len(fingerprint.NAME_HASH_PREFIX):]
        legacy = {**current, "name_hash": fingerprint.LEGACY_NAME_HASH_PREFIX + digest}

        self.assertEqual(fingerprint.canonical_name_hash(legacy["name_hash"]), current["name_hash"])
        self.assertTrue(fingerprint.name_hash_equal(legacy["name_hash"], current["name_hash"]))

        result = fingerprint.compare_snapshots(
            {current["document_id"]: current},
            {legacy["document_id"]: legacy},
        )
        self.assertEqual(result["changed_count"], 0)
        self.assertEqual(result["unchanged_count"], 1)

    def test_rename_or_move_is_a_change(self) -> None:
        first = fingerprint.fingerprint_row(
            {"source": "s", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "旧名称"}
        )
        renamed = fingerprint.fingerprint_row(
            {"source": "s", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "新名称"}
        )
        moved = fingerprint.fingerprint_row(
            {"source": "s", "docGuid": "g1", "target_uri": "viking://other/a", "name": "旧名称"}
        )
        self.assertNotEqual(first["name_hash"], renamed["name_hash"])
        self.assertNotEqual(first["name_hash"], moved["name_hash"])

    def test_source_and_guid_prevent_same_name_collision(self) -> None:
        left = fingerprint.fingerprint_row(
            {"source": "left", "docGuid": "same", "target_uri": "viking://docs/a", "name": "同名"}
        )
        right = fingerprint.fingerprint_row(
            {"source": "right", "docGuid": "same", "target_uri": "viking://docs/a", "name": "同名"}
        )
        self.assertNotEqual(left["document_id"], right["document_id"])
        self.assertNotEqual(left["name_hash"], right["name_hash"])

    def test_snapshot_comparison_is_idempotent(self) -> None:
        rows = [{"source": "s", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "A"}]
        current = fingerprint.snapshot_rows(rows)
        result = fingerprint.compare_snapshots(current, current)
        self.assertEqual(result["changed_count"], 0)
        self.assertEqual(result["new_count"], 0)
        self.assertEqual(result["unchanged_count"], 1)
        self.assertEqual(result["revision_kind"], "name_hash")
        self.assertTrue(result["heuristic"])

    def test_legacy_sha256_name_prefix_is_compatible(self) -> None:
        row = fingerprint.fingerprint_row(
            {"source": "s", "docGuid": "g1", "target_uri": "viking://docs/a", "name": "A"}
        )
        legacy = row["name_hash"].replace(fingerprint.NAME_HASH_PREFIX, fingerprint.LEGACY_NAME_HASH_PREFIX, 1)
        self.assertEqual(fingerprint.canonical_name_hash(legacy), row["name_hash"])
        self.assertTrue(fingerprint.name_hash_equal(legacy, row["name_hash"]))


if __name__ == "__main__":
    unittest.main()
