from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "concept_content_audit_queue.py"
SPEC = importlib.util.spec_from_file_location("concept_content_audit_queue", MODULE_PATH)
assert SPEC and SPEC.loader
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


NOW = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


def row(source_id: str, path: str, *, active: bool = False, publish: str = "2026-08-01T00:00:00Z"):
    return {
        "source_id": source_id,
        "source": "repo",
        "path": path,
        "name": path.rsplit("/", 1)[-1],
        "name_hash": "sha256:" + source_id,
        "revision_kind": "name_hash",
        "heuristic": True,
        "origin": "ledger",
        "metadata_status": "observed",
        "publishTime": publish,
    }


def source_for_bucket(bucket: int, prefix: str) -> str:
    """Find a deterministic test id in one rotation bucket."""
    for index in range(10000):
        source_id = f"{prefix}-{index}"
        if audit.stable_bucket(source_id) == bucket:
            return source_id
    raise AssertionError(f"could not find source id for bucket {bucket}")


def sources_for_bucket(bucket: int, prefix: str, count: int, bucket_count: int = audit.BUCKET_COUNT):
    found = []
    for index in range(100000):
        source_id = f"{prefix}-{index}"
        if audit.stable_bucket(source_id, bucket_count) == bucket:
            found.append(source_id)
            if len(found) == count:
                return found
    raise AssertionError(f"could not find {count} source ids for bucket {bucket}")


class ConceptContentAuditQueueTests(unittest.TestCase):
    def test_rotation_is_stable_and_active_source_is_prioritized(self) -> None:
        active = row("s-active", "viking://docs/active.md")
        normal = row("s-normal", "viking://docs/normal.md")
        manifest = {
            "name_hash_rule": "source+path+name:v1",
            "documents": [active, normal],
            "active_source_checks": [{
                "concept": "概念", "source_uri": active["path"],
                "status": "mapped", "matched_source_ids": [active["source_id"]],
            }],
            "unmapped_active_sources": [],
        }
        first = audit.build_audit_queue(manifest, now=NOW, limit=100)
        second = audit.build_audit_queue(manifest, now=NOW, limit=100)
        self.assertEqual(first["selected_bucket"], second["selected_bucket"])
        self.assertEqual([x["source_id"] for x in first["items"]], [x["source_id"] for x in second["items"]])
        self.assertEqual(first["items"][0]["priority"], "active")

    def test_initial_normal_sources_only_enter_current_rotation_bucket(self) -> None:
        selected_bucket = audit._week_bucket(NOW)
        in_bucket_id = source_for_bucket(selected_bucket, "normal-in")
        out_bucket_id = source_for_bucket((selected_bucket + 1) % audit.BUCKET_COUNT, "normal-out")
        manifest = {
            "documents": [
                row(in_bucket_id, "viking://docs/in.md"),
                row(out_bucket_id, "viking://docs/out.md"),
            ],
            "active_source_checks": [],
            "unmapped_active_sources": [],
        }

        result = audit.build_audit_queue(manifest, now=NOW, limit=100)

        self.assertEqual(result["metrics"]["due_count"], 0)
        self.assertEqual(result["metrics"]["planned_count"], 1)
        self.assertEqual(result["metrics"]["initial_rotation_count"], 1)
        self.assertEqual(result["items"][0]["source_id"], in_bucket_id)
        self.assertEqual(result["items"][0]["reason"], "initial_rotation_bucket")

    def test_initial_active_source_is_queued_even_outside_rotation_bucket(self) -> None:
        selected_bucket = audit._week_bucket(NOW)
        active_id = source_for_bucket((selected_bucket + 1) % audit.BUCKET_COUNT, "active-initial")
        normal_id = source_for_bucket((selected_bucket + 1) % audit.BUCKET_COUNT, "normal-initial")
        active = row(active_id, "viking://docs/active-initial.md")
        normal = row(normal_id, "viking://docs/normal-initial.md")
        manifest = {
            "documents": [active, normal],
            "active_source_checks": [{
                "concept": "概念",
                "source_uri": active["path"],
                "status": "mapped",
                "matched_source_ids": [active_id],
            }],
            "unmapped_active_sources": [],
        }

        result = audit.build_audit_queue(manifest, now=NOW, limit=100)

        self.assertEqual(result["metrics"]["due_count"], 0)
        self.assertEqual(result["metrics"]["initial_active_count"], 1)
        self.assertEqual([item["source_id"] for item in result["items"]], [active_id])
        self.assertEqual(result["items"][0]["reason"], "initial_active_audit")

    def test_previous_normal_audit_becomes_due_after_ninety_days(self) -> None:
        current = row("normal-old", "viking://docs/old.md")
        manifest = {"documents": [current], "active_source_checks": [], "unmapped_active_sources": []}
        previous = {"observed": {"normal-old": {
            "name_hash": current["name_hash"],
            "publish_time": current["publishTime"],
            "last_content_audit_at": "2026-05-01T00:00:00Z",
        }}}

        result = audit.build_audit_queue(manifest, previous, now=NOW, limit=100)

        self.assertEqual(result["metrics"]["due_count"], 1)
        self.assertIn("audit_due", result["items"][0]["reason"])

    def test_recent_normal_audit_outside_rotation_bucket_is_not_due(self) -> None:
        selected_bucket = audit._week_bucket(NOW)
        source_id = source_for_bucket((selected_bucket + 1) % audit.BUCKET_COUNT, "normal-recent")
        current = row(source_id, "viking://docs/recent.md")
        manifest = {"documents": [current], "active_source_checks": [], "unmapped_active_sources": []}
        previous = {"observed": {source_id: {
            "name_hash": current["name_hash"],
            "publish_time": current["publishTime"],
            "last_content_audit_at": "2026-08-20T00:00:00Z",
        }}}

        result = audit.build_audit_queue(manifest, previous, now=NOW, limit=100)

        self.assertEqual(result["metrics"]["due_count"], 0)
        self.assertEqual(result["metrics"]["planned_count"], 0)
        self.assertEqual(result["items"], [])

    def test_rotation_cursor_pages_large_bucket_without_repeating(self) -> None:
        # A one-bucket fixture makes the cursor behavior independent of the
        # calendar week and forces more candidates than the per-run limit.
        source_ids = sources_for_bucket(0, "cursor", 10, bucket_count=1)
        manifest = {
            "documents": [
                row(source_id, f"viking://docs/{index}.md")
                for index, source_id in enumerate(source_ids)
            ],
            "active_source_checks": [],
            "unmapped_active_sources": [],
        }

        first = audit.build_audit_queue(manifest, now=NOW, limit=3, bucket_count=1)
        second = audit.build_audit_queue(manifest, first, now=NOW, limit=3, bucket_count=1)
        third = audit.build_audit_queue(manifest, second, now=NOW, limit=3, bucket_count=1)
        fourth = audit.build_audit_queue(manifest, third, now=NOW, limit=3, bucket_count=1)

        first_ids = [item["source_id"] for item in first["items"]]
        second_ids = [item["source_id"] for item in second["items"]]
        all_ids = first_ids + second_ids + [item["source_id"] for item in third["items"]]
        all_ids += [item["source_id"] for item in fourth["items"]]
        self.assertEqual(len(first_ids), 3)
        self.assertEqual(len(second_ids), 3)
        self.assertTrue(set(first_ids).isdisjoint(second_ids))
        self.assertEqual(set(all_ids), set(source_ids))
        self.assertEqual(len(all_ids), len(set(all_ids)))
        self.assertEqual(first["metrics"]["rotation_offset"], 0)
        self.assertEqual(first["metrics"]["rotation_next_offset"], 3)
        self.assertEqual(second["metrics"]["rotation_offset"], 3)
        self.assertEqual(second["metrics"]["rotation_next_offset"], 6)
        self.assertEqual(fourth["metrics"]["rotation_selected_count"], 1)
        self.assertEqual(fourth["rotation_offsets"]["0"], 10)

    def test_mandatory_items_do_not_advance_rotation_cursor_for_unselected_rows(self) -> None:
        source_ids = sources_for_bucket(0, "mandatory-cursor", 4, bucket_count=1)
        active = row(source_ids[0], "viking://docs/active.md")
        normals = [row(source_id, f"viking://docs/normal-{index}.md") for index, source_id in enumerate(source_ids[1:])]
        manifest = {
            "documents": [active, *normals],
            "active_source_checks": [{
                "concept": "概念",
                "source_uri": active["path"],
                "status": "mapped",
                "matched_source_ids": [active["source_id"]],
            }],
            "unmapped_active_sources": [],
        }

        result = audit.build_audit_queue(manifest, now=NOW, limit=2, bucket_count=1)

        self.assertEqual(result["metrics"]["mandatory_selected_count"], 1)
        self.assertEqual(result["metrics"]["rotation_selected_count"], 1)
        self.assertEqual(result["metrics"]["rotation_offset"], 0)
        self.assertEqual(result["metrics"]["rotation_next_offset"], 1)
        self.assertEqual(result["items"][0]["source_id"], active["source_id"])

    def test_name_unchanged_publish_time_change_becomes_suspect(self) -> None:
        current = row("s1", "viking://docs/a.md", publish="2026-08-22T00:00:00Z")
        manifest = {"documents": [current], "active_source_checks": [], "unmapped_active_sources": []}
        previous = {"observed": {"s1": {
            "name_hash": current["name_hash"],
            "publish_time": "2026-08-01T00:00:00Z",
            "last_content_audit_at": "2026-08-20T00:00:00Z",
        }}}
        result = audit.build_audit_queue(manifest, previous, now=NOW, limit=100)
        self.assertEqual(result["metrics"]["publish_suspect_count"], 1)
        self.assertIn("publish_time_changed", result["items"][0]["reason"])

    def test_legacy_and_current_name_prefixes_are_equivalent(self) -> None:
        current = row("s-prefix", "viking://docs/prefix.md")
        digest = current["name_hash"][len(audit.LEGACY_NAME_HASH_PREFIX):]
        current["name_hash"] = audit.NAME_HASH_PREFIX + digest
        manifest = {"documents": [current], "active_source_checks": [], "unmapped_active_sources": []}
        previous = {"observed": {"s-prefix": {
            "name_hash": audit.LEGACY_NAME_HASH_PREFIX + digest,
            "publish_time": current["publishTime"],
            "last_content_audit_at": "2026-08-20T00:00:00Z",
        }}}

        result = audit.build_audit_queue(manifest, previous, now=NOW, limit=100)

        self.assertEqual(result["metrics"]["publish_suspect_count"], 0)
        self.assertEqual(result["items"], [])
        self.assertEqual(result["name_hash_prefix"], audit.NAME_HASH_PREFIX)
        self.assertEqual(result["name_hash_format"], audit.NAME_HASH_FORMAT)

    def test_unmapped_active_source_is_forced_into_queue(self) -> None:
        manifest = {
            "documents": [],
            "active_source_checks": [],
            "unmapped_active_sources": [{"concept": "概念", "source_uri": "viking://docs/missing.md"}],
        }
        result = audit.build_audit_queue(manifest, now=NOW, limit=100)
        self.assertEqual(result["metrics"]["unmapped_active_count"], 1)
        self.assertEqual(result["items"][0]["metadata_status"], "unmapped")
        self.assertEqual(result["items"][0]["priority"], "active")

    def test_limit_is_enforced_without_dropping_observed_state(self) -> None:
        docs = [row(f"s{i}", f"viking://docs/{i}.md") for i in range(20)]
        manifest = {"documents": docs, "active_source_checks": [], "unmapped_active_sources": []}
        result = audit.build_audit_queue(manifest, now=NOW, limit=3)
        self.assertEqual(len(result["items"]), 3)
        self.assertEqual(len(result["observed"]), 20)


if __name__ == "__main__":
    unittest.main()
