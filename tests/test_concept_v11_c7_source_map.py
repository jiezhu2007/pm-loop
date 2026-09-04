from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_c7_source_map import apply_closure, build_closure, verify_closure  # noqa: E402
from concept_v11_schema import migrate_schema  # noqa: E402
from concept_v11_schema_v2 import migrate_schema_v2  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


EPOCH = "concept-test-epoch"
OWNER = "concept-c7-test"


def source_manifest() -> dict:
    return {
        "schema_version": "concept-source-manifest.v1",
        "generated_at": "2026-08-31T00:00:00Z",
        "heuristic": True,
        "active_source_checks": [
            {
                "concept": "Mapped",
                "source_uri": "viking://resources/test/mapped.md",
                "status": "mapped",
                "match_mode": "exact",
                "matched_source_ids": ["test:one"],
                "matched_paths": ["viking://resources/test/mapped.md"],
            },
            {
                "concept": "Missing",
                "source_uri": "viking://resources/test/missing.md",
                "status": "unmapped",
                "match_mode": "none",
                "matched_source_ids": [],
                "matched_paths": [],
            },
            {
                "concept": "Conflict",
                "source_uri": "viking://resources/test/conflict.md",
                "status": "conflict",
                "match_mode": "exact",
                "matched_source_ids": ["test:a", "test:b"],
                "matched_paths": ["viking://resources/test/conflict.md"],
            },
        ],
    }


def content_readback(*, mapped_ok: bool = True) -> dict:
    return {
        "schema": "concept-v11.c7-content-readback.v1",
        "observed_at": "2026-08-31T00:01:00Z",
        "unique_uri_count": 1,
        "verified_count": 1 if mapped_ok else 0,
        "failed_count": 0 if mapped_ok else 1,
        "rows": [
            {
                "uri": "viking://resources/test/mapped.md",
                "status": "verified" if mapped_ok else "failed",
                "bytes": 4,
                "content_sha256": "sha256:abcd" if mapped_ok else None,
            }
        ],
    }


class ConceptV11C7SourceMapTests(unittest.TestCase):
    def test_closure_only_maps_unique_identity_with_verified_content(self) -> None:
        closure = build_closure(
            manifest=source_manifest(),
            manifest_hash="sha256:manifest",
            readback=content_readback(),
            readback_hash="sha256:readback",
            namespace_epoch=EPOCH,
            owner=OWNER,
            observed_at="2026-08-31T00:02:00Z",
        )
        self.assertEqual(closure["terminal_status_counts"], {"mapped": 1, "quarantined": 2})
        mapped = next(row for row in closure["rows"] if row["concept"] == "Mapped")
        self.assertEqual(mapped["status"], "mapped")
        self.assertEqual(mapped["leaf_uri"], mapped["source_uri"])
        quarantined = [row for row in closure["rows"] if row["status"] == "quarantined"]
        self.assertTrue(all(row["owner"] == OWNER for row in quarantined))
        self.assertTrue(all(row["expires_at"] and row["next_action"] for row in quarantined))
        self.assertTrue(all(row["lineage"]["escalation"] for row in quarantined))

    def test_readback_failure_downgrades_metadata_mapping_to_quarantine(self) -> None:
        closure = build_closure(
            manifest=source_manifest(),
            manifest_hash="sha256:manifest",
            readback=content_readback(mapped_ok=False),
            readback_hash="sha256:readback",
            namespace_epoch=EPOCH,
            owner=OWNER,
            observed_at="2026-08-31T00:02:00Z",
        )
        mapped = next(row for row in closure["rows"] if row["concept"] == "Mapped")
        self.assertEqual(mapped["status"], "quarantined")
        self.assertEqual(mapped["resolution_reason"], "content_readback_failed")

    def test_apply_is_idempotent_and_keeps_admission_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            migrate_schema(
                store,
                migration_id="concept-test-v1",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            migrate_schema_v2(
                store,
                migration_id="concept-test-v2",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            closure = build_closure(
                manifest=source_manifest(),
                manifest_hash="sha256:manifest",
                readback=content_readback(),
                readback_hash="sha256:readback",
                namespace_epoch=EPOCH,
                owner=OWNER,
                observed_at="2026-08-31T00:02:00Z",
            )
            lease = store.acquire_migration_lease(
                migration_id="concept-c7-test",
                stage_id="C7-SOURCE-MAP-CLOSURE",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            first = apply_closure(
                store,
                closure=closure,
                namespace_epoch=EPOCH,
                owner=OWNER,
                lease_id=lease["lease_id"],
            )
            second = apply_closure(
                store,
                closure=closure,
                namespace_epoch=EPOCH,
                owner=OWNER,
                lease_id=lease["lease_id"],
            )
            self.assertEqual(first, {"inserted": 3, "updated": 0, "unchanged": 0})
            self.assertEqual(second, {"inserted": 0, "updated": 0, "unchanged": 3})
            verification = verify_closure(store, closure=closure, namespace_epoch=EPOCH)
            self.assertEqual(verification["status"], "PASS")
            self.assertEqual(verification["status_counts"], {"mapped": 1, "quarantined": 2})
            self.assertEqual(verification["concept_admission"]["admission_state"], "disabled")
            store.release_migration_lease(lease_id=lease["lease_id"])

    def test_verification_retains_prior_closure_rows_as_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            migrate_schema(
                store,
                migration_id="concept-test-v1",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            migrate_schema_v2(
                store,
                migration_id="concept-test-v2",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            closure = build_closure(
                manifest=source_manifest(),
                manifest_hash="sha256:manifest",
                readback=content_readback(),
                readback_hash="sha256:readback",
                namespace_epoch=EPOCH,
                owner=OWNER,
                observed_at="2026-08-31T00:02:00Z",
            )
            lease = store.acquire_migration_lease(
                migration_id="concept-c7-test",
                stage_id="C7-SOURCE-MAP-CLOSURE",
                migration_epoch=EPOCH,
                owner=OWNER,
            )
            apply_closure(store, closure=closure, namespace_epoch=EPOCH, owner=OWNER, lease_id=lease["lease_id"])
            store.release_migration_lease(lease_id=lease["lease_id"])
            verification = verify_closure(store, closure=closure, namespace_epoch=EPOCH)
            self.assertEqual(verification["status"], "PASS")
            self.assertEqual(verification["retained_historical_row_count"], 0)

            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO concept_source_map(" \
                    "map_id,concept_id,namespace_epoch,source_id,source_uri,leaf_uri,identity_method," \
                    "status,confidence,conflict_set_id,owner,evidence_refs_json,evidence_set_hash," \
                    "next_action,expires_at,lineage_json,resolved_at,resolved_by,resolution_reason,created_at,updated_at) " \
                    "SELECT 'map-history',concept_id,namespace_epoch,source_id,?,'',identity_method," \
                    "status,confidence,conflict_set_id,owner,evidence_refs_json,evidence_set_hash," \
                    "next_action,expires_at,?,resolved_at,resolved_by,resolution_reason,created_at,updated_at " \
                    "FROM concept_source_map WHERE source_uri=?",
                    ("viking://resources/test/prior.md", json.dumps({"closure_hash": "sha256:prior-closure"}), "viking://resources/test/missing.md"),
                )
            verification = verify_closure(store, closure=closure, namespace_epoch=EPOCH)
            self.assertEqual(verification["status"], "PASS")
            self.assertEqual(verification["retained_historical_row_count"], 1)
            self.assertEqual(verification["unaccounted_extra_row_count"], 0)


if __name__ == "__main__":
    unittest.main()
