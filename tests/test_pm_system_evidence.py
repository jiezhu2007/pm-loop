from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_evidence import EvidenceGateway  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class EvidenceGatewayTests(unittest.TestCase):
    def test_snapshot_manifest_items_evidence_and_generation_are_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = EvidenceGateway(store)
            snapshot = gateway.commit_snapshot(source_id="ku", source_revision="2026-08-29", content_sha256="content-1", manifest={"items": [{"resource_id": "doc-1"}]})
            self.assertFalse(snapshot["deduplicated"])
            same = gateway.commit_snapshot(source_id="ku", source_revision="2026-08-29", content_sha256="content-1", manifest={"items": [{"resource_id": "doc-1"}]})
            self.assertTrue(same["deduplicated"])
            gateway.add_source_item(snapshot_id=snapshot["snapshot_id"], resource_id="doc-1", revision_id="rev-1", uri="viking://doc-1", content_sha256="content-1")
            self.assertTrue(gateway.check_manifest(snapshot["snapshot_id"])["consistent"])
            evidence = gateway.add_evidence(snapshot_id=snapshot["snapshot_id"], resource_id="doc-1", revision_id="rev-1", evidence_role="product-version", excerpt_hash="excerpt-1", verified=True)
            generation = gateway.stage_generation(domain="product", generation_hash="generation-1", source_watermark="2026-08-29", knowledge_watermark="knowledge-1")
            with store.transaction() as connection:
                connection.execute("UPDATE evidence_refs SET generation_id=? WHERE evidence_id=?", (generation["generation_id"], evidence["evidence_id"]))
            self.assertEqual(gateway.activate_generation(generation["generation_id"])["status"], "active")
            self.assertEqual(gateway.activate_generation(generation["generation_id"])["deduplicated"], True)
            self.assertEqual(gateway.watermarks()["knowledge"], "knowledge-1")

    def test_unverified_evidence_blocks_generation_activation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            gateway = EvidenceGateway(store)
            snapshot = gateway.commit_snapshot(source_id="ontology", source_revision="r1", content_sha256="c1", manifest={})
            generation = gateway.stage_generation(domain="ontology", generation_hash="g1", source_watermark="r1", knowledge_watermark="k1")
            gateway.add_evidence(snapshot_id=snapshot["snapshot_id"], resource_id="doc", revision_id="r1", evidence_role="requirement", excerpt_hash="e1", verified=False, generation_id=generation["generation_id"])
            with self.assertRaises(ValueError):
                gateway.activate_generation(generation["generation_id"])

    def test_timeline_event_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            gateway = EvidenceGateway(PMSystemStore(Path(temp) / "pm-system.db"))
            first = gateway.record_timeline_event(event_type="assessment", idempotency_key="assessment:1", payload={"subject": "demo"})
            second = gateway.record_timeline_event(event_type="assessment", idempotency_key="assessment:1", payload={"subject": "demo"})
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            self.assertEqual(first["timeline_event_id"], second["timeline_event_id"])


if __name__ == "__main__":
    unittest.main()

