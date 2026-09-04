from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.concept_v11_c6_provider_shadow import reconcile_provider_shadow, run_provider_shadow


class ShadowTransport:
    def upload_file(self, path: Path, *, timeout: float) -> dict:
        return {"result": {"temp_file_id": "temp-shadow"}}

    def add_resource(self, *, temp_file_id: str, target_uri: str, timeout: float) -> dict:
        return {"result": {"task_id": "task-shadow"}}

    def get_task(self, task_id: str, *, timeout: float) -> dict:
        return {
            "result": {
                "status": "completed",
                "result": {
                    "queue_status": {
                        "Semantic": {"processed": 1, "requeue_count": 0, "error_count": 0}
                    }
                },
            }
        }

    def glob_content(self, uri: str, *, timeout: float) -> dict:
        return {"result": {"matches": [uri + "/probe.md"]}}

    def read_content(self, uri: str, *, timeout: float) -> dict:
        return {"result": "provider provenance sample\n"}


class ConceptV11ProviderShadowTests(unittest.TestCase):
    def test_reads_the_discovered_leaf_when_openviking_normalizes_extension(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "probe.txt"
            source.write_text("provider provenance sample\n", encoding="utf-8")
            report = run_provider_shadow(
                transport=ShadowTransport(),
                source=source,
                target_uri="viking://resources/__pm_v11_provider_shadow__/test/sample",
                approved_model="auto",
                observation_seconds=1,
                poll_seconds=0.1,
                request_timeout=1,
            )

        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["model_resolution_status"], "unknown")
        self.assertEqual(report["model_resolution_gate"], "provider_configuration_trusted")
        self.assertTrue(report["content_verified"])
        self.assertEqual(
            report["read_back_uri"],
            "viking://resources/__pm_v11_provider_shadow__/test/sample/probe.md",
        )

    def test_reconciles_only_a_leaf_name_failure_without_a_second_semantic_task(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "probe.txt"
            source.write_text("provider provenance sample\n", encoding="utf-8")
            legacy = {
                "schema": "concept-v11.c6-provider-shadow.v1",
                "stage_id": "C6-PROVIDER-SHADOW",
                "status": "HOLD",
                "read_only_pm_database": True,
                "concept_admission_changed": False,
                "namespace_isolated": True,
                "target_uri": "viking://resources/__pm_v11_provider_shadow__/test/sample",
                "processing_mode": "semantic_and_vectors",
                "wait": False,
                "model_requested": "auto",
                "approved_model_for_shadow": "auto",
                "model_resolved": None,
                "model_resolution_status": "unknown",
                "source_hash": "sha256:" + __import__("hashlib").sha256(source.read_bytes()).hexdigest(),
                "accepted": True,
                "task_id": "task-shadow",
                "remote_status": "completed",
                "queue_status": {"Semantic": {"processed": 1, "requeue_count": 0, "error_count": 0}},
                "errors": ["RuntimeError:HTTP 404: File not found: old-name.txt"],
                "external_provider_calls": 1,
            }
            legacy_path = root / "legacy.json"
            legacy_path.write_text(__import__("json").dumps(legacy), encoding="utf-8")
            report = reconcile_provider_shadow(
                transport=ShadowTransport(),
                source=source,
                legacy_report_path=legacy_path,
                readback_uri="viking://resources/__pm_v11_provider_shadow__/test/sample/probe.md",
                request_timeout=1,
            )

        self.assertEqual(report["status"], "PASS")
        self.assertTrue(report["content_verified"])
        self.assertEqual(report["reconciliation"]["read_only_openviking_requests"], 1)
        self.assertEqual(report["external_provider_calls"], 1)
