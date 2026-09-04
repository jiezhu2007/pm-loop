from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_scheduler import Scheduler  # noqa: E402
from pm_system_v45_g8 import run_g8  # noqa: E402
from pm_system_v45_migration import _check, run_stage  # noqa: E402


class V45MigrationTests(unittest.TestCase):
    def test_stage_cannot_skip_previous_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report = run_stage("G2", db_path=root / "pm.db", report_dir=root / "reports", migration_id="m", epoch="e", owner="test", apply=False, lease_seconds=30)
            self.assertEqual(report["decision"], "HOLD")
            self.assertIn("previous stage gate", {item["name"] for item in report["checks"]})
            self.assertTrue((root / "reports" / "g2-检查报告.md").is_file())
            self.assertTrue((root / "reports" / "g2-检查报告.html").is_file())

    def test_g0_apply_writes_persistent_freeze_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report = run_stage("G0", db_path=root / "pm.db", report_dir=root / "reports", migration_id="m", epoch="e", owner="test", apply=True, lease_seconds=30)
            self.assertEqual(report["decision"], "PASS")
            self.assertEqual(report["snapshot"]["schema_version"], 6)
            freeze = PMSystemStore(root / "pm.db", max_schema_version=7).migration_freeze()
            self.assertEqual(freeze["migration_id"], "m")
            self.assertEqual(freeze["state"], "freeze")
            self.assertEqual(PMSystemStore(root / "pm.db", max_schema_version=7).schema_version(), 7)

    def test_same_stage_revalidation_is_explicit_and_preserves_the_pass_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            kwargs = {
                "db_path": root / "pm.db",
                "report_dir": root / "reports",
                "migration_id": "m",
                "epoch": "e",
                "owner": "test",
                "lease_seconds": 30,
            }
            first = run_stage("G0", apply=True, **kwargs)
            duplicate = run_stage("G0", apply=False, **kwargs)
            canonical = json.loads((root / "reports/g0-检查报告.json").read_text(encoding="utf-8"))
            self.assertEqual(first["decision"], "PASS")
            self.assertEqual(duplicate["decision"], "HOLD")
            self.assertTrue(duplicate["canonical_report_preserved"])
            self.assertEqual(canonical["decision"], "PASS")

            revalidated = run_stage("G0", apply=False, revalidate=True, **kwargs)
            self.assertEqual(revalidated["decision"], "PASS")
            self.assertEqual(revalidated["mode"], "revalidate-check")
            self.assertEqual(revalidated["revalidation"]["previous_decision"], "PASS")

    def test_g8_gate_requires_recovery_manifest_and_final_watermarks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            for name in ("source", "content", "knowledge", "active_generation"):
                store.put_watermark(
                    source_domain="pm-runtime",
                    watermark_name=name,
                    captured_at=1,
                    sequence=1,
                    value=f"{name}-v1",
                    producer="test",
                )
            manifest_path = root / "g8-recovery-manifest.json"
            manifest_path.write_text(json.dumps(run_g8(), ensure_ascii=False), encoding="utf-8")
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            self.assertTrue(all(item["status"] == "PASS" for item in checks), checks)

            with store.transaction() as connection:
                connection.execute(
                    "UPDATE watermarks SET state='unknown' WHERE source_domain='pm-runtime' AND watermark_name='knowledge'"
                )
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final structured watermarks"]["status"], "HOLD")

    def test_g8_allows_frozen_pending_memory_event_as_deferred(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            store.set_migration_freeze(
                migration_id="m",
                migration_epoch="e",
                stage_id="G8",
                owner="test",
                deadline_at="2099-01-01T00:00:00Z",
            )
            store.enqueue_memory_change(
                name="2026-08-30.md",
                mtime=1,
                content_hash="memory-hash",
                snapshot_uri="viking://resources/memory/2026-08-30.md/2026-08-30.md",
                file_path=str(root / "2026-08-30.md"),
                namespace_epoch="e",
            )
            for name in ("source", "content", "knowledge", "active_generation"):
                store.put_watermark(
                    source_domain="pm-runtime",
                    watermark_name=name,
                    captured_at=1,
                    sequence=1,
                    value=f"{name}-v1",
                    producer="test",
                )
            manifest_path = root / "g8-recovery-manifest.json"
            manifest_path.write_text(json.dumps(run_g8(), ensure_ascii=False), encoding="utf-8")
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final drain and orphan fence"]["status"], "PASS")
            detail = json.loads(by_name["final drain and orphan fence"]["detail"])
            self.assertEqual(detail["outbox"], 0)
            self.assertEqual(detail["memory_deferred"], 1)
            self.assertEqual(detail["memory_event_orphans"], 0)

            with store.transaction() as connection:
                connection.execute("UPDATE outbox_items SET profile='wrong-profile' WHERE kind='memory'")
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final drain and orphan fence"]["status"], "HOLD")
            detail = json.loads(by_name["final drain and orphan fence"]["detail"])
            self.assertEqual(detail["memory_event_orphans"], 1)

    def test_g8_allows_missing_active_generation_only_as_disabled_capability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            for name in ("source", "content", "knowledge"):
                store.put_watermark(
                    source_domain="pm-runtime",
                    watermark_name=name,
                    captured_at=1,
                    sequence=1,
                    value={"status": "accepted"},
                    producer="test",
                )
            store.put_watermark(
                source_domain="pm-runtime",
                watermark_name="active_generation",
                captured_at=1,
                sequence=1,
                value={
                    "status": "missing",
                    "reason": "concept refresh is disabled",
                    "refresh_disabled": True,
                },
                producer="test",
                state="missing",
            )
            manifest_path = root / "g8-recovery-manifest.json"
            manifest_path.write_text(json.dumps(run_g8(), ensure_ascii=False), encoding="utf-8")
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final structured watermarks"]["status"], "SKIPPED/HOLD")
            self.assertIn("active_generation_rows=0", by_name["final structured watermarks"]["detail"])

    def test_g8_rejects_missing_active_generation_when_refresh_is_not_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            for name in ("source", "content", "knowledge"):
                store.put_watermark(
                    source_domain="pm-runtime",
                    watermark_name=name,
                    captured_at=1,
                    sequence=1,
                    value={"status": "accepted"},
                    producer="test",
                )
            store.put_watermark(
                source_domain="pm-runtime",
                watermark_name="active_generation",
                captured_at=1,
                sequence=1,
                value={"status": "missing", "reason": "no marker", "refresh_disabled": False},
                producer="test",
                state="missing",
            )
            manifest_path = root / "g8-recovery-manifest.json"
            manifest_path.write_text(json.dumps(run_g8(), ensure_ascii=False), encoding="utf-8")
            checks = _check(
                "G8",
                store,
                apply=False,
                migration_id="m",
                epoch="e",
                g8_manifest_path=manifest_path,
            )
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final structured watermarks"]["status"], "HOLD")

    def test_g8_drain_counts_non_resource_outbox_and_probe_leases(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            now = "2026-08-30T00:00:00Z"
            scheduler = Scheduler(store, max_slots=1)
            accepted = store.accept({"job_type": "run", "loop_id": "g8-active-model", "idempotency_key": "g8-active-model"})
            scheduler.claim_next(worker_id="g8-active-model")
            scheduler.begin_model_call(accepted["run_id"], stage="analysis", model_input_hash="hash", prompt_version="v1", provider="oneapi")
            with store.transaction() as connection:
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,kind,resource_id,revision_id,processing_mode,provider,profile,namespace_epoch,payload_json,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,? ,?,?)",
                    ("skill-outbox", "skill-key", "skill", "skill-resource", "r1", "vectors_only", "oneapi", "skill-sync", "e", "{}", "pending", now, now),
                )
                connection.execute(
                    "INSERT INTO provider_buckets(provider_key,provider,endpoint,model,updated_at) VALUES(?,?,?,?,?)",
                    ("oneapi|endpoint|model", "oneapi", "endpoint", "model", now),
                )
                connection.execute(
                    "INSERT INTO provider_probe_leases(provider_key,probe_token,leased_at,expires_at) VALUES(?,?,?,?)",
                    ("oneapi|endpoint|model", "probe-token", now, "2099-01-01T00:00:00Z"),
                )
            for name in ("source", "content", "knowledge"):
                store.put_watermark(
                    source_domain="pm-runtime",
                    watermark_name=name,
                    captured_at=1,
                    sequence=1,
                    value={"status": "accepted"},
                    producer="test",
                )
            store.put_watermark(
                source_domain="pm-runtime",
                watermark_name="active_generation",
                captured_at=1,
                sequence=1,
                value={"status": "missing", "reason": "concept refresh is disabled", "refresh_disabled": True},
                producer="test",
                state="missing",
            )
            manifest_path = root / "g8-recovery-manifest.json"
            manifest_path.write_text(json.dumps(run_g8(), ensure_ascii=False), encoding="utf-8")
            checks = _check("G8", store, apply=False, migration_id="m", epoch="e", g8_manifest_path=manifest_path)
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["final drain and orphan fence"]["status"], "HOLD")
            detail = json.loads(by_name["final drain and orphan fence"]["detail"])
            self.assertEqual(detail["outbox"], 1)
            self.assertEqual(detail["provider_probe_leases"], 1)
            self.assertEqual(detail["model_calls"], 1)

    def test_g9_requires_resolved_read_only_review_and_current_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            store.set_migration_freeze(
                migration_id="m",
                migration_epoch="e",
                stage_id="G9",
                owner="test",
                deadline_at="2099-01-01T00:00:00Z",
            )
            artifact = root / "reviewed.txt"
            artifact.write_text("reviewed", encoding="utf-8")
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            canonical = root / "canonical.py"
            runtime = root / "runtime.py"
            canonical.write_text("module = 1\n", encoding="utf-8")
            runtime.write_text("module = 1\n", encoding="utf-8")
            runtime_digest = hashlib.sha256(canonical.read_bytes()).hexdigest()
            manifest_path = root / "g9-independent-review-manifest.json"
            manifest = {
                "decision": "PASS",
                "read_only": True,
                "production_state_touched": False,
                "external_provider_calls": 0,
                "rounds": [
                    {"round": 1, "p0": 0, "p1": 1, "findings": [{"id": "P1-1", "severity": "P1", "status": "resolved"}]},
                    {"round": 2, "p0": 0, "p1": 0, "findings": []},
                ],
                "reviewed_artifacts": [{"path": str(artifact), "sha256": digest}],
                "runtime_hashes": [{
                    "name": "module",
                    "canonical_path": str(canonical),
                    "runtime_path": str(runtime),
                    "canonical_sha256": runtime_digest,
                    "runtime_sha256": runtime_digest,
                }],
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            checks = _check("G9", store, apply=False, migration_id="m", epoch="e", g9_manifest_path=manifest_path)
            self.assertTrue(all(item["status"] == "PASS" for item in checks), checks)

            artifact.write_text("changed", encoding="utf-8")
            checks = _check("G9", store, apply=False, migration_id="m", epoch="e", g9_manifest_path=manifest_path)
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["reviewed artifact hashes"]["status"], "HOLD")

            artifact.write_text("reviewed", encoding="utf-8")
            runtime.write_text("module = 2\n", encoding="utf-8")
            checks = _check("G9", store, apply=False, migration_id="m", epoch="e", g9_manifest_path=manifest_path)
            by_name = {item["name"]: item for item in checks}
            self.assertEqual(by_name["canonical/runtime hash convergence"]["status"], "HOLD")


if __name__ == "__main__":
    unittest.main()
