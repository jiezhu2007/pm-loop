from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_production_canary import run_canary  # noqa: E402
from concept_v11_schema import migrate_schema, record_model_policy, record_probe  # noqa: E402
from concept_v11_schema_v2 import bind_profile_policy, migrate_schema_v2, set_admission_cas  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class FakeTransport:
    url = "http://fake-openviking"

    def __init__(self) -> None:
        self.expected_hash = ""
        self.tasks = {"task-1": {"status": "completed", "task_id": "task-1"}}
        self.uploads = []
        self.adds = []

    def upload_file(self, path: Path, *, timeout=None):
        self.uploads.append(Path(path))
        return {"result": {"temp_file_id": "temp-1"}}

    def add_resource(self, body, *, timeout=None, idempotency_key=None):
        self.adds.append((dict(body), idempotency_key))
        return {"status": "accepted", "task_id": "task-1"}

    def read_content(self, target_uri, *, timeout=None):
        return {"status": "ok", "result": {"content_hash": self.expected_hash}}

    def get_task(self, task_id: str, *, timeout=None):
        return self.tasks.get(task_id, {"status": "running"})


class ProductionCanaryTests(unittest.TestCase):
    def _setup(self, root: Path, *, admission_ttl: int = 3600) -> tuple[PMSystemStore, Path, Path, Path]:
        db = root / "pm-system.db"
        store = PMSystemStore(db)
        store.set_migration_freeze(
            migration_id="v45-concept-runtime-test",
            migration_epoch="v45-concept-runtime-test",
            stage_id="C2-SHARED-RUNTIME",
            owner="test",
            deadline_at="2099-01-01T00:00:00Z",
            state="released",
        )
        lease = store.acquire_migration_lease(migration_id="concept-v1", stage_id="C1", migration_epoch="v45-test", owner="test")
        migrate_schema(store, migration_id="concept-v1", migration_epoch="v45-test", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        lease = store.acquire_migration_lease(migration_id="concept-v2", stage_id="C2", migration_epoch="v45-test", owner="test")
        migrate_schema_v2(store, migration_id="concept-v2", migration_epoch="v45-test", owner="test", lease_id=lease["lease_id"])
        store.release_migration_lease(lease_id=lease["lease_id"])
        policy = record_model_policy(
            store,
            {
                "policy_version": "concept-v11-oneapi-auto-v1",
                "provider": "oneapi",
                "requested_model": "auto",
                "allowed_models": [],
                "capability_class": "concept-compiler-and-semantic",
                "privacy_scope": "local-private",
                "status": "active",
            },
        )
        bind_profile_policy(store, namespace_epoch="v45-test", policy_version=policy["policy_version"])
        set_admission_cas(
            store,
            namespace_epoch="v45-test",
            expected_state="disabled",
            expected_version=1,
            state="canary",
            snapshot_id="canary-snapshot-test",
            policy_version=policy["policy_version"],
            operator="test",
            evidence_hash="sha256:canary-evidence",
            ttl_seconds=admission_ttl,
        )
        with store.transaction() as connection:
            connection.execute(
                "INSERT INTO generations(generation_id,domain,generation_hash,status,source_watermark,knowledge_watermark,created_at,active_at) VALUES(?,?,?,?,?,?,?,?)",
                ("generation-test", "concepts", "sha256:generation-test", "active", "sha256:source", "sha256:knowledge", "2026-09-03T00:00:00Z", "2026-09-03T00:00:00Z"),
            )
        now = datetime.now(timezone.utc)
        for probe_type in ("client_accept_probe", "backend_semantic_probe"):
            record_probe(
                store,
                {
                    "probe_id": f"probe-{probe_type}",
                    "probe_type": probe_type,
                    "namespace_epoch": "v45-test",
                    "profile": "pm-semantic",
                    "processing_mode": "semantic_and_vectors",
                    "provider": "oneapi",
                    "model_policy_version": policy["policy_version"],
                    "capability_state": "ready",
                    "observed_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "expires_at": (now + timedelta(hours=1)).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )

        concept_root = root / "skill"
        (concept_root / "state" / "pages").mkdir(parents=True)
        (concept_root / "state" / "pages" / "DataAgent.md").write_text("# DataAgent\n", encoding="utf-8")
        (concept_root / "state" / "concepts-ledger.json").write_text(
            json.dumps({"DataAgent": {"status": "active"}}, ensure_ascii=False), encoding="utf-8"
        )
        coverage = root / "source-coverage-current.json"
        coverage.write_text(
            json.dumps(
                {
                    "schema": "concept-v11.source-coverage-report.v1",
                    "status": "PASS",
                    "gate": {"p3_closed": True},
                    "concepts": [
                        {
                            "concept": "DataAgent",
                            "coverage_status": "refreshable",
                            "references": [{
                                "map_id": "map-data-agent",
                                "source_uri": "viking://resources/shengsuan/source-1",
                                "source_map_status": "mapped",
                                "disposition": "mapped",
                                "evidence_set_hash": "sha256:source-1",
                            }],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        health = root / "health.json"
        health.write_text(json.dumps({"checks": {"system": {"passed": True, "checker_error": False}}}), encoding="utf-8")
        return store, concept_root, coverage, health

    def test_dry_run_is_read_only_and_rejects_unhealthy_marker(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, concept_root, coverage, health = self._setup(root)
            result = run_canary(
                store.db_path,
                concept_root=concept_root,
                coverage_path=coverage,
                health_path=health,
                namespace_epoch="v45-test",
                runtime_epoch="v45-concept-runtime-test",
                concepts=("DataAgent",),
            )
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["external_provider_calls"], 0)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0], 0)
            health.write_text(json.dumps({"checks": {"system": {"passed": False, "checker_error": False}}}), encoding="utf-8")
            blocked = run_canary(
                store.db_path,
                concept_root=concept_root,
                coverage_path=coverage,
                health_path=health,
                namespace_epoch="v45-test",
                runtime_epoch="v45-concept-runtime-test",
                concepts=("DataAgent",),
            )
            self.assertEqual(blocked["status"], "HOLD")
            self.assertTrue(any(error.startswith("health:") for error in blocked["errors"]))

    def test_apply_uses_shared_outbox_and_leaves_registry_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, concept_root, coverage, health = self._setup(root)
            source = concept_root / "state" / "pages" / "DataAgent.md"
            transport = FakeTransport()
            transport.expected_hash = "sha256:" + __import__("hashlib").sha256(source.read_bytes()).hexdigest()
            result = run_canary(
                store.db_path,
                concept_root=concept_root,
                coverage_path=coverage,
                health_path=health,
                namespace_epoch="v45-test",
                runtime_epoch="v45-concept-runtime-test",
                concepts=("DataAgent",),
                canary_id="canary-test-1",
                apply=True,
                authorization_id="auth-test-1",
                backup_root=root / "backups",
                artifact_root=root / "resource-outbox",
                observation_seconds=5,
                poll_seconds=0.01,
                transport=transport,
            )
            self.assertEqual(result["status"], "PASS")
            self.assertTrue(result["backup"]["verified"])
            self.assertTrue(result["production_registry_unchanged"])
            self.assertTrue(result["active_zero_after"])
            self.assertEqual(len(transport.uploads), 1)
            self.assertEqual(len(transport.adds), 1)
            self.assertFalse(transport.adds[0][0].get("wait"))
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM outbox_items WHERE status IN ('pending','in_flight','retry_wait')").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT admission_state,version FROM concept_admissions").fetchone()[:], ("canary", 2))
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_versions").fetchone()[0], 0)

    def test_expired_canary_is_blocked_before_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, concept_root, coverage, health = self._setup(root)
            with store.transaction() as connection:
                connection.execute("UPDATE concept_admissions SET expires_at=?", ("2000-01-01T00:00:00Z",))
            result = run_canary(
                store.db_path,
                concept_root=concept_root,
                coverage_path=coverage,
                health_path=health,
                namespace_epoch="v45-test",
                runtime_epoch="v45-concept-runtime-test",
                concepts=("DataAgent",),
                apply=True,
                authorization_id="auth-test-1",
                backup_root=root / "backups",
                transport=FakeTransport(),
            )
            self.assertEqual(result["status"], "HOLD")
            self.assertIn("admission_snapshot_expired", result["errors"])
            self.assertNotIn("backup", result)


if __name__ == "__main__":
    unittest.main()
