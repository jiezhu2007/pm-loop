from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore, SCHEMA_VERSION  # noqa: E402
from retention_observer import build_observation, sign_plan, write_observation  # noqa: E402
from retention_read_model import RetentionReadModel, _advice, _sum_unknown_logical_bytes  # noqa: E402
from retention_reclaimer import PlanValidationError, run_reclaimer, verify_plan_item_descriptor  # noqa: E402
from retention_registry import (  # noqa: E402
    RetentionConfigError,
    canonical_json,
    load_bundle,
    trusted_roots,
)


class RetentionV4Tests(unittest.TestCase):
    signing_key = b"plan-signing-key-for-tests-000000000000"
    capability_key = b"capability-signing-key-tests-00000000"
    observed_at = datetime(2026, 9, 3, 4, 0, tzinfo=timezone.utc)
    reclaim_at = datetime(2026, 9, 3, 6, 0, tzinfo=timezone.utc)

    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _paths(self, root: Path, *, kill_switch: bool = False) -> dict[str, Path]:
        project = root / "project"
        home = root / "home"
        source = project / "state" / "cache"
        source.mkdir(parents=True)
        home.mkdir()
        target = source / "daily-20260701.log"
        target.write_bytes(b"retention-fixture\n")
        registry = {
            "schema_version": "pm-loop.retention-source-registry.v1",
            "registry_version": 1,
            "default_mode": "observe_only",
            "sources": [{
                "source_id": "fixture-cache",
                "display_name": "测试缓存",
                "owner": "tests",
                "root_ref": {"root_id": "project", "relative_path": "state/cache"},
                "adapter": "filesystem_tree",
                "object_contract": "operational-log.v1",
                "declared_authority": "rebuildable_cache",
                "discovery": {"max_depth": 2, "include_names": ["*"], "exclude_names": [".DS_Store"]},
                "reference_providers": ["log-retention-index"],
                "freshness_sla_hours": 8760,
                "mode": "observe_only",
            }],
        }
        policy = {
            "schema_version": "pm-loop.retention-policy.v3",
            "policy_version": 3,
            "default_class": "R5",
            "default_action": "hold",
            "rules": [{
                "rule_id": "expire-fixture-cache",
                "match": {"object_contract": ["operational-log.v1"], "authority": ["rebuildable_cache"]},
                "class": "R2",
                "action": "expire",
                "hot_days": 0,
                "quarantine_days": 7,
                "mode": "observe_only",
            }],
        }
        capability = {
            "capability_id": "cap-fixture-cache-v1",
            "source_id": "fixture-cache",
            "root_id": "project",
            "relative_subtree": "state/cache",
            "object_contract": "operational-log.v1",
            "action_profile": "expire-file-v1",
            "rollout_phase": "canary",
            "max_objects_per_batch": 1,
            "max_bytes_per_day": 1024 * 1024,
            "valid_from": "2026-09-02T00:00:00Z",
            "expires_at": "2026-09-05T00:00:00Z",
            "approving_adr": "ADR-999",
            "signature": "",
        }
        unsigned = dict(capability)
        unsigned.pop("signature")
        signature = hmac.new(self.capability_key, canonical_json(unsigned).encode("utf-8"), hashlib.sha256).digest()
        capability["signature"] = "base64:" + base64.b64encode(signature).decode("ascii")
        capabilities = {
            "schema_version": "pm-loop.retention-deletion-capabilities.v1",
            "capability_version": 1,
            "kill_switch": kill_switch,
            "default_action": "deny",
            "capabilities": [capability],
        }
        paths = {
            "project": project,
            "home": home,
            "target": target,
            "registry": root / "retention-source-registry.json",
            "policy": root / "retention-policy.v3.json",
            "capabilities": root / "retention-deletion-capabilities.json",
            "state": root / "retention-state",
            "db": root / "pm-system.db",
        }
        self._write_json(paths["registry"], registry)
        self._write_json(paths["policy"], policy)
        self._write_json(paths["capabilities"], capabilities)
        return paths

    def _observation(self, paths: dict[str, Path], *, run_id: str = "observer-fixture") -> tuple[dict, dict]:
        bundle = load_bundle(paths["registry"], paths["policy"], paths["capabilities"])
        roots = trusted_roots(project_root=paths["project"], home=paths["home"])
        value = build_observation(
            bundle=bundle,
            roots=roots,
            run_id=run_id,
            occurrence_id="retention-observer:fixture",
            signing_key=self.signing_key,
            capability_key=self.capability_key,
            schedule_registry_hash="manual",
            observed_at=self.observed_at,
        )
        item = dict(value["inventory"]["items"][0])
        identity = item["inode_identity"]
        item["due_at"] = (self.reclaim_at - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        value["plan"]["items"] = [{
            "object_id": item["object_id"],
            "source_id": item["source_id"],
            "root_id": item["root_id"],
            "relative_path": item["relative_path"],
            "object_contract": item["object_contract"],
            "action_profile": "expire-file-v1",
            "st_dev": identity["st_dev"],
            "st_ino": identity["st_ino"],
            "file_type": identity["file_type"],
            "size": identity["size"],
            "mtime_ns": identity["mtime_ns"],
            "nlink": identity["nlink"],
            "content_hash": item["content_hash"],
            "expected_reclaim_bytes": item["allocated_bytes"],
            "due_at": item["due_at"],
            "gate_results": {
                "capability": "cap-fixture-cache-v1",
                "rollout_phase": "canary",
                "max_objects_per_batch": 1,
                "max_bytes_per_day": 1024 * 1024,
            },
        }]
        value["plan"]["signature"] = sign_plan(value["plan"], self.signing_key)
        result = write_observation(paths["state"], value, db_path=paths["db"])
        return value, result

    def test_registry_rejects_code_path_escape_and_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            document = json.loads(paths["registry"].read_text(encoding="utf-8"))
            document["sources"][0]["command"] = "rm -rf ignored"
            self._write_json(paths["registry"], document)
            with self.assertRaisesRegex(RetentionConfigError, "forbidden executable field"):
                load_bundle(paths["registry"], paths["policy"], paths["capabilities"])

            paths = self._paths(Path(temp) / "escape")
            document = json.loads(paths["registry"].read_text(encoding="utf-8"))
            document["sources"][0]["root_ref"]["relative_path"] = "../outside"
            self._write_json(paths["registry"], document)
            with self.assertRaisesRegex(RetentionConfigError, "relative_path"):
                load_bundle(paths["registry"], paths["policy"], paths["capabilities"])

            paths = self._paths(Path(temp) / "overlap")
            document = json.loads(paths["registry"].read_text(encoding="utf-8"))
            duplicate = dict(document["sources"][0])
            duplicate["source_id"] = "fixture-cache-child"
            duplicate["root_ref"] = {"root_id": "project", "relative_path": "state/cache/child"}
            document["sources"].append(duplicate)
            self._write_json(paths["registry"], document)
            with self.assertRaisesRegex(RetentionConfigError, "overlapping source subtrees"):
                load_bundle(paths["registry"], paths["policy"], paths["capabilities"])

    def test_explicitly_ignored_child_source_has_single_log_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project, home = root / "project", root / "home"
            log = project / "state" / "timeline" / "logs" / "daily-20260701.log"
            log.parent.mkdir(parents=True)
            log.write_text("finished\n", encoding="utf-8")
            migration_evidence = log.parent / "crontab.bak.20260815-011809.txt"
            migration_evidence.write_text("legacy scheduler evidence\n", encoding="utf-8")
            unsupported_log = log.parent / "cron.log"
            unsupported_log.write_text("legacy writer\n", encoding="utf-8")
            old_ns = int((self.observed_at - timedelta(days=31)).timestamp() * 1_000_000_000)
            os.utime(log, ns=(old_ns, old_ns))
            (project / "state" / "timeline" / ".daily.lock").write_text("", encoding="utf-8")
            registry = {
                "schema_version": "pm-loop.retention-source-registry.v1", "registry_version": 1, "default_mode": "observe_only",
                "sources": [
                    {
                        "source_id": "timeline-state", "display_name": "时间轴", "owner": "tests",
                        "root_ref": {"root_id": "project", "relative_path": "state/timeline"},
                        "adapter": "json_ledger", "object_contract": "pm-timeline.v1", "declared_authority": "decision_record",
                        "discovery": {"max_depth": 3, "include_names": ["*.json"], "exclude_names": [".DS_Store"], "ignore_relative_paths": ["logs/**", ".daily.lock"]},
                        "reference_providers": ["timeline-index"], "freshness_sla_hours": 8760, "mode": "observe_only",
                    },
                    {
                        "source_id": "timeline-logs", "display_name": "运行日志", "owner": "tests",
                        "root_ref": {"root_id": "project", "relative_path": "state/timeline/logs"},
                        "adapter": "filesystem_tree", "object_contract": "operational-log.v1", "declared_authority": "rebuildable_cache",
                        "discovery": {"max_depth": 1, "include_names": ["*.log"], "exclude_names": [".DS_Store"], "ignore_relative_paths": ["crontab.bak.20260815-011809.txt"]},
                        "reference_providers": ["log-retention-index"], "freshness_sla_hours": 8760, "mode": "observe_only",
                    },
                    {
                        "source_id": "timeline-migration-evidence", "display_name": "迁移留证", "owner": "tests",
                        "root_ref": {"root_id": "project", "relative_path": "state/timeline/logs/crontab.bak.20260815-011809.txt"},
                        "adapter": "filesystem_tree", "object_contract": "pm-run-artifact.v1", "declared_authority": "audit_evidence",
                        "discovery": {"max_depth": 1, "include_names": ["crontab.bak.20260815-011809.txt"], "exclude_names": [".DS_Store"]},
                        "reference_providers": ["scheduler-run-index"], "freshness_sla_hours": 8760, "mode": "observe_only",
                    },
                ],
            }
            policy = {
                "schema_version": "pm-loop.retention-policy.v3", "policy_version": 3, "default_class": "R5", "default_action": "hold",
                "rules": [
                    {"rule_id": "logs", "match": {"object_contract": ["operational-log.v1"]}, "class": "R2", "action": "expire", "hot_days": 30, "mode": "observe_only"},
                    {"rule_id": "records", "match": {"authority": ["decision_record"]}, "class": "R0", "action": "protect", "mode": "observe_only"},
                    {"rule_id": "audit", "match": {"authority": ["audit_evidence"]}, "class": "R4", "action": "hold", "mode": "observe_only"},
                ],
            }
            capabilities = {"schema_version": "pm-loop.retention-deletion-capabilities.v1", "capability_version": 1, "kill_switch": True, "default_action": "deny", "capabilities": []}
            registry_path, policy_path, capabilities_path = root / "registry.json", root / "policy.json", root / "capabilities.json"
            self._write_json(registry_path, registry)
            self._write_json(policy_path, policy)
            self._write_json(capabilities_path, capabilities)
            value = build_observation(
                bundle=load_bundle(registry_path, policy_path, capabilities_path), roots=trusted_roots(project_root=project, home=home),
                run_id="logs", occurrence_id="manual:logs", signing_key=self.signing_key, observed_at=self.observed_at,
            )
            items = value["inventory"]["items"]
            self.assertEqual(len(items), 3)
            by_source = {item["source_id"]: item for item in items}
            daily_item = next(item for item in items if item["relative_path"].endswith("daily-20260701.log"))
            unsupported_item = next(item for item in items if item["relative_path"].endswith("cron.log"))
            self.assertEqual(daily_item["processability"], "eligible")
            self.assertEqual(unsupported_item["processability"], "protected")
            self.assertEqual(unsupported_item["retention_class"], "R4")
            self.assertEqual(unsupported_item["reason_codes"], ["unsupported_log_terminally_protected"])
            self.assertEqual(by_source["timeline-migration-evidence"]["processability"], "protected")
            self.assertEqual(by_source["timeline-migration-evidence"]["retention_class"], "R4")
            self.assertFalse(any(item["reason_code"] == "excluded_object" for item in value["unknowns"]["items"]))
            parent = next(item for item in value["inventory"]["sources"] if item["source_id"] == "timeline-state")
            self.assertEqual(parent["status"], "healthy")
            self.assertTrue(parent["inventory_complete"])
            self.assertEqual(parent["ignored_system_noise_count"], 4)

    def test_runtime_snapshot_index_requires_committed_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project, home = root / "project", root / "home"
            runtime = home / ".codex" / "pm-loop" / "runtime"
            runtime.mkdir(parents=True)
            backups = home / ".codex" / "pm-loop" / "scheduler-migration" / "runtime-backups"
            required = ("config/schedule-registry.json", "scripts/pm_loop_scheduler.py", "scripts/pm_scheduled_handlers.py", "scripts/retention_observer.py")
            for index in range(1, 5):
                snapshot = backups / f"2026090{index}T000000Z"
                entries = []
                for relative in required:
                    path = snapshot / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(f"snapshot {index} {relative}\n", encoding="utf-8")
                    entries.append({"relative_path": relative, "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size})
                self._write_json(snapshot / "snapshot-manifest.json", {
                    "schema_version": "pm-loop.runtime-backup-manifest.v1", "snapshot_id": snapshot.name,
                    "status": "completed", "completed_at": f"2026-09-0{index}T00:00:00Z", "files": entries,
                })
            legacy = backups / "legacy-without-manifest"
            (legacy / "scripts").mkdir(parents=True)
            (legacy / "scripts" / "old.py").write_text("legacy\n", encoding="utf-8")
            registry = {
                "schema_version": "pm-loop.retention-source-registry.v1", "registry_version": 1, "default_mode": "observe_only",
                "sources": [{
                    "source_id": "scheduler-runtime-backups", "display_name": "Runtime", "owner": "tests",
                    "root_ref": {"root_id": "pm_loop", "relative_path": "scheduler-migration/runtime-backups"},
                    "adapter": "filesystem_tree", "object_contract": "rebuildable-runtime-backup.v1", "declared_authority": "rebuildable_cache",
                    "discovery": {"max_depth": 3, "include_names": ["*"], "exclude_names": [".DS_Store"]},
                    "reference_providers": ["runtime-snapshot-index"], "freshness_sla_hours": 8760, "mode": "observe_only",
                }],
            }
            policy = {"schema_version": "pm-loop.retention-policy.v3", "policy_version": 3, "default_class": "R5", "default_action": "hold", "rules": [{"rule_id": "backups", "match": {"object_contract": ["rebuildable-runtime-backup.v1"]}, "class": "R2", "action": "expire", "hot_days": 7, "mode": "observe_only"}]}
            capabilities = {"schema_version": "pm-loop.retention-deletion-capabilities.v1", "capability_version": 1, "kill_switch": True, "default_action": "deny", "capabilities": []}
            registry_path, policy_path, capabilities_path = root / "registry.json", root / "policy.json", root / "capabilities.json"
            self._write_json(registry_path, registry)
            self._write_json(policy_path, policy)
            self._write_json(capabilities_path, capabilities)
            value = build_observation(
                bundle=load_bundle(registry_path, policy_path, capabilities_path), roots=trusted_roots(project_root=project, home=home),
                run_id="snapshots", occurrence_id="manual:snapshots", signing_key=self.signing_key, observed_at=self.observed_at,
            )
            items = value["inventory"]["items"]
            retained = [item for item in items if item.get("snapshot_state") == "retained_complete"]
            retired = [item for item in items if item.get("snapshot_state") == "retired_complete"]
            legacy_items = [item for item in items if item.get("snapshot_state") == "manifest_missing"]
            self.assertEqual(len(retained), 15)
            self.assertEqual(len(retired), 5)
            self.assertEqual(len(legacy_items), 1)
            self.assertFalse(any("reference_graph_incomplete" in item["reason_codes"] for item in items))
            self.assertEqual(value["unknowns"]["items"], [])
            self.assertTrue(all(item["processability"] == "protected" for item in legacy_items))
            self.assertTrue(all(item["retention_class"] == "R4" for item in legacy_items))
            self.assertTrue(all(item["reason_codes"] == ["legacy_snapshot_protected"] for item in legacy_items))

    def test_runtime_snapshot_physical_action_verifies_group_and_post_check(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project, home = root / "project", root / "home"
            runtime = home / ".codex" / "pm-loop" / "runtime"
            runtime.mkdir(parents=True)
            backups = home / ".codex" / "pm-loop" / "scheduler-migration" / "runtime-backups"
            required = (
                "config/schedule-registry.json",
                "scripts/pm_loop_scheduler.py",
                "scripts/pm_scheduled_handlers.py",
                "scripts/retention_observer.py",
            )
            snapshots = []
            for index in range(1, 5):
                snapshot = backups / f"2026090{index}T000000Z"
                entries = []
                for relative in required:
                    path = snapshot / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    content = "{}\n" if path.suffix == ".json" else f"SNAPSHOT = {index}\n"
                    path.write_text(content, encoding="utf-8")
                    entries.append({
                        "relative_path": relative,
                        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
                        "bytes": path.stat().st_size,
                    })
                self._write_json(snapshot / "snapshot-manifest.json", {
                    "schema_version": "pm-loop.runtime-backup-manifest.v1",
                    "snapshot_id": snapshot.name,
                    "status": "completed",
                    "completed_at": f"2026-09-0{index}T00:00:00Z",
                    "files": entries,
                })
                snapshots.append(snapshot)
            registry = {
                "schema_version": "pm-loop.retention-source-registry.v1",
                "registry_version": 1,
                "default_mode": "observe_only",
                "sources": [{
                    "source_id": "scheduler-runtime-backups", "display_name": "Runtime", "owner": "tests",
                    "root_ref": {"root_id": "pm_loop", "relative_path": "scheduler-migration/runtime-backups"},
                    "adapter": "filesystem_tree", "object_contract": "rebuildable-runtime-backup.v1",
                    "declared_authority": "rebuildable_cache",
                    "discovery": {"max_depth": 3, "include_names": ["*"], "exclude_names": [".DS_Store"]},
                    "reference_providers": ["runtime-snapshot-index"], "freshness_sla_hours": 8760,
                    "mode": "observe_only",
                }],
            }
            policy = {
                "schema_version": "pm-loop.retention-policy.v3", "policy_version": 3,
                "default_class": "R5", "default_action": "hold",
                "rules": [{
                    "rule_id": "backups", "match": {"object_contract": ["rebuildable-runtime-backup.v1"]},
                    "class": "R2", "action": "expire", "hot_days": 7, "mode": "observe_only",
                }],
            }
            capability = {
                "capability_id": "cap-runtime-test-v1", "source_id": "scheduler-runtime-backups",
                "root_id": "pm_loop", "relative_subtree": "scheduler-migration/runtime-backups",
                "object_contract": "rebuildable-runtime-backup.v1", "action_profile": "expire-runtime-snapshot-v1",
                "rollout_phase": "canary", "max_objects_per_batch": 1, "max_bytes_per_day": 1024 * 1024,
                "valid_from": "2026-09-01T00:00:00Z", "expires_at": "2026-10-01T00:00:00Z",
                "approving_adr": "ADR-999", "signature": "",
            }
            unsigned = dict(capability)
            unsigned.pop("signature")
            capability["signature"] = "base64:" + base64.b64encode(
                hmac.new(self.capability_key, canonical_json(unsigned).encode("utf-8"), hashlib.sha256).digest()
            ).decode("ascii")
            capabilities = {
                "schema_version": "pm-loop.retention-deletion-capabilities.v1", "capability_version": 1,
                "kill_switch": False, "default_action": "deny", "capabilities": [capability],
            }
            registry_path, policy_path, capabilities_path = root / "registry.json", root / "policy.json", root / "capabilities.json"
            self._write_json(registry_path, registry)
            self._write_json(policy_path, policy)
            self._write_json(capabilities_path, capabilities)
            observed_at = datetime(2026, 9, 20, 5, 0, tzinfo=timezone.utc)
            value = build_observation(
                bundle=load_bundle(registry_path, policy_path, capabilities_path),
                roots=trusted_roots(project_root=project, home=home),
                run_id="runtime-physical", occurrence_id="manual:runtime-physical",
                signing_key=self.signing_key, capability_key=self.capability_key,
                schedule_registry_hash="manual", observed_at=observed_at,
            )
            self.assertEqual(len(value["plan"]["items"]), 1)
            self.assertEqual(value["plan"]["items"][0]["snapshot_id"], snapshots[0].name)
            state = root / "retention-state"
            db = root / "pm-system.db"
            write_observation(state, value, db_path=db)
            result = run_reclaimer(
                state_root=state, registry_path=registry_path, policy_path=policy_path,
                capabilities_path=capabilities_path, project_root=project, home=home,
                run_id="runtime-physical-apply", dry_run=False, signing_key=self.signing_key,
                capability_key=self.capability_key, current=observed_at + timedelta(hours=1), db_path=db,
            )
            self.assertEqual(result["status"], "applied_verified", msg=result)
            store = PMSystemStore(db, auto_migrate=False)
            action = store.list_retention_actions()[0]
            with store.connect() as connection:
                event = connection.execute(
                    "SELECT payload_json FROM retention_action_events WHERE action_id=? AND state='verified'",
                    (action["action_id"],),
                ).fetchone()
            action_payload = json.loads(event[0])
            self.assertEqual(action_payload["restore_smoke"]["status"], "passed")
            self.assertEqual(action["state"], "verified")
            self.assertFalse(snapshots[0].exists())
            self.assertTrue(all(path.is_dir() for path in snapshots[1:]))

    def test_capability_scope_signature_and_canary_quota_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            document = json.loads(paths["capabilities"].read_text(encoding="utf-8"))
            document["capabilities"][0]["relative_subtree"] = "state"
            self._write_json(paths["capabilities"], document)
            with self.assertRaisesRegex(RetentionConfigError, "expands beyond"):
                load_bundle(paths["registry"], paths["policy"], paths["capabilities"])

            paths = self._paths(Path(temp) / "quota")
            document = json.loads(paths["capabilities"].read_text(encoding="utf-8"))
            document["capabilities"][0]["max_objects_per_batch"] = 2
            self._write_json(paths["capabilities"], document)
            with self.assertRaisesRegex(RetentionConfigError, "canary capability"):
                load_bundle(paths["registry"], paths["policy"], paths["capabilities"])

    def test_dry_run_claims_nonce_uses_fencing_and_cannot_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            first = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-first", dry_run=True, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at, db_path=paths["db"],
            )
            self.assertEqual(first["status"], "dry_run_verified")
            self.assertTrue(paths["target"].is_file())
            self.assertGreater(first["claim"]["fencing_token"], 0)
            actions = PMSystemStore(paths["db"], auto_migrate=False).list_retention_actions()
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["state"], "verified")
            second = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-replay", dry_run=True, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at + timedelta(minutes=5), db_path=paths["db"],
            )
            self.assertEqual(second["status"], "held")
            self.assertEqual(second["reason_code"], "claim_failed")
            self.assertIn("nonce", second["message"])

    def test_expire_file_physical_action_removes_sealed_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-apply-log", dry_run=False, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at, db_path=paths["db"],
            )
            self.assertEqual(result["status"], "applied_verified")
            self.assertFalse(paths["target"].exists())
            self.assertEqual(result["actions"][0]["status"], "verified")
            self.assertGreater(result["reclaimed_logical_bytes"], 0)

    def test_descriptor_verification_rejects_hardlink_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            value, _ = self._observation(paths)
            os.link(paths["target"], paths["target"].with_name("second-link.bin"))
            roots = trusted_roots(project_root=paths["project"], home=paths["home"])
            with self.assertRaisesRegex(PlanValidationError, "identity changed"):
                verify_plan_item_descriptor(value["plan"]["items"][0], roots)

    def test_physical_preflight_failure_is_terminal_not_prepared(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            paths["target"].write_text("changed after observer\n", encoding="utf-8")
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-preflight-failure", dry_run=False, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at, db_path=paths["db"],
            )
            self.assertEqual(result["reason_code"], "physical_reclaim_failed")
            actions = PMSystemStore(paths["db"], auto_migrate=False).list_retention_actions()
            self.assertEqual(actions[0]["state"], "held")
            self.assertEqual(PMSystemStore(paths["db"], auto_migrate=False).retention_reconciliation_queue(), [])

    def test_config_drift_and_kill_switch_do_not_consume_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            policy = json.loads(paths["policy"].read_text(encoding="utf-8"))
            policy["rules"][0]["hot_days"] = 1
            self._write_json(paths["policy"], policy)
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-drift", dry_run=True, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at, db_path=paths["db"],
            )
            self.assertEqual(result["reason_code"], "plan_validation_failed")
            self.assertTrue(paths["target"].is_file())

        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp), kill_switch=True)
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-disabled", dry_run=False, current=self.reclaim_at,
            )
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["reason_code"], "disabled")
            self.assertTrue(paths["target"].is_file())

    def test_empty_plan_can_close_formally_outside_action_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            value = build_observation(
                bundle=load_bundle(paths["registry"], paths["policy"], paths["capabilities"]),
                roots=trusted_roots(project_root=paths["project"], home=paths["home"]),
                run_id="observer-empty-after-hours", occurrence_id="manual:observer-empty-after-hours",
                signing_key=self.signing_key, capability_key=self.capability_key,
                schedule_registry_hash="manual", observed_at=self.observed_at,
            )
            self.assertEqual(value["plan"]["items"], [])
            write_observation(paths["state"], value, db_path=paths["db"])
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-empty-after-hours", dry_run=False, signing_key=self.signing_key,
                capability_key=self.capability_key, current=datetime(2026, 9, 3, 9, 0, tzinfo=timezone.utc),
                db_path=paths["db"],
            )
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["reason_code"], "no_due_items")
            self.assertIs(result["dry_run"], False)

    def test_due_plan_remains_deferred_outside_action_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            result = run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-due-after-hours", dry_run=False, signing_key=self.signing_key,
                capability_key=self.capability_key, current=datetime(2026, 9, 3, 9, 0, tzinfo=timezone.utc),
                db_path=paths["db"],
            )
            self.assertEqual(result["status"], "deferred")
            self.assertEqual(result["reason_code"], "outside_business_window")
            self.assertTrue(paths["target"].is_file())

    def test_read_model_replays_ledger_paginates_and_keeps_advice_private(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            paths = self._paths(Path(temp))
            self._observation(paths)
            run_reclaimer(
                state_root=paths["state"], registry_path=paths["registry"], policy_path=paths["policy"],
                capabilities_path=paths["capabilities"], project_root=paths["project"], home=paths["home"],
                run_id="reclaimer-read-model", dry_run=True, signing_key=self.signing_key,
                capability_key=self.capability_key, current=self.reclaim_at, db_path=paths["db"],
            )
            model = RetentionReadModel(state_root=paths["state"], registry_path=paths["registry"], db_path=paths["db"])
            snapshot = model.snapshot()
            self.assertEqual(snapshot["summary"]["reclaimed_90d_bytes"]["coverage"], "retention_action_ledger")
            page = model.resource("actions", {"page": ["1"], "page_size": ["1"], "status": ["verified"]})
            self.assertEqual(page["pagination"]["total"], 1)
            self.assertEqual(len(page["actions"]), 1)
            advice = _advice({
                "unknown_id": "unk-1", "object_id": "obj-1", "source_id": "fixture-cache",
                "reason_code": "unregistered_source", "logical_bytes": 12,
                "growth_7d_bytes": None, "object_count": 1,
                "evidence_handles": ["retention://unknown/unk-1", "/Users/private/secret.txt"],
            })["prompt"]
            self.assertIn("retention://unknown/unk-1", advice)
            self.assertNotIn("/Users/", advice)
            self.assertNotIn("secret.txt", advice)

    def test_read_model_unknown_capacity_sums_disjoint_unregistered_roots(self) -> None:
        items = [
            {"source_id": "unregistered", "unknown_id": "unk-a", "logical_bytes": 11},
            {"source_id": "unregistered", "unknown_id": "unk-b", "logical_bytes": 23},
            {"source_id": "registered", "unknown_id": "unk-c", "logical_bytes": 7},
        ]
        self.assertEqual(_sum_unknown_logical_bytes(items), 41)

    def test_pm_store_schema_contains_retention_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm-system.db")
            self.assertEqual(store.schema_version(), SCHEMA_VERSION)
            with store.connect() as connection:
                tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for name in ("retention_sources", "retention_inventory", "retention_unknowns", "retention_plans", "retention_actions", "retention_nonce_consumptions", "retention_leases"):
                self.assertIn(name, tables)


if __name__ == "__main__":
    unittest.main()
