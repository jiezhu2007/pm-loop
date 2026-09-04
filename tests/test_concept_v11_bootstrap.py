from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from datetime import datetime, timezone

from concept_v11_bootstrap import _apply_maintenance_exemptions, validate_snapshot_inputs  # noqa: E402


class ConceptBootstrapMaintenanceExemptionTests(unittest.TestCase):
    def test_only_named_maintenance_findings_are_exempted(self):
        effective, applied, requested = _apply_maintenance_exemptions(
            [
                "check_not_pass:Codex automation 状态",
                "check_not_pass:产品情报周度比较门禁",
                "check_not_pass:运行时隔离",
            ],
            [
                "maintenance:v4-4-s10-human-removed",
                "maintenance:fde-weekly-deferred",
            ],
        )
        self.assertEqual(effective, ["check_not_pass:运行时隔离"])
        self.assertEqual(
            requested,
            [
                "maintenance:v4-4-s10-human-removed",
                "maintenance:fde-weekly-deferred",
            ],
        )
        self.assertEqual({item["id"] for item in applied}, set(requested))

    def test_unknown_or_unobserved_exemption_remains_a_blocker(self):
        effective, applied, requested = _apply_maintenance_exemptions(
            ["check_not_pass:Codex automation 状态"],
            [
                "maintenance:v4-4-s10-human-removed",
                "maintenance:fde-weekly-deferred",
                "maintenance:unknown",
            ],
        )
        self.assertEqual(applied[0]["id"], "maintenance:v4-4-s10-human-removed")
        self.assertIn("maintenance_exemption_not_observed:maintenance:fde-weekly-deferred", effective)
        self.assertIn("maintenance_exemption_unknown:maintenance:unknown", effective)
        self.assertEqual(len(requested), 3)

    def test_latest_fresh_probe_supersedes_expired_history(self):
        now = datetime(2026, 9, 3, 5, 10, tzinfo=timezone.utc)
        base = {
            "namespace_epoch": "epoch-1",
            "profile": "pm-semantic",
            "provider": "oneapi",
            "model_policy_version": "policy-1",
            "capability_state": "ready",
        }
        probes = [
            {**base, "probe_id": "old-client", "probe_type": "client_accept_probe", "observed_at": "2026-09-01T05:00:00Z", "expires_at": "2026-09-02T05:00:00Z"},
            {**base, "probe_id": "new-client", "probe_type": "client_accept_probe", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
            {**base, "probe_id": "old-semantic", "probe_type": "backend_semantic_probe", "observed_at": "2026-09-01T05:00:00Z", "expires_at": "2026-09-02T05:00:00Z"},
            {**base, "probe_id": "new-semantic", "probe_type": "backend_semantic_probe", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
        ]
        errors = validate_snapshot_inputs(
            namespace_epoch="epoch-1",
            runtime_epoch="runtime-1",
            freeze={"state": "released"},
            active={key: 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")},
            schema={"schema_version": 2, "hot_projection_composite_key": True},
            admission={"namespace_epoch": "epoch-1", "admission_state": "disabled"},
            profile={"namespace_epoch": "epoch-1", "profile": "pm-semantic", "pending_count": 0, "pending_soft_limit": 1, "outbox_hard_cap": 1, "pause_fence": "open", "policy_hash": "policy-hash"},
            policies=[{"policy_version": "policy-1", "provider": "oneapi", "requested_model": "auto", "allowed_models_json": "[]", "policy_hash": "policy-hash"}],
            probes=probes,
            resolutions=[{"policy_version": "policy-1", "provider": "oneapi", "model_requested": "auto", "resolution_status": "unknown"}],
            watermarks={
                **{name: {"state": "accepted", "value_hash": "sha256:test"} for name in ("source", "content", "knowledge")},
                "active_generation": {"state": "accepted", "value_hash": "sha256:generation", "value": {"generation_id": "generation-test", "generation_hash": "sha256:generation"}},
            },
            active_generation=[{"generation_id": "generation-test", "generation_hash": "sha256:generation", "status": "active"}],
            health={"effective_errors": []},
            reports={},
            now=now,
        )
        self.assertFalse(any(error.startswith("capability_probe_") for error in errors))

    def test_content_source_preflight_is_a_hard_gate(self):
        now = datetime(2026, 9, 3, 5, 10, tzinfo=timezone.utc)
        base = {
            "namespace_epoch": "epoch-1",
            "profile": "pm-semantic",
            "provider": "oneapi",
            "model_policy_version": "policy-1",
            "capability_state": "ready",
            "observed_at": "2026-09-03T05:05:00Z",
            "expires_at": "2026-09-04T05:05:00Z",
        }
        errors = validate_snapshot_inputs(
            namespace_epoch="epoch-1",
            runtime_epoch="runtime-1",
            freeze={"state": "released"},
            active={key: 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")},
            schema={"schema_version": 2, "hot_projection_composite_key": True},
            admission={"namespace_epoch": "epoch-1", "admission_state": "disabled"},
            profile={"namespace_epoch": "epoch-1", "profile": "pm-semantic", "pending_count": 0, "pending_soft_limit": 1, "outbox_hard_cap": 1, "pause_fence": "open", "policy_hash": "policy-hash"},
            policies=[{"policy_version": "policy-1", "provider": "oneapi", "requested_model": "auto", "allowed_models_json": "[]", "policy_hash": "policy-hash"}],
            probes=[
                {**base, "probe_id": "client", "probe_type": "client_accept_probe"},
                {**base, "probe_id": "semantic", "probe_type": "backend_semantic_probe"},
            ],
            resolutions=[{"policy_version": "policy-1", "provider": "oneapi", "model_requested": "auto", "resolution_status": "unknown"}],
            watermarks={
                **{name: {"state": "accepted", "value_hash": "sha256:test"} for name in ("source", "content", "knowledge")},
                "active_generation": {"state": "accepted", "value_hash": "sha256:generation", "value": {"generation_id": "generation-test", "generation_hash": "sha256:generation"}},
            },
            active_generation=[{"generation_id": "generation-test", "generation_hash": "sha256:generation", "status": "active"}],
            health={"effective_errors": []},
            reports={"content_source": {"errors": ["unexpected_status:HOLD"]}},
            now=now,
        )
        self.assertIn("content_source:unexpected_status:HOLD", errors)

    def test_active_generation_watermark_must_match_the_active_generation(self):
        now = datetime(2026, 9, 3, 5, 10, tzinfo=timezone.utc)
        base = {
            "namespace_epoch": "epoch-1",
            "profile": "pm-semantic",
            "provider": "oneapi",
            "model_policy_version": "policy-1",
            "capability_state": "ready",
            "observed_at": "2026-09-03T05:05:00Z",
            "expires_at": "2026-09-04T05:05:00Z",
        }
        errors = validate_snapshot_inputs(
            namespace_epoch="epoch-1",
            runtime_epoch="runtime-1",
            freeze={"state": "released"},
            active={key: 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")},
            schema={"schema_version": 2, "hot_projection_composite_key": True},
            admission={"namespace_epoch": "epoch-1", "admission_state": "disabled"},
            profile={"namespace_epoch": "epoch-1", "profile": "pm-semantic", "pending_count": 0, "pending_soft_limit": 1, "outbox_hard_cap": 1, "pause_fence": "open", "policy_hash": "policy-hash"},
            policies=[{"policy_version": "policy-1", "provider": "oneapi", "requested_model": "auto", "allowed_models_json": "[]", "policy_hash": "policy-hash"}],
            probes=[
                {**base, "probe_id": "client", "probe_type": "client_accept_probe"},
                {**base, "probe_id": "semantic", "probe_type": "backend_semantic_probe"},
            ],
            resolutions=[{"policy_version": "policy-1", "provider": "oneapi", "model_requested": "auto", "resolution_status": "unknown"}],
            watermarks={
                **{name: {"state": "accepted", "value_hash": "sha256:test"} for name in ("source", "content", "knowledge")},
                "active_generation": {"state": "missing", "value": {"generation_id": "generation-test", "generation_hash": "sha256:generation"}},
            },
            active_generation=[{"generation_id": "generation-test", "generation_hash": "sha256:generation", "status": "active"}],
            health={"effective_errors": []},
            reports={},
            now=now,
        )
        self.assertIn("active_generation_watermark_not_accepted", errors)

    def test_canary_recovery_snapshot_allows_only_explicit_rollback(self):
        now = datetime(2026, 9, 3, 5, 10, tzinfo=timezone.utc)
        common = {
            "namespace_epoch": "epoch-1",
            "runtime_epoch": "runtime-1",
            "freeze": {"state": "released"},
            "active": {key: 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")},
            "schema": {"schema_version": 2, "hot_projection_composite_key": True},
            "profile": {"namespace_epoch": "epoch-1", "profile": "pm-semantic", "pending_count": 0, "pending_soft_limit": 1, "outbox_hard_cap": 1, "pause_fence": "open", "policy_hash": "policy-hash"},
            "policies": [{"policy_version": "policy-1", "provider": "oneapi", "requested_model": "auto", "allowed_models_json": "[]", "policy_hash": "policy-hash"}],
            "probes": [
                {"probe_id": "client", "probe_type": "client_accept_probe", "namespace_epoch": "epoch-1", "profile": "pm-semantic", "provider": "oneapi", "model_policy_version": "policy-1", "capability_state": "ready", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
                {"probe_id": "semantic", "probe_type": "backend_semantic_probe", "namespace_epoch": "epoch-1", "profile": "pm-semantic", "provider": "oneapi", "model_policy_version": "policy-1", "capability_state": "ready", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
            ],
            "resolutions": [{"policy_version": "policy-1", "provider": "oneapi", "model_requested": "auto", "resolution_status": "unknown"}],
            "watermarks": {**{name: {"state": "accepted", "value_hash": "sha256:test"} for name in ("source", "content", "knowledge")}, "active_generation": {"state": "accepted", "value_hash": "sha256:generation", "value": {"generation_id": "generation-test", "generation_hash": "sha256:generation"}}},
            "active_generation": [{"generation_id": "generation-test", "generation_hash": "sha256:generation", "status": "active"}],
            "health": {"effective_errors": []},
            "reports": {},
            "now": now,
            "admission": {"namespace_epoch": "epoch-1", "admission_state": "canary"},
        }
        rollback_errors = validate_snapshot_inputs(**common, transition_target="disabled")
        self.assertFalse(any(error.startswith("concept_admission_not_safe") for error in rollback_errors))
        entry_errors = validate_snapshot_inputs(**common, transition_target="canary")
        self.assertIn("concept_admission_not_safe_for_canary", entry_errors)

    def test_incremental_snapshot_ttl_can_be_preflighted_for_one_time_continuous_migration(self):
        now = datetime(2026, 9, 3, 5, 10, tzinfo=timezone.utc)
        common = {
            "namespace_epoch": "epoch-1",
            "runtime_epoch": "runtime-1",
            "freeze": {"state": "released"},
            "active": {key: 0 for key in ("jobs", "runs", "outbox_items", "semantic_tasks", "slots", "tokens", "migration_leases", "dispatch_leases", "probe_leases")},
            "schema": {"schema_version": 2, "hot_projection_composite_key": True},
            "admission": {"namespace_epoch": "epoch-1", "admission_state": "incremental"},
            "profile": {"namespace_epoch": "epoch-1", "profile": "pm-semantic", "pending_count": 0, "pending_soft_limit": 1, "outbox_hard_cap": 1, "pause_fence": "open", "policy_hash": "policy-hash"},
            "policies": [{"policy_version": "policy-1", "provider": "oneapi", "requested_model": "auto", "allowed_models_json": "[]", "policy_hash": "policy-hash"}],
            "probes": [
                {"probe_id": "client", "probe_type": "client_accept_probe", "namespace_epoch": "epoch-1", "profile": "pm-semantic", "provider": "oneapi", "model_policy_version": "policy-1", "capability_state": "ready", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
                {"probe_id": "semantic", "probe_type": "backend_semantic_probe", "namespace_epoch": "epoch-1", "profile": "pm-semantic", "provider": "oneapi", "model_policy_version": "policy-1", "capability_state": "ready", "observed_at": "2026-09-03T05:05:00Z", "expires_at": "2026-09-04T05:05:00Z"},
            ],
            "resolutions": [{"policy_version": "policy-1", "provider": "oneapi", "model_requested": "auto", "resolution_status": "unknown"}],
            "watermarks": {**{name: {"state": "accepted", "value_hash": "sha256:test"} for name in ("source", "content", "knowledge")}, "active_generation": {"state": "accepted", "value_hash": "sha256:generation", "value": {"generation_id": "generation-test", "generation_hash": "sha256:generation"}}},
            "active_generation": [{"generation_id": "generation-test", "generation_hash": "sha256:generation", "status": "active"}],
            "health": {"effective_errors": []},
            "reports": {},
            "now": now,
        }
        errors = validate_snapshot_inputs(**common, transition_target="incremental")
        self.assertFalse(any(error.startswith("concept_admission_not_safe") for error in errors))


if __name__ == "__main__":
    unittest.main()
