from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
MODULE_PATH = ROOT / "scripts/pm_system_s10_final_gate.py"
spec = importlib.util.spec_from_file_location("pm_system_s10_final_gate", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S10FinalGateTests(unittest.TestCase):
    def _ready(self) -> dict:
        return {
            "control_plane": [{"pid": 1}],
            "worker": [{"pid": 2}],
            "memory_sync": [{"pid": 3}],
            "forbidden": [],
        }

    def _observations_pass(self) -> dict:
        return {
            "sample_count": 2,
            "distinct_local_dates": ["2026-08-31", "2026-09-01"],
            "pass_dates": ["2026-08-31", "2026-09-01"],
            "selected_dates": ["2026-08-31", "2026-09-01"],
            "stability": {"checked": True, "pass": True},
            "pass": True,
        }

    def _observations_one_workday(self) -> dict:
        return {
            "sample_count": 7,
            "distinct_local_dates": ["2026-08-29"],
            "pass_dates": ["2026-08-29"],
            "selected_dates": ["2026-08-29"],
            "invalid": [],
            "rejected": [],
            "stability": {"checked": False, "pass": False},
            "pass": False,
        }

    def _automations_ready(self) -> dict:
        return {
            "automation": {"status": "ACTIVE"},
            "databuilder": {"status": "ACTIVE"},
            "v4-4-s10": {"status": "ACTIVE"},
        }

    def _cockpit_ready(self) -> dict:
        return {
            "available": True,
            "status": "healthy",
            "health_truthful": True,
            "evidence_complete": True,
            "unknown_key_modules": [],
            "missing_watermarks": [],
            "dead_letter": 0,
        }

    def test_all_restored_components_pass(self) -> None:
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "integrity_check": "ok", "tables": {"error_events": {"total": 0}}, "active": [], "slots": {"free": 2}, "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module,
            "automation_statuses",
            return_value=self._automations_ready(),
        ), patch.object(
            module, "observation_samples", return_value=self._observations_pass()
        ), patch.object(module, "cockpit_snapshot", return_value=self._cockpit_ready()):
            result = module.audit()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["errors"], [])

    def test_active_queue_or_writer_blocks_unfreeze(self) -> None:
        processes = self._ready()
        processes["forbidden"] = [{"pid": 9, "command": "weekly-sync-and-refresh.sh"}]
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=processes
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "integrity_check": "ok", "tables": {"error_events": {"total": 0}}, "active": [{"table": "jobs", "status": "running", "count": 1}], "slots": {"leased": 1}, "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module,
            "automation_statuses",
            return_value=self._automations_ready(),
        ), patch.object(
            module, "observation_samples", return_value=self._observations_pass()
        ):
            result = module.audit()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("active queue state remains", result["errors"])
        self.assertIn("business writer process observed", result["errors"])

    def test_missing_observation_automation_blocks_final_gate(self) -> None:
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "integrity_check": "ok", "tables": {"error_events": {"total": 0}}, "active": [], "slots": {"free": 2}, "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module,
            "automation_statuses",
            return_value={"automation": {"status": "ACTIVE"}, "databuilder": {"status": "ACTIVE"}},
        ), patch.object(
            module, "observation_samples", return_value=self._observations_pass()
        ):
            result = module.audit()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("required Codex Automation is missing: v4-4-s10", result["errors"])

    def test_same_day_samples_do_not_satisfy_two_workday_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            sample = {
                "phase_id": "S10-observation",
                "observed_at": "2026-08-29T01:00:00Z",
                "status": "PASS",
                "read_only": True,
                "production_state_touched": False,
                "external_provider_calls": 0,
                "hard_issues": [],
                "pending": [],
                "timeline": {"all_normal": True},
                "database": {"active": [], "slots": {"leased": 0}, "performance": {"queue_wait": {"p95_s": None}, "outbox": {"oldest_age_s": 0}, "retry_amplification": 0}},
                "processes": {"rss": {"total_mb": 20}},
                "health": {"total": 11, "passed": 11},
            }
            (report_dir / "20260829-S10-observation-sample-99-manifest.json").write_text(json.dumps(sample), encoding="utf-8")
            sample["observed_at"] = "2026-08-29T02:00:00Z"
            (report_dir / "20260829-S10-observation-sample-100-manifest.json").write_text(json.dumps(sample), encoding="utf-8")
            with patch.object(module, "REPORT_DIR", report_dir):
                result = module.observation_samples()
        self.assertFalse(result["pass"])
        self.assertEqual(result["selected_dates"], ["2026-08-29"])
        self.assertIn("need two distinct local workdays", " ".join(result["errors"]))

    def test_manual_workday_waiver_only_changes_date_gate(self) -> None:
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "integrity_check": "ok", "tables": {"error_events": {"total": 0}}, "active": [], "slots": {"free": 2}, "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module, "automation_statuses", return_value=self._automations_ready()
        ), patch.object(
            module, "observation_samples", return_value=self._observations_one_workday()
        ), patch.object(module, "cockpit_snapshot", return_value=self._cockpit_ready()):
            result = module.audit({
                "id": "v44-s10-workday-waiver-20260829-01",
                "waived_by": "用户明确授权",
                "reason": "人工豁免 S10 两个不同工作日样本门禁",
            })
        self.assertEqual(result["status"], "PASS_WITH_WAIVER")
        self.assertTrue(result["effective_pass"])
        self.assertEqual(result["errors"], [])
        self.assertTrue(result["waiver"]["applied"])

    def test_workday_waiver_does_not_bypass_rejected_samples(self) -> None:
        observations = self._observations_one_workday()
        observations["rejected"] = ["2026-08-29: health check incomplete"]
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "tables": {}, "active": [], "slots": {"free": 2}},
        ), patch.object(
            module, "automation_statuses", return_value=self._automations_ready()
        ), patch.object(
            module, "observation_samples", return_value=observations
        ):
            result = module.audit({"id": "waiver", "waived_by": "user", "reason": "date only"})
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertFalse(result["waiver"]["applied"])
        self.assertIn(module.WORKDAY_GATE_ERROR, result["errors"])

    def test_two_distinct_workdays_with_stable_metrics_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            base = {
                "phase_id": "S10-observation",
                "status": "PASS",
                "read_only": True,
                "production_state_touched": False,
                "external_provider_calls": 0,
                "hard_issues": [],
                "pending": [],
                "timeline": {"all_normal": True},
                "database": {"active": [], "slots": {"leased": 0}, "performance": {"queue_wait": {"p95_s": 0}, "outbox": {"oldest_age_s": 0}, "retry_amplification": 0}},
                "processes": {"rss": {"total_mb": 100}},
                "health": {"total": 11, "passed": 11},
            }
            for index, observed_at in enumerate(("2026-08-31T01:00:00Z", "2026-09-01T01:00:00Z"), start=1):
                sample = dict(base, observed_at=observed_at)
                (report_dir / f"202608{30 + index}-S10-observation-sample-01-manifest.json").write_text(json.dumps(sample), encoding="utf-8")
            with patch.object(module, "REPORT_DIR", report_dir):
                result = module.observation_samples()
        self.assertTrue(result["pass"])
        self.assertEqual(result["selected_dates"], ["2026-08-31", "2026-09-01"])
        self.assertTrue(result["stability"]["pass"])

    def test_false_healthy_cockpit_blocks_final_gate(self) -> None:
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "tables": {"error_events": {"total": 0}}, "active": [], "slots": {"free": 2}, "integrity_check": "ok", "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module, "cockpit_snapshot", return_value={
                "available": True,
                "status": "healthy",
                "health_truthful": False,
                "unknown_key_modules": ["Worker"],
                "missing_watermarks": ["knowledge"],
                "dead_letter": 0,
            }
        ), patch.object(
            module, "automation_statuses", return_value=self._automations_ready()
        ), patch.object(module, "observation_samples", return_value=self._observations_pass()):
            result = module.audit()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("cockpit reported healthy while key signals or watermarks are missing/stale", result["errors"])

    def test_degraded_cockpit_is_disclosed_without_hiding_terminal_dead_letters(self) -> None:
        with patch.object(module, "launch_flag", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(
            module, "launchd_loaded", return_value=True
        ), patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "process_snapshot", return_value=self._ready()
        ), patch.object(
            module,
            "db_snapshot",
            return_value={"exists": True, "schema_version": 6, "tables": {"error_events": {"total": 0}}, "active": [], "slots": {"free": 2}, "integrity_check": "ok", "terminal_failed": {"total": 16}, "dead_letter": {"total": 4}, "leases": {"outbox_dispatch": 0, "provider_probe": 0}, "providers": []},
        ), patch.object(
            module, "cockpit_snapshot", return_value={
                "available": True,
                "status": "degraded",
                "health_truthful": True,
                "unknown_key_modules": ["Worker"],
                "missing_watermarks": ["knowledge"],
                "terminal_failed": 16,
                "dead_letter": 4,
            }
        ), patch.object(
            module, "automation_statuses", return_value=self._automations_ready()
        ), patch.object(module, "observation_samples", return_value=self._observations_pass()):
            result = module.audit()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["errors"], [])
        self.assertTrue(any("terminal dead-letter rows=4" in item for item in result["warnings"]))
        self.assertTrue(any("terminal failed rows=16" in item for item in result["warnings"]))


if __name__ == "__main__":
    unittest.main()
