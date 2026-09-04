from __future__ import annotations

import importlib.util
import sys
import unittest
from unittest.mock import patch


ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
MODULE_PATH = ROOT / "scripts/pm_system_s10_observe.py"
spec = importlib.util.spec_from_file_location("pm_system_s10_observe", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S10ObserveTests(unittest.TestCase):
    def _base(self, all_normal: bool) -> dict:
        return {
            "flags": {"PM_V44_AUTOMATION_FREEZE": "off", "PM_V44_ADMISSION": "on"},
            "processes": {"control_plane": [{"pid": 1}], "worker": [{"pid": 2}], "memory_sync": [{"pid": 3}], "forbidden": []},
            "database": {"exists": True, "schema_version": 6, "integrity_check": "ok", "active": [], "slots": {"free": 2}, "tables": {"error_events_total": 0}, "providers": []},
            "cockpit": {"available": True, "status": "healthy", "health_truthful": True},
            "timeline": {"normal": {"daily": all_normal, "weekly": all_normal}, "all_normal": all_normal, "freshness": {"daily": {"fresh": all_normal}, "weekly": {"fresh": all_normal}}, "markers": {}},
            "health": {"exists": True, "fresh": True, "passed": 11, "total": 11, "pending": []},
            "logs": {},
        }

    def test_pending_marker_keeps_observation_open(self) -> None:
        with patch.object(module, "_launchctl_getenv", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(module, "process_snapshot", return_value=self._base(False)["processes"]), patch.object(module, "db_snapshot", return_value=self._base(False)["database"]), patch.object(module, "cockpit_snapshot", return_value=self._base(False)["cockpit"]), patch.object(module, "timeline_snapshot", return_value=self._base(False)["timeline"]), patch.object(module, "health_snapshot", return_value=self._base(False)["health"]), patch.object(module, "log_snapshot", return_value={}):
            result = module.observe()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertEqual(result["hard_issues"], [])
        self.assertIn("daily/weekly normal completion marker not observed", result["pending"])

    def test_normal_marker_and_clean_runtime_pass(self) -> None:
        base = self._base(True)
        with patch.object(module, "_launchctl_getenv", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(module, "process_snapshot", return_value=base["processes"]), patch.object(module, "db_snapshot", return_value=base["database"]), patch.object(module, "cockpit_snapshot", return_value=base["cockpit"]), patch.object(module, "timeline_snapshot", return_value=base["timeline"]), patch.object(module, "health_snapshot", return_value=base["health"]), patch.object(module, "log_snapshot", return_value={}):
            result = module.observe()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["hard_issues"], [])
        self.assertEqual(result["pending"], [])

    def test_active_queue_is_hard_issue(self) -> None:
        base = self._base(True)
        base["database"]["active"] = [{"table": "jobs", "status": "running", "count": 1}]
        with patch.object(module, "_launchctl_getenv", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(module, "process_snapshot", return_value=base["processes"]), patch.object(module, "db_snapshot", return_value=base["database"]), patch.object(module, "cockpit_snapshot", return_value=base["cockpit"]), patch.object(module, "timeline_snapshot", return_value=base["timeline"]), patch.object(module, "health_snapshot", return_value=base["health"]), patch.object(module, "log_snapshot", return_value={}):
            result = module.observe()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("active queue state remains", result["hard_issues"])

    def test_first_provider_throttle_is_a_hard_issue_even_when_circuit_closed(self) -> None:
        base = self._base(True)
        base["database"]["providers"] = [{
            "provider_key": "oneapi|default|model",
            "throttle_until": "2099-01-01T00:00:00Z",
            "circuit_state": "closed",
            "throttled": True,
        }]
        with patch.object(module, "_launchctl_getenv", side_effect=lambda name: "off" if name == "PM_V44_AUTOMATION_FREEZE" else "on"), patch.object(module, "process_snapshot", return_value=base["processes"]), patch.object(module, "db_snapshot", return_value=base["database"]), patch.object(module, "cockpit_snapshot", return_value=base["cockpit"]), patch.object(module, "timeline_snapshot", return_value=base["timeline"]), patch.object(module, "health_snapshot", return_value=base["health"]), patch.object(module, "log_snapshot", return_value={}):
            result = module.observe()
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("provider bucket is open/throttled: oneapi|default|model", result["hard_issues"])


if __name__ == "__main__":
    unittest.main()
