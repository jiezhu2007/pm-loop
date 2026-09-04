from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/pm_system_s9_3_3_health_restore.py"
SPEC = importlib.util.spec_from_file_location("pm_system_s9_3_3_health_restore", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class S933HealthRestoreTests(unittest.TestCase):
    def test_audit_passes_when_contracts_and_read_only_boundaries_hold(self) -> None:
        contracts = {
            MODULE.HEALTH_LABEL: {"valid": True, "loaded": True},
            MODULE.HEARTBEAT_LABEL: {"valid": True, "loaded": True},
        }
        health = {"exists": True, "check_count": 11, "failed_count": 0, "checker_errors": []}
        flags = {
            "PM_V44_AUTOMATION_FREEZE": {"value": "on", "source": "launchctl"},
            "PM_V44_ADMISSION": {"value": "freeze", "source": "launchctl"},
        }
        with patch.object(MODULE, "plist_contract", side_effect=lambda label, script: contracts[label]), patch.object(
            MODULE, "launch_flag", side_effect=lambda name: flags[name]
        ), patch.object(MODULE, "latest_health", return_value=health), patch.object(
            MODULE, "db_snapshot", return_value={"sha256": "same"}
        ), patch.object(MODULE, "process_probe", return_value=[]):
            result = MODULE.audit(run_commands=False, production_db=Path("/tmp/pm-system-test.db"))
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["next_phase"], "S9.3.4")

    def test_audit_holds_when_health_command_or_flag_is_bad(self) -> None:
        contracts = {
            MODULE.HEALTH_LABEL: {"valid": True, "loaded": True},
            MODULE.HEARTBEAT_LABEL: {"valid": True, "loaded": True},
        }
        flags = {
            "PM_V44_AUTOMATION_FREEZE": {"value": "off", "source": "launchctl"},
            "PM_V44_ADMISSION": {"value": "freeze", "source": "launchctl"},
        }
        runs = {
            "system-health-check": {"returncode": 1},
            "heartbeat": {"returncode": 0},
        }
        with patch.object(MODULE, "plist_contract", side_effect=lambda label, script: contracts[label]), patch.object(
            MODULE, "launch_flag", side_effect=lambda name: flags[name]
        ), patch.object(MODULE, "latest_health", return_value={"exists": True, "check_count": 11, "failed_count": 0, "checker_errors": []}), patch.object(
            MODULE, "db_snapshot", return_value={"sha256": "same"}
        ), patch.object(MODULE, "process_probe", return_value=[]), patch.object(
            MODULE, "run_entrypoint", side_effect=[runs["system-health-check"], runs["heartbeat"]]
        ):
            result = MODULE.audit(run_commands=True, production_db=Path("/tmp/pm-system-test.db"))
        self.assertEqual(result["status"], "HOLD_CONTINUE")
        self.assertIn("manual health/heartbeat command failed", result["errors"])
        self.assertIn("freeze/admission flags changed or are not on/freeze", result["errors"])


if __name__ == "__main__":
    unittest.main()
