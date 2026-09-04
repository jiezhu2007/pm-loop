from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/pm_system_s9_3_4_timeline_restore.py"
spec = importlib.util.spec_from_file_location("pm_system_s9_3_4_timeline_restore", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S934TimelineRestoreTests(unittest.TestCase):
    def test_marker_pass_requires_freeze_terminal_marker(self) -> None:
        base = {"exists": True, "task": "pm-timeline-daily", "status": "ok", "reason": "maintenance_expected:v44_freeze", "exit_code": None, "finished_at": "2026-08-29T00:00:00+08:00"}
        self.assertTrue(module.marker_pass(base, "pm-timeline-daily"))
        explicit_zero = dict(base)
        explicit_zero["exit_code"] = 0
        self.assertTrue(module.marker_pass(explicit_zero, "pm-timeline-daily"))
        for key, value in (("reason", "completed"), ("status", "running"), ("exit_code", 1)):
            changed = dict(base)
            changed[key] = value
            self.assertFalse(module.marker_pass(changed, "pm-timeline-daily"))

    def test_contract_requires_executable_script_and_noop_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            script = root / "daily.sh"
            plist = root / "daily.plist"
            script.write_text("freeze_active() { return 0; }\nmaintenance_expected:v44_freeze\nexit 0\n", encoding="utf-8")
            script.chmod(0o755)
            plist.write_bytes(__import__("plistlib").dumps({"ProgramArguments": ["/bin/bash", str(script)], "WorkingDirectory": str(module.TIMELINE_ROOT), "EnvironmentVariables": {"CODEX_PYTHON": module.PYTHON}, "StandardOutPath": str(module.TIMELINE_STATE / "logs/x.log"), "StandardErrorPath": str(module.TIMELINE_STATE / "logs/x.log")}))
            spec_value = {"label": "test.daily", "script": script, "plist": plist, "schedule": "test"}
            with patch.object(module, "launchd_loaded", return_value=False):
                value = module.plist_contract("daily", spec_value)
            self.assertTrue(value["valid"])

    def test_audit_does_not_run_weekly_when_daily_fails(self) -> None:
        contracts = {name: {"valid": True, "loaded": True} for name in module.JOBS}
        failed_daily = {"status": "HOLD_CONTINUE", "errors": ["manual entrypoint failed"]}
        with patch.object(module, "plist_contract", side_effect=lambda name, spec: contracts[name]), patch.object(
            module, "run_one", return_value=failed_daily
        ) as run_one, patch.object(module, "launch_flag", return_value="on"), patch.object(
            module, "writer_processes", return_value=[]
        ):
            value = module.audit(production_db=Path("/tmp/pm-s934-test.db"), run_commands=True)
        self.assertEqual(run_one.call_count, 1)
        self.assertEqual(value["weekly"]["status"], "SKIPPED")
        self.assertEqual(value["status"], "HOLD_CONTINUE")

    def test_audit_passes_only_when_both_entries_are_pass(self) -> None:
        contracts = {name: {"valid": True, "loaded": True} for name in module.JOBS}
        passed = {"status": "PASS", "unchanged": {"timeline": True, "timeline_logs": True, "reviews": True, "production_db": True}, "marker_changed": True}
        with patch.object(module, "plist_contract", side_effect=lambda name, spec: contracts[name]), patch.object(
            module, "run_one", side_effect=[passed, passed]
        ), patch.object(module, "launch_flag", side_effect=lambda name: "on" if name == "PM_V44_AUTOMATION_FREEZE" else "freeze"), patch.object(
            module, "writer_processes", return_value=[]
        ):
            value = module.audit(production_db=Path("/tmp/pm-s934-test.db"), run_commands=True)
        self.assertEqual(value["status"], "PASS")
        self.assertEqual(value["next_phase"], "S9.3.5")


if __name__ == "__main__":
    unittest.main()
