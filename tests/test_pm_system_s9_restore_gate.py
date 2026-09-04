from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
MODULE_PATH = ROOT / "scripts/pm_system_s9_restore_gate.py"
spec = importlib.util.spec_from_file_location("pm_system_s9_restore_gate", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S929RestoreGateTests(unittest.TestCase):
    def test_report_status_requires_pass_marker(self) -> None:
        path = Path("/tmp/s929-status.md")
        path.write_text("### 判定：`PASS`\n", encoding="utf-8")
        try:
            self.assertTrue(module.report_status(path)["pass"])
        finally:
            path.unlink(missing_ok=True)

    def test_report_status_accepts_html_metadata_cell(self) -> None:
        path = Path("/tmp/s929-status.html")
        path.write_text("<dt>当前判定</dt><dd><strong>PASS（只读）</strong></dd>", encoding="utf-8")
        try:
            self.assertTrue(module.report_status(path)["pass"])
        finally:
            path.unlink(missing_ok=True)

    def test_gate_requires_freeze_and_all_report_pairs(self) -> None:
        frozen = lambda name: {"value": "freeze" if name == "PM_V44_ADMISSION" else "on", "source": "test"}
        pair = {"markdown": {"pass": True}, "html": {"pass": True}}
        reports = [pair for _ in module.REQUIRED_REPORTS]
        labels = {label: False for label in module.ALL_WRITER_LABELS}
        labels[module.ONEAPI_LABEL] = True
        labels[module.OPENVIKING_LABEL] = True
        with patch.object(module, "report_status", side_effect=[{"pass": True}, {"pass": True}] * len(module.REQUIRED_REPORTS)), patch.object(
            module, "launch_flag", side_effect=frozen
        ), patch.object(module, "launchd_loaded", side_effect=lambda label: labels[label]), patch.object(
            module, "lock_is_held", return_value=False
        ), patch.object(module, "writer_processes", return_value=[]):
            value = module.audit()
        self.assertEqual(value["status"], "PASS")
        self.assertTrue(value["read_only"])
        self.assertFalse(value["production_state_touched"])

    def test_frozen_allowed_services_may_be_loaded(self) -> None:
        frozen = lambda name: {"value": "freeze" if name == "PM_V44_ADMISSION" else "on", "source": "test"}
        labels = {label: False for label in module.ALL_WRITER_LABELS}
        labels[module.ONEAPI_LABEL] = True
        labels[module.OPENVIKING_LABEL] = True
        labels.update({label: True for label in module.FROZEN_ALLOWED_LABELS})
        with patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "launch_flag", side_effect=frozen
        ), patch.object(module, "launchd_loaded", side_effect=lambda label: labels[label]), patch.object(
            module, "lock_is_held", return_value=False
        ), patch.object(module, "writer_processes", return_value=[]):
            value = module.audit()
        self.assertEqual(value["status"], "PASS")
        self.assertEqual(value["unexpected_loaded_labels"], [])
        self.assertTrue(all(value["frozen_allowed_labels"].values()))

    def test_business_writer_loaded_still_blocks_gate(self) -> None:
        frozen = lambda name: {"value": "freeze" if name == "PM_V44_ADMISSION" else "on", "source": "test"}
        labels = {label: False for label in module.ALL_WRITER_LABELS}
        labels[module.ONEAPI_LABEL] = True
        labels[module.OPENVIKING_LABEL] = True
        labels["com.zhujie14.weekly-sync-and-refresh"] = True
        with patch.object(module, "report_status", return_value={"pass": True}), patch.object(
            module, "launch_flag", side_effect=frozen
        ), patch.object(module, "launchd_loaded", side_effect=lambda label: labels[label]), patch.object(
            module, "lock_is_held", return_value=False
        ), patch.object(module, "writer_processes", return_value=[]):
            value = module.audit()
        self.assertEqual(value["status"], "HOLD_CONTINUE")
        self.assertIn("com.zhujie14.weekly-sync-and-refresh", value["unexpected_loaded_labels"])


if __name__ == "__main__":
    unittest.main()
