from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
MODULE_PATH = ROOT / "scripts/pm_system_s9_timeline_seal.py"
spec = importlib.util.spec_from_file_location("pm_system_s9_timeline_seal", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S928TimelineSealTests(unittest.TestCase):
    def test_file_record_reports_hash_and_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "file.txt"
            path.write_text("seal", encoding="utf-8")
            self.assertTrue(module.file_record(path)["exists"])
            self.assertTrue(module.file_record(path)["sha256"])
            self.assertFalse(module.file_record(path.with_name("missing"))["exists"])

    def test_audit_is_read_only_and_preserves_freeze(self) -> None:
        frozen = lambda name: {"value": "freeze" if name == "PM_V44_ADMISSION" else "on", "source": "test"}
        valid_plist = {
            "valid": True,
            "installed": {"sha256": "installed"},
            "previous_backup": {"sha256": "backup"},
            "loaded": False,
        }
        with patch.object(module, "launch_flag", side_effect=frozen), patch.object(module, "launchd_loaded", return_value=False), patch.object(
            module, "writer_processes", return_value=[]
        ), patch.object(module, "plist_record", return_value=valid_plist), patch.object(
            module, "file_record", return_value={"exists": True, "sha256": "x"}
        ):
            value = module.audit()
        self.assertEqual(value["status"], "PASS")
        self.assertTrue(value["read_only"])
        self.assertFalse(value["production_state_touched"])
        self.assertEqual(value["freeze_before"], value["freeze_after"])


if __name__ == "__main__":
    unittest.main()
