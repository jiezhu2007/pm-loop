from __future__ import annotations

import fcntl
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/pm_system_s9_timeline_dry_run.py"
spec = importlib.util.spec_from_file_location("pm_system_s9_timeline_dry_run", MODULE_PATH)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class S927TimelineDryRunTests(unittest.TestCase):
    def test_lock_competition_probe_is_isolated_and_non_blocking(self) -> None:
        value = module.lock_competition_probe()
        self.assertTrue(value["isolated"])
        self.assertTrue(value["contender_observed_held"])
        self.assertTrue(value["released_observed_free"])
        self.assertFalse(value["production_state_touched"])

    def test_review_pair_detects_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source"
            mirror = root / "mirror"
            source.mkdir()
            mirror.mkdir()
            (source / "2026-W35-review.html").write_text("source", encoding="utf-8")
            (mirror / "2026-W35-review.html").write_text("mirror", encoding="utf-8")
            with patch.object(module, "REVIEW_SOURCE", source), patch.object(module, "REVIEW_MIRROR", mirror):
                value = module.review_pair()
            self.assertFalse(value["latest_pair_equal"])
            self.assertEqual(value["mismatched"], ["2026-W35-review.html"])

    def test_state_snapshot_reports_free_lock_without_mutating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "probe.lock"
            path.write_text("", encoding="utf-8")
            before = module.file_state(path)
            self.assertFalse(module.lock_is_held(path))
            after = module.file_state(path)
            self.assertEqual(before["sha256"], after["sha256"])

    def test_catchup_refuses_missing_lock_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            catchup = Path(temp) / "catchup.py"
            catchup.write_text("", encoding="utf-8")
            with patch.object(module, "CATCHUP", catchup), patch.object(module, "CATCHUP_LOCK", Path(temp) / "missing.lock"):
                value = module.run_catchup_dry_run()
            self.assertIsNone(value["exit_code"])
            self.assertIn("refused to create it", value["error"])

    def test_skip_catchup_audit_stays_read_only(self) -> None:
        def frozen_flag(name):
            return {"value": "freeze" if name == "PM_V44_ADMISSION" else "on", "source": "test"}

        with patch.object(module, "launch_flag", side_effect=frozen_flag), patch.object(
            module, "launchd_loaded", return_value=False
        ), patch.object(module, "writer_processes", return_value=[]), patch.object(
            module,
            "state_snapshot",
            side_effect=[
                {"state": 1, "locks": [{"held": False}]},
                {"state": 1, "locks": [{"held": False}]},
            ],
        ), patch.object(module, "script_contracts", return_value={"declared_targets": {}}), patch.object(
            module, "plist_contract", return_value={"valid": True}
        ), patch.object(
            module, "review_pair", return_value={"latest_pair_equal": True, "mismatched": []}
        ), patch.object(
            module, "lock_competition_probe", return_value={"contender_observed_held": True, "released_observed_free": True}
        ):
            value = module.audit(run_catchup=False)
        self.assertTrue(value["read_only"])
        self.assertTrue(value["state_unchanged"])
        self.assertEqual(value["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
