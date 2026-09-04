from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_s9_dry_run import build_dry_run  # noqa: E402


class S9DryRunTests(unittest.TestCase):
    def test_dry_run_is_read_only_and_builds_replayable_missed_task_inventory(self) -> None:
        data = build_dry_run()
        self.assertTrue(data["read_only"])
        self.assertTrue(data["python312_exists"])
        self.assertFalse(data["production_state_touched"])
        self.assertEqual(data["external_provider_calls"], 0)
        by_status = data["pending_uploads"]["by_status"]
        self.assertEqual(data["pending_uploads"]["rows"], sum(by_status.values()))
        self.assertEqual(data["findings"]["pending_queued_unknown"], data["pending_uploads"]["queued_unknown_count"])
        self.assertGreaterEqual(data["pending_uploads"]["rows"], 0)
        self.assertEqual(data["hash_only_checkpoint"]["status"], "running")
        self.assertIsInstance(data["findings"]["not_loaded_labels"], list)
        self.assertNotIn("com.zhujie14.product-intelligence-monitor", data["findings"]["unpinned_python_labels"])
        labels = {item["label"] for item in data["launchagents"]}
        self.assertIn("com.zhujie14.weekly-sync-and-refresh", labels)


if __name__ == "__main__":
    unittest.main()
