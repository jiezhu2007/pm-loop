from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_v45_freeze_continuation import continue_freeze  # noqa: E402


class V45FreezeContinuationTests(unittest.TestCase):
    def test_advances_only_persistent_freeze_and_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            store.set_migration_freeze(
                migration_id="m",
                migration_epoch="e",
                stage_id="G3",
                owner="old",
                deadline_at="2099-01-01T00:00:00Z",
            )
            with patch("pm_system_v45_freeze_continuation._matching_processes", return_value=[]), patch(
                "pm_system_v45_freeze_continuation.automation_statuses",
                return_value={"databuilder": "PAUSED", "automation": "PAUSED", "v4-4-s10": "PAUSED"},
            ):
                report = continue_freeze(
                    db_path=root / "pm.db",
                    report_dir=root / "reports",
                    migration_id="m",
                    epoch="e",
                    owner="new",
                    from_stage="G3",
                    to_stage="G4",
                )
            self.assertEqual(report["decision"], "PASS")
            self.assertEqual(report["after"]["freeze"]["stage_id"], "G4")
            self.assertEqual(report["after"]["freeze"]["state"], "freeze")
            self.assertFalse(report["business_state_mutated"])
            self.assertTrue((root / "reports" / "freeze-continuation-g4-检查报告.md").is_file())
            self.assertTrue((root / "reports" / "freeze-continuation-g4-检查报告.html").is_file())
            self.assertEqual(PMSystemStore(root / "pm.db").migration_freeze()["stage_id"], "G4")

    def test_refuses_wrong_source_stage_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            store.set_migration_freeze(
                migration_id="m",
                migration_epoch="e",
                stage_id="G1",
                owner="old",
                deadline_at="2099-01-01T00:00:00Z",
            )
            report = continue_freeze(
                db_path=root / "pm.db",
                report_dir=root / "reports",
                migration_id="m",
                epoch="e",
                owner="new",
                from_stage="G3",
                to_stage="G4",
            )
            self.assertEqual(report["decision"], "HOLD")
            self.assertEqual(report["after"]["freeze"]["stage_id"], "G1")


if __name__ == "__main__":
    unittest.main()
