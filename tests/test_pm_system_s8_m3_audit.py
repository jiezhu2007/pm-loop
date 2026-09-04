from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.pm_system_s8_m3_audit import (
    aggregate_directory,
    checkpoint_summary,
    extract_last_json,
    isolated_store_summary,
    ledger_summary,
    reconcile,
    read_runtime_flag,
    write_isolated_observations,
)


class S84M3AuditTests(unittest.TestCase):
    def test_extracts_final_plan_totals_and_kept_uris_are_reconciled(self) -> None:
        payload = extract_last_json("noise {\"x\": 1}\n{" + '"discovered": 2, "kept": 1, "skipped": 1}' )
        self.assertEqual(payload, {"discovered": 2, "kept": 1, "skipped": 1})

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            ledger = {"a": {"source": "feature-list", "target_uri": "viking://resources/a"}}
            ledger_path = path / "ledger.json"
            ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
            summary = ledger_summary(ledger_path)
            result = reconcile(
                {"feature-list": {"kept_uris": ["viking://resources/a", "viking://resources/b"]}},
                summary,
            )
            self.assertEqual(result["feature-list"]["inventory_hits"], 1)
            self.assertEqual(result["feature-list"]["inventory_missing_from_ledger"], 1)

    def test_observations_land_only_in_isolated_store_and_replay_is_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            task_dir = root / "tasks"
            task_dir.mkdir()
            task_file = task_dir / "task.json"
            task_file.write_text(
                json.dumps({"task_id": "t1", "task_type": "add_resource", "status": "completed", "resource_id": "viking://resources/a"}),
                encoding="utf-8",
            )
            before = aggregate_directory(task_dir)
            observations = [{"task_id": "t1", "task_type": "add_resource", "external_status": "completed", "classification": "terminal", "resource_uri": "viking://resources/a", "reason": "terminal_status", "payload_sha256": "x"}]
            db_path = root / "audit" / "pm-system.db"
            first = write_isolated_observations(db_path, observations)
            second = write_isolated_observations(db_path, observations)
            self.assertEqual(first["observation_rows"], 1)
            self.assertEqual(second["observation_rows"], 1)
            self.assertEqual(before, aggregate_directory(task_dir))

    def test_missing_checkpoint_source_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            (report_dir / "one-hash-checkpoint.json").write_text(
                json.dumps({"sources": {"feature-list": {"status": "completed", "total": 1, "processed": 1, "hashed": 1, "unchanged": 0, "completed_doc_guids": ["a"]}}}),
                encoding="utf-8",
            )
            result = checkpoint_summary(report_dir)
            self.assertIn("ontology", result["missing_expected_sources"])
            self.assertEqual(result["by_source"]["feature-list"]["completed"], 1)

    def test_runtime_flag_prefers_launchctl_and_has_environment_fallback(self) -> None:
        launchctl_result = type(
            "Completed",
            (),
            {"returncode": 0, "stdout": "freeze\n", "stderr": ""},
        )()
        with patch("scripts.pm_system_s8_m3_audit.subprocess.run", return_value=launchctl_result):
            self.assertEqual(
                read_runtime_flag("PM_V44_ADMISSION"),
                {"value": "freeze", "source": "launchctl"},
            )

        fallback_result = type(
            "Completed",
            (),
            {"returncode": 1, "stdout": "", "stderr": "not found"},
        )()
        with patch.dict("os.environ", {"PM_V44_ADMISSION": "freeze"}, clear=False):
            with patch("scripts.pm_system_s8_m3_audit.subprocess.run", return_value=fallback_result):
                self.assertEqual(
                    read_runtime_flag("PM_V44_ADMISSION"),
                    {"value": "freeze", "source": "process_environment"},
                )


if __name__ == "__main__":
    unittest.main()
