from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_schedule_registry import RegistryError, canonical_hash, is_business_window_open, latest_scheduled_at, load_registry, occurrence_key, validate_document


class ScheduleRegistryTests(unittest.TestCase):
    def test_canonical_registry_has_calendar_and_dependency_tasks(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        self.assertEqual(len(registry.tasks), 14)
        self.assertTrue(registry.registry_hash.startswith("sha256:"))
        document = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
        self.assertEqual(registry.registry_hash, canonical_hash(document))
        report = registry.task("product-docs-gap-report")
        self.assertEqual(report.calendar, {"kind": "weekly", "weekday": 2, "hour": 14, "minute": 0})
        self.assertEqual(registry.task("databuilder-product-gap-report").calendar, {"kind": "weekly", "weekday": 2, "hour": 10, "minute": 0})
        self.assertEqual(registry.task("weekly-report-reminder").calendar, {"kind": "weekly", "weekday": 7, "hour": 17, "minute": 0})
        self.assertEqual(registry.task("weekly-report-reminder").delivery_policy, "dry_run")
        self.assertEqual(registry.task("competitive-radar-ingest").calendar, {"kind": "daily", "hour": 10, "minute": 30})
        self.assertEqual(registry.task("competitive-radar-brief").calendar, {"kind": "weekly", "weekday": 2, "hour": 16, "minute": 30})
        self.assertEqual(registry.task("retention-observer").calendar, {"kind": "weekly", "weekday": 3, "hour": 11, "minute": 0})
        self.assertEqual(registry.task("retention-reclaimer").calendar, {"kind": "weekly", "weekday": 4, "hour": 14, "minute": 0})
        self.assertEqual(registry.task("retention-reclaimer").execution_window["last_batch_start"], "16:30")
        self.assertTrue(registry.task("retention-reclaimer").execution_window["defer_on_active_p0_p1"])
        self.assertEqual(registry.task("concept-inventory-compaction").calendar, {"kind": "weekly", "weekday": 3, "hour": 11, "minute": 15})
        self.assertEqual(registry.task("concept-inventory-compaction").profile, "maintenance")
        self.assertEqual(registry.task("artifact-inventory").calendar, {"kind": "weekly", "weekday": 3, "hour": 12, "minute": 0})
        self.assertEqual(registry.task("artifact-inventory").profile, "maintenance")
        planner = registry.task("concept-refresh-planner")
        self.assertFalse(planner.is_calendar)
        self.assertEqual(planner.trigger["upstream_schedule_key"], "weekly-sync-and-refresh")
        self.assertEqual(planner.trigger["terminal_status"], "completed")

    def test_timezone_and_weekday_are_deterministic(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        monday = datetime(2026, 9, 7, 2, 0, tzinfo=timezone.utc)  # 10:00 Shanghai
        task = registry.task("weekly-sync-and-refresh")
        scheduled = latest_scheduled_at(task, monday, timezone_name=registry.timezone_name)
        self.assertEqual(scheduled.isoformat(), "2026-08-31T02:05:00+00:00")
        self.assertEqual(occurrence_key(task, scheduled), "weekly-sync-and-refresh:20260831T020500Z")

    def test_daily_before_window_uses_previous_day(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        task = registry.task("pm-timeline-daily")
        before = datetime(2026, 9, 7, 5, 0, tzinfo=timezone.utc)  # 13:00 Shanghai
        scheduled = latest_scheduled_at(task, before, timezone_name=registry.timezone_name)
        self.assertEqual(scheduled.isoformat(), "2026-09-06T05:37:00+00:00")

    def test_business_window_boundaries(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        self.assertTrue(is_business_window_open(datetime(2026, 9, 7, 2, 0, tzinfo=timezone.utc), timezone_name=registry.timezone_name))
        self.assertTrue(is_business_window_open(datetime(2026, 9, 7, 10, 0, tzinfo=timezone.utc), timezone_name=registry.timezone_name))
        self.assertFalse(is_business_window_open(datetime(2026, 9, 7, 1, 59, tzinfo=timezone.utc), timezone_name=registry.timezone_name))
        self.assertFalse(is_business_window_open(datetime(2026, 9, 7, 10, 1, tzinfo=timezone.utc), timezone_name=registry.timezone_name))

    def test_registry_rejects_schedule_outside_business_window(self) -> None:
        document = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
        document["tasks"][0]["calendar"]["hour"] = 9
        with self.assertRaisesRegex(RegistryError, "10:00–18:00"):
            validate_document(document)


if __name__ == "__main__":
    unittest.main()
