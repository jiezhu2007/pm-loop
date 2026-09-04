from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from check_unified_scheduler import _planner_version_error, _registry_task_summary  # noqa: E402


class UnifiedSchedulerHealthTests(unittest.TestCase):
    def test_health_check_derives_task_keys_from_canonical_registry(self) -> None:
        registry = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
        original_task_count = len(registry["tasks"])
        future_task = copy.deepcopy(registry["tasks"][0])
        future_task.update({"schedule_key": "future-calendar-task", "handler": "future_calendar_handler"})
        registry["tasks"].append(future_task)

        task_keys, tasks, errors = _registry_task_summary(registry)

        self.assertEqual(errors, [])
        self.assertEqual(len(tasks), original_task_count + 1)
        self.assertIn("concept-refresh-planner", task_keys)
        self.assertIn("future-calendar-task", task_keys)

    def test_health_check_rejects_duplicate_or_empty_schedule_keys(self) -> None:
        registry = json.loads((ROOT / "scripts" / "schedule-registry.json").read_text(encoding="utf-8"))
        original_task_count = len(registry["tasks"])
        registry["tasks"].append(copy.deepcopy(registry["tasks"][0]))
        registry["tasks"].append({"schedule_key": "", "calendar": {"kind": "daily", "hour": 10, "minute": 0}})

        _keys, _tasks, errors = _registry_task_summary(registry)

        self.assertIn("duplicate schedule_key: weekly-sync-and-refresh", errors)
        self.assertIn(f"task[{original_task_count + 1}].schedule_key is required", errors)

    def test_v1_dependency_events_before_v12_are_historical_not_current_drift(self) -> None:
        cutover = "2026-09-03T06:43:00Z"
        self.assertIsNone(
            _planner_version_error(
                expected="concept-refresh-planner.v2",
                actual="concept-refresh-planner.v1",
                created_at="2026-09-03T06:20:00Z",
                event_status="consumed",
                v12_cutover_at=cutover,
            )
        )
        self.assertEqual(
            _planner_version_error(
                expected="concept-refresh-planner.v2",
                actual="concept-refresh-planner.v1",
                created_at="2026-09-03T06:44:00Z",
                event_status="consumed",
                v12_cutover_at=cutover,
            ),
            "planner_version_mismatch",
        )
        self.assertEqual(
            _planner_version_error(
                expected="concept-refresh-planner.v2",
                actual="concept-refresh-planner.v1",
                created_at="2026-09-03T06:20:00Z",
                event_status="pending",
                v12_cutover_at=cutover,
            ),
            "planner_version_mismatch",
        )


if __name__ == "__main__":
    unittest.main()
