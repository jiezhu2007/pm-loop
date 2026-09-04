from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_schedule_registry import RegistryError, load_registry  # noqa: E402
from pm_schedule_replay import build_request, run_replay  # noqa: E402


class PMScheduleReplayTests(unittest.TestCase):
    def test_build_request_marks_supplemental_manual_replay(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        schedule = registry.task("product-docs-gap-report")
        request = build_request(
            schedule=schedule,
            registry_hash=registry.registry_hash,
            reason="close evidence gap",
            now=datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(request["trigger_kind"], "manual_replay")
        self.assertEqual(request["payload"]["display_role"], "supplemental_replay")
        self.assertEqual(request["payload"]["replay_mode"], "dry_run")
        self.assertTrue(request["payload"]["replay_id"].startswith("replay-"))

    def test_confirmed_request_carries_explicit_physical_authorization(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        schedule = registry.task("retention-reclaimer")
        request = build_request(
            schedule=schedule,
            registry_hash=registry.registry_hash,
            reason="execute approved empty-plan acceptance",
            replay_mode="confirmed",
        )
        self.assertEqual(request["payload"]["replay_mode"], "confirmed")
        self.assertIs(request["payload"]["physical_action_authorized"], True)

    def test_reminder_replay_keeps_dry_run_policy(self) -> None:
        registry = load_registry(ROOT / "scripts" / "schedule-registry.json")
        schedule = registry.task("weekly-report-reminder")
        request = build_request(schedule=schedule, registry_hash=registry.registry_hash, reason="verify delivery boundary")
        self.assertEqual(request["payload"]["delivery_policy"], "dry_run")
        self.assertNotIn("delivery_authorized", request["payload"])

    def test_confirm_is_fail_closed_before_store_write(self) -> None:
        args = argparse.Namespace(
            dry_run=False,
            confirm=True,
            reason="operator approved replay",
            schedule_key="product-docs-gap-report",
            db_path=Path("/tmp/should-not-write.db"),
            registry=ROOT / "scripts" / "schedule-registry.json",
            runtime_registry=ROOT / "scripts" / "schedule-registry.json",
            canonical_registry=ROOT / "scripts" / "schedule-registry.json",
            lock_path=Path("/tmp/should-not-lock"),
            wait_seconds=1,
            poll_seconds=0.1,
        )
        with patch("pm_schedule_replay.PMLoopDispatcher") as dispatcher:
            with self.assertRaisesRegex(ValueError, "only supported for retention-reclaimer"):
                run_replay(args)
            dispatcher.assert_not_called()

    def test_dependency_task_is_rejected_before_store_write(self) -> None:
        args = argparse.Namespace(
            dry_run=True,
            confirm=False,
            reason="dependency replay must use canonical evidence",
            schedule_key="concept-refresh-planner",
            db_path=Path("/tmp/should-not-write.db"),
            registry=ROOT / "scripts" / "schedule-registry.json",
            runtime_registry=ROOT / "scripts" / "schedule-registry.json",
            canonical_registry=ROOT / "scripts" / "schedule-registry.json",
            lock_path=Path("/tmp/should-not-lock"),
            wait_seconds=1,
            poll_seconds=0.1,
        )
        with patch("pm_schedule_replay.PMSystemStore") as store, patch("pm_schedule_replay.build_request") as request:
            with self.assertRaisesRegex(RegistryError, "dependency_task_requires_canonical_replay"):
                run_replay(args)
        store.assert_not_called()
        request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
