from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_runtime import TERMINAL_EVENT_TYPES, project_state  # noqa: E402


class RunStateProjectionTests(unittest.TestCase):
    def test_v2_gate_and_action_stages_replay_to_distinct_states(self) -> None:
        request = {
            "run_id": "run-state-fixture",
            "loop_id": "concept-review",
            "permission_mode": "approved_action",
            "runtime": {"kind": "codex"},
            "created_at": "2026-08-17T00:00:00Z",
        }
        expected = [
            ("run/created", "queued"),
            ("run/started", "running"),
            ("source/started", "collecting"),
            ("source/completed", "reasoning"),
            ("analysis/started", "analyzing"),
            ("tool/call", "analyzing"),
            ("tool/result", "analyzing"),
            ("analysis/completed", "verifying"),
            ("assistant/draft", "verifying"),
            ("artifact/written", "verifying"),
            ("gate/requested", "awaiting_human"),
            ("gate/paused", "paused"),
            ("gate/changes_requested", "changes_requested"),
            ("gate/approved", "action_queued"),
            ("action/queued", "action_queued"),
            ("action/started", "executing"),
            ("action/completed", "verifying"),
            ("run/completed", "completed"),
        ]
        events = []
        for seq, (event_type, status) in enumerate(expected, start=1):
            events.append({"seq": seq, "type": event_type, "at": f"2026-08-17T00:00:{seq:02d}Z", "data": {}})
            projected = project_state(request, events)
            self.assertEqual(projected["status"], status, event_type)

        projected = project_state(request, events)
        self.assertEqual(projected["last_event"]["type"], "run/completed")
        self.assertEqual(projected["completed_at"], "2026-08-17T00:00:18Z")

    def test_only_namespaced_run_events_close_a_run(self) -> None:
        request = {"run_id": "run-terminal-fixture", "loop_id": "daily-radar", "runtime": {"kind": "codex"}}
        bare_state = project_state(request, [{"seq": 1, "type": "completed", "at": "2026-08-17T00:00:01Z", "data": {}}])
        self.assertEqual(bare_state["status"], "unknown")
        self.assertIsNone(bare_state["completed_at"])
        self.assertEqual(
            TERMINAL_EVENT_TYPES,
            {"run/completed", "run/failed", "run/cancelled", "run/rejected", "gate/rejected"},
        )

    def test_gate_rejected_is_a_terminal_projection(self) -> None:
        request = {"run_id": "run-rejected-fixture", "loop_id": "concept-review", "runtime": {"kind": "codex"}}
        projected = project_state(
            request,
            [{"seq": 1, "type": "gate/rejected", "at": "2026-08-17T00:00:01Z", "data": {}}],
        )
        self.assertEqual(projected["status"], "rejected")
        self.assertEqual(projected["completed_at"], "2026-08-17T00:00:01Z")

    def test_retry_clears_previous_terminal_metadata(self) -> None:
        request = {"run_id": "run-retry-fixture", "loop_id": "daily-radar", "runtime": {"kind": "codex"}}
        events = [
            {"seq": 1, "type": "run/failed", "at": "2026-08-17T00:00:01Z", "data": {"error": "source down"}},
            {"seq": 2, "type": "run/retrying", "at": "2026-08-17T00:00:02Z", "data": {"attempt": 2}},
        ]
        projected = project_state(request, events)
        self.assertEqual(projected["status"], "retrying")
        self.assertIsNone(projected["completed_at"])
        self.assertIsNone(projected["error"])


if __name__ == "__main__":
    unittest.main()
