from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import ov_memory_sync  # noqa: E402
import pm_system_watcher_runtime_sync  # noqa: E402


class OpenVikingMemorySyncTests(unittest.TestCase):
    def test_legacy_pending_is_imported_and_quarantined_without_remote_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("legacy", encoding="utf-8")
            pending = root / "pending.json"
            pending.write_text(json.dumps({"items": [{"name": "note.md", "status": "queued", "task_id": ""}]}), encoding="utf-8")
            from pm_system_store import PMSystemStore

            store = PMSystemStore(root / "pm.db")
            result = ov_memory_sync.import_legacy_pending(store, mirror, namespace_epoch="v45", pending_path=pending)
            self.assertEqual(result["imported_count"], 1)
            with store.connect() as connection:
                event = connection.execute("SELECT state FROM memory_change_events").fetchone()
                outbox = connection.execute("SELECT kind,profile,status,terminal_reason,payload_json FROM outbox_items").fetchone()
            self.assertEqual(event[0], "quarantine")
            self.assertEqual(tuple(outbox[:4]), ("memory", "memory-skill", "quarantine", "legacy_pending_uncertain_remote_state"))
            self.assertTrue(json.loads(outbox[4])["legacy_pending_import"]["remote_result_unknown"])

    def test_retry_delay_is_exponential_and_capped(self) -> None:
        self.assertEqual(ov_memory_sync._watch_retry_delay(1, base=5, maximum=20), 5)
        self.assertEqual(ov_memory_sync._watch_retry_delay(2, base=5, maximum=20), 10)
        self.assertEqual(ov_memory_sync._watch_retry_delay(3, base=5, maximum=20), 20)
        self.assertEqual(ov_memory_sync._watch_retry_delay(8, base=5, maximum=20), 20)

    def test_permanent_failure_is_quarantined_and_mtime_reactivates(self) -> None:
        now = datetime(2026, 8, 29, 13, 0, tzinfo=timezone.utc)
        first = ov_memory_sync._watch_retry_plan(
            {}, mtime_ns=100, error="ov_rest failed: HTTP 404 Not Found", now=now
        )
        self.assertEqual(first["status"], "quarantined")
        self.assertEqual(first["retry_attempt"], 1)
        self.assertIsNone(first["next_retry_at"])

        changed = ov_memory_sync._watch_retry_plan(
            first,
            mtime_ns=101,
            error="ov_rest failed: HTTP 503 Service Unavailable",
            max_attempts=3,
            backoff_base=5,
            backoff_max=30,
            now=now,
        )
        self.assertEqual(changed["status"], "retry_wait")
        self.assertEqual(changed["retry_attempt"], 1)
        self.assertEqual(changed["mtime_ns"], 101)

    def test_pending_retry_state_is_atomic_and_replaced_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "pending.json"
            with mock.patch.object(ov_memory_sync, "STATE_PATH", state_path):
                ov_memory_sync.record_pending(
                    "note.md",
                    None,
                    "retry_wait",
                    "temporary outage",
                    retry_attempt=2,
                    next_retry_at="2026-08-29T13:00:10Z",
                    mtime_ns=100,
                )
                saved = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(len(saved["items"]), 1)
                self.assertEqual(saved["items"][0]["retry_attempt"], 2)

                ov_memory_sync.record_pending("note.md", {"task_id": "task-1"}, "queued")
                saved = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(len(saved["items"]), 1)
                self.assertEqual(saved["items"][0]["status"], "queued")
                self.assertNotIn("retry_attempt", saved["items"][0])

    def test_watch_quarantines_after_budget_without_repeating_forever(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("initial", encoding="utf-8")
            initial_mtime = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 1:
                    note.write_text("changed", encoding="utf-8")
                    os.utime(note, ns=(initial_mtime + 1_000_000_000,) * 2)
                elif sleep_calls >= 3:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=2,
                backoff_base=0,
                backoff_max=0,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(ov_memory_sync, "list_remote_notes", return_value=["note.md"]),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=True),
                mock.patch.object(
                    ov_memory_sync,
                    "remote_write",
                    side_effect=RuntimeError("ov_rest failed: HTTP 503 Service Unavailable"),
                ) as remote_write,
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            self.assertEqual(remote_write.call_count, 2)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["items"][0]["status"], "quarantined")
            self.assertEqual(saved["items"][0]["retry_attempt"], 2)

    def test_create_error_reconciles_matching_remote_content_without_duplicate_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("initial", encoding="utf-8")
            initial_mtime = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 1:
                    note.write_text("changed", encoding="utf-8")
                    os.utime(note, ns=(initial_mtime + 1_000_000_000,) * 2)
                elif sleep_calls >= 2:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=3,
                backoff_base=0,
                backoff_max=0,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(
                    ov_memory_sync,
                    "list_remote_notes",
                    side_effect=[[], ["note.md"]],
                ),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=True),
                mock.patch.object(
                    ov_memory_sync,
                    "remote_write",
                    side_effect=RuntimeError("connection reset after server commit"),
                ) as remote_write,
                mock.patch.object(ov_memory_sync, "remote_read", return_value="changed") as remote_read,
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            self.assertEqual(remote_write.call_count, 1)
            remote_read.assert_called_once_with("note.md", strict=True)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["items"][0]["status"], "queued")
            self.assertNotIn("retry_attempt", saved["items"][0])

    def test_create_error_refresh_failure_enters_retry_without_second_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("initial", encoding="utf-8")
            initial_mtime = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 1:
                    note.write_text("changed", encoding="utf-8")
                    os.utime(note, ns=(initial_mtime + 1_000_000_000,) * 2)
                elif sleep_calls >= 2:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=3,
                backoff_base=0,
                backoff_max=0,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(
                    ov_memory_sync,
                    "list_remote_notes",
                    side_effect=[[], RuntimeError("HTTP 503 Service Unavailable")],
                ),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=True),
                mock.patch.object(
                    ov_memory_sync,
                    "remote_write",
                    side_effect=RuntimeError("connection reset after server commit"),
                ) as remote_write,
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            self.assertEqual(remote_write.call_count, 1)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["items"][0]["status"], "retry_wait")
            self.assertEqual(saved["items"][0]["retry_attempt"], 1)

    def test_create_error_readback_miss_enters_retry_without_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("initial", encoding="utf-8")
            initial_mtime = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 1:
                    note.write_text("changed", encoding="utf-8")
                    os.utime(note, ns=(initial_mtime + 1_000_000_000,) * 2)
                elif sleep_calls >= 2:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=3,
                backoff_base=0,
                backoff_max=0,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(
                    ov_memory_sync,
                    "list_remote_notes",
                    side_effect=[[], ["note.md"]],
                ),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=True),
                mock.patch.object(
                    ov_memory_sync,
                    "remote_write",
                    side_effect=RuntimeError("connection reset after server commit"),
                ) as remote_write,
                mock.patch.object(ov_memory_sync, "remote_read", return_value=None) as remote_read,
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            self.assertEqual(remote_write.call_count, 1)
            remote_read.assert_called_once_with("note.md", strict=True)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["items"][0]["status"], "retry_wait")
            self.assertEqual(saved["items"][0]["retry_attempt"], 1)

    def test_service_outage_persists_changed_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("initial", encoding="utf-8")
            initial_mtime = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 1:
                    note.write_text("changed", encoding="utf-8")
                    os.utime(note, ns=(initial_mtime + 1_000_000_000,) * 2)
                else:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=3,
                backoff_base=5,
                backoff_max=30,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(ov_memory_sync, "list_remote_notes", return_value=[]),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=False),
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            item = saved["items"][0]
            self.assertEqual(item["status"], "retry_wait")
            self.assertEqual(item["mtime_ns"], note.stat().st_mtime_ns)
            self.assertIn("next_retry_at", item)

    def test_restart_replays_durable_retry_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mirror = root / "mirror"
            mirror.mkdir()
            note = mirror / "note.md"
            note.write_text("changed", encoding="utf-8")
            mtime_ns = note.stat().st_mtime_ns
            state_path = root / "pending.json"
            state_path.write_text(json.dumps({"items": [{
                "name": "note.md",
                "status": "retry_wait",
                "retry_attempt": 1,
                "next_retry_at": "2000-01-01T00:00:00Z",
                "mtime_ns": mtime_ns,
            }]}), encoding="utf-8")
            sleep_calls = 0

            def fake_sleep(_interval: float) -> None:
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls >= 1:
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                mirror=str(mirror),
                interval=0.01,
                max_attempts=3,
                backoff_base=0,
                backoff_max=0,
            )
            with (
                mock.patch.object(ov_memory_sync, "STATE_PATH", state_path),
                mock.patch.object(ov_memory_sync, "list_remote_notes", return_value=[]),
                mock.patch.object(ov_memory_sync, "ov_reachable", return_value=True),
                mock.patch.object(ov_memory_sync, "remote_write", return_value={"task_id": "task-1"}) as remote_write,
                mock.patch.object(ov_memory_sync.time, "sleep", side_effect=fake_sleep),
            ):
                self.assertEqual(ov_memory_sync.cmd_watch(args), 0)

            remote_write.assert_called_once_with("note.md", "changed", exists=False)

    def test_runtime_sync_is_atomic_and_hash_checked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source.py"
            destination = root / "runtime" / "watcher.py"
            source.write_text("print('watcher')\n", encoding="utf-8")
            with (
                mock.patch.object(pm_system_watcher_runtime_sync, "SOURCE", source),
                mock.patch.object(pm_system_watcher_runtime_sync, "DESTINATION", destination),
            ):
                result = pm_system_watcher_runtime_sync.sync(backup_root=root / "backup")
            self.assertEqual(result["source_sha256"], result["after_sha256"])
            self.assertEqual(destination.read_text(encoding="utf-8"), "print('watcher')\n")
            self.assertEqual(result["production_state_touched"], False)


if __name__ == "__main__":
    unittest.main()
