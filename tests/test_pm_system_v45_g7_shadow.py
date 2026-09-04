from __future__ import annotations

import tempfile
import time
import unittest
import json
import sqlite3
from pathlib import Path
from unittest import mock

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import pm_system_v45_g7_shadow as ov_memory_sync  # noqa: E402
from pm_system_v45_g7_shadow import (  # noqa: E402
    MIN_RESOURCE_BYTES,
    _read_queue_processing_starts,
    _timestamp_from_payload,
    collect_profile,
    update_manifest,
)


class V45G7ShadowTests(unittest.TestCase):
    def test_manifest_merge_is_profile_scoped_and_hashes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shadow.json"
            first = {"profile": "fast-vector", "sample_count": 1000, "duration_seconds": 1800}
            manifest = update_manifest(path, first, namespace="viking://resources/v45-r2-g7-shadow")
            self.assertEqual(manifest["profiles"]["fast-vector"]["sample_count"], 1000)
            self.assertTrue(manifest["manifest_sha256"].startswith("sha256:"))
            self.assertEqual(manifest["minimums"]["policy"], "all")
            second = {"profile": "pm-semantic", "sample_count": 0, "duration_seconds": 1}
            manifest = update_manifest(path, second, namespace="viking://resources/v45-r2-g7-shadow")
            self.assertEqual(set(manifest["profiles"]), {"fast-vector", "pm-semantic"})

    def test_namespace_must_be_dedicated_shadow_prefix(self) -> None:
        with self.assertRaises(ValueError):
            collect_profile(
                profile="fast-vector",
                processing_mode="vectors_only",
                namespace="viking://resources/project-docs/unsafe",
                sample_count=0,
                duration_seconds=1,
                source_size=MIN_RESOURCE_BYTES,
                submit_workers=1,
            )
        self.assertEqual(MIN_RESOURCE_BYTES, 10 * 1024)

    def test_queue_timestamp_requires_explicit_dequeue_or_worker_start(self) -> None:
        timestamp, source = _timestamp_from_payload({"result": {"status": "running", "updated_at": 123.0}})
        self.assertIsNone(timestamp)
        self.assertIsNone(source)
        timestamp, source = _timestamp_from_payload({"result": {"worker_started_at": "2026-08-30T00:00:01Z"}})
        self.assertEqual(timestamp, 1788048001.0)
        self.assertEqual(source, "worker_started_at")

    def test_queue_db_processing_start_is_matched_by_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "queue.db"
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "CREATE TABLE queue_messages ("
                    "id INTEGER PRIMARY KEY, queue_name TEXT, message_id TEXT, data TEXT, "
                    "timestamp INTEGER, status TEXT, processing_started_at INTEGER, created_at INTEGER)"
                )
                payload = {"task_id": "task-1", "root_uri": "viking://resources/v45-r2-g7-shadow/task/1"}
                connection.execute(
                    "INSERT INTO queue_messages "
                    "(queue_name,message_id,data,timestamp,status,processing_started_at,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    ("AddResource", "m-1", json.dumps({"data": list(json.dumps(payload).encode("utf-8"))}), 100, "processing", 101, 100),
                )
            starts, errors, rows, unmatched = _read_queue_processing_starts(
                path, namespace="viking://resources/v45-r2-g7-shadow", profile="task"
            )
        self.assertEqual(errors, [])
        self.assertEqual(rows, 1)
        self.assertEqual(unmatched, 0)
        self.assertEqual(starts["task-1"]["processing_started_at"], 101.0)
        self.assertEqual(starts["task-1"]["source"], "queue_db.processing_started_at")

    def test_queue_db_missing_row_does_not_invent_wait(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "queue.db"
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "CREATE TABLE queue_messages ("
                    "id INTEGER PRIMARY KEY, queue_name TEXT, message_id TEXT, data TEXT, "
                    "timestamp INTEGER, status TEXT, processing_started_at INTEGER, created_at INTEGER)"
                )
            starts, errors, rows, unmatched = _read_queue_processing_starts(path)
        self.assertEqual(starts, {})
        self.assertEqual(errors, [])
        self.assertEqual(rows, 0)
        self.assertEqual(unmatched, 0)

    def test_queue_db_unmatched_payload_stays_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "queue.db"
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "CREATE TABLE queue_messages ("
                    "id INTEGER PRIMARY KEY, queue_name TEXT, message_id TEXT, data TEXT, "
                    "timestamp INTEGER, status TEXT, processing_started_at INTEGER, created_at INTEGER)"
                )
                connection.execute(
                    "INSERT INTO queue_messages "
                    "(queue_name,message_id,data,timestamp,status,processing_started_at,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        "Semantic",
                        "m-2",
                        json.dumps(
                            {
                                "data": list(
                                    json.dumps(
                                        {
                                            "task_id": "task-unknown",
                                            "root_uri": "viking://resources/other-shadow/profile/task",
                                        }
                                    ).encode("utf-8")
                                )
                            }
                        ),
                        100,
                        "processing",
                        101,
                        100,
                    ),
                )
            starts, errors, rows, unmatched = _read_queue_processing_starts(
                path, namespace="viking://resources/v45-r2-g7-shadow", profile="fast-vector"
            )
        self.assertEqual(starts, {})
        self.assertEqual(errors, [])
        self.assertEqual(rows, 1)
        self.assertEqual(unmatched, 1)

    def test_collection_polls_while_submitting(self) -> None:
        class FakeOpenViking:
            def __init__(self) -> None:
                self.counter = 0

            def upload_file(self, source: Path, timeout: int) -> dict:
                return {"result": {"temp_file_id": "tmp"}}

            def request(self, method: str, path: str, body: dict | None = None, timeout: int = 0) -> dict:
                if method == "POST":
                    self.counter += 1
                    return {"result": {"task_id": f"task-{self.counter}"}}
                return {"result": {"status": "completed"}}

        clock = {"now": 100.0}
        real_sleep = time.sleep

        def fake_sleep(seconds: float) -> None:
            real_sleep(0.001)
            clock["now"] += seconds

        def fake_snapshot() -> dict:
            return {"rss_mb": 100.0, "wal_bytes": 10.0, "errors": []}

        with mock.patch.object(ov_memory_sync.time, "time", side_effect=lambda: clock["now"]), mock.patch.object(
            ov_memory_sync.time, "sleep", side_effect=fake_sleep
        ), mock.patch.object(ov_memory_sync, "_load_ov_rest", return_value=FakeOpenViking()), mock.patch.object(
            ov_memory_sync, "_host_snapshot", side_effect=fake_snapshot
        ):
            result = ov_memory_sync.collect_profile(
                profile="fast-vector",
                processing_mode="vectors_only",
                namespace="viking://resources/v45-r2-g7-shadow",
                sample_count=2,
                duration_seconds=5,
                source_size=MIN_RESOURCE_BYTES,
                submit_workers=1,
            )

        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["accepted_count"], 2)
        self.assertLessEqual(result["metrics"]["content_verified_p99_s"], 1.0)

    def test_partial_task_result_does_not_become_probe_error(self) -> None:
        class PartialOpenViking:
            def __init__(self) -> None:
                self.counter = 0

            def upload_file(self, source: Path, timeout: int) -> dict:
                return {"result": {"temp_file_id": "tmp"}}

            def request(self, method: str, path: str, body: dict | None = None, timeout: int = 0) -> dict:
                if method == "POST":
                    self.counter += 1
                    return {"result": {"task_id": f"partial-{self.counter}"}}
                return {
                    "result": {
                        "status": "completed",
                        "stage": "completed",
                        "result": {"queue_status": ["malformed"], "usage": None},
                    }
                }

        clock = {"now": 100.0}

        def fake_sleep(seconds: float) -> None:
            clock["now"] += seconds

        with mock.patch.object(ov_memory_sync.time, "time", side_effect=lambda: clock["now"]), mock.patch.object(
            ov_memory_sync.time, "sleep", side_effect=fake_sleep
        ), mock.patch.object(ov_memory_sync, "_load_ov_rest", return_value=PartialOpenViking()), mock.patch.object(
            ov_memory_sync, "_host_snapshot", return_value={"rss_mb": 100.0, "wal_bytes": 10.0, "errors": []}
        ):
            result = ov_memory_sync.collect_profile(
                profile="fast-vector",
                processing_mode="vectors_only",
                namespace="viking://resources/v45-r2-g7-shadow",
                sample_count=1,
                duration_seconds=1,
                source_size=MIN_RESOURCE_BYTES,
                submit_workers=1,
            )

        self.assertEqual(result["sample_count"], 1)
        self.assertNotIn("probe_error:AttributeError", result["last_status_sample"].values())
        self.assertIn("invalid_queue_status", result["queue_observation"]["errors"])


if __name__ == "__main__":
    unittest.main()
