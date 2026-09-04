from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "scripts"))

from pm_resource_dispatcher import (  # noqa: E402
    DispatchHTTPError,
    DispatchTimeoutError,
    OpenVikingTransport,
    PMResourceDispatcher,
    _walk_status,
)
from pm_system_store import PMSystemStore  # noqa: E402


class FakeTransport:
    def __init__(self, response=None, error=None) -> None:
        self.url = "http://fake-openviking"
        self.response = response or {"status": "accepted", "task_id": "task-1"}
        self.error = error
        self.uploads = []
        self.bodies = []
        self.tasks = {}

    def upload_file(self, path, *, timeout=None):
        self.uploads.append((Path(path), timeout))
        if self.error:
            raise self.error
        return {"result": {"temp_file_id": "temp-1"}}

    def add_resource(self, body, *, timeout=None, idempotency_key=None):
        self.bodies.append((dict(body), timeout))
        if self.error:
            raise self.error
        return self.response

    def get_task(self, task_id, *, timeout=None):
        return self.tasks.get(str(task_id), {"status": "running"})


class NotFoundTaskTransport(FakeTransport):
    def get_task(self, task_id, *, timeout=None):
        raise DispatchHTTPError(404, "Not Found")


class PMResourceDispatcherTests(unittest.TestCase):
    def test_nested_task_status_beats_generic_ok_envelope(self) -> None:
        self.assertEqual(
            _walk_status({"status": "ok", "result": {"status": "processing", "task_id": "task-2"}}),
            ("processing", "task-2"),
        )

    def test_file_submission_is_outbox_backed_and_reasonless(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport)
            result = dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            self.assertEqual(result["status"], "queued")
            self.assertEqual(len(transport.uploads), 0)
            self.assertEqual(len(transport.bodies), 0)
            with store.connect() as connection:
                row = connection.execute("SELECT status,payload_json FROM outbox_items").fetchone()
            self.assertEqual(row[0], "pending")
            payload = json.loads(row[1])
            self.assertNotIn("reason", json.dumps(payload))
            durable_source = Path(payload["file_path"])
            self.assertTrue(durable_source.is_file())

            dispatched = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(dispatched[0]["status"], "completed")
            self.assertEqual(len(transport.uploads), 1)
            self.assertEqual(len(transport.bodies), 1)
            self.assertNotIn("reason", json.dumps(transport.bodies[0][0]))
            with store.connect() as connection:
                status = connection.execute("SELECT status FROM outbox_items").fetchone()[0]
                semantic_status = connection.execute("SELECT status FROM semantic_tasks").fetchone()[0]
            self.assertEqual(status, "completed")
            self.assertEqual(semantic_status, "accepted")

            replay = dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            self.assertTrue(replay["deduplicated"])
            self.assertEqual(len(transport.uploads), 1)

    def test_explicit_wait_strict_dispatches_and_waits_without_duplicate_submit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "strict-report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(response={"status": "accepted", "task_id": "task-strict"})
            transport.tasks["task-strict"] = {"status": "completed", "task_id": "task-strict"}
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")

            result = dispatcher.submit_file(
                path=source,
                target_uri="viking://resources/project-docs/strict-report",
                wait=True,
                strict=True,
                timeout=1,
            )

            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["strict_status"], "completed")
            self.assertEqual(len(transport.uploads), 1)
            self.assertEqual(len(transport.bodies), 1)
            self.assertTrue(transport.bodies[0][0]["wait"])
            self.assertTrue(transport.bodies[0][0]["strict"])
            with store.connect() as connection:
                row = connection.execute("SELECT status FROM semantic_tasks").fetchone()
            self.assertEqual(row[0], "completed")

    def test_explicit_strict_timeout_does_not_resubmit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "strict-timeout.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(response={"status": "accepted", "task_id": "task-running"})
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")

            result = dispatcher.submit_file(
                path=source,
                target_uri="viking://resources/project-docs/strict-timeout",
                strict=True,
                timeout=0.1,
            )

            self.assertEqual(result["strict_status"], "timeout")
            self.assertEqual(len(transport.uploads), 1)
            self.assertEqual(len(transport.bodies), 1)
            replay = dispatcher.submit_file(
                path=source,
                target_uri="viking://resources/project-docs/strict-timeout",
                strict=True,
                timeout=0.1,
            )
            self.assertTrue(replay["deduplicated"])
            self.assertEqual(len(transport.uploads), 1)

    def test_429_is_recorded_in_shared_gateway_without_incrementing_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(error=DispatchHTTPError(429, "Too Many Requests", retry_after="60"))
            dispatcher = PMResourceDispatcher(store, transport=transport)
            result = dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            self.assertEqual(result["status"], "queued")
            dispatcher.dispatch_pending(limit=1)
            with store.connect() as connection:
                row = connection.execute("SELECT status,attempt FROM outbox_items").fetchone()
            self.assertEqual(tuple(row), ("retry_wait", 0))

    def test_native_timeout_is_retryable_transport_failure(self) -> None:
        transport = OpenVikingTransport(url="http://fake-openviking")
        with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("socket timeout")):
            with self.assertRaises(DispatchTimeoutError):
                transport._request("GET", "/health", timeout=1)

    def test_native_timeout_moves_outbox_to_retry_wait(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(error=DispatchTimeoutError("socket timeout"))
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            result = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "retry_wait")
            with store.connect() as connection:
                row = connection.execute("SELECT status,attempt FROM outbox_items").fetchone()
            self.assertEqual(tuple(row), ("retry_wait", 1))

    def test_multipart_upload_encodes_non_ascii_filename_in_header(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "V4.4升级报告.md"
            source.write_text("ascii body", encoding="utf-8")
            transport = OpenVikingTransport(url="http://fake-openviking")
            with mock.patch.object(transport, "_request", return_value={}) as request:
                transport.upload_file(source, timeout=1)
            body = request.call_args.kwargs["data"]
            self.assertIn(b'filename="V4.4.md"', body)
            self.assertIn(b"filename*=UTF-8''V4.4%E5%8D%87%E7%BA%A7%E6%8A%A5%E5%91%8A.md", body)
            self.assertNotIn("升级报告".encode("utf-8"), body.split(b"\r\n\r\n", 1)[0])

    def test_non_ascii_idempotency_key_is_hashed_for_http_header(self) -> None:
        transport = OpenVikingTransport(url="http://fake-openviking")
        with mock.patch.object(transport, "_request", return_value={}) as request:
            transport.add_resource(
                {"to": "viking://resources/project-docs/升级报告.md"},
                timeout=1,
                idempotency_key="viking://resources/project-docs/升级报告.md|revision|vectors_only|openviking",
            )
        header = request.call_args.kwargs["headers"]["Idempotency-Key"]
        self.assertTrue(header.startswith("pm-v44-"))
        header.encode("ascii")

    def test_batch_dispatch_stops_after_429_and_preserves_throttle(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(error=DispatchHTTPError(429, "Too Many Requests", retry_after="60"))
            dispatcher = PMResourceDispatcher(store, transport=transport)
            for index in range(3):
                dispatcher.submit_file(
                    path=source,
                    target_uri=f"viking://resources/project-docs/report-{index}",
                    processing_mode="semantic_and_vectors",
                )
            results = dispatcher.dispatch_pending(limit=3)
            self.assertEqual(len(results), 1)
            self.assertEqual(len(transport.uploads), 1)
            with store.connect() as connection:
                bucket = connection.execute("SELECT circuit_state,throttle_until FROM provider_buckets").fetchone()
            self.assertEqual(bucket[0], "closed")
            self.assertIsNotNone(bucket[1])

    def test_async_dispatch_survives_cleanup_of_callers_temporary_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "temporary-report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            accepted = dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            source.unlink()
            result = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(accepted["status"], "queued")
            self.assertEqual(result[0]["status"], "completed")
            self.assertEqual(transport.uploads[0][0].read_text(encoding="utf-8"), "report")

    def test_directory_root_readback_resolves_revision_named_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "semantic-report.md"
            source.write_text("semantic source", encoding="utf-8")
            revision = hashlib.sha256(source.read_bytes()).hexdigest()
            target = "viking://resources/project-docs/semantic-report.md"
            leaf = f"{target}/{revision[:40]}_chunk.md"

            class DirectoryTransport(FakeTransport):
                def __init__(self) -> None:
                    super().__init__(response={"status": "completed", "task_id": "task-directory"})
                    self.reads = []
                    self.listings = []

                def read_content(self, target_uri, *, timeout=None):
                    self.reads.append(target_uri)
                    if target_uri == target:
                        raise DispatchHTTPError(
                            400,
                            "Bad Request",
                            body=f"Directory URI is not readable as a file: {target}",
                        )
                    if target_uri == leaf:
                        return {"status": "ok", "result": "semantic projection"}
                    raise AssertionError(target_uri)

                def list_uri(self, target_uri, *, timeout=None):
                    self.listings.append(target_uri)
                    self.assert_target = target_uri
                    return {"status": "ok", "result": [{"uri": leaf, "isDir": False}]}

            store = PMSystemStore(root / "pm-system.db")
            transport = DirectoryTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(
                path=source,
                target_uri=target,
                processing_mode="semantic_and_vectors",
            )
            result = dispatcher.dispatch_pending(limit=1)

            read_back = result[0]["content_read_back"]
            self.assertTrue(read_back["verified"])
            self.assertEqual(read_back["status"], "verified_directory_leaf")
            self.assertEqual(read_back["verification_mode"], "source_hash_prefix_and_leaf_read")
            self.assertEqual(read_back["resolved_uri"], leaf)
            self.assertEqual(read_back["source_hash_prefix"], revision[:40])
            self.assertEqual(transport.listings, [target])
            self.assertEqual(transport.reads, [target, leaf])
            with store.connect() as connection:
                projection = connection.execute(
                    "SELECT content_state,semantic_state FROM resource_projections"
                ).fetchone()
                operation = connection.execute(
                    "SELECT response_state,response_json FROM operation_ledger WHERE operation_type='content_read_back'"
                ).fetchone()
            self.assertEqual(tuple(projection), ("content_verified", "semantic_completed"))
            self.assertEqual(operation[0], "completed")
            self.assertEqual(json.loads(operation[1])["resolved_uri"], leaf)

    def test_accepted_task_is_observed_without_resubmission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(response={"status": "accepted", "task_id": "task-1"})
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            dispatcher.dispatch_pending(limit=1)
            transport.tasks["task-1"] = {"status": "completed"}
            observed = dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            self.assertEqual(observed[0]["status"], "completed")
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM semantic_tasks").fetchone()[0], "completed")
                projection = connection.execute(
                    "SELECT content_state,semantic_state,verified_at,semantic_completed_at,terminal_reason FROM resource_projections"
                ).fetchone()
            self.assertEqual(projection[0:2], ("content_verified", "semantic_completed"))
            self.assertTrue(projection[2])
            self.assertTrue(projection[3])
            self.assertIsNone(projection[4])
            self.assertEqual(len(transport.uploads), 1)

    def test_active_response_without_task_id_is_not_acknowledged(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "missing-task-id.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(response={"status": "accepted"})
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/missing-task-id")
            result = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "failed")
            with store.connect() as connection:
                outbox_status, task_status = connection.execute(
                    "SELECT o.status,t.status FROM outbox_items AS o JOIN semantic_tasks AS t ON t.outbox_id=o.outbox_id"
                ).fetchone()
            self.assertEqual(outbox_status, "failed")
            self.assertEqual(task_status, "failed")
            self.assertEqual(len(transport.uploads), 1)

    def test_missing_remote_task_becomes_quarantine_without_resubmission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = NotFoundTaskTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            dispatcher.dispatch_pending(limit=1)
            observed = dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            self.assertEqual(observed[0]["status"], "quarantine")
            self.assertEqual(len(transport.uploads), 1)
            with store.connect() as connection:
                task = connection.execute("SELECT status,error_fingerprint FROM semantic_tasks").fetchone()
                observation = connection.execute("SELECT observation_attempt,last_error_fingerprint FROM semantic_task_observations").fetchone()
            self.assertEqual(task[0], "quarantine")
            self.assertTrue(task[1])
            self.assertEqual(observation[0], 1)
            self.assertEqual(observation[1], task[1])

    def test_unknown_remote_status_exhausts_observation_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm-system.db")
            transport = FakeTransport(response={"status": "accepted", "task_id": "task-1"})
            transport.tasks["task-1"] = {"status": "provider-internal-state"}
            dispatcher = PMResourceDispatcher(
                store,
                transport=transport,
                artifact_root=root / "artifacts",
                observation_max_attempts=2,
                observation_backoff_seconds=0,
            )
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/report")
            dispatcher.dispatch_pending(limit=1)
            first = dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            second = dispatcher.reconcile_tasks(limit=1, min_age_seconds=0)
            self.assertEqual(first[0]["status"], "accepted")
            self.assertEqual(second[0]["status"], "quarantine")
            with store.connect() as connection:
                task = connection.execute("SELECT status FROM semantic_tasks").fetchone()[0]
                observation = connection.execute("SELECT observation_attempt,next_attempt_at FROM semantic_task_observations").fetchone()
            self.assertEqual(task, "quarantine")
            self.assertEqual(observation[0], 2)
            self.assertIsNone(observation[1])


if __name__ == "__main__":
    unittest.main()
