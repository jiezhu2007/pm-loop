from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_memory_dispatcher import MemorySkillWriter  # noqa: E402
from pm_resource_dispatcher import DispatchHTTPError, DispatchTimeoutError, OpenVikingTransport, PMResourceDispatcher  # noqa: E402
from pm_system_store import PMSystemStore, now_iso  # noqa: E402


class FakeMemoryTransport:
    def __init__(self, *, write_response=None, read_content="hello"):
        self.write_response = write_response or {"status": "ok"}
        self.read_content_value = read_content
        self.writes = []
        self.tasks = {}
        self.reads = []
        self.write_calls = 0

    def write_content(self, target_uri, content, **kwargs):
        self.write_calls += 1
        self.writes.append((target_uri, content, dict(kwargs)))
        return self.write_response

    def read_content(self, target_uri, **_kwargs):
        self.reads.append(target_uri)
        return {"status": "ok", "result": {"content": self.read_content_value}}

    def get_task(self, task_id, **_kwargs):
        value = self.tasks.get(task_id)
        if isinstance(value, Exception):
            raise value
        return value or {"status": "processing"}


class UnknownThenReadbackTransport(FakeMemoryTransport):
    def write_content(self, target_uri, content, **kwargs):
        self.write_calls += 1
        self.writes.append((target_uri, content, dict(kwargs)))
        raise DispatchTimeoutError("write response lost")


class MemorySkillWriterTests(unittest.TestCase):
    def test_transport_uses_content_write_schema_only(self):
        transport = OpenVikingTransport(url="http://127.0.0.1:1933")
        captured = {}

        def request(method, path, *, data=None, headers=None, timeout=None):
            captured.update(method=method, path=path, body=json.loads(data.decode("utf-8")), headers=headers, timeout=timeout)
            return {"status": "accepted", "task_id": "task"}

        with mock.patch.object(transport, "_request", side_effect=request):
            transport.write_content(
                "viking://resources/memory/note.md/note.md",
                "hello",
                mode="replace",
                processing_mode="vectors_only",
                wait=False,
                idempotency_key="memory-key",
            )
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v1/content/write")
        self.assertEqual(set(captured["body"]), {"uri", "content", "mode", "wait", "processing_mode"})
        self.assertEqual(captured["body"]["mode"], "replace")
        self.assertEqual(captured["body"]["processing_mode"], "vectors_only")
        self.assertFalse(captured["body"]["wait"])

    def test_transport_uses_fs_stat_schema(self):
        transport = OpenVikingTransport(url="http://127.0.0.1:1933")
        captured = {}

        def request(method, path, *, data=None, headers=None, timeout=None):
            captured.update(method=method, path=path, data=data, headers=headers, timeout=timeout)
            return {"status": "ok", "result": {"isDir": True}}

        with mock.patch.object(transport, "_request", side_effect=request):
            transport.stat_uri("viking://resources/memory/note.md", timeout=1)
        self.assertEqual(captured["method"], "GET")
        self.assertTrue(captured["path"].startswith("/api/v1/fs/stat?uri="))

    def test_transport_uses_fs_mkdir_schema(self):
        transport = OpenVikingTransport(url="http://127.0.0.1:1933")
        captured = {}

        def request(method, path, *, data=None, headers=None, timeout=None):
            captured.update(method=method, path=path, body=json.loads(data.decode("utf-8")), headers=headers, timeout=timeout)
            return {"status": "ok"}

        with mock.patch.object(transport, "_request", side_effect=request):
            transport.mkdir("viking://resources/memory/note.md", timeout=1)
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v1/fs/mkdir")
        self.assertEqual(captured["body"], {"uri": "viking://resources/memory/note.md"})

    def _setup(self, content="hello", transport=None):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        source = root / "note.md"
        source.write_text(content, encoding="utf-8")
        store = PMSystemStore(root / "pm.db")
        writer = MemorySkillWriter(store, transport=transport or FakeMemoryTransport(read_content=content), observation_backoff_seconds=0)
        writer.submit_file(path=source)
        return temp, root, source, store, writer

    def test_content_write_is_strict_and_readback_completes(self):
        temp, _root, _source, store, writer = self._setup()
        try:
            result = writer.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "completed")
            call = writer.transport.writes[0]
            self.assertEqual(call[2], {"mode": "replace", "processing_mode": "vectors_only", "wait": False, "timeout": 30.0, "idempotency_key": call[2]["idempotency_key"]})
            with store.connect() as connection:
                outbox = connection.execute("SELECT kind,profile,status FROM outbox_items").fetchone()
                projection = connection.execute("SELECT content_state,local_hash,remote_hash,verified_at FROM memory_projections").fetchone()
                tasks = connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0]
            self.assertEqual(tuple(outbox), ("memory", "memory-skill", "completed"))
            self.assertEqual(projection[0], "completed")
            self.assertEqual(projection[1], hashlib.sha256(b"hello").hexdigest())
            self.assertEqual(projection[1], projection[2])
            self.assertTrue(projection[3])
            self.assertEqual(tasks, 0)
        finally:
            temp.cleanup()

    def test_accepted_task_is_reconciled_without_resubmission(self):
        transport = FakeMemoryTransport(write_response={"status": "accepted", "task_id": "memory-task-1"})
        temp, _root, _source, store, writer = self._setup(transport=transport)
        try:
            first = writer.dispatch_pending(limit=1)
            self.assertEqual(first[0]["status"], "awaiting_task")
            transport.tasks["memory-task-1"] = {"status": "completed"}
            observed = writer.reconcile_tasks(limit=1)
            self.assertEqual(observed[0]["status"], "completed")
            self.assertEqual(transport.write_calls, 1)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM outbox_items").fetchone()[0], "completed")
        finally:
            temp.cleanup()

    def test_unknown_write_response_uses_readback_before_retry(self):
        temp, _root, _source, store, writer = self._setup(transport=UnknownThenReadbackTransport(read_content="hello"))
        try:
            result = writer.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "completed")
            self.assertEqual(writer.transport.write_calls, 1)
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM outbox_items").fetchone()[0], "completed")
        finally:
            temp.cleanup()

    def test_missing_memory_parent_is_created_then_write_retried_once(self):
        class MissingParentTransport(FakeMemoryTransport):
            def __init__(self):
                super().__init__(read_content="hello")
                self.mkdir_calls = []
                self.list_calls = []

            def write_content(self, target_uri, content, **kwargs):
                self.write_calls += 1
                self.writes.append((target_uri, content, dict(kwargs)))
                if self.write_calls == 1:
                    raise DispatchHTTPError(404, "Not Found", body="parent directory does not exist")
                return {"status": "ok"}

            def list_uri(self, target_uri, **_kwargs):
                self.list_calls.append(target_uri)
                raise DispatchHTTPError(404, "Not Found")

            def mkdir(self, target_uri, **_kwargs):
                self.mkdir_calls.append(target_uri)
                return {"status": "ok"}

        transport = MissingParentTransport()
        temp, _root, _source, store, writer = self._setup(transport=transport)
        try:
            result = writer.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "completed")
            self.assertEqual(transport.list_calls, ["viking://resources/memory/note.md"])
            self.assertEqual(transport.mkdir_calls, ["viking://resources/memory/note.md"])
            self.assertEqual(transport.write_calls, 2)
            self.assertEqual(transport.writes[1][2]["mode"], "create")
        finally:
            temp.cleanup()

    def test_existing_memory_parent_does_not_retry_other_404(self):
        class ExistingParentTransport(FakeMemoryTransport):
            def __init__(self):
                super().__init__(read_content="hello")
                self.mkdir_calls = []

            def write_content(self, target_uri, content, **kwargs):
                self.write_calls += 1
                raise DispatchHTTPError(404, "Not Found", body="unexpected write failure")

            def stat_uri(self, target_uri, **_kwargs):
                return {"status": "ok", "result": {"isDir": False}}

            def list_uri(self, target_uri, **_kwargs):
                return {"status": "ok", "result": []}

            def mkdir(self, target_uri, **_kwargs):
                self.mkdir_calls.append(target_uri)

        transport = ExistingParentTransport()
        temp, _root, _source, store, writer = self._setup(transport=transport)
        try:
            result = writer.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "failed")
            self.assertEqual(result[0]["terminal_reason"], "permanent")
            self.assertEqual(transport.write_calls, 1)
            self.assertEqual(transport.mkdir_calls, [])
        finally:
            temp.cleanup()

    def test_task_404_is_quarantined(self):
        transport = FakeMemoryTransport(write_response={"status": "accepted", "task_id": "gone"})
        transport.tasks["gone"] = DispatchHTTPError(404, "Not Found")
        temp, _root, _source, store, writer = self._setup(transport=transport)
        try:
            writer.dispatch_pending(limit=1)
            observed = writer.reconcile_tasks(limit=1)
            self.assertEqual(observed[0]["status"], "quarantine")
            with store.connect() as connection:
                outbox = connection.execute("SELECT status,terminal_reason FROM outbox_items").fetchone()
                projection = connection.execute("SELECT content_state,terminal_reason FROM memory_projections").fetchone()
            self.assertEqual(tuple(outbox), ("quarantine", "task_not_found"))
            self.assertEqual(tuple(projection), ("quarantine", "task_not_found"))
        finally:
            temp.cleanup()

    def test_older_revision_is_quarantined_when_newer_edit_is_admitted(self):
        temp, root, source, store, writer = self._setup(content="first")
        try:
            source.write_text("second", encoding="utf-8")
            writer.submit_file(path=source)
            result = writer.dispatch_pending(limit=1)
            self.assertEqual(result[0]["status"], "quarantine")
            self.assertEqual(result[0]["terminal_reason"], "superseded_revision")
            with store.connect() as connection:
                rows = connection.execute("SELECT revision_id,status,terminal_reason FROM outbox_items ORDER BY created_at").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][1], "quarantine")
            self.assertEqual(rows[0][2], "superseded_revision")
            self.assertEqual(rows[1][1], "pending")
        finally:
            temp.cleanup()

    def test_resource_dispatcher_does_not_claim_memory(self):
        temp, _root, _source, store, _writer = self._setup()
        try:
            resource = PMResourceDispatcher(store, transport=FakeMemoryTransport())
            self.assertEqual(resource.dispatch_pending(limit=1), [])
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT status FROM outbox_items").fetchone()[0], "pending")
        finally:
            temp.cleanup()


if __name__ == "__main__":
    unittest.main()
