from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_cockpit import CockpitReadModel  # noqa: E402
from pm_system_gateway import SemanticGateway  # noqa: E402
from pm_system_scheduler import AdmissionFrozen, Scheduler  # noqa: E402
from pm_system_store import PMSystemStore, canonical_status  # noqa: E402
from pm_resource_dispatcher import DispatchTimeoutError, PMResourceDispatcher  # noqa: E402


class V45R2ContractTests(unittest.TestCase):
    def test_v7_rebuilds_partially_upgraded_tables_and_removes_legacy_unique(self) -> None:
        """A crash after adding v7 columns must not preserve v6 global keys."""
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pm.db"
            PMSystemStore(path, max_schema_version=6)
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "INSERT INTO jobs(job_id,idempotency_key,job_type,run_id,status,priority,profile,payload_json,attempt,queued_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    ("job-old", "same-key", "run", "run-old", "queued", 50, "interactive", "{}", 0, "2026-08-30T00:00:00Z", "2026-08-30T00:00:00Z"),
                )
                connection.execute(
                    "INSERT INTO outbox_items(outbox_id,idempotency_key,resource_id,revision_id,processing_mode,provider,profile,payload_json,status,attempt,next_attempt_at,error_fingerprint,created_at,updated_at,retry_deadline_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("outbox-old", "same-key", "resource-old", "rev-old", "vectors_only", "openviking", "interactive", "{}", "pending", 0, None, None, "2026-08-30T00:00:00Z", "2026-08-30T00:00:00Z", None),
                )
                connection.execute(
                    "INSERT INTO semantic_tasks(semantic_task_id,dedupe_key,outbox_id,resource_id,revision_id,processing_mode,provider,status,attempt,openviking_task_id,error_fingerprint,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("semantic-old", "same-dedupe", "outbox-old", "resource-old", "rev-old", "semantic_and_vectors", "oneapi", "queued", 0, None, None, "2026-08-30T00:00:00Z", "2026-08-30T00:00:00Z"),
                )
                # Simulate a process that added v7 columns but stopped before
                # the table rebuild and migration marker.
                connection.execute("ALTER TABLE jobs ADD COLUMN namespace_epoch TEXT NOT NULL DEFAULT 'v4'")
                connection.execute("ALTER TABLE outbox_items ADD COLUMN kind TEXT NOT NULL DEFAULT 'resource'")
                connection.execute("ALTER TABLE outbox_items ADD COLUMN namespace_epoch TEXT NOT NULL DEFAULT 'v4'")
                connection.execute("ALTER TABLE outbox_items ADD COLUMN owner TEXT NOT NULL DEFAULT 'pm-system'")
                connection.execute("ALTER TABLE semantic_tasks ADD COLUMN profile TEXT NOT NULL DEFAULT 'pm-semantic'")
                connection.execute("ALTER TABLE semantic_tasks ADD COLUMN namespace_epoch TEXT NOT NULL DEFAULT 'v4'")
                connection.commit()

            store = PMSystemStore(path)
            with sqlite3.connect(path) as connection:
                for table, column in (("jobs", "idempotency_key"), ("outbox_items", "idempotency_key"), ("semantic_tasks", "dedupe_key")):
                    single_column_unique = []
                    for index in connection.execute(f"PRAGMA index_list({table})").fetchall():
                        if int(index[2]) != 1:
                            continue
                        index_columns = [row[2] for row in connection.execute(f"PRAGMA index_info({index[1]})").fetchall()]
                        if index_columns == [column]:
                            single_column_unique.append(index[1])
                    self.assertEqual(single_column_unique, [], (table, single_column_unique))
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM semantic_tasks").fetchone()[0], 1)

            first = store.accept({"job_type": "run", "loop_id": "partial", "idempotency_key": "same-key", "profile": "fast-vector", "namespace_epoch": "v4"})
            second = store.accept({"job_type": "run", "loop_id": "partial", "idempotency_key": "same-key", "profile": "pm-semantic", "namespace_epoch": "v5"})
            self.assertFalse(first["deduplicated"])
            self.assertFalse(second["deduplicated"])

    def test_v7_compatibility_repair_adds_provider_token_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pm.db"
            PMSystemStore(path)
            with sqlite3.connect(path) as connection:
                connection.execute("ALTER TABLE model_calls RENAME TO model_calls_partial")
                connection.execute(
                    "CREATE TABLE model_calls AS SELECT call_id,run_id,stage,attempt,status,model_input_hash,prompt_version,provider,started_at,completed_at,artifact_uri,error_fingerprint FROM model_calls_partial"
                )
                connection.execute("DROP TABLE model_calls_partial")
                connection.execute("UPDATE schema_migrations SET checksum=checksum WHERE version=7")
            PMSystemStore(path)
            with sqlite3.connect(path) as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(model_calls)")}
            self.assertIn("provider_token_id", columns)

    def test_watermark_cursor_rejects_replay_and_quarantines_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm.db")
            first = store.put_watermark(source_domain="source", watermark_name="knowledge", captured_at=10, sequence=1, value="k1", producer="test")
            same = store.put_watermark(source_domain="source", watermark_name="knowledge", captured_at=10, sequence=1, value="k1", producer="test")
            conflict = store.put_watermark(source_domain="source", watermark_name="knowledge", captured_at=10, sequence=1, value="k2", producer="test")
            old = store.put_watermark(source_domain="source", watermark_name="knowledge", captured_at=9, sequence=9, value="old", producer="test")
            self.assertEqual(first["outcome"], "accepted")
            self.assertEqual(same["outcome"], "idempotent")
            self.assertEqual(conflict["outcome"], "quarantine")
            self.assertEqual(old["outcome"], "replay_rejected")
            self.assertEqual(store.list_watermarks()[0]["value"], "k1")
            self.assertEqual([row["state"] for row in store.list_watermark_events()], ["replay_rejected", "quarantine", "idempotent", "accepted"])

    def test_memory_change_is_durable_and_not_claimed_by_resource_gateway(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm.db")
            event = store.enqueue_memory_change(name="MEMORY.md", mtime=1, content_hash="abc", file_path="/tmp/MEMORY.md")
            replay = store.enqueue_memory_change(name="MEMORY.md", mtime=2, content_hash="abc", file_path="/tmp/MEMORY.md")
            self.assertEqual(event["outbox_id"], replay["outbox_id"])
            with store.connect() as connection:
                row = connection.execute("SELECT kind,profile,status FROM outbox_items").fetchone()
            self.assertEqual(tuple(row), ("memory", "memory-skill", "pending"))
            self.assertEqual(SemanticGateway(store).dispatch_once(), [])

    def test_operation_ledger_and_failure_classification_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm.db")
            first = store.begin_operation(operation_type="add_resource", idempotency_key="k", request_hash="h")
            second = store.begin_operation(operation_type="add_resource", idempotency_key="k", request_hash="h")
            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            with self.assertRaises(ValueError):
                store.begin_operation(operation_type="add_resource", idempotency_key="k", request_hash="different")
            incomplete = store.classify_historical_failure(entity_type="outbox", entity_id="o1", original_status="failed", evidence={"owner": "pm"})
            complete = store.classify_historical_failure(entity_type="outbox", entity_id="o2", original_status="permanent_failed", evidence={key: key for key in ("artifact_uri", "model_input_hash", "provider", "error_fingerprint", "owner", "revision_id")})
            self.assertEqual(incomplete["failure_class"], "quarantine")
            self.assertEqual(complete["failure_class"], "replayable")
            self.assertEqual(canonical_status("permanent_failed"), "failed")

    def test_cockpit_uses_structured_watermark_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm.db")
            for name, value in (("source", "r1"), ("content", "c1"), ("knowledge", "k1"), ("active_generation", "g1")):
                store.put_watermark(source_domain="test", watermark_name=name, captured_at=1, sequence=1, value=value, producer="test")
            snapshot = CockpitReadModel(store).snapshot()
            self.assertEqual(snapshot["watermarks"]["knowledge"]["value"], "k1")

    def test_provider_token_limit_is_global_across_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp, patch.dict("os.environ", {"PM_V45_PROVIDER_MAX_CONCURRENCY": "1"}, clear=False):
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            scheduler = Scheduler(store, max_slots=2)
            first = store.accept({"job_type": "run", "loop_id": "r2", "idempotency_key": "run-1"})
            second = store.accept({"job_type": "run", "loop_id": "r2", "idempotency_key": "run-2"})
            scheduler.claim_next(worker_id="w1")
            scheduler.claim_next(worker_id="w2")
            call = scheduler.begin_model_call(first["run_id"], stage="analysis", model_input_hash="h1", prompt_version="v1", provider="oneapi", endpoint="ep", model="m")
            with self.assertRaises(AdmissionFrozen):
                scheduler.begin_model_call(second["run_id"], stage="analysis", model_input_hash="h2", prompt_version="v1", provider="oneapi", endpoint="ep", model="m")
            scheduler.finish_model_call(call["call_id"], status="completed")
            released = scheduler.begin_model_call(second["run_id"], stage="analysis", model_input_hash="h2", prompt_version="v1", provider="oneapi", endpoint="ep", model="m")
            self.assertTrue(released["provider_token_id"])
            with store.connect() as connection:
                active = connection.execute(
                    "SELECT COUNT(*) FROM provider_tokens WHERE provider=? AND endpoint=? AND model=? AND released_at IS NULL",
                    ("oneapi", "ep", "m"),
                ).fetchone()[0]
            self.assertEqual(active, 1)

    def test_unknown_add_response_allows_one_controlled_resend_then_stops(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.md"
            source.write_text("report", encoding="utf-8")
            store = PMSystemStore(root / "pm.db")

            class AddUnknownOnceTransport:
                url = "http://fake-openviking"

                def __init__(self) -> None:
                    self.upload_count = 0
                    self.add_count = 0

                def upload_file(self, path, *, timeout=None):
                    self.upload_count += 1
                    return {"result": {"temp_file_id": "temp-1"}}

                def add_resource(self, body, *, timeout=None, idempotency_key=None):
                    self.add_count += 1
                    if self.add_count == 1:
                        raise DispatchTimeoutError("response lost")
                    return {"status": "completed", "task_id": "task-1"}

                def get_task(self, task_id, *, timeout=None):
                    return {"status": "completed", "task_id": task_id}

            transport = AddUnknownOnceTransport()
            dispatcher = PMResourceDispatcher(store, transport=transport, artifact_root=root / "artifacts")
            dispatcher.submit_file(path=source, target_uri="viking://resources/project-docs/r2-retry")
            first = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(first[0]["status"], "retry_wait")
            with store.transaction() as connection:
                connection.execute("UPDATE outbox_items SET next_attempt_at='2000-01-01T00:00:00Z'")
            second = dispatcher.dispatch_pending(limit=1)
            self.assertEqual(second[0]["status"], "completed")
            self.assertEqual(transport.add_count, 2)
            self.assertEqual(transport.upload_count, 1)
            with store.connect() as connection:
                rows = connection.execute(
                    "SELECT operation_type,response_state,attempt,request_hash,namespace_epoch FROM operation_ledger ORDER BY operation_type,attempt"
                ).fetchall()
            self.assertEqual([(row[0], row[1], row[2]) for row in rows], [("add_resource", "unknown", 1), ("add_resource", "completed", 2), ("temp_upload", "accepted", 1)])
            self.assertTrue(rows[0][3])
            self.assertEqual({row[4] for row in rows}, {"v4"})


if __name__ == "__main__":
    unittest.main()
