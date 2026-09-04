from __future__ import annotations

import asyncio
import datetime
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


try:
    from openviking.storage.queuefs.semantic_processor import SemanticProcessor
    from openviking.storage.collection_schemas import TextEmbeddingHandler
    from openviking.storage.queuefs.embedding_msg import EmbeddingMsg
    from openviking.utils.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerOpen,
        _STATE_HALF_OPEN,
    )
    from openviking.utils.model_retry import (
        ERROR_CLASS_INVALID_RESOURCE,
        ERROR_CLASS_TRANSIENT,
        classify_api_error,
        is_retryable_rate_limit_error,
        rate_limit_retry_delay,
        retry_after_seconds,
    )
    from openviking.storage.queuefs.semantic_msg import SemanticMsg
    from openviking_cli.exceptions import NotFoundError
    from openviking.storage.errors import LockAcquisitionError
except Exception as exc:  # pragma: no cover - depends on the pinned runtime
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


@unittest.skipIf(_IMPORT_ERROR is not None, f"OpenViking runtime unavailable: {_IMPORT_ERROR}")
class OpenVikingRuntimeReliabilityTests(unittest.TestCase):
    def test_not_found_resource_is_not_transient(self) -> None:
        error = NotFoundError("viking://resources/skills/missing/SKILL.md", "directory")
        self.assertEqual(classify_api_error(error), ERROR_CLASS_INVALID_RESOURCE)

    def test_invalid_resource_is_terminal_and_not_requeued(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        events: list[tuple[str, str]] = []
        processor.set_callbacks(
            on_success=lambda: events.append(("success", "")),
            on_requeue=lambda: events.append(("requeue", "")),
            on_error=lambda message, _data: events.append(("error", message)),
        )
        message = {
            "data": json.dumps(
                {
                    "id": "semantic-message-1",
                    "uri": "viking://resources/project-docs/missing",
                    "context_type": "resource",
                }
            )
        }
        missing = NotFoundError(
            "viking://resources/project-docs/missing", "directory"
        )
        with patch(
            "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
            new=AsyncMock(side_effect=missing),
        ), patch.object(
            processor, "_reenqueue_semantic_msg", new=AsyncMock()
        ) as requeue:
            asyncio.run(processor.on_dequeue(message))

        self.assertEqual([kind for kind, _message in events], ["error"])
        requeue.assert_not_awaited()

    def test_semantic_retry_count_is_persisted_and_incremented(self) -> None:
        message = SemanticMsg(
            uri="viking://resources/project-docs/existing",
            context_type="resource",
            retry_count=3,
        )
        restored = SemanticMsg.from_dict(json.loads(message.to_json()))
        self.assertEqual(restored.retry_count, 3)

        processor = SemanticProcessor(max_concurrent_llm=1)
        processor.max_semantic_retries = 5
        requeue = AsyncMock()
        events: list[str] = []
        processor.set_callbacks(
            on_success=lambda: events.append("success"),
            on_requeue=lambda: events.append("requeue"),
            on_error=lambda _message, _data: events.append("error"),
        )
        payload = {
            "data": json.dumps(
                {
                    **message.to_dict(),
                    "id": "semantic-message-retry",
                }
            )
        }
        with patch(
            "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
            new=AsyncMock(side_effect=RuntimeError("429 upstream rate limit")),
        ), patch.object(processor, "_reenqueue_semantic_msg", new=requeue):
            asyncio.run(processor.on_dequeue(payload))

        requeue.assert_awaited_once()
        queued_message = requeue.await_args.args[0]
        self.assertEqual(queued_message.retry_count, 4)
        self.assertEqual(events, ["requeue", "success"])

    def test_semantic_retry_limit_moves_transient_item_to_dead_letter(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor.max_semantic_retries = 0
        events: list[tuple[str, str]] = []
        processor.set_callbacks(
            on_success=lambda: events.append(("success", "")),
            on_requeue=lambda: events.append(("requeue", "")),
            on_error=lambda message, _data: events.append(("error", message)),
        )
        message = {
            "data": json.dumps(
                {
                    "id": "semantic-message-dead-letter",
                    "uri": "viking://resources/project-docs/existing",
                    "context_type": "resource",
                    "retry_count": 0,
                }
            )
        }
        with patch(
            "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
            new=AsyncMock(side_effect=RuntimeError("429 upstream rate limit")),
        ), patch.object(
            processor, "_reenqueue_semantic_msg", new=AsyncMock()
        ) as requeue:
            asyncio.run(processor.on_dequeue(message))

        requeue.assert_not_awaited()
        self.assertEqual([kind for kind, _message in events], ["error"])
        self.assertIn("dead_letter", events[0][1])

    def test_open_circuit_respects_retry_limit(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor.max_semantic_retries = 0
        processor._circuit_breaker._failure_threshold = 1  # type: ignore[attr-defined]
        processor._circuit_breaker.record_failure(RuntimeError("429"))
        events: list[str] = []
        processor.set_callbacks(
            on_success=lambda: events.append("success"),
            on_requeue=lambda: events.append("requeue"),
            on_error=lambda message, _data: events.append(message),
        )
        message = {
            "data": json.dumps(
                {
                    "id": "semantic-message-open",
                    "uri": "viking://resources/project-docs/existing",
                    "context_type": "resource",
                }
            )
        }
        with patch.object(
            processor, "_reenqueue_semantic_msg", new=AsyncMock()
        ) as requeue:
            asyncio.run(processor.on_dequeue(message))

        requeue.assert_not_awaited()
        self.assertEqual(len(events), 1)
        self.assertIn("dead_letter", events[0])

    def test_rate_limit_retry_after_and_fallback_backoff(self) -> None:
        provider_error = RuntimeError("upstream rate limit")
        provider_error.retry_after = 37.5  # type: ignore[attr-defined]
        provider_error.response = SimpleNamespace(headers={})  # type: ignore[attr-defined]
        self.assertTrue(is_retryable_rate_limit_error(provider_error))
        self.assertEqual(retry_after_seconds(provider_error), 37.5)

        header_error = RuntimeError("429")
        header_error.response = SimpleNamespace(  # type: ignore[attr-defined]
            headers={"Retry-After": "19"}
        )
        self.assertEqual(retry_after_seconds(header_error), 19.0)
        retry_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=25)
        date_error = RuntimeError("429")
        date_error.response = SimpleNamespace(  # type: ignore[attr-defined]
            headers={"Retry-After": retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")}
        )
        self.assertGreaterEqual(retry_after_seconds(date_error) or 0, 0)
        self.assertLessEqual(retry_after_seconds(date_error) or 999, 26)
        self.assertEqual(classify_api_error(RuntimeError("429")), ERROR_CLASS_TRANSIENT)

        with patch("openviking.utils.model_retry.random.uniform", return_value=1.0):
            self.assertEqual(rate_limit_retry_delay(1), 5.0)
            self.assertEqual(rate_limit_retry_delay(2), 10.0)
            self.assertEqual(rate_limit_retry_delay(10), 120.0)

    def test_circuit_breaker_exposes_full_window_and_allows_one_probe(self) -> None:
        breaker = CircuitBreaker(
            failure_threshold=1,
            reset_timeout=300,
            max_reset_timeout=600,
        )
        breaker.record_failure(RuntimeError("429"))
        self.assertGreater(breaker.retry_after, 290)
        with self.assertRaises(CircuitBreakerOpen):
            breaker.check()
        probe_time = breaker._last_failure_time + 301  # type: ignore[attr-defined]
        with patch("openviking.utils.circuit_breaker.time.monotonic", return_value=probe_time):
            breaker.check()
            with self.assertRaises(CircuitBreakerOpen):
                breaker.check()
            breaker.record_success()
            breaker.check()

    def test_half_open_task_local_error_releases_probe(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor._circuit_breaker._state = _STATE_HALF_OPEN  # type: ignore[attr-defined]
        processor._circuit_breaker._probe_in_flight = False  # type: ignore[attr-defined]
        processor.max_semantic_retries = 0
        processor.set_callbacks(
            on_success=lambda: None,
            on_requeue=lambda: None,
            on_error=lambda *_args: None,
        )
        message = {
            "data": json.dumps(
                {
                    "id": "half-open-local-error",
                    "uri": "viking://resources/project-docs/missing",
                    "context_type": "resource",
                }
            )
        }
        missing = NotFoundError(
            "viking://resources/project-docs/missing", "directory"
        )
        with patch(
            "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
            new=AsyncMock(side_effect=missing),
        ):
            asyncio.run(processor.on_dequeue(message))

        self.assertEqual(processor._circuit_breaker._state, _STATE_HALF_OPEN)  # type: ignore[attr-defined]
        self.assertFalse(processor._circuit_breaker._probe_in_flight)  # type: ignore[attr-defined]
        self.assertTrue(processor._circuit_breaker.check())

    def test_half_open_lock_error_uses_task_retry_without_sticking_probe(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor._circuit_breaker._state = _STATE_HALF_OPEN  # type: ignore[attr-defined]
        processor._circuit_breaker._probe_in_flight = False  # type: ignore[attr-defined]
        processor.max_semantic_retries = 1
        processor.set_callbacks(
            on_success=lambda: None,
            on_requeue=lambda: None,
            on_error=lambda *_args: None,
        )
        message = {
            "data": json.dumps(
                {
                    "id": "half-open-lock-error",
                    "uri": "viking://resources/project-docs/existing",
                    "context_type": "resource",
                }
            )
        }
        lock_error = LockAcquisitionError("path busy")
        with patch(
            "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
            new=AsyncMock(side_effect=lock_error),
        ), patch.object(
            processor,
            "_reenqueue_semantic_msg",
            new=AsyncMock(),
        ), patch.object(
            processor,
            "_lock_retry_delay",
            return_value=0,
        ):
            asyncio.run(processor.on_dequeue(message))

        self.assertFalse(processor._circuit_breaker._probe_in_flight)  # type: ignore[attr-defined]

    def test_half_open_cancellation_releases_probe(self) -> None:
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor._circuit_breaker._state = _STATE_HALF_OPEN  # type: ignore[attr-defined]
        processor._circuit_breaker._probe_in_flight = False  # type: ignore[attr-defined]
        processor.set_callbacks(
            on_success=lambda: None,
            on_requeue=lambda: None,
            on_error=lambda *_args: None,
        )
        message = {
            "data": json.dumps(
                {
                    "id": "half-open-cancelled",
                    "uri": "viking://resources/project-docs/cancelled",
                    "context_type": "memory",
                }
            )
        }

        async def scenario() -> None:
            started = asyncio.Event()
            semantic_lock = SimpleNamespace(lock=None, close=AsyncMock())

            async def blocked_process(*_args, **_kwargs):
                started.set()
                await asyncio.Event().wait()

            with patch(
                "openviking.storage.queuefs.semantic_processor.SemanticLockScope.resolve",
                new=AsyncMock(return_value=semantic_lock),
            ), patch.object(
                processor,
                "_process_memory_directory",
                new=blocked_process,
            ):
                task = asyncio.create_task(processor.on_dequeue(message))
                await started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        asyncio.run(scenario())
        self.assertFalse(processor._circuit_breaker._probe_in_flight)  # type: ignore[attr-defined]

    def test_half_open_embedding_task_error_releases_probe(self) -> None:
        handler = object.__new__(TextEmbeddingHandler)
        handler._vikingdb = SimpleNamespace(  # type: ignore[attr-defined]
            is_closing=False,
            has_queue_manager=False,
        )
        handler._embedder = object()  # type: ignore[attr-defined]
        handler._vector_dim = 512  # type: ignore[attr-defined]
        handler._circuit_breaker = CircuitBreaker()  # type: ignore[attr-defined]
        handler._circuit_breaker._state = _STATE_HALF_OPEN  # type: ignore[attr-defined]
        handler._circuit_breaker._probe_in_flight = False  # type: ignore[attr-defined]
        handler._breaker_open_last_log_at = 0.0  # type: ignore[attr-defined]
        handler._breaker_open_suppressed_count = 0  # type: ignore[attr-defined]
        handler._breaker_open_log_interval = 30.0  # type: ignore[attr-defined]
        handler.set_callbacks(
            on_success=lambda: None,
            on_requeue=lambda: None,
            on_error=lambda *_args: None,
        )
        message = EmbeddingMsg(
            "oversized",
            {
                "uri": "viking://resources/project-docs/oversized",
                "account_id": "default",
                "abstract": "oversized",
            },
        )
        with patch(
            "openviking.storage.collection_schemas.embed_compat",
            new=AsyncMock(side_effect=RuntimeError("413 payload too large")),
        ):
            asyncio.run(handler.on_dequeue({"data": message.to_json()}))

        self.assertFalse(handler._circuit_breaker._probe_in_flight)  # type: ignore[attr-defined]

if __name__ == "__main__":
    unittest.main()
