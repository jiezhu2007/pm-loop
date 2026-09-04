import threading
import time
import unittest
from unittest import mock

from scripts import concept_refresh_adapter as adapter


class _FakeSearch:
    def __init__(self, contents):
        self.contents = contents

    def read_content(self, uri):
        value = self.contents[uri]
        if isinstance(value, BaseException):
            raise value
        return value


class FetchDocumentsTests(unittest.TestCase):
    def test_parallel_fetch_preserves_order_and_drops_failures(self):
        hits = [{"uri": uri, "score": index} for index, uri in enumerate(("a", "b", "c"))]
        search = _FakeSearch({"a": "alpha", "b": RuntimeError("transient"), "c": "charlie"})

        docs = adapter.fetch_documents(search, hits, max_chars=20, fetch_jobs=3)

        self.assertEqual([doc["uri"] for doc in docs], ["a", "c"])
        self.assertEqual([doc["content"] for doc in docs], ["alpha", "charlie"])

    def test_parallel_fetch_is_bounded(self):
        hits = [{"uri": str(index)} for index in range(4)]
        active = 0
        peak = 0
        lock = threading.Lock()

        def read_content(_uri):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return "body"

        search = mock.Mock(read_content=read_content)
        docs = adapter.fetch_documents(search, hits, max_chars=20, fetch_jobs=2)

        self.assertEqual(len(docs), len(hits))
        self.assertLessEqual(peak, 2)

    def test_batch_timeout_does_not_wait_for_stuck_worker_and_records_uri(self):
        release = threading.Event()
        started = threading.Event()
        hits = [{"uri": "fast"}, {"uri": "stuck"}]

        def read_content(uri):
            if uri == "stuck":
                started.set()
                release.wait(2)
            return f"body-{uri}"

        search = mock.Mock(read_content=read_content)
        try:
            with mock.patch.dict(
                "os.environ",
                {"CONCEPTS_DOC_FETCH_BATCH_TIMEOUT": "0.1"},
            ):
                started_at = time.monotonic()
                outcomes = adapter.fetch_document_outcomes(
                    search,
                    hits,
                    max_chars=20,
                    fetch_jobs=2,
                )
                elapsed = time.monotonic() - started_at
        finally:
            release.set()

        self.assertTrue(started.is_set())
        self.assertLess(elapsed, 0.5)
        self.assertEqual([item["uri"] for item in outcomes], ["fast", "stuck"])
        self.assertEqual(outcomes[0]["status"], "available")
        self.assertEqual(outcomes[1]["status"], "unavailable")
        self.assertEqual(outcomes[1]["error"], "batch_timeout")


if __name__ == "__main__":
    unittest.main()
