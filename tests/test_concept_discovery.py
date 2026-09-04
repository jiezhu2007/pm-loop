from __future__ import annotations

import json
import multiprocessing
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_learning import ConceptLearningStore, discover_from_uris  # noqa: E402
from concept_recheck import collect_delta_evidence  # noqa: E402


def _discover_in_process(
    skill_root: str,
    uris: list[str],
    revisions: dict[str, str],
    ready: multiprocessing.Queue,
    start: multiprocessing.synchronize.Event,
    result: multiprocessing.Queue,
) -> None:
    store = ConceptLearningStore(Path(skill_root))
    ready.put(True)
    start.wait()
    result.put(
        discover_from_uris(
            store,
            uris,
            source="weekly-document-delta",
            evidence_revisions=revisions,
        )
    )


class ConceptDiscoveryIdempotencyTests(unittest.TestCase):
    def _store(self, root: Path) -> ConceptLearningStore:
        skill_root = root / "codex" / "skills" / "shengsuan-concepts"
        skill_root.mkdir(parents=True, exist_ok=True)
        return ConceptLearningStore(skill_root)

    def test_same_snapshot_reuses_run_despite_order_and_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))

            first = discover_from_uris(
                store,
                ["viking://evidence/b", "viking://evidence/a", "viking://evidence/a"],
                source="weekly-document-delta",
            )
            second = discover_from_uris(
                store,
                ["viking://evidence/a", "viking://evidence/b"],
                source="weekly-document-delta",
            )

            self.assertEqual(second["run_id"], first["run_id"])
            self.assertEqual(len(store.discovery_runs()), 1)
            self.assertEqual(first["updated_uris"], ["viking://evidence/a", "viking://evidence/b"])
            self.assertTrue(first["input_fingerprint"].startswith("sha256:"))

    def test_legacy_run_is_reused_without_resetting_triage_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            store.discovery_root.mkdir(parents=True)
            legacy = {
                "schema_version": "concept-learning.discovery.v1",
                "run_id": "discover-legacy-production-shape",
                "created_at": "2026-08-17T01:54:33Z",
                "source": "weekly-document-delta",
                "updated_uris": ["viking://evidence/a"],
                "unmatched_uris": ["viking://evidence/a"],
                "candidate_ids": ["cand-reviewed"],
                "status": "triaged",
            }
            (store.discovery_root / "discover-legacy-production-shape.json").write_text(
                json.dumps(legacy, ensure_ascii=False),
                encoding="utf-8",
            )

            reused = discover_from_uris(store, ["viking://evidence/a"], source="weekly-document-delta")

            self.assertEqual(reused, legacy)
            self.assertEqual(len(store.discovery_runs()), 1)
            self.assertEqual(reused["status"], "triaged")
            self.assertEqual(reused["candidate_ids"], ["cand-reviewed"])

    def test_evidence_or_matching_result_change_creates_new_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uri = "viking://evidence/a"

            first = discover_from_uris(store, [uri], source="weekly-document-delta")
            changed_evidence = discover_from_uris(
                store,
                [uri, "viking://evidence/b"],
                source="weekly-document-delta",
            )
            self.assertNotEqual(changed_evidence["run_id"], first["run_id"])

            store.save_ledger({"已归类概念": {"sources": [uri]}})
            changed_matching = discover_from_uris(store, [uri], source="weekly-document-delta")

            self.assertNotEqual(changed_matching["run_id"], first["run_id"])
            self.assertEqual(changed_matching["unmatched_uris"], [])
            self.assertEqual(len(store.discovery_runs()), 3)

    def test_uri_substring_is_not_treated_as_active_source_match(self) -> None:
        """Only an exact Active ``sources`` URI is already classified.

        A path such as ``.../资源队列...`` must remain in the discovery inbox;
        triage needs the evidence body to decide whether it is an alias/merge
        of ``计算资源`` or a genuinely new concept.
        """
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            exact = "viking://evidence/计算资源说明"
            fragment = "viking://evidence/资源队列说明"
            store.save_ledger({"计算资源": {"status": "active", "sources": [exact]}})

            result = discover_from_uris(
                store,
                [exact, fragment],
                source="weekly-document-delta",
            )

            self.assertEqual(result["unmatched_uris"], [fragment])

    def test_same_uri_revision_change_creates_new_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            uri = "viking://evidence/a"

            first = discover_from_uris(
                store,
                [uri],
                source="weekly-document-delta",
                evidence_revisions={uri: "sha256:first"},
            )
            repeated = discover_from_uris(
                store,
                [uri],
                source="weekly-document-delta",
                evidence_revisions={uri: "sha256:first"},
            )
            changed = discover_from_uris(
                store,
                [uri],
                source="weekly-document-delta",
                evidence_revisions={uri: "sha256:second"},
            )

            self.assertEqual(repeated["run_id"], first["run_id"])
            self.assertNotEqual(changed["run_id"], first["run_id"])
            self.assertEqual(len(store.discovery_runs()), 2)

    def test_overlapping_snapshots_only_enqueue_new_or_changed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            a = "viking://evidence/a"
            b = "viking://evidence/b"
            c = "viking://evidence/c"

            first = discover_from_uris(
                store,
                [a, b],
                source="weekly-document-delta",
                evidence_revisions={a: "sha256:a1", b: "sha256:b1"},
            )
            second = discover_from_uris(
                store,
                [b, c],
                source="weekly-document-delta",
                evidence_revisions={b: "sha256:b1", c: "sha256:c1"},
            )
            repeated = discover_from_uris(
                store,
                [b, c],
                source="weekly-document-delta",
                evidence_revisions={b: "sha256:b1", c: "sha256:c1"},
            )
            changed = discover_from_uris(
                store,
                [b],
                source="weekly-document-delta",
                evidence_revisions={b: "sha256:b2"},
            )

            self.assertEqual(first["unmatched_uris"], [a, b])
            self.assertEqual(second["unmatched_uris"], [c])
            self.assertEqual(second["evidence_revisions"], {c: "sha256:c1"})
            self.assertEqual(repeated["run_id"], second["run_id"])
            self.assertEqual(changed["unmatched_uris"], [b])
            self.assertEqual(changed["evidence_revisions"], {b: "sha256:b2"})
            self.assertEqual(len(store.discovery_runs()), 3)

    def test_reuses_run_that_covers_current_unmatched_uri(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            a = "viking://evidence/a"
            b = "viking://evidence/b"
            known = "viking://evidence/already-classified"

            first = discover_from_uris(
                store,
                [a],
                source="weekly-document-delta",
                evidence_revisions={a: "sha256:a1"},
            )
            newest = discover_from_uris(
                store,
                [b],
                source="weekly-document-delta",
                evidence_revisions={b: "sha256:b1"},
            )
            # Force B to be the newest same-source row. The observed snapshot is
            # different because it also contains a URI that is now classified.
            newest_path = store.discovery_root / f"{newest['run_id']}.json"
            newest_path.write_text(json.dumps(newest, ensure_ascii=False), encoding="utf-8")
            store.save_ledger({"已归类概念": {"sources": [known]}})

            reused = discover_from_uris(
                store,
                [a, known],
                source="weekly-document-delta",
                evidence_revisions={a: "sha256:a1", known: "sha256:known1"},
            )

            self.assertEqual(reused["run_id"], first["run_id"])
            self.assertNotEqual(reused["run_id"], newest["run_id"])
            self.assertEqual(len(store.discovery_runs()), 2)

    def test_revision_aware_input_upgrades_legacy_run_without_resetting_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            store.discovery_root.mkdir(parents=True)
            uri = "viking://evidence/a"
            legacy = {
                "schema_version": "concept-learning.discovery.v1",
                "run_id": "discover-legacy-production-shape",
                "created_at": "2026-08-17T01:54:33Z",
                "source": "weekly-document-delta",
                "updated_uris": [uri],
                "unmatched_uris": [uri],
                "candidate_ids": ["cand-reviewed"],
                "status": "triaged",
            }
            path = store.discovery_root / "discover-legacy-production-shape.json"
            path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

            upgraded = discover_from_uris(
                store,
                [uri],
                source="weekly-document-delta",
                evidence_revisions={uri: "2026-08-17T03:00:00Z"},
            )

            self.assertEqual(upgraded["run_id"], legacy["run_id"])
            self.assertEqual(upgraded["status"], "triaged")
            self.assertEqual(upgraded["candidate_ids"], ["cand-reviewed"])
            self.assertEqual(upgraded["evidence_revisions"], {uri: "2026-08-17T03:00:00Z"})
            self.assertEqual(len(store.discovery_runs()), 1)

    def test_collect_delta_evidence_uses_internal_time_and_public_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            codex_root = Path(temp) / "codex"
            internal = codex_root / "skills" / "shengsuan-sync" / "state"
            public = codex_root / "skills" / "databuilder-public-docs" / "state"
            internal.mkdir(parents=True)
            public.mkdir(parents=True)
            internal_uri = "viking://resources/shengsuan/internal"
            public_uri = "viking://resources/shengsuan/public"
            (internal / "ledger.json").write_text(
                json.dumps({"a": {"target_uri": internal_uri, "publishTime": "2026-08-17 10:00:00"}}),
                encoding="utf-8",
            )
            (public / "ledger.json").write_text(
                json.dumps({"b": {"viking_uri": public_uri, "sha256": "sha256:body", "fetched_at": "2026-08-17T02:00:00Z"}}),
                encoding="utf-8",
            )

            evidence = collect_delta_evidence(codex_root)

            self.assertEqual(evidence[internal_uri], "2026-08-17 10:00:00")
            self.assertEqual(evidence[public_uri], "sha256:body")

    def test_concurrent_same_snapshot_creates_one_complete_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            barrier = threading.Barrier(8)

            def discover() -> dict:
                barrier.wait()
                independent_store = self._store(root)
                return discover_from_uris(
                    independent_store,
                    ["viking://evidence/a", "viking://evidence/b"],
                    source="weekly-document-delta",
                )

            with ThreadPoolExecutor(max_workers=8) as executor:
                rows = list(executor.map(lambda _: discover(), range(8)))

            self.assertEqual(len({row["run_id"] for row in rows}), 1)
            self.assertEqual(len(store.discovery_runs()), 1)
            persisted = json.loads(next(store.discovery_root.glob("*.json")).read_text(encoding="utf-8"))
            self.assertEqual(persisted["run_id"], rows[0]["run_id"])
            self.assertEqual(persisted["status"], "needs_agent_triage")

    def test_concurrent_overlapping_snapshots_record_each_revision_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            skill_root = str(store.skill_root)
            a = "viking://evidence/a"
            b = "viking://evidence/b"
            c = "viking://evidence/c"
            context = multiprocessing.get_context("spawn")
            ready = context.Queue()
            start = context.Event()
            result = context.Queue()
            workers = [
                context.Process(
                    target=_discover_in_process,
                    args=(skill_root, [a, b], {a: "sha256:a1", b: "sha256:b1"}, ready, start, result),
                ),
                context.Process(
                    target=_discover_in_process,
                    args=(skill_root, [b, c], {b: "sha256:b1", c: "sha256:c1"}, ready, start, result),
                ),
            ]
            for worker in workers:
                worker.start()
            for _ in workers:
                self.assertTrue(ready.get(timeout=10))
            start.set()
            returned = [result.get(timeout=10) for _ in workers]
            for worker in workers:
                worker.join(timeout=10)
                self.assertEqual(worker.exitcode, 0)

            runs = store.discovery_runs()
            occurrences = {
                uri: sum(run.get("unmatched_uris", []).count(uri) for run in runs)
                for uri in (a, b, c)
            }
            self.assertEqual(occurrences, {a: 1, b: 1, c: 1})
            self.assertEqual(len(runs), 2)
            self.assertEqual({row["run_id"] for row in returned}, {row["run_id"] for row in runs})


if __name__ == "__main__":
    unittest.main()
