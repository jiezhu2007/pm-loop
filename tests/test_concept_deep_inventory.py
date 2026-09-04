from __future__ import annotations

import hashlib
import io
import json
import sys
import tempfile
import time
import unittest
from collections import Counter, defaultdict
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import concept_deep_inventory as inventory  # noqa: E402
from concept_learning import ConceptLearningStore  # noqa: E402


TEST_ROOT = "viking://resources/shengsuan/test"


class FakeOpenViking:
    def __init__(self, bodies, *, failures=None, delays=None) -> None:
        self.bodies = dict(bodies)
        self.uris = sorted(self.bodies)
        self.failures = Counter(failures or {})
        self.delays = dict(delays or {})
        self.page_calls = defaultdict(list)

    def glob(self, root, pattern, node_limit):
        return self.uris[:node_limit]

    def read_content_page(self, uri, offset, limit):
        self.page_calls[uri].append((offset, limit))
        if self.failures[uri] > 0:
            self.failures[uri] -= 1
            raise OSError("transient read failure")
        if offset == 0 and self.delays.get(uri):
            time.sleep(self.delays[uri])
        lines = self.bodies[uri].splitlines(keepends=True)
        return "".join(lines[offset : offset + limit])


class RequestOpenViking:
    def __init__(self, result="line\n") -> None:
        self.result = result
        self.requests = []

    def _request(self, method, path, body=None, query=None):
        self.requests.append((method, path, body, dict(query or {})))
        return {"status": "ok", "result": self.result}


def valid_content(name, refs, aliases=None):
    metadata = {
        "concept": name,
        "aliases": aliases or [],
        "category": "product_capability",
        "last_updated": "2026-08-20",
        "sources": refs,
        "related_concepts": [],
        "related_customers": [],
        "latest_version": "未标注",
    }
    return (
        "---\n"
        + json.dumps(metadata, ensure_ascii=False, indent=2)
        + "\n---\n\n"
        + f"# {name}\n\n"
        + "## 定义\n该概念仅依据给出的完整正文证据归纳，仍需本人审核确认。\n\n"
        + "## 能力边界\n- 仅覆盖来源中明确出现的能力描述，并逐项回看来源。\n\n"
        + "## 已知限制\n- 未从常识推断交付状态、版本或客户可用性。\n\n"
        + "## 版本演进\n- 当前来源未形成可验证的统一版本记录。\n\n"
        + "## 关联概念\n- 待人工审核后建立正式关联。\n\n"
        + "## 出现过的客户/评估\n- 不从文档路径自动推断客户关系。\n\n"
        + "## 证据与待确认点\n"
        + "\n".join(f"- {uri}" for uri in refs)
        + "\n- 待确认正式能力边界和归属。"
    )


def successful_invoker(prompt, timeout):
    groups = json.loads(prompt.split("CANDIDATES=", 1)[1])
    decisions = []
    for group in groups:
        refs = [item["uri"] for item in group["evidence"]]
        name = group["term"]
        decisions.append(
            {
                "decision": "new_concept",
                "name": name,
                "aliases": [],
                "category": "product_capability",
                "content": valid_content(name, refs),
                "evidence_uris": refs,
                "reason": ["跨文档证据"],
                "confidence": 0.82,
            }
        )
    return decisions


def term_group(name, refs=None):
    refs = refs or [f"viking://evidence/{name}/a", f"viking://evidence/{name}/b"]
    return {
        "term": name,
        "normalized_term": inventory._normalize_term(name),
        "document_count": len(refs),
        "evidence": [{"uri": uri, "excerpt": f"{name} evidence"} for uri in refs],
        "seeded": False,
    }


def ignore_decision(name, refs=None):
    return {
        "decision": "ignore",
        "name": name,
        "aliases": [],
        "category": "product_capability",
        "content": "",
        "evidence_uris": refs or [],
        "reason": ["not a product concept"],
        "confidence": 0.8,
    }


class DeepInventoryTests(unittest.TestCase):
    def _store(self, root: Path, concepts="concepts: []\n"):
        skill = root / "codex" / "skills" / "shengsuan-concepts"
        skill.mkdir(parents=True)
        (skill / "config.yaml").write_text(concepts, encoding="utf-8")
        return ConceptLearningStore(skill)

    def _execute(self, store, client, state_dir, **kwargs):
        defaults = {
            "roots": [TEST_ROOT],
            "excludes": [],
            "node_limit": 100,
            "max_workers": 2,
            "read_batch_size": 2,
            "batch_size": 2,
            "page_size": 13,
            "prompt_char_budget": 6000,
            "llm_timeout": 1,
            "llm_retries": 1,
            "retry_delay": 0,
            "invoker": successful_invoker,
        }
        defaults.update(kwargs)
        return inventory.execute(store, client, state_dir=state_dir, **defaults)

    def test_reads_every_page_and_hashes_complete_body(self):
        body = "".join(f"第{index}行 DataMesh 能力说明。\n" for index in range(8))
        uri = f"{TEST_ROOT}/a.md"
        client = FakeOpenViking({uri: body})

        result = inventory.read_full_document(client, uri, page_size=3)

        self.assertEqual(result["char_count"], len(body))
        self.assertEqual(result["byte_count"], len(body.encode("utf-8")))
        self.assertEqual(result["page_count"], 3)
        self.assertEqual(
            result["content_hash"],
            "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(client.page_calls[uri], [(0, 3), (3, 3), (6, 3)])
        self.assertEqual(result["text"], body)

    def test_raw_pages_preserve_blank_line_boundaries_and_hash(self):
        body = "alpha\nbeta\ngamma\n\nomega\nlast"
        uri = f"{TEST_ROOT}/blank-boundary.md"
        client = FakeOpenViking({uri: body})

        result = inventory.read_full_document(client, uri, page_size=3)

        self.assertEqual(client.page_calls[uri], [(0, 3), (3, 3), (6, 3)])
        self.assertEqual(result["text"], body)
        self.assertEqual(
            result["content_hash"],
            "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest(),
        )

    def _write_source_ledger(self, root, uri, body, *, ledger="shengsuan-sync"):
        ledger_path = root / "codex" / "skills" / ledger / "state" / "ledger.json"
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(
            json.dumps(
                {
                    "source-1": {
                        "target_uri": uri,
                        "sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                    }
                }
            ),
            encoding="utf-8",
        )

    def test_source_revision_resolves_leaf_below_synced_resource(self):
        uri = f"{TEST_ROOT}/page.html/leaf/part.md"
        parent = f"{TEST_ROOT}/page.html"
        revision = "sha256:" + ("a" * 64)

        self.assertEqual(
            inventory._source_revision_for_uri(uri, {parent: revision}),
            revision,
        )
        self.assertEqual(
            inventory._resolved_source_revisions([uri], {parent: revision}),
            {uri: revision},
        )

    def test_cross_run_cache_reuses_record_when_source_revision_matches(self):
        uri = f"{TEST_ROOT}/cached.md"
        body = "DataMesh 证据正文\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            self._write_source_ledger(root, uri, body)

            first_client = FakeOpenViking({uri: body})
            first = self._execute(
                store,
                first_client,
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )
            self.assertEqual(first["status"], "completed")
            self.assertGreater(len(first_client.page_calls[uri]), 0)
            self.assertTrue((root / "state" / "evidence-cache.json.gz").exists())
            # The historical path remains readable during the migration.
            self.assertEqual(
                inventory._read_json(root / "state" / "evidence-cache.json")["schema_version"],
                inventory.EVIDENCE_CACHE_SCHEMA,
            )
            self.assertTrue((root / "state" / "evidence-cache.meta.json").exists())

            second_client = FakeOpenViking({uri: body})
            second = self._execute(
                store,
                second_client,
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )

        self.assertEqual(second["status"], "completed")
        self.assertEqual(second_client.page_calls[uri], [])
        self.assertEqual(second["evidence_cache"]["cache_hits"], 1)
        self.assertEqual(second["evidence_cache"]["cache_misses"], 0)
        self.assertEqual(second["evidence_cache"]["source_hash_rows"], 1)

    def test_cross_run_cache_misses_when_source_revision_changes(self):
        uri = f"{TEST_ROOT}/changed.md"
        first_body = "DataMesh old\n"
        second_body = "DataMesh new\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            self._write_source_ledger(root, uri, first_body)
            self._execute(
                store,
                FakeOpenViking({uri: first_body}),
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )

            self._write_source_ledger(root, uri, second_body)
            second_client = FakeOpenViking({uri: second_body})
            second = self._execute(
                store,
                second_client,
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )

        self.assertEqual(second["status"], "completed")
        self.assertGreater(len(second_client.page_calls[uri]), 0)
        self.assertEqual(second["evidence_cache"]["cache_hits"], 0)
        self.assertEqual(second["evidence_cache"]["cache_misses"], 1)

    def test_changed_document_classification_is_conservative_without_current_hash(self):
        uris = [f"{TEST_ROOT}/{name}.md" for name in ("stable", "changed", "unknown", "new")]
        previous = {
            "schema_version": inventory.BASELINE_SCHEMA,
            "source_revisions": {
                uris[0]: {"revision": "sha256:" + "a" * 64},
                uris[1]: {"revision": "sha256:" + "b" * 64},
                uris[2]: {"revision": "sha256:" + "c" * 64},
            },
        }
        current = {
            uris[0]: "sha256:" + "a" * 64,
            uris[1]: "sha256:" + "d" * 64,
            uris[3]: "sha256:" + "e" * 64,
        }

        result = inventory._changed_documents(uris, current, previous)

        self.assertEqual(result["unchanged_uris"], [uris[0]])
        self.assertEqual(set(result["changed_uris"]), {uris[1], uris[2], uris[3]})
        self.assertEqual(result["unknown_revision_uris"], [uris[2]])
        self.assertEqual(result["new_uris"], [uris[3]])

    def test_content_dedup_preserves_all_source_uris(self):
        body_hash = inventory.content_hash("same")
        documents = [
            {
                "uri": f"{TEST_ROOT}/one.md",
                "content_hash": body_hash,
                "char_count": 4,
                "byte_count": 4,
                "page_count": 1,
                "terms": [{"term": "DataMesh", "excerpt": "same"}],
            },
            {
                "uri": f"{TEST_ROOT}/two.md",
                "content_hash": body_hash,
                "char_count": 4,
                "byte_count": 4,
                "page_count": 1,
                "terms": [{"term": "DataMesh", "excerpt": "same"}],
            },
        ]

        summary = inventory._content_dedup_summary(documents)

        self.assertEqual(summary["document_count"], 2)
        self.assertEqual(summary["unique_content_count"], 1)
        self.assertEqual(summary["duplicate_document_count"], 1)
        self.assertEqual(summary["duplicate_ratio"], 0.5)
        group = summary["groups"][body_hash]
        self.assertEqual(group["uris"], sorted(item["uri"] for item in documents))
        self.assertEqual(group["duplicate_count"], 1)

    def test_scan_reuses_term_extraction_for_duplicate_bodies(self):
        uris = [f"{TEST_ROOT}/duplicate-{index}.md" for index in range(2)]
        body = "# DataMesh\n\n相同正文\n"
        client = FakeOpenViking({uri: body for uri in uris})
        with tempfile.TemporaryDirectory() as temp:
            with mock.patch.object(inventory, "_terms", wraps=inventory._terms) as extract:
                artifact = inventory._scan_evidence_batch(
                    client,
                    uris,
                    0,
                    Path(temp) / "batch.json",
                    page_size=100,
                    max_workers=1,
                    seed_terms=[],
                )

        self.assertEqual(extract.call_count, 1)
        self.assertEqual(artifact["content_dedup_hits"], 1)
        self.assertEqual(artifact["completed_count"], 2)

    def test_baseline_only_materializes_without_llm_or_candidates(self):
        uri = f"{TEST_ROOT}/baseline-only.md"
        body = "DataMesh baseline evidence\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            self._write_source_ledger(root, uri, body)
            client = FakeOpenViking({uri: body})

            def forbidden(prompt, timeout):
                raise AssertionError("baseline-only must not invoke LLM")

            result = self._execute(
                store,
                client,
                root / "state",
                roots=[TEST_ROOT],
                baseline_only=True,
                invoker=forbidden,
                read_batch_size=1,
            )
            baseline = json.loads((root / "state" / "incremental-baseline.json").read_text())
            dedup = inventory._read_json(root / "state" / "content-dedup.json")
            manifest = json.loads(next((root / "state" / "runs").glob("*.json")).read_text())

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["candidate_count"], 0)
        self.assertEqual(store.list_candidates(), [])
        self.assertTrue(result["baseline_ready"])
        self.assertTrue(baseline["baseline_ready"])
        self.assertEqual(baseline["source_hash_coverage"], 1.0)
        self.assertEqual(baseline["evidence_cache_coverage"], 1.0)
        self.assertEqual(dedup["document_count"], 1)
        self.assertEqual(manifest["config"]["mode"], "baseline_only")
        self.assertEqual(manifest["stage_progress"]["llm_reduce"]["status"], "skipped")
        self.assertEqual(manifest["stage_progress"]["candidate_write"]["status"], "skipped")

    def test_baseline_reports_incomplete_when_ledger_hash_is_missing(self):
        uri = f"{TEST_ROOT}/baseline-incomplete.md"
        body = "DataMesh baseline without ledger hash\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            result = self._execute(
                store,
                FakeOpenViking({uri: body}),
                root / "state",
                baseline_only=True,
                roots=[TEST_ROOT],
                read_batch_size=1,
            )

        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["baseline_ready"])
        self.assertEqual(result["baseline"]["source_hash_coverage"], 0.0)
        self.assertEqual(result["baseline"]["evidence_cache_coverage"], 1.0)

    def test_migrate_completed_legacy_run_without_rereading_or_candidates(self):
        uri = f"{TEST_ROOT}/legacy-baseline.md"
        body = "普通正文，没有可提取产品术语。\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            first = self._execute(
                store,
                FakeOpenViking({uri: body}),
                root / "state",
                roots=[TEST_ROOT],
                deterministic=True,
                read_batch_size=1,
            )
            state = root / "state"
            for name in (
                "evidence-cache.json",
                "evidence-cache.json.gz",
                "evidence-cache.meta.json",
                "incremental-baseline.json",
                "content-dedup.json",
                "content-dedup.json.gz",
            ):
                path = state / name
                if path.exists():
                    path.unlink()
            migrated = inventory.materialize_baseline_from_run(
                store,
                state_dir=state,
                run_id=first["run_id"],
            )
            baseline = json.loads((state / "incremental-baseline.json").read_text())
            cache = inventory._read_json(state / "evidence-cache.json")

        self.assertEqual(migrated["status"], "completed")
        self.assertFalse(migrated["baseline_ready"])
        self.assertEqual(migrated["cache_entry_count"], 1)
        self.assertEqual(baseline["evidence_cache_coverage"], 1.0)
        self.assertEqual(baseline["source_hash_coverage"], 0.0)
        self.assertEqual(cache["schema_version"], inventory.EVIDENCE_CACHE_SCHEMA)
        self.assertEqual(store.list_candidates(), [])

    def test_cache_hit_does_not_rewrite_unchanged_cache_snapshot(self):
        uri = f"{TEST_ROOT}/stable-cache.md"
        body = "DataMesh stable cache\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            self._write_source_ledger(root, uri, body)
            self._execute(
                store,
                FakeOpenViking({uri: body}),
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )

            with mock.patch.object(inventory, "_persist_evidence_cache") as persist:
                second = self._execute(
                    store,
                    FakeOpenViking({uri: body}),
                    root / "state",
                    deterministic=True,
                    read_batch_size=1,
                )

        self.assertEqual(second["evidence_cache"]["cache_hits"], 1)
        persist.assert_not_called()

    def test_cache_without_trusted_source_revision_does_not_skip_read(self):
        uri = f"{TEST_ROOT}/untrusted.md"
        body = "DataMesh untrusted\n"
        fingerprint = inventory._terms_fingerprint([])
        record = {
            "uri": uri,
            "content_hash": inventory.content_hash(body),
            "char_count": len(body),
            "byte_count": len(body.encode("utf-8")),
            "page_count": 1,
            "terms": [{"term": "DataMesh", "excerpt": "DataMesh"}],
        }
        cache = {
            uri: {
                "source_revision": record["content_hash"],
                "terms_fingerprint": fingerprint,
                "record": record,
            }
        }
        client = FakeOpenViking({uri: body})
        with tempfile.TemporaryDirectory() as temp:
            artifact = inventory._scan_evidence_batch(
                client,
                [uri],
                0,
                Path(temp) / "batch.json",
                page_size=100,
                max_workers=1,
                seed_terms=[],
                cache_entries=cache,
                source_revisions={},
                terms_fingerprint=fingerprint,
            )

        self.assertGreater(len(client.page_calls[uri]), 0)
        self.assertEqual(artifact["cache_hits"], 0)
        self.assertEqual(artifact["source_hash_rows"], 0)

    def test_openviking_read_requests_raw_content(self):
        client = RequestOpenViking()

        page = inventory._read_page(client, "viking://evidence/doc", 3, 5)

        self.assertEqual(page, "line\n")
        self.assertEqual(
            client.requests,
            [
                (
                    "GET",
                    "/api/v1/content/read",
                    None,
                    {
                        "uri": "viking://evidence/doc",
                        "offset": 3,
                        "limit": 5,
                        "raw": "true",
                    },
                )
            ],
        )

    def test_controlled_lowercase_product_terms_without_generic_camel_noise(self):
        terms = list(
            inventory._terms(
                "dataAgent 与 datasearch 是产品名；createdAt 和 contentHash 是普通字段。"
            )
        )

        self.assertEqual(terms, ["dataAgent", "datasearch"])

    def test_term_extraction_ignores_source_ids_urls_and_fenced_code(self):
        terms = list(
            inventory._terms(
                "# DataBuilder 能力\n"
                "> 来源: https://ku.baidu-int.com/knowledge/HFVrC7hq1Q/QPyt4Fmnf4\n"
                "```sql\nSELECT String FROM Table\n```\n"
                "正文使用 DataMesh 与 dataAgent。\n"
            )
        )

        self.assertIn("DataBuilder", terms)
        self.assertIn("DataMesh", terms)
        self.assertIn("dataAgent", terms)
        self.assertNotIn("HFVrC7hq1Q", terms)
        self.assertNotIn("QPyt4Fmnf4", terms)
        self.assertNotIn("SELECT", terms)
        self.assertNotIn("String", terms)

    def test_term_groups_require_two_distinct_content_hashes(self):
        documents = [
            {
                "uri": f"{TEST_ROOT}/copy-{index}.md",
                "content_hash": "sha256:same",
                "terms": [{"term": "DataMesh", "excerpt": "DataMesh"}],
            }
            for index in range(2)
        ]

        self.assertEqual(inventory.build_term_groups(documents, []), [])

    def test_active_alias_match_is_alias_and_not_new_concept(self):
        existing = [
            {
                "name": "数据搜索",
                "aliases": ["DataSearch", "data search"],
                "source": "config",
                "status": "active",
            }
        ]
        documents = [
            {
                "uri": f"{TEST_ROOT}/search-{index}.md",
                "content_hash": f"sha256:search-{index}",
                "terms": [{"term": "Data-Search", "excerpt": "Data-Search"}],
            }
            for index in range(2)
        ]

        groups = inventory.build_term_groups(documents, existing)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["active_match"]["target"], "数据搜索")
        matched, unmatched = inventory._active_match_decisions(groups)
        self.assertEqual(unmatched, [])
        self.assertEqual(matched[0]["decision"], "alias")
        self.assertEqual(matched[0]["name"], "数据搜索")
        self.assertEqual(matched[0]["target"], "数据搜索")

    def test_active_match_is_cached_per_normalized_term(self):
        existing = [
            {
                "name": "数据搜索",
                "aliases": ["DataSearch"],
                "source": "config",
                "status": "active",
            }
        ]
        documents = [
            {
                "uri": f"{TEST_ROOT}/search-{index}.md",
                "content_hash": f"sha256:search-{index}",
                "terms": [{"term": term, "excerpt": term}],
            }
            for index, term in enumerate(("DataSearch", "data-search", "DATASEARCH"))
        ]
        original = inventory._active_match_for_term
        with mock.patch.object(
            inventory,
            "_active_match_for_term",
            side_effect=original,
        ) as matcher:
            groups = inventory.build_term_groups(documents, existing)

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["active_match"]["target"], "数据搜索")
        self.assertEqual(matcher.call_count, 1)

    def test_sanitize_hard_guard_rewrites_active_new_concept_to_merge(self):
        group = {
            **term_group("资源队列"),
            "active_match": {
                "target": "计算资源",
                "matched_surface": "计算资源",
                "match_type": "fuzzy",
                "decision": "merge",
                "score": 0.8,
                "category": "运维与商业",
            },
        }
        refs = [item["uri"] for item in group["evidence"]]
        raw = [
            {
                "group_term": "资源队列",
                "decision": "new_concept",
                "name": "资源队列",
                "aliases": [],
                "target": "",
                "category": "product_capability",
                "content": valid_content("资源队列", refs),
                "evidence_uris": refs,
                "reason": ["model tried to create a concept"],
                "confidence": 0.9,
            }
        ]

        sanitized = inventory._sanitize_decisions(raw, [group])

        self.assertEqual(sanitized[0]["decision"], "merge")
        self.assertEqual(sanitized[0]["name"], "计算资源")
        self.assertEqual(sanitized[0]["target"], "计算资源")
        self.assertEqual(sanitized[0]["content"], "")

    def test_active_fuzzy_fragments_become_merge_decisions(self):
        existing = [
            {
                "name": "计算资源",
                "aliases": ["compute", "资源管理"],
                "source": "config",
                "status": "active",
            },
            {
                "name": "数据授权",
                "aliases": ["行列权限"],
                "source": "config",
                "status": "active",
            },
        ]
        documents = []
        for term in ("资源队列", "行权限"):
            for index in range(2):
                documents.append(
                    {
                        "uri": f"{TEST_ROOT}/{term}-{index}.md",
                        "content_hash": f"sha256:{term}-{index}",
                        "terms": [{"term": term, "excerpt": term}],
                    }
                )

        groups = inventory.build_term_groups(documents, existing)
        by_term = {group["term"]: group for group in groups}
        self.assertEqual(by_term["资源队列"]["active_match"]["target"], "计算资源")
        self.assertEqual(by_term["行权限"]["active_match"]["target"], "数据授权")
        matched, unmatched = inventory._active_match_decisions(groups)
        self.assertEqual(unmatched, [])
        self.assertEqual({item["decision"] for item in matched}, {"merge"})

    def test_broader_term_does_not_merge_into_narrower_active_concept(self):
        existing = [
            {
                "name": "大模型算子",
                "aliases": ["大模型处理算子"],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        self.assertIsNone(inventory._active_match_for_term("大模型", index))

    def test_non_controlled_shared_prefix_does_not_force_merge(self):
        existing = [
            {
                "name": "数据卷",
                "aliases": ["非结构化存储"],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        self.assertIsNone(inventory._active_match_for_term("非结构化数据", index))

    def test_short_active_alias_inside_specific_term_is_controlled_merge(self):
        existing = [
            {
                "name": "Ontology",
                "aliases": ["本体"],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        match = inventory._active_match_for_term("本体创建", index)

        self.assertEqual(match["target"], "Ontology")
        self.assertEqual(match["decision"], "merge")

    def test_longest_active_surface_wins_for_contained_terms(self):
        existing = [
            {"name": "数据集", "aliases": [], "source": "config", "status": "active"},
            {"name": "数据集成", "aliases": [], "source": "config", "status": "active"},
        ]
        index = inventory._active_concept_index(existing)

        match = inventory._active_match_for_term("数据集成任务", index)

        self.assertEqual(match["target"], "数据集成")

    def test_controlled_resource_terms_exclude_generic_resource_phrases(self):
        existing = [
            {
                "name": "计算资源",
                "aliases": [],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        self.assertEqual(inventory._active_match_for_term("资源队列", index)["target"], "计算资源")
        self.assertEqual(inventory._active_match_for_term("资源配置", index)["target"], "计算资源")
        for term in ("关联资源", "资源消耗", "资源监控", "等应用的资源需求"):
            self.assertIsNone(inventory._active_match_for_term(term, index), term)

    def test_historical_pipeline_typo_normalizes_to_active_alias(self):
        existing = [
            {
                "name": "数据管道",
                "aliases": ["pipeline"],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        match = inventory._active_match_for_term("PiplineBuilder", index)

        self.assertEqual(match["target"], "数据管道")

    def test_dataset_library_name_is_not_product_dataset_match(self):
        existing = [
            {
                "name": "数据集",
                "aliases": ["dataset"],
                "source": "config",
                "status": "active",
            }
        ]
        index = inventory._active_concept_index(existing)

        self.assertIsNone(inventory._active_match_for_term("RayDataset", index))

    def test_pending_candidate_is_not_an_active_match_or_new_group(self):
        existing = [
            {
                "name": "待审核概念",
                "aliases": ["pending alias"],
                "source": "candidate",
                "status": "ready_for_review",
            }
        ]
        documents = [
            {
                "uri": f"{TEST_ROOT}/pending-{index}.md",
                "content_hash": f"sha256:pending-{index}",
                "terms": [{"term": "待审核概念", "excerpt": "待审核概念"}],
            }
            for index in range(2)
        ]

        self.assertEqual(inventory.build_term_groups(documents, existing), [])

    def test_active_match_never_calls_new_concept_invoker(self):
        uris = [f"{TEST_ROOT}/search-{index}.md" for index in range(2)]
        bodies = {
            uri: f"正文中反复出现 DataSearch，证据文档 {index}"
            for index, uri in enumerate(uris)
        }
        calls = []

        def forbidden(prompt, timeout):
            calls.append(prompt)
            raise AssertionError("active match must not enter the new-concept LLM branch")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(
                root,
                "concepts:\n  - name: 数据搜索\n    aliases: [DataSearch]\n",
            )
            result = self._execute(
                store,
                FakeOpenViking(bodies),
                root / "state",
                invoker=forbidden,
            )

        self.assertEqual(calls, [])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["candidate_count"], 0)
        self.assertEqual(result["decision_counts"]["alias"], 1)
        self.assertEqual(result["decision_counts"]["new_concept"], 0)

    def test_term_selection_is_bounded_and_keeps_seeded_group(self):
        groups = [
            term_group("HFVrC7hq1Q"),
            term_group("DataBuilder"),
            {**term_group("AI-FDE"), "seeded": True},
            term_group("数据资产"),
        ]

        selected, summary = inventory.select_term_groups(groups, max_groups=2)

        self.assertEqual(len(selected), 2)
        self.assertIn("AI-FDE", [item["term"] for item in selected])
        self.assertNotIn("HFVrC7hq1Q", [item["term"] for item in selected])
        self.assertEqual(summary["noise_term_count"], 1)
        self.assertEqual(summary["deferred_term_count"], 1)

    def test_as_completed_checkpoints_fast_document_before_slow_head(self):
        slow = f"{TEST_ROOT}/00-slow.md"
        fast = f"{TEST_ROOT}/01-fast.md"
        client = FakeOpenViking(
            {slow: "DataMesh slow", fast: "DataMesh fast"},
            delays={slow: 0.08},
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "batch.json"
            writes = []
            original = inventory._atomic_json

            def capture(target, value):
                if target == path and value.get("schema_version") == inventory.EVIDENCE_SCHEMA:
                    writes.append([item["uri"] for item in value.get("documents", [])])
                original(target, value)

            with mock.patch.object(inventory, "_atomic_json", side_effect=capture):
                result = inventory._scan_evidence_batch(
                    client,
                    [slow, fast],
                    0,
                    path,
                    page_size=100,
                    max_workers=2,
                    seed_terms=[],
                    checkpoint_every=1,
                    checkpoint_interval=3600,
                )

        self.assertEqual(result["status"], "completed")
        self.assertIn([fast], writes)
        self.assertEqual([item["uri"] for item in result["documents"]], [slow, fast])

    def test_evidence_checkpoint_is_throttled_and_final_flush_is_complete(self):
        uris = [f"{TEST_ROOT}/{index}.md" for index in range(10)]
        client = FakeOpenViking({uri: f"DataMesh document {index}" for index, uri in enumerate(uris)})
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "batch.json"
            writes = []
            original = inventory._atomic_json

            def capture(target, value):
                if target == path and value.get("schema_version") == inventory.EVIDENCE_SCHEMA:
                    writes.append((value["status"], value["completed_count"]))
                original(target, value)

            with mock.patch.object(inventory, "_atomic_json", side_effect=capture):
                result = inventory._scan_evidence_batch(
                    client,
                    uris,
                    0,
                    path,
                    page_size=100,
                    max_workers=1,
                    seed_terms=[],
                    checkpoint_every=4,
                    checkpoint_interval=3600,
                )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["completed_count"], 10)
        self.assertEqual(writes, [("retry_pending", 4), ("retry_pending", 8), ("completed", 10)])

    def test_evidence_error_is_flushed_when_batch_does_not_complete(self):
        good = f"{TEST_ROOT}/good.md"
        bad = f"{TEST_ROOT}/bad.md"
        client = FakeOpenViking(
            {good: "DataMesh good", bad: "DataMesh bad"},
            failures={bad: 1},
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "batch.json"
            writes = []
            original = inventory._atomic_json

            def capture(target, value):
                if target == path and value.get("schema_version") == inventory.EVIDENCE_SCHEMA:
                    writes.append(value)
                original(target, value)

            with mock.patch.object(inventory, "_atomic_json", side_effect=capture):
                result = inventory._scan_evidence_batch(
                    client,
                    [good, bad],
                    0,
                    path,
                    page_size=100,
                    max_workers=2,
                    seed_terms=[],
                    checkpoint_every=8,
                    checkpoint_interval=3600,
                )

        self.assertEqual(result["status"], "retry_pending")
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["pending_count"], 1)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[-1]["errors"][0]["uri"], bad)

    def test_complete_run_creates_review_candidate_and_audit_only(self):
        uris = [f"{TEST_ROOT}/a.md", f"{TEST_ROOT}/b.md"]
        bodies = {uri: f"正文中稳定出现 DataMesh，来源 {index}" for index, uri in enumerate(uris)}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            result = self._execute(store, FakeOpenViking(bodies), root / "state")

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["resource_count"], 2)
            self.assertEqual(result["read_count"], 2)
            self.assertEqual(result["unreadable_count"], 0)
            self.assertGreaterEqual(result["full_page_count"], 2)
            self.assertEqual(result["candidate_count"], 1)
            candidate = store.read_candidate(result["candidate_ids"][0])
            self.assertEqual(candidate["concept"], "DataMesh")
            self.assertEqual(candidate["kind"], "new_concept")
            self.assertEqual(candidate["status"], "ready_for_review")
            self.assertEqual(candidate["source_refs"], uris)
            self.assertEqual(len(candidate["evidence"]), 2)
            self.assertNotIn("approved_at", candidate)
            self.assertNotIn("published_at", candidate)
            self.assertEqual(store.load_ledger(), {})
            audit = (
                store.state_root / "logs" / "concept-agent-audit.jsonl"
            ).read_text(encoding="utf-8")
            self.assertIn('"event": "candidate.created"', audit)
            self.assertIn('"concept": "DataMesh"', audit)
            flat = json.loads(
                next((root / "state" / "runs").glob("*.json")).read_text()
            )
            self.assertEqual(flat["status"], "completed")
            self.assertEqual(flat["scan_cursor"], 2)
            self.assertEqual(
                flat["progress"],
                {"processed": 2, "read": 2, "unreadable": 0, "total": 2},
            )

    def test_prompt_partition_respects_character_budget(self):
        groups = [
            {
                "term": f"ProductTerm{index}",
                "normalized_term": f"productterm{index}",
                "document_count": 2,
                "evidence": [
                    {"uri": f"viking://a/{index}", "excerpt": "证据" * 500},
                    {"uri": f"viking://b/{index}", "excerpt": "证据" * 500},
                ],
                "seeded": False,
            }
            for index in range(5)
        ]
        budget = 2200
        batches = inventory.partition_term_groups(groups, [], budget, max_groups=3)
        self.assertEqual(sum(len(batch) for batch in batches), 5)
        self.assertTrue(all(len(inventory._prompt(batch, [])) <= budget for batch in batches))
        self.assertTrue(all(len(batch) <= 3 for batch in batches))

    def test_transient_llm_failure_retries_and_completes(self):
        uris = [f"{TEST_ROOT}/a.md", f"{TEST_ROOT}/b.md"]
        client = FakeOpenViking(
            {uri: f"DataMesh 正文 {index}" for index, uri in enumerate(uris)}
        )
        calls = 0

        def flaky(prompt, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("temporary")
            return successful_invoker(prompt, timeout)

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            result = self._execute(
                store,
                client,
                root / "state",
                invoker=flaky,
                llm_retries=1,
            )
            manifest = json.loads(
                next((root / "state" / "runs").glob("*/manifest.json")).read_text()
            )

        self.assertEqual(calls, 2)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(manifest["llm"]["pending_batches"], 0)

    def test_missing_duplicate_and_unknown_decisions_are_retry_pending(self):
        groups = [term_group("DataMesh"), term_group("DataFabric")]
        cases = {
            "missing": [ignore_decision("DataMesh")],
            "duplicate": [ignore_decision("DataMesh"), ignore_decision("DataMesh")],
            "unknown": [ignore_decision("DataMesh"), ignore_decision("InventedTerm")],
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, decisions in cases.items():
                with self.subTest(name=name):
                    artifact = inventory._process_llm_batch(
                        root / f"{name}.json",
                        0,
                        groups,
                        [],
                        lambda prompt, timeout, value=decisions: value,
                        timeout=1,
                        retries=0,
                        retry_delay=0,
                    )
                    self.assertEqual(artifact["status"], "retry_pending")
                    self.assertEqual(artifact["decisions"], [])
                    self.assertIn("ValueError", artifact["errors"][-1]["error"])

    def test_decision_evidence_must_belong_to_its_term_group(self):
        mesh = term_group("DataMesh")
        fabric = term_group("DataFabric")
        decisions = [
            ignore_decision("DataMesh", [fabric["evidence"][0]["uri"]]),
            ignore_decision("DataFabric"),
        ]
        with tempfile.TemporaryDirectory() as temp:
            artifact = inventory._process_llm_batch(
                Path(temp) / "batch.json",
                0,
                [mesh, fabric],
                [],
                lambda prompt, timeout: decisions,
                timeout=1,
                retries=0,
                retry_delay=0,
            )

        self.assertEqual(artifact["status"], "retry_pending")
        self.assertIn("not from term group", artifact["errors"][-1]["error"])

    def test_alias_decision_can_bind_candidate_group_with_group_term(self):
        group = term_group("dataAgent")
        decision = ignore_decision("Data Agent", [item["uri"] for item in group["evidence"]])
        decision["decision"] = "alias"
        decision["group_term"] = "dataAgent"

        sanitized = inventory._sanitize_decisions([decision], [group])

        self.assertEqual(sanitized[0]["name"], "Data Agent")
        self.assertEqual(sanitized[0]["group_term"], "dataAgent")

    def test_new_concept_frontmatter_contract_is_retryable(self):
        group = term_group("DataMesh")
        refs = [item["uri"] for item in group["evidence"]]
        base = {
            "decision": "new_concept",
            "name": "DataMesh",
            "aliases": ["Data Mesh"],
            "category": "product_capability",
            "content": valid_content("DataMesh", refs, ["Data Mesh"]),
            "evidence_uris": refs,
            "reason": ["cross-document evidence"],
            "confidence": 0.82,
        }

        def replace_frontmatter(content, mutate):
            marker = content.find("\n---\n", 4)
            metadata = yaml.safe_load(content[4:marker])
            mutate(metadata)
            return "---\n" + yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False) + content[marker:]

        cases = {
            "concept": lambda metadata: metadata.update(concept="OtherConcept"),
            "aliases": lambda metadata: metadata.update(aliases=[]),
            "sources": lambda metadata: metadata.update(sources=list(reversed(refs))),
            "schema": lambda metadata: metadata.pop("latest_version"),
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, mutate in cases.items():
                with self.subTest(name=name):
                    decision = dict(base)
                    decision["content"] = replace_frontmatter(base["content"], mutate)
                    artifact = inventory._process_llm_batch(
                        root / f"frontmatter-{name}.json",
                        0,
                        [group],
                        [],
                        lambda prompt, timeout, value=decision: [value],
                        timeout=1,
                        retries=0,
                        retry_delay=0,
                    )
                    self.assertEqual(artifact["status"], "retry_pending")
                    self.assertIn("ValueError", artifact["errors"][-1]["error"])

    def test_permanent_llm_failure_pauses_without_candidate_then_resume_only_retries_llm(self):
        uris = [f"{TEST_ROOT}/a.md", f"{TEST_ROOT}/b.md"]
        client = FakeOpenViking(
            {uri: f"DataMesh 正文 {index}" for index, uri in enumerate(uris)}
        )

        def broken(prompt, timeout):
            raise TimeoutError("still unavailable")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            state = root / "state"
            first = self._execute(
                store,
                client,
                state,
                invoker=broken,
                llm_retries=1,
            )
            reads_after_first = {uri: list(calls) for uri, calls in client.page_calls.items()}
            manifest_path = next((state / "runs").glob("*/manifest.json"))
            run_id = json.loads(manifest_path.read_text())["run_id"]

            self.assertEqual(first["status"], "paused_retryable")
            self.assertEqual(first["candidate_ids"], [])
            self.assertEqual(store.list_candidates(), [])

            second = self._execute(
                store,
                client,
                state,
                invoker=successful_invoker,
                resume_run_id=run_id,
            )

            self.assertEqual(second["status"], "completed")
            self.assertEqual(second["candidate_count"], 1)
            self.assertEqual(
                {uri: list(calls) for uri, calls in client.page_calls.items()},
                reads_after_first,
            )
            llm_artifact = json.loads(next((state / "runs" / run_id / "llm").glob("*.json")).read_text())
            self.assertEqual(llm_artifact["attempts"], 3)

    def test_global_alias_dedup_creates_only_first_candidate(self):
        refs = ["viking://a", "viking://b"]
        decisions = [
            {
                "decision": "new_concept",
                "name": "DataMesh",
                "aliases": ["DataFabric"],
                "content": valid_content("DataMesh", refs, ["DataFabric"]),
                "evidence_uris": refs,
                "confidence": 0.8,
            },
            {
                "decision": "new_concept",
                "name": "data-fabric",
                "aliases": [],
                "content": valid_content("data-fabric", refs),
                "evidence_uris": refs,
                "confidence": 0.8,
            },
        ]
        with tempfile.TemporaryDirectory() as temp:
            store = self._store(Path(temp))
            saved = inventory._save_candidates(store, "run-1", decisions, [])
            resumed = inventory._save_candidates(store, "run-1", decisions, [])
            audit_lines = (
                store.state_root / "logs" / "concept-agent-audit.jsonl"
            ).read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["concept"], "DataMesh")
        self.assertEqual([item["candidate_id"] for item in resumed], [saved[0]["candidate_id"]])
        self.assertEqual(len(audit_lines), 1)

    def test_single_flight_rejects_overlapping_execute(self):
        uri = f"{TEST_ROOT}/a.md"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            state = root / "state"
            with inventory._single_flight(state):
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    self._execute(store, FakeOpenViking({uri: "DataMesh"}), state)

    def test_main_is_disabled_before_state_or_auto_publish(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / "result.json"
            stdout = io.StringIO()
            with mock.patch.object(inventory, "execute") as execute, redirect_stdout(stdout):
                rc = inventory.main(
                    [
                        "--codex-root",
                        str(root / "codex"),
                        "--state-dir",
                        str(root / "pm-loop"),
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(stdout.getvalue())["status"], "disabled")
            execute.assert_not_called()
            self.assertFalse(output.exists())

            forbidden = root / "forbidden.json"
            stdout = io.StringIO()
            with mock.patch.object(inventory, "execute") as execute, redirect_stdout(stdout):
                rc = inventory.main(
                    [
                        "--codex-root",
                        str(root / "codex"),
                        "--auto-approve-publish",
                        "--output",
                        str(forbidden),
                    ]
                )
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(stdout.getvalue())["status"], "disabled")
            execute.assert_not_called()
            self.assertFalse(forbidden.exists())

    def test_result_contract_can_prove_5735_of_5735(self):
        documents = [
            {"page_count": 1, "char_count": 10, "byte_count": 10}
            for _ in range(5735)
        ]
        manifest = {
            "run_id": "deep-run",
            "status": "completed",
            "resource_count": 5735,
            "resource_snapshot_hash": "sha256:test",
            "llm": {"pending_batches": 0},
        }
        result = inventory._result(
            manifest,
            documents=documents,
            unreadable_count=0,
            term_count=0,
            decisions=[],
            candidates=[],
        )
        self.assertEqual(result["resource_count"], 5735)
        self.assertEqual(result["read_count"], 5735)
        self.assertEqual(result["snapshot"]["deep_read_coverage"], 1.0)
        self.assertEqual(result["full_page_count"], 5735)
        self.assertEqual(result["full_byte_count"], 57350)

    def test_large_json_artifact_round_trips_with_legacy_filename_fallback(self):
        payload = {
            "schema_version": inventory.EVIDENCE_SCHEMA,
            "documents": [{"uri": "viking://evidence/a", "terms": []}],
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            compressed = root / "batch-00000.json.gz"
            legacy = root / "legacy.json"

            inventory._atomic_json(compressed, payload)
            self.assertEqual(inventory._read_json(compressed), payload)
            # A caller using the historical .json name can discover the new
            # compressed sibling during a rolling upgrade.
            self.assertEqual(inventory._read_json(root / "batch-00000.json"), payload)

            inventory._atomic_json(legacy, payload)
            # A new reader can resume a pre-compression run as well.
            self.assertEqual(inventory._read_json(root / "legacy.json.gz"), payload)

    def test_new_cache_writes_keep_existing_plain_artifacts(self):
        """The gzip migration must be additive; historical JSON stays intact."""
        cache_entries = {
            "viking://evidence/a": {
                "source_revision": "sha256:" + ("a" * 64),
                "content_hash": "sha256:" + ("b" * 64),
                "terms_fingerprint": "sha256:" + ("c" * 64),
                "record": {"uri": "viking://evidence/a", "terms": []},
            }
        }
        dedup_payload = {
            "schema_version": inventory.CONTENT_DEDUP_SCHEMA,
            "document_count": 1,
            "unique_content_count": 1,
            "duplicate_document_count": 0,
            "groups": {},
            "uri_to_content_hash": {},
        }
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            cache_plain = state / "evidence-cache.json"
            dedup_plain = state / "content-dedup.json"
            cache_gzip = inventory._evidence_cache_path(state)
            dedup_gzip = inventory._content_dedup_path(state)

            # Simulate an existing pre-migration installation.
            legacy_cache = {
                "schema_version": inventory.EVIDENCE_CACHE_SCHEMA,
                "updated_at": "2026-08-21T00:00:00Z",
                "entries": {
                    "viking://evidence/legacy": {
                        "record": {"uri": "viking://evidence/legacy"}
                    }
                },
            }
            legacy_dedup = {
                "schema_version": inventory.CONTENT_DEDUP_SCHEMA,
                "document_count": 1,
                "unique_content_count": 1,
                "duplicate_document_count": 0,
                "groups": {},
                "uri_to_content_hash": {},
            }
            inventory._atomic_json(cache_plain, legacy_cache)
            inventory._atomic_json(dedup_plain, legacy_dedup)

            inventory._persist_evidence_cache(cache_gzip, cache_entries)
            inventory._atomic_json(dedup_gzip, dedup_payload)

            self.assertTrue(cache_plain.exists())
            self.assertTrue(cache_gzip.exists())
            self.assertTrue(dedup_plain.exists())
            self.assertTrue(dedup_gzip.exists())
            self.assertEqual(inventory._read_json(cache_plain), legacy_cache)
            self.assertEqual(
                inventory._read_json(cache_gzip)["entries"], cache_entries
            )
            self.assertEqual(inventory._read_json(dedup_plain), legacy_dedup)
            self.assertEqual(inventory._read_json(dedup_gzip), dedup_payload)

    def test_corrupt_compressed_artifact_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "batch.json.gz"
            path.write_bytes(b"not a gzip stream")

            sentinel = {"rebuild": True}
            self.assertIs(inventory._read_json(path, sentinel), sentinel)

    def test_new_run_uses_compressed_large_artifact_paths(self):
        uri = f"{TEST_ROOT}/gzip-artifact.md"
        body = "DataMesh gzip artifact evidence\n"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = self._store(root)
            result = self._execute(
                store,
                FakeOpenViking({uri: body}),
                root / "state",
                deterministic=True,
                read_batch_size=1,
            )
            run_root = root / "state" / "runs" / result["run_id"]

            self.assertTrue((run_root / "evidence" / "batch-00000.json.gz").exists())
            self.assertFalse((run_root / "evidence" / "batch-00000.json").exists())
            self.assertTrue((run_root / "term-groups-v2.json.gz").exists())
            manifest = json.loads((run_root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["artifact_storage"]["large_json_encoding"], "gzip")
            self.assertEqual(manifest["artifact_storage"]["legacy_fallback"], True)
            self.assertIn(
                "evidence_cache",
                manifest["artifact_storage"]["compressed_artifacts"],
            )
            self.assertIn(
                "content_dedup",
                manifest["artifact_storage"]["compressed_artifacts"],
            )
            self.assertEqual(
                manifest["artifact_storage"]["evidence_cache"],
                "evidence-cache.json.gz",
            )
            self.assertEqual(
                manifest["artifact_storage"]["content_dedup"],
                "content-dedup.json.gz",
            )

    def test_partial_legacy_evidence_checkpoint_resumes_into_gzip(self):
        uri = f"{TEST_ROOT}/legacy-checkpoint.md"
        body = "DataMesh legacy checkpoint evidence\n"
        legacy_artifact = {
            "schema_version": inventory.EVIDENCE_SCHEMA,
            "batch_index": 0,
            "status": "retry_pending",
            "uris": [uri],
            "documents": [],
            "errors": [{"uri": uri, "error": "temporary"}],
            "completed_count": 0,
            "pending_count": 1,
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy = root / "batch-00000.json"
            compressed = root / "batch-00000.json.gz"
            inventory._atomic_json(legacy, legacy_artifact)

            result = inventory._scan_evidence_batch(
                FakeOpenViking({uri: body}),
                [uri],
                0,
                compressed,
                page_size=100,
                max_workers=1,
                seed_terms=[],
            )

            self.assertEqual(result["status"], "completed")
            self.assertTrue(compressed.exists())
            self.assertEqual(len(inventory._read_json(compressed)["documents"]), 1)


if __name__ == "__main__":
    unittest.main()
