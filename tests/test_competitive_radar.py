from __future__ import annotations

import contextlib
import io
import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from competitive_radar import brief, ingest, main, normalize_url, structured_page_fields, submit  # noqa: E402
from competitive_radar_read_model import (  # noqa: E402
    CompetitiveRadarReadModel,
    _evidence_level,
    _safe_source_url,
    _signal_cards,
)
from pm_system_store import PMSystemStore  # noqa: E402


class CompetitiveRadarTests(unittest.TestCase):
    def test_every_registered_source_has_public_dom_fallback(self) -> None:
        from competitive_radar import BROWSER_FALLBACK_HOSTS, _browser_fallback_policy

        registry_path = ROOT / "docs" / "产品情报监控" / "竞品雷达" / "source-registry.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        sources = registry.get("sources") or []
        self.assertEqual(len(sources), 8)
        for source in sources:
            policy = _browser_fallback_policy(source)
            self.assertIsNotNone(policy, source.get("source_id"))
            host = source["url"].split("/", 3)[2].split(":", 1)[0].lower()
            self.assertIn(host, BROWSER_FALLBACK_HOSTS)
            self.assertIn("fetch_wall_timeout", policy["on_failure"])

    def test_structured_page_fields_extract_title_description_and_headings(self) -> None:
        fields = structured_page_fields(
            '<html><head><title>Agent News</title><meta name="description" content="Runtime updates"></head>'
            '<body><h1>Computer Use</h1><h2>Recovery</h2><script>ignored()</script></body></html>'
        )
        self.assertEqual(fields["page_title"], "Agent News")
        self.assertEqual(fields["meta_description"], "Runtime updates")
        self.assertEqual(fields["headlines"], ["Computer Use", "Recovery"])

    def test_submit_normalizes_and_deduplicates_public_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = submit("https://example.com/a?utm_source=x&b=2", state_root=root)
            second = submit("https://example.com/a?b=2", state_root=root)
            self.assertEqual(first["status"], "accepted")
            self.assertEqual(second["status"], "deduplicated")
            self.assertEqual(first["url"], "https://example.com/a?b=2")

    def test_submit_rejects_credentials_and_private_hosts(self) -> None:
        self.assertEqual(normalize_url("https://user:pass@example.com")[1], "credentials_in_url")
        self.assertEqual(normalize_url("http://127.0.0.1:8080/a")[1], "private_host_rejected")

    def test_fetch_rejects_public_hostname_resolving_to_private_address(self) -> None:
        from competitive_radar import _fetch

        with patch("competitive_radar.socket.getaddrinfo", return_value=[(2, 1, 6, "", ("127.0.0.1", 443))]):
            result = _fetch("https://public.example/news")
        self.assertEqual(result["retrieval_status"], "rejected")
        self.assertEqual(result["failure_reason"], "private_resolved_host_rejected")

    def test_ingest_and_brief_keep_evidence_and_reviewer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            registry = project / "docs" / "产品情报监控" / "竞品雷达"
            registry.mkdir(parents=True)
            (registry / "source-registry.json").write_text(json.dumps({"sources": [{"source_id": "test-palantir", "source": "fixture", "url": "https://example.com/news", "vendor": "Palantir", "product": "AIP", "route": "A", "capability_layer": "Action", "trust": "official", "fetch_mode": "fixture"}]}, ensure_ascii=False), encoding="utf-8")
            evidence = "New Action capability adds governed execution logs, scoped tool permissions, and replayable recovery for enterprise agent workflows."
            translation = "新的 Action 能力为企业 Agent 工作流新增受治理的执行日志、范围受限的工具权限和可回放恢复能力。"
            (registry / "translation-overrides.json").write_text(json.dumps({"translations": {evidence: translation}}, ensure_ascii=False), encoding="utf-8")
            def fake_fetch(url: str, timeout: int = 8, headers: dict | None = None, wall_timeout: int = 10) -> dict:
                return {"retrieval_status": "ok", "http_status": 200, "content_type": "text/plain", "captured_at": "2026-09-02T02:00:00Z", "content_hash": "sha256:fixture", "body_bytes": len(evidence), "truncated": False, "title_excerpt": "Fixture", "body_excerpt": evidence}
            with patch("competitive_radar._fetch_bounded", side_effect=fake_fetch):
                result = ingest(state_root=root / "state", project_root=project, run_id="ingest-test")
            self.assertEqual(result["ok_count"], 1)
            report = brief(state_root=root / "state", project_root=project, run_id="brief-test")
            self.assertEqual(report["status"], "reviewed")
            self.assertTrue(Path(report["html"]).is_file())
            rendered = Path(report["html"]).read_text(encoding="utf-8")
            self.assertIn("<main class=\"report-shell\">", rendered)
            self.assertIn("<h1>Agent 竞品雷达</h1>", rendered)
            self.assertIn("<table>", rendered)
            self.assertIn("本期实际抓取内容", rendered)
            self.assertIn("具体标题、摘要或条目", rendered)
            self.assertIn("具体抓取到", rendered)
            self.assertIn("证据覆盖率", rendered)
            self.assertLess(rendered.find('<main class="report-shell">'), rendered.find("<pre>"))
            pointer = json.loads((root / "state" / "latest.json").read_text(encoding="utf-8"))
            self.assertEqual(pointer["report_status"], "reviewed")
            self.assertEqual(pointer["review_run_id"], report["review_run_id"])

    def test_brief_contains_competitor_summary_and_shengsuan_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            registry = project / "docs" / "产品情报监控" / "竞品雷达"
            registry.mkdir(parents=True)
            (registry / "source-registry.json").write_text(json.dumps({"sources": []}), encoding="utf-8")
            state = root / "state"
            state.mkdir(parents=True)
            (state / "latest-ingest.json").write_text(
                json.dumps(
                    {
                        "captured_at": "2026-09-02T02:00:00Z",
                        "signals": [
                            {
                                "source_id": "palantir-news",
                                "source": "Palantir 官方新闻",
                                "source_url": "https://example.com/palantir",
                                "vendor": "Palantir",
                                "product": "AIP / Foundry",
                                "route": "A",
                                "retrieval_status": "ok",
                                "evidence_id": "e1",
                                "source_snapshot_uri": "/tmp/e1",
                                "content_hash": "sha256:e1",
                                "locator": {"kind": "body_excerpt"},
                                "body_excerpt": "<title>Newsroom</title><h1>Ontology and actions</h1>",
                                "original_evidence": [{"original": "Palantir Foundry connects ontology data to governed actions so delivery teams can trace decisions and execution outcomes.", "translation_zh": "Palantir Foundry 将本体数据连接到受治理的 Action，使交付团队可以追溯决策和执行结果。", "translation_status": "translated", "kind": "detail"}],
                            },
                            {
                                "source_id": "anthropic-news",
                                "source": "Anthropic 官方发布",
                                "source_url": "https://example.com/anthropic",
                                "vendor": "Anthropic",
                                "product": "Claude / Computer Use",
                                "route": "C",
                                "retrieval_status": "ok",
                                "evidence_id": "e2",
                                "source_snapshot_uri": "/tmp/e2",
                                "content_hash": "sha256:e2",
                                "locator": {"kind": "body_excerpt"},
                                "body_excerpt": "<title>Computer Use</title><h1>Recovery controls</h1>",
                                "original_evidence": [{"original": "Computer Use adds recovery controls and execution visibility for agents working across desktop applications and browsers.", "translation_zh": "Computer Use 为跨桌面应用和浏览器工作的 Agent 增加恢复控制与执行可见性。", "translation_status": "translated", "kind": "detail"}],
                            },
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            report = brief(state_root=state, project_root=project, run_id="brief-summary")
            rendered = Path(report["html"]).read_text(encoding="utf-8")
            self.assertIn("本期竞品总结", rendered)
            self.assertIn("对胜算的参考", rendered)
            self.assertIn("Palantir", rendered)
            self.assertIn("Computer Use", rendered)
            self.assertIn("执行 Runtime", rendered)

    def test_repeated_ingest_does_not_duplicate_unchanged_signal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            registry = project / "docs" / "产品情报监控" / "竞品雷达"
            registry.mkdir(parents=True)
            (registry / "source-registry.json").write_text(json.dumps({"sources": [{"source_id": "stable", "url": "https://example.com/news", "vendor": "Vendor", "product": "Agent", "route": "A", "trust": "official", "fetch_mode": "fixture"}]}), encoding="utf-8")
            fetched = {"retrieval_status": "ok", "http_status": 200, "content_type": "text/plain", "captured_at": "2026-09-02T02:00:00Z", "content_hash": "sha256:unchanged", "body_excerpt": "same"}
            with patch("competitive_radar._fetch_bounded", return_value=fetched):
                ingest(state_root=root / "state", project_root=project, run_id="one")
                ingest(state_root=root / "state", project_root=project, run_id="two")
            ledger = (root / "state" / "signal-ledger.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(ledger), 1)
            watermarks = json.loads((root / "state" / "source-watermarks.json").read_text(encoding="utf-8"))
            self.assertEqual(watermarks["stable"]["content_hash"], "sha256:unchanged")

    def test_youtube_fetch_policy_retries_with_atom_accept_header(self) -> None:
        from competitive_radar import _fetch_source

        source = {
            "fetch_policy": {
                "timeout_seconds": 20,
                "max_attempts": 2,
                "headers": {"Accept": "application/atom+xml,application/xml;q=0.9"},
            }
        }
        unavailable = {"retrieval_status": "unavailable", "failure_reason": "http_500"}
        available = {"retrieval_status": "ok", "content_hash": "sha256:atom"}
        with patch("competitive_radar._fetch_bounded", side_effect=[unavailable, available]) as fetch:
            result = _fetch_source(source, "https://www.youtube.com/feeds/videos.xml?channel_id=fixture")
        self.assertEqual(result["retrieval_status"], "ok")
        self.assertEqual(result["request_attempts"], 2)
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(fetch.call_args.kwargs["timeout"], 20)
        self.assertEqual(fetch.call_args.kwargs["headers"]["Accept"], "application/atom+xml,application/xml;q=0.9")
        self.assertEqual(fetch.call_args.kwargs["wall_timeout"], 22)

    def test_fetch_bounded_enforces_wall_timeout(self) -> None:
        from competitive_radar import _fetch_bounded

        with patch("competitive_radar.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=["fetch"], timeout=10)):
            result = _fetch_bounded("https://example.com/news", timeout=8, headers={}, wall_timeout=10)
        self.assertEqual(result["retrieval_status"], "unavailable")
        self.assertEqual(result["failure_reason"], "fetch_wall_timeout")

    def test_fetch_bounded_reads_child_json_result(self) -> None:
        from competitive_radar import _fetch_bounded

        child_result = {"retrieval_status": "ok", "http_status": 200, "content_hash": "sha256:child"}
        completed = subprocess.CompletedProcess(args=["fetch"], returncode=0, stdout="diagnostic\n" + json.dumps(child_result) + "\n", stderr="")
        with patch("competitive_radar.subprocess.run", return_value=completed):
            result = _fetch_bounded("https://example.com/news", timeout=8, headers={}, wall_timeout=10)
        self.assertEqual(result, child_result)

    def test_fetch_cli_runs_one_unbounded_primitive_fetch(self) -> None:
        child_result = {"retrieval_status": "ok", "http_status": 200, "content_hash": "sha256:fetch-cli"}
        output = io.StringIO()
        with patch("competitive_radar._fetch", return_value=child_result) as fetch, contextlib.redirect_stdout(output):
            code = main(["fetch", "https://example.com/news", "--timeout", "12", "--headers-json", '{"Accept":"application/json"}'])
        self.assertEqual(code, 0)
        fetch.assert_called_once_with("https://example.com/news", timeout=12, headers={"Accept": "application/json"})
        self.assertEqual(json.loads(output.getvalue()), child_result)

    def test_explicit_browser_fallback_persists_public_dom_evidence(self) -> None:
        from competitive_radar import _source_record

        source = {
            "source_id": "openai-agents",
            "source": "OpenAI 官方发布",
            "url": "https://openai.com/news/",
            "vendor": "OpenAI",
            "product": "Agent / Deep Research",
            "route": "B",
            "trust": "official",
            "fetch_mode": "web",
            "browser_fallback": {"mode": "public_dom", "on_failure": ["http_403"], "timeout_seconds": 35},
        }
        fallback = {
            "retrieval_status": "ok",
            "http_status": 200,
            "content_type": "text/html; source=browser-dom",
            "captured_at": "2026-09-02T02:00:00Z",
            "content_hash": "sha256:browser",
            "body_bytes": 64,
            "truncated": False,
            "page_title": "OpenAI 新闻",
            "meta_description": "公开产品发布",
            "headlines": ["Agent 更新"],
            "title_excerpt": "OpenAI 新闻",
            "body_excerpt": "Agent 更新",
            "resolved_source_url": "https://openai.com/zh-Hans-CN/news/",
            "retrieval_method": "browser_dom_fallback",
        }
        with tempfile.TemporaryDirectory() as temp, patch(
            "competitive_radar._fetch_source", return_value={"retrieval_status": "unavailable", "failure_reason": "http_403"}
        ), patch("competitive_radar._browser_fetch", return_value=fallback) as browser:
            record = _source_record(source, run_id="browser-test", state_root=Path(temp))
            snapshot_exists = Path(record["source_snapshot_uri"]).is_file()
        self.assertEqual(record["retrieval_status"], "ok")
        self.assertEqual(record["retrieval_method"], "browser_dom_fallback")
        self.assertEqual(record["http_failure_reason"], "http_403")
        self.assertTrue(record["evidence_id"])
        self.assertTrue(snapshot_exists)
        browser.assert_called_once()

    def test_browser_fallback_is_not_enabled_for_unconfigured_or_non_whitelisted_source(self) -> None:
        from competitive_radar import _source_record

        source = {
            "source_id": "manual",
            "url": "https://example.com",
            "vendor": "Vendor",
            "product": "Product",
            "route": "B",
            "browser_fallback": {"mode": "public_dom", "on_failure": ["http_403"]},
        }
        with tempfile.TemporaryDirectory() as temp, patch(
            "competitive_radar._fetch_source", return_value={"retrieval_status": "unavailable", "failure_reason": "http_403"}
        ), patch("competitive_radar._browser_fetch") as browser:
            record = _source_record(source, run_id="manual-test", state_root=Path(temp))
        self.assertEqual(record["failure_reason"], "http_403")
        browser.assert_not_called()

    def test_browser_fallback_failure_preserves_http_cause(self) -> None:
        from competitive_radar import _source_record

        source = {
            "source_id": "openai-agents",
            "url": "https://openai.com/news/",
            "browser_fallback": {"mode": "public_dom", "on_failure": ["http_403"]},
        }
        with tempfile.TemporaryDirectory() as temp, patch(
            "competitive_radar._fetch_source", return_value={"retrieval_status": "unavailable", "failure_reason": "http_403"}
        ), patch(
            "competitive_radar._browser_fetch", return_value={"retrieval_status": "unavailable", "failure_reason": "browser_timeout"}
        ):
            record = _source_record(source, run_id="browser-fail", state_root=Path(temp))
        self.assertEqual(record["failure_reason"], "http_403")
        self.assertTrue(record["fallback_attempted"])
        self.assertEqual(record["fallback_failure_reason"], "browser_timeout")

    def test_browser_fetch_reads_explicit_marker_from_stderr_and_closes_task(self) -> None:
        from competitive_radar import _browser_fetch

        payload = {
            "url": "https://openai.com/zh-Hans-CN/news/",
            "title": "OpenAI 新闻",
            "description": "公开产品发布",
            "headings": ["最新发布"],
            "entries": [],
            "text": "公开页面正文",
        }
        stderr = "PM_COMPETITIVE_RADAR_BROWSER_RESULT:" + json.dumps(payload, ensure_ascii=False) + "\nPM_COMPETITIVE_RADAR_BROWSER_TASK:123\n"
        completed = subprocess.CompletedProcess(args=["ego-browser", "nodejs"], returncode=0, stdout="", stderr=stderr)
        with patch("competitive_radar.subprocess.run", return_value=completed) as run:
            result = _browser_fetch("https://openai.com/news/", source_id="openai-agents", run_id="stderr-test", timeout=35)
        self.assertEqual(result["retrieval_status"], "ok")
        self.assertEqual(result["retrieval_method"], "browser_dom_fallback")
        self.assertEqual(result["resolved_source_url"], "https://openai.com/zh-Hans-CN/news/")
        self.assertEqual(run.call_count, 2)

    def test_browser_fetch_prefers_atom_entries_over_xml_viewer_noise(self) -> None:
        from competitive_radar import _browser_fetch

        payload = {
            "url": "https://www.youtube.com/feeds/videos.xml?channel_id=fixture",
            "title": "",
            "description": "",
            "headings": [],
            "entries": [{"title": "Use Your Computer and Browser", "published": "2026-08-28T20:51:46+00:00", "link": "https://youtu.be/fixture"}],
            "text": "This XML file does not appear to have any style information associated with it.",
        }
        stderr = "PM_COMPETITIVE_RADAR_BROWSER_RESULT:" + json.dumps(payload, ensure_ascii=False) + "\nPM_COMPETITIVE_RADAR_BROWSER_TASK:123\n"
        completed = subprocess.CompletedProcess(args=["ego-browser", "nodejs"], returncode=0, stdout="", stderr=stderr)
        with patch("competitive_radar.subprocess.run", return_value=completed):
            result = _browser_fetch("https://www.youtube.com/feeds/videos.xml?channel_id=fixture", source_id="youtube-ai", run_id="atom-clean-test", timeout=35)
        self.assertEqual(result["retrieval_status"], "ok")
        self.assertIn("Use Your Computer and Browser", result["body_excerpt"])
        self.assertNotIn("style information", result["body_excerpt"])

    def test_atom_entries_keep_summary_content(self) -> None:
        from competitive_radar import _atom_entries

        entries = _atom_entries(
            '<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Use Your Computer</title>'
            '<published>2026-09-03T00:00:00Z</published><link href="https://example.com/video"/>'
            '<summary>See how an agent completes work across desktop apps and a browser.</summary></entry></feed>'
        )
        self.assertEqual(entries[0]["title"], "Use Your Computer")
        self.assertEqual(entries[0]["summary"], "See how an agent completes work across desktop apps and a browser.")

    def test_evidence_prefers_detail_body_and_link_over_index_title(self) -> None:
        from competitive_radar import _source_evidence_records

        records = _source_evidence_records(
            {
                "source_id": "fixture",
                "page_title": "Newsroom",
                "details": [{"title": "Agent Runtime update", "text": "The runtime adds replayable execution logs and recovery controls.", "href": "https://example.com/news/runtime"}],
            },
            translations={"Agent Runtime update；The runtime adds replayable execution logs and recovery controls.": "Agent Runtime 新增可回放执行日志与恢复控制。"},
        )
        self.assertEqual(records[0]["kind"], "detail")
        self.assertEqual(records[0]["link"], "https://example.com/news/runtime")
        self.assertIn("replayable execution logs", records[0]["original"])

    def test_content_depth_marks_title_only_source_as_metadata(self) -> None:
        from competitive_radar import _content_depth, _review

        row = {
            "source": "Index only",
            "retrieval_status": "ok",
            "content_depth": "metadata",
            "evidence_id": "e1",
            "source_snapshot_uri": "/tmp/e1",
            "content_hash": "sha256:e1",
            "locator": {"kind": "body_excerpt"},
            "original_evidence": [{"original": "Category title", "translation_zh": "分类标题", "translation_status": "translated", "kind": "headline"}],
        }
        self.assertEqual(_content_depth(row), "metadata")
        review = _review("# draft", [row])
        self.assertEqual(review["status"], "PASS")
        self.assertIn("Index only", review["shallow_sources"])

    def test_report_shows_content_depth_and_detail_link(self) -> None:
        from competitive_radar import _report_html

        row = {
            "source_id": "fixture",
            "source": "Fixture",
            "source_url": "https://example.com/news",
            "vendor": "Vendor",
            "product": "Agent",
            "route": "A",
            "retrieval_status": "ok",
            "content_depth": "detail",
            "evidence_id": "e1",
            "source_snapshot_uri": "/tmp/e1",
            "content_hash": "sha256:e1",
            "locator": {"kind": "body_excerpt"},
            "original_evidence": [{"original": "Agent update: details", "translation_zh": "Agent 更新：详细内容", "translation_status": "translated", "kind": "detail", "link": "https://example.com/news/detail"}],
        }
        rendered = _report_html("# draft", signals=[row], valid=[row], report_status="reviewed", gate_status="PASS", review={"status": "PASS"})
        self.assertIn("正文/摘要覆盖率", rendered)
        self.assertIn("内容深度 detail", rendered)
        self.assertIn('href="https://example.com/news/detail"', rendered)

    def test_empty_brief_does_not_create_or_replace_latest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            registry = project / "docs" / "产品情报监控" / "竞品雷达"
            registry.mkdir(parents=True)
            (registry / "source-registry.json").write_text(json.dumps({"sources": []}), encoding="utf-8")
            state = root / "state"
            (state / "latest-ingest.json").parent.mkdir(parents=True)
            (state / "latest-ingest.json").write_text(json.dumps({"signals": [{"source_id": "down", "retrieval_status": "unavailable", "failure_reason": "timeout"}]}), encoding="utf-8")
            report = brief(state_root=state, project_root=project, run_id="brief-degraded")
            self.assertEqual(report["status"], "degraded")
            self.assertFalse((state / "latest.json").exists())

    def test_partial_collection_publishes_latest_with_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            registry = project / "docs" / "产品情报监控" / "竞品雷达"
            registry.mkdir(parents=True)
            (registry / "source-registry.json").write_text(
                json.dumps(
                    {
                        "sources": [
                            {"source_id": "ok", "source": "ok", "url": "https://example.com/ok", "vendor": "Vendor", "product": "Agent", "route": "A", "trust": "official"},
                            {"source_id": "down", "source": "down", "url": "https://example.com/down", "vendor": "Vendor", "product": "Agent", "route": "B", "trust": "official"},
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            state = root / "state"
            state.mkdir(parents=True)
            (state / "latest-ingest.json").write_text(
                json.dumps(
                    {
                        "captured_at": "2026-09-02T02:00:00Z",
                        "signals": [
                            {"source_id": "ok", "source": "ok", "source_url": "https://example.com/ok", "vendor": "Vendor", "product": "Agent", "route": "A", "retrieval_status": "ok", "evidence_id": "e1", "source_snapshot_uri": "/tmp/e1", "content_hash": "sha256:e1", "locator": {"kind": "body_excerpt"}, "body_excerpt": "<p>Readable signal with an actual capability description for a governed agent runtime and recovery workflow.</p>", "original_evidence": [{"original": "Readable signal with an actual capability description for a governed agent runtime and recovery workflow.", "translation_zh": "可读信号：说明一个受治理 Agent Runtime 及其恢复工作流的实际能力。", "translation_status": "translated", "kind": "body_excerpt"}]},
                            {"source_id": "down", "source": "down", "source_url": "https://example.com/down", "vendor": "Vendor", "product": "Agent", "route": "B", "retrieval_status": "unavailable", "failure_reason": "timeout"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            report = brief(state_root=state, project_root=project, run_id="brief-partial")
            self.assertEqual(report["status"], "reviewed")
            self.assertEqual(report["gate_status"], "PASS_WITH_WARN")
            self.assertEqual(report["evidence_coverage"], 0.5)
            pointer = json.loads((state / "latest.json").read_text(encoding="utf-8"))
            self.assertEqual(pointer["gate_status"], "PASS_WITH_WARN")
            self.assertEqual(pointer["evidence_coverage"], 0.5)
            rendered = Path(report["html"]).read_text(encoding="utf-8")
            self.assertIn("Readable signal", rendered)
            self.assertNotIn("&lt;p&gt;Readable signal&lt;/p&gt;", rendered)
            self.assertIn("本期有来源不可用", rendered)

    def test_pointer_is_persisted_in_coordination_store_and_read_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            store = PMSystemStore(db)
            store.upsert_competitive_radar_latest({"run_id": "r1", "report_uri": str(root / "report.md"), "html_uri": str(root / "report.html"), "report_hash": "sha256:r1", "report_status": "reviewed", "gate_status": "PASS", "review_run_id": "review:r1", "evidence_coverage": 1.0, "published_at": "2026-09-02T02:00:00Z"})
            model = CompetitiveRadarReadModel(db_path=db, state_root=root / "state", project_root=root)
            self.assertEqual(model.latest()["run_id"], "r1")
            self.assertEqual(model.snapshot()["source_version"], "sha256:r1")

    def test_read_model_does_not_treat_missing_report_pointer_as_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = PMSystemStore(root / "pm.db")
            store.upsert_competitive_radar_latest({
                "run_id": "r-missing",
                "report_uri": str(root / "missing.md"),
                "html_uri": str(root / "missing.html"),
                "report_hash": "sha256:r-missing",
                "report_status": "reviewed",
                "gate_status": "PASS",
                "review_run_id": "review:r-missing",
                "evidence_coverage": 1.0,
                "published_at": "2026-09-04T02:00:00Z",
            })
            snapshot = CompetitiveRadarReadModel(
                db_path=root / "pm.db", state_root=root / "state", project_root=root
            ).snapshot()
            self.assertFalse(snapshot["latest"]["report_available"])
            self.assertFalse(snapshot["latest"]["html_available"])

    def test_control_plane_report_link_requires_confirmed_file_availability(self) -> None:
        source = (ROOT / "web" / "pm-loop-control-plane" / "index.html").read_text(encoding="utf-8")
        self.assertIn(
            "var available=latest.report_available===true||latest.html_available===true;",
            source,
        )
        self.assertNotIn(
            "Boolean(latest.report_available||latest.html_available||latest.report_uri)",
            source,
        )

    def test_control_plane_suppresses_credential_bearing_source_links(self) -> None:
        source = (ROOT / "web" / "pm-loop-control-plane" / "index.html").read_text(encoding="utf-8")
        self.assertIn(
            "!candidate.username&&!candidate.password",
            source,
        )

    def test_read_model_projects_signal_cards_and_explicit_capability_map(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            report = root / "report.md"
            html = root / "report.html"
            report.write_text("# report\n", encoding="utf-8")
            html.write_text("<h1>report</h1>", encoding="utf-8")
            PMSystemStore(root / "pm.db").upsert_competitive_radar_latest({
                "run_id": "r-map", "report_uri": str(report), "html_uri": str(html),
                "report_hash": "sha256:r-map", "report_status": "reviewed", "gate_status": "PASS",
                "review_run_id": "review:r-map", "evidence_coverage": 1.0, "published_at": "2026-09-04T02:00:00Z",
            })
            (state / "latest-ingest.json").write_text(json.dumps({"run_id": "ingest-map", "captured_at": "2026-09-04T02:00:00Z", "signals": [
                {"signal_id": "s1", "source_id": "official", "source": "Official", "vendor": "Vendor A", "product": "Agent", "route": "A", "capability_layer": "Ontology/Action/Governance", "fact_type": "official_fact", "captured_at": "2026-09-04T01:00:00Z", "retrieval_status": "ok", "content_depth": "detail", "evidence_id": "e1", "source_snapshot_uri": "/tmp/e1", "content_hash": "sha256:e1", "locator": {"kind": "detail"}, "original_evidence": [{"original": "Governed action evidence", "translation_zh": "受治理 Action 证据", "translation_status": "translated", "kind": "detail"}], "requirement_ids": ["req-1"], "task_ids": ["task-1"], "version_ids": ["ver-1"]},
                {"signal_id": "s2", "source_id": "community", "source": "Community", "vendor": "Vendor B", "product": "Runtime", "route": "C", "capability_layer": "Runtime/MCP/Skills", "fact_type": "community_feedback", "captured_at": "2026-09-04T01:01:00Z", "retrieval_status": "ok", "content_depth": "card", "evidence_id": "e2", "source_snapshot_uri": "/tmp/e2", "content_hash": "sha256:e2", "locator": {"kind": "card"}, "original_evidence": [{"original": "Runtime signal", "translation_zh": "Runtime 信号", "translation_status": "translated", "kind": "card"}]},
                {"signal_id": "s3", "source_id": "index", "source": "Index", "vendor": "Vendor C", "product": "Unknown", "route": "B", "capability_layer": "Research/Personal workbench", "fact_type": "official_fact", "captured_at": "2026-09-04T01:02:00Z", "retrieval_status": "ok", "content_depth": "metadata", "evidence_id": "e3", "source_snapshot_uri": "/tmp/e3", "content_hash": "sha256:e3", "locator": {"kind": "title"}, "original_evidence": [{"original": "Title only", "translation_zh": "", "translation_status": "missing", "kind": "headline"}]},
            ]}, ensure_ascii=False), encoding="utf-8")
            snapshot = CompetitiveRadarReadModel(db_path=root / "pm.db", state_root=state, project_root=root).snapshot()
            self.assertEqual(snapshot["schema_version"], "competitive-radar.read-model.v2")
            cards = {item["signal_id"]: item for item in snapshot["signal_cards"]}
            self.assertEqual(cards["s1"]["evidence_level"], "A")
            self.assertEqual(cards["s1"]["association_status"], "explicit")
            self.assertEqual(cards["s1"]["suggested_action"], "验证")
            self.assertEqual(cards["s2"]["evidence_level"], "C")
            self.assertEqual(cards["s2"]["threat_opportunity"], "机会候选")
            self.assertEqual(cards["s3"]["evidence_level"], "D")
            self.assertEqual(cards["s3"]["suggested_action"], "补证")
            cell = snapshot["capability_map"]["cells"][0]
            self.assertEqual(cell["association_status"], "explicit")
            self.assertIn("禁止标题语义映射", snapshot["capability_map"]["association_policy"])

    def test_read_model_keeps_evidence_and_links_bounded(self) -> None:
        self.assertEqual(
            _evidence_level({"content_depth": "summary", "fact_type": "official_fact"}),
            "B",
        )
        self.assertIsNone(_safe_source_url("javascript:alert(1)"))
        self.assertIsNone(_safe_source_url("https://user:pass@example.com/news"))
        self.assertEqual(_safe_source_url("https://example.com/news"), "https://example.com/news")
        cards = _signal_cards([
            {
                "signal_id": "unsafe",
                "source": "Untrusted",
                "vendor": "Vendor",
                "product": "Agent",
                "source_url": "javascript:alert(1)",
                "retrieval_status": "ok",
                "content_depth": "summary",
                "fact_type": "official_fact",
                "original_evidence": [{"original": "A summary", "kind": "summary"}],
            }
        ])
        self.assertEqual(cards[0]["evidence_level"], "B")
        self.assertIsNone(cards[0]["source_url"])
        self.assertEqual(cards[0]["multi_source_status"], "来源标识未记录")
        self.assertIn("来源链接协议或格式不受支持，已抑制跳转", cards[0]["unknown_scope"])
        self.assertEqual(cards[0]["judgement_type"], "分析推断")
        self.assertEqual(cards[0]["threat_opportunity"], "威胁候选")

    def test_read_model_does_not_fallback_to_derived_latest_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            (state / "latest.json").write_text(json.dumps({"run_id": "unpublished"}), encoding="utf-8")
            model = CompetitiveRadarReadModel(db_path=root / "missing.db", state_root=state, project_root=root)
            self.assertIsNone(model.latest())
            self.assertEqual(model.snapshot()["report_status"], "not_recorded")

    def test_key_signal_contains_original_and_chinese_translation(self) -> None:
        from competitive_radar import _source_evidence_records

        records = _source_evidence_records(
            {"source_id": "fixture", "headlines": ["New Agent capability"], "page_title": "Agent News"},
            translations={"New Agent capability": "新的 Agent 能力", "Agent News": "Agent 新闻"},
        )
        self.assertEqual(records[0]["original"], "New Agent capability")
        self.assertEqual(records[0]["translation_zh"], "新的 Agent 能力")
        self.assertEqual(records[0]["translation_status"], "translated")

    def test_github_repository_count_gets_dynamic_translation(self) -> None:
        from competitive_radar import _source_evidence_records

        records = _source_evidence_records(
            {"source_id": "github-agent", "headlines": ["Here are 19,041 public repositories matching this topic..."]},
            translations={},
        )
        self.assertEqual(records[0]["translation_zh"], "有 19,041 个公开仓库匹配该主题……")
        self.assertEqual(records[0]["translation_status"], "translated")

    def test_reviewer_blocks_missing_translation(self) -> None:
        from competitive_radar import _review

        result = _review(
            "# draft",
            [{"retrieval_status": "ok", "evidence_id": "e1", "source_snapshot_uri": "/tmp/e1", "content_hash": "sha256:e1", "locator": {"kind": "body_excerpt"}, "original_evidence": [{"original": "English title", "translation_status": "missing"}]}],
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertTrue(result["p0_p1"])

    def test_snapshot_persists_original_evidence(self) -> None:
        from competitive_radar import _source_record

        source = {"source_id": "fixture", "url": "https://example.com/news", "vendor": "Vendor", "product": "Agent", "route": "A", "trust": "official"}
        fetched = {"retrieval_status": "ok", "content_hash": "sha256:fixture", "captured_at": "2026-09-03T00:00:00Z", "body_excerpt": "New Agent capability", "headlines": ["New Agent capability"]}
        with tempfile.TemporaryDirectory() as temp, patch("competitive_radar._fetch_source", return_value=fetched):
            record = _source_record(source, run_id="snapshot-test", state_root=Path(temp), translations={"New Agent capability": "新的 Agent 能力"})
            snapshot = json.loads(Path(record["source_snapshot_uri"]).read_text(encoding="utf-8"))
        self.assertEqual(snapshot["schema_version"], "competitive-radar.evidence-snapshot.v2")
        self.assertEqual(snapshot["original_evidence"][0]["translation_zh"], "新的 Agent 能力")

    def test_report_uses_scr_order_and_contains_translation(self) -> None:
        from competitive_radar import _report_html

        rendered = _report_html(
            "# draft",
            run_id="scr-test",
            captured_at="2026-09-03T00:00:00Z",
            signals=[{"source_id": "fixture", "source": "Fixture", "vendor": "Vendor", "product": "Agent", "route": "A", "retrieval_status": "ok", "evidence_id": "e1", "source_snapshot_uri": "/tmp/e1", "content_hash": "sha256:e1", "locator": {"kind": "body_excerpt"}, "original_evidence": [{"original": "New Agent capability", "translation_zh": "新的 Agent 能力", "translation_status": "translated", "kind": "headline"}]}],
            valid=[{"source_id": "fixture", "source": "Fixture", "vendor": "Vendor", "product": "Agent", "route": "A", "retrieval_status": "ok", "evidence_id": "e1", "source_snapshot_uri": "/tmp/e1", "content_hash": "sha256:e1", "locator": {"kind": "body_excerpt"}, "original_evidence": [{"original": "New Agent capability", "translation_zh": "新的 Agent 能力", "translation_status": "translated", "kind": "headline"}]}],
            report_status="reviewed",
            gate_status="PASS",
            review={"status": "PASS"},
        )
        self.assertLess(rendered.find("结论先行"), rendered.find("Situation：本期事实"))
        self.assertLess(rendered.find("Situation：本期事实"), rendered.find("Complication：本期变化"))
        self.assertLess(rendered.find("Complication：本期变化"), rendered.find("Resolution：对胜算的动作"))
        self.assertIn("New Agent capability", rendered)
        self.assertIn("新的 Agent 能力", rendered)

    def test_metadata_is_not_a_valid_report_signal(self) -> None:
        from competitive_radar import _has_content_evidence

        metadata = {
            "source_id": "fixture",
            "retrieval_status": "ok",
            "content_depth": "metadata",
            "original_evidence": [{"kind": "page_title", "original": "Agent news", "translation_zh": "Agent 新闻", "translation_status": "translated"}],
        }
        summary = {
            "source_id": "fixture",
            "retrieval_status": "ok",
            "content_depth": "summary",
            "original_evidence": [{"kind": "summary", "original": "This release adds a replayable agent runtime with execution logs, recovery controls, and scoped tool permissions.", "translation_zh": "此次发布新增可回放 Agent Runtime，包含执行日志、恢复控制和范围受限的工具权限。", "translation_status": "translated"}],
        }
        self.assertFalse(_has_content_evidence(metadata))
        self.assertTrue(_has_content_evidence(summary))

    def test_hacker_news_evidence_includes_attention_context(self) -> None:
        from competitive_radar import _source_evidence_records

        records = _source_evidence_records(
            {"source_id": "hackernews-ai", "body_excerpt": "1. Agent release ( example.com ) 321 points by user 2 hours ago | hide | 45 comments"},
            translations={"Agent release": "Agent 发布"},
        )
        self.assertEqual(records[0]["original"], "Agent release (Source: example.com; 321 points; 45 comments)")
        self.assertEqual(records[0]["translation_zh"], "Agent release（来源：example.com；321 分；45 条评论）")

    def test_browser_script_uses_one_navigation_wait_and_one_detail_limit(self) -> None:
        from competitive_radar import _browser_script

        script = _browser_script("fixture", "https://openai.com/news/", 11, 8, 1)
        self.assertIn("openOrReuseTab(\"https://openai.com/news/\", { wait: false })", script)
        self.assertIn("gotoAndWait(\"https://openai.com/news/\", { timeout: 11", script)
        self.assertIn(".slice(0, 1)", script)
        self.assertNotIn("wait: true", script)

    def test_blocked_draft_does_not_replace_current_weekly_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            radar = project / "docs" / "产品情报监控" / "竞品雷达"
            radar.mkdir(parents=True)
            (radar / "source-registry.json").write_text(json.dumps({"sources": []}), encoding="utf-8")
            week = datetime.now(timezone.utc).strftime("%G-W%V")
            weekly = radar / "周报" / f"{week}-Agent竞品雷达.md"
            weekly.parent.mkdir(parents=True)
            weekly.write_text("stable published report", encoding="utf-8")
            state = root / "state"
            state.mkdir()
            (state / "latest.json").write_text(json.dumps({"run_id": "stable"}), encoding="utf-8")
            (state / "latest-ingest.json").write_text(
                json.dumps(
                    {
                        "signals": [
                            {
                                "source_id": "draft",
                                "source": "Draft",
                                "source_url": "https://example.com/draft",
                                "vendor": "Vendor",
                                "product": "Agent",
                                "route": "A",
                                "retrieval_status": "ok",
                                "content_depth": "detail",
                                "evidence_id": "e1",
                                "source_snapshot_uri": "/tmp/e1",
                                "content_hash": "sha256:e1",
                                "locator": {"kind": "body_excerpt"},
                                "original_evidence": [{"kind": "detail", "original": "A detailed English release note about a new agent runtime with replayable execution controls and audit logs.", "translation_status": "missing"}],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            report = brief(state_root=state, project_root=project, run_id="blocked-draft")
            self.assertEqual(report["status"], "degraded")
            self.assertIn("drafts", report["markdown"])
            self.assertEqual(weekly.read_text(encoding="utf-8"), "stable published report")
            self.assertEqual(json.loads((state / "latest.json").read_text(encoding="utf-8"))["run_id"], "stable")

    def test_reannotate_refreshes_existing_snapshot_without_refetching(self) -> None:
        from competitive_radar import reannotate

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            radar = project / "docs" / "产品情报监控" / "竞品雷达"
            radar.mkdir(parents=True)
            source = {"source_id": "fixture", "url": "https://example.com/news", "vendor": "Vendor", "product": "Agent", "route": "A", "trust": "official"}
            (radar / "source-registry.json").write_text(json.dumps({"sources": [source]}), encoding="utf-8")
            (radar / "translation-overrides.json").write_text(json.dumps({"translations": {"Agent release adds replayable execution logs for governed work.": "Agent 发布为受治理工作新增可回放执行日志。"}}, ensure_ascii=False), encoding="utf-8")
            state = root / "state"
            snapshot = state / "raw" / "fixture" / "fixture-run.json"
            snapshot.parent.mkdir(parents=True)
            signal = {**source, "retrieval_status": "ok", "content_hash": "sha256:fixture", "source_snapshot_uri": str(snapshot), "body_excerpt": "Agent release adds replayable execution logs for governed work.", "original_evidence": [], "translation_status": "missing"}
            snapshot.write_text(json.dumps(signal), encoding="utf-8")
            ingest_path = state / "ingest" / "fixture-run.json"
            ingest_path.parent.mkdir(parents=True)
            ingest_path.write_text(json.dumps({"run_id": "fixture-run", "signals": [signal]}), encoding="utf-8")
            result = reannotate(state_root=state, project_root=project, run_id="fixture-run")
            refreshed = json.loads((state / "latest-ingest.json").read_text(encoding="utf-8"))["signals"][0]
            refreshed_snapshot = json.loads(snapshot.read_text(encoding="utf-8"))
            self.assertEqual(result["updated"], 1)
            self.assertEqual(refreshed["translation_status"], "translated")
            self.assertEqual(refreshed_snapshot["original_evidence"][0]["translation_zh"], "Agent 发布为受治理工作新增可回放执行日志。")
