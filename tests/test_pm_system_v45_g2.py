from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import pm_system_v45_g2 as g2  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class V45G2Tests(unittest.TestCase):
    def test_collect_g2_uses_real_markers_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source.json"
            content = root / "content.meta.json"
            manifest = root / "source-manifest.json"
            weekly = root / "weekly.done"
            source.write_text(json.dumps({"status": "success", "run_id": "run-1", "finished_at": "2026-08-30T00:00:00Z", "exit_code": 0, "totals": {"success": 3}}), encoding="utf-8")
            content.write_text(json.dumps({"schema_version": "concept-source-manifest.v1", "generated_at": "2026-08-29T00:00:00Z", "metrics": {"document_count": 4}}), encoding="utf-8")
            manifest.write_text("{\"documents\":[]}", encoding="utf-8")
            weekly.write_text(json.dumps({"status": "ok", "concept_refresh_disabled": True, "finished_at": "2026-08-30T00:00:01Z"}), encoding="utf-8")
            knowledge = {"captured_at": 10, "sequence": 2, "value": {"status": "accepted"}, "state": "accepted", "producer": "fixture.knowledge"}
            with patch.object(g2, "produce_knowledge", return_value=knowledge):
                first = g2.collect_g2(db_path=root / "pm.db", source_marker=source, content_marker=content, content_manifest=manifest, weekly_marker=weekly, generation_dir=root / "empty")
                second = g2.collect_g2(db_path=root / "pm.db", source_marker=source, content_marker=content, content_manifest=manifest, weekly_marker=weekly, generation_dir=root / "empty")
            self.assertEqual(set(first["watermarks"]), {"source", "content", "knowledge", "active_generation"})
            self.assertEqual(first["watermarks"]["active_generation"]["state"], "missing")
            self.assertEqual(first["outcomes"]["source"]["outcome"], "accepted")
            self.assertEqual(second["outcomes"]["source"]["outcome"], "idempotent")
            store = PMSystemStore(root / "pm.db")
            rows = store.list_watermarks(source_domain="pm-runtime")
            self.assertEqual({row["watermark_name"] for row in rows}, {"source", "content", "knowledge", "active_generation"})
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(DISTINCT module) FROM module_health_snapshots").fetchone()[0], 9)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM metric_rollups WHERE metric_name LIKE 'g2.%'").fetchone()[0], 3)

    def test_missing_and_conflicting_cursors_do_not_advance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = PMSystemStore(Path(temp) / "pm.db")
            first = store.put_watermark(source_domain="pm-runtime", watermark_name="source", captured_at=100, sequence=1, value={"status": "accepted", "v": 1}, producer="test")
            replay = store.put_watermark(source_domain="pm-runtime", watermark_name="source", captured_at=99, sequence=9, value={"status": "accepted", "v": 0}, producer="test")
            conflict = store.put_watermark(source_domain="pm-runtime", watermark_name="source", captured_at=100, sequence=1, value={"status": "accepted", "v": 2}, producer="test")
            self.assertEqual(first["outcome"], "accepted")
            self.assertEqual(replay["outcome"], "replay_rejected")
            self.assertEqual(conflict["outcome"], "quarantine")
            self.assertEqual(store.list_watermarks(source_domain="pm-runtime")[0]["value"], '{"status":"accepted","v":1}')


if __name__ == "__main__":
    unittest.main()
