from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_s8_gate import REPORT_PAIRS, build_gate  # noqa: E402


class S8CloseoutGateTests(unittest.TestCase):
    def test_s8_closeout_requires_all_report_pairs_and_passed_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            for _, stem in REPORT_PAIRS:
                markdown = report_dir / f"{stem}.md"
                markdown.write_text("# Fixture\n\n当前判定：**PASS**\n", encoding="utf-8")
                markdown.with_suffix(".html").write_text("<!doctype html><title>PASS</title>\n", encoding="utf-8")
            (report_dir / "20260829-S8.4-M3-统一只读对账报告.md").write_text(
                "当前判定：**PASS**\nPM_V44_AUTOMATION_FREEZE=on\nPM_V44_ADMISSION=freeze\nWriter=0\n",
                encoding="utf-8",
            )
            (report_dir / "20260829-S8.4-M3-统一只读对账-manifest.json").write_text(
                json.dumps({"status": "PASS"}), encoding="utf-8"
            )
            (report_dir / "20260829-S8.5-只读门禁-manifest.json").write_text(
                json.dumps({
                    "gate": {
                        "status": "PASS",
                        "skill_non_orphan_consistent": True,
                        "historical_runs_consistent": True,
                        "concept_chain_read_only": True,
                        "generation_recovery_replayable": True,
                        "known_non_blocking": {"concept_active_mapping_gap": 260},
                    }
                }),
                encoding="utf-8",
            )
            (report_dir / "20260829-S8.6-容量压测-manifest.json").write_text(
                json.dumps({
                    "status": "pass",
                    "production_state_touched": False,
                    "external_provider_calls": 0,
                    "levels": [{"width": width, "status": "pass"} for width in (2, 4, 8)],
                }),
                encoding="utf-8",
            )
            gate = build_gate(report_dir)
        self.assertEqual(gate["status"], "PASS")
        self.assertTrue(gate["read_only"])
        self.assertTrue(gate["checks"]["all_s8_report_pairs_complete"])
        self.assertTrue(gate["checks"]["s8.4_m3_manifest_pass"])
        self.assertTrue(gate["checks"]["s8.5_manifest_and_guards_pass"])
        self.assertTrue(gate["checks"]["s8.6_capacity_manifest_pass"])
        self.assertTrue(gate["checks"]["s8.6_isolation"])
        self.assertTrue(gate["checks"]["freeze_evidence"])
        self.assertEqual(gate["known_follow_ups"]["unmapped_concept_active_references"], 260)
        self.assertFalse(gate["known_follow_ups"]["automatic_recovery_allowed"])


if __name__ == "__main__":
    unittest.main()
