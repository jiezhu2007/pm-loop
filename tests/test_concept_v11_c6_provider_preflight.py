import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.concept_v11_c6_provider_preflight import evaluate_policy, preflight


class ConceptV11C6ProviderPreflightTest(unittest.TestCase):
    def test_oneapi_auto_empty_allowlist_is_valid(self) -> None:
        result = evaluate_policy(
            {
                "policy_version": "auto-v1",
                "provider": "oneapi",
                "requested_model": "auto",
                "allowed_models_json": "[]",
            }
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["allowlist_mode"], "oneapi_auto")
        self.assertFalse(result["allowlist_required"])
        self.assertEqual(result["auto_provider_resolution"], "delegated_to_oneapi")
        self.assertEqual(result["model_resolution_gate"], "provider_configuration_trusted")
        self.assertFalse(result["model_resolution_required"])
        self.assertEqual(result["errors"], [])

    def test_fixed_model_empty_allowlist_is_still_invalid(self) -> None:
        result = evaluate_policy(
            {
                "policy_version": "fixed-v1",
                "provider": "oneapi",
                "requested_model": "gpt-fixed",
                "allowed_models_json": "[]",
            }
        )
        self.assertEqual(result["status"], "HOLD")
        self.assertIn("allowlist_empty:fixed-v1", result["errors"])

    def test_preflight_separates_auto_policy_from_missing_probe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "pm-system.db"
            connection = sqlite3.connect(db_path)
            connection.executescript(
                """
                CREATE TABLE concept_model_policies (
                    policy_version TEXT PRIMARY KEY,
                    provider TEXT,
                    requested_model TEXT,
                    allowed_models_json TEXT,
                    capability_class TEXT,
                    privacy_scope TEXT,
                    cost_limit REAL,
                    latency_limit_seconds REAL,
                    policy_hash TEXT,
                    status TEXT,
                    created_at TEXT
                );
                CREATE TABLE concept_capability_probes (observed_at TEXT);
                CREATE TABLE concept_admissions (namespace_epoch TEXT, admission_state TEXT, version INTEGER);
                CREATE TABLE concept_profile_admissions (
                    namespace_epoch TEXT,
                    profile TEXT,
                    pending_count INTEGER,
                    pending_soft_limit INTEGER,
                    outbox_hard_cap INTEGER,
                    pause_fence TEXT,
                    policy_hash TEXT
                );
                """
            )
            connection.execute(
                "INSERT INTO concept_model_policies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("auto-v1", "oneapi", "auto", "[]", "concept", "local-private", None, 900, "sha256:x", "active", "2026-08-31"),
            )
            connection.execute("INSERT INTO concept_admissions VALUES ('epoch-1', 'disabled', 1)")
            connection.execute("INSERT INTO concept_profile_admissions VALUES ('epoch-1', 'pm-semantic', 0, 2, 8, 'open', NULL)")
            connection.commit()
            connection.close()

            result = preflight(db_path)
            self.assertEqual(result["status"], "HOLD")
            self.assertEqual(result["policy_status"], "PASS")
            self.assertNotIn("allowlist_empty:auto-v1", result["errors"])
            self.assertIn("capability_probe_missing", result["errors"])
            self.assertEqual(result["external_provider_calls"], 0)


if __name__ == "__main__":
    unittest.main()
