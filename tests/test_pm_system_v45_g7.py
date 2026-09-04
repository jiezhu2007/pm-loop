from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_v45_g7 import BASELINE_AUTHORIZATION_ID, MIN_DURATION_SECONDS, MIN_RESOURCE_BYTES, MIN_RESOURCES, MIN_TASKS, MINIMUM_POLICY, _hash, run_g7  # noqa: E402


class V45G7Tests(unittest.TestCase):
    def _store(self, path: Path) -> None:
        store = PMSystemStore(path)
        store.set_migration_freeze(migration_id="v45-r2-20260830", migration_epoch="v45-r2-20260830", stage_id="G7", owner="test", deadline_at="2099-01-01T00:00:00Z")

    def _profile(self) -> dict:
        return {
            "sample_count": MIN_TASKS,
            "duration_seconds": MIN_DURATION_SECONDS,
            "resource_count": MIN_RESOURCES,
            "min_resource_bytes": MIN_RESOURCE_BYTES,
            "processing_mode": "vectors_only",
            "provider_calls_verified": True,
            "queue_wait_missing_count": 0,
            "metric_sources": {
                "accepted": "client_monotonic_submission_latency",
                "queue_wait": "shadow_worker.dequeue_event",
                "content_verified": "task_state_poll_completion",
                "semantic_model_latency": "provider_call_ledger",
                "lock_wait": "host_openviking_lock_telemetry",
                "rss": "host_process_rss_sample",
                "wal": "host_queue_db_wal_sample",
            },
            "metrics": {"accepted_p95_ms": 1, "accepted_p99_ms": 2, "queue_wait_p95_s": 0, "queue_wait_p99_s": 0, "queue_wait_max_s": 0, "content_verified_p95_s": 1, "content_verified_p99_s": 2, "semantic_model_latency_p95_s": 1, "semantic_model_latency_p99_s": 2, "semantic_model_latency_max_s": 3, "memory_link_lag_p95_s": 1, "memory_link_lag_p99_s": 2, "lock_wait_p95_ms": 1, "lock_wait_max_ms": 2, "rss_growth_ratio": 0, "wal_peak_ratio": 1, "retry_amplification": 0},
        }

    def _model_shadow(self, *, unknown: int = 0, attempt_two: int | None = None) -> dict:
        ledger = []
        for index in range(MIN_TASKS):
            run_id = f"run-{index}"
            ledger.append(
                {
                    "run_id": run_id,
                    "stage": "analysis",
                    "attempt": 1,
                    "status": "result_unknown" if index < unknown else "completed",
                    "model_input_hash": f"sha256:input-{index}",
                    "prompt_version": "fixture-v1",
                    "provider": "oneapi",
                }
            )
            if index < unknown:
                ledger.append(
                    {
                        "run_id": run_id,
                        "stage": "analysis",
                        "attempt": 2,
                        "status": "completed",
                        "model_input_hash": f"sha256:input-{index}",
                        "prompt_version": "fixture-v1",
                        "provider": "oneapi",
                    }
                )
        return {
            "sample_count": MIN_TASKS,
            "run_count": MIN_TASKS,
            "model_call_count": MIN_TASKS + unknown,
            "duration_seconds": MIN_DURATION_SECONDS,
            "provider_calls_verified": True,
            "external_provider_calls": 0,
            "production_state_touched": False,
            "metric_sources": {"model_calls": "temporary_sqlite.model_calls+deterministic_oneapi_fixture"},
            "metrics": {"retry_amplification": 0},
            "response_unknown_count": unknown,
            "attempt_two_count": unknown if attempt_two is None else attempt_two,
            "active_slots_after_release": 0,
            "active_provider_tokens_after_release": 0,
            "evidence_role": "isolated_model_contract_fixture",
            "transport": "deterministic_oneapi_fixture",
            "model_calls_ledger": ledger,
            "model_calls_ledger_sha256": _hash(ledger),
        }

    def test_missing_real_shadow_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            result = run_g7(db_path=db, shadow_manifest=root / "missing.json", manifest_path=root / "manifest.json", execute=False)
            self.assertEqual(result["decision"], "HOLD")
            self.assertTrue(any(item["name"] == "shadow profile fast-vector" and item["status"] == "HOLD" for item in result["checks"]))
            self.assertTrue(any(item["name"] == "shadow profile memory-link" and item["status"] == "SKIPPED/HOLD" for item in result["checks"]))

    def test_complete_shadow_can_pass_only_with_all_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: self._profile() for name in ("fast-vector", "pm-semantic", "memory-link", "codex-model")}}), encoding="utf-8")
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow()), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "PASS")
            self.assertEqual(result["minimums"]["policy"], MINIMUM_POLICY)

    def test_sample_count_without_sustained_duration_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["duration_seconds"] = MIN_DURATION_SECONDS - 1
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            self.assertTrue(any(item["name"] == "shadow profile fast-vector" and item["status"] == "HOLD" for item in result["checks"]))

    def test_sustained_duration_without_enough_samples_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["sample_count"] = MIN_TASKS - 1
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            self.assertTrue(any(item["name"] == "shadow profile fast-vector" and item["status"] == "HOLD" for item in result["checks"]))

    def test_uncollected_metrics_hold_without_runner_crash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metrics"]["lock_wait_p95_ms"] = None
            profile["metrics"]["lock_wait_max_ms"] = None
            profile["metrics"]["rss_growth_ratio"] = None
            profile["metrics"]["wal_peak_ratio"] = None
            shadow = root / "shadow.json"
            shadow.write_text(
                json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}),
                encoding="utf-8",
            )
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            fast_vector = next(item for item in result["checks"] if item["name"] == "shadow profile fast-vector")
            self.assertEqual(fast_vector["status"], "HOLD")
            self.assertIn("missing:lock_wait_p95_ms", fast_vector["detail"])
            self.assertIn("missing:rss_growth_ratio", fast_vector["detail"])

    def test_memory_link_lag_is_not_required_for_non_memory_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metrics"].pop("memory_link_lag_p95_s")
            profile["metrics"].pop("memory_link_lag_p99_s")
            shadow = root / "shadow.json"
            shadow.write_text(
                json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}),
                encoding="utf-8",
            )
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow()), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "PASS")
            for profile_name in ("fast-vector", "pm-semantic", "codex-model"):
                check = next(item for item in result["checks"] if item["name"] == f"shadow profile {profile_name}")
                self.assertEqual(check["status"], "PASS")
                self.assertIn("memory_link_lag_p95_s", check["detail"])

    def test_queue_wait_poll_source_holds_even_with_numeric_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metric_sources"]["queue_wait"] = "task_state_poll_resolution"
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            fast_vector = next(item for item in result["checks"] if item["name"] == "shadow profile fast-vector")
            self.assertIn("invalid:queue_wait_source", fast_vector["detail"])

    def test_queue_db_source_is_accepted_when_samples_are_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            shadow = root / "shadow.json"
            profile = self._profile()
            profile["metric_sources"]["queue_wait"] = "queue_db.processing_started_at"
            shadow.write_text(
                json.dumps({
                    "minimums": {"policy": MINIMUM_POLICY},
                    "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")},
                }),
                encoding="utf-8",
            )
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow()), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch(
                "pm_system_v45_g7._isolated_capacity",
                return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0},
            ):
                result = run_g7(db_path=db, shadow_manifest=shadow, model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "PASS")

    def test_missing_model_shadow_holds_without_runner_crash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            result = run_g7(db_path=db, shadow_manifest=root / "missing.json", model_shadow_manifest=root / "missing-model.json", manifest_path=root / "manifest.json", execute=False)
            self.assertEqual(result["decision"], "HOLD")
            model_check = next(item for item in result["checks"] if item["name"] == "codex-model OneAPI model-call shadow")
            self.assertEqual(model_check["status"], "HOLD")
            self.assertIn("missing:model_shadow_manifest", model_check["detail"])

    def test_valid_model_shadow_passes_independently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow(unknown=3)), encoding="utf-8")
            from pm_system_v45_g7 import _model_shadow_check

            result = _model_shadow_check(model_shadow)
            self.assertEqual(result["status"], "PASS")

    def test_model_shadow_attempt_mismatch_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow(unknown=3, attempt_two=2)), encoding="utf-8")
            from pm_system_v45_g7 import _model_shadow_check

            result = _model_shadow_check(model_shadow)
            self.assertEqual(result["status"], "HOLD")
            self.assertIn("attempt_two_count=2!=3", result["detail"])

    def test_model_shadow_ledger_tamper_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            payload = self._model_shadow(unknown=3)
            payload["model_calls_ledger"][0]["model_input_hash"] = "sha256:tampered"
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(payload), encoding="utf-8")
            from pm_system_v45_g7 import _model_shadow_check

            result = _model_shadow_check(model_shadow)
            self.assertEqual(result["status"], "HOLD")
            self.assertIn("invalid:model_shadow.model_calls_ledger_sha256", result["detail"])

    def test_invalid_model_shadow_holds_without_runner_crash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text("{invalid", encoding="utf-8")
            result = run_g7(db_path=db, shadow_manifest=root / "missing.json", model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=False)
            self.assertEqual(result["decision"], "HOLD")
            self.assertTrue((root / "manifest.json").is_file())

    def test_missing_metric_sources_hold_without_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile.pop("metric_sources")
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": {name: profile for name in ("fast-vector", "pm-semantic", "codex-model")}}), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            fast_vector = next(item for item in result["checks"] if item["name"] == "shadow profile fast-vector")
            self.assertIn("missing:queue_wait_source", fast_vector["detail"])
            self.assertIn("missing:rss_source", fast_vector["detail"])

    def test_semantic_model_latency_has_a_profile_specific_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metrics"]["semantic_model_latency_p95_s"] = 121
            shadow = root / "shadow.json"
            profiles = {name: self._profile() for name in ("fast-vector", "pm-semantic", "codex-model")}
            profiles["pm-semantic"] = profile
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": profiles}), encoding="utf-8")
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow()), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["decision"], "HOLD")
            semantic = next(item for item in result["checks"] if item["name"] == "shadow profile pm-semantic")
            self.assertIn("semantic_model_latency_p95_s=121.0>120.0", semantic["detail"])

    def test_semantic_model_latency_is_required_for_pm_semantic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metrics"].pop("semantic_model_latency_p95_s")
            profile["metrics"].pop("semantic_model_latency_p99_s")
            profile["metrics"].pop("semantic_model_latency_max_s")
            profile["metric_sources"].pop("semantic_model_latency")
            shadow = root / "shadow.json"
            profiles = {name: self._profile() for name in ("fast-vector", "pm-semantic", "codex-model")}
            profiles["pm-semantic"] = profile
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": profiles}), encoding="utf-8")
            model_shadow = root / "model-shadow.json"
            model_shadow.write_text(json.dumps(self._model_shadow()), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, model_shadow_manifest=model_shadow, manifest_path=root / "manifest.json", execute=True)
            semantic = next(item for item in result["checks"] if item["name"] == "shadow profile pm-semantic")
            self.assertEqual(semantic["status"], "HOLD")
            self.assertIn("missing:semantic_model_latency_p95_s", semantic["detail"])
            self.assertIn("missing:semantic_model_latency_source", semantic["detail"])

    def test_explicit_baseline_allows_next_stage_without_calling_it_optimized(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profile = self._profile()
            profile["metrics"]["accepted_p95_ms"] = 101
            profiles = {name: self._profile() for name in ("fast-vector", "pm-semantic", "codex-model")}
            profiles["fast-vector"] = profile
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": profiles}), encoding="utf-8")
            baseline = root / "baseline.json"
            baseline.write_text(json.dumps({
                "schema_version": "pm-system.v45-r4-g7-baseline-acceptance.v1",
                "authorization_id": BASELINE_AUTHORIZATION_ID,
                "decision": "ACCEPTED_BASELINE",
                "baseline_complete": True,
                "performance_issues_recorded": True,
                "optimization_deferred": True,
                "allow_next_stage": True,
                "production_state_touched": False,
                "external_provider_calls": 0,
                "does_not_authorize_concurrency_increase": True,
                "source_shadow_sha256": __import__("hashlib").sha256(shadow.read_bytes()).hexdigest(),
                "accepted_check_names": ["shadow profile fast-vector", "codex-model OneAPI model-call shadow"],
                "accepted_deviations": [{"issue": "accepted p95 baseline", "follow_up": "optimize later"}],
            }), encoding="utf-8")
            with patch("pm_system_v45_g7._health", return_value={"healthy": True, "status": "healthy"}), patch("pm_system_v45_g7._isolated_capacity", return_value={"status": "pass", "production_state_touched": False, "external_provider_calls": 0}):
                result = run_g7(db_path=db, shadow_manifest=shadow, baseline_manifest=baseline, manifest_path=root / "manifest.json", execute=True)
            self.assertEqual(result["strict_decision"], "HOLD")
            self.assertEqual(result["baseline_acceptance"]["status"], "PASS")
            self.assertEqual(result["decision"], "PASS_WITH_BASELINE")

    def test_baseline_shadow_hash_mismatch_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "pm.db"
            self._store(db)
            profiles = {name: self._profile() for name in ("fast-vector", "pm-semantic", "codex-model")}
            shadow = root / "shadow.json"
            shadow.write_text(json.dumps({"minimums": {"policy": MINIMUM_POLICY}, "profiles": profiles}), encoding="utf-8")
            baseline = root / "baseline.json"
            baseline.write_text(json.dumps({
                "schema_version": "pm-system.v45-r4-g7-baseline-acceptance.v1",
                "authorization_id": BASELINE_AUTHORIZATION_ID,
                "decision": "ACCEPTED_BASELINE",
                "baseline_complete": True,
                "performance_issues_recorded": True,
                "optimization_deferred": True,
                "allow_next_stage": True,
                "production_state_touched": False,
                "external_provider_calls": 0,
                "does_not_authorize_concurrency_increase": True,
                "source_shadow_sha256": "wrong",
                "accepted_check_names": [],
                "accepted_deviations": [{"issue": "none", "follow_up": "none"}],
            }), encoding="utf-8")
            result = run_g7(db_path=db, shadow_manifest=shadow, baseline_manifest=baseline, manifest_path=root / "manifest.json", execute=False)
            self.assertEqual(result["decision"], "HOLD")
            self.assertIn("invalid:baseline.source_shadow_sha256", result["baseline_acceptance"]["violations"])


if __name__ == "__main__":
    unittest.main()
