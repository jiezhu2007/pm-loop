from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from concept_v11_baseline_seed import _build_plan, _concept_id, _apply  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


def _sha(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _coverage(concepts: list[str]) -> dict:
    rows = []
    for name in concepts:
        rows.append({
            "concept": name,
            "concept_id": _concept_id(name),
            "coverage_status": "refreshable",
            "reference_count": 1,
            "references": [{
                "map_id": f"map-{name}",
                "source_uri": f"viking://source/{name}",
                "source_map_status": "mapped",
                "disposition": "mapped",
                "evidence_set_hash": _sha(name),
            }],
        })
    report = {
        "schema": "concept-v11.source-coverage-report.v1",
        "status": "PASS",
        "gate": {"p3_closed": True},
        "report_hash": "sha256:coverage-report",
        "source_manifest_hash": "sha256:source-manifest",
        "expected_concept_count": len(concepts),
        "concept_count": len(concepts),
        "concepts": rows,
    }
    return report


class BaselineSeedTests(unittest.TestCase):
    def _fixture(self, root: Path, concepts: list[str]) -> tuple[Path, Path, Path]:
        concept_root = root / "concepts"
        (concept_root / "state" / "pages").mkdir(parents=True)
        (concept_root / "state" / "concepts-ledger.json").write_text(
            json.dumps({name: {"status": "active"} for name in concepts}), encoding="utf-8"
        )
        for name in concepts:
            (concept_root / "state" / "pages" / f"{name}.md").write_text(f"# {name}\n", encoding="utf-8")
        coverage_path = root / "coverage.json"
        coverage_path.write_text(json.dumps(_coverage(concepts)), encoding="utf-8")
        db_path = root / "pm-system.db"
        with sqlite3.connect(db_path) as connection:
            connection.executescript("""
                CREATE TABLE concept_admissions (
                    namespace_epoch TEXT PRIMARY KEY, admission_state TEXT, version INTEGER
                );
                CREATE TABLE concept_versions (
                    version_id TEXT PRIMARY KEY, concept_id TEXT, namespace_epoch TEXT, version TEXT,
                    generation_id TEXT, content TEXT, content_hash TEXT, source_snapshot_hash TEXT,
                    evidence_set_hash TEXT, compiler_version TEXT, policy_version TEXT, status TEXT,
                    created_at TEXT, provenance TEXT
                );
                CREATE TABLE concept_hot_projection (
                    concept_id TEXT, namespace_epoch TEXT, generation_id TEXT, projection_state TEXT,
                    outbox_item_id TEXT, observed_content_hash TEXT, observed_at TEXT, updated_at TEXT,
                    provenance TEXT, UNIQUE(concept_id,namespace_epoch)
                );
                CREATE TABLE concept_publish_ledger (
                    publish_id TEXT PRIMARY KEY, concept_id TEXT, namespace_epoch TEXT, version_id TEXT,
                    previous_generation TEXT, current_generation TEXT, current_hot_generation TEXT,
                    desired_hot_generation TEXT, projection_state TEXT, projection_outbox_id TEXT,
                    operator TEXT, evidence_hash TEXT, created_at TEXT, updated_at TEXT, provenance TEXT
                );
                CREATE TABLE generations (
                    generation_id TEXT PRIMARY KEY, domain TEXT, generation_hash TEXT, status TEXT,
                    source_watermark TEXT, knowledge_watermark TEXT, created_at TEXT, active_at TEXT,
                    UNIQUE(domain,generation_hash)
                );
                CREATE TABLE migration_leases (
                    migration_id TEXT, stage_id TEXT, migration_epoch TEXT, owner TEXT,
                    lease_id TEXT PRIMARY KEY, acquired_at TEXT, lease_expires_at TEXT, state TEXT
                );
                CREATE TABLE watermarks (
                    source_domain TEXT, watermark_name TEXT, captured_at INTEGER, sequence INTEGER,
                    value_hash TEXT, value TEXT, producer TEXT, state TEXT,
                    PRIMARY KEY(source_domain,watermark_name)
                );
                CREATE TABLE watermark_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT, source_domain TEXT, watermark_name TEXT,
                    captured_at INTEGER, sequence INTEGER, value_hash TEXT, state TEXT,
                    observed_at TEXT, details_json TEXT
                );
                INSERT INTO concept_admissions VALUES ('epoch-1','disabled',7);
            """)
            for index, name in enumerate(concepts):
                concept_id = _concept_id(name)
                content = f"# {name}\n"
                content_hash = _sha(content)
                version_id = f"version-{index}"
                connection.execute(
                    "INSERT INTO concept_versions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (version_id, concept_id, "epoch-1", "v1", "legacy-import-epoch-1", content, content_hash, None, _sha(name), "legacy-import", None, "active", "2026-01-01T00:00:00Z", "legacy_import"),
                )
                connection.execute(
                    "INSERT INTO concept_hot_projection VALUES (?,?,?,?,?,?,?,?,?)",
                    (concept_id, "epoch-1", "legacy-import-epoch-1", "legacy_imported", None, content_hash, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "legacy_import"),
                )
                connection.execute(
                    "INSERT INTO concept_publish_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"publish-{index}", concept_id, "epoch-1", version_id, None, "legacy-import-epoch-1", "legacy-import-epoch-1", "legacy-import-epoch-1", "legacy_imported", None, "legacy", _sha(name), "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "legacy_import"),
                )
            connection.commit()
        return db_path, concept_root, coverage_path

    def test_partial_coverage_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A", "B"])
            report = json.loads(coverage.read_text())
            report["status"] = "HOLD"
            report["gate"]["p3_closed"] = False
            coverage.write_text(json.dumps(report))
            plan = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(plan["status"], "HOLD")
            self.assertIn("coverage_not_pass:HOLD", plan["errors"])
            with sqlite3.connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM generations").fetchone()[0], 0)

    def test_explicit_roll_replaces_one_active_generation_after_legacy_page_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A", "B"])
            initial = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            store = PMSystemStore(db, auto_migrate=False)
            lease = store.acquire_migration_lease(
                migration_id="initial-seed", stage_id="P5.5-BASELINE-SEED", migration_epoch="epoch-1", owner="tester"
            )
            _apply(
                initial,
                db_path=db,
                backup_root=root / "backups",
                owner="tester",
                migration_id="initial-seed",
                lease_id=lease["lease_id"],
            )
            store.release_migration_lease(lease_id=lease["lease_id"])

            (concept_root / "state" / "pages" / "B.md").write_text("# B rebuilt\n", encoding="utf-8")
            blocked = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(blocked["status"], "HOLD")
            self.assertIn("active_generation_already_exists", blocked["errors"])

            roll = _build_plan(
                db,
                concept_root,
                coverage,
                "epoch-1",
                2,
                allow_active_generation_replacement=True,
            )
            self.assertEqual(roll["status"], "PASS")
            self.assertTrue(roll["allow_active_generation_replacement"])
            self.assertEqual(
                next(item for item in roll["members"] if item["concept"] == "B")["version_action"],
                "append_current",
            )
            lease = store.acquire_migration_lease(
                migration_id="baseline-roll", stage_id="P5.5-BASELINE-SEED", migration_epoch="epoch-1", owner="tester"
            )
            applied = _apply(
                roll,
                db_path=db,
                backup_root=root / "backups",
                owner="tester",
                migration_id="baseline-roll",
                lease_id=lease["lease_id"],
                allow_active_generation_replacement=True,
            )
            store.release_migration_lease(lease_id=lease["lease_id"])
            self.assertEqual(applied["status"], "APPLIED")
            self.assertEqual(applied["rollback"]["previous_generation"], [initial["generation_id"]])
            with sqlite3.connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM generations WHERE domain='concepts' AND status='active'").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT status FROM generations WHERE generation_id=?", (initial["generation_id"],)).fetchone()[0], "superseded")
                self.assertEqual(connection.execute("SELECT content FROM concept_versions WHERE concept_id=? AND status='active'", (_concept_id("B"),)).fetchone()[0], "# B rebuilt\n")
                value = json.loads(connection.execute("SELECT value FROM watermarks WHERE source_domain='pm-runtime' AND watermark_name='active_generation'").fetchone()[0])
                self.assertEqual(value["generation_id"], roll["generation_id"])
                self.assertEqual(value["generation_hash"], roll["generation_hash"])

    def test_historical_exclusions_require_one_current_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A"])
            report = json.loads(coverage.read_text())
            refs = report["concepts"][0]["references"]
            refs.append({
                "map_id": "map-A-history",
                "source_uri": "viking://source/A-history",
                "source_map_status": "quarantined",
                "disposition": "historical_exclusion",
                "evidence_set_hash": _sha("A-history"),
            })
            coverage.write_text(json.dumps(report), encoding="utf-8")

            self.assertEqual(_build_plan(db, concept_root, coverage, "epoch-1", 1)["status"], "PASS")

            refs[0]["disposition"] = "historical_exclusion"
            coverage.write_text(json.dumps(report), encoding="utf-8")
            plan = _build_plan(db, concept_root, coverage, "epoch-1", 1)
            self.assertEqual(plan["status"], "HOLD")
            self.assertIn("coverage_current_source_missing:A", plan["errors"])

    def test_retired_concept_allows_historical_exclusions_only_with_tombstone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A"])
            report = json.loads(coverage.read_text())
            concept = report["concepts"][0]
            concept["coverage_status"] = "retired_with_evidence"
            concept["references"][0]["disposition"] = "historical_exclusion"
            concept["retirement"] = {
                "decision": "retired_with_evidence",
                "retirement_content_sha256": _sha("A retirement"),
            }
            coverage.write_text(json.dumps(report), encoding="utf-8")

            self.assertEqual(_build_plan(db, concept_root, coverage, "epoch-1", 1)["status"], "PASS")

            del concept["retirement"]
            coverage.write_text(json.dumps(report), encoding="utf-8")
            plan = _build_plan(db, concept_root, coverage, "epoch-1", 1)
            self.assertEqual(plan["status"], "HOLD")
            self.assertIn("coverage_retirement_evidence_missing:A", plan["errors"])

    def test_apply_switches_all_pointers_and_is_idempotent_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A", "B"])
            plan = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(plan["status"], "PASS")
            store = PMSystemStore(db, auto_migrate=False)
            lease = store.acquire_migration_lease(
                migration_id="seed-test", stage_id="P5.5-BASELINE-SEED", migration_epoch="epoch-1", owner="tester"
            )
            applied = _apply(plan, db_path=db, backup_root=root / "backups", owner="tester", migration_id="seed-test", lease_id=lease["lease_id"])
            store.release_migration_lease(lease_id=lease["lease_id"])
            self.assertEqual(applied["status"], "APPLIED")
            self.assertTrue(applied["rollback"]["backup"]["verified"])
            with sqlite3.connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM generations WHERE domain='concepts' AND status='active'").fetchone()[0], 1)

    def test_legacy_page_drift_appends_current_version_without_rewriting_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db, concept_root, coverage = self._fixture(root, ["A", "B"])
            (concept_root / "state" / "pages" / "A.md").write_text("# A updated\n", encoding="utf-8")

            plan = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(plan["status"], "PASS")
            actions = {item["concept"]: item["version_action"] for item in plan["members"]}
            self.assertEqual(actions, {"A": "append_current", "B": "reuse_current"})

            store = PMSystemStore(db, auto_migrate=False)
            lease = store.acquire_migration_lease(
                migration_id="seed-drift-test", stage_id="P5.5-BASELINE-SEED", migration_epoch="epoch-1", owner="tester"
            )
            applied = _apply(
                plan,
                db_path=db,
                backup_root=root / "backups",
                owner="tester",
                migration_id="seed-drift-test",
                lease_id=lease["lease_id"],
            )
            store.release_migration_lease(lease_id=lease["lease_id"])
            self.assertEqual(applied["status"], "APPLIED")

            with sqlite3.connect(db) as connection:
                versions = connection.execute(
                    "SELECT version_id,version,status,content,content_hash,provenance FROM concept_versions "
                    "WHERE concept_id=? ORDER BY rowid",
                    (_concept_id("A"),),
                ).fetchall()
                self.assertEqual(len(versions), 2)
                self.assertEqual(versions[0][2], "superseded")
                self.assertEqual(versions[0][5], "legacy_import")
                self.assertEqual(versions[1][2], "active")
                self.assertEqual(versions[1][3], "# A updated\n")
                self.assertEqual(versions[1][5], "baseline_seed")
                current_version_id = versions[1][0]
                self.assertEqual(
                    connection.execute(
                        "SELECT version_id FROM concept_publish_ledger WHERE concept_id=? AND namespace_epoch=? "
                        "ORDER BY updated_at DESC,created_at DESC,rowid DESC LIMIT 1",
                        (_concept_id("A"), "epoch-1"),
                    ).fetchone()[0],
                    current_version_id,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT generation_id,projection_state,observed_content_hash,provenance "
                        "FROM concept_hot_projection WHERE concept_id=? AND namespace_epoch=?",
                        (_concept_id("A"), "epoch-1"),
                    ).fetchone(),
                    (plan["generation_id"], "active", versions[1][4], "baseline_seed"),
                )

            rerun = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(rerun["status"], "PASS")
            self.assertEqual({item["concept"]: item["version_action"] for item in rerun["members"]}, {"A": "reuse_current", "B": "reuse_current"})
            applied_again = _apply(rerun, db_path=db, backup_root=root / "backups", owner="tester")
            self.assertTrue(applied_again["idempotent"])
            with sqlite3.connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_versions").fetchone()[0], 3)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_publish_ledger").fetchone()[0], 3)
                generation_id = connection.execute("SELECT generation_id FROM generations WHERE status='active'").fetchone()[0]
                self.assertEqual(generation_id, plan["generation_id"])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_versions WHERE generation_id=?", (generation_id,)).fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_hot_projection WHERE generation_id=? AND projection_state='active'", (generation_id,)).fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM concept_publish_ledger WHERE current_generation=? AND projection_state='active'", (generation_id,)).fetchone()[0], 2)
            rerun = _build_plan(db, concept_root, coverage, "epoch-1", 2)
            self.assertEqual(rerun["status"], "PASS")
            applied_again = _apply(rerun, db_path=db, backup_root=root / "backups", owner="tester")
            self.assertTrue(applied_again["idempotent"])
            with sqlite3.connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM generations WHERE domain='concepts' AND status='active'").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
