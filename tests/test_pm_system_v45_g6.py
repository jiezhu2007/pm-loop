from __future__ import annotations

import json
import plistlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_system_store import PMSystemStore  # noqa: E402
from pm_system_v45_g6 import CANONICAL_PYTHON, apply_g6  # noqa: E402


class V45G6Tests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path, Path, Path]:
        project = root / "project"
        codex = root / "codex"
        launch = root / "launch"
        report = root / "manifest.json"
        (project / "scripts").mkdir(parents=True)
        (project / "memory/openviking").mkdir(parents=True)
        (codex / "pm-loop/runtime/scripts").mkdir(parents=True)
        launch.mkdir()
        db = codex / "pm-loop/state/pm-system.db"
        PMSystemStore(db)
        PMSystemStore(db).set_migration_freeze(
            migration_id="v45-r2-20260830",
            migration_epoch="v45-r2-20260830",
            stage_id="G6",
            owner="test",
            deadline_at="2099-01-01T00:00:00Z",
        )
        for name in ("pm_system_store.py", "pm_system_scheduler.py", "pm_system_worker.py", "pm_system_gateway.py", "pm_resource_dispatcher.py", "pm_system_cockpit.py", "pm_system_evidence.py", "pm_system_s10_observe.py", "pm_system_s10_final_gate.py", "pm_loop_control_plane_server.py", "pm_loop_control_plane.py", "ov_memory_sync.py", "pm_system_s9_writer_preflight.py", "pm_system_s9_3_3_health_restore.py", "pm_system_s9_timeline_dry_run.py"):
            (project / "scripts" / name).write_text("# canonical\n", encoding="utf-8")
        def write_plist(name: str, script: str, *, watcher: bool = False) -> None:
            env = {}
            args = [CANONICAL_PYTHON, str(codex / "pm-loop/runtime/scripts" / script)]
            if watcher:
                args += ["--mirror", str(project / "memory/openviking"), "watch", "--interval", "5", "--durable-events"]
                env = {"PM_V45_MEMORY_EVENT_MODE": "outbox", "PM_V45_NAMESPACE_EPOCH": "v45-r2-20260830"}
            value = {"Label": name, "ProgramArguments": args, "EnvironmentVariables": env}
            (project / "scripts" / f"{name}.plist").write_bytes(plistlib.dumps(value))
        write_plist("com.zhujie14.pm-loop-control-plane", "pm_loop_control_plane_server.py")
        write_plist("com.zhujie14.pm-system-worker", "pm_system_worker.py")
        write_plist("com.zhujie14.ov-memory-sync", "ov_memory_sync.py", watcher=True)
        for automation_id in ("databuilder", "automation", "v4-4-s10"):
            directory = codex / "automations" / automation_id
            directory.mkdir(parents=True)
            (directory / "automation.toml").write_text('status = "PAUSED"\n', encoding="utf-8")
        return project, codex, launch, report

    def test_runtime_sync_and_watcher_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            project, codex, launch, report = self._fixture(Path(temp))
            result = apply_g6(project_root=project, codex_root=codex, launch_root=launch, db_path=codex / "pm-loop/state/pm-system.db", backup_root=Path(temp) / "backup", manifest_path=report, execute_launchd=False)
            self.assertEqual(result["decision"], "PASS", result["issues"])
            self.assertEqual(result["external_provider_calls"], 0)
            self.assertEqual(result["process_cardinality"], {label: 0 for label in result["process_cardinality"]})
            watcher = plistlib.loads((launch / "com.zhujie14.ov-memory-sync.plist").read_bytes())
            self.assertIn("--durable-events", watcher["ProgramArguments"])
            self.assertEqual(watcher["EnvironmentVariables"]["PM_V45_MEMORY_EVENT_MODE"], "outbox")
            self.assertEqual(json.loads(report.read_text())["decision"], "PASS")

    def test_freeze_mismatch_holds_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            project, codex, launch, report = self._fixture(Path(temp))
            PMSystemStore(codex / "pm-loop/state/pm-system.db").set_migration_freeze(
                migration_id="v45-r2-20260830", migration_epoch="v45-r2-20260830", stage_id="G5", owner="test", deadline_at="2099-01-01T00:00:00Z"
            )
            result = apply_g6(project_root=project, codex_root=codex, launch_root=launch, db_path=codex / "pm-loop/state/pm-system.db", backup_root=Path(temp) / "backup", manifest_path=report, execute_launchd=False)
            self.assertEqual(result["decision"], "HOLD")
            self.assertTrue(any("persistent freeze" in item for item in result["issues"]))


if __name__ == "__main__":
    unittest.main()
