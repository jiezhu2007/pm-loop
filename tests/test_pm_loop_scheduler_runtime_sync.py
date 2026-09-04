from __future__ import annotations

import plistlib
import json
import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_scheduler_runtime_sync import CONFIG_FILES, FILES, sync  # noqa: E402


class SchedulerRuntimeSyncTests(unittest.TestCase):
    def test_sync_copies_scheduler_surface_and_creates_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            runtime = root / "runtime"
            backup = root / "backups"
            compat = root / "codex-scripts"
            checker = root / "skills" / "system-health-check" / "scripts" / "check_unified_scheduler.py"
            compat.mkdir()
            old_catchup = compat / "catchup.py"
            old_catchup.write_text("old\n", encoding="utf-8")
            checker.parent.mkdir(parents=True)
            checker.write_text("old checker\n", encoding="utf-8")
            for name in FILES:
                path = runtime / "scripts" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"old {name}\n", encoding="utf-8")
            for name in CONFIG_FILES:
                path = runtime / "config" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"{{\"old\": \"{name}\"}}\n", encoding="utf-8")
            result = sync(
                runtime_root=runtime,
                backup_root=backup,
                compat_root=compat,
                checker_path=checker,
            )
            self.assertTrue(result["verified"])
            self.assertTrue((runtime / "scripts" / "pm_loop_scheduler.py").is_file())
            self.assertTrue((runtime / "scripts" / "pm_scheduled_handlers.py").is_file())
            self.assertTrue((runtime / "scripts" / "concept_refresh_planner.py").is_file())
            self.assertTrue((runtime / "scripts" / "pm_system_gateway.py").is_file())
            self.assertTrue((runtime / "scripts" / "pm_resource_dispatcher.py").is_file())
            self.assertTrue((runtime / "scripts" / "pm_schedule_registry.py").is_file())
            self.assertTrue((runtime / "scripts" / "pm_system_cockpit.py").is_file())
            self.assertTrue((runtime / "scripts" / "process_utils.py").is_file())
            self.assertTrue((runtime / "scripts" / "retention_observer.py").is_file())
            self.assertTrue((runtime / "scripts" / "retention_reclaimer.py").is_file())
            self.assertTrue((runtime / "scripts" / "retention_read_model.py").is_file())
            self.assertTrue((runtime / "scripts" / "concept_inventory_compaction.py").is_file())
            self.assertTrue((runtime / "config" / "schedule-registry.json").is_file())
            self.assertTrue((runtime / "config" / "retention-source-registry.json").is_file())
            self.assertTrue((runtime / "config" / "retention-policy.v3.json").is_file())
            self.assertTrue((runtime / "config" / "retention-deletion-capabilities.json").is_file())
            self.assertTrue((compat / "catchup.py").is_file())
            self.assertIn("unified PM Loop scheduler", (compat / "catchup.py").read_text(encoding="utf-8"))
            self.assertIn("runtime mirror under", (compat / "catchup.py").read_text(encoding="utf-8"))
            self.assertEqual(len(list(backup.iterdir())), 1)
            self.assertTrue((next(backup.iterdir()) / "compat" / "catchup.py").is_file())
            self.assertTrue((next(backup.iterdir()) / "checker" / "check_unified_scheduler.py").is_file())
            self.assertIsNotNone(result["snapshot_manifest"])
            manifest = json.loads((next(backup.iterdir()) / "snapshot-manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(len(manifest["files"]), len(FILES) + len(CONFIG_FILES))
            self.assertEqual(
                checker.read_text(encoding="utf-8"),
                (ROOT / "scripts" / "check_unified_scheduler.py").read_text(encoding="utf-8"),
            )

    def test_scheduler_plist_only_invokes_dispatcher(self) -> None:
        value = plistlib.loads((ROOT / "scripts" / "com.zhujie14.pm-scheduler.plist").read_bytes())
        self.assertEqual(value["Label"], "com.zhujie14.pm-scheduler")
        arguments = value["ProgramArguments"]
        self.assertIn("pm_loop_scheduler.py", arguments[1])
        self.assertEqual(arguments[arguments.index("--mode") + 1], "calendar")
        registry_arg = arguments[arguments.index("--registry") + 1]
        self.assertEqual(registry_arg, "__PM_LOOP_RUNTIME_ROOT__/config/schedule-registry.json")
        canonical_registry_arg = arguments[arguments.index("--canonical-registry") + 1]
        self.assertEqual(canonical_registry_arg, "__PM_LOOP_PROJECT_ROOT__/scripts/schedule-registry.json")
        self.assertNotIn("--runtime-registry", arguments)
        self.assertEqual(value["StartInterval"], 30)
        self.assertTrue(value["RunAtLoad"])
        self.assertNotIn("weekly-sync-and-refresh.sh", " ".join(arguments))


if __name__ == "__main__":
    unittest.main()
