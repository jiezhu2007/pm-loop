from __future__ import annotations

import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_scheduler_cutover import LEGACY_LABELS, cutover  # noqa: E402


class SchedulerCutoverTests(unittest.TestCase):
    def _write_legacy_plists(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        for label in LEGACY_LABELS:
            (root / f"{label}.plist").write_bytes(plistlib.dumps({"Label": label, "ProgramArguments": ["/bin/true"]}))

    def test_plan_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            launch_agents = root / "LaunchAgents"
            self._write_legacy_plists(launch_agents)
            result = cutover(launch_agents=launch_agents, backup_root=root / "backups", domain="gui/501", apply=False)
            self.assertEqual(result["status"], "planned")
            self.assertFalse((root / "backups").exists())
            self.assertTrue(all(item["exists"] for item in result["legacy"]))

    def test_apply_backs_up_then_disables_and_boots_out_each_label(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            launch_agents = root / "LaunchAgents"
            self._write_legacy_plists(launch_agents)
            commands: list[list[str]] = []

            def fake_runner(command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, "", "")

            result = cutover(launch_agents=launch_agents, backup_root=root / "backups", domain="gui/501", apply=True, runner=fake_runner)
            self.assertEqual(result["status"], "applied")
            backup = Path(result["backup_dir"])
            self.assertTrue((backup / "manifest.json").is_file())
            self.assertEqual({path.stem for path in backup.glob("*.plist")}, set(LEGACY_LABELS))
            for label in LEGACY_LABELS:
                current = plistlib.loads((launch_agents / f"{label}.plist").read_bytes())
                backup_value = plistlib.loads((backup / f"{label}.plist").read_bytes())
                self.assertTrue(current["Disabled"])
                self.assertNotIn("Disabled", backup_value)
            self.assertEqual(len(commands), len(LEGACY_LABELS) * 2)
            self.assertTrue(all(command[0:2] == ["launchctl", "disable"] or command[0:2] == ["launchctl", "bootout"] for command in commands))


if __name__ == "__main__":
    unittest.main()
