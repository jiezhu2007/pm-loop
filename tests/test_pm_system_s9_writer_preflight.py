from __future__ import annotations

import os
import plistlib
import tempfile
import unittest
from pathlib import Path

from scripts.pm_system_s9_writer_preflight import (
    ACTIVE_SCRIPTS,
    CANONICAL_PYTHON,
    LAUNCH_ROOT,
    WRITER_JOBS,
    extract_totals,
    tree_fingerprint,
    validate_plist,
    validate_script,
)


class S9WriterPreflightTests(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("PM_LOOP_VERIFY_INSTALLED_RUNTIME") == "1",
        "requires the deployment machine's Codex runtime and LaunchAgents",
    )
    def test_installed_writer_scripts_use_only_codex_python_312(self) -> None:
        for path in ACTIVE_SCRIPTS:
            with self.subTest(path=path):
                result = validate_script(path)
                self.assertTrue(result["valid"], result["errors"])

    @unittest.skipUnless(
        os.environ.get("PM_LOOP_VERIFY_INSTALLED_RUNTIME") == "1",
        "requires the deployment machine's Codex runtime and LaunchAgents",
    )
    def test_installed_launchd_contracts_are_canonical(self) -> None:
        for label, contract in WRITER_JOBS.items():
            with self.subTest(label=label):
                result = validate_plist(LAUNCH_ROOT / f"{label}.plist", contract)
                self.assertTrue(result["valid"], result["errors"])

    def test_memory_watcher_uses_the_frozen_codex_runtime_copy(self) -> None:
        program = WRITER_JOBS["com.zhujie14.ov-memory-sync"]["program"]
        self.assertEqual(program[1], str(Path.home() / ".codex/pm-loop/runtime/scripts/ov_memory_sync.py"))

    def test_script_validator_rejects_claude_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "entry.sh"
            path.write_text('PY="${CODEX_PYTHON:-${CLAUDE_PYTHON:-/usr/bin/python3}}"\n', encoding="utf-8")
            result = validate_script(path)
            self.assertFalse(result["valid"])
            self.assertTrue(any("CLAUDE_PYTHON" in item for item in result["errors"]))

    def test_plist_validator_checks_python_and_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "job.plist"
            contract = {
                "program": [CANONICAL_PYTHON, "/tmp/job.py"],
                "working_directory": "/tmp",
                "log_root": "/tmp/logs",
                "python_arguments": [0],
            }
            path.write_bytes(plistlib.dumps({
                "ProgramArguments": [CANONICAL_PYTHON, "/tmp/job.py"],
                "WorkingDirectory": "/tmp",
                "StandardOutPath": "/tmp/logs/job.log",
                "StandardErrorPath": "/tmp/logs/job.log",
            }))
            self.assertTrue(validate_plist(path, contract)["valid"])

    def test_tree_fingerprint_detects_content_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "state.json"
            path.write_text("one", encoding="utf-8")
            before = tree_fingerprint(root)
            path.write_text("two", encoding="utf-8")
            after = tree_fingerprint(root)
            self.assertNotEqual(before["sha256"], after["sha256"])

    def test_extract_totals_uses_last_plan_json(self) -> None:
        value = extract_totals('noise {"discovered":1,"kept":1}\n{"discovered":2,"kept":2}')
        self.assertEqual(value["discovered"], 2)


if __name__ == "__main__":
    unittest.main()
