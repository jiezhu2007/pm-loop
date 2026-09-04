from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_SCRIPTS = PROJECT_ROOT / "scripts"
RUNTIME_SCRIPTS = Path.home() / ".codex" / "pm-loop" / "runtime" / "scripts"


class DisabledConceptCliTests(unittest.TestCase):
    """Direct concept write entrances remain harmless after workflow retirement."""

    def _run(
        self,
        script_dir: Path,
        script_name: str,
        args: list[str],
        *,
        env: dict[str, str],
    ) -> dict:
        script = script_dir / script_name
        self.assertTrue(script.is_file(), script)
        result = subprocess.run(
            [sys.executable, str(script), *args],
            cwd=script_dir,
            env={
                **os.environ,
                **env,
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "", result.stderr)
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:  # pragma: no cover - diagnostic path
            self.fail(f"{script} returned non-JSON output: {result.stdout!r}: {exc}")
        self.assertEqual(value.get("status"), "disabled")
        self.assertTrue(value.get("read_only"))
        return value

    @staticmethod
    def _snapshot(root: Path) -> list[str]:
        return sorted(str(path.relative_to(root)) for path in root.rglob("*"))

    def test_all_project_and_runtime_cli_write_paths_are_disabled(self) -> None:
        """The guard runs before files, OpenViking, or LLM state are touched."""

        with tempfile.TemporaryDirectory(prefix="concept-disabled-cli-") as temp:
            root = Path(temp)
            codex_root = root / "codex"
            skill_root = root / "skill"
            # A sentinel LLM module would leave a marker if triage loaded it.
            llm_path = codex_root / "skills" / "shengsuan-concepts" / "scripts" / "llm_runner.py"
            llm_path.parent.mkdir(parents=True)
            marker = root / "llm-imported"
            llm_path.write_text(
                "from pathlib import Path\n"
                f"Path({str(marker)!r}).write_text('imported', encoding='utf-8')\n",
                encoding="utf-8",
            )
            # Deliberately point discovery/triage at paths that do not exist;
            # a disabled CLI must return before opening them.
            missing_uris = root / "missing-uris.txt"
            missing_manual = root / "missing-manual.jsonl"
            missing_content = root / "missing-reviewed.md"
            env = {
                "CODEX_HOME": str(root / "codex-home"),
                "OPENVIKING_URL": "http://127.0.0.1:1",
            }

            for script_dir in (PROJECT_SCRIPTS, RUNTIME_SCRIPTS):
                before = self._snapshot(root)
                self._run(
                    script_dir,
                    "concept_discovery.py",
                    ["--codex-root", str(codex_root), "--uris-file", str(missing_uris)],
                    env=env,
                )
                self._run(
                    script_dir,
                    "concept_discovery_triage.py",
                    ["--codex-root", str(codex_root), "--run-id", "missing-run"],
                    env=env,
                )
                self._run(
                    script_dir,
                    "concept_signal_discovery.py",
                    [
                        "--codex-root",
                        str(codex_root),
                        "--manual-path",
                        str(missing_manual),
                        "--json",
                    ],
                    env=env,
                )
                self._run(
                    script_dir,
                    "concept_candidate_admin.py",
                    [
                        "--skill-root",
                        str(skill_root),
                        "supersede-refresh",
                        "--expected-count",
                        "0",
                        "--apply",
                    ],
                    env=env,
                )
                self._run(
                    script_dir,
                    "concept_candidate_admin.py",
                    [
                        "--skill-root",
                        str(skill_root),
                        "restore-new-concept",
                        "--source-candidate",
                        "missing-candidate",
                        "--content",
                        str(missing_content),
                    ],
                    env=env,
                )
                self._run(
                    script_dir,
                    "concept_reclassify_candidates.py",
                    ["--skill-root", str(skill_root), "--apply"],
                    env=env,
                )
                self.assertEqual(self._snapshot(root), before)

            self.assertFalse(marker.exists(), "triage imported an LLM module while disabled")

    def test_legacy_bootstrap_is_disabled_before_loading_dependencies(self) -> None:
        """The manual first-compile command cannot revive concept writes."""

        script = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "scripts" / "bootstrap.py"
        self.assertTrue(script.is_file(), script)
        with tempfile.TemporaryDirectory(prefix="concept-disabled-bootstrap-") as temp:
            root = Path(temp)
            result = subprocess.run(
                [sys.executable, str(script), "--all"],
                cwd=root,
                env={
                    **os.environ,
                    "CODEX_HOME": str(root / "codex-home"),
                    "OPENVIKING_URL": "http://127.0.0.1:1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            payload = json.loads(result.stdout)
            self.assertEqual(payload.get("status"), "disabled")
            self.assertTrue(payload.get("read_only"))
            self.assertEqual(self._snapshot(root), [])


if __name__ == "__main__":
    unittest.main()
