from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import concept_v11_provider_runtime_sync as runtime_sync


class ConceptV11ProviderRuntimeSyncTests(unittest.TestCase):
    def test_sync_backs_up_and_atomically_converges_all_provider_scripts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical"
            runtime = root / "runtime"
            for name in runtime_sync.RUNTIME_FILES:
                source = canonical / "scripts" / name
                target = runtime / "scripts" / name
                source.parent.mkdir(parents=True, exist_ok=True)
                target.parent.mkdir(parents=True, exist_ok=True)
                source.write_text(f"canonical:{name}\n", encoding="utf-8")
                target.write_text(f"runtime:{name}\n", encoding="utf-8")
            with patch.object(runtime_sync, "PROJECT_ROOT", canonical):
                dry_run = runtime_sync.sync(runtime_root=runtime, backup_root=root / "backups", apply=False)
                applied = runtime_sync.sync(runtime_root=runtime, backup_root=root / "backups", apply=True)

            self.assertEqual(dry_run["status"], "DRY_RUN")
            self.assertEqual(set(dry_run["would_change"]), set(runtime_sync.RUNTIME_FILES))
            self.assertEqual(applied["status"], "PASS")
            self.assertTrue(applied["verified"])
            for name in runtime_sync.RUNTIME_FILES:
                self.assertEqual(
                    (runtime / "scripts" / name).read_text(encoding="utf-8"),
                    f"canonical:{name}\n",
                )
                self.assertEqual(
                    Path(applied["files"][name]["backup"]).read_text(encoding="utf-8"),
                    f"runtime:{name}\n",
                )


if __name__ == "__main__":
    unittest.main()
