from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_runtime import RunStore  # noqa: E402
from pm_system_shadow import compare_shadow, import_legacy_shadow, legacy_digest  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402


class ShadowProjectionTests(unittest.TestCase):
    def test_shadow_projection_is_idempotent_and_does_not_mutate_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy_dir = root / "legacy"
            legacy = RunStore(legacy_dir)
            complete = legacy.create({"loop_id": "complete"})
            legacy.append(complete["run_id"], "run/started")
            legacy.append(complete["run_id"], "run/completed")
            cancelled = legacy.create({"loop_id": "cancelled"})
            legacy.append(cancelled["run_id"], "run/cancelled")
            before = legacy_digest(legacy_dir)
            store = PMSystemStore(root / "pm-system.db")
            first = import_legacy_shadow(store, legacy_dir)
            second = import_legacy_shadow(store, legacy_dir)
            after = legacy_digest(legacy_dir)
            self.assertEqual(first["created"], 2)
            self.assertEqual(second["created"], 0)
            self.assertEqual(second["existing"], 2)
            self.assertEqual(before, after)
            comparison = compare_shadow(store, legacy_dir)
            self.assertTrue(comparison["status_counts_equal"])
            self.assertEqual(comparison["legacy_count"], comparison["shadow_count"])
            with store.connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs WHERE profile='legacy-shadow'").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()

