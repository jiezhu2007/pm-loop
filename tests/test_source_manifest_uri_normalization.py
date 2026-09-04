from __future__ import annotations

import sys
import unittest
from pathlib import Path


SKILL_SCRIPTS = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from source_manifest import normalize_path  # noqa: E402


class SourceManifestUriNormalizationTests(unittest.TestCase):
    def test_preserves_full_width_parentheses_in_openviking_uri(self) -> None:
        uri = "viking://resources/shengsuan/product-management/iteration/【概要设计】DataBuilder-版本管理（Global-Branching）/技术方案.md"
        self.assertEqual(normalize_path(uri), uri)

    def test_still_normalizes_slashes_and_scheme_case(self) -> None:
        uri = "VIKING://resources//shengsuan\\product-management/概念.md/"
        self.assertEqual(
            normalize_path(uri),
            "viking://resources/shengsuan/product-management/概念.md",
        )


if __name__ == "__main__":
    unittest.main()
