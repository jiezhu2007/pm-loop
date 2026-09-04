#!/usr/bin/env python3
"""Record unmatched document evidence for the new-concept discovery loop."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from concept_learning import ConceptLearningStore, discover_from_uris
from concept_workflow_guard import CONCEPT_REFRESH_DISABLED, emit_disabled


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="概念新发现证据登记")
    parser.add_argument("--codex-root", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--uris-file", type=Path, required=True)
    parser.add_argument("--revisions-file", type=Path, help="URI -> sha256/publishTime JSON map")
    parser.add_argument("--source", default="document_delta")
    args = parser.parse_args(list(argv) if argv is not None else None)
    # Discovery is a write-producing refresh stage.  Keep the CLI boundary
    # fail-closed so stale callers cannot create discovery runs after the
    # Concept Learning workflow has been retired.
    if CONCEPT_REFRESH_DISABLED:
        return emit_disabled("discovery")
    uris = [line.strip() for line in args.uris_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    revisions: Dict[str, Any] = {}
    if args.revisions_file:
        value = json.loads(args.revisions_file.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("revisions file must contain a JSON object")
        revisions = value
    store = ConceptLearningStore(args.codex_root.expanduser() / "skills" / "shengsuan-concepts")
    result = discover_from_uris(store, uris, source=args.source, evidence_revisions=revisions)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
