#!/usr/bin/env python3
"""Audited repair for pre-binding vectors-only concept Canary Outbox rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from pm_system_gateway import SemanticGateway
from pm_system_store import PMSystemStore


DEFAULT_DB_PATH = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the bounded payload repair")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--outbox-id", action="append", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.apply:
        result = {"schema": "concept-v11.vectors-only-policy-repair.v1", "status": "DRY_RUN", "outbox_ids": args.outbox_id}
    else:
        try:
            repaired = SemanticGateway(PMSystemStore(args.db_path.expanduser().resolve(), auto_migrate=False)).repair_vectors_only_concept_policy(args.outbox_id)
            result = {"schema": "concept-v11.vectors-only-policy-repair.v1", "status": "PASS", "repaired": repaired, "external_calls": {"oneapi": 0, "openviking": 0}}
        except (OSError, ValueError, RuntimeError) as exc:
            result = {"schema": "concept-v11.vectors-only-policy-repair.v1", "status": "HOLD", "error": f"{type(exc).__name__}:{exc}"}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
