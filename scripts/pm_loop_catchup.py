#!/usr/bin/env python3
"""Compatibility entry point for PM Loop login/startup catch-up.

Catch-up is a dispatcher mode, not a second scheduler.  This module contains
no task list and never invokes the service manager; the unified dispatcher reads the
canonical registry and records the same occurrence/Job evidence as a normal
calendar tick.  A one-shot compatibility invocation may fall back to the
canonical file when its runtime mirror has not been materialized yet; the live
Scheduler still requires canonical/runtime hash agreement and remains fail-closed.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from pm_loop_scheduler import DEFAULT_DB_PATH, DEFAULT_LOCK_PATH, DEFAULT_RUNTIME_REGISTRY, PMLoopDispatcher
from pm_schedule_registry import DEFAULT_REGISTRY_PATH


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--runtime-registry", type=Path, default=DEFAULT_RUNTIME_REGISTRY)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--now")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else datetime.now(timezone.utc)
    runtime_registry = args.runtime_registry
    if runtime_registry is not None and not runtime_registry.expanduser().is_file():
        runtime_registry = None
    result = PMLoopDispatcher(
        args.db_path,
        registry_path=args.registry,
        runtime_registry_path=runtime_registry,
        lock_path=args.lock_path,
    ).tick(now=now, mode="catchup", dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
