#!/usr/bin/env python3
"""Compatibility entry point for the unified PM Loop scheduler catch-up mode.

The historical ``~/.codex/scripts/catchup.py`` path is kept for callers that
still invoke it, but it delegates to the runtime dispatcher and never owns a
task list or calls launchctl.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


RUNTIME_ROOT = Path(
    os.environ.get("PM_LOOP_RUNTIME_ROOT", str(Path.home() / ".codex" / "pm-loop" / "runtime"))
).expanduser()
RUNTIME_SCRIPTS = RUNTIME_ROOT / "scripts"
if not RUNTIME_SCRIPTS.is_dir():
    RUNTIME_SCRIPTS = Path(__file__).resolve().parent
    RUNTIME_ROOT = RUNTIME_SCRIPTS.parent
sys.path.insert(0, str(RUNTIME_SCRIPTS))

from pm_loop_catchup import main  # noqa: E402


if __name__ == "__main__":
    # ``pm_schedule_registry`` keeps the source checkout default beside the
    # module.  The installed compatibility path must explicitly use the
    # runtime mirror under ``config`` instead.
    args = list(sys.argv[1:])
    if "--registry" not in args:
        args = ["--registry", str(RUNTIME_SCRIPTS.parent / "config" / "schedule-registry.json"), *args]
    if "--runtime-registry" not in args:
        args = [*args, "--runtime-registry", str(RUNTIME_SCRIPTS.parent / "config" / "schedule-registry.json")]
    raise SystemExit(main(args))
