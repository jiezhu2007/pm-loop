#!/usr/bin/env python3
"""Stable Control Plane entry point for the resumable deep inventory runner.

Keep this filename stable for Control Plane and launchd callers.  The actual
implementation lives in ``concept_deep_inventory``; importing the legacy
``concept_full_inventory`` here would silently re-enable the bounded/partial
runner and lose the full-document checkpoint contract.
"""

from concept_deep_inventory import main


if __name__ == "__main__":
    raise SystemExit(main())
