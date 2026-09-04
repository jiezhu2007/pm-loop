"""Test-only helpers for environments that prohibit loopback listeners."""

from __future__ import annotations

import unittest
from typing import Any


def create_loopback_server(server_type: Any, address: tuple[str, int], *args: Any) -> Any:
    try:
        return server_type(address, *args)
    except PermissionError as exc:
        raise unittest.SkipTest("loopback listeners are blocked by this execution environment") from exc
