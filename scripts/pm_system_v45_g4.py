#!/usr/bin/env python3
"""Read-only V4.5 G4 MemoryLink capability probe.

G4 must distinguish a real standalone MemoryLink contract from OpenViking's
session commit/extraction APIs.  This module only reads ``/openapi.json`` and
never creates a session, commits messages, writes resources, or changes the
coordination database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional


DEFAULT_URL = os.environ.get("OPENVIKING_URL", "http://127.0.0.1:1933").rstrip("/")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def probe_openapi(base_url: str = DEFAULT_URL, *, timeout: float = 30.0) -> dict[str, Any]:
    """Return auditable path evidence and a conservative G4 decision."""
    url = base_url.rstrip("/") + "/openapi.json"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    captured_at = _now()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            document = json.loads(raw.decode("utf-8"))
            if not isinstance(document, Mapping):
                raise ValueError("OpenAPI document is not an object")
            paths = sorted(str(path) for path in (document.get("paths") or {}).keys())
            lower = {path: path.lower() for path in paths}
            session_paths = [path for path in paths if "/session" in lower[path]]
            memory_paths = [path for path in paths if "memory" in lower[path]]
            # A standalone link endpoint must name both concepts.  Session
            # commit/extract APIs are intentionally excluded: they persist
            # conversation memory and do not prove resource linking semantics.
            independent_link_paths = [
                path
                for path in paths
                if "memory" in lower[path]
                and "link" in lower[path]
                and "/session" not in lower[path]
            ]
            resource_paths = [path for path in paths if "/resource" in lower[path]]
            return {
                "captured_at": captured_at,
                "url": url,
                "http_status": int(getattr(response, "status", 200)),
                "openapi_version": document.get("openapi"),
                "service_version": (document.get("info") or {}).get("version") if isinstance(document.get("info"), Mapping) else None,
                "document_sha256": hashlib.sha256(raw).hexdigest(),
                "path_count": len(paths),
                "session_paths": session_paths,
                "memory_paths": memory_paths,
                "independent_memory_link_paths": independent_link_paths,
                "resource_paths": resource_paths,
                "standalone_memory_link_api": bool(independent_link_paths),
                "status": "accepted",
                "decision": "api_present_needs_smoke" if independent_link_paths else "skipped_hold",
            }
    except Exception as exc:  # pragma: no cover - exercised by host probe
        return {
            "captured_at": captured_at,
            "url": url,
            "status": "unknown",
            "decision": "hold",
            "error": f"{type(exc).__name__}: {exc}",
            "standalone_memory_link_api": False,
            "session_paths": [],
            "memory_paths": [],
            "independent_memory_link_paths": [],
            "resource_paths": [],
        }


def classify_g4(probe: Mapping[str, Any], *, adapter: Optional[str] = None, smoke: Optional[str] = None) -> dict[str, Any]:
    """Map probe evidence to PASS, PASS_WITH_SKIP, or HOLD.

    A path listing alone never proves the adapter contract.  A configured
    adapter therefore remains HOLD until its separately recorded smoke marker
    is explicitly ``pass``.
    """
    if str(probe.get("status")) != "accepted":
        return {"decision": "HOLD", "reason": "OpenAPI probe unavailable", "probe": dict(probe)}
    if bool(probe.get("standalone_memory_link_api")):
        if adapter and str(smoke or "").lower() == "pass":
            return {"decision": "PASS", "reason": "standalone API and adapter smoke marker passed", "probe": dict(probe), "adapter": adapter}
        return {"decision": "HOLD", "reason": "standalone API exists but adapter/read-back smoke is not proven", "probe": dict(probe), "adapter": adapter or ""}
    if adapter:
        return {"decision": "HOLD", "reason": "adapter configured without independent API smoke proof", "probe": dict(probe), "adapter": adapter}
    return {"decision": "PASS_WITH_SKIP", "reason": "no standalone MemoryLink API/adapter; linking remains disabled", "probe": dict(probe)}


def run_g4(*, base_url: str = DEFAULT_URL, timeout: float = 30.0, adapter: Optional[str] = None, smoke: Optional[str] = None) -> dict[str, Any]:
    probe = probe_openapi(base_url, timeout=timeout)
    result = classify_g4(probe, adapter=adapter, smoke=smoke)
    return {"schema": "pm-system.v45-r2-g4-manifest.v1", "captured_at": _now(), **result}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ov-url", default=DEFAULT_URL)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--adapter", default=os.environ.get("PM_V45_MEMORY_LINK_ADAPTER", ""))
    parser.add_argument("--smoke", default=os.environ.get("PM_V45_MEMORY_LINK_SMOKE", ""))
    args = parser.parse_args()
    result = run_g4(base_url=args.ov_url, timeout=args.timeout, adapter=args.adapter or None, smoke=args.smoke or None)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["decision"] in {"PASS", "PASS_WITH_SKIP"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
