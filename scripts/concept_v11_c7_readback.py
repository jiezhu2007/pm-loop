#!/usr/bin/env python3
"""Collect an independent, read-only OpenViking content read-back manifest.

The input is the metadata-only C7 source manifest.  Each unique source URI is
read once through the local ``openviking-rest`` facade; no resource write,
semantic submission, or database mutation is performed here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_ROOT = Path.home() / ".codex"
OV_REST = CODEX_ROOT / "skills" / "openviking-rest" / "scripts" / "ov_rest.py"
SCHEMA = "concept-v11.c7-content-readback.v1"
SOURCE_MANIFEST_SCHEMA = "concept-source-manifest.v1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _hash_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON: {path}: {exc}") from exc


def _unique_uris(manifest: Mapping[str, Any]) -> list[str]:
    if str(manifest.get("schema_version") or "") != SOURCE_MANIFEST_SCHEMA:
        raise RuntimeError("unsupported source manifest schema")
    values: set[str] = set()
    for row in manifest.get("active_source_checks") or []:
        if isinstance(row, Mapping):
            uri = str(row.get("source_uri") or "").strip()
            if uri:
                values.add(uri)
    return sorted(values)


def _read_one(uri: str, *, timeout: int, ov_rest: Path) -> dict[str, Any]:
    observed_at = _now()
    command = [sys.executable, str(ov_rest), "read", uri, "--limit", "-1"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=max(1, int(timeout)), check=False)
    except subprocess.TimeoutExpired as exc:
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": f"timeout:{int(timeout)}s"}
    except OSError as exc:
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": f"process:{type(exc).__name__}"}
    raw = (completed.stdout or "").strip()
    if completed.returncode != 0:
        detail = (completed.stderr or raw or f"exit:{completed.returncode}").strip().replace("\n", " ")
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": detail[:500]}
    try:
        response = json.loads(raw)
    except json.JSONDecodeError:
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": "invalid_json_response"}
    if not isinstance(response, Mapping) or str(response.get("status") or "") != "ok":
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": "read_not_ok"}
    content = response.get("result")
    if isinstance(content, str):
        body = content
    elif content is None:
        return {"uri": uri, "status": "failed", "observed_at": observed_at, "error": "empty_content"}
    else:
        body = json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "uri": uri,
        "status": "verified",
        "observed_at": observed_at,
        "bytes": len(body.encode("utf-8")),
        "content_sha256": _hash_text(body),
    }


def collect(
    *,
    manifest_path: Path,
    output_path: Path,
    concurrency: int = 4,
    timeout: int = 60,
    ov_rest: Path = OV_REST,
) -> dict[str, Any]:
    manifest_path = manifest_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    manifest = _read_json(manifest_path)
    uris = _unique_uris(manifest)
    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(int(concurrency), 16))) as pool:
        futures = {pool.submit(_read_one, uri, timeout=timeout, ov_rest=ov_rest): uri for uri in uris}
        for future in as_completed(futures):
            rows.append(future.result())
    rows.sort(key=lambda row: str(row.get("uri") or ""))
    verified = sum(1 for row in rows if row.get("status") == "verified")
    failed = len(rows) - verified
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "observed_at": _now(),
        "source_manifest_path": str(manifest_path),
        "source_manifest_sha256": _file_hash(manifest_path),
        "unique_uri_count": len(rows),
        "verified_count": verified,
        "failed_count": failed,
        "rows": rows,
        "status": "verified" if failed == 0 else "partial",
        "read_only": True,
        "external_writes": {"openviking": 0, "database": 0},
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output_path)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--ov-rest", type=Path, default=OV_REST)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = collect(
            manifest_path=args.manifest,
            output_path=args.output,
            concurrency=args.concurrency,
            timeout=args.timeout,
            ov_rest=args.ov_rest,
        )
    except Exception as exc:
        result = {"schema": SCHEMA, "status": "HOLD", "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(result, ensure_ascii=False))
        return 1
    print(json.dumps({key: result[key] for key in ("schema", "status", "unique_uri_count", "verified_count", "failed_count", "read_only", "external_writes")}, ensure_ascii=False))
    return 0 if result["status"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
