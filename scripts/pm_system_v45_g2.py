#!/usr/bin/env python3
"""Collect the real V4.5 G2 observation and watermark projection.

The producer reads existing local markers and the local OpenViking health/
resource index.  It never invents a cursor: unavailable signals are written as
an explicit ``missing`` observation with the evidence that was checked.  The
four watermark rows are idempotent and use ``PMSystemStore.put_watermark`` so
out-of-order, replay and same-cursor conflict handling stays in one place.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from pm_system_store import PMSystemStore, now_iso


CODEX_ROOT = Path(os.environ.get("PM_LOOP_CODEX_ROOT", str(Path.home() / ".codex"))).expanduser()
DEFAULT_DB = CODEX_ROOT / "pm-loop/state/pm-system.db"
DEFAULT_SOURCE_MARKER = CODEX_ROOT / "skills/shengsuan-sync/state/direct-run.json"
DEFAULT_CONTENT_MARKER = CODEX_ROOT / "skills/shengsuan-concepts/state/source-manifest.meta.json"
DEFAULT_CONTENT_MANIFEST = CODEX_ROOT / "skills/shengsuan-concepts/state/source-manifest.json"
DEFAULT_WEEKLY_MARKER = CODEX_ROOT / "scripts/state/weekly-sync-and-refresh.done"
DEFAULT_GENERATION_DIR = CODEX_ROOT / "skills/shengsuan-concepts/state"
DEFAULT_OV_URL = os.environ.get("OPENVIKING_URL", "http://127.0.0.1:1933").rstrip("/")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    evidence: dict[str, Any] = {"path": str(path), "exists": path.is_file()}
    if not path.is_file():
        return None, evidence
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            evidence["error"] = "marker is not a JSON object"
            return None, evidence
        evidence.update({"sha256": hashlib.sha256(raw).hexdigest(), "mtime_ns": path.stat().st_mtime_ns})
        return value, evidence
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        return None, evidence


def _epoch_ms(value: Any) -> Optional[int]:
    if value is None or not str(value).strip():
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    parsed = parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def _file_cursor(path: Path, marker: Mapping[str, Any], *time_keys: str) -> tuple[int, int]:
    for key in time_keys:
        captured = _epoch_ms(marker.get(key))
        if captured is not None:
            return captured, 0
    try:
        stat = path.stat()
        return int(stat.st_mtime * 1000), int(stat.st_mtime_ns % 1_000_000)
    except OSError:
        return int(datetime.now(timezone.utc).timestamp() * 1000), 0


def _redact_totals(value: Any) -> Any:
    """Keep only scalar marker totals; never copy document content into DB."""
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, (str, int, float, bool)) or item is None:
            result[str(key)] = item
    return result


def produce_source(path: Path) -> dict[str, Any]:
    marker, evidence = _read_json(path)
    if marker is None:
        return {"captured_at": int(datetime.now(timezone.utc).timestamp() * 1000), "sequence": 0, "value": {"status": "missing", "evidence": evidence}, "state": "missing", "producer": "g2.source-marker"}
    captured, sequence = _file_cursor(path, marker, "finished_at", "updated_at", "started_at")
    totals = _redact_totals(marker.get("totals"))
    value = {"status": str(marker.get("status") or "unknown"), "run_id": marker.get("run_id"), "mode": marker.get("mode"), "exit_code": marker.get("exit_code"), "finished_at": marker.get("finished_at"), "totals": totals, "evidence": evidence}
    state = "accepted" if value["status"] in {"success", "completed", "ok"} and marker.get("exit_code", 0) == 0 else "unknown"
    return {"captured_at": captured, "sequence": sequence, "value": value, "state": state, "producer": "g2.source-marker"}


def produce_content(path: Path, manifest_path: Path) -> dict[str, Any]:
    marker, evidence = _read_json(path)
    if marker is None:
        return {"captured_at": int(datetime.now(timezone.utc).timestamp() * 1000), "sequence": 0, "value": {"status": "missing", "evidence": evidence}, "state": "missing", "producer": "g2.content-manifest"}
    manifest_evidence = {"path": str(manifest_path), "exists": manifest_path.is_file()}
    if manifest_path.is_file():
        try:
            manifest_evidence.update({"sha256": _sha256(manifest_path), "size_bytes": manifest_path.stat().st_size})
        except OSError as exc:
            manifest_evidence["error"] = f"{type(exc).__name__}: {exc}"
    captured, sequence = _file_cursor(path, marker, "generated_at", "updated_at")
    metrics = _redact_totals(marker.get("metrics"))
    manifest_available = bool(manifest_evidence.get("exists")) and not bool(manifest_evidence.get("error"))
    value = {"status": "accepted" if manifest_available else "unknown", "schema_version": marker.get("schema_version"), "generated_at": marker.get("generated_at"), "metrics": metrics, "manifest": manifest_evidence, "marker": evidence}
    return {"captured_at": captured, "sequence": sequence, "value": value, "state": "accepted" if manifest_available else "unknown", "producer": "g2.content-manifest"}


def _request_json(url: str, timeout: float = 5.0) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            value = json.loads(raw.decode("utf-8"))
            return value if isinstance(value, dict) else None, {"url": url, "http_status": response.status, "sha256": hashlib.sha256(raw).hexdigest()}
    except Exception as exc:  # network/protocol errors become explicit evidence
        return None, {"url": url, "error": f"{type(exc).__name__}: {exc}"}


def produce_knowledge(base_url: str) -> dict[str, Any]:
    health, health_evidence = _request_json(base_url + "/health")
    encoded = urllib.parse.quote("viking://resources/shengsuan", safe="")
    listing, listing_evidence = _request_json(base_url + "/api/v1/fs/ls?uri=" + encoded)
    captured = int(datetime.now(timezone.utc).timestamp() * 1000)
    value: dict[str, Any] = {"health": {"status": (health or {}).get("status"), "healthy": (health or {}).get("healthy"), "version": (health or {}).get("version"), "evidence": health_evidence}, "resource_index": {"status": "accepted" if listing is not None else "unknown", "entry_count": _entry_count(listing), "evidence": listing_evidence}}
    state = "accepted" if health and bool(health.get("healthy")) and listing is not None else "unknown"
    return {"captured_at": captured, "sequence": _entry_count(listing), "value": value, "state": state, "producer": "g2.openviking-probe"}


def _entry_count(value: Any) -> int:
    """Count fs/ls entries across the server's mapping and list responses."""
    if isinstance(value, list):
        return len(value)
    if not isinstance(value, Mapping):
        return 0
    result = value.get("result")
    if isinstance(result, list):
        return len(result)
    if isinstance(result, Mapping):
        for key in ("entries", "items", "resources", "children"):
            item = result.get(key)
            if isinstance(item, list):
                return len(item)
    for key in ("entries", "items", "resources", "children"):
        item = value.get(key)
        if isinstance(item, list):
            return len(item)
    return 0


def produce_active_generation(generation_dir: Path, weekly_marker: Path) -> dict[str, Any]:
    candidates = [generation_dir / name for name in ("active-generation.json", "active_generation.json", "current-generation.json", "generation.json")]
    for path in candidates:
        marker, evidence = _read_json(path)
        if marker is None:
            continue
        captured, sequence = _file_cursor(path, marker, "activated_at", "active_at", "generated_at", "updated_at")
        return {"captured_at": captured, "sequence": sequence, "value": {"status": "accepted", "generation": _redact_totals(marker), "evidence": evidence}, "state": "accepted", "producer": "g2.active-generation"}
    weekly, weekly_evidence = _read_json(weekly_marker)
    value = {"status": "missing", "reason": "no active generation marker; concept refresh remains disabled", "candidate_paths": [str(path) for path in candidates], "refresh_disabled": bool((weekly or {}).get("concept_refresh_disabled")) if weekly else None, "weekly_marker": weekly_evidence}
    captured, sequence = _file_cursor(weekly_marker, weekly or {}, "finished_at", "updated_at")
    return {"captured_at": captured, "sequence": sequence, "value": value, "state": "missing", "producer": "g2.active-generation"}


def _module_snapshot(module: str, status: str, details: Mapping[str, Any], source_version: str) -> tuple[str, str, str, str, str]:
    return module, status, now_iso(), json.dumps(dict(details), ensure_ascii=False, sort_keys=True, separators=(",", ":")), source_version


def collect_g2(*, db_path: Path = DEFAULT_DB, source_marker: Path = DEFAULT_SOURCE_MARKER, content_marker: Path = DEFAULT_CONTENT_MARKER, content_manifest: Path = DEFAULT_CONTENT_MANIFEST, weekly_marker: Path = DEFAULT_WEEKLY_MARKER, generation_dir: Path = DEFAULT_GENERATION_DIR, ov_url: str = DEFAULT_OV_URL) -> dict[str, Any]:
    store = PMSystemStore(db_path)
    producers = {"source": produce_source(source_marker), "content": produce_content(content_marker, content_manifest), "knowledge": produce_knowledge(ov_url), "active_generation": produce_active_generation(generation_dir, weekly_marker)}
    outcomes: dict[str, Any] = {}
    for name, item in producers.items():
        outcomes[name] = store.put_watermark(source_domain="pm-runtime", watermark_name=name, captured_at=item["captured_at"], sequence=item["sequence"], value=item["value"], producer=item["producer"], state=item["state"])
        # Module snapshots are observation-only.  Migration freeze is a real
        # state, so stopped business processes are recorded as maintenance,
        # not as healthy placeholders.
    source_version = "g2:" + hashlib.sha256(json.dumps(producers, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
    with store.transaction() as connection:
        modules = {
            "Worker": ("maintenance", {"migration_freeze": True, "producer": "g2.runtime"}),
            "OneAPI": ("unknown", {"probe": "not_called_by_g2", "provider": "oneapi"}),
            "OpenViking": ("healthy" if producers["knowledge"]["state"] == "accepted" else "unknown", producers["knowledge"]["value"]),
            "Source": ("healthy" if producers["source"]["state"] == "accepted" else "unknown", producers["source"]["value"]),
            "Evidence": ("healthy" if producers["content"]["state"] == "accepted" else "unknown", producers["content"]["value"]),
            "Runtime": ("maintenance", {"migration_freeze": True, "producer": "g2.runtime"}),
            "Memory watcher": ("maintenance", {"migration_freeze": True, "producer": "g2.runtime"}),
            "Scheduler": ("maintenance", {"migration_freeze": True, "producer": "g2.runtime"}),
            "Outbox Writer": ("maintenance", {"migration_freeze": True, "producer": "g2.runtime"}),
        }
        for module, (status, details) in modules.items():
            connection.execute("INSERT INTO module_health_snapshots(module,status,observed_at,details_json,source_version) VALUES(?,?,?,?,?)", _module_snapshot(module, status, details, source_version))
        for metric_name, value in (("g2.watermark.count", len(producers)), ("g2.watermark.accepted", sum(item["state"] == "accepted" for item in producers.values())), ("g2.watermark.missing", sum(item["state"] == "missing" for item in producers.values()))):
            bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat(timespec="minutes").replace("+00:00", "Z")
            connection.execute("INSERT INTO metric_rollups(metric_name,bucket_start,value,dimensions_json,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(metric_name,bucket_start,dimensions_json) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", (metric_name, bucket, float(value), "{}", now_iso()))
    return {"captured_at": now_iso(), "db_path": str(db_path), "watermarks": producers, "outcomes": outcomes, "source_version": source_version}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--source-marker", type=Path, default=DEFAULT_SOURCE_MARKER)
    parser.add_argument("--content-marker", type=Path, default=DEFAULT_CONTENT_MARKER)
    parser.add_argument("--content-manifest", type=Path, default=DEFAULT_CONTENT_MANIFEST)
    parser.add_argument("--weekly-marker", type=Path, default=DEFAULT_WEEKLY_MARKER)
    parser.add_argument("--generation-dir", type=Path, default=DEFAULT_GENERATION_DIR)
    parser.add_argument("--ov-url", default=DEFAULT_OV_URL)
    args = parser.parse_args()
    print(json.dumps(collect_g2(**vars(args)), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
