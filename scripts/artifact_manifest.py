#!/usr/bin/env python3
"""Write and atomically register local-private PM Worker Artifact Manifests."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

try:  # Worker imports this module from the runtime scripts directory.
    from pm_loop_runtime import atomic_json_write
except ModuleNotFoundError:  # Unit tests may import it as scripts.artifact_manifest.
    from .pm_loop_runtime import atomic_json_write


SCHEMA = "pm-loop.artifact-manifest.v1"
INDEX_SCHEMA = "pm-loop.artifact-manifest-index.v1"
_TITLES = {
    "databuilder-product-gap-report": "DataBuilder 产品缺口与安排建议",
    "product-docs-gap-report": "胜算产品资料缺失周报",
    "competitive-radar-brief": "竞品雷达周报",
    "product-intelligence-monitor": "产品情报周度比较",
    "artifact-inventory": "PM Loop 全项目产物盘点",
}
_TYPES = {
    "databuilder-product-gap-report": "product_gap_report",
    "product-docs-gap-report": "knowledge_gap_report",
    "competitive-radar-brief": "competitive_radar_report",
    "product-intelligence-monitor": "product_intelligence_delta",
    "artifact-inventory": "artifact_inventory",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _safe_relative_path(raw_path: Any, project_root: Path) -> str | None:
    """Return a project-relative regular-file path without following links."""
    try:
        path = Path(str(raw_path)).expanduser()
        lexical = path.absolute()
        root = project_root.absolute()
        resolved_root = root.resolve(strict=True)
        try:
            relative = lexical.relative_to(root)
        except ValueError:
            # /var and /private/var are the same macOS location. Normalise
            # only this physical alias before applying the in-root link fence.
            lexical = lexical.resolve(strict=True)
            root = resolved_root
            relative = lexical.relative_to(root)
        current = root
        if current.is_symlink():
            return None
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                return None
        if not lexical.is_file():
            return None
        lexical.resolve(strict=True).relative_to(resolved_root)
        return relative.as_posix()
    except (OSError, RuntimeError, ValueError):
        return None


def _manifest_root(project_root: Path) -> Path:
    return project_root / "state" / "pm-loop" / "artifact-manifests"


def _registry_root(project_root: Path) -> Path:
    return project_root / "state" / "pm-loop" / "artifact-registry"


def _index_path(project_root: Path) -> Path:
    return _registry_root(project_root) / "manifest-index.json"


def _load_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": INDEX_SCHEMA, "entries": []}
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("artifact manifest index is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("artifact manifest index is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != INDEX_SCHEMA:
        raise RuntimeError("artifact manifest index schema is invalid")
    entries = value.get("entries")
    if not isinstance(entries, list) or not all(isinstance(item, dict) for item in entries):
        raise RuntimeError("artifact manifest index entries are invalid")
    return value


@contextmanager
def _index_lock(path: Path) -> Iterator[None]:
    """Serialize index updates across Worker processes without trusting links."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise RuntimeError("artifact manifest index lock is not a regular file")
    descriptor = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield None
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _manifest_path(project_root: Path, artifact_id: str) -> Path:
    digest = artifact_id.rsplit(":", 1)[-1]
    return _manifest_root(project_root) / digest[:2] / f"{digest}.json"


def _safe_existing_manifest(project_root: Path, relative: Any) -> Path | None:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        return None
    path = project_root / relative
    root = _manifest_root(project_root)
    try:
        path.relative_to(root)
    except ValueError:
        return None
    return path if _safe_relative_path(path, project_root) is not None else None


def _read_manifest(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("artifact manifest is unreadable") from exc
    if not isinstance(value, Mapping) or value.get("schema_version") != SCHEMA:
        raise RuntimeError("artifact manifest schema is invalid")
    return value


def _run_record(execution: Mapping[str, Any], generated_at: str | None) -> dict[str, Any]:
    return {
        "run_id": str(execution.get("run_id") or "") or None,
        "job_id": str(execution.get("job_id") or "") or None,
        "registered_at": _now(),
        "generated_at": generated_at,
    }


def _content_hash_and_representations(package: Mapping[str, Any], project_root: Path) -> tuple[str, dict[str, str], int]:
    """Prefer business body hash over runtime evidence hash for idempotency."""
    representations: dict[str, str] = {}
    hashes: dict[str, list[str]] = {"primary_markdown": [], "primary_html": [], "handler_evidence": []}
    fallback_artifacts: list[dict[str, str]] = []
    evidence_count = 0
    for item in package.get("artifacts") or []:
        if not isinstance(item, Mapping):
            continue
        evidence_count += 1
        role = str(item.get("role") or "")
        content_hash = str(item.get("sha256") or "")
        if role in hashes and content_hash:
            hashes[role].append(content_hash)
        if content_hash:
            fallback_artifacts.append({"role": role, "sha256": content_hash})
        relative = _safe_relative_path(item.get("uri"), project_root)
        if relative is None:
            continue
        suffix = Path(relative).suffix.lower()
        if role == "primary_markdown" or suffix in {".md", ".mdx"}:
            representations.setdefault("markdown_path", relative)
        elif role == "primary_html" or suffix in {".html", ".htm"}:
            representations.setdefault("html_path", relative)
        elif suffix == ".pdf":
            representations.setdefault("pdf_path", relative)
    for role in ("primary_markdown", "primary_html", "handler_evidence"):
        if hashes[role]:
            return hashes[role][0], representations, evidence_count
    return _canonical_hash({"artifacts": fallback_artifacts, "execution_status": (package.get("outcome") or {}).get("execution_status")}), representations, evidence_count


def _generated_at(package: Mapping[str, Any]) -> str | None:
    for stage in package.get("stages") or []:
        if isinstance(stage, Mapping) and stage.get("completed_at"):
            return str(stage["completed_at"])
    return None


def write_worker_artifact_manifest(*, project_root: Path, package: Mapping[str, Any]) -> Path:
    """Commit a Manifest and its index entry.

    The index is the live Registry ingress. A manifest is written before its
    index pointer, so an interrupted write can leave only an unreachable local
    file; it cannot expose a half-written manifest. The business identity is
    ``schedule_key + occurrence_id + content_hash``. Retried Runs append a run
    record to that identity; changed content for one occurrence creates a new
    version with an explicit ``supersedes`` link.
    """
    project_root = Path(project_root).expanduser().resolve()
    task = package.get("task") if isinstance(package.get("task"), Mapping) else {}
    execution = package.get("execution") if isinstance(package.get("execution"), Mapping) else {}
    outcome = package.get("outcome") if isinstance(package.get("outcome"), Mapping) else {}
    schedule_key = str(task.get("schedule_key") or execution.get("schedule_key") or "unknown")
    occurrence_id = str(execution.get("occurrence_id") or "")
    content_hash, representations, evidence_count = _content_hash_and_representations(package, project_root)
    artifact_id = "artifact:sha256:" + hashlib.sha256(
        f"{schedule_key}:{occurrence_id}:{content_hash}".encode("utf-8")
    ).hexdigest()
    generated_at = _generated_at(package)
    index_path = _index_path(project_root)
    lock_path = index_path.with_suffix(".lock")

    with _index_lock(lock_path):
        index = _load_index(index_path)
        entries = list(index.get("entries") or [])
        matching = next((item for item in entries if item.get("artifact_id") == artifact_id), None)
        if isinstance(matching, Mapping):
            existing_path = _safe_existing_manifest(project_root, matching.get("manifest_path"))
            if existing_path is None:
                raise RuntimeError("indexed artifact manifest is unavailable")
            existing = _read_manifest(existing_path)
            if any(existing.get(key) != expected for key, expected in {
                "artifact_id": artifact_id,
                "schedule_key": schedule_key,
                "occurrence_id": occurrence_id or None,
                "content_hash": content_hash,
            }.items()):
                raise RuntimeError("artifact manifest identity collision")
            run_records = list(matching.get("runs") or [])
            record = _run_record(execution, generated_at)
            if record["run_id"] and not any(item.get("run_id") == record["run_id"] for item in run_records if isinstance(item, Mapping)):
                run_records.append(record)
                matching = {**dict(matching), "runs": run_records, "last_registered_at": record["registered_at"]}
                index["entries"] = [matching if item.get("artifact_id") == artifact_id else item for item in entries]
                index["updated_at"] = record["registered_at"]
                atomic_json_write(index_path, index)
            return existing_path

        prior = [
            item for item in entries
            if isinstance(item, Mapping)
            and item.get("schedule_key") == schedule_key
            and item.get("occurrence_id") == (occurrence_id or None)
            and item.get("artifact_id")
        ]
        prior.sort(key=lambda item: str(item.get("last_registered_at") or item.get("registered_at") or ""))
        supersedes = prior[-1].get("artifact_id") if prior else None
        manifest = {
            "schema_version": SCHEMA,
            "artifact_id": artifact_id,
            "title": _TITLES.get(schedule_key, schedule_key),
            "artifact_type": _TYPES.get(schedule_key, "run_evidence"),
            "domain": "runtime_evidence" if not representations else "business",
            "status": str(outcome.get("execution_status") or "unknown"),
            "generated_at": generated_at,
            "producer": schedule_key,
            "schedule_key": schedule_key,
            "occurrence_id": occurrence_id or None,
            "job_id": str(execution.get("job_id") or "") or None,
            "run_id": str(execution.get("run_id") or "") or None,
            "content_hash": content_hash,
            "supersedes": supersedes,
            "representations": representations,
            "evidence": {
                "artifact_count": evidence_count,
                "grade": "unknown",
                "freshness": "unknown",
                "visibility_gap": "unknown",
            },
            "related": {"customers": [], "requirements": [], "tasks": [], "versions": [], "decisions": []},
            "failure": {
                "class": str(outcome.get("failure_class") or "none"),
                "reason": str(outcome.get("impact") or ""),
                "safe_statement": str(outcome.get("safe_statement") or "未记录"),
            },
            "retention_class": "R0_formal" if representations else "R1_evidence",
            "visibility": "local_private",
            "source": {"kind": "worker_task_package", "path_disclosed": False},
        }
        destination = _manifest_path(project_root, artifact_id)
        if destination.exists():
            if destination.is_symlink() or not destination.is_file():
                raise RuntimeError("artifact manifest destination is unsafe")
            existing = _read_manifest(destination)
            if not all(existing.get(key) == manifest.get(key) for key in ("artifact_id", "schedule_key", "occurrence_id", "content_hash")):
                raise RuntimeError("artifact manifest identity collision")
            # An interrupted prior index commit left this immutable manifest
            # unreachable. Reusing exactly the same identity makes recovery
            # possible without overwriting a business record.
            manifest = dict(existing)
        else:
            # Write first. Only the index makes the Manifest visible to readers.
            atomic_json_write(destination, manifest)
        registered_at = _now()
        entry = {
            "artifact_id": artifact_id,
            "schedule_key": schedule_key,
            "occurrence_id": occurrence_id or None,
            "content_hash": content_hash,
            "manifest_path": destination.relative_to(project_root).as_posix(),
            "supersedes": supersedes,
            "registered_at": registered_at,
            "last_registered_at": registered_at,
            "runs": [_run_record(execution, generated_at)],
        }
        entries.append(entry)
        entries.sort(key=lambda item: (str(item.get("schedule_key") or ""), str(item.get("occurrence_id") or ""), str(item.get("artifact_id") or "")))
        atomic_json_write(index_path, {"schema_version": INDEX_SCHEMA, "updated_at": registered_at, "entries": entries})
        return destination


__all__ = ["INDEX_SCHEMA", "SCHEMA", "write_worker_artifact_manifest"]
