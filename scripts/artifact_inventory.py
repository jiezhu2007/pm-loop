#!/usr/bin/env python3
"""Read-only, repeatable inventory of project artifacts.

The inventory deliberately does not move, rename, delete, or open files.  It
records regular files under the project root, excluding only explicitly
identified VCS, cache, temporary, and inventory-output directories.  A stable
artifact id is derived from the relative path and SHA-256 content hash, so a
second run over unchanged files is idempotent while changed files can be
linked with ``supersedes`` to the previous observation.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SCHEMA = "pm-loop.artifact-inventory.v1"
HASH_ALGORITHM = "sha256"
DEFAULT_OUTPUT_RELATIVE = Path("state/pm-loop/artifact-inventory")

# These are intentionally narrow and explainable.  Normal data/report
# directories such as docs/, output/, projects/, noteai/ and craft 知识库 are
# included; only obvious transient/runtime caches are omitted.
EXACT_EXCLUDED_DIR_NAMES = frozenset(
    {
        ".git",
        ".worktrees",
        "node_modules",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "__pycache__",
        ".cache",
        "cache",
        "caches",
        "tmp",
        "temp",
        ".tmp",
        ".venv",
        "venv",
        "env",
        ".playwright-cli",
    }
)
EXCLUDED_DIR_PREFIXES = (".tmp",)

EXTENSION_TYPES: Mapping[str, str] = {
    ".md": "markdown",
    ".mdx": "markdown",
    ".html": "html",
    ".htm": "html",
    ".json": "json",
    ".jsonl": "jsonl",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".txt": "text",
    ".csv": "csv",
    ".tsv": "tsv",
    ".py": "source_code",
    ".js": "source_code",
    ".ts": "source_code",
    ".tsx": "source_code",
    ".jsx": "source_code",
    ".sh": "source_code",
    ".zsh": "source_code",
    ".plist": "configuration",
    ".ini": "configuration",
    ".conf": "configuration",
    ".xml": "xml",
    ".pdf": "pdf",
    ".docx": "document",
    ".doc": "document",
    ".xlsx": "spreadsheet",
    ".xls": "spreadsheet",
    ".pptx": "presentation",
    ".ppt": "presentation",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".gif": "image",
    ".webp": "image",
    ".svg": "image",
    ".mp4": "media",
    ".mov": "media",
    ".zip": "archive",
    ".gz": "archive",
}

# The Root Inventory has to observe the whole project.  The Artifact Registry
# does not: only approved business material and PM runtime evidence get a
# content hash and an openable Registry row.  Code, configuration and unknown
# material remain metadata-only until a separate, explicit policy admits them.
BUSINESS_ROOTS = frozenset({"docs", "output", "outputs", "ku-archive", "projects", "demo"})
DEPENDENCY_ROOTS = frozenset({"scripts", "web", "tests"})
SENSITIVE_METADATA_ROOTS = frozenset({"memory", "noteai", "craft 知识库", ".claude", ".openclaw"})
REGISTRY_EXCLUDED_FILENAMES = frozenset({".DS_Store"})
REGISTRY_EXCLUDED_PREFIXES = (
    ("outputs", "openviking-skill-conflicts-20260814"),
    ("output", "pm-loop-control-plane"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> Tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix() if path != root else "."


def exclusion_reason(path: Path, root: Path, output_root: Path) -> Optional[str]:
    """Return a machine-readable exclusion reason, if this directory is omitted."""
    try:
        path.relative_to(output_root)
        return "inventory_output"
    except ValueError:
        pass
    name = path.name
    if name in EXACT_EXCLUDED_DIR_NAMES:
        return "cache_or_temporary_directory"
    if any(name.startswith(prefix) for prefix in EXCLUDED_DIR_PREFIXES):
        return "cache_or_temporary_directory"
    return None


def infer_artifact_type(relative_path: str) -> Tuple[str, str]:
    path = Path(relative_path)
    extension = path.suffix.lower()
    if extension in EXTENSION_TYPES:
        return EXTENSION_TYPES[extension], "extension"
    parts = set(path.parts)
    if "runs" in parts or "run" in parts or "outbox" in parts:
        return "run_evidence", "directory_rule"
    if "docs" in parts:
        return "document", "directory_rule"
    return "unknown", "unknown"


def classify_path(relative_path: str) -> Dict[str, Any]:
    """Classify a root entry without deriving any semantic relationship.

    This deliberately uses controlled path policy only.  It may classify a
    file as a business candidate but never infers customers, requirements,
    tasks, versions, or an authorization to expose a file.
    """
    parts = Path(relative_path).parts
    top = parts[0] if parts else ""
    if Path(relative_path).name in REGISTRY_EXCLUDED_FILENAMES or Path(relative_path).name.startswith("."):
        return {
            "artifact_domain": "derived_metadata",
            "classification_basis": "path_policy:system_metadata",
            "registry_eligible": False,
            "content_hash_policy": "metadata_only",
        }
    if any(parts[: len(prefix)] == prefix for prefix in REGISTRY_EXCLUDED_PREFIXES):
        return {
            "artifact_domain": "derived_metadata",
            "classification_basis": "path_policy:known_backup_or_runtime_acceptance",
            "registry_eligible": False,
            "content_hash_policy": "metadata_only",
        }
    if top == "state" and len(parts) >= 2 and parts[1] == "pm-loop":
        return {
            "artifact_domain": "runtime_evidence",
            "classification_basis": "path_policy:state/pm-loop",
            "registry_eligible": True,
            "content_hash_policy": "sha256",
        }
    if top in BUSINESS_ROOTS:
        return {
            "artifact_domain": "business",
            "classification_basis": f"path_policy:{top}",
            "registry_eligible": True,
            "content_hash_policy": "sha256",
        }
    if top in SENSITIVE_METADATA_ROOTS or top.startswith("."):
        return {
            "artifact_domain": "sensitive_metadata",
            "classification_basis": f"path_policy:{top or 'root_dotfile'}",
            "registry_eligible": False,
            "content_hash_policy": "metadata_only",
        }
    if top in DEPENDENCY_ROOTS:
        return {
            "artifact_domain": "dependency",
            "classification_basis": f"path_policy:{top}",
            "registry_eligible": False,
            "content_hash_policy": "metadata_only",
        }
    return {
        "artifact_domain": "unknown",
        "classification_basis": "path_policy:unclassified",
        "registry_eligible": False,
        "content_hash_policy": "metadata_only",
    }


def artifact_id(relative_path: str, content_hash: str) -> str:
    identity = sha256_bytes(canonical_json({"relative_path": relative_path, "content_hash": content_hash}))
    return f"artifact:sha256:{identity}"


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_manifest(path: Path) -> Dict[str, Any]:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                value = json.load(stream)
        else:
            value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    if not isinstance(value, dict):
        return {}
    snapshot_path = value.get("inventory_snapshot_path")
    if isinstance(snapshot_path, str) and snapshot_path:
        candidate = (path.parent / snapshot_path).resolve()
        try:
            candidate.relative_to(path.parent.resolve())
        except ValueError:
            return {}
        return read_manifest(candidate)
    return value


def _lstat_metadata(path: Path) -> Dict[str, Any]:
    """Return metadata without following symlinks."""
    stat = path.lstat()
    metadata: Dict[str, Any] = {
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "mode": int(stat.st_mode),
    }
    if path.is_symlink():
        try:
            metadata["link_target"] = os.readlink(path)
        except OSError as exc:
            metadata["link_target_error"] = str(exc)
    return metadata


def scan_project(
    root: Path,
    output_root: Path,
    previous: Mapping[str, Any],
    *,
    max_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    root = root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    scan_started_at = utc_now()
    started_monotonic = time.monotonic()
    budget_seconds = max(0.0, float(max_seconds)) if max_seconds is not None else None
    deadline_monotonic = started_monotonic + budget_seconds if budget_seconds is not None else None
    budget_exhausted = False
    artifacts: List[Dict[str, Any]] = []
    # Root inventory is the audit layer: every discovered path is represented,
    # including excluded subtree descendants.  Artifacts are the hashable
    # regular-file subset consumed by the future Registry.
    root_inventory: List[Dict[str, Any]] = []
    seen_paths: set[str] = set()
    excluded: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    previous_by_path = {
        str(item.get("relative_path")): item
        for item in (previous.get("artifacts") or [])
        if isinstance(item, dict) and item.get("relative_path")
    }

    class ScanBudgetExceeded(RuntimeError):
        pass

    def require_budget() -> None:
        nonlocal budget_exhausted
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            budget_exhausted = True
            raise ScanBudgetExceeded("scan_budget_exhausted")

    def add_root_entry(path: Path, kind: str, status: str, reason: Optional[str] = None, **extra: Any) -> None:
        rel = _relative(path, root)
        if rel in seen_paths:
            return
        seen_paths.add(rel)
        entry: Dict[str, Any] = {"path": rel, "kind": kind, "status": status}
        if reason:
            entry["exclusion_reason"] = reason
        try:
            entry.update(_lstat_metadata(path))
        except (OSError, PermissionError) as exc:
            entry["status"] = "unknown"
            entry["unknown_reason"] = "lstat_failed"
            entry["error"] = str(exc)
            errors.append({"path": rel, "stage": "lstat", "error": str(exc)})
        entry.update(extra)
        root_inventory.append(entry)

    add_root_entry(root, "directory", "observed")

    def scan_excluded_subtree(directory: Path, reason: str) -> None:
        """Record metadata for all descendants of an excluded directory.

        We do not hash these files (they are explicitly outside the Artifact
        Registry subset), but their paths, metadata, and reason are retained so
        an exclusion cannot silently hide files from Root Inventory.
        """
        for nested_str, nested_dirs, nested_files in os.walk(
            directory, topdown=True, followlinks=False, onerror=lambda exc: errors.append({"path": str(exc.filename or directory), "stage": "walk", "error": str(exc)})
        ):
            require_budget()
            nested = Path(nested_str)
            kept_nested: List[str] = []
            for name in sorted(nested_dirs):
                require_budget()
                child = nested / name
                try:
                    linked = child.is_symlink()
                except OSError as exc:
                    linked = False
                    errors.append({"path": _relative(child, root), "stage": "lstat", "error": str(exc)})
                if linked:
                    add_root_entry(child, "symlink", "excluded", "symlink_not_followed", parent_exclusion=reason)
                else:
                    add_root_entry(child, "directory", "excluded", reason, parent_exclusion=reason)
                    kept_nested.append(name)
            nested_dirs[:] = kept_nested
            for name in sorted(nested_files):
                require_budget()
                child = nested / name
                kind = "symlink" if child.is_symlink() else "file"
                add_root_entry(child, kind, "excluded", "symlink_not_followed" if kind == "symlink" else reason, parent_exclusion=reason)

    # os.walk does not follow symlinked directories.  We record every excluded
    # subtree descendant separately to make the root-level coverage auditable.
    try:
        for current_str, dirs, files in os.walk(
            root,
            topdown=True,
            followlinks=False,
            onerror=lambda exc: errors.append({"path": str(exc.filename or root), "stage": "walk", "error": str(exc)}),
        ):
            require_budget()
            current = Path(current_str)
            kept_dirs: List[str] = []
            for directory in sorted(dirs):
                require_budget()
                candidate = current / directory
                rel = _relative(candidate, root)
                reason = exclusion_reason(candidate, root, output_root)
                try:
                    is_link = candidate.is_symlink()
                except OSError as exc:
                    is_link = False
                    errors.append({"path": rel, "stage": "lstat", "error": str(exc)})
                if is_link:
                    add_root_entry(candidate, "symlink", "excluded", "symlink_not_followed")
                    excluded.append({"path": rel, "kind": "directory", "reason": "symlink_not_followed"})
                elif reason:
                    add_root_entry(candidate, "directory", "excluded", reason)
                    excluded.append({"path": rel, "kind": "directory", "reason": reason})
                    scan_excluded_subtree(candidate, reason)
                else:
                    add_root_entry(candidate, "directory", "observed")
                    kept_dirs.append(directory)
            dirs[:] = kept_dirs

            for filename in sorted(files):
                require_budget()
                path = current / filename
                rel = _relative(path, root)
                try:
                    if path.is_symlink():
                        add_root_entry(path, "symlink", "excluded", "symlink_not_followed")
                        excluded.append({"path": rel, "kind": "file", "reason": "symlink_not_followed"})
                        continue
                    stat = path.stat()
                    if not path.is_file():
                        add_root_entry(path, "special", "excluded", "not_regular_file")
                        excluded.append({"path": rel, "kind": "special", "reason": "not_regular_file"})
                        continue
                    classification = classify_path(rel)
                    if not classification["registry_eligible"]:
                        add_root_entry(
                            path,
                            "file",
                            "observed",
                            content_hash=None,
                            content_hash_state="not_collected_policy_metadata_only",
                            **classification,
                        )
                        continue
                    content_hash, bytes_read = file_sha256(path)
                    previous_item = previous_by_path.get(rel)
                    item: Dict[str, Any] = {
                        "artifact_id": artifact_id(rel, content_hash),
                        "relative_path": rel,
                        "source_path": str(path),
                        "content_hash": f"sha256:{content_hash}",
                        "hash_algorithm": HASH_ALGORITHM,
                        "size_bytes": bytes_read,
                        "mtime_ns": stat.st_mtime_ns,
                        "observed_at": scan_started_at,
                        "artifact_type": infer_artifact_type(rel)[0],
                        "artifact_type_basis": infer_artifact_type(rel)[1],
                        **classification,
                        "status": "observed",
                        "visibility": "local_private",
                        "retention_class": "R5_unknown",
                        "relation_state": "unknown",
                    }
                    if isinstance(previous_item, dict):
                        previous_hash = str(previous_item.get("content_hash") or "")
                        if previous_hash == item["content_hash"]:
                            item["first_seen_artifact_id"] = previous_item.get("first_seen_artifact_id") or previous_item.get("artifact_id")
                        elif previous_item.get("artifact_id"):
                            item["supersedes"] = previous_item["artifact_id"]
                    artifacts.append(item)
                    add_root_entry(
                        path,
                        "file",
                        "observed",
                        artifact_id=item["artifact_id"],
                        content_hash=item["content_hash"],
                        content_hash_state="collected",
                        **classification,
                    )
                except (OSError, PermissionError, UnicodeError) as exc:
                    errors.append({"path": rel, "stage": "hash", "error": str(exc)})
                    artifacts.append(
                        {
                            "artifact_id": None,
                            "relative_path": rel,
                            "source_path": str(path),
                            "content_hash": None,
                            "hash_algorithm": HASH_ALGORITHM,
                            "observed_at": scan_started_at,
                            "artifact_type": infer_artifact_type(rel)[0],
                            "artifact_type_basis": infer_artifact_type(rel)[1],
                            "status": "unknown",
                            "unknown_reason": "read_or_stat_failed",
                            "error": str(exc),
                            "visibility": "local_private",
                        }
                    )
                    add_root_entry(path, "file", "unknown", "read_or_stat_failed")
    except ScanBudgetExceeded:
        errors.append({"path": ".", "stage": "scan_budget", "error": "max_seconds_exceeded"})

    current_paths = {str(item.get("relative_path")) for item in artifacts}
    previous_unseen = [
        {"relative_path": path, "prior_artifact_id": item.get("artifact_id"), "conclusion": "unknown_not_observed"}
        for path, item in sorted(previous_by_path.items())
        if path not in current_paths
    ] if not budget_exhausted else []
    # This is intentionally a content digest, independent of observed_at and
    # mtime, making the same tree produce the same inventory_hash.
    identity_rows = [
        {
            "relative_path": item.get("relative_path"),
            "content_hash": item.get("content_hash"),
            "size_bytes": item.get("size_bytes"),
            "status": item.get("status"),
        }
        for item in artifacts
    ]
    identity_rows.sort(key=lambda item: str(item.get("relative_path")))
    # Inventory output is created after the root scan.  It is still listed in
    # Root Inventory on later scans, but excluded from snapshot identity so the
    # inventory does not create a new hash merely by recording itself.
    root_identity_rows = [
        {
            "path": item.get("path"),
            "kind": item.get("kind"),
            "status": item.get("status"),
            "exclusion_reason": item.get("exclusion_reason"),
            # Directory allocation/entry size is filesystem metadata, not
            # source content.  Excluding it prevents self-output churn from
            # changing a source snapshot through an ancestor directory.
            "size_bytes": item.get("size_bytes") if item.get("kind") == "file" else None,
            "content_hash": item.get("content_hash"),
            "link_target": item.get("link_target"),
        }
        for item in root_inventory
        if item.get("exclusion_reason") != "inventory_output"
    ]
    root_identity_rows.sort(key=lambda item: str(item.get("path")))
    identity_exclusions = [item for item in excluded if item.get("reason") != "inventory_output"]
    artifact_registry_hash = f"sha256:{sha256_bytes(canonical_json({'artifacts': identity_rows}))}"
    inventory_identity = {"root_inventory": root_identity_rows, "artifacts": identity_rows, "excluded": identity_exclusions, "errors": errors}
    inventory_hash = f"sha256:{sha256_bytes(canonical_json(inventory_identity))}"
    scan_finished_at = utc_now()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    complete = not errors and not budget_exhausted and len(seen_paths) == len(root_inventory)
    excluded_regular = [item for item in root_inventory if item.get("status") == "excluded" and item.get("kind") == "file"]
    observed_regular = [item for item in root_inventory if item.get("status") == "observed" and item.get("kind") == "file"]
    unknown_entries = [item for item in root_inventory if item.get("status") == "unknown"]
    return {
        "schema_version": SCHEMA,
        "inventory_hash": inventory_hash,
        "artifact_registry_hash": artifact_registry_hash,
        "observed_at": scan_started_at,
        "scan_started_at": scan_started_at,
        "scan_finished_at": scan_finished_at,
        "scan_duration_seconds": duration_seconds,
        "scan_status": "completed" if complete else "partial",
        "root": str(root),
        "read_only": True,
        "hash_algorithm": HASH_ALGORITHM,
        "scan_policy": {
            "regular_files_only": True,
            "follow_symlinks": False,
            "excluded_exact_directory_names": sorted(EXACT_EXCLUDED_DIR_NAMES),
            "excluded_directory_prefixes": list(EXCLUDED_DIR_PREFIXES),
            "excluded_output_directory": str(output_root),
            "max_seconds": budget_seconds,
            "normal_directories_included": ["docs", "state", "scripts", "web", "projects", "output", "outputs"],
            "registry_eligible_domains": ["business", "runtime_evidence"],
            "metadata_only_domains": ["dependency", "sensitive_metadata", "unknown"],
        },
        "completeness": {
            "inventory_complete": complete,
            "root_inventory_complete": complete,
            "artifact_inventory_complete": complete,
            "deletion_conclusion_allowed": False,
            "unknown_scope": "read_errors, budget-exhausted remainder and previous paths not observed are unknown; no deletion conclusion",
        },
        "summary": {
            "regular_file_count": sum(1 for item in artifacts if item.get("status") == "observed"),
            "unknown_file_count": sum(1 for item in artifacts if item.get("status") == "unknown"),
            "root_entry_count": len(root_inventory),
            "root_regular_file_count": len(observed_regular) + len(excluded_regular),
            "root_observed_regular_file_count": len(observed_regular),
            "root_excluded_regular_file_count": len(excluded_regular),
            "root_unknown_entry_count": len(unknown_entries),
            "root_logical_bytes": sum(int(item.get("size_bytes") or 0) for item in root_inventory if item.get("kind") == "file"),
            "artifact_registry_file_count": len(artifacts),
            "root_entries_by_domain": {
                domain: sum(1 for item in root_inventory if item.get("artifact_domain") == domain)
                for domain in sorted({str(item.get("artifact_domain")) for item in root_inventory if item.get("artifact_domain")})
            },
            "excluded_logical_bytes": sum(int(item.get("size_bytes") or 0) for item in excluded_regular),
            "root_excluded_by_reason": {
                reason: sum(1 for item in root_inventory if item.get("exclusion_reason") == reason)
                for reason in sorted({str(item.get("exclusion_reason")) for item in root_inventory if item.get("exclusion_reason")})
            },
            "excluded_entry_count": len(excluded),
            "excluded_directory_count": sum(1 for item in excluded if item.get("kind") == "directory"),
            "excluded_file_count": sum(1 for item in excluded if item.get("kind") == "file"),
            "error_count": len(errors),
            "budget_exhausted": budget_exhausted,
            "previous_unseen_count": len(previous_unseen),
            "logical_bytes": sum(int(item.get("size_bytes") or 0) for item in artifacts if item.get("status") == "observed"),
        },
        "artifacts": artifacts,
        "root_inventory": root_inventory,
        "excluded": excluded,
        "errors": errors,
        "previous_unseen": previous_unseen,
    }


def _atomic_gzip_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as raw:
            # mtime=0 makes the stored artifact reproducible for the same
            # inventory payload, rather than baking wall-clock time into gzip.
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                compressed.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"))
                compressed.write(b"\n")
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _pointer_manifest(value: Mapping[str, Any], snapshot_name: str, kind: str) -> Dict[str, Any]:
    return {
        "schema_version": "pm-loop.legacy-manifest.v1",
        "manifest_kind": kind,
        "inventory_hash": value["inventory_hash"],
        "root_snapshot_hash": value["inventory_hash"],
        "artifact_registry_hash": value["artifact_registry_hash"],
        "observed_at": value["observed_at"],
        "scan_started_at": value.get("scan_started_at"),
        "scan_finished_at": value.get("scan_finished_at"),
        "scan_duration_seconds": value.get("scan_duration_seconds"),
        "scan_status": value.get("scan_status"),
        "root": value["root"],
        "read_only": True,
        "hash_algorithm": HASH_ALGORITHM,
        "scan_policy": value["scan_policy"],
        "completeness": value["completeness"],
        "summary": value["summary"],
        "inventory_snapshot_path": snapshot_name,
        "exclusions_recorded_in_snapshot": True,
        "errors_recorded_in_snapshot": True,
    }


def write_outputs(value: Mapping[str, Any], output_root: Path) -> Dict[str, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    inventory_hash = str(value["inventory_hash"]).split(":", 1)[-1]
    run_path = output_root / "runs" / f"{inventory_hash}.json.gz"
    if not run_path.is_file():
        _atomic_gzip_json(run_path, value)
    latest_path = output_root / "latest.json"
    legacy_path = output_root / "legacy_manifest.json"
    attempt_path = output_root / "last-attempt.json"
    _atomic_json(attempt_path, _pointer_manifest(value, str(run_path.relative_to(output_root)), "last_attempt"))
    if value.get("completeness", {}).get("inventory_complete") is True:
        _atomic_json(latest_path, _pointer_manifest(value, str(run_path.relative_to(output_root)), "latest_pointer"))
        _atomic_json(legacy_path, _pointer_manifest(value, str(run_path.relative_to(output_root)), "legacy_manifest"))
    return {"run": run_path, "latest": latest_path, "legacy_manifest": legacy_path, "last_attempt": attempt_path}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="project root to scan")
    parser.add_argument("--output-dir", type=Path, default=None, help="inventory output directory")
    parser.add_argument("--max-seconds", type=float, default=None, help="stop after this budget and keep latest.json unchanged")
    args = parser.parse_args(argv)
    root = args.root.expanduser().resolve()
    output_root = (args.output_dir or (root / DEFAULT_OUTPUT_RELATIVE)).expanduser().resolve()
    # Materialize only the designated inventory-output boundary before taking
    # the snapshot.  Its descendants are explicitly excluded from identity,
    # so later pointer/run writes cannot perturb the source-tree hash.
    output_root.mkdir(parents=True, exist_ok=True)
    previous = read_manifest(output_root / "latest.json")
    value = scan_project(root, output_root, previous, max_seconds=args.max_seconds)
    paths = write_outputs(value, output_root)
    result = {
        "inventory_hash": value["inventory_hash"],
        "scan_status": value["scan_status"],
        "scan_duration_seconds": value["scan_duration_seconds"],
        "summary": value["summary"],
        "completeness": value["completeness"],
        "outputs": {key: str(path) for key, path in paths.items() if path.is_file()},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
