#!/usr/bin/env python3
"""Read-only inventory and plan producer for PM Loop retention."""

from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import subprocess
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from retention_registry import (
    ACTION_PROFILES,
    ADAPTER_BUNDLE_VERSION,
    DEFAULT_CAPABILITIES,
    DEFAULT_POLICY,
    DEFAULT_SOURCE_REGISTRY,
    DISCOVERY_BASES,
    RESOLVER_VERSION,
    RetentionBundle,
    RetentionConfigError,
    canonical_hash,
    canonical_json,
    load_bundle,
    matching_capability,
    normalize_relative_path,
    policy_for,
    resolve_source_path,
    root_identities,
    trusted_roots,
    worker_build_digest,
)


OBSERVER_SCHEMA = "pm-loop.retention-observer.v1"
INVENTORY_SCHEMA = "pm-loop.retention-inventory.v1"
PLAN_SCHEMA = "pm-loop.retention-plan.v4"
UNKNOWNS_SCHEMA = "pm-loop.retention-unknowns.v1"
SIGNER_KEY_ID = "pm-loop-retention-plan-v1"
KEYCHAIN_SERVICE = "pm-loop-retention-plan-v1"
CAPABILITY_KEYCHAIN_SERVICE = "pm-loop-retention-capability-v1"
CODE_IGNORES = frozenset({".DS_Store", "__pycache__", ".events.lock", ".observer.lock", "retention"})


def now_iso(value: Optional[datetime] = None) -> str:
    current = value or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def atomic_json_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _safe_run_id(value: Optional[str]) -> str:
    candidate = str(value or "").strip()
    if candidate and len(candidate) <= 120 and all(char.isalnum() or char in "-_." for char in candidate):
        return candidate
    return "ret-observe-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:10]


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _allocated_bytes(info: os.stat_result) -> int:
    return int(getattr(info, "st_blocks", 0) or 0) * 512


def _relative(path: Path, source_path: Path) -> str:
    if source_path.is_file():
        return source_path.name
    return path.relative_to(source_path).as_posix()


def _matches(name: str, patterns: Sequence[str]) -> bool:
    return any(pattern == "*" or fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def _matches_relative(relative: str, patterns: Sequence[str]) -> bool:
    return any(
        fnmatch.fnmatchcase(relative, pattern) or (pattern.endswith("/**") and relative == pattern[:-3])
        for pattern in patterns
    )


def _iter_objects(source_path: Path, *, max_depth: int) -> Iterable[Path]:
    if source_path.is_file():
        yield source_path
        return
    stack = [(source_path, 0)]
    while stack:
        parent, depth = stack.pop()
        try:
            children = sorted(parent.iterdir(), key=lambda item: item.name)
        except (OSError, PermissionError):
            raise
        for child in children:
            if child.name in CODE_IGNORES:
                continue
            info = child.lstat()
            if stat.S_ISLNK(info.st_mode):
                yield child
            elif stat.S_ISDIR(info.st_mode):
                if depth < max_depth:
                    stack.append((child, depth + 1))
            elif stat.S_ISREG(info.st_mode):
                yield child


def _object_fact(path: Path, source_path: Path, source: Mapping[str, Any]) -> Dict[str, Any]:
    info = path.lstat()
    relative_in_source = _relative(path, source_path)
    source_relative = PurePosixPath(str(source["root_ref"]["relative_path"]))
    object_relative = str(source_relative / PurePosixPath(relative_in_source)) if source_path.is_dir() else str(source_relative)
    identity = [source["source_id"], source["root_ref"]["root_id"], object_relative]
    object_id = "obj-" + hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()[:24]
    is_link = stat.S_ISLNK(info.st_mode)
    content_hash = None if is_link else _file_hash(path)
    return {
        "object_id": object_id,
        "source_id": source["source_id"],
        "adapter": source["adapter"],
        "object_contract": source["object_contract"],
        "authority": source["declared_authority"],
        "root_id": source["root_ref"]["root_id"],
        "relative_path": object_relative,
        "realpath_hash": canonical_hash([source["root_ref"]["root_id"], object_relative]),
        "inode_identity": {"st_dev": info.st_dev, "st_ino": info.st_ino, "file_type": "symlink" if is_link else "regular_file", "size": info.st_size, "mtime_ns": info.st_mtime_ns, "nlink": info.st_nlink},
        "logical_bytes": int(info.st_size),
        "allocated_bytes": _allocated_bytes(info),
        "content_hash": content_hash,
        "observed_at": now_iso(),
        "reference_state": "unknown",
        "reference_count": None,
        "hold_ids": [],
        "discovery_status": "registered",
        "processability": "observed",
        "reason_codes": [],
    }


def _identity_tuple(item: Mapping[str, Any]) -> Tuple[Any, ...]:
    identity = item["inode_identity"]
    return (item["relative_path"], identity["st_dev"], identity["st_ino"], identity["size"], identity["mtime_ns"], identity["nlink"], item.get("content_hash"))


def _metadata_tuple(path: Path, source_path: Path, source: Mapping[str, Any]) -> Tuple[Any, ...]:
    info = path.lstat()
    relative_in_source = _relative(path, source_path)
    source_relative = PurePosixPath(str(source["root_ref"]["relative_path"]))
    object_relative = str(source_relative / PurePosixPath(relative_in_source)) if source_path.is_dir() else str(source_relative)
    return (object_relative, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_nlink)


def _fact_metadata_tuple(item: Mapping[str, Any]) -> Tuple[Any, ...]:
    identity = item["inode_identity"]
    return (item["relative_path"], identity["st_dev"], identity["st_ino"], identity["size"], identity["mtime_ns"], identity["nlink"])


def _source_relative(item: Mapping[str, Any], source: Mapping[str, Any]) -> str:
    source_root = PurePosixPath(str(source["root_ref"]["relative_path"]))
    item_path = PurePosixPath(str(item["relative_path"]))
    try:
        return str(item_path.relative_to(source_root))
    except ValueError:
        return item_path.name


def _verified_runtime_snapshot(path: Path, snapshot_id: str) -> Optional[Dict[str, Any]]:
    """Return immutable evidence only for a committed, hash-closed snapshot."""
    manifest_path = path / "snapshot-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = manifest.get("files")
        if (
            manifest.get("schema_version") != "pm-loop.runtime-backup-manifest.v1"
            or manifest.get("snapshot_id") != snapshot_id
            or manifest.get("status") != "completed"
            or not isinstance(files, list)
        ):
            return None
        expected = {
            "config/schedule-registry.json",
            "scripts/pm_loop_scheduler.py",
            "scripts/pm_scheduled_handlers.py",
            "scripts/retention_observer.py",
        }
        seen = set()
        for row in files:
            if not isinstance(row, Mapping):
                return None
            relative = normalize_relative_path(row.get("relative_path"))
            digest = str(row.get("sha256") or "")
            candidate = path.joinpath(*PurePosixPath(relative).parts)
            if not candidate.is_file() or candidate.is_symlink() or _file_hash(candidate) != digest:
                return None
            seen.add(relative)
        if not expected.issubset(seen):
            return None
        completed_at = parse_iso(str(manifest.get("completed_at")))
        normalized_files = sorted(
            [{
                "relative_path": normalize_relative_path(row.get("relative_path")),
                "sha256": str(row.get("sha256") or ""),
                "bytes": int(row.get("bytes") or 0),
            }
            for row in files],
            key=lambda item: item["relative_path"],
        )
        return {
            "completed_at": completed_at,
            "manifest_sha256": _file_hash(manifest_path),
            "files_digest": canonical_hash(normalized_files),
            "file_count": len(normalized_files),
            "logical_bytes": sum(int(row["bytes"]) for row in normalized_files) + manifest_path.stat().st_size,
        }
    except (OSError, ValueError, json.JSONDecodeError, RetentionConfigError):
        return None


def _resolve_runtime_snapshots(
    source: Mapping[str, Any], source_path: Path, objects: Sequence[Dict[str, Any]], roots: Mapping[str, Path],
) -> None:
    """Close runtime backup references by committed snapshot, never by file age."""
    runtime = Path(roots["pm_loop"]) / "runtime"
    completed: Dict[str, Dict[str, Any]] = {}
    if runtime.is_dir():
        try:
            for child in source_path.iterdir():
                if child.is_dir() and not child.is_symlink():
                    evidence = _verified_runtime_snapshot(child, child.name)
                    if evidence:
                        completed[child.name] = evidence
        except OSError:
            completed = {}
    retained = {
        name
        for name, _ in sorted(
            completed.items(), key=lambda item: (item[1]["completed_at"], item[0]), reverse=True
        )[:3]
    }
    for item in objects:
        parts = PurePosixPath(_source_relative(item, source)).parts
        snapshot_id = parts[0] if parts else ""
        item["snapshot_id"] = snapshot_id or None
        evidence = completed.get(snapshot_id)
        if evidence:
            item["snapshot_completed_at"] = now_iso(evidence["completed_at"])
            item["snapshot_manifest_sha256"] = evidence["manifest_sha256"]
            item["snapshot_files_digest"] = evidence["files_digest"]
            item["snapshot_file_count"] = evidence["file_count"]
            item["snapshot_logical_bytes"] = evidence["logical_bytes"]
        if not runtime.is_dir():
            item["reference_state"] = "unknown"
            item["reference_count"] = None
            item["snapshot_state"] = "runtime_missing"
        elif snapshot_id in retained:
            item["reference_state"] = "active"
            item["reference_count"] = 1
            item["hold_ids"] = ["runtime-backup-retention-set"]
            item["snapshot_state"] = "retained_complete"
        elif snapshot_id in completed:
            item["reference_state"] = "closed"
            item["reference_count"] = 0
            item["hold_ids"] = ["runtime-backup-snapshot-action-required"]
            item["snapshot_state"] = "retired_complete"
        else:
            # Historical copies have no atomic completion proof. Keep them
            # protected; changing this to a name/mtime heuristic would reopen
            # the exact failure mode this index replaces.
            item["reference_state"] = "closed"
            item["reference_count"] = 0
            item["hold_ids"] = ["runtime-backup-snapshot-manifest-missing"]
            item["snapshot_state"] = "manifest_missing"


def _resolve_references(
    source: Mapping[str, Any], source_path: Path, objects: Sequence[Dict[str, Any]], roots: Mapping[str, Path], observed_at: datetime,
) -> None:
    providers = set(source.get("reference_providers", []))
    if "runtime-snapshot-index" in providers:
        _resolve_runtime_snapshots(source, source_path, objects, roots)
    if "log-retention-index" in providers:
        for item in objects:
            due_at = item.get("due_at")
            due = parse_iso(str(due_at)) if due_at else observed_at
            name = PurePosixPath(str(item.get("relative_path") or "")).name
            daily = re.fullmatch(r"daily-(\d{8})\.log", name)
            archived_monitor = re.fullmatch(r"monitor-\d{8}T\d{6}Z(?:-[0-9]+)?\.log", name)
            if not daily and not archived_monitor and name != "monitor.log":
                item["retention_class"] = "R4"
                item["policy_rule_id"] = "protect-unsupported-operational-log"
                item["proposed_action"] = "protect"
                item["processability"] = "protected"
                item["reason_codes"] = ["unsupported_log_terminally_protected"]
                item["reference_state"] = "closed"
                item["reference_count"] = 0
                item["hold_ids"] = ["unsupported-log-contract-terminal"]
                item["sealed"] = False
                continue
            local_today = observed_at.astimezone(timezone(timedelta(hours=8))).strftime("%Y%m%d")
            sealed = bool(archived_monitor or (daily and daily.group(1) < local_today))
            item["sealed"] = sealed
            if sealed and observed_at >= due:
                item["reference_state"] = "closed"
                item["reference_count"] = 0
            else:
                item["reference_state"] = "active"
                item["reference_count"] = 1
                item["hold_ids"] = ["operational-log-hot-window" if sealed else "operational-log-active-writer"]


def _scan_source(
    source: Mapping[str, Any], source_path: Path, bundle: RetentionBundle, observed_at: datetime, roots: Mapping[str, Path],
) -> Tuple[Dict[str, Any], list[Dict[str, Any]], list[Dict[str, Any]]]:
    status = "healthy"
    reason_codes: list[str] = []
    objects: list[Dict[str, Any]] = []
    unknowns: list[Dict[str, Any]] = []
    ignored_count = ignored_bytes = 0
    if not source_path.exists():
        return ({"source_id": source["source_id"], "display_name": source["display_name"], "status": "path_missing", "mode": source["mode"], "inventory_complete": False, "deletion_conclusion_allowed": False, "object_count": None, "logical_bytes": None, "allocated_bytes": None, "freshness": "unknown", "reason_codes": ["path_missing"]}, objects, unknowns)
    try:
        candidates = list(_iter_objects(source_path, max_depth=int(source["discovery"]["max_depth"])))
        first_pass = []
        for path in candidates:
            relative = _relative(path, source_path)
            if _matches_relative(relative, source["discovery"].get("ignore_relative_paths", [])):
                ignored_count += 1
                ignored_bytes += int(path.lstat().st_size)
                continue
            fact = _object_fact(path, source_path, source)
            name = path.name
            excluded = _matches(name, source["discovery"]["exclude_names"])
            included = _matches(name, source["discovery"]["include_names"])
            if fact["inode_identity"]["file_type"] == "symlink" or fact["inode_identity"]["nlink"] != 1:
                fact["processability"] = "held"
                fact["reason_codes"] = ["path_policy_violation"]
                status = "partial"
                reason_codes.append("path_policy_violation")
            elif excluded or not included:
                fact["processability"] = "held"
                fact["reason_codes"] = ["excluded_object"]
                unknowns.append(_unknown_from_object(fact, source, "excluded_object", severity="P2"))
            first_pass.append(fact)
        # A second metadata pass detects writers without reading large files a
        # second time. The first-pass hash remains bound to stable identity,
        # size and mtime. It must use the same source-local ignore rules as
        # the first pass: ignored children can belong to a nested source and
        # are not evidence that this parent source changed mid-inventory.
        second = []
        for path in candidates:
            relative = _relative(path, source_path)
            if _matches_relative(relative, source["discovery"].get("ignore_relative_paths", [])):
                continue
            if path.exists() or path.is_symlink():
                second.append(_metadata_tuple(path, source_path, source))
        stable = sorted(_fact_metadata_tuple(item) for item in first_pass) == sorted(second)
        if not stable:
            status = "partial"
            reason_codes.append("inventory_partial")
        objects = first_pass
    except PermissionError:
        status, reason_codes = "permission_denied", ["inventory_partial"]
    except OSError:
        status, reason_codes = "partial", ["inventory_partial"]

    newest_ns = max((item["inode_identity"]["mtime_ns"] for item in objects), default=None)
    newest = datetime.fromtimestamp(newest_ns / 1_000_000_000, tz=timezone.utc) if newest_ns else None
    freshness = "unknown" if newest is None else "fresh" if observed_at - newest <= timedelta(hours=int(source["freshness_sla_hours"])) else "stale"
    if freshness == "stale" and status == "healthy":
        status = "stale"
        reason_codes.append("source_stale")
    for item in objects:
        rule = policy_for(item, bundle)
        item.update({"retention_class": rule["class"], "policy_rule_id": rule["rule_id"], "proposed_action": rule["action"]})
        hot_days = int(rule.get("hot_days") or 0)
        due = datetime.fromtimestamp(item["inode_identity"]["mtime_ns"] / 1_000_000_000, tz=timezone.utc) + timedelta(days=hot_days)
        item["due_at"] = now_iso(due) if rule["class"] in {"R1", "R2", "R3"} else None
    _resolve_references(source, source_path, objects, roots, observed_at)
    for item in objects:
        rule = policy_for(item, bundle)
        if item["reason_codes"]:
            continue
        if rule["class"] in {"R0", "R4"}:
            item["processability"] = "protected"
        elif rule["class"] == "R5":
            item["processability"] = "needs_decision"
            item["reason_codes"] = ["unclassified_object"]
        elif item.get("reference_state") == "closed" and not item.get("hold_ids"):
            item["processability"] = "eligible"
        elif item.get("reference_state") == "unknown":
            item["processability"] = "held"
            item["reason_codes"] = ["reference_graph_incomplete"]
        elif item.get("snapshot_state") == "manifest_missing":
            # Three-agent majority decision: historical snapshots without an
            # atomic completion manifest are terminally protected. They are
            # visible inventory, not unresolved deletion candidates.
            item["retention_class"] = "R4"
            item["policy_rule_id"] = "protect-legacy-runtime-snapshot"
            item["proposed_action"] = "hold"
            item["processability"] = "protected"
            item["reason_codes"] = ["legacy_snapshot_protected"]
        elif item.get("snapshot_state") == "retired_complete":
            item["processability"] = "managed_by_snapshot_group"
            item["reason_codes"] = ["snapshot_group_member"]
        else:
            item["processability"] = "held"
            item["reason_codes"] = ["active_reference_or_lease"]
    unclassified = [item for item in objects if "unclassified_object" in item["reason_codes"]]
    if unclassified:
        unknowns.append(_aggregate_unknown(source, unclassified, "unclassified_object", "P2"))
    if status in {"partial", "permission_denied"}:
        unknowns.append(_aggregate_unknown(source, objects, "inventory_partial", "P1"))
    if status == "stale":
        unknowns.append(_aggregate_unknown(source, objects, "source_stale", "P1"))
    return ({
        "source_id": source["source_id"], "display_name": _safe_label(source["display_name"]), "status": status,
        "mode": source["mode"], "adapter": source["adapter"], "object_contract": source["object_contract"],
        "freshness": freshness, "freshness_sla_hours": source["freshness_sla_hours"],
        "inventory_complete": status in {"healthy", "stale"}, "deletion_conclusion_allowed": status == "healthy",
        "object_count": len(objects), "logical_bytes": sum(item["logical_bytes"] for item in objects),
        "allocated_bytes": sum(item["allocated_bytes"] for item in objects), "newest_object_at": now_iso(newest) if newest else None,
        "ignored_system_noise_count": ignored_count, "ignored_system_noise_bytes": ignored_bytes,
        "reason_codes": sorted(set(reason_codes)),
    }, objects, unknowns)


def _safe_label(value: Any) -> str:
    return "".join(char for char in str(value or "") if ord(char) >= 32)[:80] or "未命名来源"


def _unknown_from_object(item: Mapping[str, Any], source: Mapping[str, Any], reason: str, *, severity: str) -> Dict[str, Any]:
    return {
        "unknown_id": "unk-" + hashlib.sha256(canonical_json([item["object_id"], reason]).encode("utf-8")).hexdigest()[:20],
        "object_id": item["object_id"], "source_id": source["source_id"], "source_label": _safe_label(source["display_name"]),
        "reason_code": reason, "severity": severity, "logical_bytes": item["logical_bytes"], "growth_7d_bytes": None,
        "first_seen_at": item["observed_at"], "last_seen_at": item["observed_at"], "reference_state": item.get("reference_state", "unknown"),
        "status": "needs_decision", "evidence_handles": [f"retention://object/{item['object_id']}"]
    }


def _aggregate_unknown(source: Mapping[str, Any], items: Sequence[Mapping[str, Any]], reason: str, severity: str) -> Dict[str, Any]:
    object_ids = sorted(str(item.get("object_id")) for item in items)[:20]
    identifier = "unk-" + hashlib.sha256(canonical_json([source["source_id"], reason]).encode("utf-8")).hexdigest()[:20]
    return {
        "unknown_id": identifier, "object_id": identifier, "source_id": source["source_id"], "source_label": _safe_label(source["display_name"]),
        "reason_code": reason, "severity": severity, "object_count": len(items),
        "logical_bytes": sum(int(item.get("logical_bytes") or 0) for item in items), "growth_7d_bytes": None,
        "first_seen_at": now_iso(), "last_seen_at": now_iso(), "reference_state": "unknown", "status": "needs_decision",
        "evidence_handles": [f"retention://unknown/{identifier}"] + [f"retention://object/{value}" for value in object_ids[:3]],
    }


def _frontier_dispositions(roots: Mapping[str, Path], sources: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    registered = [(str(source["root_ref"]["root_id"]), str(source["root_ref"]["relative_path"])) for source in sources]
    result = []
    for root_id, bases in DISCOVERY_BASES.items():
        root = roots[root_id]
        for base_text in bases:
            base = root.joinpath(*PurePosixPath(base_text).parts)
            if not base.is_dir():
                continue
            if any(known_root == root_id and (known == base_text or PurePosixPath(known) in PurePosixPath(base_text).parents) for known_root, known in registered):
                continue
            try:
                children = list(base.iterdir())
            except OSError:
                continue
            for child in children:
                if child.name in CODE_IGNORES:
                    continue
                child_relative = str(PurePosixPath(base_text) / child.name)
                if any(known_root == root_id and (known == child_relative or PurePosixPath(child_relative) in PurePosixPath(known).parents) for known_root, known in registered):
                    continue
                logical, allocated, count = _subtree_size(child)
                identifier = "unk-" + hashlib.sha256(canonical_json([root_id, child_relative, "unregistered_source"]).encode("utf-8")).hexdigest()[:20]
                result.append({
                    "disposition_id": identifier,
                    "root_id": root_id,
                    "relative_path": child_relative,
                    "path_identity_hash": canonical_hash([root_id, child_relative]),
                    "category": "unregistered_protected",
                    "retention_class": "R4",
                    "action": "protect",
                    "reason_code": "unregistered_source_terminally_protected",
                    "object_count": count,
                    "logical_bytes": logical,
                    "allocated_bytes": allocated,
                    "status": "resolved",
                    "evidence_handles": [f"retention://disposition/{identifier}"],
                })
    return result


def _runtime_snapshot_plan_items(
    inventory: Sequence[Mapping[str, Any]], roots: Mapping[str, Path], bundle: RetentionBundle, current: datetime,
) -> list[Dict[str, Any]]:
    """Create one manifest-bound action per retired snapshot, never per file."""
    grouped: Dict[str, list[Mapping[str, Any]]] = {}
    for item in inventory:
        if item.get("source_id") != "scheduler-runtime-backups" or item.get("snapshot_state") != "retired_complete":
            continue
        snapshot_id = str(item.get("snapshot_id") or "")
        if snapshot_id:
            grouped.setdefault(snapshot_id, []).append(item)
    result: list[Dict[str, Any]] = []
    root = Path(roots["pm_loop"])
    for snapshot_id, members in sorted(grouped.items()):
        first = members[0]
        completed_at = parse_iso(str(first.get("snapshot_completed_at")))
        rule = policy_for(first, bundle)
        due_at = completed_at + timedelta(days=int(rule.get("hot_days") or 0))
        snapshot_relative = str(PurePosixPath("scheduler-migration/runtime-backups") / snapshot_id)
        snapshot_path = root.joinpath(*PurePosixPath(snapshot_relative).parts)
        info = snapshot_path.lstat()
        result.append({
            "object_id": "snapshot-" + hashlib.sha256(snapshot_relative.encode("utf-8")).hexdigest()[:24],
            "object_kind": "runtime_snapshot",
            "source_id": "scheduler-runtime-backups",
            "adapter": "filesystem_tree",
            "object_contract": "rebuildable-runtime-backup.v1",
            "authority": "rebuildable_cache",
            "root_id": "pm_loop",
            "relative_path": snapshot_relative,
            "realpath_hash": canonical_hash(["pm_loop", snapshot_relative]),
            "inode_identity": {
                "st_dev": info.st_dev,
                "st_ino": info.st_ino,
                "file_type": "directory",
                "size": info.st_size,
                "mtime_ns": info.st_mtime_ns,
                "nlink": info.st_nlink,
            },
            "logical_bytes": int(first.get("snapshot_logical_bytes") or 0),
            "allocated_bytes": sum(int(item.get("allocated_bytes") or 0) for item in members),
            "content_hash": first.get("snapshot_manifest_sha256"),
            "snapshot_id": snapshot_id,
            "snapshot_completed_at": first.get("snapshot_completed_at"),
            "snapshot_manifest_sha256": first.get("snapshot_manifest_sha256"),
            "snapshot_files_digest": first.get("snapshot_files_digest"),
            "snapshot_file_count": first.get("snapshot_file_count"),
            "reference_state": "closed",
            "reference_count": 0,
            "retention_class": rule["class"],
            "policy_rule_id": rule["rule_id"],
            "proposed_action": rule["action"],
            "processability": "eligible" if current >= due_at else "observed",
            "reason_codes": [] if current >= due_at else ["retention_hot_window"],
            "due_at": now_iso(due_at),
            "quarantine_days": int(rule.get("quarantine_days") or 0),
        })
    return result


def _subtree_size(path: Path) -> Tuple[int, int, int]:
    logical = allocated = count = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode):
                count += 1
            elif stat.S_ISDIR(info.st_mode):
                stack.extend(current.iterdir())
            elif stat.S_ISREG(info.st_mode):
                logical += info.st_size
                allocated += _allocated_bytes(info)
                count += 1
        except OSError:
            continue
    return logical, allocated, count


def _load_keychain_secret(service: str) -> Optional[bytes]:
    try:
        result = subprocess.run(
            ["/usr/bin/security", "find-generic-password", "-a", os.environ.get("USER", "zhujie14"), "-s", service, "-w"],
            capture_output=True, text=True, timeout=10, check=False,
            env={"PATH": "/usr/bin:/bin", "HOME": str(Path.home())},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = result.stdout.strip()
    return value.encode("utf-8") if result.returncode == 0 and len(value) >= 32 else None


def _load_signing_key() -> Optional[bytes]:
    return _load_keychain_secret(KEYCHAIN_SERVICE)


def sign_plan(plan: Mapping[str, Any], key: bytes) -> str:
    unsigned = dict(plan)
    unsigned.pop("signature", None)
    digest = hmac.new(key, canonical_json(unsigned).encode("utf-8"), hashlib.sha256).digest()
    return "base64:" + base64.b64encode(digest).decode("ascii")


def build_observation(
    *,
    bundle: RetentionBundle,
    roots: Mapping[str, Path],
    run_id: str,
    occurrence_id: str,
    signing_key: Optional[bytes] = None,
    capability_key: Optional[bytes] = None,
    schedule_registry_hash: Optional[str] = None,
    observed_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = observed_at or datetime.now(timezone.utc)
    identities = root_identities(roots)
    source_rows, inventory, unknowns = [], [], []
    for source in bundle.sources:
        try:
            path = resolve_source_path(source, roots)
            source_row, objects, blocked = _scan_source(source, path, bundle, current, roots)
        except RetentionConfigError:
            source_row, objects, blocked = ({"source_id": source["source_id"], "display_name": _safe_label(source["display_name"]), "status": "registry_invalid", "mode": source["mode"], "inventory_complete": False, "deletion_conclusion_allowed": False, "object_count": None, "logical_bytes": None, "allocated_bytes": None, "freshness": "unknown", "reason_codes": ["path_policy_violation"]}, [], [])
        source_rows.append(source_row)
        inventory.extend(objects)
        unknowns.extend(blocked)
    source_health = {str(row["source_id"]): row.get("status") == "healthy" for row in source_rows}
    dispositions = _frontier_dispositions(roots, bundle.sources)
    inventory.sort(key=lambda item: item["object_id"])
    unknowns.sort(key=lambda item: (item["severity"], item["reason_code"], item["unknown_id"]))
    snapshot_token = canonical_hash({"roots": identities, "objects": [_identity_tuple(item) for item in inventory], "observed_at": now_iso(current)})
    reference_hash = canonical_hash({"snapshot_token": snapshot_token, "items": [[item["object_id"], item["reference_state"], item["reference_count"]] for item in inventory]})
    inventory_hash = canonical_hash(inventory)
    worker_digest = worker_build_digest([Path(__file__), Path(__file__).with_name("retention_registry.py")])
    plans = []
    plan_candidates = list(inventory) + _runtime_snapshot_plan_items(inventory, roots, bundle, current)
    for item in plan_candidates:
        if (
            item["retention_class"] not in {"R1", "R2", "R3"}
            or item["processability"] != "eligible"
            or not source_health.get(str(item.get("source_id") or ""), False)
        ):
            continue
        if item.get("object_kind") == "runtime_snapshot":
            profile = "expire-runtime-snapshot-v1"
        else:
            profile = "repack-compressed-pair-v1" if item["proposed_action"] == "repack" else "expire-file-v1"
        capability = matching_capability(item, profile, bundle, signing_key=capability_key, current=current)
        if not capability:
            continue
        identity = item["inode_identity"]
        plans.append({
            "object_id": item["object_id"], "source_id": item["source_id"], "root_id": item["root_id"],
            "relative_path": item["relative_path"], "object_contract": item["object_contract"], "action_profile": profile,
            "st_dev": identity["st_dev"], "st_ino": identity["st_ino"], "file_type": identity["file_type"], "size": identity["size"],
            "mtime_ns": identity["mtime_ns"], "nlink": identity["nlink"], "content_hash": item["content_hash"],
            "expected_reclaim_bytes": item["allocated_bytes"],
            "expected_reclaim_logical_bytes": item["logical_bytes"],
            "expected_reclaim_allocated_bytes": item["allocated_bytes"],
            "due_at": item["due_at"],
            "object_kind": item.get("object_kind", "file"),
            "snapshot_id": item.get("snapshot_id"),
            "snapshot_manifest_sha256": item.get("snapshot_manifest_sha256"),
            "snapshot_files_digest": item.get("snapshot_files_digest"),
            "snapshot_file_count": item.get("snapshot_file_count"),
            "quarantine_days": int(item.get("quarantine_days") or 0),
            "gate_results": {
                "capability": capability["capability_id"], "rollout_phase": capability["rollout_phase"],
                "max_objects_per_batch": capability["max_objects_per_batch"], "max_bytes_per_day": capability["max_bytes_per_day"],
            },
        })
    issued = current.astimezone(timezone.utc)
    plan = {
        "schema": PLAN_SCHEMA, "plan_id": "ret-plan-" + uuid.uuid4().hex, "observer_occurrence_id": occurrence_id or f"manual:{run_id}",
        "issued_at": now_iso(issued), "not_before": now_iso(issued), "expires_at": now_iso(issued + timedelta(hours=36)),
        "nonce": secrets.token_urlsafe(24), "signer_key_id": SIGNER_KEY_ID, "source_registry_hash": bundle.source_registry_hash,
        "policy_hash": bundle.policy_hash, "deletion_capability_hash": bundle.deletion_capability_hash, "inventory_hash": inventory_hash,
        "reference_graph_hash": reference_hash, "snapshot_token": snapshot_token, "worker_build_digest": worker_digest,
        "adapter_bundle_digest": canonical_hash([ADAPTER_BUNDLE_VERSION, sorted(ACTION_PROFILES)]), "resolver_version": RESOLVER_VERSION,
        "schedule_registry_hash": str(schedule_registry_hash or "manual"),
        "root_identities": identities, "items": plans, "signature": None,
    }
    key = signing_key or _load_signing_key()
    signature_status = "signed" if key else "key_unavailable"
    if key:
        plan["signature"] = sign_plan(plan, key)
    source_statuses = {row["status"] for row in source_rows}
    result_status = "observed" if source_statuses <= {"healthy", "stale"} and key else "partial"
    return {
        "schema_version": OBSERVER_SCHEMA, "run_id": run_id, "occurrence_id": occurrence_id or None, "status": result_status,
        "observed_at": now_iso(current), "mode": bundle.global_mode, "signature_status": signature_status,
        "source_registry_hash": bundle.source_registry_hash, "policy_hash": bundle.policy_hash,
        "deletion_capability_hash": bundle.deletion_capability_hash, "snapshot_token": snapshot_token,
        "inventory": {"schema_version": INVENTORY_SCHEMA, "snapshot_token": snapshot_token, "observed_at": now_iso(current), "inventory_hash": inventory_hash, "sources": source_rows, "items": inventory},
        "unknowns": {
            "schema_version": UNKNOWNS_SCHEMA,
            "snapshot_token": snapshot_token,
            "observed_at": now_iso(current),
            "items": unknowns,
            "terminal_dispositions": dispositions,
        },
        "plan": plan,
        "summary": {
            "source_count": len(source_rows), "healthy_source_count": sum(1 for row in source_rows if row["status"] == "healthy"),
            "partial_source_count": sum(1 for row in source_rows if row["status"] not in {"healthy"}), "managed_object_count": len(inventory),
            "managed_logical_bytes": sum(item["logical_bytes"] for item in inventory), "managed_allocated_bytes": sum(item["allocated_bytes"] for item in inventory),
            "unknown_count": len(unknowns), "unknown_logical_bytes": sum(int(item.get("logical_bytes") or 0) for item in unknowns),
            "terminal_disposition_count": len(dispositions),
            "terminal_disposition_logical_bytes": sum(int(item.get("logical_bytes") or 0) for item in dispositions),
            "planned_object_count": len(plans), "planned_allocated_bytes": sum(int(item["expected_reclaim_bytes"]) for item in plans),
        },
    }


def _merge_unknown_history(state_root: Path, value: Dict[str, Any]) -> None:
    pointer_path = Path(state_root).expanduser().resolve() / "latest-observer.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        result_path = pointer_path.parent / normalize_relative_path(pointer["result"])
        prior_result = json.loads(result_path.read_text(encoding="utf-8"))
        unknown_path = pointer_path.parent / normalize_relative_path(prior_result["artifacts"]["unknowns"])
        prior_doc = json.loads(unknown_path.read_text(encoding="utf-8"))
    except (OSError, KeyError, ValueError, json.JSONDecodeError, RetentionConfigError):
        return
    prior = {str(item.get("unknown_id")): item for item in prior_doc.get("items", []) if isinstance(item, Mapping)}
    current_at = parse_iso(str(value["observed_at"]))
    for item in value["unknowns"].get("items", []):
        previous = prior.get(str(item.get("unknown_id")))
        if not isinstance(previous, Mapping):
            continue
        item["first_seen_at"] = previous.get("first_seen_at") or item.get("first_seen_at")
        item["last_seen_at"] = value["observed_at"]
        try:
            prior_at = parse_iso(str(previous.get("last_seen_at") or previous.get("first_seen_at")))
            elapsed_days = (current_at - prior_at).total_seconds() / 86400
            if 5 <= elapsed_days <= 9 and item.get("logical_bytes") is not None and previous.get("logical_bytes") is not None:
                item["growth_7d_bytes"] = int(item["logical_bytes"]) - int(previous["logical_bytes"])
        except (TypeError, ValueError):
            pass


def write_observation(state_root: Path, value: Mapping[str, Any], *, db_path: Optional[Path] = None) -> Dict[str, Any]:
    root = Path(state_root).expanduser().resolve()
    run_id = str(value["run_id"])
    run_root = root / "observer" / run_id
    if run_root.exists():
        raise FileExistsError(f"immutable observer artifact already exists: {run_id}")
    run_root.mkdir(parents=True, exist_ok=False)
    atomic_json_write(run_root / "inventory.json", value["inventory"])
    atomic_json_write(run_root / "unknowns.json", value["unknowns"])
    atomic_json_write(run_root / "plan.json", value["plan"])
    result = {key: child for key, child in value.items() if key not in {"inventory", "unknowns", "plan"}}
    result["artifacts"] = {"inventory": f"observer/{run_id}/inventory.json", "unknowns": f"observer/{run_id}/unknowns.json", "plan": f"observer/{run_id}/plan.json", "result": f"observer/{run_id}/result.json"}
    result["artifact_digest"] = canonical_hash({"inventory": value["inventory"], "unknowns": value["unknowns"], "plan": value["plan"]})
    atomic_json_write(run_root / "result.json", result)
    if db_path is not None:
        from pm_system_store import PMSystemStore

        PMSystemStore(Path(db_path).expanduser().resolve()).record_retention_observation(value, artifact_digest=result["artifact_digest"])
    pointer = {"schema_version": "pm-loop.retention-latest.v1", "kind": "observer", "run_id": run_id, "status": result["status"], "observed_at": result["observed_at"], "result": result["artifacts"]["result"], "artifact_digest": result["artifact_digest"]}
    atomic_json_write(root / "latest-observer.json", pointer)
    return result


def run_observer(
    *, state_root: Path, registry_path: Path = DEFAULT_SOURCE_REGISTRY, policy_path: Path = DEFAULT_POLICY,
    capabilities_path: Path = DEFAULT_CAPABILITIES, project_root: Optional[Path] = None, home: Optional[Path] = None,
    run_id: Optional[str] = None, occurrence_id: Optional[str] = None, signing_key: Optional[bytes] = None,
    capability_key: Optional[bytes] = None, db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    bundle = load_bundle(registry_path, policy_path, capabilities_path)
    resolved_run_id = _safe_run_id(run_id or os.environ.get("PM_SCHEDULE_RUN_ID"))
    value = build_observation(
        bundle=bundle, roots=trusted_roots(project_root=project_root, home=home), run_id=resolved_run_id,
        occurrence_id=str(occurrence_id or os.environ.get("PM_SCHEDULED_OCCURRENCE_ID") or ""), signing_key=signing_key,
        capability_key=capability_key or _load_keychain_secret(CAPABILITY_KEYCHAIN_SERVICE),
        schedule_registry_hash=os.environ.get("PM_SCHEDULE_REGISTRY_HASH") or "manual",
    )
    _merge_unknown_history(state_root, value)
    effective_db = db_path or (Path(os.environ["PM_SCHEDULE_DB_PATH"]) if os.environ.get("PM_SCHEDULE_DB_PATH") else None)
    return write_observation(state_root, value, db_path=effective_db)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, default=Path.home() / ".codex" / "pm-loop" / "state" / "retention")
    parser.add_argument("--registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--capabilities", type=Path, default=DEFAULT_CAPABILITIES)
    parser.add_argument("--project-root", type=Path, default=Path.home() / "Documents" / "project")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = run_observer(state_root=args.state_root, registry_path=args.registry, policy_path=args.policy, capabilities_path=args.capabilities, project_root=args.project_root, run_id=args.run_id, db_path=args.db_path)
    except (RetentionConfigError, OSError, ValueError) as exc:
        print(json.dumps({"schema_version": OBSERVER_SCHEMA, "status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in {"observed", "partial"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
