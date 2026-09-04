#!/usr/bin/env python3
"""Fail-closed executor for signed PM Loop retention plans.

The v4 rollout is intentionally disabled until a separately signed deletion
capability exists.  This module still validates the complete plan envelope and
emits immutable dry-run/disabled evidence through the PM Worker.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import uuid
from datetime import datetime, time, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

from retention_observer import CAPABILITY_KEYCHAIN_SERVICE, KEYCHAIN_SERVICE, PLAN_SCHEMA, _load_keychain_secret, _load_signing_key, now_iso, sign_plan
from retention_registry import (
    ACTION_PROFILES,
    ADAPTER_BUNDLE_VERSION,
    DEFAULT_CAPABILITIES,
    DEFAULT_POLICY,
    DEFAULT_SOURCE_REGISTRY,
    RESOLVER_VERSION,
    RetentionBundle,
    RetentionConfigError,
    canonical_hash,
    load_bundle,
    matching_capability,
    normalize_relative_path,
    root_identities,
    trusted_roots,
    worker_build_digest,
)


RECLAIMER_SCHEMA = "pm-loop.retention-reclaimer.v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")


class PlanValidationError(ValueError):
    pass


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
    return "ret-reclaim-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:10]


def _safe_artifact_path(state_root: Path, relative: Any) -> Path:
    text = normalize_relative_path(relative)
    root = Path(state_root).expanduser().resolve()
    path = root.joinpath(*PurePosixPath(text).parts)
    try:
        path.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise PlanValidationError("artifact path escapes retention state root") from exc
    if path.is_symlink():
        raise PlanValidationError("artifact symlink is not allowed")
    return path


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlanValidationError(f"cannot read immutable retention artifact: {exc}") from exc
    if not isinstance(value, dict):
        raise PlanValidationError("retention artifact must be an object")
    return value


def _parse_iso(value: Any) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise PlanValidationError("invalid plan timestamp") from exc


def _business_window_open(current: datetime) -> bool:
    local = current.astimezone(SHANGHAI).timetz().replace(tzinfo=None)
    return time(10, 0) <= local <= time(16, 30)


def _hash_fd(file_descriptor: int) -> str:
    digest = hashlib.sha256()
    with os.fdopen(os.dup(file_descriptor), "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def verify_plan_item_descriptor(item: Mapping[str, Any], roots: Mapping[str, Path]) -> Dict[str, Any]:
    """Re-open one exact object below a trusted root without following links."""
    root_id = str(item.get("root_id") or "")
    if root_id not in roots:
        raise PlanValidationError("retention plan references an unknown root")
    relative = normalize_relative_path(item.get("relative_path"))
    parts = PurePosixPath(relative).parts
    if not parts:
        raise PlanValidationError("retention object path is empty")
    flags_directory = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags_file = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    root_fd = os.open(str(Path(roots[root_id]).expanduser().resolve()), flags_directory)
    opened = [root_fd]
    try:
        root_info = os.fstat(root_fd)
        parent_fd = root_fd
        for part in parts[:-1]:
            child_fd = os.open(part, flags_directory, dir_fd=parent_fd)
            opened.append(child_fd)
            info = os.fstat(child_fd)
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != root_info.st_dev:
                raise PlanValidationError("retention path crosses an unsafe directory boundary")
            parent_fd = child_fd
        file_fd = os.open(parts[-1], flags_file, dir_fd=parent_fd)
        opened.append(file_fd)
        info = os.fstat(file_fd)
        expected = {
            "st_dev": int(item.get("st_dev") or -1), "st_ino": int(item.get("st_ino") or -1),
            "size": int(item.get("size") or -1), "mtime_ns": int(item.get("mtime_ns") or -1),
            "nlink": int(item.get("nlink") or -1),
        }
        actual = {"st_dev": info.st_dev, "st_ino": info.st_ino, "size": info.st_size, "mtime_ns": info.st_mtime_ns, "nlink": info.st_nlink}
        if not stat.S_ISREG(info.st_mode) or actual != expected or info.st_dev != root_info.st_dev or info.st_nlink != 1:
            raise PlanValidationError("retention object identity changed")
        content_hash = _hash_fd(file_fd)
        if content_hash != item.get("content_hash"):
            raise PlanValidationError("retention object content hash changed")
        stats = os.statvfs(str(Path(roots[root_id]).expanduser().resolve()))
        available = int(stats.f_bavail) * int(stats.f_frsize)
        if available < 2 * 1024 * 1024 * 1024:
            raise PlanValidationError("retention temporary-space headroom is below 2 GiB")
        return {
            "status": "descriptor_verified", "object_id": item.get("object_id"), "content_hash": content_hash,
            "allocated_bytes": int(getattr(info, "st_blocks", 0) or 0) * 512, "available_bytes": available,
        }
    except OSError as exc:
        raise PlanValidationError(f"retention descriptor verification failed: {exc}") from exc
    finally:
        for descriptor in reversed(opened):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _target_path(item: Mapping[str, Any], roots: Mapping[str, Path]) -> Path:
    root_id = str(item.get("root_id") or "")
    if root_id not in roots:
        raise PlanValidationError("retention action references an unknown root")
    relative = normalize_relative_path(item.get("relative_path"))
    root = Path(roots[root_id]).expanduser().resolve()
    target = root.joinpath(*PurePosixPath(relative).parts)
    try:
        target.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise PlanValidationError("retention action target escapes trusted root") from exc
    return target


def _quarantine_path(state_root: Path, action_id: str, item: Mapping[str, Any]) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", str(action_id)):
        raise PlanValidationError("invalid retention action id")
    relative = normalize_relative_path(item.get("relative_path"))
    root_id = str(item.get("root_id") or "")
    if not re.fullmatch(r"[a-z0-9_-]{1,40}", root_id):
        raise PlanValidationError("invalid retention root id")
    root = Path(state_root).expanduser().resolve() / "quarantine" / str(action_id) / root_id
    target = root.joinpath(*PurePosixPath(relative).parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise PlanValidationError("retention quarantine target already exists")
    return target


def _verify_runtime_snapshot(path: Path, item: Mapping[str, Any], *, check_identity: bool) -> Dict[str, Any]:
    if path.is_symlink() or not path.is_dir():
        raise PlanValidationError("runtime snapshot is not a safe directory")
    info = path.lstat()
    if check_identity:
        nested_identity = item.get("inode_identity")
        expected = nested_identity if isinstance(nested_identity, Mapping) else item
        actual = {
            "st_dev": info.st_dev,
            "st_ino": info.st_ino,
            "size": info.st_size,
            "mtime_ns": info.st_mtime_ns,
            "nlink": info.st_nlink,
        }
        wanted = {key: int(expected.get(key) or -1) for key in actual}
        if actual != wanted:
            raise PlanValidationError("runtime snapshot identity changed")
    manifest_path = path / "snapshot-manifest.json"
    manifest = _read_json(manifest_path)
    rows = manifest.get("files")
    snapshot_id = str(item.get("snapshot_id") or "")
    if (
        manifest.get("schema_version") != "pm-loop.runtime-backup-manifest.v1"
        or manifest.get("status") != "completed"
        or manifest.get("snapshot_id") != snapshot_id
        or path.name != snapshot_id
        or not isinstance(rows, list)
    ):
        raise PlanValidationError("runtime snapshot manifest contract mismatch")
    manifest_hash = _sha256_file(manifest_path)
    if manifest_hash != item.get("snapshot_manifest_sha256") or manifest_hash != item.get("content_hash"):
        raise PlanValidationError("runtime snapshot manifest hash changed")
    normalized = []
    expected_paths = {"snapshot-manifest.json"}
    logical_bytes = manifest_path.stat().st_size
    for row in rows:
        if not isinstance(row, Mapping):
            raise PlanValidationError("runtime snapshot manifest entry is invalid")
        relative = normalize_relative_path(row.get("relative_path"))
        candidate = path.joinpath(*PurePosixPath(relative).parts)
        if candidate.is_symlink() or not candidate.is_file():
            raise PlanValidationError("runtime snapshot member is unavailable")
        size = int(row.get("bytes") or 0)
        digest = str(row.get("sha256") or "")
        if candidate.stat().st_size != size or _sha256_file(candidate) != digest:
            raise PlanValidationError("runtime snapshot member hash changed")
        normalized.append({"relative_path": relative, "sha256": digest, "bytes": size})
        expected_paths.add(relative)
        logical_bytes += size
    normalized.sort(key=lambda row: row["relative_path"])
    if canonical_hash(normalized) != item.get("snapshot_files_digest"):
        raise PlanValidationError("runtime snapshot file-set digest changed")
    if len(normalized) != int(item.get("snapshot_file_count") or -1):
        raise PlanValidationError("runtime snapshot file count changed")
    actual_paths = set()
    for candidate in path.rglob("*"):
        if candidate.is_symlink() or (not candidate.is_file() and not candidate.is_dir()):
            raise PlanValidationError("runtime snapshot contains an unsafe member")
        if candidate.is_file():
            actual_paths.add(candidate.relative_to(path).as_posix())
    if actual_paths != expected_paths:
        raise PlanValidationError("runtime snapshot contains uncommitted members")
    return {
        "status": "snapshot_verified",
        "snapshot_id": snapshot_id,
        "manifest_sha256": manifest_hash,
        "files_digest": canonical_hash(normalized),
        "file_count": len(normalized),
        "logical_bytes": logical_bytes,
    }


def _runtime_snapshot_restore_smoke(path: Path) -> Dict[str, Any]:
    python_count = json_count = 0
    for candidate in path.rglob("*"):
        if not candidate.is_file() or candidate.name == "snapshot-manifest.json":
            continue
        if candidate.suffix == ".py":
            compile(candidate.read_text(encoding="utf-8"), str(candidate), "exec")
            python_count += 1
        elif candidate.suffix == ".json":
            json.loads(candidate.read_text(encoding="utf-8"))
            json_count += 1
    if python_count < 1 or json_count < 1:
        raise PlanValidationError("runtime snapshot restore smoke has insufficient coverage")
    return {"status": "passed", "compiled_python_files": python_count, "parsed_json_files": json_count}


def _verify_for_profile(item: Mapping[str, Any], roots: Mapping[str, Path]) -> Dict[str, Any]:
    if item.get("action_profile") == "expire-runtime-snapshot-v1":
        return _verify_runtime_snapshot(_target_path(item, roots), item, check_identity=True)
    return verify_plan_item_descriptor(item, roots)


def _assert_no_open_writer(path: Path) -> Dict[str, Any]:
    lsof = Path("/usr/sbin/lsof")
    if not lsof.is_file():
        raise PlanValidationError("open-writer verifier is unavailable")
    try:
        result = subprocess.run(
            [str(lsof), "-Ffn", "--", str(path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PlanValidationError("open-writer verification failed") from exc
    if result.returncode == 0 and result.stdout.strip():
        raise PlanValidationError("sealed log still has an open file handle")
    if result.returncode not in {0, 1}:
        raise PlanValidationError("open-writer verifier returned an unexpected status")
    return {"status": "no_open_handle", "verifier": "lsof"}


def _apply_action(
    *, item: Mapping[str, Any], action_id: str, fencing_token: int, roots: Mapping[str, Path],
    state_root: Path, store: Any,
) -> Dict[str, Any]:
    profile = str(item.get("action_profile") or "")
    source = _target_path(item, roots)
    preflight = _verify_for_profile(item, roots)
    if profile == "expire-file-v1":
        name = source.name
        if not (re.fullmatch(r"daily-\d{8}\.log", name) or re.fullmatch(r"monitor-\d{8}T\d{6}Z(?:-[0-9]+)?\.log", name)):
            raise PlanValidationError("expire-file-v1 only accepts sealed operational logs")
        writer_check = _assert_no_open_writer(source)
    elif profile != "expire-runtime-snapshot-v1":
        raise PlanValidationError("retention physical action profile is not implemented")
    else:
        writer_check = {"status": "not_applicable", "reason": "manifest_closed_snapshot"}
    quarantine = _quarantine_path(state_root, action_id, item)
    if source.stat().st_dev != quarantine.parent.stat().st_dev:
        raise PlanValidationError("retention quarantine crosses a filesystem boundary")
    applied_recorded = False
    os.replace(source, quarantine)
    _fsync_directory(source.parent)
    _fsync_directory(quarantine.parent)
    try:
        store.transition_retention_action(
            action_id, fencing_token=fencing_token, state="applied",
            reason_code="synchronous_quarantine", message="对象已进入同文件系统隔离区。",
            payload={"quarantine": str(quarantine), "preflight": preflight, "writer_check": writer_check},
        )
        applied_recorded = True
        if source.exists() or source.is_symlink():
            raise PlanValidationError("retention source path still exists after quarantine")
        if profile == "expire-runtime-snapshot-v1":
            quarantine_check = _verify_runtime_snapshot(quarantine, item, check_identity=False)
            smoke = _runtime_snapshot_restore_smoke(quarantine)
            shutil.rmtree(quarantine)
        else:
            if quarantine.is_symlink() or not quarantine.is_file() or _sha256_file(quarantine) != item.get("content_hash"):
                raise PlanValidationError("quarantined log identity changed")
            quarantine_check = {"status": "quarantine_hash_verified", "sha256": _sha256_file(quarantine)}
            smoke = {"status": "passed", "check": "sealed_log_removed"}
            quarantine.unlink()
        _fsync_directory(quarantine.parent)
        if source.exists() or quarantine.exists():
            raise PlanValidationError("retention post-check failed")
        reclaimed_logical = int(item.get("expected_reclaim_logical_bytes") or item.get("expected_reclaim_bytes") or 0)
        reclaimed_allocated = int(item.get("expected_reclaim_allocated_bytes") or item.get("expected_reclaim_bytes") or 0)
        store.transition_retention_action(
            action_id, fencing_token=fencing_token, state="verified",
            reason_code="physical_reclaim_verified", message="隔离、恢复验证和物理回收已完成。",
            reclaimed_logical_bytes=reclaimed_logical,
            reclaimed_allocated_bytes=reclaimed_allocated,
            payload={"preflight": preflight, "writer_check": writer_check, "quarantine_check": quarantine_check, "restore_smoke": smoke},
        )
        return {
            "action_id": action_id,
            "object_id": item.get("object_id"),
            "status": "verified",
            "reason_code": "physical_reclaim_verified",
            "reclaimed_logical_bytes": reclaimed_logical,
            "reclaimed_allocated_bytes": reclaimed_allocated,
        }
    except Exception as exc:
        recovery = {"attempted": False, "status": "not_needed"}
        if quarantine.exists() and not source.exists():
            try:
                source.parent.mkdir(parents=True, exist_ok=True)
                os.replace(quarantine, source)
                _fsync_directory(source.parent)
                recovery = {"attempted": True, "status": "restored"}
            except Exception as restore_exc:
                recovery = {"attempted": True, "status": "failed", "error": f"{type(restore_exc).__name__}: {restore_exc}"}
        target_state = "rolled_back" if applied_recorded and recovery["status"] == "restored" else "manual_attention" if applied_recorded else "held"
        store.transition_retention_action(
            action_id, fencing_token=fencing_token, state=target_state,
            reason_code="physical_reclaim_failed", message=str(exc)[:500], payload={"recovery": recovery},
        )
        raise PlanValidationError(f"physical reclaim failed: {exc}") from exc


def _load_latest_plan(state_root: Path) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    root = Path(state_root).expanduser().resolve()
    pointer = _read_json(root / "latest-observer.json")
    result = _read_json(_safe_artifact_path(root, pointer.get("result")))
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise PlanValidationError("observer result has no artifact closure")
    plan = _read_json(_safe_artifact_path(root, artifacts.get("plan")))
    inventory = _read_json(_safe_artifact_path(root, artifacts.get("inventory")))
    unknowns = _read_json(_safe_artifact_path(root, artifacts.get("unknowns")))
    digest = canonical_hash({"inventory": inventory, "unknowns": unknowns, "plan": plan})
    if digest != result.get("artifact_digest") or digest != pointer.get("artifact_digest"):
        raise PlanValidationError("observer artifact digest mismatch")
    return pointer, result, plan


def validate_plan(
    plan: Mapping[str, Any], *, bundle: RetentionBundle, roots: Mapping[str, Path], signing_key: bytes,
    capability_key: Optional[bytes] = None, current: Optional[datetime] = None,
    expected_occurrence_id: Optional[str] = None, expected_schedule_registry_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = current or datetime.now(timezone.utc)
    if plan.get("schema") != PLAN_SCHEMA:
        raise PlanValidationError("unsupported retention plan schema")
    required = {
        "plan_id", "observer_occurrence_id", "issued_at", "not_before", "expires_at", "nonce", "signer_key_id",
        "source_registry_hash", "policy_hash", "deletion_capability_hash", "inventory_hash", "reference_graph_hash",
        "snapshot_token", "worker_build_digest", "adapter_bundle_digest", "resolver_version", "schedule_registry_hash",
        "root_identities", "items", "signature",
    }
    if not required.issubset(plan):
        raise PlanValidationError("retention plan envelope is incomplete")
    if plan.get("signer_key_id") != KEYCHAIN_SERVICE or not str(plan.get("nonce") or ""):
        raise PlanValidationError("retention plan signer/nonce is invalid")
    signature = str(plan.get("signature") or "")
    if not signature.startswith("base64:") or not hmac.compare_digest(signature, sign_plan(plan, signing_key)):
        raise PlanValidationError("retention plan signature mismatch")
    if plan.get("source_registry_hash") != bundle.source_registry_hash or plan.get("policy_hash") != bundle.policy_hash or plan.get("deletion_capability_hash") != bundle.deletion_capability_hash:
        raise PlanValidationError("retention config hash mismatch")
    if plan.get("resolver_version") != RESOLVER_VERSION:
        raise PlanValidationError("retention resolver version mismatch")
    adapter_digest = canonical_hash([ADAPTER_BUNDLE_VERSION, sorted(ACTION_PROFILES)])
    if plan.get("adapter_bundle_digest") != adapter_digest:
        raise PlanValidationError("retention adapter bundle mismatch")
    build_digest = worker_build_digest([Path(__file__).with_name("retention_observer.py"), Path(__file__).with_name("retention_registry.py")])
    if plan.get("worker_build_digest") != build_digest:
        raise PlanValidationError("retention worker build mismatch")
    if plan.get("root_identities") != root_identities(roots):
        raise PlanValidationError("retention trusted-root identity changed")
    if expected_occurrence_id and plan.get("observer_occurrence_id") != expected_occurrence_id:
        raise PlanValidationError("observer occurrence binding mismatch")
    if expected_schedule_registry_hash and plan.get("schedule_registry_hash") != expected_schedule_registry_hash:
        raise PlanValidationError("schedule registry binding mismatch")
    if now < _parse_iso(plan["not_before"]):
        raise PlanValidationError("retention plan is not active yet")
    if now > _parse_iso(plan["expires_at"]):
        raise PlanValidationError("retention plan expired")
    items = plan.get("items")
    if not isinstance(items, list):
        raise PlanValidationError("retention plan items must be an array")
    capability_counts: Dict[str, Dict[str, int]] = {}
    for item in items:
        if not isinstance(item, Mapping):
            raise PlanValidationError("retention plan item must be an object")
        profile = str(item.get("action_profile") or "")
        if profile not in ACTION_PROFILES:
            raise PlanValidationError("retention plan references an unknown fixed action profile")
        normalize_relative_path(item.get("relative_path"))
        if profile == "expire-runtime-snapshot-v1":
            if item.get("object_kind") != "runtime_snapshot" or item.get("file_type") != "directory":
                raise PlanValidationError("runtime snapshot plan item is not group-bound")
            for field in ("snapshot_id", "snapshot_manifest_sha256", "snapshot_files_digest", "snapshot_file_count"):
                if not item.get(field):
                    raise PlanValidationError("runtime snapshot plan item has incomplete manifest evidence")
        elif item.get("file_type") != "regular_file" or int(item.get("nlink") or 0) != 1:
            raise PlanValidationError("retention plan contains an unsafe object identity")
        try:
            due_at = _parse_iso(item.get("due_at"))
        except PlanValidationError:
            raise PlanValidationError("retention plan item has no valid due_at")
        if due_at > now:
            raise PlanValidationError("retention plan contains an item that is not due")
        gate = item.get("gate_results")
        capability = matching_capability(item, profile, bundle, signing_key=capability_key, current=now)
        if not isinstance(gate, Mapping) or capability is None or gate.get("capability") != capability.get("capability_id"):
            raise PlanValidationError("retention deletion capability is not valid")
        if int(gate.get("max_objects_per_batch") or 0) != int(capability.get("max_objects_per_batch") or 0) or int(gate.get("max_bytes_per_day") or 0) != int(capability.get("max_bytes_per_day") or 0):
            raise PlanValidationError("retention capability quota binding mismatch")
        capability_id = str(capability["capability_id"])
        row = capability_counts.setdefault(capability_id, {"objects": 0, "bytes": 0})
        row["objects"] += 1
        row["bytes"] += int(item.get("expected_reclaim_bytes") or 0)
        if row["objects"] > int(capability["max_objects_per_batch"]):
            raise PlanValidationError("retention plan exceeds capability batch limit")
        if row["bytes"] > int(capability["max_bytes_per_day"]):
            raise PlanValidationError("retention plan exceeds capability byte limit")
    return {"status": "verified", "plan_id": plan["plan_id"], "item_count": len(items), "verified_at": now_iso(now)}


def write_result(state_root: Path, result: Mapping[str, Any]) -> Dict[str, Any]:
    root = Path(state_root).expanduser().resolve()
    run_id = str(result["run_id"])
    run_root = root / "reclaimer" / run_id
    if run_root.exists():
        raise FileExistsError(f"immutable reclaimer artifact already exists: {run_id}")
    run_root.mkdir(parents=True, exist_ok=False)
    value = dict(result)
    value["artifact"] = f"reclaimer/{run_id}/result.json"
    value["artifact_digest"] = canonical_hash(value)
    atomic_json_write(run_root / "result.json", value)
    pointer = {"schema_version": "pm-loop.retention-latest.v1", "kind": "reclaimer", "run_id": run_id, "status": value["status"], "reason_code": value.get("reason_code"), "observed_at": value["observed_at"], "result": value["artifact"], "artifact_digest": value["artifact_digest"]}
    atomic_json_write(root / "latest-reclaimer.json", pointer)
    return value


def run_reclaimer(
    *, state_root: Path, registry_path: Path = DEFAULT_SOURCE_REGISTRY, policy_path: Path = DEFAULT_POLICY,
    capabilities_path: Path = DEFAULT_CAPABILITIES, project_root: Optional[Path] = None, home: Optional[Path] = None,
    run_id: Optional[str] = None, dry_run: bool = False, signing_key: Optional[bytes] = None,
    capability_key: Optional[bytes] = None, current: Optional[datetime] = None, db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    bundle = load_bundle(registry_path, policy_path, capabilities_path)
    resolved_run_id = _safe_run_id(run_id or os.environ.get("PM_SCHEDULE_RUN_ID"))
    observed_at = current or datetime.now(timezone.utc)
    base = {"schema_version": RECLAIMER_SCHEMA, "run_id": resolved_run_id, "occurrence_id": os.environ.get("PM_SCHEDULED_OCCURRENCE_ID") or None, "observed_at": now_iso(observed_at), "mode": bundle.global_mode, "dry_run": bool(dry_run), "actions": [], "reclaimed_logical_bytes": 0, "reclaimed_allocated_bytes": 0}
    if bundle.capabilities.get("kill_switch", True):
        return write_result(state_root, {**base, "status": "skipped", "reason_code": "disabled", "message": "自动回收未启用；capability kill switch 已打开。"})
    try:
        pointer, observer_result, plan = _load_latest_plan(state_root)
        key = signing_key or _load_signing_key()
        if not key:
            raise PlanValidationError("retention signing key unavailable")
        roots = trusted_roots(project_root=project_root, home=home)
        capability_secret = capability_key or _load_keychain_secret(CAPABILITY_KEYCHAIN_SERVICE)
        verification = validate_plan(
            plan, bundle=bundle, roots=roots, signing_key=key, capability_key=capability_secret, current=observed_at,
            expected_occurrence_id=str(observer_result.get("occurrence_id") or plan.get("observer_occurrence_id") or ""),
            expected_schedule_registry_hash=os.environ.get("PM_SCHEDULE_REGISTRY_HASH") or None,
        )
        if observer_result.get("run_id") != pointer.get("run_id"):
            raise PlanValidationError("observer pointer/run binding mismatch")
    except (OSError, PlanValidationError, RetentionConfigError) as exc:
        return write_result(state_root, {**base, "status": "held", "reason_code": "plan_validation_failed", "message": str(exc)[:500]})
    if not plan["items"]:
        return write_result(state_root, {**base, "status": "skipped", "reason_code": "no_due_items", "plan_id": plan["plan_id"], "verification": verification, "message": "签名计划有效，但没有满足全部门禁的到期对象。"})
    if not _business_window_open(observed_at):
        return write_result(state_root, {**base, "status": "deferred", "reason_code": "outside_business_window", "plan_id": plan["plan_id"], "verification": verification, "message": "存在待处理对象，但当前不在 10:00-16:30 新批次执行窗口。"})
    effective_db = db_path or (Path(os.environ["PM_SCHEDULE_DB_PATH"]) if os.environ.get("PM_SCHEDULE_DB_PATH") else None)
    if effective_db is None:
        return write_result(state_root, {**base, "status": "held", "reason_code": "coordination_store_unavailable", "plan_id": plan["plan_id"], "message": "缺少 PM System Store，不能原子消费 nonce 或取得 fencing lease。"})
    from pm_system_store import PMSystemStore

    store = PMSystemStore(Path(effective_db).expanduser().resolve())
    blockers = store.retention_runtime_blockers(limit=1)
    if blockers:
        return write_result(state_root, {**base, "status": "deferred", "reason_code": "business_blackout", "plan_id": plan["plan_id"], "message": "存在活动 P0/P1 或高优先级业务任务，本次维护窗口顺延。"})
    pending = store.retention_reconciliation_queue(limit=1)
    if pending:
        return write_result(state_root, {**base, "status": "held", "reason_code": "reconciliation_required", "plan_id": plan["plan_id"], "message": "存在 prepared/applied 中间态，必须先完成 reconciliation。"})
    reclaimer_occurrence_id = str(base["occurrence_id"] or f"manual:{resolved_run_id}")
    try:
        claim = store.claim_retention_plan(
            plan=plan, artifact_digest=str(pointer.get("artifact_digest") or ""),
            reclaimer_occurrence_id=reclaimer_occurrence_id, owner=resolved_run_id,
        )
    except (OSError, ValueError) as exc:
        return write_result(state_root, {**base, "status": "held", "reason_code": "claim_failed", "plan_id": plan["plan_id"], "message": str(exc)[:500]})
    actions = []
    if not dry_run:
        for index, (item, action_id) in enumerate(zip(plan["items"], claim["action_ids"])):
            try:
                action = _apply_action(
                    item=item, action_id=action_id, fencing_token=claim["fencing_token"], roots=roots,
                    state_root=Path(state_root), store=store,
                )
                actions.append(action)
            except (OSError, PlanValidationError, ValueError) as exc:
                try:
                    store.transition_retention_action(
                        action_id, fencing_token=claim["fencing_token"], state="held",
                        reason_code="physical_reclaim_preflight_failed", message=str(exc)[:500],
                    )
                except ValueError:
                    # An action that crossed the applied boundary records its
                    # own rolled_back/manual_attention terminal state.
                    pass
                for remaining_id in claim["action_ids"][index + 1:]:
                    store.transition_retention_action(
                        remaining_id, fencing_token=claim["fencing_token"], state="held",
                        reason_code="batch_aborted", message="同批首个异常触发熔断。",
                    )
                store.release_retention_leases(owner=resolved_run_id, fencing_token=claim["fencing_token"])
                return write_result(state_root, {
                    **base, "status": "held", "reason_code": "physical_reclaim_failed", "plan_id": plan["plan_id"],
                    "claim": claim, "actions": actions, "message": str(exc)[:500],
                })
        store.release_retention_leases(owner=resolved_run_id, fencing_token=claim["fencing_token"])
        reclaimed_logical = sum(int(item.get("reclaimed_logical_bytes") or 0) for item in actions)
        reclaimed_allocated = sum(int(item.get("reclaimed_allocated_bytes") or 0) for item in actions)
        return write_result(state_root, {
            **base, "status": "applied_verified", "reason_code": "physical_reclaim_verified", "plan_id": plan["plan_id"],
            "claim": claim, "actions": actions, "reclaimed_logical_bytes": reclaimed_logical,
            "reclaimed_allocated_bytes": reclaimed_allocated,
            "message": "签名计划内对象已完成同步隔离、恢复验证、物理回收和 post-check。",
        })
    for index, (item, action_id) in enumerate(zip(plan["items"], claim["action_ids"])):
        try:
            descriptor = _verify_for_profile(item, roots)
            action = store.transition_retention_action(
                action_id, fencing_token=claim["fencing_token"], state="verified",
                reason_code="dry_run_descriptor_verified", message="描述符、identity 和 content hash 已复验；未执行物理动作。",
                payload=descriptor,
            )
            actions.append({"action_id": action_id, "object_id": item.get("object_id"), "status": "verified", "reason_code": "dry_run_descriptor_verified"})
        except (OSError, PlanValidationError, ValueError) as exc:
            store.transition_retention_action(action_id, fencing_token=claim["fencing_token"], state="held", reason_code="descriptor_verification_failed", message=str(exc)[:500])
            for remaining_id in claim["action_ids"][index + 1:]:
                store.transition_retention_action(remaining_id, fencing_token=claim["fencing_token"], state="held", reason_code="batch_aborted", message="同批首个异常触发熔断。")
            store.release_retention_leases(owner=resolved_run_id, fencing_token=claim["fencing_token"])
            return write_result(state_root, {**base, "status": "held", "reason_code": "descriptor_verification_failed", "plan_id": plan["plan_id"], "claim": claim, "actions": actions, "message": str(exc)[:500]})
    store.release_retention_leases(owner=resolved_run_id, fencing_token=claim["fencing_token"])
    return write_result(state_root, {**base, "status": "dry_run_verified", "reason_code": "no_physical_action", "plan_id": plan["plan_id"], "verification": verification, "claim": claim, "actions": actions, "planned_object_count": len(plan["items"]), "planned_allocated_bytes": sum(int(item.get("expected_reclaim_bytes") or 0) for item in plan["items"]), "message": "计划、nonce、fencing lease 和对象描述符验证通过；dry-run 未移动、隔离或删除任何对象。"})


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, default=Path.home() / ".codex" / "pm-loop" / "state" / "retention")
    parser.add_argument("--registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--capabilities", type=Path, default=DEFAULT_CAPABILITIES)
    parser.add_argument("--project-root", type=Path, default=Path.home() / "Documents" / "project")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = run_reclaimer(state_root=args.state_root, registry_path=args.registry, policy_path=args.policy, capabilities_path=args.capabilities, project_root=args.project_root, run_id=args.run_id, dry_run=args.dry_run, db_path=args.db_path)
    except (OSError, RetentionConfigError, ValueError) as exc:
        print(json.dumps({"schema_version": RECLAIMER_SCHEMA, "status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in {"skipped", "deferred", "dry_run_verified", "applied_verified"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
