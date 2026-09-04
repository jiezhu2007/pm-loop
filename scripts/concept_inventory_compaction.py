#!/usr/bin/env python3
"""Stage and verify narrowly-scoped compaction for concept deep-inventory state.

Physical replacement is limited to two independently verified representations:
``content-dedup.json`` and the completed legacy deep-inventory run.  Evidence
cache and other product state remain protected.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional


SCHEMA = "pm-loop.concept-inventory-compaction.v1"
DEFAULT_STATE_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "state"
DEFAULT_DEEP_RUN = "deep-inventory-20260820T120658Z-6257c2"
ALLOWED_APPLY_TARGETS = frozenset({"content-dedup.json", f"runs/{DEFAULT_DEEP_RUN}"})
DEFAULT_MANIFEST_TOOL = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "scripts" / "source_manifest.py"
DEFAULT_CONCEPTS_LEDGER = Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "state" / "concepts-ledger.json"
DEFAULT_SOURCE_LEDGERS = (
    Path.home() / ".codex" / "skills" / "shengsuan-sync" / "state" / "ledger.json",
    Path.home() / ".codex" / "skills" / "databuilder-public-docs" / "state" / "ledger.json",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def sha256_gzip_payload(path: Path) -> str:
    digest = hashlib.sha256()
    with gzip.open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_signature(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return {"kind": "object", "key_count": len(value), "canonical_hash": "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()}
    if isinstance(value, list):
        return {"kind": "array", "item_count": len(value), "canonical_hash": "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()}
    return {"kind": type(value).__name__, "canonical_hash": "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()}


def canonical_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_gzip_json(path: Path) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def fsync_directory(path: Path) -> None:
    """Persist a rename/unlink in the containing directory."""
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(str(path), flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as stream:
            shutil.copyfileobj(stream, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


def safe_run_id(value: str) -> str:
    candidate = str(value).strip()
    if not candidate or len(candidate) > 120 or any(not (char.isalnum() or char in "-_.") for char in candidate):
        raise ValueError("run_id must contain only letters, digits, '-', '_' or '.'")
    return candidate


def under(root: Path, candidate: Path) -> Path:
    resolved_root = root.resolve()
    resolved = candidate.resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("compaction target escapes state root") from exc
    return resolved


def _content_dedup_status(full_inventory: Path) -> Dict[str, Any]:
    original = full_inventory / "content-dedup.json"
    compressed = full_inventory / "content-dedup.json.gz"
    result: Dict[str, Any] = {
        "target": "content-dedup.json", "original": str(original), "compressed": str(compressed),
        "allowlisted_for_apply": True, "status": "held", "reason_code": None,
    }
    if not original.is_file() or not compressed.is_file():
        result["reason_code"] = "representation_missing"
        return result
    original_hash, restored_hash = sha256_file(original), sha256_gzip_payload(compressed)
    result.update({
        "original_bytes": original.stat().st_size,
        "compressed_bytes": compressed.stat().st_size,
        "original_sha256": original_hash,
        "restored_sha256": restored_hash,
        "estimated_reclaim_bytes": max(0, original.stat().st_size - compressed.stat().st_size),
    })
    if original_hash != restored_hash:
        result["reason_code"] = "restore_hash_mismatch"
        return result
    result.update({"status": "eligible", "reason_code": "verified_equivalent_representation"})
    return result


def _evidence_cache_status(full_inventory: Path) -> Dict[str, Any]:
    original = full_inventory / "evidence-cache.json"
    compressed = full_inventory / "evidence-cache.json.gz"
    result: Dict[str, Any] = {
        "target": "evidence-cache.json", "original": str(original), "compressed": str(compressed),
        "allowlisted_for_apply": False, "status": "held", "reason_code": "not_in_allowlist",
    }
    if not original.is_file() or not compressed.is_file():
        result["reason_code"] = "representation_missing"
        return result
    try:
        original_signature = json_signature(read_json(original))
        compressed_signature = json_signature(read_gzip_json(compressed))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, gzip.BadGzipFile) as exc:
        result["reason_code"] = "record_equivalence_unreadable"
        result["error"] = type(exc).__name__
        return result
    result.update({
        "original_bytes": original.stat().st_size,
        "compressed_bytes": compressed.stat().st_size,
        "original_signature": original_signature,
        "compressed_signature": compressed_signature,
        "record_equivalence": original_signature == compressed_signature,
    })
    if original_signature != compressed_signature:
        result["reason_code"] = "record_equivalence_mismatch"
    return result


def _verified_deep_archive(full_inventory: Path, run_id: str) -> Optional[Dict[str, Any]]:
    """Find a still-intact, consumer-verified archive for one deep run.

    The weekly observe-only task must retain this proof even when a newer
    plan-only result updates ``latest.json``. It validates every persisted
    artifact again before projecting the prior verification into a new plan.
    """
    candidates = sorted((full_inventory / "compaction" / "runs").glob("*/result.json"), reverse=True)
    original_resources = full_inventory / "runs" / run_id / "resources.json"
    for result_path in candidates:
        try:
            result = read_json(result_path)
            archive = result.get("deep_archive") if isinstance(result, Mapping) else None
            if not isinstance(archive, Mapping) or str(archive.get("deep_run_id") or "") != run_id:
                continue
            smoke = archive.get("consumer_smoke")
            if not isinstance(smoke, Mapping) or smoke.get("status") != "passed":
                continue
            archive_path = Path(str(archive.get("archive") or "")).resolve()
            projection_path = Path(str(archive.get("projection") or "")).resolve()
            manifest_path = Path(str(archive.get("archive_file_manifest") or "")).resolve()
            if not all(path.is_file() for path in (original_resources, archive_path, projection_path, manifest_path)):
                continue
            if (
                sha256_file(original_resources) != str(smoke.get("original_resources_sha256") or "")
                or sha256_file(projection_path) != str(smoke.get("projection_resources_sha256") or "")
                or sha256_file(archive_path) != str(archive.get("archive_sha256") or "")
                or sha256_file(manifest_path) != str(archive.get("archive_file_manifest_sha256") or "")
            ):
                continue
            return {
                "verification_run_id": str(result.get("run_id") or ""),
                "verification_result": str(result_path),
                "archive": str(archive_path),
                "projection": str(projection_path),
                "verified_at": str(result.get("completed_at") or ""),
                "archive_sha256": str(archive.get("archive_sha256") or ""),
                "archive_file_manifest": str(manifest_path),
                "archive_file_manifest_sha256": str(archive.get("archive_file_manifest_sha256") or ""),
                "projection_sha256": str(archive.get("projection_sha256") or ""),
                "consumer_smoke": dict(smoke),
            }
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return None


def _verified_deep_replacement(full_inventory: Path, run_id: str) -> Optional[Dict[str, Any]]:
    for result_path in sorted((full_inventory / "compaction" / "runs").glob("*/result.json"), reverse=True):
        try:
            result = read_json(result_path)
            replacement = result.get("deep_replacement") if isinstance(result, Mapping) else None
            if not isinstance(replacement, Mapping) or replacement.get("status") != "applied" or replacement.get("deep_run_id") != run_id:
                continue
            archive = Path(str(replacement.get("archive") or "")).resolve()
            projection = Path(str(replacement.get("current_projection") or "")).resolve()
            evidence = Path(str(replacement.get("manifest") or "")).resolve()
            if not all(path.is_file() for path in (archive, projection, evidence)):
                continue
            if sha256_file(archive) != replacement.get("archive_sha256") or sha256_file(projection) != replacement.get("current_projection_sha256"):
                continue
            return {
                "replacement_run_id": str(result.get("run_id") or ""),
                "replacement_result": str(result_path),
                "archive": str(archive),
                "projection": str(projection),
                "manifest": str(evidence),
                "reclaimed_logical_bytes": int(replacement.get("reclaimed_logical_bytes") or 0),
            }
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return None


def _deep_run_status(full_inventory: Path, run_id: str) -> Dict[str, Any]:
    run = full_inventory / "runs" / run_id
    result: Dict[str, Any] = {
        "target": f"runs/{run_id}", "allowlisted_for_apply": False, "status": "held",
        "reason_code": "consumer_compatibility_not_implemented", "run_id": run_id,
    }
    if not run.is_dir():
        replacement = _verified_deep_replacement(full_inventory, run_id)
        if replacement:
            result.update({
                "status": "archived",
                "reason_code": "archive_replacement_verified",
                "allowlisted_for_apply": False,
                "archive_replacement": replacement,
            })
            return result
        result["reason_code"] = "deep_run_missing"
        return result
    files = [path for path in sorted(run.rglob("*")) if path.is_file() and not path.is_symlink()]
    result.update({
        "original_bytes": sum(path.stat().st_size for path in files),
        "file_count": len(files),
        "manifest_present": (run / "manifest.json").is_file(),
        "resources_present": (run / "resources.json").is_file(),
    })
    verification = _verified_deep_archive(full_inventory, run_id)
    if verification:
        result.update(
            {
                "status": "eligible",
                "allowlisted_for_apply": True,
                "reason_code": "consumer_archive_verified",
                "consumer_archive_verification": verification,
                "estimated_reclaim_bytes": int(result.get("original_bytes") or 0),
            }
        )
    return result


def build_plan(state_root: Path, *, deep_run: str = DEFAULT_DEEP_RUN) -> Dict[str, Any]:
    root = Path(state_root).expanduser().resolve()
    full_inventory = under(root, root / "full-inventory")
    content = _content_dedup_status(full_inventory)
    evidence = _evidence_cache_status(full_inventory)
    deep = _deep_run_status(full_inventory, deep_run)
    return {
        "schema_version": SCHEMA,
        "created_at": now_iso(),
        "state_root": str(root),
        "full_inventory": str(full_inventory),
        "targets": [content, evidence, deep],
        "eligible_apply_targets": [
            item["target"] for item in (content, deep)
            if item.get("status") == "eligible" and item.get("target") in ALLOWED_APPLY_TARGETS
        ],
        "estimated_reclaim_bytes": sum(
            int(item.get("estimated_reclaim_bytes") or 0) for item in (content, deep)
            if item.get("status") == "eligible" and item.get("target") in ALLOWED_APPLY_TARGETS
        ),
    }


def _stage_deep_archive(full_inventory: Path, run_id: str, output_root: Path, *, compaction_run_id: str) -> Dict[str, Any]:
    run = under(full_inventory, full_inventory / "runs" / run_id)
    if not run.is_dir():
        raise ValueError("deep inventory run is missing")
    archive_root = output_root / "archives"
    archive_root.mkdir(parents=True, exist_ok=True)
    archive = archive_root / f"{run_id}.tar.gz"
    temporary = archive.with_suffix(".tar.gz.tmp")
    if archive.exists():
        raise ValueError("archive already exists; use a new compaction run id")
    files = [path for path in sorted(run.rglob("*")) if path.is_file() and not path.is_symlink()]
    with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as bundle:
        for path in files:
            bundle.add(path, arcname=path.relative_to(run).as_posix(), recursive=False)
    os.replace(temporary, archive)
    manifest_files = [
        {"relative_path": path.relative_to(run).as_posix(), "bytes": path.stat().st_size, "sha256": sha256_file(path)}
        for path in files
    ]
    archive_manifest = {
        "schema_version": SCHEMA,
        "kind": "deep-inventory-archive-manifest",
        "deep_run_id": run_id,
        "created_at": now_iso(),
        "files": manifest_files,
    }
    archive_manifest_path = output_root / "manifests" / f"{run_id}-archive-manifest.json"
    atomic_json_write(archive_manifest_path, archive_manifest)
    with tarfile.open(archive, "r:gz") as bundle:
        names = sorted(member.name for member in bundle.getmembers() if member.isfile())
    expected_names = sorted(entry["relative_path"] for entry in manifest_files)
    if names != expected_names:
        archive.unlink(missing_ok=True)
        raise RuntimeError("deep inventory archive member list mismatch")
    resources = run / "resources.json"
    # This is the stable compatibility location a future consumer may opt
    # into.  It is intentionally distinct from the compaction work directory.
    projection = full_inventory / "compatible-resources" / compaction_run_id / "resources.json"
    projection.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(resources, projection)
    if sha256_file(resources) != sha256_file(projection):
        raise RuntimeError("resource projection hash mismatch")
    return {
        "status": "staged", "archive": str(archive), "archive_sha256": sha256_file(archive),
        "archive_bytes": archive.stat().st_size, "file_count": len(manifest_files),
        "archive_file_manifest": str(archive_manifest_path),
        "archive_file_manifest_sha256": sha256_file(archive_manifest_path),
        "archive_files_digest": canonical_hash(manifest_files),
        "projection": str(projection), "projection_sha256": sha256_file(projection),
    }


def _relative_archive_member(member: tarfile.TarInfo) -> PurePosixPath:
    path = PurePosixPath(member.name)
    if member.issym() or member.islnk() or path.is_absolute() or ".." in path.parts:
        raise RuntimeError("unsafe archive member")
    if not member.isfile() or not member.name:
        raise RuntimeError("archive contains a non-file member")
    return path


def _restore_archive_for_smoke(archive: Path, expected_files: list[Mapping[str, Any]], destination: Path) -> None:
    expected = {str(item["relative_path"]): dict(item) for item in expected_files}
    restored: set[str] = set()
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            relative = _relative_archive_member(member)
            key = relative.as_posix()
            expected_item = expected.get(key)
            if expected_item is None:
                raise RuntimeError("archive contains an unexpected member")
            target = destination.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            stream = bundle.extractfile(member)
            if stream is None:
                raise RuntimeError("archive member cannot be read")
            with target.open("wb") as output:
                shutil.copyfileobj(stream, output)
            if target.stat().st_size != int(expected_item["bytes"]) or sha256_file(target) != str(expected_item["sha256"]):
                raise RuntimeError("archive restore hash mismatch")
            restored.add(key)
    if restored != set(expected):
        raise RuntimeError("archive restore member set mismatch")


def _manifest_fingerprint(value: Mapping[str, Any]) -> str:
    """Hash only the source-manifest outputs a consumer actually observes."""
    return canonical_hash(
        {
            "document_mappings": value.get("document_mappings"),
            "active_source_unique_checks": value.get("active_source_unique_checks"),
            "metrics": value.get("metrics"),
            "conflicts": value.get("conflicts"),
        }
    )


def _write_source_manifest(
    *, manifest_tool: Path, ledgers: tuple[Path, ...], concepts_ledger: Path, inventory: Path, output: Path,
) -> Mapping[str, Any]:
    if not manifest_tool.is_file() or not concepts_ledger.is_file() or not all(path.is_file() for path in ledgers):
        raise ValueError("consumer smoke inputs are unavailable")
    command = [sys.executable, str(manifest_tool)]
    for ledger in ledgers:
        command.extend(("--ledger", str(ledger)))
    command.extend(("--inventory", str(inventory), "--concepts-ledger", str(concepts_ledger), "--output", str(output)))
    try:
        subprocess.run(command, check=True, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"source manifest consumer smoke failed: {type(exc).__name__}") from exc
    value = read_json(output)
    if not isinstance(value, Mapping):
        raise RuntimeError("source manifest consumer returned a non-object")
    return value


def _consumer_smoke(
    *, full_inventory: Path, staged: Mapping[str, Any], ledgers: tuple[Path, ...], concepts_ledger: Path, manifest_tool: Path,
) -> Dict[str, Any]:
    """Verify original, compatibility projection and restored archive alike.

    The smoke uses the actual source-manifest consumer, but confines all its
    output and the archive extraction to a temporary directory.  It never
    writes active ledgers, baseline pointers, concept pages or OpenViking.
    """
    original = full_inventory / "runs" / str(staged["deep_run_id"]) / "resources.json"
    projection = Path(str(staged["projection"])).resolve()
    archive = Path(str(staged["archive"])).resolve()
    archive_manifest = read_json(Path(str(staged["archive_file_manifest"])))
    expected_files = archive_manifest.get("files") if isinstance(archive_manifest, Mapping) else None
    if not original.is_file() or not projection.is_file() or not archive.is_file() or not isinstance(expected_files, list):
        raise RuntimeError("consumer smoke staging artifact missing")
    with tempfile.TemporaryDirectory(prefix="concept-compaction-smoke-") as temporary_name:
        temporary = Path(temporary_name)
        restored_root = temporary / "restored"
        _restore_archive_for_smoke(archive, expected_files, restored_root)
        outputs: Dict[str, Mapping[str, Any]] = {}
        for label, inventory in (
            ("original", original),
            ("projection", projection),
            ("archive_restore", restored_root / "resources.json"),
        ):
            outputs[label] = _write_source_manifest(
                manifest_tool=manifest_tool,
                ledgers=ledgers,
                concepts_ledger=concepts_ledger,
                inventory=inventory,
                output=temporary / f"{label}-source-manifest.json",
            )
        fingerprints = {label: _manifest_fingerprint(value) for label, value in outputs.items()}
        if len(set(fingerprints.values())) != 1:
            raise RuntimeError("consumer source-manifest fingerprint mismatch")
        return {
            "status": "passed",
            "consumer": "shengsuan-concepts/source_manifest.py",
            "source_manifest_fingerprints": fingerprints,
            "original_resources_sha256": sha256_file(original),
            "projection_resources_sha256": sha256_file(projection),
            "archive_restore_resources_sha256": sha256_file(restored_root / "resources.json"),
            "ledger_hashes": {str(path): sha256_file(path) for path in ledgers},
            "concepts_ledger_sha256": sha256_file(concepts_ledger),
        }


def _archive_only_consumer_smoke(
    *, archive: Path, archive_manifest: Path, projection: Path,
    expected_fingerprint: str, ledgers: tuple[Path, ...], concepts_ledger: Path, manifest_tool: Path,
) -> Dict[str, Any]:
    manifest_value = read_json(archive_manifest)
    expected_files = manifest_value.get("files") if isinstance(manifest_value, Mapping) else None
    if not isinstance(expected_files, list):
        raise RuntimeError("archive-only smoke has no file manifest")
    with tempfile.TemporaryDirectory(prefix="concept-compaction-archive-only-") as temporary_name:
        temporary = Path(temporary_name)
        restored_root = temporary / "restored"
        _restore_archive_for_smoke(archive, expected_files, restored_root)
        fingerprints: Dict[str, str] = {}
        for label, inventory in (
            ("current_projection", projection),
            ("archive_restore", restored_root / "resources.json"),
        ):
            value = _write_source_manifest(
                manifest_tool=manifest_tool,
                ledgers=ledgers,
                concepts_ledger=concepts_ledger,
                inventory=inventory,
                output=temporary / f"{label}-source-manifest.json",
            )
            fingerprints[label] = _manifest_fingerprint(value)
        if set(fingerprints.values()) != {expected_fingerprint}:
            raise RuntimeError("archive-only consumer fingerprint mismatch")
        return {
            "status": "passed",
            "consumer": "shengsuan-concepts/source_manifest.py",
            "source_manifest_fingerprints": fingerprints,
            "expected_fingerprint": expected_fingerprint,
            "projection_resources_sha256": sha256_file(projection),
            "archive_restore_resources_sha256": sha256_file(restored_root / "resources.json"),
        }


def _resolve_content_consumer(path: Optional[Path] = None) -> Path:
    """Locate the deployed deep-inventory reader used by the canary."""
    candidates = [
        Path(path).expanduser().resolve() if path is not None else None,
        Path(__file__).resolve().with_name("concept_deep_inventory.py"),
        Path.home() / ".codex" / "pm-loop" / "runtime" / "scripts" / "concept_deep_inventory.py",
        Path.home() / ".codex" / "skills" / "shengsuan-concepts" / "scripts" / "concept_deep_inventory.py",
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    raise ValueError("content-dedup consumer is unavailable")


def _content_dedup_consumer_canary(
    *, original: Path, compressed: Path, consumer_script: Optional[Path] = None,
) -> Dict[str, Any]:
    """Exercise the deployed reader while only the gzip representation exists."""
    if original.exists():
        raise RuntimeError("consumer canary requires the original representation to be absent")
    if not compressed.is_file():
        raise RuntimeError("consumer canary compressed representation is missing")
    consumer_path = _resolve_content_consumer(consumer_script)
    module_name = f"concept_deep_inventory_canary_{hashlib.sha256(str(consumer_path).encode()).hexdigest()[:12]}"
    spec = importlib.util.spec_from_file_location(module_name, consumer_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load content-dedup consumer")
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(consumer_path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        try:
            sys.path.remove(str(consumer_path.parent))
        except ValueError:
            pass
    reader = getattr(module, "_read_json", None)
    if not callable(reader):
        raise RuntimeError("content-dedup consumer has no _read_json reader")
    missing = object()
    observed = reader(original, missing)
    expected = read_gzip_json(compressed)
    if observed is missing or observed != expected:
        raise RuntimeError("gzip-only consumer readback mismatch")
    preferred_path_fn = getattr(module, "_content_dedup_path", None)
    preferred_path = preferred_path_fn(original.parent) if callable(preferred_path_fn) else compressed
    preferred = reader(Path(preferred_path), missing)
    if preferred is missing or preferred != expected:
        raise RuntimeError("preferred gzip consumer readback mismatch")
    return {
        "status": "passed",
        "consumer": str(consumer_path),
        "reader": "_read_json",
        "fallback_path": str(original),
        "preferred_path": str(preferred_path),
        "observed_canonical_hash": canonical_hash(observed),
        "expected_canonical_hash": canonical_hash(expected),
    }


def _write_replacement_evidence(
    output_root: Path, evidence: Mapping[str, Any], *, name: str = "content-dedup-replacement.json",
) -> Dict[str, Any]:
    manifest_path = output_root / "manifests" / name
    if manifest_path.exists():
        raise ValueError("content-dedup replacement evidence already exists")
    atomic_json_write(manifest_path, dict(evidence))
    return {"manifest": str(manifest_path), "manifest_sha256": sha256_file(manifest_path)}


def apply_content_replacement(
    plan: Mapping[str, Any], *, output_root: Path, confirmation: bool,
    consumer_script: Optional[Path] = None,
) -> Dict[str, Any]:
    if not confirmation:
        raise ValueError("physical replacement requires --confirm-content-dedup-reclaim")
    targets = {str(item["target"]): item for item in plan.get("targets", []) if isinstance(item, Mapping)}
    content = targets.get("content-dedup.json")
    if not content or content.get("status") != "eligible" or content.get("target") not in ALLOWED_APPLY_TARGETS:
        raise ValueError("content-dedup.json is not eligible for physical replacement")
    state_root = Path(str(plan.get("state_root") or "")).expanduser().resolve()
    if not state_root.is_dir():
        raise ValueError("compaction state root is unavailable")
    original = under(state_root, Path(str(content["original"])))
    compressed = under(state_root, Path(str(content["compressed"])))
    output_root = under(state_root, Path(output_root))
    if sha256_file(original) != content.get("original_sha256") or sha256_gzip_payload(compressed) != content.get("restored_sha256"):
        raise RuntimeError("content-dedup representation changed after planning")
    quarantine = output_root / "quarantine" / "content-dedup.json"
    quarantine.parent.mkdir(parents=True, exist_ok=True)
    if quarantine.exists():
        raise ValueError("content-dedup quarantine target already exists")
    intent = {
        "schema_version": SCHEMA,
        "action": "replace_with_verified_gzip",
        "created_at": now_iso(),
        "target": dict(content),
        "original": str(original),
        "compressed": str(compressed),
        "quarantine": str(quarantine),
        "pre_original_sha256": sha256_file(original),
        "pre_restored_sha256": sha256_gzip_payload(compressed),
    }
    intent_info = _write_replacement_evidence(
        output_root, {**intent, "kind": "replacement-intent"}, name="content-dedup-replacement-intent.json",
    )
    os.replace(original, quarantine)
    fsync_directory(quarantine.parent)
    fsync_directory(original.parent)
    try:
        canary = _content_dedup_consumer_canary(
            original=original, compressed=compressed, consumer_script=consumer_script,
        )
    except Exception as exc:
        recovery: Dict[str, Any] = {"attempted": True, "status": "failed"}
        try:
            if original.exists():
                raise RuntimeError("original path unexpectedly reappeared")
            os.replace(quarantine, original)
            fsync_directory(original.parent)
            fsync_directory(quarantine.parent)
            recovery = {"attempted": True, "status": "restored", "restored_sha256": sha256_file(original)}
        except Exception as restore_exc:
            recovery = {
                "attempted": True,
                "status": "failed",
                "error": f"{type(restore_exc).__name__}: {restore_exc}",
                "quarantine_retained": quarantine.is_file(),
            }
            evidence = {
                **intent,
                "kind": "replacement-result",
                "completed_at": now_iso(),
                "result": "recovery_failed",
                "canary": {"status": "failed", "error": f"{type(exc).__name__}: {exc}"},
                "recovery": recovery,
                "intent_evidence": intent_info,
            }
            result_info = _write_replacement_evidence(output_root, evidence)
            raise RuntimeError(f"content-dedup canary failed and recovery failed; evidence={result_info['manifest']}") from restore_exc
        evidence = {
            **intent,
            "kind": "replacement-result",
            "completed_at": now_iso(),
            "result": "reverted_after_canary_failure",
            "canary": {"status": "failed", "error": f"{type(exc).__name__}: {exc}"},
            "recovery": recovery,
            "intent_evidence": intent_info,
        }
        result_info = _write_replacement_evidence(output_root, evidence)
        return {
            "status": "reverted",
            "manifest": result_info["manifest"],
            "manifest_sha256": result_info["manifest_sha256"],
            "intent_manifest": intent_info["manifest"],
            "reclaimed_logical_bytes": 0,
            "canary": evidence["canary"],
            "recovery": recovery,
        }
    os.unlink(quarantine)
    fsync_directory(quarantine.parent)
    post_original = original.exists()
    post_compressed_hash = sha256_gzip_payload(compressed)
    evidence = {
        **intent,
        "kind": "replacement-result",
        "completed_at": now_iso(),
        "result": "original_removed",
        "canary": canary,
        "recovery": {"attempted": False, "status": "not_needed"},
        "post_original_exists": post_original,
        "post_restored_sha256": post_compressed_hash,
        "intent_evidence": intent_info,
    }
    result_info = _write_replacement_evidence(output_root, evidence)
    return {
        "status": "applied",
        "manifest": result_info["manifest"],
        "manifest_sha256": result_info["manifest_sha256"],
        "intent_manifest": intent_info["manifest"],
        "reclaimed_logical_bytes": int(content.get("original_bytes") or 0),
        "canary": canary,
        "recovery": evidence["recovery"],
    }


def apply_deep_archive_replacement(
    plan: Mapping[str, Any], *, output_root: Path, confirmation: bool,
    source_ledgers: tuple[Path, ...], concepts_ledger: Path, manifest_tool: Path,
) -> Dict[str, Any]:
    if not confirmation:
        raise ValueError("deep archive physical replacement requires explicit confirmation")
    targets = {str(item["target"]): item for item in plan.get("targets", []) if isinstance(item, Mapping)}
    target_name = f"runs/{DEFAULT_DEEP_RUN}"
    deep = targets.get(target_name)
    if not deep or deep.get("status") != "eligible" or target_name not in ALLOWED_APPLY_TARGETS:
        raise ValueError("deep inventory run is not eligible for archive replacement")
    verification = deep.get("consumer_archive_verification")
    if not isinstance(verification, Mapping):
        raise ValueError("deep inventory archive verification is unavailable")
    state_root = Path(str(plan.get("state_root") or "")).expanduser().resolve()
    full_inventory = under(state_root, Path(str(plan.get("full_inventory") or "")))
    source = under(state_root, full_inventory / "runs" / DEFAULT_DEEP_RUN)
    output_root = under(state_root, Path(output_root))
    archive = under(state_root, Path(str(verification.get("archive") or "")))
    archive_manifest = under(state_root, Path(str(verification.get("archive_file_manifest") or "")))
    prior_projection = under(state_root, Path(str(verification.get("projection") or "")))
    if sha256_file(archive) != verification.get("archive_sha256"):
        raise RuntimeError("deep archive changed after planning")
    if sha256_file(archive_manifest) != verification.get("archive_file_manifest_sha256"):
        raise RuntimeError("deep archive manifest changed after planning")
    if sha256_file(prior_projection) != verification.get("projection_sha256"):
        raise RuntimeError("deep projection changed after planning")
    expected_fingerprints = dict(verification.get("consumer_smoke") or {}).get("source_manifest_fingerprints") or {}
    expected_fingerprint = str(expected_fingerprints.get("original") or expected_fingerprints.get("projection") or "")
    if not expected_fingerprint:
        raise RuntimeError("deep archive consumer fingerprint is unavailable")

    current_root = full_inventory / "compatible-resources" / "current"
    current_projection = current_root / "resources.json"
    atomic_copy(prior_projection, current_projection)
    if sha256_file(current_projection) != verification.get("projection_sha256"):
        raise RuntimeError("stable deep projection hash mismatch")
    current_manifest = current_root / "manifest.json"
    atomic_json_write(current_manifest, {
        "schema_version": SCHEMA,
        "kind": "deep-inventory-current-projection",
        "deep_run_id": DEFAULT_DEEP_RUN,
        "created_at": now_iso(),
        "resources": str(current_projection),
        "resources_sha256": sha256_file(current_projection),
        "archive": str(archive),
        "archive_sha256": sha256_file(archive),
        "archive_file_manifest": str(archive_manifest),
        "archive_file_manifest_sha256": sha256_file(archive_manifest),
    })
    quarantine = output_root / "quarantine" / DEFAULT_DEEP_RUN
    quarantine.parent.mkdir(parents=True, exist_ok=True)
    if quarantine.exists():
        raise ValueError("deep inventory quarantine target already exists")
    intent = {
        "schema_version": SCHEMA,
        "kind": "deep-archive-replacement-intent",
        "deep_run_id": DEFAULT_DEEP_RUN,
        "created_at": now_iso(),
        "source": str(source),
        "source_bytes": int(deep.get("original_bytes") or 0),
        "source_file_count": int(deep.get("file_count") or 0),
        "archive": str(archive),
        "archive_sha256": sha256_file(archive),
        "archive_file_manifest": str(archive_manifest),
        "current_projection": str(current_projection),
        "current_projection_sha256": sha256_file(current_projection),
        "quarantine": str(quarantine),
    }
    intent_path = output_root / "manifests" / "deep-archive-replacement-intent.json"
    atomic_json_write(intent_path, intent)
    os.replace(source, quarantine)
    fsync_directory(source.parent)
    fsync_directory(quarantine.parent)
    deleted = False
    try:
        smoke = _archive_only_consumer_smoke(
            archive=archive,
            archive_manifest=archive_manifest,
            projection=current_projection,
            expected_fingerprint=expected_fingerprint,
            ledgers=tuple(Path(path).expanduser().resolve() for path in source_ledgers),
            concepts_ledger=Path(concepts_ledger).expanduser().resolve(),
            manifest_tool=Path(manifest_tool).expanduser().resolve(),
        )
        ready_path = output_root / "manifests" / "deep-archive-replacement-ready.json"
        atomic_json_write(ready_path, {**intent, "kind": "deep-archive-replacement-ready", "verified_at": now_iso(), "consumer_smoke": smoke})
        shutil.rmtree(quarantine)
        deleted = True
        fsync_directory(quarantine.parent)
        if source.exists() or quarantine.exists():
            raise RuntimeError("deep archive replacement post-check failed")
        result_evidence = {
            **intent,
            "kind": "deep-archive-replacement-result",
            "completed_at": now_iso(),
            "result": "original_removed",
            "consumer_smoke": smoke,
            "post_original_exists": source.exists(),
            "post_quarantine_exists": quarantine.exists(),
            "ready_evidence": str(ready_path),
            "ready_evidence_sha256": sha256_file(ready_path),
        }
        result_path = output_root / "manifests" / "deep-archive-replacement.json"
        atomic_json_write(result_path, result_evidence)
        return {
            "status": "applied",
            "deep_run_id": DEFAULT_DEEP_RUN,
            "archive": str(archive),
            "archive_sha256": sha256_file(archive),
            "current_projection": str(current_projection),
            "current_projection_sha256": sha256_file(current_projection),
            "manifest": str(result_path),
            "manifest_sha256": sha256_file(result_path),
            "consumer_smoke": smoke,
            "reclaimed_logical_bytes": int(deep.get("original_bytes") or 0),
        }
    except Exception as exc:
        recovery_status = "failed"
        try:
            if quarantine.exists() and not source.exists():
                os.replace(quarantine, source)
            elif deleted and not source.exists():
                restore_root = output_root / "restore" / DEFAULT_DEEP_RUN
                manifest_value = read_json(archive_manifest)
                _restore_archive_for_smoke(archive, list(manifest_value.get("files") or []), restore_root)
                os.replace(restore_root, source)
            fsync_directory(source.parent)
            if source.is_dir():
                recovery_status = "restored"
        except Exception:
            recovery_status = "failed"
        failure_path = output_root / "manifests" / "deep-archive-replacement-failure.json"
        atomic_json_write(failure_path, {**intent, "kind": "deep-archive-replacement-failure", "completed_at": now_iso(), "error": f"{type(exc).__name__}: {exc}", "recovery_status": recovery_status})
        return {
            "status": "reverted" if recovery_status == "restored" else "manual_attention",
            "deep_run_id": DEFAULT_DEEP_RUN,
            "manifest": str(failure_path),
            "manifest_sha256": sha256_file(failure_path),
            "recovery": {"status": recovery_status},
            "reclaimed_logical_bytes": 0,
        }


def run(
    state_root: Path, *, run_id: str, stage_deep_archive: bool = False, apply: bool = False, confirmation: bool = False,
    apply_deep_archive: bool = False, deep_confirmation: bool = False,
    source_ledgers: tuple[Path, ...] = DEFAULT_SOURCE_LEDGERS, concepts_ledger: Path = DEFAULT_CONCEPTS_LEDGER,
    manifest_tool: Path = DEFAULT_MANIFEST_TOOL, content_consumer: Optional[Path] = None,
) -> Dict[str, Any]:
    root = Path(state_root).expanduser().resolve()
    safe = safe_run_id(run_id)
    output_root = under(root, root / "full-inventory" / "compaction" / "runs" / safe)
    if output_root.exists():
        raise ValueError("compaction run already exists")
    plan = build_plan(root)
    result: Dict[str, Any] = {
        "schema_version": SCHEMA,
        "run_id": safe,
        "occurrence_id": str(os.environ.get("PM_SCHEDULED_OCCURRENCE_ID") or "").strip() or None,
        "started_at": now_iso(),
        "plan": plan,
        "mode": "manual_apply" if apply else "observe_only",
        "status": "verified",
    }
    if stage_deep_archive:
        staged = _stage_deep_archive(Path(plan["full_inventory"]), DEFAULT_DEEP_RUN, output_root, compaction_run_id=safe)
        staged["deep_run_id"] = DEFAULT_DEEP_RUN
        staged["consumer_smoke"] = _consumer_smoke(
            full_inventory=Path(plan["full_inventory"]),
            staged=staged,
            ledgers=tuple(Path(path).expanduser().resolve() for path in source_ledgers),
            concepts_ledger=Path(concepts_ledger).expanduser().resolve(),
            manifest_tool=Path(manifest_tool).expanduser().resolve(),
        )
        result["deep_archive"] = staged
        result["status"] = "staged_verified"
    if apply:
        replacement = apply_content_replacement(
            plan, output_root=output_root, confirmation=confirmation, consumer_script=content_consumer,
        )
        result["content_replacement"] = replacement
        result["status"] = "applied" if replacement["status"] == "applied" else replacement["status"]
    if apply_deep_archive:
        deep_replacement = apply_deep_archive_replacement(
            plan,
            output_root=output_root,
            confirmation=deep_confirmation,
            source_ledgers=source_ledgers,
            concepts_ledger=concepts_ledger,
            manifest_tool=manifest_tool,
        )
        result["deep_replacement"] = deep_replacement
        result["status"] = "applied" if deep_replacement["status"] == "applied" else deep_replacement["status"]
    result["completed_at"] = now_iso()
    result_path = output_root / "result.json"
    atomic_json_write(result_path, result)
    latest_marker = root / "full-inventory" / "compaction" / "latest.json"
    atomic_json_write(
        latest_marker,
        {
            "schema_version": SCHEMA,
            "status": result["status"],
            "mode": result["mode"],
            "run_id": safe,
            "occurrence_id": result["occurrence_id"],
            "completed_at": result["completed_at"],
            "result": str(result_path),
            "result_sha256": sha256_file(result_path),
            "physical_reclaim_executed": bool((apply or apply_deep_archive) and result["status"] == "applied"),
        },
    )
    result["latest_marker"] = str(latest_marker)
    return result


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--run-id", default=os.environ.get("PM_SCHEDULE_RUN_ID") or "concept-compaction-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--stage-deep-archive", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-content-dedup-reclaim", action="store_true")
    parser.add_argument("--apply-deep-archive", action="store_true")
    parser.add_argument("--confirm-deep-archive-reclaim", action="store_true")
    parser.add_argument("--content-consumer", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        value = run(
            args.state_root, run_id=args.run_id, stage_deep_archive=args.stage_deep_archive,
            apply=args.apply, confirmation=args.confirm_content_dedup_reclaim,
            apply_deep_archive=args.apply_deep_archive, deep_confirmation=args.confirm_deep_archive_reclaim,
            content_consumer=args.content_consumer,
        )
    except (OSError, ValueError, RuntimeError, tarfile.TarError, gzip.BadGzipFile, json.JSONDecodeError) as exc:
        print(json.dumps({"schema_version": SCHEMA, "status": "failed", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 1
    print(json.dumps(value, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
