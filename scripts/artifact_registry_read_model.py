#!/usr/bin/env python3
"""Read-only Artifact Registry projection for historical and live artifacts.

Root Inventory is the complete project-file audit. The manifest index is the
live Worker ingress. This projection joins the two sources without reading
candidate document bodies, exposing only policy-approved metadata and opaque
open handles.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


INVENTORY_SCHEMA = "pm-loop.artifact-inventory.v1"
MANIFEST_SCHEMA = "pm-loop.artifact-manifest.v1"
MANIFEST_INDEX_SCHEMA = "pm-loop.artifact-manifest-index.v1"
SCHEMA = "pm-loop.artifact-registry-read-model.v1"
MAX_PAGE_SIZE = 100
MAX_SEARCH_LENGTH = 200
OPEN_KINDS = {
    ".html": "html",
    ".htm": "html",
    ".md": "markdown",
    ".mdx": "markdown",
    ".pdf": "pdf",
    ".json": "json",
    ".jsonl": "json",
    ".txt": "text",
}
MANIFEST_REPRESENTATIONS = {
    "markdown_path": "markdown",
    "html_path": "html",
    "pdf_path": "pdf",
}
OPEN_LABELS = {"html": "打开 HTML", "markdown": "打开 Markdown", "pdf": "打开 PDF", "json": "打开 JSON", "text": "打开文本"}
ARTIFACT_ID_RE = re.compile(r"artifact:sha256:[0-9a-f]{64}")
_ALLOWED_TOP_LEVELS = ("docs", "output", "outputs", "ku-archive", "projects", "demo")
_DISPLAY_TYPE_PRIORITY = {
    "markdown": 0,
    "html": 0,
    "pdf": 0,
    "document": 0,
    "presentation": 1,
    "spreadsheet": 1,
    "csv": 2,
    "media": 3,
    "image": 4,
    "run_evidence": 5,
    "archive": 6,
    "json": 7,
    "jsonl": 7,
    "text": 7,
    "yaml": 8,
    "source_code": 8,
    "unknown": 9,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
    except ValueError:
        return None


def _related(value: Any) -> Dict[str, list[str]]:
    """Expose only explicit manifest identifiers, never inferred title matches."""
    if not isinstance(value, Mapping):
        return {"customers": [], "requirements": [], "tasks": [], "versions": [], "decisions": []}
    return {
        key: [str(item) for item in items if isinstance(item, (str, int, float))]
        for key in ("customers", "requirements", "tasks", "versions", "decisions")
        for items in [value.get(key) if isinstance(value.get(key), list) else []]
    }


def _safe_regular_file(path: Path, root: Path) -> Optional[Path]:
    """Validate a lexical path without following a symlink at any segment."""
    try:
        lexical_root = root.absolute()
        lexical_path = path.absolute()
        resolved_root = lexical_root.resolve(strict=True)
        try:
            relative = lexical_path.relative_to(lexical_root)
        except ValueError:
            # Permit the macOS /var -> /private/var alias, but only after the
            # physical path is proven to remain below the approved root.
            lexical_path = lexical_path.resolve(strict=True)
            lexical_root = resolved_root
            relative = lexical_path.relative_to(lexical_root)
        current = lexical_root
        if current.is_symlink():
            return None
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                return None
        if not lexical_path.is_file():
            return None
        lexical_path.resolve(strict=True).relative_to(resolved_root)
        return lexical_path
    except (OSError, RuntimeError, ValueError):
        return None


class ArtifactRegistryReadModel:
    """Bounded, cached projection of immutable inventory and manifest sources."""

    read_only = True

    def __init__(self, *, project_root: Path, inventory_root: Optional[Path] = None) -> None:
        self.project_root = Path(project_root).expanduser().resolve()
        self.inventory_root = Path(inventory_root or self.project_root / "state" / "pm-loop" / "artifact-inventory").expanduser().resolve()
        self.manifest_root = self.project_root / "state" / "pm-loop" / "artifact-manifests"
        self.registry_root = self.project_root / "state" / "pm-loop" / "artifact-registry"
        self._lock = threading.RLock()
        self._cached_signature: Optional[tuple[Optional[tuple[int, int]], ...]] = None
        self._cached_sources: Optional[Dict[str, Any]] = None

    def _pointer_path(self) -> Path:
        return self.inventory_root / "latest.json"

    def _last_attempt_path(self) -> Path:
        return self.inventory_root / "last-attempt.json"

    def _manifest_index_path(self) -> Path:
        return self.registry_root / "manifest-index.json"

    @staticmethod
    def _signature(path: Path, root: Path) -> Optional[tuple[int, int]]:
        safe = _safe_regular_file(path, root)
        if safe is None:
            return None
        try:
            stat = safe.stat()
            return stat.st_mtime_ns, stat.st_size
        except OSError:
            return None

    def _load_inventory(self) -> Optional[Dict[str, Any]]:
        pointer_path = _safe_regular_file(self._pointer_path(), self.inventory_root)
        if pointer_path is None:
            return None
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            reference = pointer.get("inventory_snapshot_path")
            if not isinstance(reference, str) or not reference or Path(reference).is_absolute():
                raise ValueError("invalid_inventory_snapshot_reference")
            snapshot = _safe_regular_file(self.inventory_root / reference, self.inventory_root)
            if snapshot is None or snapshot.suffix != ".gz":
                raise ValueError("inventory_snapshot_unavailable")
            with gzip.open(snapshot, "rt", encoding="utf-8") as stream:
                value = json.load(stream)
            if not isinstance(value, dict) or value.get("schema_version") != INVENTORY_SCHEMA:
                raise ValueError("invalid_inventory_schema")
            if value.get("root") != str(self.project_root):
                raise ValueError("inventory_root_mismatch")
            return value
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            return None

    def _load_last_attempt(self) -> Optional[Dict[str, Any]]:
        path = _safe_regular_file(self._last_attempt_path(), self.inventory_root)
        if path is None:
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or value.get("manifest_kind") != "last_attempt":
                return None
            return value
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None

    # Compatibility for the original inventory-only read model and its
    # consumers. New code should use the joined Registry projection.
    def _load_snapshot(self) -> Optional[Dict[str, Any]]:
        return self._sources().get("inventory")

    def _safe_representation(self, relative: Any) -> Optional[str]:
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
            return None
        path = self.project_root / relative
        for top_level in _ALLOWED_TOP_LEVELS:
            if _safe_regular_file(path, self.project_root / top_level) is not None:
                return relative
        return relative if _safe_regular_file(path, self.project_root / "state" / "pm-loop") is not None else None

    def _load_manifest_entries(self) -> tuple[list[Dict[str, Any]], Optional[Dict[str, Any]]]:
        index_path = _safe_regular_file(self._manifest_index_path(), self.registry_root)
        if index_path is None:
            return [], None
        try:
            index = json.loads(index_path.read_text(encoding="utf-8"))
            entries = index.get("entries") if isinstance(index, Mapping) else None
            if not isinstance(index, Mapping) or index.get("schema_version") != MANIFEST_INDEX_SCHEMA or not isinstance(entries, list):
                raise ValueError("invalid_manifest_index")
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            return [], None

        records: list[Dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            artifact_id = str(entry.get("artifact_id") or "")
            relative_manifest = entry.get("manifest_path")
            if not ARTIFACT_ID_RE.fullmatch(artifact_id) or not isinstance(relative_manifest, str) or Path(relative_manifest).is_absolute():
                continue
            manifest_path = self.project_root / relative_manifest
            if _safe_regular_file(manifest_path, self.manifest_root) is None:
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if not isinstance(manifest, Mapping) or manifest.get("schema_version") != MANIFEST_SCHEMA:
                continue
            if any(manifest.get(key) != entry.get(key) for key in ("artifact_id", "schedule_key", "occurrence_id", "content_hash")):
                continue
            representations = manifest.get("representations") if isinstance(manifest.get("representations"), Mapping) else {}
            evidence = manifest.get("evidence") if isinstance(manifest.get("evidence"), Mapping) else {}
            failure = manifest.get("failure") if isinstance(manifest.get("failure"), Mapping) else {}
            open_paths: Dict[str, str] = {}
            for field, kind in MANIFEST_REPRESENTATIONS.items():
                safe = self._safe_representation(representations.get(field))
                if safe is not None:
                    open_paths[kind] = safe
            records.append({
                "artifact_id": artifact_id,
                "title": str(manifest.get("title") or "未命名产物"),
                "artifact_type": str(manifest.get("artifact_type") or "unknown"),
                "artifact_type_basis": "worker_manifest",
                "artifact_domain": str(manifest.get("domain") or "unknown"),
                "status": str(manifest.get("status") or "unknown"),
                "generated_at": manifest.get("generated_at"),
                "generated_at_status": "recorded" if manifest.get("generated_at") else "not_recorded",
                "observed_at": entry.get("registered_at"),
                "content_hash": manifest.get("content_hash"),
                "supersedes": manifest.get("supersedes"),
                "first_seen_artifact_id": None,
                "visibility": "local_private",
                "relation_state": "explicit" if any((manifest.get("related") or {}).values()) else "unknown",
                "related": _related(manifest.get("related")),
                "evidence_grade": str(evidence.get("grade") or "unknown"),
                "freshness": str(evidence.get("freshness") or "unknown"),
                "visibility_gap": str(evidence.get("visibility_gap") or "unknown"),
                "evidence_artifact_count": evidence.get("artifact_count"),
                "failure_class": str(failure.get("class") or "none"),
                "failure_reason": str(failure.get("reason") or ""),
                "failure_safe_statement": str(failure.get("safe_statement") or "未记录"),
                "retention_class": str(manifest.get("retention_class") or "R5_unknown"),
                "source_kind": "worker_manifest",
                "producer": manifest.get("producer"),
                "schedule_key": manifest.get("schedule_key"),
                "occurrence_id": manifest.get("occurrence_id"),
                "run_count": len(entry.get("runs") or []),
                "size_bytes": None,
                "relative_path": None,
                "open_paths": open_paths,
            })
        return records, dict(index)

    def _sources(self) -> Dict[str, Any]:
        signature = (
            self._signature(self._pointer_path(), self.inventory_root),
            self._signature(self._last_attempt_path(), self.inventory_root),
            self._signature(self._manifest_index_path(), self.registry_root),
        )
        with self._lock:
            if signature == self._cached_signature and self._cached_sources is not None:
                return self._cached_sources
            inventory = self._load_inventory()
            last_attempt = self._load_last_attempt()
            manifests, index = self._load_manifest_entries()
            self._cached_signature = signature
            self._cached_sources = {"inventory": inventory, "last_attempt": last_attempt, "manifests": manifests, "manifest_index": index}
            return self._cached_sources

    @staticmethod
    def _inventory_record(item: Mapping[str, Any]) -> Dict[str, Any]:
        relative = str(item.get("relative_path") or "")
        suffix = Path(relative).suffix.lower()
        kind = OPEN_KINDS.get(suffix)
        return {
            "artifact_id": item.get("artifact_id"),
            "title": Path(relative).name or "未命名产物",
            "relative_path": relative,
            "artifact_type": item.get("artifact_type") or "unknown",
            "artifact_type_basis": item.get("artifact_type_basis") or "unknown",
            "artifact_domain": item.get("artifact_domain") or "unknown",
            "status": item.get("status") or "unknown",
            "observed_at": item.get("observed_at"),
            "size_bytes": item.get("size_bytes"),
            "content_hash": item.get("content_hash"),
            "supersedes": item.get("supersedes"),
            "first_seen_artifact_id": item.get("first_seen_artifact_id"),
            "visibility": "local_private",
            "relation_state": item.get("relation_state") or "unknown",
            "related": {"customers": [], "requirements": [], "tasks": [], "versions": [], "decisions": []},
            "evidence_grade": "unknown",
            "freshness": "unknown",
            "visibility_gap": "unknown",
            "evidence_artifact_count": None,
            "failure_class": "none",
            "failure_reason": "",
            "failure_safe_statement": "未记录",
            "retention_class": item.get("retention_class") or "R5_unknown",
            "generated_at": None,
            "generated_at_status": "not_recorded",
            "source_kind": "legacy_inventory",
            "producer": None,
            "schedule_key": None,
            "occurrence_id": None,
            "run_count": 0,
            "open_paths": {kind: relative} if kind else {},
        }

    @staticmethod
    def _source_version(inventory: Optional[Mapping[str, Any]], index: Optional[Mapping[str, Any]]) -> str:
        seed = {
            "inventory": (inventory or {}).get("artifact_registry_hash") or (inventory or {}).get("inventory_hash") or "not_recorded",
            "manifest_index": hashlib.sha256(json.dumps(index or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
        }
        return "sha256:" + hashlib.sha256(json.dumps(seed, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

    @staticmethod
    def _attention(item: Mapping[str, Any]) -> tuple[str, int]:
        status = str(item.get("status") or "unknown")
        if status in {"failed", "partial", "expired"} or str(item.get("failure_class") or "none") not in {"", "none", "unknown"}:
            return "needs_attention", 0
        if str(item.get("evidence_grade") or "unknown") == "unknown" or str(item.get("relation_state") or "unknown") == "unknown":
            return "needs_evidence", 1
        if item.get("generated_at") is None:
            return "generated_at_not_recorded", 2
        return "available", 3

    @staticmethod
    def _sort_key(item: Mapping[str, Any]) -> tuple[int, int, float, str]:
        stamped = _parse_time(item.get("generated_at") or item.get("observed_at"))
        return (
            int(item.get("attention_rank") or 0),
            _DISPLAY_TYPE_PRIORITY.get(str(item.get("artifact_type") or "unknown"), 9),
            -stamped.timestamp() if stamped is not None else 0.0,
            str(item.get("title") or ""),
        )

    def _records(self) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
        sources = self._sources()
        inventory = sources["inventory"]
        last_attempt = sources["last_attempt"]
        manifests = sources["manifests"]
        rows: Dict[str, Dict[str, Any]] = {}
        if isinstance(inventory, Mapping):
            legacy_records = [self._inventory_record(item) for item in inventory.get("artifacts") or [] if isinstance(item, Mapping) and item.get("artifact_id")]
            # A same-directory/same-stem MD, HTML and PDF set is a safe
            # filesystem-level representation candidate, not a semantic title
            # relation. Keep the inferred basis visible and do not derive any
            # business associations from it.
            paired: Dict[str, list[Dict[str, Any]]] = {}
            unpaired: list[Dict[str, Any]] = []
            for item in legacy_records:
                relative = str(item.get("relative_path") or "")
                suffix = Path(relative).suffix.lower()
                if suffix in {".md", ".mdx", ".html", ".htm", ".pdf"}:
                    path = Path(relative)
                    paired.setdefault((path.parent / path.stem).as_posix(), []).append(item)
                else:
                    unpaired.append(item)
            for key, candidates in paired.items():
                kinds = {next(iter(item.get("open_paths") or {}), None) for item in candidates}
                if len(candidates) < 2 or len(kinds - {None}) < 2:
                    unpaired.extend(candidates)
                    continue
                preferred = next((item for item in candidates if "html" in (item.get("open_paths") or {})), candidates[0])
                combined_paths: Dict[str, str] = {}
                for item in candidates:
                    combined_paths.update(item.get("open_paths") or {})
                combined = dict(preferred)
                combined.update({
                    "title": Path(key).name or "未命名产物",
                    "relative_path": key,
                    "artifact_type_basis": "same_parent_stem_inferred_representation_group",
                    "source_kind": "legacy_inventory_inferred_representation_group",
                    "open_paths": combined_paths,
                })
                rows[str(combined["artifact_id"])] = combined
            for item in unpaired:
                rows[str(item["artifact_id"])] = item
        # A Manifest is the current truth for its generated artifact and must
        # be visible immediately, without waiting for the next root scan.
        for item in manifests:
            rows[str(item["artifact_id"])] = item
        values = list(rows.values())
        for item in values:
            attention, rank = self._attention(item)
            item["attention_state"] = attention
            item["attention_rank"] = rank
        values.sort(key=self._sort_key)
        return values, sources

    @staticmethod
    def _public_item(item: Mapping[str, Any]) -> Dict[str, Any]:
        paths = item.get("open_paths") if isinstance(item.get("open_paths"), Mapping) else {}
        representations = [
            {"kind": kind, "label": OPEN_LABELS.get(kind, f"打开 {kind}"), "url": f"/artifacts/registry/{item.get('artifact_id')}/{kind}"}
            for kind in ("html", "markdown", "pdf", "json", "text")
            if paths.get(kind)
        ]
        first = representations[0] if representations else None
        return {
            key: item.get(key)
            for key in (
                "artifact_id", "title", "relative_path", "artifact_type", "artifact_type_basis", "artifact_domain", "status",
                "observed_at", "size_bytes", "content_hash", "supersedes", "first_seen_artifact_id", "visibility",
                "relation_state", "generated_at", "generated_at_status", "source_kind", "producer", "schedule_key",
                "occurrence_id", "run_count",
                "related", "evidence_grade", "freshness", "visibility_gap", "evidence_artifact_count",
                "failure_class", "failure_reason", "failure_safe_statement", "retention_class", "attention_state",
            )
        } | {
            "open_representations": representations,
            "open_kind": first["kind"] if first else None,
            "open_url": first["url"] if first else None,
        }

    def summary(self) -> Dict[str, Any]:
        records, sources = self._records()
        inventory = sources["inventory"]
        last_attempt = sources["last_attempt"]
        index = sources["manifest_index"]
        inventory_status = "observed" if isinstance(inventory, Mapping) else "not_recorded"
        manifest_status = "observed" if isinstance(index, Mapping) else "not_recorded"
        source_status = "observed" if inventory_status == "observed" or manifest_status == "observed" else "not_recorded"
        return {
            "schema_version": SCHEMA,
            "read_only": True,
            "read_at": _now(),
            "source_status": source_status,
            "source_version": self._source_version(inventory, index),
            "inventory_source_status": inventory_status,
            "manifest_source_status": manifest_status,
            "inventory_hash": (inventory or {}).get("inventory_hash"),
            "artifact_registry_hash": (inventory or {}).get("artifact_registry_hash"),
            "observed_at": (inventory or {}).get("observed_at"),
            "completeness": (inventory or {}).get("completeness") or {},
            "summary": (inventory or {}).get("summary") or {},
            "scan_policy": (inventory or {}).get("scan_policy") or {},
            "last_attempt": {
                "observed_at": (last_attempt or {}).get("observed_at"),
                "scan_status": (last_attempt or {}).get("scan_status") or ("not_recorded" if last_attempt is None else "unknown"),
                "scan_duration_seconds": (last_attempt or {}).get("scan_duration_seconds"),
                "inventory_complete": ((last_attempt or {}).get("completeness") or {}).get("inventory_complete"),
            },
            "manifest_summary": {"entry_count": len(sources["manifests"]), "index_updated_at": (index or {}).get("updated_at")},
            "artifact_count": len(records),
            "visibility": "local_private",
            "note": "Root Inventory 是历史审计层；Worker Manifest Index 是实时登记层。两者只展示受控元数据，不读取正文。",
        }

    def list_artifacts(
        self,
        *,
        cursor: int = 0,
        limit: int = 50,
        search: str = "",
        artifact_domain: str = "",
        artifact_type: str = "",
        status: str = "",
        source_kind: str = "",
        time_scope: str = "",
    ) -> Dict[str, Any]:
        rows, _ = self._records()
        query = str(search or "").strip().lower()[:MAX_SEARCH_LENGTH]

        def time_matches(item: Mapping[str, Any]) -> bool:
            if time_scope == "undated":
                return item.get("generated_at") is None
            if time_scope not in {"recent_7d", "recent_30d"}:
                return True
            stamped = _parse_time(item.get("generated_at"))
            if stamped is None:
                return False
            days = 7 if time_scope == "recent_7d" else 30
            return stamped >= datetime.now(timezone.utc) - timedelta(days=days)

        filtered = [
            item for item in rows
            if (not query or query in " ".join(str(item.get(key) or "") for key in ("title", "relative_path", "producer", "schedule_key")).lower())
            and (not artifact_domain or str(item.get("artifact_domain") or "") == artifact_domain)
            and (not artifact_type or str(item.get("artifact_type") or "") == artifact_type)
            and (not status or str(item.get("status") or "") == status)
            and (not source_kind or str(item.get("source_kind") or "") == source_kind)
            and time_matches(item)
        ]
        start = max(0, int(cursor or 0))
        bounded = max(1, min(int(limit or 50), MAX_PAGE_SIZE))
        page = filtered[start : start + bounded]
        next_cursor = start + bounded if start + bounded < len(filtered) else None
        return {
            **self.summary(),
            "items": [self._public_item(item) for item in page],
            "total": len(filtered),
            "next_cursor": next_cursor,
            "page_limit": bounded,
            "filters": {
                "search": query or None,
                "artifact_domain": artifact_domain or None,
                "artifact_type": artifact_type or None,
                "status": status or None,
                "source_kind": source_kind or None,
                "time_scope": time_scope or None,
            },
        }

    def facets(self) -> Dict[str, Any]:
        rows, _ = self._records()

        def counts(key: str) -> list[Dict[str, Any]]:
            values: Dict[str, int] = {}
            for item in rows:
                value = str(item.get(key) or "unknown")
                values[value] = values.get(value, 0) + 1
            return [{"value": value, "count": values[value]} for value in sorted(values)]

        return {**self.summary(), "artifact_domains": counts("artifact_domain"), "artifact_types": counts("artifact_type"), "statuses": counts("status"), "source_kinds": counts("source_kind")}

    def detail(self, artifact_id: str) -> Dict[str, Any]:
        if not ARTIFACT_ID_RE.fullmatch(str(artifact_id or "")):
            raise KeyError(artifact_id)
        rows, _ = self._records()
        by_id = {str(item.get("artifact_id")): item for item in rows}
        for item in rows:
            if item.get("artifact_id") == artifact_id:
                versions: list[Dict[str, Any]] = []
                current = item
                visited: set[str] = set()
                while current is not None and str(current.get("artifact_id")) not in visited and len(versions) < 20:
                    current_id = str(current.get("artifact_id"))
                    visited.add(current_id)
                    versions.append(self._public_item(current))
                    parent = current.get("supersedes")
                    current = by_id.get(str(parent)) if parent else None
                descendants = [self._public_item(candidate) for candidate in rows if candidate.get("supersedes") == artifact_id]
                attention = str(item.get("attention_state") or "available")
                prompt = (
                    f"请在本机私有 PM Loop 中只读核对产物 {item.get('title') or artifact_id}。"
                    f"当前状态：{item.get('status') or 'unknown'}；关注项：{attention}。"
                    "请检查 Manifest、Run、checkpoint 与受控表示，区分数据事实、未知项和下一步建议；"
                    "不要发布、上传、修改权限或重跑任务。"
                )
                return {
                    **self.summary(),
                    "artifact": self._public_item(item),
                    "version_chain": {"ancestors_including_current": versions, "direct_successors": descendants},
                    "codex_advice": {"available": True, "prompt": prompt},
                }
        raise KeyError(artifact_id)

    def open_path(self, artifact_id: str, kind: str) -> Optional[Path]:
        if kind not in set(OPEN_KINDS.values()):
            return None
        if not ARTIFACT_ID_RE.fullmatch(str(artifact_id or "")):
            return None
        rows, _ = self._records()
        record = next((item for item in rows if item.get("artifact_id") == artifact_id), None)
        if record is None:
            return None
        relative = (record.get("open_paths") or {}).get(kind)
        if not isinstance(relative, str):
            return None
        path = self.project_root / relative
        # Every open revalidates root containment, file type and every link.
        for root_name in _ALLOWED_TOP_LEVELS:
            safe = _safe_regular_file(path, self.project_root / root_name)
            if safe is not None:
                return safe
        return _safe_regular_file(path, self.project_root / "state" / "pm-loop")


__all__ = ["ArtifactRegistryReadModel", "SCHEMA"]
