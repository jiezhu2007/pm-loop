#!/usr/bin/env python3
"""Persistence primitives for the Concept Learning Loop.

The control plane deliberately keeps candidates, usage events, and discovery
results outside the active concept ledger.  This module is stdlib-only so the
local HTTP service and one-shot workers can use the same files without a
database daemon.
"""

from __future__ import annotations

import difflib
import fcntl
import hashlib
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional


CANDIDATE_SCHEMA = "concept-learning.candidate.v1"
USAGE_SCHEMA = "concept-learning.usage.v1"
DISCOVERY_SCHEMA = "concept-learning.discovery.v1"
_MISSING_CANDIDATE = object()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _atomic_json_create(path: Path, value: Dict[str, Any]) -> bool:
    """Create a complete JSON file exactly once without replacing a winner."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            # The hard link publishes only the fully-written temp file and
            # fails atomically when another process has already published it.
            os.link(tmp, path)
        except FileExistsError:
            return False
        return True
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _read_json(path: Path, default: Any) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return value


def content_hash(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def _normalized_strings(values: Iterable[Any]) -> List[str]:
    if isinstance(values, str):
        values = [values]
    normalized = {str(value).strip() for value in values if value is not None and str(value).strip()}
    return sorted(normalized)


def _normalized_revisions(
    evidence_revisions: Optional[Mapping[str, Any]],
    updated_uris: Iterable[Any],
) -> Dict[str, str]:
    if not evidence_revisions:
        return {}
    allowed = set(_normalized_strings(updated_uris))
    return {
        str(uri).strip(): str(revision).strip()
        for uri, revision in sorted(evidence_revisions.items(), key=lambda item: str(item[0]))
        if str(uri).strip() in allowed and revision is not None and str(revision).strip()
    }


def discovery_input_fingerprint(
    source: str,
    updated_uris: Iterable[Any],
    unmatched_uris: Iterable[Any],
    evidence_revisions: Optional[Mapping[str, Any]] = None,
) -> str:
    """Hash the semantically relevant Discovery input, independent of order."""
    normalized_uris = _normalized_strings(updated_uris)
    revisions = _normalized_revisions(evidence_revisions, normalized_uris)
    payload = {
        "schema_version": "concept-learning.discovery-input.v2" if revisions else "concept-learning.discovery-input.v1",
        "source": str(source).strip(),
        "updated_uris": normalized_uris,
        "unmatched_uris": _normalized_strings(unmatched_uris),
    }
    if revisions:
        payload["evidence_revisions"] = revisions
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return content_hash(canonical)


def _stored_discovery_fingerprint(run: Dict[str, Any]) -> Optional[str]:
    value = run.get("input_fingerprint")
    if isinstance(value, str) and value:
        return value
    if "source" not in run or not isinstance(run.get("updated_uris"), list) or not isinstance(run.get("unmatched_uris"), list):
        return None
    revisions = run.get("evidence_revisions")
    return discovery_input_fingerprint(
        str(run.get("source") or ""),
        run["updated_uris"],
        run["unmatched_uris"],
        revisions if isinstance(revisions, dict) else None,
    )


def _legacy_discovery_fingerprint(run: Dict[str, Any]) -> Optional[str]:
    if "source" not in run or not isinstance(run.get("updated_uris"), list) or not isinstance(run.get("unmatched_uris"), list):
        return None
    return discovery_input_fingerprint(str(run.get("source") or ""), run["updated_uris"], run["unmatched_uris"])


def unified_diff(before: str, after: str, limit: int = 12000) -> List[Dict[str, str]]:
    """Return a compact, UI-friendly section diff without inventing semantics."""
    if before == after:
        return []
    lines = list(difflib.unified_diff(before.splitlines(), after.splitlines(), lineterm=""))
    text = "\n".join(lines)[:limit]
    return [{"section": "markdown", "change": text}]


class ConceptLearningStore:
    """File-backed candidate, usage, and discovery store for one skill root."""

    def __init__(self, skill_root: Path) -> None:
        self.skill_root = skill_root.expanduser().resolve()
        self.state_root = self.skill_root / "state"
        self.candidates_root = self.state_root / "candidates"
        self.content_root = self.candidates_root / "content"
        self.usage_root = self.state_root / "usage"
        self.discovery_root = self.state_root / "discovery"

    @property
    def ledger_path(self) -> Path:
        return self.state_root / "concepts-ledger.json"

    def load_ledger(self) -> Dict[str, Any]:
        value = _read_json(self.ledger_path, {})
        return value if isinstance(value, dict) else {}

    def save_ledger(self, ledger: Dict[str, Any]) -> None:
        _atomic_json(self.ledger_path, ledger)

    @contextmanager
    def concept_lock(self, concept: str) -> Iterator[None]:
        """Serialize proposal and review transitions for one concept."""
        lock_root = self.candidates_root / ".locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        lock_name = hashlib.sha256(concept.encode("utf-8")).hexdigest() + ".lock"
        with (lock_root / lock_name).open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def candidate_lock(self, candidate_id: str) -> Iterator[None]:
        """Serialize read-modify-write transitions for one Candidate file."""
        lock_root = self.candidates_root / ".candidate-locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        lock_name = hashlib.sha256(candidate_id.encode("utf-8")).hexdigest() + ".lock"
        with (lock_root / lock_name).open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def ledger_lock(self) -> Iterator[None]:
        """Serialize whole-ledger transactions across different concepts."""
        lock_path = self.state_root / ".concepts-ledger.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def discovery_transaction(self) -> Iterator[None]:
        """Serialize discovery scan, evidence filtering, and run creation."""
        lock_path = self.state_root / ".discovery.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def candidate_path(self, candidate_id: str) -> Path:
        return self.candidates_root / f"{candidate_id}.json"

    def content_path(self, candidate_id: str) -> Path:
        return self.content_root / f"{candidate_id}.md"

    def candidate_content_hash(self, candidate: Dict[str, Any]) -> str:
        candidate_id = str(candidate.get("candidate_id") or "")
        path = Path(str(candidate.get("content_path") or self.content_path(candidate_id))).expanduser()
        if not path.is_absolute():
            path = self.skill_root / path
        if not path.is_file():
            raise FileNotFoundError(f"candidate content missing: {path}")
        return content_hash(path.read_text(encoding="utf-8"))

    def read_candidate(self, candidate_id: str) -> Dict[str, Any]:
        path = self.candidate_path(candidate_id)
        value = _read_json(path, None)
        if not isinstance(value, dict):
            raise FileNotFoundError(f"unknown candidate: {candidate_id}")
        return value

    def list_candidates(self, status: Optional[str] = None, concept: Optional[str] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not self.candidates_root.is_dir():
            return rows
        for path in sorted(self.candidates_root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            value = _read_json(path, None)
            if not isinstance(value, dict) or value.get("schema_version") != CANDIDATE_SCHEMA:
                continue
            if status and value.get("status") != status:
                continue
            if concept and value.get("concept") != concept:
                continue
            value = dict(value)
            content_path = Path(str(value.get("content_path") or self.content_path(str(value.get("candidate_id")))))
            value["content_available"] = content_path.is_file()
            rows.append(value)
        return rows

    def save_candidate(self, candidate: Dict[str, Any], content: Optional[str] = None) -> Dict[str, Any]:
        value = dict(candidate)
        value.setdefault("schema_version", CANDIDATE_SCHEMA)
        value.setdefault("candidate_id", "cand-" + uuid.uuid4().hex[:12])
        value.setdefault("created_at", now_iso())
        value.setdefault("updated_at", value["created_at"])
        value.setdefault("status", "ready_for_review")
        if content is not None:
            path = self.content_path(str(value["candidate_id"]))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            value["content_path"] = str(path)
            value["content_hash"] = content_hash(content)
        elif value.get("content_path"):
            value["content_path"] = str(Path(str(value["content_path"])).expanduser())
        _atomic_json(self.candidate_path(str(value["candidate_id"])), value)
        return value

    def update_candidate(
        self,
        candidate_id: str,
        *,
        expected_statuses: Optional[Iterable[str]] = None,
        **updates: Any,
    ) -> Dict[str, Any]:
        """Atomically update a Candidate, optionally as a status CAS."""
        expected = None
        if expected_statuses is not None:
            expected = {str(status) for status in expected_statuses}
        with self.candidate_lock(candidate_id):
            value = self.read_candidate(candidate_id)
            current_status = str(value.get("status") or "")
            if expected is not None and current_status not in expected:
                raise ValueError(
                    f"candidate status conflict: expected {sorted(expected)}, got {current_status}"
                )
            value.update(updates)
            value["updated_at"] = now_iso()
            _atomic_json(self.candidate_path(candidate_id), value)
            return value

    def update_candidate_cas(
        self,
        candidate_id: str,
        *,
        expected_file_sha256: str,
        expected_statuses: Optional[Iterable[str]] = None,
        **updates: Any,
    ) -> Dict[str, Any]:
        """Atomically update a Candidate after a whole-file hash check.

        Callers that need both a status guard and a review-time snapshot guard
        should use this method instead of nesting ``candidate_lock`` around
        ``update_candidate``.  The latter opens the same flock twice and can
        block indefinitely on macOS.
        """

        expected = None
        if expected_statuses is not None:
            expected = {str(status) for status in expected_statuses}
        path = self.candidate_path(candidate_id)
        with self.candidate_lock(candidate_id):
            if not path.is_file():
                raise FileNotFoundError(f"unknown candidate: {candidate_id}")
            actual_sha256 = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
            if actual_sha256 != str(expected_file_sha256):
                raise ValueError("candidate changed after planning")
            value = self.read_candidate(candidate_id)
            current_status = str(value.get("status") or "")
            if expected is not None and current_status not in expected:
                raise ValueError(
                    f"candidate status conflict: expected {sorted(expected)}, got {current_status}"
                )
            value.update(updates)
            value["updated_at"] = now_iso()
            _atomic_json(path, value)
            return value

    def candidate_for_concept(self, concept: str, include_terminal: bool = False) -> Optional[Dict[str, Any]]:
        terminal = {"published", "rejected", "superseded", "failed"}
        for candidate in self.list_candidates(concept=concept):
            if include_terminal or candidate.get("status") not in terminal:
                return candidate
        return None

    def append_usage(self, event: Dict[str, Any]) -> Dict[str, Any]:
        value = {
            "schema_version": USAGE_SCHEMA,
            "event": str(event.get("event") or "concept.used"),
            "concept": str(event.get("concept") or ""),
            "run_id": event.get("run_id"),
            "query_hash": event.get("query_hash"),
            "confidence": event.get("confidence"),
            "user_override": bool(event.get("user_override", False)),
            "feedback": event.get("feedback"),
            "ts": str(event.get("ts") or now_iso()),
        }
        if not value["concept"]:
            raise ValueError("concept is required")
        path = self.usage_root / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return value

    def usage_events(self, concept: Optional[str] = None) -> List[Dict[str, Any]]:
        path = self.usage_root / "events.jsonl"
        if not path.is_file():
            return []
        rows: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(value, dict):
                continue
            if concept and value.get("concept") != concept:
                continue
            rows.append(value)
        return rows

    def usage_summary(self, concept: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        events = []
        for event in self.usage_events(concept):
            try:
                stamp = datetime.fromisoformat(str(event.get("ts", "")).replace("Z", "+00:00"))
            except ValueError:
                continue
            if stamp >= cutoff:
                events.append(event)
        concepts: Dict[str, Dict[str, Any]] = {}
        for event in events:
            name = str(event.get("concept") or "")
            if not name:
                continue
            row = concepts.setdefault(name, {"hits_30d": 0, "not_found": 0, "overrides_30d": 0, "feedback_count": 0, "avg_confidence": None})
            row["hits_30d"] += 1
            if event.get("event") in {"concept.not_found", "not_found"}:
                row["not_found"] += 1
            if event.get("user_override"):
                row["overrides_30d"] += 1
            if event.get("feedback") is not None:
                row["feedback_count"] += 1
        for name, row in concepts.items():
            values = [float(e["confidence"]) for e in events if e.get("concept") == name and isinstance(e.get("confidence"), (int, float))]
            row["avg_confidence"] = round(sum(values) / len(values), 4) if values else None
            row["override_rate"] = round(row["overrides_30d"] / row["hits_30d"], 4) if row["hits_30d"] else 0
        return {"window_days": days, "events": len(events), "concepts": concepts}

    def append_discovery_run(self, run: Dict[str, Any]) -> Dict[str, Any]:
        value = dict(run)
        value.setdefault("schema_version", DISCOVERY_SCHEMA)
        fingerprint = _stored_discovery_fingerprint(value)
        if fingerprint:
            value["input_fingerprint"] = fingerprint
            for existing in self.discovery_runs():
                if _stored_discovery_fingerprint(existing) == fingerprint:
                    return existing
                # The first revision-aware run upgrades a URI-only legacy row
                # in place. This preserves its triage state without creating a
                # duplicate, while establishing a baseline for future changes.
                if value.get("evidence_revisions") and not existing.get("evidence_revisions"):
                    if _legacy_discovery_fingerprint(existing) == _legacy_discovery_fingerprint(value):
                        upgraded = dict(existing)
                        upgraded["evidence_revisions"] = dict(value["evidence_revisions"])
                        upgraded["input_fingerprint"] = fingerprint
                        run_id = str(upgraded.get("run_id") or "")
                        if run_id:
                            _atomic_json(self.discovery_root / f"{run_id}.json", upgraded)
                            return upgraded
        if not value.get("run_id"):
            value["run_id"] = "discover-" + (fingerprint.removeprefix("sha256:") if fingerprint else uuid.uuid4().hex[:10])
        value.setdefault("created_at", now_iso())
        path = self.discovery_root / f"{value['run_id']}.json"
        if fingerprint:
            if _atomic_json_create(path, value):
                return value
            existing = _read_json(path, None)
            if isinstance(existing, dict) and _stored_discovery_fingerprint(existing) == fingerprint:
                return existing
            raise FileExistsError(f"discovery run id collision: {value['run_id']}")
        _atomic_json(path, value)
        return value

    def discovery_runs(self) -> List[Dict[str, Any]]:
        rows = []
        for path in sorted(self.discovery_root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True) if self.discovery_root.is_dir() else []:
            value = _read_json(path, None)
            if isinstance(value, dict):
                rows.append(value)
        return rows

    def update_discovery_run(self, run_id: str, **updates: Any) -> Dict[str, Any]:
        with self.discovery_transaction():
            path = self.discovery_root / f"{run_id}.json"
            value = _read_json(path, None)
            if not isinstance(value, dict):
                raise FileNotFoundError(f"unknown discovery run: {run_id}")
            value.update(updates)
            value["updated_at"] = now_iso()
            _atomic_json(path, value)
            return value

    def enrich_concept(
        self,
        name: str,
        record: Dict[str, Any],
        *,
        candidate: Any = _MISSING_CANDIDATE,
        usage_summary: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        default_usage = {"hits_30d": 0, "not_found": 0, "overrides_30d": 0, "feedback_count": 0, "avg_confidence": None, "override_rate": 0}
        if usage_summary is None:
            usage = self.usage_summary(name).get("concepts", {}).get(name, default_usage)
        else:
            usage = usage_summary.get(name, default_usage)
        # ``None`` is a meaningful read result for callers that already built
        # a candidate index.  Only the legacy omitted-argument path should
        # scan the candidate directory again.
        if candidate is _MISSING_CANDIDATE:
            candidate = self.candidate_for_concept(name)
        result = dict(record)
        result["usage"] = usage
        result["candidate"] = candidate
        return result


def make_candidate(
    *,
    concept: str,
    kind: str,
    content: str,
    before: str = "",
    base_version: Optional[str] = None,
    source_refs: Optional[Iterable[str]] = None,
    evidence: Optional[Iterable[Dict[str, Any]]] = None,
    reason: Optional[Iterable[str]] = None,
    confidence: Optional[float] = None,
    status: str = "ready_for_review",
    **extra: Any,
) -> Dict[str, Any]:
    source_refs_list = list(source_refs or [])
    evidence_list = list(evidence or [])
    value: Dict[str, Any] = {
        "schema_version": CANDIDATE_SCHEMA,
        "candidate_id": "cand-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6],
        "concept": concept,
        "kind": kind,
        "base_version": base_version or "unversioned",
        "proposed_version": "pending",
        "diff": unified_diff(before, content),
        "evidence": evidence_list,
        "source_refs": source_refs_list,
        "reason": list(reason or []),
        "confidence": confidence,
        "status": status,
        "created_at": now_iso(),
        **extra,
    }
    return value


def discover_from_uris(
    store: ConceptLearningStore,
    uris: Iterable[str],
    source: str = "document_delta",
    evidence_revisions: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Create transparent discovery records for URIs not attributable to an existing concept.

    This intentionally does not invent a concept name from a filename.  It
    records the unclassified evidence as a discovery item; a later Agent or
    human can propose a name after reading the source.
    """
    with store.discovery_transaction():
        return _discover_from_uris_locked(store, uris, source, evidence_revisions)


def _discover_from_uris_locked(
    store: ConceptLearningStore,
    uris: Iterable[str],
    source: str,
    evidence_revisions: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    uri_list = _normalized_strings(uris)
    ledger = store.load_ledger()
    # Membership is provenance based.  A document URI that merely contains an
    # Active concept name (for example ``.../资源队列...`` or ``.../行权限...``)
    # is still an unclassified input and must reach triage.  URI/name
    # substring matching used to silently discard these fragments before the
    # Agent could classify them as an alias/merge of an Active concept.
    known_sources = {
        str(uri).strip()
        for item in ledger.values()
        if isinstance(item, dict)
        for uri in (item.get("sources") or [])
        if str(uri).strip()
    }
    unknown = []
    for uri in uri_list:
        if uri in known_sources:
            continue
        unknown.append(uri)
    revisions = _normalized_revisions(evidence_revisions, uri_list)

    # Preserve exact snapshot idempotency first, including legacy rows whose
    # fingerprint is derived on read.  Evidence-level filtering below handles
    # overlapping, but non-identical, windows such as [A, B] -> [B, C].
    raw_fingerprint = discovery_input_fingerprint(source, uri_list, unknown, revisions)
    existing_runs = store.discovery_runs()
    for existing in existing_runs:
        if _stored_discovery_fingerprint(existing) == raw_fingerprint:
            return existing

    pending_unknown: List[str] = []
    for uri in unknown:
        revision = revisions.get(uri)
        already_recorded = False
        for existing in existing_runs:
            if str(existing.get("source") or "") != str(source):
                continue
            if uri not in _normalized_strings(existing.get("unmatched_uris") or []):
                continue
            existing_revisions = existing.get("evidence_revisions")
            if revision:
                if isinstance(existing_revisions, dict) and str(existing_revisions.get(uri) or "") == revision:
                    already_recorded = True
                    break
            else:
                already_recorded = True
                break
        if not already_recorded:
            pending_unknown.append(uri)

    # Every still-unmatched URI is already represented by an inbox row at the
    # same revision.  Reuse that durable work item instead of creating an empty
    # run merely because the surrounding snapshot changed.
    if unknown and not pending_unknown:
        ranked_existing: List[tuple[int, Dict[str, Any]]] = []
        for existing in existing_runs:
            if str(existing.get("source") or "") != str(source):
                continue
            existing_uris = set(_normalized_strings(existing.get("unmatched_uris") or []))
            existing_revisions = existing.get("evidence_revisions")
            covered = 0
            for uri in unknown:
                if uri not in existing_uris:
                    continue
                revision = revisions.get(uri)
                if revision and (
                    not isinstance(existing_revisions, dict)
                    or str(existing_revisions.get(uri) or "") != revision
                ):
                    continue
                covered += 1
            if covered:
                ranked_existing.append((covered, existing))
        if ranked_existing:
            # discovery_runs() is newest-first, so max() keeps the newest row
            # when two durable work items cover the same number of inputs.
            return max(ranked_existing, key=lambda row: row[0])[1]

    pending_revisions = _normalized_revisions(revisions, pending_unknown)
    fingerprint = discovery_input_fingerprint(source, pending_unknown, pending_unknown, pending_revisions)
    payload: Dict[str, Any] = {
        "source": source,
        "updated_uris": pending_unknown,
        "unmatched_uris": pending_unknown,
        "observed_uris": uri_list,
        "observed_unmatched_uris": unknown,
        "candidate_ids": [],
        "status": "needs_agent_triage",
        "input_fingerprint": fingerprint,
    }
    if pending_revisions:
        payload["evidence_revisions"] = pending_revisions
    run = store.append_discovery_run(
        payload
    )
    return run


__all__ = ["ConceptLearningStore", "make_candidate", "discover_from_uris", "discovery_input_fingerprint", "now_iso", "content_hash"]
