#!/usr/bin/env python3
"""Minimal M2 snapshot, evidence and active-generation coordinator."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from pm_system_store import PMSystemStore, now_iso


def _stable_id(prefix: str, value: str) -> str:
    return f"{prefix}-{hashlib.sha256(value.encode('utf-8')).hexdigest()[:24]}"


class EvidenceGateway:
    def __init__(self, store: PMSystemStore) -> None:
        self.store = store

    def commit_snapshot(
        self,
        *,
        source_id: str,
        source_revision: str,
        content_sha256: str,
        manifest: Mapping[str, Any],
        captured_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not source_id or not source_revision or not content_sha256:
            raise ValueError("source_id, source_revision and content_sha256 are required")
        manifest_value = dict(manifest)
        key = f"{source_id}|{source_revision}|{content_sha256}"
        snapshot_id = _stable_id("snapshot", key)
        timestamp = captured_at or now_iso()
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT snapshot_id,status FROM source_snapshots WHERE source_id=? AND source_revision=? AND content_sha256=?", (source_id, source_revision, content_sha256)).fetchone()
            if existing is not None:
                return {"snapshot_id": existing[0], "status": existing[1], "deduplicated": True}
            connection.execute("INSERT INTO source_snapshots(snapshot_id,source_id,source_revision,content_sha256,manifest_json,status,captured_at,created_at) VALUES(?,?,?,?,?,?,?,?)", (snapshot_id, source_id, source_revision, content_sha256, json.dumps(manifest_value, ensure_ascii=False, separators=(",", ":")), "committed", timestamp, timestamp))
        return {"snapshot_id": snapshot_id, "status": "committed", "deduplicated": False}

    def add_source_item(
        self,
        *,
        snapshot_id: str,
        resource_id: str,
        revision_id: str,
        uri: str,
        content_sha256: str,
        status: str = "verified",
    ) -> Dict[str, Any]:
        if status not in {"verified", "unreadable", "unknown"}:
            raise ValueError("invalid source item status")
        item_id = _stable_id("item", f"{snapshot_id}|{resource_id}|{revision_id}")
        with self.store.transaction() as connection:
            row = connection.execute("SELECT source_item_id,status FROM source_items WHERE snapshot_id=? AND resource_id=? AND revision_id=?", (snapshot_id, resource_id, revision_id)).fetchone()
            if row is not None:
                return {"source_item_id": row[0], "status": row[1], "deduplicated": True}
            if connection.execute("SELECT 1 FROM source_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone() is None:
                raise KeyError(snapshot_id)
            connection.execute("INSERT INTO source_items(source_item_id,snapshot_id,resource_id,revision_id,uri,content_sha256,status,created_at) VALUES(?,?,?,?,?,?,?,?)", (item_id, snapshot_id, resource_id, revision_id, uri, content_sha256, status, now_iso()))
        return {"source_item_id": item_id, "status": status, "deduplicated": False}

    def check_manifest(self, snapshot_id: str) -> Dict[str, Any]:
        with self.store.connect() as connection:
            return self._check_manifest_unlocked(connection, snapshot_id)

    @staticmethod
    def _check_manifest_unlocked(connection: Any, snapshot_id: str) -> Dict[str, Any]:
        row = connection.execute("SELECT manifest_json FROM source_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone()
        if row is None:
            raise KeyError(snapshot_id)
        manifest = json.loads(row[0] or "{}")
        items = connection.execute("SELECT resource_id,revision_id,uri,content_sha256,status FROM source_items WHERE snapshot_id=? ORDER BY resource_id,revision_id", (snapshot_id,)).fetchall()
        expected = manifest.get("items") if isinstance(manifest, Mapping) else None
        expected_items = [dict(item) for item in expected] if isinstance(expected, list) and all(isinstance(item, Mapping) for item in expected) else None
        actual = [dict(item) for item in items]
        mismatches: list[Dict[str, Any]] = []
        if expected_items is not None:
            # Compare the complete identity when the manifest provides it;
            # older manifests that only carried resource_id remain valid but
            # still get duplicate/coverage checks.
            def identity(item: Mapping[str, Any]) -> tuple:
                return tuple(str(item.get(key, "")) for key in ("resource_id", "revision_id", "uri", "content_sha256"))

            actual_identities = [identity(item) for item in actual]
            for index, expected_item in enumerate(expected_items):
                candidates = [item for item in actual if all(key not in expected_item or str(item.get(key, "")) == str(expected_item.get(key, "")) for key in ("resource_id", "revision_id", "uri", "content_sha256"))]
                if len(candidates) != 1:
                    mismatches.append({"expected": dict(expected_item), "matches": len(candidates)})
            if len(set(actual_identities)) != len(actual_identities):
                mismatches.append({"duplicate_actual_items": len(actual_identities) - len(set(actual_identities))})
        expected_count = len(expected_items) if expected_items is not None else None
        return {
            "snapshot_id": snapshot_id,
            "manifest_item_count": expected_count,
            "actual_item_count": len(actual),
            "items": actual,
            "mismatches": mismatches,
            "consistent": expected_items is None or (expected_count == len(actual) and not mismatches),
            "unverified_count": sum(1 for item in actual if item["status"] != "verified"),
        }

    def add_evidence(
        self,
        *,
        snapshot_id: str,
        resource_id: str,
        revision_id: str,
        evidence_role: str,
        excerpt_hash: str,
        verified: bool = True,
        generation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not evidence_role or not excerpt_hash:
            raise ValueError("evidence_role and excerpt_hash are required")
        evidence_id = _stable_id("evidence", f"{snapshot_id}|{resource_id}|{revision_id}|{evidence_role}|{excerpt_hash}")
        with self.store.transaction() as connection:
            if connection.execute("SELECT 1 FROM source_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone() is None:
                raise KeyError(snapshot_id)
            existing = connection.execute("SELECT evidence_id,verified FROM evidence_refs WHERE snapshot_id=? AND resource_id=? AND revision_id=? AND evidence_role=? AND excerpt_hash=?", (snapshot_id, resource_id, revision_id, evidence_role, excerpt_hash)).fetchone()
            if existing is not None:
                return {"evidence_id": existing[0], "verified": bool(existing[1]), "deduplicated": True}
            connection.execute("INSERT INTO evidence_refs(evidence_id,generation_id,snapshot_id,resource_id,revision_id,evidence_role,excerpt_hash,verified,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (evidence_id, generation_id, snapshot_id, resource_id, revision_id, evidence_role, excerpt_hash, int(bool(verified)), now_iso()))
        return {"evidence_id": evidence_id, "verified": bool(verified), "deduplicated": False}

    def stage_generation(self, *, domain: str, generation_hash: str, source_watermark: Optional[str], knowledge_watermark: Optional[str]) -> Dict[str, Any]:
        if not domain or not generation_hash:
            raise ValueError("domain and generation_hash are required")
        generation_id = _stable_id("generation", f"{domain}|{generation_hash}")
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT generation_id,status FROM generations WHERE domain=? AND generation_hash=?", (domain, generation_hash)).fetchone()
            if existing is not None:
                return {"generation_id": existing[0], "status": existing[1], "deduplicated": True}
            connection.execute("INSERT INTO generations(generation_id,domain,generation_hash,status,source_watermark,knowledge_watermark,created_at) VALUES(?,?,?,?,?,?,?)", (generation_id, domain, generation_hash, "staged", source_watermark, knowledge_watermark, now_iso()))
        return {"generation_id": generation_id, "status": "staged", "deduplicated": False}

    def activate_generation(self, generation_id: str) -> Dict[str, Any]:
        timestamp = now_iso()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT domain,status FROM generations WHERE generation_id=?", (generation_id,)).fetchone()
            if row is None:
                raise KeyError(generation_id)
            if row[1] == "active":
                return {"generation_id": generation_id, "status": "active", "deduplicated": True}
            # Only complete, verified evidence may be attached to an active
            # generation.  A staged generation with no evidence must remain
            # inert instead of becoming an apparently valid empty release.
            evidence_rows = connection.execute("SELECT snapshot_id FROM evidence_refs WHERE generation_id=?", (generation_id,)).fetchall()
            if not evidence_rows:
                raise ValueError("generation has no evidence")
            bad = connection.execute("SELECT COUNT(*) FROM evidence_refs WHERE generation_id=? AND verified=0", (generation_id,)).fetchone()[0]
            if bad:
                raise ValueError("generation has unverified evidence")
            for snapshot_row in evidence_rows:
                manifest = self._check_manifest_unlocked(connection, str(snapshot_row[0]))
                if not manifest["consistent"] or manifest["unverified_count"]:
                    raise ValueError(f"snapshot evidence is incomplete: {snapshot_row[0]}")
            connection.execute("UPDATE generations SET status='superseded' WHERE domain=? AND status='active'", (row[0],))
            connection.execute("UPDATE generations SET status='active',active_at=? WHERE generation_id=?", (timestamp, generation_id))
        return {"generation_id": generation_id, "status": "active", "deduplicated": False}

    def record_timeline_event(self, *, event_type: str, idempotency_key: str, payload: Mapping[str, Any], source_run_id: Optional[str] = None, occurred_at: Optional[str] = None) -> Dict[str, Any]:
        event_id = _stable_id("timeline", idempotency_key)
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT timeline_event_id FROM timeline_events WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if existing is not None:
                return {"timeline_event_id": existing[0], "deduplicated": True}
            connection.execute("INSERT INTO timeline_events(timeline_event_id,event_type,idempotency_key,source_run_id,payload_json,occurred_at) VALUES(?,?,?,?,?,?)", (event_id, event_type, idempotency_key, source_run_id, json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")), occurred_at or now_iso()))
        return {"timeline_event_id": event_id, "deduplicated": False}

    def watermarks(self) -> Dict[str, Any]:
        with self.store.connect() as connection:
            def structured(name: str) -> Any:
                row = connection.execute(
                    "SELECT value,state FROM watermarks WHERE watermark_name=? ORDER BY captured_at DESC,sequence DESC,rowid DESC LIMIT 1",
                    (name,),
                ).fetchone()
                if row is not None:
                    # The existence of a structured row suppresses legacy
                    # fallback.  An explicit non-accepted state is evidence
                    # of missing or invalid data, not permission to reuse an
                    # older generation.
                    if str(row[1] or "") != "accepted":
                        return (None, True)
                    try:
                        return (json.loads(row[0]), True)
                    except (TypeError, json.JSONDecodeError):
                        return (row[0], True)
                return (None, False)

            source, source_structured = structured("source")
            content, content_structured = structured("content")
            knowledge, knowledge_structured = structured("knowledge")
            active, active_structured = structured("active_generation")
            # Pre-G2 fixtures have no structured rows.  Keep the read-only
            # compatibility bridge, ordered by captured timestamp rather than
            # TEXT MAX; G2 production reads never depend on it.
            if source is None and not source_structured:
                row = connection.execute("SELECT source_revision FROM source_snapshots WHERE status='committed' ORDER BY captured_at DESC,rowid DESC LIMIT 1").fetchone()
                source = row[0] if row else None
            if content is None and not content_structured:
                row = connection.execute("SELECT captured_at FROM source_snapshots WHERE status='committed' ORDER BY captured_at DESC,rowid DESC LIMIT 1").fetchone()
                content = row[0] if row else None
            if knowledge is None and not knowledge_structured:
                row = connection.execute("SELECT knowledge_watermark FROM generations WHERE status='active' ORDER BY created_at DESC,rowid DESC LIMIT 1").fetchone()
                knowledge = row[0] if row else None
            if active is None and not active_structured:
                row = connection.execute("SELECT active_at FROM generations WHERE status='active' AND active_at IS NOT NULL ORDER BY active_at DESC,rowid DESC LIMIT 1").fetchone()
                active = row[0] if row else None
        return {"source": source, "content": content, "knowledge": knowledge, "active_generation": active}


__all__ = ["EvidenceGateway"]
