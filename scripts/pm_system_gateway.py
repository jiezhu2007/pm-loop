#!/usr/bin/env python3
"""Outbox and provider-throttle primitives for V4.4 S3.

The gateway deliberately stops at a durable dispatch decision.  A caller may
attach its OpenViking client after the decision; retry and provider state are
still recorded here so individual workers cannot create their own retry loop.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pm_system_store import MigrationFrozen, PMSystemStore, now_iso
from concept_v11_schema_v2 import admission_is_live


TRANSIENT_FAILURES = {"transient", "connection", "timeout", "502", "503", "504"}
PERMANENT_FAILURES = {"invalid_resource", "schema", "provider_auth", "unknown", "permanent"}
RATE_LIMIT_FAILURES = {"rate_limit", "429"}
QUARANTINE_FAILURES = {"quarantine", "unsupported", "evidence_missing", "unknown_quarantine"}
RATE_LIMIT_STEPS = (30, 60, 120, 300)
TERMINAL_OUTBOX_STATUSES = frozenset({"completed", "failed", "dead_letter", "quarantine"})
SUPPORTED_OUTBOX_KINDS = frozenset({"resource", "concept"})
SUPPORTED_PROCESSING_MODES = frozenset({"vectors_only", "semantic_and_vectors", "semantic_only"})
CONCEPT_SEMANTIC_MODES = frozenset({"semantic_only", "semantic_and_vectors"})
CONCEPT_MODEL_POLICY_VERSION = "concept-v11-oneapi-auto-v1"
DISPATCH_LANES = frozenset({"all", "fast-vector", "semantic"})
DEFAULT_DISPATCH_LEASE_SECONDS = 300
DEFAULT_RETRY_DEADLINE_SECONDS = 3600


def _normalize_pm_payload(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Keep OpenViking's resource-memory trigger out of the PM write path.

    ``reason`` is not passive metadata in OpenViking: a non-empty value starts
    resource-memory linking through a shared session.  PM audit explanations
    belong in the local RunStore/Manifest, so reject an accidental non-empty
    field and drop empty compatibility values before persisting the outbox.
    The walk is recursive because source adapters may wrap transport options
    in ``metadata`` or list-valued envelopes before they reach the Gateway.
    """

    def sanitize(value: Any, path: str) -> Any:
        if isinstance(value, Mapping):
            cleaned: Dict[str, Any] = {}
            for key, child in value.items():
                key_text = str(key)
                if key_text.lower() == "reason":
                    if child is not None and str(child).strip():
                        raise ValueError(
                            "PM resource submissions must not set OpenViking reason "
                            f"(at {path}.{key_text}); store audit text locally instead"
                        )
                    # Empty/null compatibility fields must not survive into
                    # the serialized request body either.
                    continue
                cleaned[key_text] = sanitize(child, f"{path}.{key_text}")
            return cleaned
        if isinstance(value, list):
            return [sanitize(child, f"{path}[{index}]") for index, child in enumerate(value)]
        if isinstance(value, tuple):
            return [sanitize(child, f"{path}[{index}]") for index, child in enumerate(value)]
        return value

    normalized = sanitize(dict(payload or {}), "payload")
    return normalized if isinstance(normalized, dict) else {}


def provider_key(
    provider: str,
    endpoint: str = "default",
    model: str = "default",
    *,
    operation: Optional[str] = None,
    profile: Optional[str] = None,
) -> str:
    """Build a provider bucket key.

    The first three dimensions remain compatible with the existing global
    provider-token ledger. Optional operation/profile dimensions isolate local
    backoff windows so a semantic 429 does not stall an interactive model Run.
    """
    parts = [str(provider), str(endpoint), str(model)]
    if operation is not None or profile is not None:
        parts.extend([str(operation or "default"), str(profile or "default")])
    return "|".join(parts)


def _parse_key(value: str) -> Tuple[str, str, str]:
    parts = value.split("|")
    return (parts + ["default", "default"])[:3]


def _parse_retry_after(value: Any, *, now: Optional[datetime] = None) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    if re.fullmatch(r"\d+", raw):
        return max(0, int(raw))
    try:
        target = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    return max(0, int((target - reference).total_seconds()))


def _fingerprint(category: str, detail: Optional[str]) -> str:
    source = f"{category}:{detail or ''}".encode("utf-8")
    return hashlib.sha256(source).hexdigest()[:20]


class SemanticGateway:
    def __init__(
        self,
        store: PMSystemStore,
        *,
        max_attempts: int = 3,
        circuit_threshold: int = 3,
        dispatch_lease_seconds: int = DEFAULT_DISPATCH_LEASE_SECONDS,
        retry_deadline_seconds: int = DEFAULT_RETRY_DEADLINE_SECONDS,
    ) -> None:
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if circuit_threshold <= 0:
            raise ValueError("circuit_threshold must be positive")
        if dispatch_lease_seconds <= 0:
            raise ValueError("dispatch_lease_seconds must be positive")
        if retry_deadline_seconds <= 0:
            raise ValueError("retry_deadline_seconds must be positive")
        self.store = store
        self.max_attempts = int(max_attempts)
        self.circuit_threshold = int(circuit_threshold)
        self.dispatch_lease_seconds = int(dispatch_lease_seconds)
        self.retry_deadline_seconds = int(retry_deadline_seconds)

    @staticmethod
    def _concept_admission_error(reason: str) -> RuntimeError:
        return RuntimeError(f"concept admission blocked: {reason}")

    @staticmethod
    def _concept_admission(connection: Any, namespace_epoch: str) -> Optional[Dict[str, Any]]:
        """Read the durable Admission while retaining v2.1 read compatibility."""
        try:
            row = connection.execute(
                "SELECT admission_state,expires_at,renewal_policy FROM concept_admissions WHERE namespace_epoch=?",
                (namespace_epoch,),
            ).fetchone()
            if row is None:
                return None
            return {
                "admission_state": str(row[0] or ""),
                "expires_at": row[1],
                "renewal_policy": str(row[2] or "snapshot_ttl"),
            }
        except sqlite3.OperationalError as exc:
            if "no such column" not in str(exc).lower():
                raise
            row = connection.execute(
                "SELECT admission_state,expires_at FROM concept_admissions WHERE namespace_epoch=?",
                (namespace_epoch,),
            ).fetchone()
            if row is None:
                return None
            return {
                "admission_state": str(row[0] or ""),
                "expires_at": row[1],
                "renewal_policy": "snapshot_ttl",
            }

    @staticmethod
    def _active_concept_policy(connection: Any) -> Dict[str, Any]:
        """Return the one active concept model policy or fail closed.

        Policy is a local control-plane record.  It is intentionally resolved
        inside the same short transaction as concept acceptance so a caller
        cannot race a policy change or smuggle a different model in payload.
        """
        try:
            rows = connection.execute(
                "SELECT policy_version,provider,requested_model,allowed_models_json,policy_hash,status "
                "FROM concept_model_policies WHERE status='active' ORDER BY policy_version"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise RuntimeError("concept admission blocked: model_policy_schema_unavailable") from exc
            raise
        if len(rows) != 1:
            raise RuntimeError(f"concept admission blocked: active_model_policy_not_unique:{len(rows)}")
        row = rows[0]
        policy = {
            "policy_version": str(row[0] or ""),
            "provider": str(row[1] or ""),
            "requested_model": str(row[2] or ""),
            "allowed_models_json": str(row[3] or "[]"),
            "policy_hash": str(row[4] or ""),
            "status": str(row[5] or ""),
        }
        if not policy["policy_version"] or not policy["provider"] or not policy["requested_model"] or not policy["policy_hash"]:
            raise RuntimeError("concept admission blocked: active_model_policy_incomplete")
        if policy["provider"] != "oneapi" or policy["requested_model"] != "auto":
            raise RuntimeError("concept admission blocked: active_model_policy_mismatch")
        try:
            allowed = json.loads(policy["allowed_models_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("concept admission blocked: active_model_policy_invalid_allowlist") from exc
        if not isinstance(allowed, list):
            raise RuntimeError("concept admission blocked: active_model_policy_invalid_allowlist")
        return policy

    @staticmethod
    def _bind_concept_policy(
        connection: Any,
        *,
        body: Dict[str, Any],
        namespace_epoch: str,
        profile: str,
        processing_mode: str,
        provider: str,
        endpoint: str,
        at: str,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Bind concept semantic acceptance to the persisted model policy."""
        policy = SemanticGateway._active_concept_policy(connection)
        if str(provider or "") != policy["provider"]:
            raise RuntimeError(f"concept admission blocked: provider_policy_mismatch:{provider}")
        # These fields are audit metadata, not caller-controlled routing.
        expected = {
            "provider": policy["provider"],
            "model_requested": policy["requested_model"],
            "model_policy_version": policy["policy_version"],
            "policy_version": policy["policy_version"],
            "policy_hash": policy["policy_hash"],
            "profile": profile,
            "namespace_epoch": namespace_epoch,
        }
        for key, value in expected.items():
            supplied = body.get(key)
            if supplied not in (None, "") and str(supplied) != str(value):
                raise RuntimeError(f"concept admission blocked: policy_override:{key}")
            body[key] = value
        body["model_policy_version"] = policy["policy_version"]
        body["policy_bound"] = True
        body["policy_hash"] = policy["policy_hash"]
        # The requested model is deliberately the literal ``auto``.  OneAPI
        # resolves it remotely; the local ledger never guesses the result.
        return policy["provider"], policy["requested_model"], policy

    @staticmethod
    def _bind_vectors_only_concept_policy(
        connection: Any,
        *,
        body: Dict[str, Any],
        namespace_epoch: str,
        profile: str,
        at: str,
    ) -> str:
        """Bind local profile policy for a vectors-only concept projection.

        Vectors-only projection is an OpenViking resource write, not a model
        invocation.  It still needs the profile policy hash so dispatch can
        reject stale or substituted policy.  Do not resolve an active model
        policy, check a capability probe, or reserve semantic capacity here.
        """
        row = connection.execute(
            "SELECT policy_hash FROM concept_profile_admissions "
            "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
            (profile, namespace_epoch),
        ).fetchone()
        policy_hash = str(row[0] or "") if row is not None else ""
        if not policy_hash:
            raise RuntimeError("concept admission blocked: profile_policy_hash_unavailable")
        supplied = body.get("policy_hash")
        if supplied not in (None, "") and str(supplied) != policy_hash:
            raise RuntimeError("concept admission blocked: policy_override:policy_hash")
        body["policy_hash"] = policy_hash
        body["vectors_only_policy_bound"] = True
        body["vectors_only_policy_bound_at"] = at
        return policy_hash

    @staticmethod
    def _concept_capability_ready(
        connection: Any,
        *,
        namespace_epoch: str,
        profile: str,
        processing_mode: str,
        policy_version: str,
        provider: str,
        at: str,
    ) -> bool:
        """Require a fresh accepted-capability probe before semantic dispatch.

        The probe is deliberately read inside the same short admission
        transaction.  It is only a capability check; it never performs a
        network request and never changes probe state.
        """
        row = connection.execute(
            "SELECT 1 FROM concept_capability_probes "
            "WHERE namespace_epoch=? AND profile=? AND processing_mode=? "
            "AND model_policy_version=? AND provider=? "
            "AND capability_state='ready' AND expires_at>? "
            "ORDER BY observed_at DESC LIMIT 1",
            (namespace_epoch, profile, processing_mode, policy_version, provider, at),
        ).fetchone()
        return row is not None

    @staticmethod
    def _release_concept_profile_unlocked(connection: Any, body: Mapping[str, Any], *, at: str) -> bool:
        """Release one concept profile reservation exactly once.

        Reservation state lives in the durable Outbox payload so a late ACK,
        retry callback, or worker restart cannot decrement the same profile
        twice.  This is intentionally a local SQLite update.
        """
        if str(body.get("kind") or "") != "concept" or not body.get("concept_profile_reserved"):
            return False
        profile = str(body.get("profile") or "")
        epoch = str(body.get("namespace_epoch") or "")
        if not profile or not epoch:
            return False
        connection.execute(
            "UPDATE concept_profile_admissions SET pending_count=MAX(0,pending_count-1),updated_at=? "
            "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
            (at, profile, epoch),
        )
        return True

    def release_profile_reservation(self, outbox_id: str) -> bool:
        """Release a terminal concept semantic reservation, idempotently."""
        at = now_iso()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT payload_json FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                return False
            body = json.loads(row[0] or "{}")
            released = self._release_concept_profile_unlocked(connection, body, at=at)
            if released:
                body["concept_profile_reserved"] = False
                connection.execute("UPDATE outbox_items SET payload_json=?,updated_at=? WHERE outbox_id=?", (json.dumps(body, ensure_ascii=False, separators=(",", ":")), at, outbox_id))
            return released

    @staticmethod
    def _new_token(prefix: str) -> str:
        entropy = f"{prefix}:{now_iso()}:{os.getpid()}:{os.urandom(8).hex()}"
        return f"{prefix}-{hashlib.sha256(entropy.encode('utf-8')).hexdigest()[:24]}"

    def _reclaim_expired_dispatch_leases(self, connection: Any, at: str) -> int:
        expired_rows = connection.execute(
            "SELECT outbox_id FROM outbox_dispatch_leases WHERE expires_at <= ?", (at,)
        ).fetchall()
        # ``in_flight`` is committed atomically with its dispatch lease.  A
        # missing lease can therefore only be residue from a prior runtime
        # fault or a legacy partial write, never a currently owned dispatch.
        # Requeue it through the same bounded retry path as an expired lease.
        # Without this branch the row is permanently invisible to dispatch.
        missing_rows = connection.execute(
            "SELECT o.outbox_id FROM outbox_items AS o "
            "LEFT JOIN outbox_dispatch_leases AS l ON l.outbox_id=o.outbox_id "
            "WHERE o.status='in_flight' AND l.outbox_id IS NULL"
        ).fetchall()
        reclaim_rows = [(row, "dispatch_lease_expired") for row in expired_rows]
        reclaim_rows.extend((row, "dispatch_lease_missing") for row in missing_rows)
        reclaimed = 0
        for row, category in reclaim_rows:
            outbox_id = row[0]
            connection.execute(
                "UPDATE outbox_items SET status='retry_wait',next_attempt_at=?,error_fingerprint=?,updated_at=? WHERE outbox_id=? AND status='in_flight'",
                (at, _fingerprint(category, outbox_id), at, outbox_id),
            )
            connection.execute(
                "UPDATE semantic_tasks SET status='queued',updated_at=? WHERE outbox_id=? AND status IN ('queued','in_flight','retry_wait')",
                (at, outbox_id),
            )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,))
            reclaimed += 1
        connection.execute("DELETE FROM provider_probe_leases WHERE expires_at <= ?", (at,))
        return reclaimed

    def _reserve_probe(self, connection: Any, key: str, at: str) -> bool:
        row = connection.execute("SELECT expires_at FROM provider_probe_leases WHERE provider_key=?", (key,)).fetchone()
        if row is not None and str(row[0]) > at:
            return False
        if row is not None:
            connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (key,))
        token = self._new_token("probe")
        expires = (datetime.now(timezone.utc) + timedelta(seconds=self.dispatch_lease_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
        connection.execute("INSERT INTO provider_probe_leases(provider_key,probe_token,leased_at,expires_at) VALUES(?,?,?,?)", (key, token, at, expires))
        return True

    def _ensure_bucket(self, connection: Any, key: str, *, provider: str, endpoint: str, model: str, at: str) -> None:
        connection.execute(
            "INSERT OR IGNORE INTO provider_buckets(provider_key,provider,endpoint,model,updated_at) VALUES(?,?,?,?,?)",
            (key, provider, endpoint, model, at),
        )

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def _retry_deadline(self, created_at: Any, *, now: datetime) -> str:
        created = self._parse_timestamp(created_at) or now
        return self._timestamp(created + timedelta(seconds=self.retry_deadline_seconds))

    def _expire_retry_deadlines_unlocked(self, connection: Any, at: str) -> int:
        """Move overdue pending/retry rows to one durable terminal state."""
        rows = connection.execute(
            "SELECT outbox_id,payload_json FROM outbox_items WHERE status IN ('pending','retry_wait') AND retry_deadline_at IS NOT NULL AND retry_deadline_at<=?",
            (at,),
        ).fetchall()
        for row in rows:
            outbox_id = str(row[0])
            body = json.loads(row[1] or "{}")
            fingerprint = _fingerprint("retry_deadline_exceeded", outbox_id)
            connection.execute(
                "UPDATE outbox_items SET status='dead_letter',next_attempt_at=NULL,error_fingerprint=?,terminal_reason=?,updated_at=? WHERE outbox_id=? AND status IN ('pending','retry_wait')",
                (fingerprint, "retry_deadline_exceeded", at, outbox_id),
            )
            connection.execute(
                "UPDATE semantic_tasks SET status='dead_letter',error_fingerprint=?,terminal_reason=?,updated_at=? WHERE outbox_id=? AND status IN ('queued','in_flight','retry_wait')",
                (fingerprint, "retry_deadline_exceeded", at, outbox_id),
            )
            meta = connection.execute(
                "SELECT resource_id,revision_id FROM outbox_items WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if meta:
                self._mark_projection_failure_unlocked(
                    connection,
                    resource_id=str(meta[0]),
                    revision_id=str(meta[1]),
                    status="dead_letter",
                    reason="retry_deadline_exceeded",
                    at=at,
                )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,))
            provider_key_value = str(body.get("provider_key") or "")
            if provider_key_value:
                connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (provider_key_value,))
            if self._release_concept_profile_unlocked(connection, body, at=at):
                body["concept_profile_reserved"] = False
                connection.execute("UPDATE outbox_items SET payload_json=? WHERE outbox_id=?", (json.dumps(body, ensure_ascii=False, separators=(",", ":")), outbox_id))
        return len(rows)

    @staticmethod
    def _mark_projection_failure_unlocked(
        connection: Any,
        *,
        resource_id: str,
        revision_id: str,
        status: str,
        reason: str,
        at: str,
    ) -> None:
        """Record semantic failure without erasing an independently verified body.

        Phase A content verification is stronger than a later semantic result.
        Once ``content_verified`` is present, retry/dead-letter/quarantine
        transitions may only change the semantic state and terminal reason.
        This keeps the content and semantic watermarks independently truthful.
        """
        existing = connection.execute(
            "SELECT content_state,memory_link_state FROM resource_projections WHERE resource_id=? AND revision_id=?",
            (resource_id, revision_id),
        ).fetchone()
        if existing is not None and str(existing[0]) == "content_verified":
            connection.execute(
                "UPDATE resource_projections SET semantic_state=?,terminal_reason=?,updated_at=? WHERE resource_id=? AND revision_id=?",
                (status, reason, at, resource_id, revision_id),
            )
            return
        connection.execute(
            "INSERT INTO resource_projections(resource_id,revision_id,content_state,semantic_state,memory_link_state,terminal_reason,updated_at) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(resource_id,revision_id) DO UPDATE SET content_state=?,semantic_state=?,memory_link_state=?,terminal_reason=?,updated_at=?",
            (
                resource_id,
                revision_id,
                status,
                status,
                status,
                reason,
                at,
                status,
                status,
                status,
                reason,
                at,
            ),
        )

    def enqueue(
        self,
        *,
        resource_id: str,
        revision_id: str,
        processing_mode: str,
        provider: str,
        profile: str,
        payload: Optional[Mapping[str, Any]] = None,
        endpoint: str = "default",
        model: str = "default",
        retry_deadline_at: Optional[str] = None,
        namespace_epoch: str = "v4",
        owner: str = "pm-system",
        kind: str = "resource",
    ) -> Dict[str, Any]:
        if not resource_id or not revision_id:
            raise ValueError("resource_id and revision_id are required")
        kind = str(kind or "resource").strip().lower()
        if kind not in SUPPORTED_OUTBOX_KINDS:
            raise ValueError("invalid outbox kind")
        if processing_mode not in SUPPORTED_PROCESSING_MODES:
            raise ValueError("invalid processing_mode")
        if processing_mode == "semantic_only" and kind != "concept":
            raise ValueError("semantic_only is reserved for concept projections")
        if not provider or not profile:
            raise ValueError("provider and profile are required")
        namespace_epoch = str(namespace_epoch or "v4")
        dedupe = f"{kind}|{resource_id}|{revision_id}|{processing_mode}|{provider}|{profile}|{namespace_epoch}"
        at_dt = datetime.now(timezone.utc)
        at = self._timestamp(at_dt)
        deadline = self._parse_timestamp(retry_deadline_at)
        if deadline is None:
            deadline_at = self._retry_deadline(at, now=at_dt)
        else:
            if deadline <= at_dt:
                raise ValueError("retry_deadline_at must be in the future")
            deadline_at = self._timestamp(deadline)
        body = _normalize_pm_payload(payload)
        body.update(
            {
                "resource_id": resource_id,
                "revision_id": revision_id,
                "processing_mode": processing_mode,
                "kind": kind,
                # Ordinary callers leave this false.  Explicit wait/strict
                # callers retain their request so the dispatcher can perform
                # a bounded terminal-state check without changing identity.
                "wait": bool(body.get("wait", False) or body.get("strict", False)),
            }
        )
        # Preserve caller-supplied policy fields until the concept binding
        # validates them.  Non-concept work is still normalized immediately.
        if not (kind == "concept" and processing_mode in CONCEPT_SEMANTIC_MODES):
            body.update({"provider": provider, "profile": profile, "namespace_epoch": namespace_epoch})
        key = provider_key(provider, endpoint, model)
        body["provider_key"] = key
        with self.store.transaction() as connection:
            freeze = self.store._freeze_blocks(connection)
            if freeze is not None:
                raise MigrationFrozen(f"outbox admission blocked by migration {freeze[1]}")
            policy: Optional[Dict[str, Any]] = None
            if kind == "concept" and processing_mode in CONCEPT_SEMANTIC_MODES:
                provider, model, policy = self._bind_concept_policy(
                    connection,
                    body=body,
                    namespace_epoch=namespace_epoch,
                    profile=profile,
                    processing_mode=processing_mode,
                    provider=provider,
                    endpoint=endpoint,
                    at=at,
                )
            elif kind == "concept":
                self._bind_vectors_only_concept_policy(
                    connection,
                    body=body,
                    namespace_epoch=namespace_epoch,
                    profile=profile,
                    at=at,
                )
            key = provider_key(provider, endpoint, model)
            body["provider_key"] = key
            existing = connection.execute("SELECT outbox_id,status FROM outbox_items WHERE kind=? AND profile=? AND idempotency_key=? AND namespace_epoch=?", (kind, profile, dedupe, namespace_epoch)).fetchone()
            self._ensure_bucket(connection, key, provider=provider, endpoint=endpoint, model=model, at=at)
            if existing is not None:
                return {"status": "accepted", "deduplicated": True, "outbox_id": existing[0], "outbox_status": existing[1], "idempotency_key": dedupe}
            if kind == "concept":
                try:
                    admission = self._concept_admission(connection, namespace_epoch)
                except sqlite3.OperationalError as exc:
                    if "no such table" in str(exc).lower():
                        raise self._concept_admission_error("schema_unavailable") from exc
                    raise
                if admission is None or str(admission["admission_state"]) not in {"canary", "incremental"}:
                    raise self._concept_admission_error(str(admission["admission_state"] if admission else "uninitialized"))
                if not admission_is_live(admission, at=at):
                    raise self._concept_admission_error("snapshot_expired")
                if processing_mode in CONCEPT_SEMANTIC_MODES:
                    if policy is None:
                        raise self._concept_admission_error("model_policy_missing")
                    if str(admission["admission_state"]) in {"canary", "incremental"}:
                        admission_policy = connection.execute(
                            "SELECT policy_version FROM concept_admissions WHERE namespace_epoch=?",
                            (namespace_epoch,),
                        ).fetchone()
                        if admission_policy is None or str(admission_policy[0] or "") != policy["policy_version"]:
                            raise self._concept_admission_error("admission_policy_mismatch")
                    if not self._concept_capability_ready(
                        connection,
                        namespace_epoch=namespace_epoch,
                        profile=profile,
                        processing_mode=processing_mode,
                        policy_version=policy["policy_version"],
                        provider=policy["provider"],
                        at=at,
                    ):
                        raise self._concept_admission_error(f"capability_not_ready:{processing_mode}")
                    profile_row = connection.execute(
                        "SELECT pending_count,pending_soft_limit,pending_high_water,outbox_hard_cap,pause_fence,throttle_until,policy_hash "
                        "FROM concept_profile_admissions WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
                        (profile, namespace_epoch),
                    ).fetchone()
                    if profile_row is None:
                        raise self._concept_admission_error("profile_uninitialized")
                    if str(profile_row[6] or "") != policy["policy_hash"]:
                        raise self._concept_admission_error("profile_policy_hash_mismatch")
                    if str(profile_row[4]) != "open":
                        raise self._concept_admission_error("pause_fence")
                    if profile_row[5] and str(profile_row[5]) > at:
                        raise self._concept_admission_error("throttle")
                    soft_limit = int(profile_row[1])
                    hard_limit = int(profile_row[3])
                    pending = int(profile_row[0])
                    if pending >= hard_limit:
                        raise self._concept_admission_error("hard_cap")
                    if pending >= soft_limit:
                        raise self._concept_admission_error("soft_limit")
                    body["concept_profile_reserved"] = True
            outbox_id = f"outbox-{hashlib.sha256(dedupe.encode('utf-8')).hexdigest()[:24]}"
            if kind == "concept" and body.get("concept_profile_reserved"):
                connection.execute(
                    "UPDATE concept_profile_admissions SET pending_count=?,pending_high_water=MAX(pending_high_water,?),updated_at=? "
                    "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
                    (int(profile_row[0]) + 1, int(profile_row[0]) + 1, at, profile, namespace_epoch),
                )
            connection.execute(
                "INSERT INTO outbox_items(outbox_id,idempotency_key,kind,resource_id,revision_id,processing_mode,provider,profile,owner,namespace_epoch,payload_json,status,retry_deadline_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (outbox_id, dedupe, kind, resource_id, revision_id, processing_mode, provider, profile, owner, namespace_epoch, json.dumps(body, ensure_ascii=False, separators=(",", ":")), "pending", deadline_at, at, at),
            )
            connection.execute(
                "INSERT INTO resource_projections(resource_id,revision_id,content_state,semantic_state,memory_link_state,updated_at) VALUES(?,?,?, ?, ?, ?) ON CONFLICT(resource_id,revision_id) DO UPDATE SET updated_at=excluded.updated_at",
                (resource_id, revision_id, "content_pending", "semantic_pending", "memory_link_pending", at),
            )
        return {"status": "accepted", "deduplicated": False, "outbox_id": outbox_id, "outbox_status": "pending", "idempotency_key": dedupe}

    def repair_vectors_only_concept_policy(self, outbox_ids: Sequence[str]) -> List[Dict[str, Any]]:
        """Backfill the local policy binding for already accepted Canary work.

        This intentionally has a narrow eligibility boundary.  It is only for
        durable vectors-only concept rows accepted before that binding existed;
        it cannot enqueue, dispatch, reserve capacity, or change concept facts.
        """
        requested = [str(value) for value in outbox_ids if str(value)]
        if not requested:
            return []
        if len(set(requested)) != len(requested):
            raise ValueError("outbox_ids must be unique")
        at = now_iso()
        repaired: List[Dict[str, Any]] = []
        with self.store.transaction() as connection:
            placeholders = ",".join("?" for _ in requested)
            rows = connection.execute(
                "SELECT outbox_id,kind,processing_mode,profile,namespace_epoch,payload_json,status "
                f"FROM outbox_items WHERE outbox_id IN ({placeholders}) ORDER BY outbox_id",
                tuple(requested),
            ).fetchall()
            if len(rows) != len(requested):
                found = {str(row[0]) for row in rows}
                missing = sorted(set(requested) - found)
                raise ValueError(f"outbox_missing:{','.join(missing)}")
            for row in rows:
                outbox_id, kind, processing_mode, profile, epoch, payload_json, status = row
                if str(kind) != "concept" or str(processing_mode) != "vectors_only":
                    raise ValueError(f"outbox_not_vectors_only_concept:{outbox_id}")
                if str(status) not in {"pending", "retry_wait"}:
                    raise ValueError(f"outbox_not_repairable:{outbox_id}:{status}")
                admission = self._concept_admission(connection, str(epoch))
                if admission is None or str(admission["admission_state"]) not in {"canary", "incremental"} or not admission_is_live(admission, at=at):
                    raise self._concept_admission_error("snapshot_expired")
                body = json.loads(payload_json or "{}")
                if not isinstance(body, dict):
                    raise ValueError(f"outbox_payload_invalid:{outbox_id}")
                previous_hash = body.get("policy_hash")
                policy_hash = self._bind_vectors_only_concept_policy(
                    connection,
                    body=body,
                    namespace_epoch=str(epoch),
                    profile=str(profile),
                    at=at,
                )
                body["vectors_only_policy_repair"] = {
                    "schema": "concept-v11.vectors-only-policy-repair.v1",
                    "reason": "backfill_missing_policy_hash_before_dispatch",
                    "previous_policy_hash": previous_hash,
                    "policy_hash": policy_hash,
                    "repaired_at": at,
                }
                connection.execute(
                    "UPDATE outbox_items SET payload_json=?,updated_at=? WHERE outbox_id=?",
                    (json.dumps(body, ensure_ascii=False, separators=(",", ":")), at, str(outbox_id)),
                )
                repaired.append({"outbox_id": str(outbox_id), "policy_hash": policy_hash, "updated_at": at})
        return repaired

    def can_dispatch(self, key: str, *, at: Optional[str] = None) -> bool:
        current = at or now_iso()
        with self.store.connect() as connection:
            row = connection.execute("SELECT throttle_until,circuit_state FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
        if row is None:
            return True
        # An expired open window is eligible for one probe.  The durable
        # reservation is made by dispatch_once; this read-only helper reports
        # eligibility without claiming the probe itself.
        return row[0] is None or row[0] <= current

    def dispatch_once(
        self,
        *,
        limit: int = 20,
        lane: str = "all",
        outbox_ids: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Create durable semantic tasks for dispatchable Outbox rows.

        No network call happens here.  A later worker sends the task and calls
        ``ack`` or ``fail`` exactly once for this durable dispatch.
        """
        if limit <= 0:
            return []
        lane = str(lane or "all").strip().lower()
        if lane not in DISPATCH_LANES:
            raise ValueError("invalid dispatch lane")
        selected_ids = [str(value) for value in (outbox_ids or ()) if str(value)]
        if outbox_ids is not None and not selected_ids:
            return []
        at = now_iso()
        dispatched: List[Dict[str, Any]] = []
        with self.store.transaction() as connection:
            freeze = self.store._freeze_blocks(connection)
            if freeze is not None:
                return []
            self._reclaim_expired_dispatch_leases(connection, at)
            self._expire_retry_deadlines_unlocked(connection, at)
            # Do not apply SQL LIMIT before provider filtering: one throttled
            # provider must not create head-of-line blocking for ready work.
            lane_clause = ""
            lane_params: Tuple[Any, ...] = (at,)
            if lane == "fast-vector":
                lane_clause = " AND processing_mode='vectors_only'"
            elif lane == "semantic":
                lane_clause = " AND processing_mode IN ('semantic_only','semantic_and_vectors')"
            id_clause = ""
            id_params: Tuple[Any, ...] = ()
            if outbox_ids is not None:
                id_clause = " AND outbox_id IN (" + ",".join("?" for _ in selected_ids) + ")"
                id_params = tuple(selected_ids)
            rows = connection.execute(
                "SELECT outbox_id,idempotency_key,kind,resource_id,revision_id,processing_mode,provider,profile,owner,namespace_epoch,payload_json,attempt,retry_deadline_at,created_at "
                "FROM outbox_items WHERE kind IN ('resource','concept') AND status IN ('pending','retry_wait') "
                "AND (next_attempt_at IS NULL OR next_attempt_at<=?)" + lane_clause + id_clause +
                " ORDER BY CASE WHEN processing_mode='vectors_only' THEN 0 ELSE 1 END,created_at",
                lane_params + id_params,
            ).fetchall()
            for row in rows:
                if len(dispatched) >= int(limit):
                    break
                body = json.loads(row[10] or "{}")
                retry_deadline_at = row[12]
                if not retry_deadline_at:
                    retry_deadline_at = self._retry_deadline(row[13], now=datetime.now(timezone.utc))
                    connection.execute("UPDATE outbox_items SET retry_deadline_at=? WHERE outbox_id=? AND retry_deadline_at IS NULL", (retry_deadline_at, row[0]))
                retry_deadline = self._parse_timestamp(retry_deadline_at)
                if retry_deadline is None:
                    retry_deadline_at = self._retry_deadline(row[13], now=datetime.now(timezone.utc))
                    retry_deadline = self._parse_timestamp(retry_deadline_at)
                    connection.execute("UPDATE outbox_items SET retry_deadline_at=? WHERE outbox_id=?", (retry_deadline_at, row[0]))
                if retry_deadline is None or retry_deadline <= datetime.now(timezone.utc):
                    self._expire_retry_deadlines_unlocked(connection, at)
                    continue
                key = str(body.get("provider_key") or row[6])
                if row[2] == "concept":
                    admission = self._concept_admission(connection, str(row[9]))
                    if admission is None or str(admission["admission_state"]) not in {"canary", "incremental"} or not admission_is_live(admission, at=at):
                        continue
                    if row[5] in CONCEPT_SEMANTIC_MODES and not self._concept_capability_ready(
                        connection,
                        namespace_epoch=str(row[9]),
                        profile=str(row[7]),
                        processing_mode=str(row[5]),
                        policy_version=str(body.get("model_policy_version") or body.get("policy_version") or ""),
                        provider=str(body.get("provider") or row[6]),
                        at=at,
                    ):
                        continue
                    profile_policy = connection.execute(
                        "SELECT policy_hash FROM concept_profile_admissions "
                        "WHERE workload='concept-semantic' AND profile=? AND namespace_epoch=?",
                        (str(row[7]), str(row[9])),
                    ).fetchone()
                    if profile_policy is None or str(profile_policy[0] or "") != str(body.get("policy_hash") or ""):
                        continue
                bucket = connection.execute("SELECT throttle_until,circuit_state FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
                probe = False
                # vectors_only is a local embedding path.  It must remain
                # available while the remote semantic provider is throttled.
                if row[5] == "semantic_and_vectors" and bucket:
                    throttle_until, circuit_state = bucket[0], bucket[1]
                    if throttle_until and throttle_until > at:
                        continue
                    if circuit_state == "half_open":
                        # Another task already owns the single recovery probe.
                        if not self._reserve_probe(connection, key, at):
                            continue
                        probe = True
                    if circuit_state == "open":
                        probe = connection.execute(
                            "UPDATE provider_buckets SET circuit_state='half_open',updated_at=? WHERE provider_key=? AND circuit_state='open' AND (throttle_until IS NULL OR throttle_until<=?)",
                            (at, key, at),
                        )
                        if probe.rowcount != 1:
                            continue
                        if not self._reserve_probe(connection, key, at):
                            connection.execute("UPDATE provider_buckets SET circuit_state='open',updated_at=? WHERE provider_key=? AND circuit_state='half_open'", (at, key))
                            continue
                        probe = True
                dedupe = row[1]
                task_kind = "concept" if row[2] == "concept" else "semantic"
                task = connection.execute("SELECT semantic_task_id,status,attempt FROM semantic_tasks WHERE kind=? AND profile=? AND dedupe_key=? AND namespace_epoch=?", (task_kind, row[7], dedupe, row[9])).fetchone()
                if task is None:
                    task_id = f"semantic-{hashlib.sha256(dedupe.encode('utf-8')).hexdigest()[:24]}"
                    connection.execute(
                        "INSERT INTO semantic_tasks(semantic_task_id,dedupe_key,kind,outbox_id,resource_id,revision_id,processing_mode,provider,profile,owner,namespace_epoch,status,attempt,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (task_id, dedupe, task_kind, row[0], row[3], row[4], row[5], row[6], row[7], row[8], row[9], "queued", row[11], at, at),
                    )
                else:
                    task_id = task[0]
                token = self._new_token("dispatch")
                expires = (datetime.now(timezone.utc) + timedelta(seconds=self.dispatch_lease_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
                updated = connection.execute("UPDATE outbox_items SET status='in_flight',updated_at=? WHERE outbox_id=? AND status IN ('pending','retry_wait')", (at, row[0]))
                if updated.rowcount != 1:
                    if probe:
                        connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (key,))
                    continue
                connection.execute("INSERT INTO outbox_dispatch_leases(outbox_id,dispatch_token,owner,leased_at,expires_at) VALUES(?,?,?,?,?)", (row[0], token, f"pid-{os.getpid()}", at, expires))
                dispatched.append({"outbox_id": row[0], "semantic_task_id": task_id, "idempotency_key": dedupe, "kind": row[2], "resource_id": row[3], "revision_id": row[4], "processing_mode": row[5], "profile": row[7], "owner": row[8], "namespace_epoch": row[9], "provider_key": key, "dispatch_token": token, "dispatch_expires_at": expires, "retry_deadline_at": retry_deadline_at, "attempt": row[11], "probe": probe, "wait": bool(body.get("wait", False))})
        return dispatched

    def ack(
        self,
        outbox_id: str,
        *,
        openviking_task_id: Optional[str] = None,
        dispatch_token: Optional[str] = None,
        semantic_status: str = "completed",
        content_verified: Optional[bool] = None,
    ) -> bool:
        at = now_iso()
        semantic_status = str(semantic_status or "completed").strip().lower()
        if semantic_status not in {"accepted", "processing", "completed"}:
            raise ValueError("invalid semantic_status")
        with self.store.transaction() as connection:
            row = connection.execute("SELECT idempotency_key,status,payload_json FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                return False
            lease = connection.execute("SELECT dispatch_token,leased_at FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,)).fetchone()
            if dispatch_token is not None and (lease is None or lease[0] != dispatch_token):
                return False
            if row[1] == "completed":
                return True
            if row[1] != "in_flight":
                # A response from an older attempt must not resurrect a
                # retry_wait, failed, dead-letter, or cancelled task.
                return False
            body = json.loads(row[2] or "{}")
            connection.execute("UPDATE outbox_items SET status='completed',updated_at=?,next_attempt_at=NULL,terminal_reason=NULL WHERE outbox_id=?", (at, outbox_id))
            connection.execute("UPDATE semantic_tasks SET status=?,openviking_task_id=COALESCE(?,openviking_task_id),updated_at=?,terminal_reason=NULL WHERE outbox_id=?", (semantic_status, openviking_task_id, at, outbox_id))
            outbox_meta = connection.execute("SELECT resource_id,revision_id,processing_mode,kind FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            if outbox_meta:
                existing_projection = connection.execute(
                    "SELECT content_state,verified_at FROM resource_projections WHERE resource_id=? AND revision_id=?",
                    (outbox_meta[0], outbox_meta[1]),
                ).fetchone()
                if content_verified is True:
                    content_state = "content_verified"
                elif content_verified is False:
                    content_state = str(existing_projection[0]) if existing_projection else "content_pending"
                else:
                    content_state = str(existing_projection[0]) if existing_projection else "content_pending"
                semantic_state = "semantic_completed" if semantic_status == "completed" else "semantic_pending"
                connection.execute(
                    "INSERT INTO resource_projections(resource_id,revision_id,content_state,semantic_state,memory_link_state,verified_at,semantic_completed_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(resource_id,revision_id) DO UPDATE SET content_state=excluded.content_state,semantic_state=excluded.semantic_state,verified_at=COALESCE(excluded.verified_at,resource_projections.verified_at),semantic_completed_at=COALESCE(excluded.semantic_completed_at,resource_projections.semantic_completed_at),updated_at=excluded.updated_at,terminal_reason=NULL",
                    (outbox_meta[0], outbox_meta[1], content_state, semantic_state, "memory_link_pending", at if content_state == "content_verified" else None, at if semantic_state == "semantic_completed" else None, at),
                )
            if semantic_status in {"accepted", "processing"}:
                # Observation state is local control-plane metadata. It is
                # deliberately separate from the semantic task row and the
                # Outbox retry deadline.
                connection.execute(
                    """
                    INSERT OR IGNORE INTO semantic_task_observations(semantic_task_id)
                    SELECT semantic_task_id FROM semantic_tasks WHERE outbox_id=?
                    """,
                    (outbox_id,),
                )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,))
            if semantic_status == "completed":
                if self._release_concept_profile_unlocked(connection, body, at=at):
                    body["concept_profile_reserved"] = False
                    connection.execute("UPDATE outbox_items SET payload_json=? WHERE outbox_id=?", (json.dumps(body, ensure_ascii=False, separators=(",", ":")), outbox_id))
            key = str(body.get("provider_key") or body.get("provider") or "")
            if key and body.get("processing_mode") == "semantic_and_vectors":
                # A different in-flight dispatch may have recorded a 429
                # after this lease was acquired. Do not let this late success
                # erase that newer throttle window. Half-open probes are
                # allowed to close the circuit only when no newer failure
                # changed the bucket.
                leased_at = lease[1] if lease is not None else None
                connection.execute(
                    "UPDATE provider_buckets SET throttle_until=NULL,circuit_state='closed',consecutive_429=0,last_retry_after=NULL,updated_at=? "
                    "WHERE provider_key=? AND (consecutive_429=0 OR updated_at < ? OR (circuit_state='half_open' AND updated_at <= ?))",
                    (at, key, leased_at or at, leased_at or at),
                )
                connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (key,))
            return True

    def fail(
        self,
        outbox_id: str,
        *,
        category: str,
        detail: Optional[str] = None,
        retry_after: Any = None,
        provider_key_value: Optional[str] = None,
        dispatch_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        category = str(category).lower()
        at_dt = datetime.now(timezone.utc)
        at = at_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
        fingerprint = _fingerprint(category, detail)
        with self.store.transaction() as connection:
            row = connection.execute("SELECT attempt,payload_json,status,next_attempt_at,error_fingerprint,retry_deadline_at,created_at FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                raise KeyError(outbox_id)
            lease = connection.execute("SELECT dispatch_token FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,)).fetchone()
            if dispatch_token is not None and (lease is None or lease[0] != dispatch_token):
                return {"outbox_id": outbox_id, "status": "stale_callback", "attempt": int(row[0]), "ignored": True}
            if row[2] != "in_flight":
                # Failure callbacks are at-least-once.  Only the dispatch that
                # still owns the in-flight row may change durable state.
                return {"outbox_id": outbox_id, "status": row[2], "attempt": int(row[0]), "next_attempt_at": row[3], "error_fingerprint": row[4], "ignored": True}
            attempt = int(row[0])
            body = json.loads(row[1] or "{}")
            key = provider_key_value or str(body.get("provider_key") or body.get("provider") or "oneapi|default|default")
            retry_deadline_at = row[5] or self._retry_deadline(row[6], now=at_dt)
            if row[5] is None:
                connection.execute("UPDATE outbox_items SET retry_deadline_at=? WHERE outbox_id=?", (retry_deadline_at, outbox_id))
            retry_deadline = self._parse_timestamp(retry_deadline_at) or at_dt
            if category in RATE_LIMIT_FAILURES:
                self._record_429_unlocked(connection, key, retry_after=retry_after, at=at_dt)
                bucket = connection.execute("SELECT throttle_until FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
                next_at = bucket[0] if bucket and bucket[0] else (at_dt + timedelta(seconds=RATE_LIMIT_STEPS[0])).isoformat(timespec="seconds").replace("+00:00", "Z")
                if at_dt >= retry_deadline:
                    status, next_at = "dead_letter", None
                else:
                    next_at = min(next_at, retry_deadline_at)
                    status = "retry_wait"
                new_attempt = attempt
            elif category in TRANSIENT_FAILURES:
                new_attempt = attempt + 1
                if new_attempt > self.max_attempts:
                    status, next_at = "dead_letter", None
                else:
                    delay = min(120, 2 ** (new_attempt - 1) * 2)
                    next_at = min(self._timestamp(at_dt + timedelta(seconds=delay)), retry_deadline_at)
                    status = "retry_wait"
                bucket = connection.execute("SELECT circuit_state FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
                if bucket and bucket[0] == "half_open":
                    probe_until = next_at or (at_dt + timedelta(seconds=30)).isoformat(timespec="seconds").replace("+00:00", "Z")
                    connection.execute("UPDATE provider_buckets SET circuit_state='open',throttle_until=?,updated_at=? WHERE provider_key=?", (probe_until, at, key))
            elif category in QUARANTINE_FAILURES:
                status, next_at, new_attempt = "quarantine", None, attempt
            elif category in PERMANENT_FAILURES or category:
                status, next_at, new_attempt = "failed", None, attempt
                connection.execute("UPDATE provider_buckets SET throttle_until=NULL,circuit_state='closed',consecutive_429=0,last_retry_after=NULL,updated_at=? WHERE provider_key=? AND circuit_state='half_open'", (at, key))
            connection.execute("UPDATE outbox_items SET status=?,attempt=?,next_attempt_at=?,error_fingerprint=?,terminal_reason=?,updated_at=? WHERE outbox_id=?", (status, new_attempt, next_at, fingerprint, category, at, outbox_id))
            connection.execute("UPDATE semantic_tasks SET status=?,attempt=?,error_fingerprint=?,terminal_reason=?,updated_at=? WHERE outbox_id=?", (status, new_attempt, fingerprint, category, at, outbox_id))
            meta = connection.execute("SELECT resource_id,revision_id FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            if meta:
                self._mark_projection_failure_unlocked(
                    connection,
                    resource_id=str(meta[0]),
                    revision_id=str(meta[1]),
                    status=status,
                    reason=category,
                    at=at,
                )
            connection.execute("DELETE FROM outbox_dispatch_leases WHERE outbox_id=?", (outbox_id,))
            connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (key,))
            if status in TERMINAL_OUTBOX_STATUSES and self._release_concept_profile_unlocked(connection, body, at=at):
                body["concept_profile_reserved"] = False
                connection.execute("UPDATE outbox_items SET payload_json=? WHERE outbox_id=?", (json.dumps(body, ensure_ascii=False, separators=(",", ":")), outbox_id))
            return {"outbox_id": outbox_id, "status": status, "attempt": new_attempt, "next_attempt_at": next_at, "error_fingerprint": fingerprint}

    def _record_429_unlocked(self, connection: Any, key: str, *, retry_after: Any = None, at: Optional[datetime] = None) -> Dict[str, Any]:
        current = at or datetime.now(timezone.utc)
        current_iso = current.isoformat(timespec="seconds").replace("+00:00", "Z")
        parsed = _parse_retry_after(retry_after, now=current)
        row = connection.execute("SELECT provider,endpoint,model,throttle_until,consecutive_429 FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
        if row is None:
            provider, endpoint, model = _parse_key(key)
            self._ensure_bucket(connection, key, provider=provider, endpoint=endpoint, model=model, at=current_iso)
            row = connection.execute("SELECT provider,endpoint,model,throttle_until,consecutive_429 FROM provider_buckets WHERE provider_key=?", (key,)).fetchone()
        old_until = row[3]
        count = int(row[4]) + 1
        seconds = parsed if parsed is not None else RATE_LIMIT_STEPS[min(count - 1, len(RATE_LIMIT_STEPS) - 1)]
        proposed = (current + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
        until = max(filter(None, [old_until, proposed]))
        circuit = "open" if count >= self.circuit_threshold else "closed"
        connection.execute("UPDATE provider_buckets SET throttle_until=?,circuit_state=?,consecutive_429=?,last_retry_after=?,updated_at=? WHERE provider_key=?", (until, circuit, count, None if retry_after is None else str(retry_after), current_iso, key))
        connection.execute("INSERT INTO provider_rate_limit_events(provider_key,occurred_at,retry_after_seconds) VALUES(?,?,?)", (key, current_iso, parsed if parsed is not None else seconds))
        connection.execute("DELETE FROM provider_probe_leases WHERE provider_key=?", (key,))
        return {"provider_key": key, "throttle_until": until, "consecutive_429": count, "circuit_state": circuit, "retry_after_seconds": seconds}

    def record_429(self, key: str, *, retry_after: Any = None, at: Optional[datetime] = None) -> Dict[str, Any]:
        with self.store.transaction() as connection:
            return self._record_429_unlocked(connection, key, retry_after=retry_after, at=at)

    def reset_provider(self, key: str) -> None:
        with self.store.transaction() as connection:
            connection.execute("UPDATE provider_buckets SET throttle_until=NULL,circuit_state='closed',consecutive_429=0,updated_at=? WHERE provider_key=?", (now_iso(), key))

    def retry_amplification(self) -> float:
        with self.store.connect() as connection:
            return retry_amplification_from_connection(connection)["amplification"]


def retry_amplification_from_connection(connection: Any) -> Dict[str, Any]:
    """Use one metric definition for Gateway and read-only S10 observations."""
    logical_outbox = int(connection.execute("SELECT COUNT(*) FROM outbox_items").fetchone()[0])
    outbox_attempts = int(connection.execute("SELECT COALESCE(SUM(attempt),0) FROM outbox_items").fetchone()[0])
    try:
        logical_model = int(connection.execute("SELECT COUNT(*) FROM (SELECT DISTINCT run_id,stage,model_input_hash FROM model_calls)").fetchone()[0])
        model_calls = int(connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0])
    except Exception:
        logical_model = model_calls = 0
    try:
        rate_limit_events = int(connection.execute("SELECT COUNT(*) FROM provider_rate_limit_events").fetchone()[0])
    except Exception:
        rate_limit_events = 0
    outbox_amp = outbox_attempts / logical_outbox if logical_outbox else 0.0
    model_amp = model_calls / logical_model if logical_model else 0.0
    # 429 is a provider-capacity signal, not a business retry.  The gateway
    # keeps the item in the shared throttle window without incrementing its
    # attempt, so rate-limit events must remain visible in the breakdown but
    # must not inflate the duplicate-work metric used by capacity/S10 gates.
    rate_amp = 0.0
    return {
        "amplification": round(max(outbox_amp, model_amp, rate_amp), 4),
        "outbox": {"logical": logical_outbox, "attempts": outbox_attempts, "amplification": round(outbox_amp, 4)},
        "model": {"logical": logical_model, "calls": model_calls, "amplification": round(model_amp, 4)},
        "rate_limit": {"events": rate_limit_events, "amplification": round(rate_amp, 4)},
    }


__all__ = [
    "PERMANENT_FAILURES",
    "RATE_LIMIT_FAILURES",
    "QUARANTINE_FAILURES",
    "SemanticGateway",
    "TRANSIENT_FAILURES",
    "retry_amplification_from_connection",
    "_parse_retry_after",
    "provider_key",
]
