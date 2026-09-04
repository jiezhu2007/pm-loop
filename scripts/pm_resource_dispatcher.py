#!/usr/bin/env python3
"""Single-writer Outbox dispatcher for PM resources.

Source adapters submit a local file to this module.  The file hash becomes the
revision id, the Outbox is the durable acceptance boundary, and this dispatcher
performs at most one OpenViking request sequence for each claimed item.  The
Gateway owns retry/throttle state; adapters do not create their own retry loop.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import socket
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pm_system_gateway import (
    CONCEPT_MODEL_POLICY_VERSION,
    CONCEPT_SEMANTIC_MODES,
    SemanticGateway,
    _normalize_pm_payload,
)
from pm_system_store import PMSystemStore, now_iso


DEFAULT_DB_PATH = Path.home() / ".codex" / "pm-loop" / "state" / "pm-system.db"
DEFAULT_ARTIFACT_ROOT = Path.home() / ".codex" / "pm-loop" / "resource-outbox"
DEFAULT_URL = "http://127.0.0.1:1933"
PM_RESOURCE_URI_PREFIXES = (
    "viking://resources/pm",
    "viking://resources/concepts",
    "viking://resources/project-docs",
    "viking://resources/pm-timeline",
    "viking://resources/skills",
    "viking://resources/shengsuan",
    "viking://resources/memory",
    "viking://resources/competitive",
)
LEGACY_SKILL_RESOURCE_PREFIX = "viking://resources/skills"
SUCCESS_STATUSES = {"ok", "accepted", "queued", "success", "complete", "completed", "done", "created"}
TRANSIENT_HTTP_STATUSES = {408, 425, 500, 502, 503, 504}
OBSERVATION_TERMINAL_SUCCESS = {"ok", "success", "complete", "completed", "done", "succeeded"}
OBSERVATION_TERMINAL_FAILURE = {"failed", "error", "cancelled", "canceled", "dead_letter"}
# ``accepted`` is an asynchronous OpenViking state too: it must carry a task
# handle so the local observer can eventually resolve it.
OBSERVATION_ACTIVE = {"accepted", "queued", "pending", "running", "processing", "in_progress"}
GENERIC_ENVELOPE_STATUSES = {"ok", "success", "created"}


def is_pm_resource_uri(uri: Any) -> bool:
    value = str(uri or "").strip().rstrip("/")
    return any(value == prefix or value.startswith(prefix + "/") for prefix in PM_RESOURCE_URI_PREFIXES)


def is_legacy_skill_resource_uri(uri: Any) -> bool:
    value = str(uri or "").strip().rstrip("/")
    return value == LEGACY_SKILL_RESOURCE_PREFIX or value.startswith(LEGACY_SKILL_RESOURCE_PREFIX + "/")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_content_hash(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw.removeprefix("sha256:")


def _content_hash_from_response(value: Any) -> Optional[str]:
    """Extract an explicit or byte-equivalent content hash from read-back."""
    if isinstance(value, Mapping):
        for key in ("content_hash", "content_sha256", "sha256", "hash"):
            candidate = value.get(key)
            if candidate:
                normalized = _normalize_content_hash(candidate)
                if normalized:
                    return normalized
        for key, child in value.items():
            # ``/content/read`` returns the file body directly in ``result``
            # on OpenViking 0.4.x.  Hash that scalar body, but never hash
            # status/error strings from the response envelope.
            if str(key).lower() in {"content", "text", "body", "data", "result"} and isinstance(child, str):
                return hashlib.sha256(child.encode("utf-8")).hexdigest()
            found = _content_hash_from_response(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _content_hash_from_response(child)
            if found:
                return found
    return None


def _config() -> Dict[str, str]:
    config: Dict[str, str] = {}
    path = Path.home() / ".openviking" / "ovcli.conf"
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                config.update({str(key): str(value) for key, value in loaded.items() if value is not None})
        except (OSError, ValueError):
            pass
    for key in ("OPENVIKING_URL", "OPENVIKING_API_KEY", "OPENVIKING_ACCOUNT", "OPENVIKING_USER"):
        if os.environ.get(key):
            config[key.lower().replace("openviking_", "")] = os.environ[key]
    config["url"] = str(config.get("url") or DEFAULT_URL).rstrip("/")
    return config


def _headers(config: Mapping[str, str], content_type: str = "application/json") -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if content_type:
        headers["Content-Type"] = content_type
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"
    if config.get("account"):
        headers["X-OpenViking-Account"] = str(config["account"])
    if config.get("user"):
        headers["X-OpenViking-User"] = str(config["user"])
    return headers


def _transport_idempotency_key(value: Any) -> str:
    """Return an ASCII-safe stable key for the HTTP header.

    Local PM idempotency keys intentionally contain the target URI, which may
    include Chinese filenames.  ``http.client`` only accepts Latin-1 header
    values, so hash non-Latin-1 keys for transport while retaining the full
    original key in the local Outbox ledger.
    """
    raw = str(value or "")
    if not raw:
        return ""
    try:
        raw.encode("latin-1")
    except UnicodeEncodeError:
        return "pm-v44-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return raw


class DispatchHTTPError(RuntimeError):
    def __init__(self, status_code: int, reason: str, *, retry_after: Optional[str] = None, body: str = "") -> None:
        self.status_code = int(status_code)
        self.retry_after = retry_after
        self.body = body[-2000:]
        super().__init__(f"HTTP {status_code} {reason}")


class DispatchTransportError(RuntimeError):
    category = "connection"


class DispatchTimeoutError(DispatchTransportError):
    category = "timeout"


class DispatchProtocolError(RuntimeError):
    pass


class OpenVikingTransport:
    """One-attempt HTTP transport. Retry policy stays in ``SemanticGateway``."""

    def __init__(self, *, url: Optional[str] = None, timeout: float = 30.0) -> None:
        loaded = _config()
        self.config = loaded
        self.url = str(url or loaded["url"]).rstrip("/")
        self.timeout = max(1.0, float(timeout))

    def _request(self, method: str, path: str, *, data: Optional[bytes] = None, headers: Optional[Mapping[str, str]] = None, timeout: Optional[float] = None) -> Any:
        req = urllib.request.Request(
            self.url + path,
            data=data,
            headers=dict(headers or _headers(self.config)),
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=max(1.0, float(timeout or self.timeout))) as response:
                raw = response.read()
                content_type = response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise DispatchHTTPError(exc.code, exc.reason, retry_after=exc.headers.get("Retry-After"), body=body) from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                raise DispatchTimeoutError(str(exc.reason) or "OpenViking request timed out") from exc
            raise DispatchTransportError(str(exc.reason)) from exc
        except (TimeoutError, socket.timeout) as exc:
            # urllib can expose a native socket timeout directly instead of
            # wrapping it in URLError. Keep it in the retryable transport
            # family so the Outbox does not classify it as permanent.
            raise DispatchTimeoutError(str(exc) or "OpenViking request timed out") from exc
        except OSError as exc:
            raise DispatchTransportError(str(exc)) from exc
        try:
            return json.loads(raw.decode("utf-8")) if "json" in content_type.lower() or raw.lstrip().startswith((b"{", b"[")) else raw.decode("utf-8", errors="replace")
        except (UnicodeDecodeError, ValueError) as exc:
            raise DispatchProtocolError("OpenViking returned invalid JSON") from exc

    def upload_file(self, path: Path, *, timeout: Optional[float] = None) -> Any:
        if not path.is_file():
            raise DispatchProtocolError(f"resource file is missing: {path}")
        boundary = "----pm-v44-" + uuid.uuid4().hex
        filename = path.name
        # ``http.client`` serializes header values as Latin-1.  A Chinese
        # filename must therefore use an ASCII fallback plus RFC 5987's
        # UTF-8 ``filename*`` parameter; otherwise the request fails locally
        # before it ever reaches OpenViking.
        ascii_filename = filename.encode("ascii", "ignore").decode("ascii") or "upload"
        encoded_filename = urllib.parse.quote(filename, safe="")
        disposition = (
            f'Content-Disposition: form-data; name="file"; filename="{ascii_filename}"; '
            f"filename*=UTF-8''{encoded_filename}\r\n"
        )
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        body = b"".join(
            (
                f"--{boundary}\r\n".encode(),
                disposition.encode("ascii"),
                f"Content-Type: {mime}\r\n\r\n".encode(),
                path.read_bytes(),
                f"\r\n--{boundary}\r\n".encode(),
                b'Content-Disposition: form-data; name="upload_mode"\r\n\r\nlocal\r\n',
                f"--{boundary}--\r\n".encode(),
            )
        )
        return self._request(
            "POST",
            "/api/v1/resources/temp_upload",
            data=body,
            headers=_headers(self.config, f"multipart/form-data; boundary={boundary}"),
            timeout=timeout,
        )

    def add_resource(self, body: Mapping[str, Any], *, timeout: Optional[float] = None, idempotency_key: Optional[str] = None) -> Any:
        normalized = _normalize_pm_payload(body)
        if any(str(key).lower() == "reason" for key in normalized):
            raise DispatchProtocolError("reason leaked into OpenViking resource payload")
        request_headers = _headers(self.config)
        if idempotency_key:
            # OpenViking 0.4.x does not promise provider-side exactly-once,
            # but this stable header lets a newer endpoint deduplicate a
            # response-lost replay without changing the request schema.
            request_headers["Idempotency-Key"] = _transport_idempotency_key(idempotency_key)
        return self._request(
            "POST",
            "/api/v1/resources",
            data=json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            headers=request_headers,
            timeout=timeout,
        )

    def write_content(
        self,
        target_uri: str,
        content: str,
        *,
        mode: str = "replace",
        processing_mode: str = "vectors_only",
        wait: bool = False,
        timeout: Optional[float] = None,
        idempotency_key: Optional[str] = None,
    ) -> Any:
        """Write a text file through OpenViking's content API.

        This is intentionally separate from ``add_resource``: Memory Markdown
        mirroring is a file-content operation and must not enter Resource
        ingestion or Session extraction.
        """
        normalized_mode = str(mode or "replace").strip().lower()
        normalized_processing = str(processing_mode or "vectors_only").strip().lower()
        if normalized_mode not in {"create", "replace", "append"}:
            raise ValueError("invalid content write mode")
        if normalized_processing not in {"vectors_only", "semantic_and_vectors"}:
            raise ValueError("invalid content processing_mode")
        body = {
            "uri": str(target_uri),
            "content": str(content),
            "mode": normalized_mode,
            "wait": bool(wait),
            "processing_mode": normalized_processing,
        }
        request_headers = _headers(self.config)
        if idempotency_key:
            request_headers["Idempotency-Key"] = _transport_idempotency_key(idempotency_key)
        return self._request(
            "POST",
            "/api/v1/content/write",
            data=json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            headers=request_headers,
            timeout=timeout,
        )

    def get_task(self, task_id: str, *, timeout: Optional[float] = None) -> Any:
        return self._request("GET", f"/api/v1/tasks/{urllib.parse.quote(str(task_id), safe='')}", timeout=timeout)

    def list_uri(self, target_uri: str, *, timeout: Optional[float] = None) -> Any:
        """List one OpenViking URI without changing remote state."""
        query = urllib.parse.urlencode({"uri": str(target_uri)})
        return self._request("GET", f"/api/v1/fs/ls?{query}", timeout=timeout)

    def stat_uri(self, target_uri: str, *, timeout: Optional[float] = None) -> Any:
        """Stat one OpenViking URI without changing remote state."""
        query = urllib.parse.urlencode({"uri": str(target_uri)})
        return self._request("GET", f"/api/v1/fs/stat?{query}", timeout=timeout)

    def mkdir(self, target_uri: str, *, timeout: Optional[float] = None) -> Any:
        """Create one OpenViking directory URI."""
        body = {"uri": str(target_uri).rstrip("/")}
        return self._request(
            "POST",
            "/api/v1/fs/mkdir",
            data=json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            headers=_headers(self.config),
            timeout=timeout,
        )

    def read_content(self, target_uri: str, *, timeout: Optional[float] = None) -> Any:
        """Read persisted content independently from semantic task status."""
        query = urllib.parse.urlencode({"uri": str(target_uri)})
        return self._request("GET", f"/api/v1/content/read?{query}", timeout=timeout)


def _walk_status(value: Any, *, in_error: bool = False) -> Tuple[str, Optional[str]]:
    if isinstance(value, Mapping):
        status = value.get("status")
        normalized_status = status.strip().lower() if not in_error and isinstance(status, str) and status.strip() else ""
        task_id = value.get("task_id") or value.get("taskId") or value.get("id")
        found: Tuple[str, Optional[str]] = ("", None)
        # OpenViking commonly wraps the actual task state in an outer
        # ``status=ok`` envelope. Prefer a nested task state in that case;
        # otherwise ``ok`` would incorrectly turn ``processing`` into a
        # terminal success during reconciliation.
        for key, child in value.items():
            if str(key).lower() in {"error", "errors"}:
                continue
            found = _walk_status(child, in_error=False)
            if found[0]:
                if normalized_status in GENERIC_ENVELOPE_STATUSES:
                    return found
                break
        if normalized_status:
            return normalized_status, str(task_id) if task_id else None
        if found[0]:
            return found
    elif isinstance(value, list):
        for child in value:
            found = _walk_status(child, in_error=in_error)
            if found[0]:
                return found
    return "", None


def _task_id(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key in ("task_id", "taskId", "resource_id", "resource"):
            candidate = value.get(key)
            if candidate:
                return str(candidate)
        for key, child in value.items():
            if str(key).lower() in {"error", "errors"}:
                continue
            found = _task_id(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _task_id(child)
            if found:
                return found
    return None


def _retry_after_from_response(value: Any) -> Optional[str]:
    if not isinstance(value, Mapping):
        return None
    for key in ("retry_after", "retry-after", "retryAfter"):
        if value.get(key) is not None:
            return str(value[key])
    for key, child in value.items():
        if str(key).lower() in {"error", "errors"}:
            continue
        found = _retry_after_from_response(child)
        if found:
            return found
    return None


class PMResourceDispatcher:
    def __init__(
        self,
        store: PMSystemStore,
        *,
        transport: Optional[OpenVikingTransport] = None,
        max_attempts: int = 3,
        artifact_root: Optional[Path] = None,
        observation_max_attempts: int = 3,
        observation_deadline_seconds: int = 3600,
        observation_backoff_seconds: int = 30,
    ) -> None:
        self.store = store
        self.gateway = SemanticGateway(store, max_attempts=max_attempts)
        self.transport = transport or OpenVikingTransport()
        self.artifact_root = Path(artifact_root or DEFAULT_ARTIFACT_ROOT).expanduser().resolve()
        if observation_max_attempts <= 0:
            raise ValueError("observation_max_attempts must be positive")
        if observation_deadline_seconds <= 0:
            raise ValueError("observation_deadline_seconds must be positive")
        if observation_backoff_seconds < 0:
            raise ValueError("observation_backoff_seconds must be non-negative")
        self.observation_max_attempts = int(observation_max_attempts)
        self.observation_deadline_seconds = int(observation_deadline_seconds)
        self.observation_backoff_seconds = int(observation_backoff_seconds)

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

    def _observation_deadline(self, created_at: Any, *, now: datetime) -> str:
        created = self._parse_timestamp(created_at) or now
        return self._timestamp(created + timedelta(seconds=self.observation_deadline_seconds))

    def _observation_retry_at(self, attempt: int, *, now: datetime) -> str:
        # Observation is a control-plane poll, so its backoff is intentionally
        # short and bounded. It never resubmits the resource.
        delay = self.observation_backoff_seconds * max(1, min(8, 2 ** max(0, attempt - 1)))
        return self._timestamp(now + timedelta(seconds=delay))

    def _durable_copy(self, source: Path, digest: str) -> Path:
        """Persist the source before returning local Outbox acceptance.

        Several legacy adapters clean temporary files as soon as ``ov-upload``
        returns. The worker may dispatch much later, so the Outbox must point
        at a stable artifact rather than the caller's transient path.
        """
        if source.is_relative_to(self.artifact_root):
            return source
        destination = self.artifact_root / digest[:2] / f"{digest}-{source.name}"
        if destination.is_file():
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                with source.open("rb") as stream:
                    shutil.copyfileobj(stream, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    def enqueue_file(
        self,
        *,
        path: Path,
        target_uri: str,
        kind: str = "resource",
        processing_mode: str = "vectors_only",
        provider: str = "openviking",
        profile: str = "pm-resource",
        instruction: str = "",
        wait: bool = False,
        timeout: float = 30.0,
        strict: bool = False,
        namespace_epoch: str = "v4",
        owner: str = "pm-system",
    ) -> Dict[str, Any]:
        source = Path(path).expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(source)
        target = str(target_uri or "").strip()
        if not is_pm_resource_uri(target):
            raise ValueError("PM resource dispatcher requires a PM resource URI")
        kind = str(kind or "resource").strip().lower()
        if kind not in {"resource", "concept"}:
            raise ValueError("unsupported PM outbox kind")
        if kind == "concept" and profile == "pm-resource" and processing_mode in {"semantic_only", "semantic_and_vectors"}:
            profile = "pm-semantic"
        if is_legacy_skill_resource_uri(target):
            raise ValueError(
                "legacy viking://resources/skills namespace is fenced; "
                "submit Skills through the native /api/v1/skills API"
            )
        revision = _sha256_file(source)
        durable_source = self._durable_copy(source, revision)
        payload = _normalize_pm_payload(
            {
                "file_path": str(durable_source),
                "target_uri": target,
                "instruction": instruction,
                "create_parent": True,
                "wait": bool(wait),
                "timeout": float(timeout),
                "strict": bool(strict),
                "kind": kind,
            }
        )
        if kind == "concept" and processing_mode in CONCEPT_SEMANTIC_MODES:
            # Local provenance only. OpenViking AddResource rejects unknown
            # fields, so _send deliberately does not copy these into its HTTP
            # body. The Gateway rebinds and verifies them against SQLite.
            payload["model_requested"] = "auto"
            payload["model_policy_version"] = CONCEPT_MODEL_POLICY_VERSION
        return self.gateway.enqueue(
            resource_id=target,
            revision_id=revision,
            processing_mode=processing_mode,
            provider=provider,
            profile=profile,
            payload=payload,
            endpoint=self.transport.url,
            model="auto" if kind == "concept" and processing_mode in CONCEPT_SEMANTIC_MODES else "resource-api",
            kind=kind,
            namespace_epoch=namespace_epoch,
            owner=owner,
        )

    def enqueue_concept_file(self, **kwargs: Any) -> Dict[str, Any]:
        """Submit a concept projection through the shared PM Outbox."""
        kwargs["kind"] = "concept"
        kwargs.setdefault("provider", "oneapi")
        kwargs.setdefault("profile", "pm-semantic")
        return self.enqueue_file(**kwargs)

    def _outbox_payload(self, outbox_id: str) -> Dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute("SELECT payload_json FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
        if row is None:
            raise KeyError(outbox_id)
        value = json.loads(row[0] or "{}")
        return value if isinstance(value, dict) else {}

    def _mark_content_verified(self, item: Mapping[str, Any]) -> None:
        """Promote only the Phase-A content state after an independent read-back."""
        at = now_iso()
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE resource_projections SET content_state='content_verified',verified_at=COALESCE(verified_at,?),updated_at=? "
                "WHERE resource_id=? AND revision_id=?",
                (at, at, str(item.get("resource_id") or ""), str(item.get("revision_id") or "")),
            )

    @staticmethod
    def _fs_entries(value: Any) -> List[Mapping[str, Any]]:
        """Normalize OpenViking ``/fs/ls`` response shapes."""
        rows: Any = value.get("result") if isinstance(value, Mapping) else value
        if isinstance(rows, Mapping):
            rows = rows.get("entries") or rows.get("items") or rows.get("resources") or rows.get("children")
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, Mapping) and str(row.get("uri") or "").strip()]

    def _directory_leaves(self, root_uri: str, *, expected_hash: str, timeout: float) -> List[Dict[str, Any]]:
        """Resolve a directory root to bounded readable file candidates.

        OpenViking stores a semantically processed upload as a directory whose
        leaf name starts with a prefix of the source revision hash.  The walk
        is deliberately bounded and read-only; it never submits another
        resource or changes the local retry state.
        """
        if not hasattr(self.transport, "list_uri"):
            return []
        queue: List[Tuple[str, int]] = [(root_uri.rstrip("/"), 0)]
        seen = {root_uri.rstrip("/")}
        leaves: List[Dict[str, Any]] = []
        while queue and len(leaves) < 8:
            uri, depth = queue.pop(0)
            try:
                listing = self.transport.list_uri(uri, timeout=min(timeout, 10.0))
            except Exception:
                return leaves
            for row in self._fs_entries(listing):
                child = str(row.get("uri") or "").strip()
                if not child or child in seen:
                    continue
                seen.add(child)
                is_dir = bool(row.get("isDir", row.get("is_dir", row.get("directory", False))))
                if is_dir and depth < 3:
                    queue.append((child, depth + 1))
                    continue
                if is_dir:
                    continue
                name = child.rsplit("/", 1)[-1]
                prefix = name.split("_", 1)[0].lower()
                if len(prefix) < 16 or not expected_hash.startswith(prefix):
                    continue
                leaves.append({"uri": child, "source_hash_prefix": prefix})
                if len(leaves) >= 8:
                    break
        return leaves

    def _read_back(self, item: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
        """Perform one bounded content read-back without resubmitting the resource."""
        target_uri = str(payload.get("target_uri") or item.get("resource_id") or "")
        expected = _normalize_content_hash(item.get("revision_id") or "")
        logical_key = str(item.get("idempotency_key") or item.get("outbox_id") or "")
        if not target_uri or not expected or not hasattr(self.transport, "read_content"):
            return {"verified": False, "status": "not_supported", "target_uri": target_uri}
        ledger = self.store.begin_operation(
            operation_type="content_read_back",
            idempotency_key=f"{logical_key}:content_read_back",
            target_uri=target_uri,
            request_hash=expected,
            namespace_epoch=str(item.get("namespace_epoch") or payload.get("namespace_epoch") or "v4"),
        )
        response: Any
        if ledger.get("response_state") in {"accepted", "completed"}:
            try:
                response = json.loads(str(ledger.get("response_json") or "{}"))
            except json.JSONDecodeError:
                response = {}
        else:
            try:
                response = self.transport.read_content(target_uri, timeout=min(float(payload.get("timeout") or 30.0), 10.0))
            except DispatchHTTPError as exc:
                # A semantic OpenViking upload returns ``root_uri`` as a
                # directory.  Resolve its generated leaf instead of treating
                # the valid upload as a permanent content failure.
                directory_error = exc.status_code == 400 and "directory uri" in exc.body.lower()
                if not directory_error:
                    self.store.finish_operation(str(ledger["operation_id"]), response_state="unknown", response={"error": str(exc), "target_uri": target_uri})
                    return {"verified": False, "status": "unknown", "error": type(exc).__name__, "target_uri": target_uri}
                for candidate in self._directory_leaves(
                    target_uri,
                    expected_hash=expected,
                    timeout=min(float(payload.get("timeout") or 30.0), 10.0),
                ):
                    try:
                        leaf_response = self.transport.read_content(candidate["uri"], timeout=min(float(payload.get("timeout") or 30.0), 10.0))
                    except Exception:
                        continue
                    actual_leaf = _content_hash_from_response(leaf_response)
                    if not actual_leaf:
                        continue
                    resolved = {
                        "verified": True,
                        "status": "verified_directory_leaf",
                        "verification_mode": "source_hash_prefix_and_leaf_read",
                        "expected_hash": expected,
                        "actual_hash": actual_leaf,
                        "source_hash_prefix": candidate["source_hash_prefix"],
                        "resolved_uri": candidate["uri"],
                        "target_uri": target_uri,
                    }
                    self.store.finish_operation(str(ledger["operation_id"]), response_state="completed", response=resolved)
                    self._mark_content_verified(item)
                    return resolved
                self.store.finish_operation(str(ledger["operation_id"]), response_state="unknown", response={"error": str(exc), "target_uri": target_uri, "directory_resolution": "no_readable_leaf"})
                return {"verified": False, "status": "unknown", "error": type(exc).__name__, "target_uri": target_uri, "directory_resolution": "no_readable_leaf"}
            except Exception as exc:
                self.store.finish_operation(str(ledger["operation_id"]), response_state="unknown", response={"error": str(exc), "target_uri": target_uri})
                return {"verified": False, "status": "unknown", "error": type(exc).__name__, "target_uri": target_uri}
            self.store.finish_operation(str(ledger["operation_id"]), response_state="completed", response=response)
        actual = _content_hash_from_response(response)
        verified = bool(actual and actual == expected)
        if not verified:
            self.store.finish_operation(
                str(ledger["operation_id"]),
                response_state="failed",
                response={"target_uri": target_uri, "expected_hash": expected, "actual_hash": actual},
            )
            return {"verified": False, "status": "hash_mismatch" if actual else "missing_hash", "expected_hash": expected, "actual_hash": actual, "target_uri": target_uri}
        self._mark_content_verified(item)
        return {"verified": True, "status": "verified", "expected_hash": expected, "actual_hash": actual, "target_uri": target_uri}

    def _send(self, item: Mapping[str, Any]) -> Tuple[Any, Optional[str], str, Dict[str, Any]]:
        payload = self._outbox_payload(str(item["outbox_id"]))
        source = Path(str(payload.get("file_path") or ""))
        outbox_id = str(item["outbox_id"])
        logical_key = str(item.get("idempotency_key") or outbox_id)
        revision_hash = str(item.get("revision_id") or "")
        attempt_no = int(item.get("attempt") or 0) + 1
        temp_ledger = self.store.begin_operation(
            operation_type="temp_upload",
            idempotency_key=f"{logical_key}:temp_upload",
            target_uri=str(payload.get("target_uri") or ""),
            request_hash=revision_hash,
            namespace_epoch=str(item.get("namespace_epoch") or payload.get("namespace_epoch") or "v4"),
        )
        if temp_ledger.get("response_state") in {"accepted", "completed"}:
            try:
                uploaded = json.loads(str(temp_ledger.get("response_json") or "{}"))
            except json.JSONDecodeError:
                uploaded = {}
        elif temp_ledger.get("deduplicated") and temp_ledger.get("response_state") in {"unknown", "pending"} and attempt_no > 2:
            raise DispatchProtocolError("temp_upload response remained unknown after one controlled resend")
        else:
            try:
                uploaded = self.transport.upload_file(source, timeout=float(payload.get("timeout") or 30.0))
            except Exception as exc:
                self.store.finish_operation(str(temp_ledger["operation_id"]), response_state="unknown", response={"error": str(exc)})
                raise
            self.store.finish_operation(str(temp_ledger["operation_id"]), response_state="accepted", response=uploaded)
        temp_id = None
        if isinstance(uploaded, Mapping):
            result = uploaded.get("result")
            if isinstance(result, Mapping):
                temp_id = result.get("temp_file_id")
            temp_id = temp_id or uploaded.get("temp_file_id")
        if not temp_id:
            raise DispatchProtocolError("temp_upload returned no temp_file_id")
        body = {
            "temp_file_id": str(temp_id),
            "to": payload.get("target_uri"),
            "create_parent": bool(payload.get("create_parent", True)),
            "wait": bool(payload.get("wait", False)),
            "timeout": float(payload.get("timeout") or 30.0),
            "processing_mode": item.get("processing_mode") or payload.get("processing_mode") or "vectors_only",
        }
        for key in ("instruction", "strict"):
            if payload.get(key) not in (None, ""):
                body[key] = payload[key]
        body = _normalize_pm_payload(body)
        request_hash = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        with self.store.connect() as connection:
            unknown_adds = int(connection.execute("SELECT COUNT(*) FROM operation_ledger WHERE operation_type='add_resource' AND idempotency_key=? AND response_state='unknown'", (logical_key,)).fetchone()[0])
        if unknown_adds >= 1 and attempt_no > 2:
            raise DispatchProtocolError("add_resource response remained unknown after one controlled resend")
        add_ledger = self.store.begin_operation(
            operation_type="add_resource",
            idempotency_key=logical_key,
            target_uri=str(payload.get("target_uri") or ""),
            request_hash=request_hash,
            namespace_epoch=str(item.get("namespace_epoch") or payload.get("namespace_epoch") or "v4"),
            attempt=attempt_no,
        )
        if add_ledger.get("response_state") in {"accepted", "completed"}:
            try:
                response = json.loads(str(add_ledger.get("response_json") or "{}"))
            except json.JSONDecodeError:
                response = {}
        elif add_ledger.get("deduplicated") and add_ledger.get("response_state") in {"unknown", "pending"}:
            raise DispatchTransportError("add_resource response is unknown; controlled retry required")
        else:
            try:
                response = self.transport.add_resource(
                    body,
                    timeout=float(payload.get("timeout") or 30.0),
                    idempotency_key=logical_key,
                )
            except Exception as exc:
                self.store.finish_operation(str(add_ledger["operation_id"]), response_state="unknown", response={"error": str(exc), "request_hash": request_hash})
                raise
            self.store.finish_operation(str(add_ledger["operation_id"]), response_state="completed", response=response)
        status, task = _walk_status(response)
        if status not in SUCCESS_STATUSES and status not in OBSERVATION_ACTIVE:
            raise DispatchProtocolError(f"OpenViking resource submission returned status={status or 'missing'}")
        task_id = task or _task_id(response)
        if status in OBSERVATION_ACTIVE and not task_id:
            # An accepted/processing response without a durable task handle
            # cannot be observed later. Treat it as a protocol failure instead
            # of acknowledging an item that would remain permanently pending.
            raise DispatchProtocolError("OpenViking accepted response returned no task id")
        read_back = self._read_back(item, payload)
        return response, task_id, status, read_back

    def dispatch_pending(self, *, limit: int = 20, outbox_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        # Claim one item at a time. Fast-vector gets the first opportunity on
        # every turn, so semantic descendants cannot consume its reserved
        # capacity or create head-of-line blocking. Each claim transaction is
        # fully closed before _send performs network I/O.
        for _ in range(max(0, int(limit))):
            claimed = self.gateway.dispatch_once(limit=1, lane="fast-vector", outbox_ids=outbox_ids)
            if not claimed:
                claimed = self.gateway.dispatch_once(limit=1, lane="semantic", outbox_ids=outbox_ids)
            if not claimed:
                break
            item = claimed[0]
            outbox_id = str(item["outbox_id"])
            try:
                response, task_id, remote_status, read_back = self._send(item)
                semantic_status = (
                    "completed"
                    if remote_status in OBSERVATION_TERMINAL_SUCCESS
                    else "accepted"
                    if remote_status == "accepted"
                    else "processing"
                    if remote_status in OBSERVATION_ACTIVE
                    else "accepted"
                )
                self.gateway.ack(
                    outbox_id,
                    openviking_task_id=task_id,
                    dispatch_token=item.get("dispatch_token"),
                    semantic_status=semantic_status,
                    content_verified=bool(read_back.get("verified")),
                )
                results.append({**item, "status": "completed", "semantic_status": semantic_status, "openviking_task_id": task_id, "content_read_back": read_back, "response": response})
            except DispatchHTTPError as exc:
                category = "429" if exc.status_code == 429 else ("transient" if exc.status_code in TRANSIENT_HTTP_STATUSES else "permanent")
                failure = self.gateway.fail(
                    outbox_id,
                    category=category,
                    detail=f"HTTP {exc.status_code}: {exc.body}",
                    retry_after=exc.retry_after,
                    dispatch_token=item.get("dispatch_token"),
                )
                results.append({**item, **failure})
            except DispatchTransportError as exc:
                failure = self.gateway.fail(outbox_id, category=getattr(exc, "category", "connection"), detail=str(exc), dispatch_token=item.get("dispatch_token"))
                results.append({**item, **failure})
            except (DispatchProtocolError, OSError, ValueError) as exc:
                failure = self.gateway.fail(outbox_id, category="permanent", detail=str(exc), dispatch_token=item.get("dispatch_token"))
                results.append({**item, **failure})
        return results

    def _update_observation(
        self,
        *,
        semantic_task_id: str,
        local_status: str,
        attempt: int,
        deadline_at: str,
        last_observed_at: str,
        next_attempt_at: Optional[str],
        error_fingerprint: Optional[str],
    ) -> None:
        """Persist task observation state without ever re-enqueuing a resource."""
        at = now_iso()
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE semantic_tasks SET status=?,error_fingerprint=?,updated_at=? "
                "WHERE semantic_task_id=? AND status IN ('accepted','processing')",
                (local_status, error_fingerprint, at, semantic_task_id),
            )
            resource = connection.execute(
                "SELECT resource_id,revision_id,kind FROM semantic_tasks WHERE semantic_task_id=?",
                (semantic_task_id,),
            ).fetchone()
            if resource and local_status == "completed":
                # Legacy resource tasks historically coupled content and
                # semantic completion. Concept tasks must keep Phase A
                # content verification independent and are promoted only by
                # _read_back.
                if str(resource[2]) == "concept":
                    connection.execute(
                        "UPDATE resource_projections SET semantic_state='semantic_completed',semantic_completed_at=COALESCE(semantic_completed_at,?),terminal_reason=NULL,updated_at=? WHERE resource_id=? AND revision_id=?",
                        (at, at, resource[0], resource[1]),
                    )
                else:
                    connection.execute(
                        "UPDATE resource_projections SET content_state='content_verified',semantic_state='semantic_completed',verified_at=COALESCE(verified_at,?),semantic_completed_at=COALESCE(semantic_completed_at,?),terminal_reason=NULL,updated_at=? WHERE resource_id=? AND revision_id=?",
                        (at, at, at, resource[0], resource[1]),
                    )
            elif resource and local_status in {"failed", "quarantine"}:
                connection.execute(
                    "UPDATE resource_projections SET semantic_state=?,terminal_reason=?,updated_at=? WHERE resource_id=? AND revision_id=?",
                    (local_status, error_fingerprint or local_status, at, resource[0], resource[1]),
                )
            connection.execute(
                """
                INSERT INTO semantic_task_observations(
                    semantic_task_id,observation_attempt,last_observed_at,next_attempt_at,deadline_at,last_error_fingerprint
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(semantic_task_id) DO UPDATE SET
                    observation_attempt=excluded.observation_attempt,
                    last_observed_at=excluded.last_observed_at,
                    next_attempt_at=excluded.next_attempt_at,
                    deadline_at=excluded.deadline_at,
                    last_error_fingerprint=excluded.last_error_fingerprint
                """,
                (semantic_task_id, int(attempt), last_observed_at, next_attempt_at, deadline_at, error_fingerprint),
            )
        if local_status in {"completed", "failed", "quarantine"}:
            task = self.store.connect()
            try:
                row = task.execute("SELECT outbox_id FROM semantic_tasks WHERE semantic_task_id=?", (semantic_task_id,)).fetchone()
            finally:
                task.close()
            if row is not None:
                self.gateway.release_profile_reservation(str(row[0]))

    @staticmethod
    def _observation_fingerprint(category: str, detail: Any = "") -> str:
        return hashlib.sha256(f"semantic_observation:{category}:{detail}".encode("utf-8")).hexdigest()[:20]

    def reconcile_content(self, *, limit: int = 20, min_age_seconds: int = 0, outbox_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        """Verify accepted resources without touching semantic task state."""
        if limit <= 0:
            return []
        selected_ids = [str(value) for value in (outbox_ids or ()) if str(value)]
        if outbox_ids is not None and not selected_ids:
            return []
        id_clause = ""
        id_params: Tuple[Any, ...] = ()
        if outbox_ids is not None:
            id_clause = " AND o.outbox_id IN (" + ",".join("?" for _ in selected_ids) + ")"
            id_params = tuple(selected_ids)
        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT o.outbox_id,o.idempotency_key,o.kind,o.resource_id,o.revision_id,
                       o.processing_mode,o.profile,o.owner,o.namespace_epoch,o.payload_json,
                       o.attempt,o.updated_at
                FROM outbox_items AS o
                JOIN resource_projections AS p ON p.resource_id=o.resource_id AND p.revision_id=o.revision_id
                WHERE o.status='completed' AND p.content_state!='content_verified'
                  AND o.kind IN ('resource','concept')
                  AND o.updated_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
                """ + id_clause + """
                ORDER BY o.updated_at
                LIMIT ?
                """,
                (f"-{max(0, int(min_age_seconds))} seconds",) + id_params + (max(0, int(limit)),),
            ).fetchall()
        verified: List[Dict[str, Any]] = []
        for row in rows:
            item = {
                "outbox_id": row[0],
                "idempotency_key": row[1],
                "kind": row[2],
                "resource_id": row[3],
                "revision_id": row[4],
                "processing_mode": row[5],
                "profile": row[6],
                "owner": row[7],
                "namespace_epoch": row[8],
                "attempt": row[10],
            }
            payload = json.loads(row[9] or "{}")
            result = self._read_back(item, payload if isinstance(payload, dict) else {})
            verified.append({**item, "content_read_back": result})
        return verified

    def reconcile_tasks(self, *, limit: int = 20, min_age_seconds: int = 5, outbox_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        """Observe accepted tasks and force unknown/orphan work to a local terminal state.

        OpenViking task visibility is bounded by TTL and API scope. Observation
        errors therefore use a separate, durable budget. Once the budget or
        deadline is exhausted the local task becomes ``quarantine``; the
        resource is never submitted again merely because its task disappeared.
        """
        if limit <= 0:
            return []
        selected_ids = [str(value) for value in (outbox_ids or ()) if str(value)]
        if outbox_ids is not None and not selected_ids:
            return []
        id_clause = ""
        id_params: Tuple[Any, ...] = ()
        if outbox_ids is not None:
            id_clause = " AND t.outbox_id IN (" + ",".join("?" for _ in selected_ids) + ")"
            id_params = tuple(selected_ids)
        now = datetime.now(timezone.utc)
        now_text = self._timestamp(now)
        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT t.semantic_task_id,t.outbox_id,t.openviking_task_id,t.status,t.updated_at,t.created_at,
                       COALESCE(o.observation_attempt,0),o.last_observed_at,o.next_attempt_at,o.deadline_at
                FROM semantic_tasks AS t
                LEFT JOIN semantic_task_observations AS o ON o.semantic_task_id=t.semantic_task_id
                WHERE t.status IN ('accepted','processing')
                  AND t.openviking_task_id IS NOT NULL
                  AND t.updated_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
                  AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?)
                """ + id_clause + """
                ORDER BY t.updated_at
                LIMIT ?
                """,
                (f"-{max(0, int(min_age_seconds))} seconds", now_text) + id_params + (max(0, int(limit)),),
            ).fetchall()
        observed: List[Dict[str, Any]] = []
        for row in rows:
            task_id = str(row[0])
            openviking_task_id = str(row[2])
            current_status = str(row[3]) if str(row[3]) in {"accepted", "processing"} else "accepted"
            attempt = int(row[6] or 0)
            deadline_at = str(row[9] or self._observation_deadline(row[5], now=now))
            deadline = self._parse_timestamp(deadline_at) or (now + timedelta(seconds=self.observation_deadline_seconds))
            try:
                response = self.transport.get_task(openviking_task_id, timeout=10.0)
                remote_status, _ = _walk_status(response)
                if remote_status in OBSERVATION_ACTIVE:
                    local_status = "processing"
                    fingerprint = None
                elif remote_status in OBSERVATION_TERMINAL_SUCCESS:
                    local_status = "completed"
                    fingerprint = None
                elif remote_status in OBSERVATION_TERMINAL_FAILURE:
                    local_status = "failed"
                    fingerprint = self._observation_fingerprint("remote_terminal", remote_status)
                else:
                    next_attempt = attempt + 1
                    exhausted = next_attempt >= self.observation_max_attempts or now >= deadline
                    local_status = "quarantine" if exhausted else current_status
                    fingerprint = self._observation_fingerprint("unknown_status", remote_status or "missing")
                    next_at = None if exhausted else self._observation_retry_at(next_attempt, now=now)
                    self._update_observation(
                        semantic_task_id=task_id,
                        local_status=local_status,
                        attempt=next_attempt,
                        deadline_at=deadline_at,
                        last_observed_at=now_text,
                        next_attempt_at=next_at,
                        error_fingerprint=fingerprint,
                    )
                    observed.append({
                        "semantic_task_id": task_id,
                        "openviking_task_id": openviking_task_id,
                        "status": local_status,
                        "remote_status": remote_status,
                        "observation_attempt": next_attempt,
                        "observation_deadline_at": deadline_at,
                    })
                    continue
                self._update_observation(
                    semantic_task_id=task_id,
                    local_status=local_status,
                    attempt=0,
                    deadline_at=deadline_at,
                    last_observed_at=now_text,
                    next_attempt_at=None,
                    error_fingerprint=fingerprint,
                )
                observed.append({"semantic_task_id": task_id, "openviking_task_id": openviking_task_id, "status": local_status, "remote_status": remote_status, "observation_attempt": 0, "observation_deadline_at": deadline_at})
            except DispatchHTTPError as exc:
                next_attempt = attempt + 1
                # A task 404 means the provider's task visibility/TTL has
                # ended. Quarantine it immediately rather than replaying
                # the resource and risking a duplicate write.
                missing = exc.status_code == 404
                exhausted = missing or next_attempt >= self.observation_max_attempts or now >= deadline
                local_status = "quarantine" if exhausted else current_status
                fingerprint = self._observation_fingerprint("remote_task_not_found" if missing else "http", exc.status_code)
                next_at = None if exhausted else self._observation_retry_at(next_attempt, now=now)
                self._update_observation(
                    semantic_task_id=task_id,
                    local_status=local_status,
                    attempt=next_attempt,
                    deadline_at=deadline_at,
                    last_observed_at=now_text,
                    next_attempt_at=next_at,
                    error_fingerprint=fingerprint,
                )
                observed.append({"semantic_task_id": task_id, "openviking_task_id": openviking_task_id, "status": local_status, "observation_error": f"HTTP {exc.status_code}", "observation_attempt": next_attempt, "observation_deadline_at": deadline_at})
            except (DispatchTransportError, DispatchProtocolError, OSError) as exc:
                next_attempt = attempt + 1
                exhausted = next_attempt >= self.observation_max_attempts or now >= deadline
                local_status = "quarantine" if exhausted else current_status
                fingerprint = self._observation_fingerprint(type(exc).__name__, str(exc))
                next_at = None if exhausted else self._observation_retry_at(next_attempt, now=now)
                self._update_observation(
                    semantic_task_id=task_id,
                    local_status=local_status,
                    attempt=next_attempt,
                    deadline_at=deadline_at,
                    last_observed_at=now_text,
                    next_attempt_at=next_at,
                    error_fingerprint=fingerprint,
                )
                observed.append({"semantic_task_id": task_id, "openviking_task_id": openviking_task_id, "status": local_status, "observation_error": type(exc).__name__, "observation_attempt": next_attempt, "observation_deadline_at": deadline_at})
        return observed

    def status(self, outbox_id: str) -> Dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute(
                """
                SELECT o.outbox_id,o.status,o.attempt,o.next_attempt_at,o.error_fingerprint,
                       s.status,s.openviking_task_id
                FROM outbox_items AS o
                LEFT JOIN semantic_tasks AS s ON s.outbox_id=o.outbox_id
                WHERE o.outbox_id=?
                """,
                (outbox_id,),
            ).fetchone()
        if row is None:
            raise KeyError(outbox_id)
        return {
            "outbox_id": row[0],
            "outbox_status": row[1],
            "attempt": int(row[2]),
            "next_attempt_at": row[3],
            "error_fingerprint": row[4],
            "semantic_status": row[5],
            "openviking_task_id": row[6],
        }

    def wait_for_completion(self, outbox_id: str, *, timeout: float = 30.0, poll_interval: float = 0.25) -> Dict[str, Any]:
        """Wait for one explicitly requested resource to reach a terminal state.

        This is deliberately opt-in.  Normal workers only acknowledge the
        Outbox and observe accepted tasks asynchronously, while strict callers
        get a bounded task poll that never re-submits the resource.
        """
        deadline = time.monotonic() + max(0.1, float(timeout or 30.0))
        interval = max(0.05, float(poll_interval or 0.25))
        while True:
            state = self.status(outbox_id)
            semantic = str(state.get("semantic_status") or "")
            if semantic == "completed":
                return {**state, "status": "accepted", "strict_status": "completed"}
            if semantic in {"failed", "quarantine", "dead_letter"}:
                return {**state, "status": semantic, "strict_status": "failed"}
            if state.get("openviking_task_id"):
                self.reconcile_tasks(limit=1, min_age_seconds=0)
            if time.monotonic() >= deadline:
                return {
                    **self.status(outbox_id),
                    "status": "accepted",
                    "strict_status": "timeout",
                    "strict_error": "resource task did not reach a terminal state before deadline",
                }
            time.sleep(min(interval, max(0.01, deadline - time.monotonic())))

    def submit_file(self, *, dispatch_now: bool = False, **kwargs: Any) -> Dict[str, Any]:
        wait_requested = bool(kwargs.get("wait", False) or kwargs.get("strict", False))
        accepted = self.enqueue_file(**kwargs)
        outbox_id = str(accepted["outbox_id"])
        # The normal source-adapter path is enqueue-only: acceptance is one
        # short local SQLite transaction and never waits on OpenViking.  An
        # explicit wait/strict request opts into one bounded network attempt.
        if (dispatch_now or wait_requested) and accepted.get("outbox_status") not in {"completed", "failed", "dead_letter"}:
            self.dispatch_pending(limit=1)
        if wait_requested:
            waited = self.wait_for_completion(outbox_id, timeout=float(kwargs.get("timeout") or 30.0))
            return {**accepted, **waited}
        state = self.status(outbox_id)
        if state["outbox_status"] == "completed":
            return {**accepted, **state, "status": "accepted"}
        if state["outbox_status"] in {"failed", "dead_letter"}:
            return {**accepted, **state, "status": state["outbox_status"]}
        return {**accepted, **state, "status": "queued"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    submit = sub.add_parser("submit")
    submit.add_argument("--file", required=True, type=Path)
    submit.add_argument("--to", required=True)
    submit.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    submit.add_argument("--kind", choices=("resource", "concept"), default="resource")
    submit.add_argument("--processing-mode", choices=("semantic_and_vectors", "semantic_only", "vectors_only"), default="vectors_only")
    submit.add_argument("--provider", default="openviking")
    submit.add_argument("--profile", default="pm-resource")
    submit.add_argument("--instruction", default="")
    submit.add_argument("--wait", action="store_true")
    submit.add_argument("--timeout", type=float, default=30.0)
    submit.add_argument("--strict", action="store_true")
    submit.add_argument("--dispatch-now", action="store_true", help="explicit canary/debug network attempt; normal submit is enqueue-only")

    dispatch = sub.add_parser("dispatch")
    dispatch.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    dispatch.add_argument("--limit", type=int, default=20)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    dispatcher = PMResourceDispatcher(PMSystemStore(args.db_path))
    if args.command == "submit":
        result = dispatcher.submit_file(
            path=args.file,
            target_uri=args.to,
            kind=args.kind,
            processing_mode=args.processing_mode,
            provider=args.provider,
            profile=args.profile,
            instruction=args.instruction,
            wait=args.wait,
            timeout=args.timeout,
            strict=args.strict,
            dispatch_now=args.dispatch_now,
        )
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        submitted = result.get("status") in {"accepted", "queued"}
        waited = not (args.wait or args.strict) or result.get("strict_status") == "completed"
        return 0 if submitted and waited else 1
    results = dispatcher.dispatch_pending(limit=args.limit)
    print(json.dumps(results, ensure_ascii=False, separators=(",", ":")))
    return 0 if all(item.get("status") not in {"failed", "dead_letter"} for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
