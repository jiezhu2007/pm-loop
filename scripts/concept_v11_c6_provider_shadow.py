#!/usr/bin/env python3
"""Run one bounded, isolated provider shadow for concept semantic ingestion.

The shadow deliberately bypasses the production PM Outbox because concept
admission remains disabled.  It exercises the same OpenViking REST contract in
an isolated namespace: upload, ``wait=false`` add-resource, bounded task
observation, and independent content read-back.  It never writes the PM
coordination database and never submits the production concept namespace.

OpenViking 0.4.x does not expose the actual VLM model selected for a resource
task.  The report therefore records ``model_requested=auto`` and
``model_resolution_status=unknown`` even when the local ``ov.conf`` contains a
configured model.  Under ADR-169, the local provider configuration and the
active ``oneapi/auto`` policy are the trusted routing contract; the missing
per-request model identity remains observable but is not an admission gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional


ROOT_PREFIX = "viking://resources/__pm_v11_provider_shadow__/"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_OBSERVATION_SECONDS = 120.0
DEFAULT_POLL_SECONDS = 5.0
TERMINAL_SUCCESS = {"completed", "complete", "success", "succeeded", "done"}
TERMINAL_FAILURE = {"failed", "error", "cancelled", "canceled", "dead_letter", "quarantine"}
ACTIVE = {"accepted", "queued", "pending", "running", "processing", "in_progress"}
MODEL_RESOLUTION_GATE = "provider_configuration_trusted"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _hash_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _hash_json(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _hash_bytes(encoded)


def _status(value: Any) -> str:
    if isinstance(value, Mapping):
        raw = value.get("status")
        if isinstance(raw, str) and raw.strip():
            normalized = raw.strip().lower()
            if normalized not in {"ok", "success", "created"}:
                return normalized
        for key, child in value.items():
            if str(key).lower() in {"error", "errors"}:
                continue
            found = _status(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _status(child)
            if found:
                return found
    return ""


def _task_id(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key in ("task_id", "taskId", "id"):
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


def _content_hash(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key in ("content_hash", "content_sha256", "sha256", "hash"):
            candidate = value.get(key)
            if candidate:
                raw = str(candidate).removeprefix("sha256:").strip().lower()
                if raw:
                    return raw
        for key, child in value.items():
            if str(key).lower() in {"content", "text", "body", "data", "result"} and isinstance(child, str):
                return hashlib.sha256(child.encode("utf-8")).hexdigest()
            # Do not hash arbitrary status/error scalar fields.  Only descend
            # into structured children so ``status=ok`` cannot masquerade as
            # the persisted document body.
            if isinstance(child, (Mapping, list)):
                found = _content_hash(child)
                if found:
                    return found
    elif isinstance(value, list):
        for child in value:
            found = _content_hash(child)
            if found:
                return found
    elif isinstance(value, str):
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
    return None


def _configured_model() -> Optional[str]:
    path = Path.home() / ".openviking" / "ov.conf"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    value = payload.get("vlm", {}).get("model") if isinstance(payload, Mapping) else None
    return str(value).strip() if value else None


class HTTPTransport:
    """Small transport used by the shadow; credentials are never returned."""

    def __init__(self, *, base_url: str, api_key: str = "", timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = max(1.0, float(timeout))

    def _request(self, method: str, path: str, *, body: Any = None, raw: bytes | None = None, content_type: str = "application/json", timeout: Optional[float] = None) -> Any:
        data = raw if raw is not None else (json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None)
        headers = {"Accept": "application/json", "Content-Type": content_type}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=max(1.0, float(timeout or self.timeout))) as response:
                payload = response.read()
                content = response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
        if "json" in content.lower() or payload.lstrip().startswith((b"{", b"[")):
            return json.loads(payload.decode("utf-8"))
        return payload.decode("utf-8", errors="replace")

    def upload_file(self, path: Path, *, timeout: float) -> Any:
        boundary = "----pm-v11-c6-" + uuid.uuid4().hex
        filename = path.name
        ascii_filename = filename.encode("ascii", "ignore").decode("ascii") or "upload"
        encoded_filename = urllib.parse.quote(filename, safe="")
        disposition = (
            f'Content-Disposition: form-data; name="file"; filename="{ascii_filename}"; '
            f"filename*=UTF-8''{encoded_filename}\r\n"
        )
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        payload = b"".join(
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
        return self._request("POST", "/api/v1/resources/temp_upload", raw=payload, content_type=f"multipart/form-data; boundary={boundary}", timeout=timeout)

    def add_resource(self, *, temp_file_id: str, target_uri: str, timeout: float) -> Any:
        return self._request(
            "POST",
            "/api/v1/resources",
            body={
                "temp_file_id": temp_file_id,
                "to": target_uri,
                "create_parent": True,
                "wait": False,
                "processing_mode": "semantic_and_vectors",
            },
            timeout=timeout,
        )

    def get_task(self, task_id: str, *, timeout: float) -> Any:
        return self._request("GET", f"/api/v1/tasks/{urllib.parse.quote(task_id, safe='')}", timeout=timeout)

    def read_content(self, uri: str, *, timeout: float) -> Any:
        query = urllib.parse.urlencode({"uri": uri, "offset": 0, "limit": -1, "raw": "false"})
        return self._request("GET", f"/api/v1/content/read?{query}", timeout=timeout)

    def glob_content(self, uri: str, *, timeout: float) -> Any:
        return self._request(
            "POST",
            "/api/v1/search/glob",
            body={"uri": uri, "pattern": "*", "node_limit": 32},
            timeout=timeout,
        )


def _file_matches(value: Any, *, target_uri: str) -> list[str]:
    """Return the file leaves created below an isolated resource root."""
    result = value.get("result") if isinstance(value, Mapping) else None
    matches = result.get("matches") if isinstance(result, Mapping) else None
    if not isinstance(matches, list):
        return []
    prefix = target_uri.rstrip("/") + "/"
    return [
        str(uri)
        for uri in matches
        if isinstance(uri, str) and uri.startswith(prefix) and uri != target_uri.rstrip("/")
    ]


def _load_transport(base_url: Optional[str], timeout: float) -> HTTPTransport:
    config: dict[str, Any] = {}
    path = Path.home() / ".openviking" / "ovcli.conf"
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, Mapping):
            config.update(loaded)
    except (OSError, ValueError):
        pass
    return HTTPTransport(
        base_url=str(base_url or config.get("url") or "http://127.0.0.1:1933"),
        api_key=str(config.get("api_key") or os.environ.get("OPENVIKING_API_KEY") or ""),
        timeout=timeout,
    )


def run_provider_shadow(
    *,
    transport: HTTPTransport,
    source: Path,
    target_uri: str,
    approved_model: str,
    observation_seconds: float = DEFAULT_OBSERVATION_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    request_timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if not target_uri.startswith(ROOT_PREFIX):
        raise ValueError(f"target URI must stay under isolated prefix {ROOT_PREFIX}")
    if approved_model.strip() != "auto":
        raise ValueError("approved_model must be auto for the active oneapi/auto policy")
    source = source.expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    source_bytes = source.read_bytes()
    expected_hash = _hash_bytes(source_bytes)
    observed_at = _now()
    report: dict[str, Any] = {
        "schema": "concept-v11.c6-provider-shadow.v1",
        "stage_id": "C6-PROVIDER-SHADOW",
        "observed_at": observed_at,
        "status": "HOLD",
        "read_only_pm_database": True,
        "concept_admission_changed": False,
        "target_uri": target_uri,
        "namespace_isolated": True,
        "processing_mode": "semantic_and_vectors",
        "wait": False,
        "model_requested": "auto",
        "approved_model_for_shadow": approved_model,
        "configured_model_observation": _configured_model(),
        "model_resolved": None,
        "model_resolution_status": "unknown",
        "model_resolution_gate": MODEL_RESOLUTION_GATE,
        "model_resolution_gate_status": "not_required",
        "model_resolution_evidence": "OpenViking task response has no model field; actual model identity remains unknown and does not block the trusted provider-configuration contract",
        "transport_model_field": "not_supported_by_resource_api",
        "source_bytes": len(source_bytes),
        "source_hash": expected_hash,
        "errors": [],
        "external_provider_calls": 0,
    }
    started = time.perf_counter()
    try:
        upload_started = time.perf_counter()
        uploaded = transport.upload_file(source, timeout=max(120.0, request_timeout))
        report["upload_latency_ms"] = round((time.perf_counter() - upload_started) * 1000.0, 3)
        result = uploaded.get("result") if isinstance(uploaded, Mapping) else None
        temp_id = (result or {}).get("temp_file_id") if isinstance(result, Mapping) else None
        temp_id = temp_id or (uploaded.get("temp_file_id") if isinstance(uploaded, Mapping) else None)
        if not temp_id:
            raise RuntimeError("temp_upload_missing_id")

        accepted_started = time.perf_counter()
        accepted = transport.add_resource(temp_file_id=str(temp_id), target_uri=target_uri, timeout=request_timeout)
        report["accepted_latency_ms"] = round((time.perf_counter() - accepted_started) * 1000.0, 3)
        report["accepted_response"] = accepted
        task_id = _task_id(accepted)
        report["task_id"] = task_id
        if not task_id:
            raise RuntimeError("accepted_response_missing_task_id")
        report["accepted"] = True
        report["external_provider_calls"] = 1

        deadline = time.monotonic() + max(1.0, float(observation_seconds))
        observations: list[dict[str, Any]] = []
        terminal_response: Any = None
        semantic_started = time.perf_counter()
        while True:
            response = transport.get_task(task_id, timeout=min(10.0, request_timeout))
            task_status = _status(response)
            observations.append({"at": _now(), "status": task_status or "unknown"})
            if task_status in TERMINAL_SUCCESS or task_status in TERMINAL_FAILURE:
                terminal_response = response
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("observation_budget_exhausted")
            time.sleep(max(0.1, float(poll_seconds)))
        report["observations"] = observations
        report["task_terminal_response"] = terminal_response
        report["semantic_latency_ms"] = round((time.perf_counter() - semantic_started) * 1000.0, 3)
        final_status = _status(terminal_response)
        report["remote_status"] = final_status or "unknown"
        if final_status not in TERMINAL_SUCCESS:
            raise RuntimeError(f"remote_terminal_status:{final_status or 'unknown'}")
        task_result = (terminal_response.get("result") if isinstance(terminal_response, Mapping) else None) or {}
        task_result = (task_result.get("result") if isinstance(task_result, Mapping) else None) or {}
        queue_status = task_result.get("queue_status") if isinstance(task_result, Mapping) else None
        queue_status = queue_status or {}
        report["queue_status"] = queue_status
        semantic_metrics = queue_status.get("Semantic") if isinstance(queue_status, Mapping) else None
        report["semantic_processed"] = int((semantic_metrics or {}).get("processed") or 0) if isinstance(semantic_metrics, Mapping) else 0
        report["semantic_requeue_count"] = int((semantic_metrics or {}).get("requeue_count") or 0) if isinstance(semantic_metrics, Mapping) else None
        report["semantic_error_count"] = int((semantic_metrics or {}).get("error_count") or 0) if isinstance(semantic_metrics, Mapping) else None
        report["usage"] = task_result.get("usage") if isinstance(task_result, Mapping) else None

        # OpenViking may normalize the uploaded extension (for example .txt to
        # .md). Discover the created leaf instead of assuming a filename.
        matches = _file_matches(
            transport.glob_content(target_uri, timeout=min(10.0, request_timeout)),
            target_uri=target_uri,
        )
        if len(matches) != 1:
            raise RuntimeError(f"shadow_leaf_count_invalid:{len(matches)}")
        file_uri = matches[0]
        readback_started = time.perf_counter()
        readback = transport.read_content(file_uri, timeout=min(10.0, request_timeout))
        report["read_back_latency_ms"] = round((time.perf_counter() - readback_started) * 1000.0, 3)
        actual_hash = _content_hash(readback)
        report["read_back_uri"] = file_uri
        report["read_back_hash"] = "sha256:" + actual_hash if actual_hash else None
        report["content_verified"] = bool(actual_hash and actual_hash == expected_hash.removeprefix("sha256:"))
        if not report["content_verified"]:
            raise RuntimeError("content_read_back_hash_mismatch")

        report["total_elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
        report["semantic_projection"] = "completed" if report["semantic_processed"] > 0 else "unknown"
        report["status"] = "PASS" if report["semantic_projection"] == "completed" else "HOLD"
        report["next_gate"] = "active oneapi/auto policy binding, isolated task read-back, semantic metrics, and normal Admission authorization; actual model identity is optional diagnostics"
    except Exception as exc:
        report["total_elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
        report["errors"].append(f"{type(exc).__name__}:{exc}")
        report["status"] = "HOLD"
    return report


def reconcile_provider_shadow(
    *,
    transport: HTTPTransport,
    source: Path,
    legacy_report_path: Path,
    readback_uri: str,
    request_timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Reconcile a completed legacy Shadow whose fixed leaf-name assumption failed.

    This makes one read-only content request.  It never uploads, creates a
    semantic task, or changes the PM database.  The original report remains
    untouched and is linked by content hash in the resulting evidence record.
    """
    source = source.expanduser().resolve()
    legacy_report_path = legacy_report_path.expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if not legacy_report_path.is_file():
        raise FileNotFoundError(legacy_report_path)
    if not readback_uri.startswith(ROOT_PREFIX):
        raise ValueError(f"read-back URI must stay under isolated prefix {ROOT_PREFIX}")

    legacy_bytes = legacy_report_path.read_bytes()
    legacy = json.loads(legacy_bytes.decode("utf-8"))
    if not isinstance(legacy, Mapping):
        raise ValueError("legacy provider-shadow report must be a JSON object")
    expected_hash = _hash_bytes(source.read_bytes())
    legacy_errors = legacy.get("errors")
    allowed_legacy_error = (
        isinstance(legacy_errors, list)
        and len(legacy_errors) == 1
        and "File not found:" in str(legacy_errors[0])
    )
    queue = legacy.get("queue_status")
    semantic = queue.get("Semantic") if isinstance(queue, Mapping) else None
    if (
        str(legacy.get("schema") or "") != "concept-v11.c6-provider-shadow.v1"
        or str(legacy.get("stage_id") or "") != "C6-PROVIDER-SHADOW"
        or legacy.get("read_only_pm_database") is not True
        or legacy.get("concept_admission_changed") is not False
        or legacy.get("namespace_isolated") is not True
        or str(legacy.get("model_requested") or "") != "auto"
        or str(legacy.get("approved_model_for_shadow") or "") != "auto"
        or legacy.get("accepted") is not True
        or not str(legacy.get("task_id") or "")
        or str(legacy.get("remote_status") or "").lower() not in TERMINAL_SUCCESS
        or not isinstance(semantic, Mapping)
        or int(semantic.get("processed") or 0) < 1
        or int(semantic.get("requeue_count") or 0) != 0
        or int(semantic.get("error_count") or 0) != 0
        or str(legacy.get("source_hash") or "") != expected_hash
        or not allowed_legacy_error
    ):
        raise ValueError("legacy report is not eligible for leaf-name reconciliation")

    readback = transport.read_content(readback_uri, timeout=min(10.0, request_timeout))
    actual_hash = _content_hash(readback)
    if not actual_hash or "sha256:" + actual_hash != expected_hash:
        raise RuntimeError("content_read_back_hash_mismatch")

    report = dict(legacy)
    report.update(
        {
            "status": "PASS",
            "errors": [],
            "semantic_projection": "completed",
            "content_verified": True,
            "read_back_uri": readback_uri,
            "read_back_hash": "sha256:" + actual_hash,
            "model_resolution_gate": MODEL_RESOLUTION_GATE,
            "model_resolution_gate_status": "not_required",
            "model_resolution_evidence": "OpenViking task response has no model field; actual model identity remains unknown and does not block the trusted provider-configuration contract",
            "transport_model_field": "not_supported_by_resource_api",
            "next_gate": "active oneapi/auto policy binding, isolated task read-back, semantic metrics, and normal Admission authorization; actual model identity is optional diagnostics",
            "reconciled_at": _now(),
            "reconciliation": {
                "kind": "normalized_leaf_readback",
                "legacy_report_path": str(legacy_report_path),
                "legacy_report_sha256": _hash_bytes(legacy_bytes),
                "legacy_status": legacy.get("status"),
                "superseded_errors": legacy_errors,
                "read_only_openviking_requests": 1,
            },
        }
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--namespace", default=None, help="isolated namespace; default derives from a unique run id")
    parser.add_argument("--approved-model", required=True, help="explicit temporary provider-shadow model approval")
    parser.add_argument("--reconcile-from", type=Path, help="legacy C6 report to reconcile without creating a new semantic task")
    parser.add_argument("--readback-uri", help="actual normalized leaf URI for --reconcile-from")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--observation-seconds", type=float, default=DEFAULT_OBSERVATION_SECONDS)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args()
    transport = _load_transport(args.base_url, args.timeout)
    if args.reconcile_from:
        if not args.readback_uri:
            parser.error("--readback-uri is required with --reconcile-from")
        report = reconcile_provider_shadow(
            transport=transport,
            source=args.source,
            legacy_report_path=args.reconcile_from,
            readback_uri=args.readback_uri,
            request_timeout=args.timeout,
        )
    else:
        if args.readback_uri:
            parser.error("--readback-uri is only valid with --reconcile-from")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        target = args.namespace or f"{ROOT_PREFIX}{run_id}/sample"
        report = run_provider_shadow(
            transport=transport,
            source=args.source,
            target_uri=target,
            approved_model=args.approved_model,
            observation_seconds=args.observation_seconds,
            poll_seconds=args.poll_seconds,
            request_timeout=args.timeout,
        )
    output = args.report.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"].startswith("PASS") else 1


if __name__ == "__main__":
    raise SystemExit(main())
