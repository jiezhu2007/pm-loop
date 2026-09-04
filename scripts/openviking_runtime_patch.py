#!/usr/bin/env python3
"""Apply and audit the local OpenViking queue reliability patch.

The OpenViking package is installed outside this repository.  This script keeps
the small, version-pinned edits reproducible, creates a backup before changing
anything, and writes a manifest with the before/after hashes.  It is intentionally
conservative: an unexpected package version or source layout aborts without
changing files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(
    os.environ.get(
        "OPENVIKING_PACKAGE_ROOT",
        str(
            Path(sys.executable).resolve().parent.parent
            / "lib"
            / f"python{sys.version_info.major}.{sys.version_info.minor}"
            / "site-packages"
        ),
    )
).expanduser().resolve()
OPENVIKING_ROOT = PACKAGE_ROOT / "openviking"
PATCH_ROOT = Path.home() / ".openviking" / "runtime-patches"
MANIFEST_PATH = PATCH_ROOT / "openviking-queue-reliability.json"
PATCH_VERSION = "pm-queue-reliability-v1.1"
BASE_PATCH_VERSION = "pm-queue-reliability-v1"
CANCEL_SAFE_MARKER = "pm-queue-reliability-v1.1-cancel-safe"
DASHBOARD_SKILL_COUNT_PATCH_VERSION = "pm-dashboard-skill-count-v1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_file(path: Path) -> None:
    if not path.is_file():
        raise RuntimeError(f"required OpenViking source file is missing: {path}")


def dashboard_inventory_update(text: str) -> str:
    """Count the shared installed skill catalog on the Studio dashboard."""
    old = '            self._stat_count(f"{user_root}/skills", ctx=ctx),\n'
    new = (
        '            # Dashboard "skills" represents the installed/shared agent skill catalog.\n'
        '            # User-scoped session skills are not the catalog shown by Studio.\n'
        '            self._stat_count("viking://agent/skills", ctx=ctx),\n'
    )
    return replace_once(text, old, new, label="dashboard shared skill inventory root")


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count == 0:
        raise RuntimeError(f"expected patch anchor not found: {label}")
    if count != 1:
        raise RuntimeError(f"patch anchor is not unique: {label} ({count} matches)")
    return text.replace(old, new, 1)


def replace_n(text: str, old: str, new: str, *, expected: int, label: str) -> str:
    count = text.count(old)
    if count != expected:
        raise RuntimeError(f"expected {expected} patch anchors for {label}, found {count}")
    return text.replace(old, new)


def model_retry_update(text: str) -> str:
    old = """from openviking.utils.exceptions import AllCredentialsFailedError\n\nlogger = logging.getLogger(__name__)\n"""
    new = """from openviking.utils.exceptions import AllCredentialsFailedError\n\ntry:\n    from openviking_cli.exceptions import NotFoundError as _OpenVikingNotFoundError\nexcept ImportError:  # pragma: no cover - older OpenViking versions\n    _OpenVikingNotFoundError = None\n\nlogger = logging.getLogger(__name__)\n"""
    text = replace_once(text, old, new, label="model_retry optional NotFoundError import")

    old = """ERROR_CLASS_QUOTA_EXCEEDED = \"quota_exceeded\"\nERROR_CLASS_TRANSIENT = \"transient\"  # request may succeed later\n"""
    new = """ERROR_CLASS_QUOTA_EXCEEDED = \"quota_exceeded\"\nERROR_CLASS_INVALID_RESOURCE = \"invalid_resource\"  # resource cannot succeed by retrying\nERROR_CLASS_TRANSIENT = \"transient\"  # request may succeed later\n"""
    if old in text:
        text = replace_once(text, old, new, label="model_retry invalid-resource class")
    else:
        old = """ERROR_CLASS_QUOTA_EXCEEDED = \"quota_exceeded\"\nERROR_CLASS_TRANSIENT = \"transient\"\n"""
        new = """ERROR_CLASS_QUOTA_EXCEEDED = \"quota_exceeded\"\nERROR_CLASS_INVALID_RESOURCE = \"invalid_resource\"  # resource cannot succeed by retrying\nERROR_CLASS_TRANSIENT = \"transient\"\n"""
        text = replace_once(text, old, new, label="model_retry invalid-resource class (alternate)")

    old = """_PERMANENT_IO_ERRORS = (FileNotFoundError, PermissionError, IsADirectoryError, NotADirectoryError)\n"""
    new = """_PERMANENT_IO_ERRORS = (\n    FileNotFoundError,\n    PermissionError,\n    IsADirectoryError,\n    NotADirectoryError,\n) + ((_OpenVikingNotFoundError,) if _OpenVikingNotFoundError is not None else ())\n"""
    text = replace_once(text, old, new, label="model_retry permanent IO errors")

    old = """    for exc in (error, getattr(error, \"__cause__\", None)):\n        if exc is not None and isinstance(exc, _PERMANENT_IO_ERRORS):\n            return ERROR_CLASS_PERMANENT\n"""
    new = """    for exc in (error, getattr(error, \"__cause__\", None)):\n        if exc is not None and _OpenVikingNotFoundError is not None and isinstance(\n            exc, _OpenVikingNotFoundError\n        ):\n            return ERROR_CLASS_INVALID_RESOURCE\n        if exc is not None and isinstance(exc, _PERMANENT_IO_ERRORS):\n            return ERROR_CLASS_PERMANENT\n"""
    text = replace_once(text, old, new, label="model_retry invalid-resource classification")

    old = """def rate_limit_retry_delay(attempt: int) -> float:\n    \"\"\"Exponential backoff delay with jitter for LLM rate-limit retries.\"\"\"\n    delay = min(\n        RATE_LIMIT_RETRY_MAX_DELAY_SECONDS,\n        RATE_LIMIT_RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1)),\n    )\n    return delay * random.uniform(0.8, 1.2)\n\n\n"""
    new = """def rate_limit_retry_delay(attempt: int) -> float:\n    \"\"\"Exponential backoff delay with jitter for LLM rate-limit retries.\"\"\"\n    delay = min(\n        RATE_LIMIT_RETRY_MAX_DELAY_SECONDS,\n        RATE_LIMIT_RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1)),\n    )\n    return delay * random.uniform(0.8, 1.2)\n\n\ndef retry_after_seconds(error: BaseException) -> float | None:\n    \"\"\"Read a provider Retry-After hint from an SDK exception if present.\"\"\"\n    for exc in _iter_exception_chain(error):\n        candidates = [getattr(exc, \"retry_after\", None)]\n        response = getattr(exc, \"response\", None)\n        headers = getattr(response, \"headers\", None)\n        if headers is not None:\n            candidates.extend(\n                headers.get(name) for name in (\"retry-after\", \"Retry-After\")\n            )\n        for value in candidates:\n            try:\n                if value is None or str(value).strip() == \"\":\n                    continue\n                seconds = float(value)\n            except (TypeError, ValueError):\n                continue\n            if seconds >= 0:\n                return min(seconds, RATE_LIMIT_RETRY_MAX_DELAY_SECONDS)\n    return None\n\n\n"""
    text = replace_once(text, old, new, label="model_retry retry-after helper")

    old = """            delay = _compute_delay(\n                attempt,\n                base_delay=base_delay,\n                max_delay=max_delay,\n                jitter=jitter,\n            )\n"""
    new = """            if is_retryable_rate_limit_error(e):\n                provider_delay = retry_after_seconds(e)\n                delay = max(\n                    provider_delay or 0.0,\n                    rate_limit_retry_delay(attempt + 1),\n                )\n            else:\n                delay = _compute_delay(\n                    attempt,\n                    base_delay=base_delay,\n                    max_delay=max_delay,\n                    jitter=jitter,\n                )\n"""
    # The fragment occurs once in retry_sync and once in retry_async.  Patch both.
    text = replace_n(text, old, new, expected=2, label="model_retry sync/async backoff")

    return text


def circuit_breaker_update(text: str) -> str:
    old = """        self._current_reset_timeout = reset_timeout\n        self._lock = threading.Lock()\n"""
    new = """        self._current_reset_timeout = reset_timeout\n        self._probe_in_flight = False\n        self._lock = threading.Lock()\n"""
    text = replace_once(text, old, new, label="circuit breaker probe state")

    old = """            if self._state == _STATE_HALF_OPEN:\n                return  # allow probe request\n"""
    new = """            if self._state == _STATE_HALF_OPEN:\n                if self._probe_in_flight:\n                    raise CircuitBreakerOpen(\"Circuit breaker probe already in flight\")\n                self._probe_in_flight = True\n                return\n"""
    text = replace_once(text, old, new, label="circuit breaker single half-open probe")

    old = """                self._state = _STATE_HALF_OPEN\n                logger.info(\"Circuit breaker transitioning OPEN -> HALF_OPEN (timeout elapsed)\")\n                return\n"""
    new = """                self._state = _STATE_HALF_OPEN\n                self._probe_in_flight = True\n                logger.info(\"Circuit breaker transitioning OPEN -> HALF_OPEN (timeout elapsed)\")\n                return\n"""
    text = replace_once(text, old, new, label="circuit breaker half-open transition")

    old = """    def retry_after(self) -> float:\n        \"\"\"Seconds until the breaker may transition to HALF_OPEN, capped at 30s.\n\n        Returns 0 if the breaker is CLOSED or HALF_OPEN.\n        \"\"\"\n        with self._lock:\n            if self._state != _STATE_OPEN:\n                return 0\n            remaining = self._current_reset_timeout - (time.monotonic() - self._last_failure_time)\n            return min(max(remaining, 0), 30)\n"""
    new = """    def retry_after(self) -> float:\n        \"\"\"Seconds until the breaker may transition to HALF_OPEN.\n\n        The full remaining window is returned so callers do not hot-loop every\n        30 seconds while the provider is still known to be unavailable.\n        \"\"\"\n        with self._lock:\n            if self._state == _STATE_OPEN:\n                remaining = self._current_reset_timeout - (\n                    time.monotonic() - self._last_failure_time\n                )\n                return max(remaining, 0)\n            if self._state == _STATE_HALF_OPEN and self._probe_in_flight:\n                return 5.0\n            return 0\n"""
    text = replace_once(text, old, new, label="circuit breaker full retry window")

    old = """            self._failure_count = 0\n            self._state = _STATE_CLOSED\n            self._current_reset_timeout = self._base_reset_timeout\n"""
    new = """            self._failure_count = 0\n            self._state = _STATE_CLOSED\n            self._probe_in_flight = False\n            self._current_reset_timeout = self._base_reset_timeout\n"""
    text = replace_once(text, old, new, label="circuit breaker success probe reset")

    old = """            if self._state == _STATE_HALF_OPEN:\n                self._state = _STATE_OPEN\n"""
    new = """            if self._state == _STATE_HALF_OPEN:\n                self._state = _STATE_OPEN\n                self._probe_in_flight = False\n"""
    text = replace_once(text, old, new, label="circuit breaker failure probe reset")

    return text


def semantic_processor_update(text: str) -> str:
    old = """import asyncio\nimport re\nimport threading\n"""
    new = """import asyncio\nimport random\nimport re\nimport threading\n"""
    text = replace_once(text, old, new, label="semantic processor random jitter import")

    old = """from openviking.utils.model_retry import ERROR_CLASS_INPUT_TOO_LARGE, ERROR_CLASS_PERMANENT\n"""
    new = """from openviking.utils.model_retry import (\n    ERROR_CLASS_INPUT_TOO_LARGE,\n    ERROR_CLASS_INVALID_RESOURCE,\n    ERROR_CLASS_PERMANENT,\n)\n"""
    text = replace_once(text, old, new, label="semantic processor invalid-resource import")

    old = """        wait = self._circuit_breaker.retry_after\n        if wait > 0:\n            await asyncio.sleep(wait)\n"""
    new = """        wait = self._circuit_breaker.retry_after\n        if wait > 0:\n            # Spread wake-ups after a provider outage instead of releasing a burst.\n            jitter = min(10.0, max(0.5, wait * 0.05))\n            await asyncio.sleep(wait + random.uniform(0.0, jitter))\n"""
    text = replace_once(text, old, new, label="semantic processor delayed requeue jitter")

    old = """            elif error_class == ERROR_CLASS_PERMANENT:\n                logger.critical(\n                    f\"Permanent API error processing semantic message, dropping: {e}\",\n                    exc_info=True,\n                )\n                self._circuit_breaker.record_failure(e)\n                if msg is not None:\n                    self._merge_request_stats(msg.telemetry_id, error_count=1)\n                    get_request_wait_tracker().mark_semantic_failed(\n                        msg.telemetry_id, msg.id, str(e)\n                    )\n                self.report_error(str(e), data)\n"""
    new = """            elif error_class in (ERROR_CLASS_PERMANENT, ERROR_CLASS_INVALID_RESOURCE):\n                if error_class == ERROR_CLASS_INVALID_RESOURCE:\n                    logger.error(\n                        f\"Invalid resource in semantic message, quarantining: {e}\",\n                        exc_info=True,\n                    )\n                else:\n                    logger.critical(\n                        f\"Permanent API error processing semantic message, dropping: {e}\",\n                        exc_info=True,\n                    )\n                # A bad URI is request-local and must not open the global provider breaker.\n                if msg is not None:\n                    self._merge_request_stats(msg.telemetry_id, error_count=1)\n                    get_request_wait_tracker().mark_semantic_failed(\n                        msg.telemetry_id, msg.id, str(e)\n                    )\n                self.report_error(str(e), data)\n"""
    text = replace_once(text, old, new, label="semantic processor invalid-resource terminal branch")
    return text


def model_retry_v11_update(text: str) -> str:
    """Add standards-compatible HTTP-date parsing to the v1 retry helper."""
    old = """            try:
                if value is None or str(value).strip() == "":
                    continue
                seconds = float(value)
            except (TypeError, ValueError):
                continue
            if seconds >= 0:
                return min(seconds, RATE_LIMIT_RETRY_MAX_DELAY_SECONDS)
"""
    new = """            try:
                if value is None or str(value).strip() == "":
                    continue
                raw_value = str(value).strip()
                try:
                    seconds = float(raw_value)
                except ValueError:
                    retry_at = email.utils.parsedate_to_datetime(raw_value)
                    if retry_at is None:
                        continue
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=datetime.timezone.utc)
                    seconds = retry_at.timestamp() - time.time()
            except (TypeError, ValueError, OverflowError, IndexError):
                continue
            if seconds >= 0:
                return min(seconds, RATE_LIMIT_RETRY_MAX_DELAY_SECONDS)
"""
    text = replace_once(text, old, new, label="model_retry HTTP-date retry-after")
    text = replace_once(
        text,
        "import asyncio\n",
        "import asyncio\nimport datetime\nimport email.utils\n",
        label="model_retry HTTP-date imports",
    )
    return text


def circuit_breaker_v11_update(text: str) -> str:
    """Make probe ownership explicit and allow non-provider errors to release it."""
    old = """    def check(self) -> None:\n"""
    new = """    def check(self) -> bool:\n"""
    text = replace_once(text, old, new, label="circuit breaker probe ownership signature")
    old = """            if self._state == _STATE_CLOSED:\n                return\n"""
    new = """            if self._state == _STATE_CLOSED:\n                return False\n"""
    text = replace_once(text, old, new, label="circuit breaker closed probe return")
    old = """                self._probe_in_flight = True\n                return\n            # OPEN — check if timeout elapsed\n"""
    new = """                self._probe_in_flight = True\n                return True\n            # OPEN — check if timeout elapsed\n"""
    text = replace_once(text, old, new, label="circuit breaker half-open probe return")
    old = """                self._probe_in_flight = True\n                logger.info(\"Circuit breaker transitioning OPEN -> HALF_OPEN (timeout elapsed)\")\n                return\n"""
    new = """                self._probe_in_flight = True\n                logger.info(\"Circuit breaker transitioning OPEN -> HALF_OPEN (timeout elapsed)\")\n                return True\n"""
    text = replace_once(text, old, new, label="circuit breaker open transition probe return")
    anchor = """            raise CircuitBreakerOpen(\n                f\"Circuit breaker is OPEN, retry after {self._current_reset_timeout - elapsed:.0f}s\"\n            )\n\n    @property\n"""
    replacement = """            raise CircuitBreakerOpen(\n                f\"Circuit breaker is OPEN, retry after {self._current_reset_timeout - elapsed:.0f}s\"\n            )\n\n    def release_probe(self) -> None:\n        \"\"\"Release a HALF_OPEN probe for errors unrelated to provider health.\"\"\"\n        with self._lock:\n            if self._state == _STATE_HALF_OPEN:\n                self._probe_in_flight = False\n\n    @property\n"""
    text = replace_once(text, anchor, replacement, label="circuit breaker release probe")
    return text


def semantic_processor_v11_update(text: str) -> str:
    """Release probes on every task exit and back off path-lock contention."""
    old = """    _max_cached_stats = 256\n    # Queue-level retries are separate from provider-client retries. The\n"""
    new = """    _max_cached_stats = 256\n    LOCK_RETRY_BASE_DELAY_SECONDS = 1.0\n    LOCK_RETRY_MAX_DELAY_SECONDS = 10.0\n    # Queue-level retries are separate from provider-client retries. The\n"""
    text = replace_once(text, old, new, label="semantic processor lock retry limits")
    anchor = """    async def _requeue_semantic_msg_after_error(\n        self,\n        msg: SemanticMsg,\n        data: Optional[Dict[str, Any]],\n        error: Exception,\n    ) -> None:\n"""
    replacement = """    @classmethod\n    def _lock_retry_delay(cls, retry_count: int) -> float:\n        delay = min(\n            cls.LOCK_RETRY_MAX_DELAY_SECONDS,\n            cls.LOCK_RETRY_BASE_DELAY_SECONDS * (2 ** max(0, retry_count - 1)),\n        )\n        return delay * random.uniform(0.8, 1.2)\n\n    async def _requeue_semantic_msg_after_error(\n        self,\n        msg: SemanticMsg,\n        data: Optional[Dict[str, Any]],\n        error: Exception,\n    ) -> None:\n"""
    text = replace_once(text, anchor, replacement, label="semantic processor lock retry helper")
    old = """            if not self._prepare_semantic_retry(msg, error):\n                return\n            await self._reenqueue_semantic_msg(msg)\n"""
    new = """            if not self._prepare_semantic_retry(msg, error):\n                return\n            if isinstance(error, LockAcquisitionError):\n                await asyncio.sleep(self._lock_retry_delay(msg.retry_count))\n            await self._reenqueue_semantic_msg(msg)\n"""
    text = replace_once(text, old, new, label="semantic processor lock retry backoff")
    old = """        msg: Optional[SemanticMsg] = None\n        collector = None\n        try:\n"""
    new = """        msg: Optional[SemanticMsg] = None\n        collector = None\n        probe_claimed = False\n        try:\n"""
    text = replace_once(text, old, new, label="semantic processor probe ownership state")
    old = """                self._circuit_breaker.check()\n            except CircuitBreakerOpen:\n"""
    new = """                probe_claimed = self._circuit_breaker.check()\n            except CircuitBreakerOpen:\n"""
    text = replace_once(text, old, new, label="semantic processor capture probe ownership")
    old = """        except Exception as e:\n            if isinstance(e, LockAcquisitionError):\n"""
    new = """        except Exception as e:\n            # Task-local and provider errors are handled below. The outer\n            # finally releases a claimed HALF_OPEN probe even for cancellation.\n            if isinstance(e, LockAcquisitionError):\n"""
    text = replace_once(text, old, new, label="semantic processor defer probe release")
    old = """            return None\n\n    async def on_cancelled("""
    new = """            return None\n        finally:\n            # pm-queue-reliability-v1.1-cancel-safe\n            # CancelledError inherits BaseException, so Exception handlers do\n            # not cover it. Always release task-owned HALF_OPEN probes here.\n            if probe_claimed:\n                self._circuit_breaker.release_probe()\n\n    async def on_cancelled("""
    text = replace_once(text, old, new, label="semantic processor release probe in finally")
    return text


def semantic_processor_cancel_safe_update(text: str) -> str:
    """Close the HALF_OPEN ownership gap for asyncio task cancellation."""
    if CANCEL_SAFE_MARKER in text:
        return text
    old = """        except Exception as e:\n            # Task-local and provider errors are handled below, but every path\n            # must release a claimed HALF_OPEN probe before doing so. Provider\n            # failures will then reopen the breaker via record_failure().\n            if probe_claimed:\n                self._circuit_breaker.release_probe()\n            if isinstance(e, LockAcquisitionError):\n"""
    new = """        except Exception as e:\n            # Task-local and provider errors are handled below. The outer\n            # finally releases a claimed HALF_OPEN probe even for cancellation.\n            if isinstance(e, LockAcquisitionError):\n"""
    text = replace_once(text, old, new, label="semantic processor cancellation-safe exception path")
    old = """            return None\n\n    async def on_cancelled("""
    new = """            return None\n        finally:\n            # pm-queue-reliability-v1.1-cancel-safe\n            # CancelledError inherits BaseException, so Exception handlers do\n            # not cover it. Always release task-owned HALF_OPEN probes here.\n            if probe_claimed:\n                self._circuit_breaker.release_probe()\n\n    async def on_cancelled("""
    return replace_once(text, old, new, label="semantic processor cancellation-safe finally")


def collection_schemas_v11_update(text: str) -> str:
    """Apply the same probe ownership contract to the embedding worker."""
    old = """        embedding_msg: Optional[EmbeddingMsg] = None\n        report_success = False\n"""
    new = """        embedding_msg: Optional[EmbeddingMsg] = None\n        probe_claimed = False\n        report_success = False\n"""
    text = replace_once(text, old, new, label="embedding processor probe ownership state")
    old = """                    self._circuit_breaker.check()\n                    self._breaker_open_last_log_at = 0.0\n"""
    new = """                    probe_claimed = self._circuit_breaker.check()\n                    self._breaker_open_last_log_at = 0.0\n"""
    text = replace_once(text, old, new, label="embedding processor capture probe ownership")
    old = """        finally:\n            if embedding_msg is not None and request_failed_message is not None:\n"""
    new = """        finally:\n            if probe_claimed:\n                self._circuit_breaker.release_probe()\n            if embedding_msg is not None and request_failed_message is not None:\n"""
    text = replace_once(text, old, new, label="embedding processor release probe")
    return text


def backup_and_write(path: Path, updated: str, backup_dir: Path) -> dict[str, str]:
    before_hash = sha256(path)
    backup_path = backup_dir / path.relative_to(PACKAGE_ROOT)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_path)
    mode = path.stat().st_mode
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return {"path": str(path), "backup": str(backup_path), "before": before_hash, "after": sha256(path)}


def update_config(config_path: Path, backup_dir: Path) -> dict[str, Any]:
    require_file(config_path)
    original = json.loads(config_path.read_text(encoding="utf-8"))
    updated = json.loads(json.dumps(original))
    updated.setdefault("vlm", {})["max_concurrent"] = 2
    workers = updated.setdefault("queue_workers", {})
    workers.setdefault("add_resource", {})["max_concurrent"] = 1
    workers.setdefault("add_resource", {})["file_vectorization_concurrency"] = 4
    workers.setdefault("external_parse", {})["max_concurrent"] = 1
    if updated == original:
        return {"path": str(config_path), "changed": False}

    before_hash = sha256(config_path)
    backup_path = backup_dir / "ov.conf"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(config_path, backup_path)
    fd, temp_name = tempfile.mkstemp(prefix=".ov.conf.", dir=str(config_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(updated, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, config_path.stat().st_mode)
        os.replace(temp_name, config_path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return {
        "path": str(config_path),
        "backup": str(backup_path),
        "changed": True,
        "before": before_hash,
        "after": sha256(config_path),
        "effective_limits": {
            "vlm.max_concurrent": updated["vlm"]["max_concurrent"],
            "queue_workers.add_resource.max_concurrent": workers["add_resource"]["max_concurrent"],
            "queue_workers.add_resource.file_vectorization_concurrency": workers["add_resource"][
                "file_vectorization_concurrency"
            ],
            "queue_workers.external_parse.max_concurrent": workers["external_parse"]["max_concurrent"],
        },
    }


def apply() -> dict[str, Any]:
    if not OPENVIKING_ROOT.is_dir():
        raise RuntimeError(f"OpenViking package root is missing: {OPENVIKING_ROOT}")
    version = "unknown"
    try:
        import importlib.metadata

        version = importlib.metadata.version("openviking")
    except Exception:
        pass
    if version != "0.4.16":
        raise RuntimeError(f"refusing to patch unverified OpenViking version: {version}")

    targets = {
        "model_retry": OPENVIKING_ROOT / "utils" / "model_retry.py",
        "circuit_breaker": OPENVIKING_ROOT / "utils" / "circuit_breaker.py",
        "semantic_processor": OPENVIKING_ROOT / "storage" / "queuefs" / "semantic_processor.py",
        "collection_schemas": OPENVIKING_ROOT / "storage" / "collection_schemas.py",
        "inventory": OPENVIKING_ROOT / "observability" / "usage_audit" / "inventory.py",
    }
    for path in targets.values():
        require_file(path)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = PATCH_ROOT / "backups" / timestamp
    backup_dir.mkdir(parents=True, exist_ok=False)
    changes: list[dict[str, str]] = []

    update_functions = {
        "model_retry": (model_retry_update, model_retry_v11_update),
        "circuit_breaker": (circuit_breaker_update, circuit_breaker_v11_update),
        "semantic_processor": (semantic_processor_update, semantic_processor_v11_update),
        "collection_schemas": (lambda text: text, collection_schemas_v11_update),
        "inventory": (lambda text: text, dashboard_inventory_update),
    }
    for name, path in targets.items():
        before = path.read_text(encoding="utf-8")
        if name == "inventory" and DASHBOARD_SKILL_COUNT_PATCH_VERSION in before:
            continue
        if PATCH_VERSION in before:
            if name == "semantic_processor" and CANCEL_SAFE_MARKER not in before:
                after = semantic_processor_cancel_safe_update(before)
                changes.append(backup_and_write(path, after, backup_dir))
            continue
        base_update, v11_update = update_functions[name]
        after = base_update(before) if BASE_PATCH_VERSION not in before else before
        after = v11_update(after)
        # Mark the patched source so repeated invocations are harmless and auditable.
        marker = (
            DASHBOARD_SKILL_COUNT_PATCH_VERSION
            if name == "inventory"
            else PATCH_VERSION
        )
        after = f"# {marker}\n" + after
        changes.append(backup_and_write(path, after, backup_dir))

    config_change = update_config(Path.home() / ".openviking" / "ov.conf", backup_dir)
    manifest = {
        "patch_version": PATCH_VERSION,
        "package_version": version,
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "backup_dir": str(backup_dir),
        "changes": changes,
        "config": config_change,
    }
    PATCH_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="apply the verified runtime patch")
    parser.add_argument("--manifest", action="store_true", help="print the latest patch manifest")
    args = parser.parse_args()
    try:
        if args.manifest:
            if not MANIFEST_PATH.is_file():
                print("no manifest")
                return 1
            print(MANIFEST_PATH.read_text(encoding="utf-8"), end="")
            return 0
        if not args.apply:
            parser.error("one of --apply or --manifest is required")
        print(json.dumps(apply(), ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
