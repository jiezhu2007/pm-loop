#!/usr/bin/env python3
"""Sync OpenViking `resources/memory` notes with a local mirror.

Model: the local mirror is the source of truth. OpenViking is a downstream
replica that we keep in step whenever a local note changes.

Layout in OpenViking:
    viking://resources/memory/<name>.md/<name>.md   (dir wrapping one file)
Local mirror:
    <mirror>/<name>.md

Commands:
    pull   Download every OV memory note into the local mirror.
    push   Upload local notes to OV (content/write, explicitly local-vector-only, no VLM).
    status Show what differs between local and OV without changing anything.
    watch  Poll the local mirror and push changed/new notes to OV immediately.

All traffic is local to http://127.0.0.1:1933. Memory writes explicitly send
processing_mode=vectors_only, so they do not invoke the remote VLM.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    from pm_system_store import PMSystemStore, now_iso
except ImportError:  # pragma: no cover - direct helper use without PYTHONPATH
    PMSystemStore = None  # type: ignore[assignment,misc]
    now_iso = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")  # type: ignore[assignment]

OV_CLI = Path.home() / ".codex/skills/openviking-rest/scripts/ov_rest.py"
MEMORY_ROOT = "viking://resources/memory"
DEFAULT_MIRROR = Path(__file__).resolve().parent.parent / "memory" / "openviking"
STATE_PATH = Path.home() / ".codex/scripts/state/ov-memory-sync-pending.json"
MEMORY_DB_PATH = Path(os.environ.get("PM_V45_MEMORY_DB_PATH", str(Path.home() / ".codex/pm-loop/state/pm-system.db"))).expanduser()
LOCK_PATH = Path.home() / ".codex/scripts/state/ov-memory-sync.lock"
OV_REQUEST_TIMEOUT = float(os.environ.get("OV_MEMORY_SYNC_TIMEOUT_SEC", "30"))
OV_WAIT = os.environ.get("OV_MEMORY_SYNC_WAIT", "0").lower() in {"1", "true", "yes"}
WATCH_MAX_ATTEMPTS = max(1, int(os.environ.get("OV_MEMORY_SYNC_WATCH_MAX_ATTEMPTS", "5")))
WATCH_BACKOFF_BASE_SEC = max(
    0.0, float(os.environ.get("OV_MEMORY_SYNC_WATCH_BACKOFF_BASE_SEC", "5"))
)
WATCH_BACKOFF_MAX_SEC = max(
    WATCH_BACKOFF_BASE_SEC,
    float(os.environ.get("OV_MEMORY_SYNC_WATCH_BACKOFF_MAX_SEC", "300")),
)
PERMANENT_WATCH_ERROR_MARKERS = (
    "http 400",
    "http 401",
    "http 403",
    "http 404",
    "http 422",
    "not found",
    "permission denied",
    "invalid resource",
    "invalid argument",
)

_ACTIVE_PROCESS_GROUPS: dict[int, subprocess.Popen] = {}
_ACTIVE_PROCESS_GROUPS_LOCK = threading.RLock()
_PARENT_SIGNAL_HANDLERS_INSTALLED = False
_PARENT_SIGNAL_SEEN = False
PROCESS_GROUP_TERM_GRACE = max(
    0.05, float(os.environ.get("OV_MEMORY_SYNC_TERM_GRACE_SEC", "1"))
)


class OpenVikingProbeInconclusive(RuntimeError):
    pass


class AlreadyRunning(RuntimeError):
    pass


def _register_process_group(process: subprocess.Popen) -> None:
    with _ACTIVE_PROCESS_GROUPS_LOCK:
        _ACTIVE_PROCESS_GROUPS[process.pid] = process


def _unregister_process_group(pid: int) -> None:
    with _ACTIVE_PROCESS_GROUPS_LOCK:
        _ACTIVE_PROCESS_GROUPS.pop(pid, None)


def _signal_process_group(process: subprocess.Popen, signum: int) -> None:
    """Signal an isolated session and fall back to the direct child."""
    try:
        os.killpg(process.pid, signum)
        return
    except (ProcessLookupError, PermissionError):
        pass
    if process.poll() is None:
        try:
            process.send_signal(signum)
        except (ProcessLookupError, PermissionError):
            pass


def _terminate_process_group(
    process: subprocess.Popen,
    *,
    grace: float = PROCESS_GROUP_TERM_GRACE,
) -> None:
    """Bounded TERM/KILL teardown that reaps the direct child."""
    _signal_process_group(process, signal.SIGTERM)
    try:
        process.wait(timeout=max(0.05, grace))
    except (subprocess.TimeoutExpired, OSError, ChildProcessError):
        pass
    # A leader can exit while a descendant ignores TERM; always sweep the
    # session before returning from an external signal handler.
    _signal_process_group(process, signal.SIGKILL)
    try:
        process.wait(timeout=max(0.1, grace))
    except subprocess.TimeoutExpired:
        process.wait()


def _forward_parent_signal(signum, _frame):
    global _PARENT_SIGNAL_SEEN
    if _PARENT_SIGNAL_SEEN:
        raise SystemExit(128 + signum)
    _PARENT_SIGNAL_SEEN = True
    with _ACTIVE_PROCESS_GROUPS_LOCK:
        processes = tuple(_ACTIVE_PROCESS_GROUPS.values())
    for process in processes:
        _terminate_process_group(process)
    raise SystemExit(128 + signum)


def install_parent_signal_handlers() -> None:
    global _PARENT_SIGNAL_HANDLERS_INSTALLED
    if _PARENT_SIGNAL_HANDLERS_INSTALLED or threading.current_thread() is not threading.main_thread():
        return
    signal.signal(signal.SIGTERM, _forward_parent_signal)
    signal.signal(signal.SIGINT, _forward_parent_signal)
    _PARENT_SIGNAL_HANDLERS_INSTALLED = True


@contextlib.contextmanager
def single_instance_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            if getattr(exc, "errno", None) in (11, 35) or isinstance(exc, BlockingIOError):
                raise AlreadyRunning(f"ov-memory-sync is already running: {LOCK_PATH}") from exc
            raise
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def run_process_group(command: list[str], *, timeout: float):
    process = subprocess.Popen(
        command,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _register_process_group(process)
    try:
        output, error = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _signal_process_group(process, signal.SIGTERM)
        try:
            output, error = process.communicate(timeout=PROCESS_GROUP_TERM_GRACE)
        except subprocess.TimeoutExpired:
            _signal_process_group(process, signal.SIGKILL)
            output, error = process.communicate()
        else:
            # Pipes may close when the leader exits while a detached
            # descendant remains; sweep the group unconditionally.
            _signal_process_group(process, signal.SIGKILL)
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=output if output is not None else exc.output,
            stderr=error if error is not None else exc.stderr,
        )
    finally:
        _unregister_process_group(process.pid)
    return subprocess.CompletedProcess(command, process.returncode, output, error)


def ov(*args: str) -> dict:
    """Call the ov_rest helper and return parsed JSON (raises on hard failure)."""
    proc = run_process_group(
        [sys.executable, str(OV_CLI), *args],
        timeout=max(5.0, OV_REQUEST_TIMEOUT + 5.0),
    )
    out = proc.stdout.strip()
    if proc.returncode != 0:
        try:
            payload = json.loads(out)
        except json.JSONDecodeError:
            payload = {}
        if proc.returncode == 3 and payload.get("status") == "probe_inconclusive":
            raise OpenVikingProbeInconclusive(
                "sandbox cannot observe host loopback; rerun from host context"
            )
        raise RuntimeError(f"ov_rest failed ({proc.returncode}): {proc.stderr or out}")
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"_raw": out}


def list_remote_notes() -> list[str]:
    """Return note base names (e.g. 'MEMORY.md') present under resources/memory."""
    res = ov("raw", "GET", "/api/v1/fs/ls", "--query", json.dumps({"uri": MEMORY_ROOT}))
    names = []
    for entry in res.get("result", []):
        uri = entry["uri"]
        name = uri.rsplit("/", 1)[-1]
        names.append(name)
    return sorted(names)


def require_observable_service() -> None:
    """Fail explicitly when a sandboxed caller cannot verify the local service."""
    result = ov("health")
    if not (result.get("healthy") or result.get("status") == "ok"):
        raise RuntimeError(f"OpenViking health check failed: {result}")


def remote_read(name: str, *, strict: bool = False) -> str | None:
    """Read the inner file for a note; optionally raise on non-404 failures."""
    inner = f"{MEMORY_ROOT}/{name}/{name}"
    proc = run_process_group(
        [sys.executable, str(OV_CLI), "read", inner],
        timeout=max(5.0, OV_REQUEST_TIMEOUT + 5.0),
    )
    if proc.returncode != 0:
        if strict:
            detail = (proc.stderr or proc.stdout or "remote read failed").strip()
            if "http 404" not in detail.lower() and "not found" not in detail.lower():
                raise RuntimeError(f"remote read failed for {name}: {detail[:240]}")
        return None
    data = json.loads(proc.stdout)
    return data.get("result")


def remote_write(name: str, content: str, exists: bool) -> dict:
    """Write a note's inner file. New notes use `create`, existing use `replace`."""
    inner = f"{MEMORY_ROOT}/{name}/{name}"
    mode = "replace" if exists else "create"
    args = [
        "write", inner, "--content", content, "--mode", mode,
        "--processing-mode", "vectors_only", "--timeout", str(OV_REQUEST_TIMEOUT),
    ]
    if OV_WAIT:
        args.append("--wait")
    return ov(*args)


def _load_pending_items() -> dict[str, dict]:
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    except (OSError, ValueError):
        value = {}
    items = value.get("items", []) if isinstance(value, dict) else []
    return {
        str(item.get("name")): dict(item)
        for item in items
        if isinstance(item, dict) and item.get("name")
    }


def record_pending(
    name: str,
    result: Optional[dict],
    status: str = "queued",
    error: str = "",
    *,
    retry_attempt: Optional[int] = None,
    next_retry_at: Optional[str] = None,
    mtime_ns: Optional[int] = None,
    quarantine_mtime_ns: Optional[int] = None,
) -> dict:
    """Keep accepted/failed writes observable without blocking on VLM work."""
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    except (OSError, ValueError):
        value = {}
    items = [item for item in value.get("items", []) if item.get("name") != name]
    task_id = ""
    if isinstance(result, dict):
        for container in (result, result.get("result")):
            if isinstance(container, dict):
                task_id = str(container.get("task_id") or container.get("taskId") or container.get("id") or "")[:160]
                if task_id:
                    break
    item = {
        "name": name,
        "status": status,
        "task_id": task_id,
        "error": str(error)[:240],
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if retry_attempt is not None:
        item["retry_attempt"] = max(0, int(retry_attempt))
    if next_retry_at is not None:
        item["next_retry_at"] = str(next_retry_at)
    if mtime_ns is not None:
        item["mtime_ns"] = int(mtime_ns)
    if quarantine_mtime_ns is not None:
        item["quarantine_mtime_ns"] = int(quarantine_mtime_ns)
    items.append(item)
    value["items"] = items[-500:]
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    directory = STATE_PATH.parent
    fd, temporary = tempfile.mkstemp(prefix=".ov-memory-sync-pending.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, STATE_PATH)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise
    return item


def _watch_retry_delay(attempt: int, *, base: float = WATCH_BACKOFF_BASE_SEC, maximum: float = WATCH_BACKOFF_MAX_SEC) -> float:
    """Return a capped exponential delay for one file's failed attempt."""
    if attempt <= 0:
        return 0.0
    return min(max(0.0, float(maximum)), max(0.0, float(base)) * (2 ** (attempt - 1)))


def _watch_error_is_permanent(error: BaseException | str) -> bool:
    text = str(error).lower()
    return any(marker in text for marker in PERMANENT_WATCH_ERROR_MARKERS)


def _iso_after(seconds: float, *, now: Optional[datetime] = None) -> str:
    current = now or datetime.now(timezone.utc)
    return (current + timedelta(seconds=max(0.0, float(seconds)))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _watch_retry_due(value: Optional[str], *, now: Optional[datetime] = None) -> bool:
    if not value:
        return True
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        target = datetime.fromisoformat(raw)
    except ValueError:
        return True
    target = target.replace(tzinfo=target.tzinfo or timezone.utc)
    return target <= (now or datetime.now(timezone.utc))


def _durable_events_enabled(args) -> bool:
    value = getattr(args, "durable_events", None)
    if value is not None:
        return bool(value)
    return os.environ.get("PM_V45_MEMORY_EVENT_MODE", "").strip().lower() in {"outbox", "durable", "1", "true", "yes"}


def import_legacy_pending(
    store,
    mirror: Path,
    *,
    namespace_epoch: str = "v4",
    pending_path: Path = STATE_PATH,
) -> dict:
    """Import old sidecar entries without guessing whether OV already wrote.

    A queued/failed sidecar item has no durable operation ledger, so it cannot
    be safely replayed during cutover.  Existing local files are represented
    as one deduplicated Memory event and its Outbox row, then quarantined with
    an explicit terminal reason.  Missing local files are reported as
    quarantined evidence and are never synthesized into a task.
    """
    try:
        raw = json.loads(pending_path.read_text(encoding="utf-8")) if pending_path.is_file() else {}
    except (OSError, ValueError):
        raw = {}
    items = raw.get("items", []) if isinstance(raw, dict) else []
    imported: list[dict] = []
    missing: list[dict] = []
    skipped: list[dict] = []
    for item in items:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        name = str(item["name"])
        path = mirror / name
        if not path.is_file():
            missing.append({"name": name, "legacy_status": item.get("status"), "reason": "local_mirror_file_missing"})
            continue
        try:
            content = path.read_text(encoding="utf-8")
            mtime_ns = path.stat().st_mtime_ns
        except (OSError, UnicodeDecodeError) as exc:
            missing.append({"name": name, "legacy_status": item.get("status"), "reason": f"local_mirror_read_failed:{type(exc).__name__}"})
            continue
        event = store.enqueue_memory_change(
            name=name,
            mtime=mtime_ns,
            content_hash=digest(content),
            snapshot_uri=f"{MEMORY_ROOT}/{name}/{name}",
            file_path=str(path),
            namespace_epoch=namespace_epoch,
        )
        with store.transaction() as connection:
            event_id = str(event["event_id"])
            outbox_id = str(event["outbox_id"])
            detail = {
                "legacy_pending": item,
                "legacy_pending_path": str(pending_path),
                "content_hash": digest(content),
                "local_path": str(path),
                "remote_result_unknown": True,
            }
            connection.execute(
                "UPDATE memory_change_events SET state='quarantine',consumed_at=? WHERE event_id=? AND consumed_at IS NULL",
                (now_iso(), event_id),
            )
            row = connection.execute("SELECT payload_json FROM outbox_items WHERE outbox_id=?", (outbox_id,)).fetchone()
            try:
                payload = json.loads(row[0] or "{}") if row else {}
            except (TypeError, ValueError):
                payload = {}
            payload["legacy_pending_import"] = detail
            connection.execute(
                "UPDATE outbox_items SET status='quarantine',terminal_reason=?,payload_json=?,updated_at=? WHERE outbox_id=? AND status IN ('pending','retry_wait','in_flight')",
                ("legacy_pending_uncertain_remote_state", json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), now_iso(), outbox_id),
            )
        imported.append({"name": name, "event_id": event["event_id"], "outbox_id": event["outbox_id"], "content_hash": digest(content), "legacy_status": item.get("status"), "state": "quarantine"})
    return {"pending_path": str(pending_path), "scanned": len(items), "imported": imported, "missing": missing, "skipped": skipped, "imported_count": len(imported), "missing_count": len(missing)}


def _watch_durable_events(args, mirror: Path) -> int:
    """Watch mode used by V4.5: enqueue a local event, never call OpenViking."""
    if PMSystemStore is None:
        raise RuntimeError("PMSystemStore unavailable; cannot enable durable memory events")
    store = PMSystemStore(MEMORY_DB_PATH)
    legacy = import_legacy_pending(
        store,
        mirror,
        namespace_epoch=os.environ.get("PM_V45_NAMESPACE_EPOCH", "v4"),
    )
    print(f"legacy pending imported={legacy['imported_count']} missing={legacy['missing_count']}")
    sys.stdout.flush()
    seen: dict[str, int] = {}
    pending_items = _load_pending_items()
    for path in mirror.glob("*.md"):
        try:
            seen[path.name] = path.stat().st_mtime_ns
        except OSError:
            continue
    print(f"watching {mirror} every {args.interval}s (durable memory events)")
    sys.stdout.flush()
    try:
        while True:
            changed: list[tuple[Path, int, dict]] = []
            for path in sorted(mirror.glob("*.md")):
                try:
                    mtime_ns = path.stat().st_mtime_ns
                except OSError:
                    continue
                if seen.get(path.name) == mtime_ns:
                    continue
                entry = pending_items.get(path.name, {})
                if entry.get("status") == "quarantined" and str(entry.get("quarantine_mtime_ns")) == str(mtime_ns):
                    seen[path.name] = mtime_ns
                    continue
                changed.append((path, mtime_ns, entry))
            for path, mtime_ns, _entry in changed:
                try:
                    content = path.read_text(encoding="utf-8")
                    event = store.enqueue_memory_change(
                        name=path.name,
                        mtime=mtime_ns,
                        content_hash=digest(content),
                        snapshot_uri=f"{MEMORY_ROOT}/{path.name}/{path.name}",
                        file_path=str(path),
                        namespace_epoch=os.environ.get("PM_V45_NAMESPACE_EPOCH", "v4"),
                    )
                    pending_items[path.name] = record_pending(path.name, event, "queued", mtime_ns=mtime_ns)
                    seen[path.name] = mtime_ns
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] queued memory event {path.name}")
                    sys.stdout.flush()
                except Exception as exc:
                    retry = _watch_retry_plan(_entry, mtime_ns=mtime_ns, error=exc, max_attempts=getattr(args, "max_attempts", WATCH_MAX_ATTEMPTS), backoff_base=getattr(args, "backoff_base", WATCH_BACKOFF_BASE_SEC), backoff_max=getattr(args, "backoff_max", WATCH_BACKOFF_MAX_SEC))
                    pending_items[path.name] = record_pending(path.name, None, retry["status"], str(exc), retry_attempt=retry["retry_attempt"], next_retry_at=retry["next_retry_at"], mtime_ns=mtime_ns, quarantine_mtime_ns=retry["quarantine_mtime_ns"])
                    if retry["status"] == "quarantined":
                        seen[path.name] = mtime_ns
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped")
        return 0


def _watch_retry_plan(
    entry: Optional[dict],
    *,
    mtime_ns: int,
    error: BaseException | str,
    max_attempts: int = WATCH_MAX_ATTEMPTS,
    backoff_base: float = WATCH_BACKOFF_BASE_SEC,
    backoff_max: float = WATCH_BACKOFF_MAX_SEC,
    now: Optional[datetime] = None,
) -> dict:
    """Advance one file's durable retry state without performing I/O."""
    previous = entry or {}
    same_file = str(previous.get("mtime_ns") or "") == str(mtime_ns)
    previous_attempt = int(previous.get("retry_attempt") or 0) if same_file else 0
    attempt = previous_attempt + 1
    permanent = _watch_error_is_permanent(error)
    terminal = permanent or attempt >= max(1, int(max_attempts))
    return {
        "status": "quarantined" if terminal else "retry_wait",
        "retry_attempt": attempt,
        "next_retry_at": None if terminal else _iso_after(
            _watch_retry_delay(attempt, base=backoff_base, maximum=backoff_max), now=now
        ),
        "mtime_ns": int(mtime_ns),
        "quarantine_mtime_ns": int(mtime_ns) if terminal else None,
    }


def digest(text: str) -> str:
    # OpenViking content/write strips the final newline; compare logical content
    # so the watcher does not rewrite an unchanged note forever.
    return hashlib.sha256(text.rstrip("\r\n").encode("utf-8")).hexdigest()


def cmd_pull(args) -> int:
    require_observable_service()
    mirror = Path(args.mirror)
    mirror.mkdir(parents=True, exist_ok=True)
    names = list_remote_notes()
    pulled = 0
    for name in names:
        content = remote_read(name)
        if content is None:
            print(f"  skip (unreadable): {name}")
            continue
        (mirror / name).write_text(content, encoding="utf-8")
        pulled += 1
    print(f"pulled {pulled}/{len(names)} notes -> {mirror}")
    return 0


def cmd_push(args) -> int:
    require_observable_service()
    mirror = Path(args.mirror)
    local = sorted(p for p in mirror.glob("*.md") if p.is_file())
    remote = set(list_remote_notes())
    pushed = skipped = 0
    for path in local:
        name = path.name
        content = path.read_text(encoding="utf-8")
        exists = name in remote
        current = remote_read(name) if exists else None
        if current is not None and digest(current) == digest(content):
            skipped += 1
            continue
        try:
            result = remote_write(name, content, exists=exists)
            record_pending(name, result, "complete" if OV_WAIT else "queued")
        except Exception as exc:
            record_pending(name, None, "failed", str(exc))
            raise
        remote.add(name)
        pushed += 1
        print(f"  pushed: {name}")
    print(f"push done: {pushed} written, {skipped} unchanged (of {len(local)} local)")
    return 0


def cmd_status(args) -> int:
    require_observable_service()
    mirror = Path(args.mirror)
    local = {p.name: p.read_text(encoding="utf-8") for p in mirror.glob("*.md") if p.is_file()}
    remote = set(list_remote_notes())
    only_local = sorted(set(local) - remote)
    only_remote = sorted(remote - set(local))
    differ = []
    for name in sorted(set(local) & remote):
        rc = remote_read(name)
        if rc is not None and digest(rc) != digest(local[name]):
            differ.append(name)
    print(f"local notes:  {len(local)}")
    print(f"remote notes: {len(remote)}")
    print(f"only local ({len(only_local)}): {only_local}")
    print(f"only remote ({len(only_remote)}): {only_remote}")
    print(f"content differs ({len(differ)}): {differ}")
    return 0


def ov_reachable() -> bool:
    try:
        res = ov("health")
    except (OpenVikingProbeInconclusive, RuntimeError, OSError, subprocess.TimeoutExpired):
        return False
    return bool(res.get("healthy") or res.get("status") == "ok")


def cmd_watch(args) -> int:
    mirror = Path(args.mirror)
    mirror.mkdir(parents=True, exist_ok=True)
    if _durable_events_enabled(args):
        return _watch_durable_events(args, mirror)
    print(f"watching {mirror} every {args.interval}s (Ctrl-C to stop)")
    sys.stdout.flush()
    pending_items = _load_pending_items()
    seen: dict[str, int] = {}
    for path in mirror.glob("*.md"):
        mtime_ns = path.stat().st_mtime_ns
        entry = pending_items.get(path.name, {})
        # A durable retry must survive a watcher restart.  Do not mark that
        # mtime as seen until its retry window has been processed.
        if entry.get("status") == "retry_wait" and str(entry.get("mtime_ns")) == str(mtime_ns):
            continue
        seen[path.name] = mtime_ns
    max_attempts = max(1, int(getattr(args, "max_attempts", WATCH_MAX_ATTEMPTS)))
    backoff_base = max(0.0, float(getattr(args, "backoff_base", WATCH_BACKOFF_BASE_SEC)))
    backoff_max = max(backoff_base, float(getattr(args, "backoff_max", WATCH_BACKOFF_MAX_SEC)))
    # Existence is stable for the watch process; avoid a remote read before
    # every write (the old read+wait-write pair doubled request latency).  A
    # failed initial listing is different from an empty remote tree: keep the
    # state unknown and refresh with bounded backoff, otherwise the first
    # changed note is incorrectly sent as ``create`` forever.
    remote_names: set[str] | None = None
    next_remote_refresh = 0.0
    refresh_backoff = max(float(args.interval), 5.0)
    service_retry_attempt = 0
    service_retry_until = 0.0
    try:
        remote_names = set(list_remote_notes())
        next_remote_refresh = time.monotonic() + max(30.0, refresh_backoff)
    except Exception as exc:
        print(f"WARN initial remote listing: {exc}")
        next_remote_refresh = time.monotonic() + refresh_backoff
    while True:
        try:
            now = time.monotonic()
            if remote_names is None and now >= next_remote_refresh:
                try:
                    remote_names = set(list_remote_notes())
                    next_remote_refresh = now + max(30.0, refresh_backoff)
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] remote note listing recovered")
                    sys.stdout.flush()
                except Exception as exc:
                    next_remote_refresh = now + min(60.0, refresh_backoff * 2)
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WARN remote listing: {exc}")
                    sys.stdout.flush()
            changed: list[tuple[Path, int, dict]] = []
            for path in sorted(mirror.glob("*.md")):
                mtime_ns = path.stat().st_mtime_ns
                if seen.get(path.name) == mtime_ns:
                    continue
                entry = pending_items.get(path.name, {})
                quarantine_mtime = entry.get("quarantine_mtime_ns")
                if entry.get("status") == "quarantined" and str(quarantine_mtime) == str(mtime_ns):
                    # A quarantined file is inert until its contents change.
                    seen[path.name] = mtime_ns
                    continue
                if entry.get("status") == "retry_wait" and str(entry.get("mtime_ns")) == str(mtime_ns):
                    if not _watch_retry_due(entry.get("next_retry_at")):
                        continue
                changed.append((path, mtime_ns, entry))
            if changed and time.monotonic() < service_retry_until:
                # A shared service outage should not cause a health request on
                # every watcher tick. The retry window is capped and cheap.
                time.sleep(args.interval)
                continue
            if changed and not ov_reachable():
                service_retry_attempt += 1
                delay = _watch_retry_delay(
                    service_retry_attempt, base=backoff_base, maximum=backoff_max
                )
                service_retry_until = now + delay
                # Keep the changed mtime durable while the shared service is
                # unavailable, so a process restart cannot lose the change.
                for path, mtime_ns, entry in changed:
                    pending_items[path.name] = record_pending(
                        path.name,
                        None,
                        "retry_wait",
                        "OpenViking unavailable",
                        retry_attempt=int(entry.get("retry_attempt") or 0),
                        next_retry_at=_iso_after(delay),
                        mtime_ns=mtime_ns,
                    )
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WARN OpenViking unavailable; "
                    f"retrying in {delay:g}s"
                )
                sys.stdout.flush()
                time.sleep(args.interval)
                continue
            if changed and remote_names is None:
                # Do not guess whether a note exists while the inventory is
                # unavailable; retry on the next tick after the backoff.
                time.sleep(args.interval)
                continue
            service_retry_attempt = 0
            service_retry_until = 0.0
            for path, mtime_ns, entry in changed:
                try:
                    content = path.read_text(encoding="utf-8")
                    exists = path.name in remote_names
                    try:
                        result = remote_write(path.name, content, exists=exists)
                    except Exception:
                        # A create request may have reached OpenViking before
                        # the client observed an error.  The content/write
                        # API has no request idempotency key, so never issue a
                        # second create blindly.  Reconcile one fresh
                        # inventory instead; matching content proves the
                        # original request took effect, while a mismatch is
                        # an explicit replace (the local mirror remains the
                        # source of truth).  Listing/read failures propagate
                        # into the durable per-file retry plan.
                        if exists:
                            raise
                        refreshed = set(list_remote_notes())
                        remote_names = refreshed
                        if path.name not in refreshed:
                            raise
                        current = remote_read(path.name, strict=True)
                        if current is None:
                            raise RuntimeError(
                                f"remote inventory contains {path.name}, but content read did not confirm the write"
                            )
                        if digest(current) == digest(content):
                            result = {
                                "uri": f"{MEMORY_ROOT}/{path.name}/{path.name}",
                                "mode": "create",
                                "content_updated": True,
                                "reconciled": True,
                            }
                        else:
                            result = remote_write(path.name, content, exists=True)
                    remote_names.add(path.name)
                    pending_items[path.name] = record_pending(
                        path.name,
                        result,
                        "complete" if OV_WAIT else "queued",
                        mtime_ns=mtime_ns,
                    )
                    seen[path.name] = mtime_ns
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] synced {path.name} -> OpenViking")
                    sys.stdout.flush()
                except Exception as exc:  # transient errors: log, retry next tick
                    retry = _watch_retry_plan(
                        entry,
                        mtime_ns=mtime_ns,
                        error=exc,
                        max_attempts=max_attempts,
                        backoff_base=backoff_base,
                        backoff_max=backoff_max,
                    )
                    pending_items[path.name] = record_pending(
                        path.name,
                        None,
                        retry["status"],
                        str(exc),
                        retry_attempt=retry["retry_attempt"],
                        next_retry_at=retry["next_retry_at"],
                        mtime_ns=retry["mtime_ns"],
                        quarantine_mtime_ns=retry["quarantine_mtime_ns"],
                    )
                    if retry["status"] == "quarantined":
                        seen[path.name] = mtime_ns
                        detail = "quarantined; edit the file to reactivate"
                    else:
                        delay = _watch_retry_delay(
                            retry["retry_attempt"], base=backoff_base, maximum=backoff_max
                        )
                        detail = f"retry {retry['retry_attempt']}/{max_attempts} in {delay:g}s"
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WARN {path.name}: {exc} ({detail})")
                    sys.stdout.flush()
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nstopped")
            return 0


def cmd_import_legacy(args) -> int:
    if PMSystemStore is None:
        raise RuntimeError("PMSystemStore unavailable; cannot import legacy memory events")
    store = PMSystemStore(MEMORY_DB_PATH if args.db_path is None else args.db_path)
    result = import_legacy_pending(
        store,
        Path(args.mirror),
        namespace_epoch=args.namespace_epoch,
        pending_path=Path(args.pending_path),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["missing_count"] == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Sync OpenViking resources/memory with a local mirror")
    p.add_argument("--mirror", default=str(DEFAULT_MIRROR), help="local mirror directory")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, func in (("pull", cmd_pull), ("push", cmd_push), ("status", cmd_status)):
        sp = sub.add_parser(name)
        sp.set_defaults(func=func)
    sp = sub.add_parser("watch")
    sp.add_argument("--interval", type=float, default=5.0)
    sp.add_argument("--max-attempts", type=int, default=WATCH_MAX_ATTEMPTS)
    sp.add_argument("--backoff-base", type=float, default=WATCH_BACKOFF_BASE_SEC)
    sp.add_argument("--backoff-max", type=float, default=WATCH_BACKOFF_MAX_SEC)
    sp.add_argument("--durable-events", action="store_true", help="enqueue durable memory events instead of remote writes")
    sp.set_defaults(func=cmd_watch)
    sp = sub.add_parser("import-legacy")
    sp.add_argument("--db-path", type=Path, default=None)
    sp.add_argument("--pending-path", default=str(STATE_PATH))
    sp.add_argument("--namespace-epoch", default=os.environ.get("PM_V45_NAMESPACE_EPOCH", "v4"))
    sp.set_defaults(func=cmd_import_legacy)
    return p


def main() -> int:
    args = build_parser().parse_args()
    install_parent_signal_handlers()
    try:
        with single_instance_lock():
            return args.func(args)
    except AlreadyRunning as exc:
        print(str(exc), file=sys.stderr)
        return 75
    except OpenVikingProbeInconclusive as exc:
        print(f"probe_inconclusive: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
