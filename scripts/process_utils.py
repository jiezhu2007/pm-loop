#!/usr/bin/env python3
"""Small process-group helpers used by unattended concept jobs.

The Codex/Claude command can spawn children (and the upload shell script can
spawn curl).  ``subprocess.run(timeout=...)`` only guarantees that the direct
child is interrupted; this module starts a new session and tears down the
whole process group when the deadline is reached.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from typing import Any, List, Optional


_ACTIVE_PROCESSES = {}
_ACTIVE_LOCK = threading.Lock()
_GLOBAL_SIGNAL_HANDLERS = {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def terminate_process_group(
    process: subprocess.Popen,
    *,
    grace_seconds: float = 3.0,
    isolated: bool = True,
) -> None:
    """Terminate ``process`` and all children in its session.

    ``killpg`` is intentionally best-effort: the parent may have exited while
    a child still owns the pipe, and in that case the group can disappear
    between the TERM and KILL calls.
    """

    if isolated:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    else:
        try:
            process.terminate()
        except (ProcessLookupError, PermissionError, AttributeError):
            pass
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            if isolated:
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except (ProcessLookupError, PermissionError, AttributeError):
            pass
        process.wait()
    except OSError:
        # A mocked/partially-started process may not implement wait reliably.
        pass


def _register_process(process: subprocess.Popen) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_PROCESSES[process.pid] = process


def _unregister_process(process: subprocess.Popen) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_PROCESSES.pop(process.pid, None)


def terminate_active_process_groups(*, grace_seconds: float = 2.0) -> None:
    """Terminate all tracked child sessions (including worker-thread calls)."""
    with _ACTIVE_LOCK:
        processes = list(_ACTIVE_PROCESSES.values())
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    deadline = time.monotonic() + grace_seconds
    for process in processes:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                process.wait(timeout=1.0)
            except (OSError, ChildProcessError, subprocess.TimeoutExpired):
                pass
        except (OSError, ChildProcessError):
            pass


def install_process_group_signal_handlers() -> None:
    """Forward parent TERM/INT to all active child sessions.

    This is needed by the adapter's worker pool: Python only permits signal
    handlers in the main thread, while LLM/upload calls run in worker threads.
    """
    if threading.current_thread() is not threading.main_thread() or _GLOBAL_SIGNAL_HANDLERS:
        return

    def _forward_all(signum, _frame):
        terminate_active_process_groups()
        raise SystemExit(128 + signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        _GLOBAL_SIGNAL_HANDLERS[signum] = signal.getsignal(signum)
        signal.signal(signum, _forward_all)


def run_process_group(
    command: List[str],
    *,
    timeout: float,
    input_text: Optional[str] = None,
    env: Optional[dict] = None,
    stdin: Any = None,
    capture_output: bool = True,
    grace_seconds: float = 3.0,
    start_new_session: bool = True,
) -> subprocess.CompletedProcess:
    """Run a command in an isolated process group with a hard wall-clock cap."""

    kwargs = {
        "text": True,
        "start_new_session": start_new_session,
        "env": env,
    }
    if capture_output:
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if stdin is not None:
        kwargs["stdin"] = stdin
    elif input_text is not None:
        kwargs["stdin"] = subprocess.PIPE

    try:
        process = subprocess.Popen(command, **kwargs)
    except OSError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))
    _register_process(process)

    # The timeout wrapper itself can receive TERM from weekly-refresh.  Forward
    # that signal to the child group before exiting; worker-thread callers do
    # not install handlers because Python only permits them in the main thread.
    previous_handlers = {}
    if threading.current_thread() is threading.main_thread():
        def _forward_signal(signum, _frame):
            terminate_process_group(process, grace_seconds=grace_seconds, isolated=start_new_session)
            raise SystemExit(128 + signum)

        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _forward_signal)

    try:
        stdout, stderr = process.communicate(input=input_text, timeout=timeout)
        return subprocess.CompletedProcess(command, process.returncode, stdout or "", stderr or "")
    except subprocess.TimeoutExpired as exc:
        partial_stdout = _text(getattr(exc, "stdout", None))
        partial_stderr = _text(getattr(exc, "stderr", None))
        terminate_process_group(process, grace_seconds=grace_seconds, isolated=start_new_session)
        try:
            tail_stdout, tail_stderr = process.communicate(timeout=max(1.0, grace_seconds))
        except subprocess.TimeoutExpired:
            # A descendant may have inherited a pipe despite the group kill;
            # close the descriptors instead of allowing the timeout handler to
            # block forever while collecting output.
            for stream in (getattr(process, "stdout", None), getattr(process, "stderr", None)):
                try:
                    if stream is not None:
                        stream.close()
                except OSError:
                    pass
            tail_stdout, tail_stderr = "", ""
        except Exception:
            tail_stdout, tail_stderr = "", ""
        stdout = _text(tail_stdout) or partial_stdout
        stderr = _text(tail_stderr) or partial_stderr
        detail = f"TimeoutExpired: command exceeded {timeout:g} seconds"
        stderr = f"{stderr}\n{detail}".strip()
        return subprocess.CompletedProcess(command, 124, stdout, stderr)
    except KeyboardInterrupt:
        terminate_process_group(process, grace_seconds=grace_seconds, isolated=start_new_session)
        return subprocess.CompletedProcess(command, 130, "", "Interrupted")
    finally:
        _unregister_process(process)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main(argv: Optional[List[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) < 2:
        print("usage: process_utils.py <seconds> <command> [args...]", file=sys.stderr)
        return 2
    try:
        timeout = float(args[0])
    except ValueError:
        print(f"invalid timeout: {args[0]}", file=sys.stderr)
        return 2
    if timeout <= 0:
        print("timeout must be positive", file=sys.stderr)
        return 2
    returncode = run_process_group(args[1:], timeout=timeout, capture_output=False).returncode
    return 128 + (-returncode) if returncode < 0 else returncode


if __name__ == "__main__":
    raise SystemExit(main())
