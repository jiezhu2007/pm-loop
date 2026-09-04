#!/usr/bin/env python3
"""Read-only source adapter for the personal PM Loop Control Plane.

The adapter deliberately does not call an LLM. It turns local runtime facts
into a bounded, replayable snapshot that the Codex runner can consume later.
"""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


SCHEMA_VERSION = "pm-loop.snapshot.v1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CODEX_ROOT = Path(os.environ.get("CODEX_ROOT", str(Path.home() / ".codex"))).expanduser()
DEFAULT_OUTPUT = PROJECT_ROOT / "output" / "pm-loop-control-plane"
OPENVIKING_CONFIG = Path.home() / ".openviking" / "ovcli.conf"
TIMELINE_DIR = CODEX_ROOT / "skills" / "pm-timeline" / "state" / "timeline"
NATIVE_SKILLS_URI = "viking://agent/skills"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def file_mtime(path: Path) -> Optional[str]:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except OSError:
        return None


def json_safe_write(path: Path, value: Any) -> None:
    """Write one JSON artifact atomically inside the selected output directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def parse_description(skill_file: Path) -> str:
    try:
        lines = skill_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return ""
    in_frontmatter = bool(lines and lines[0].strip() == "---")
    if not in_frontmatter:
        return ""
    for line in lines[1:80]:
        if line.strip() == "---":
            break
        if line.startswith("description:"):
            value = line.split(":", 1)[1].strip()
            return value.strip("'\"")
    return ""


def collect_skills(codex_root: Path) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    skills_root = codex_root / "skills"
    status = "healthy" if skills_root.is_dir() else "unavailable"
    for skill_file in sorted(skills_root.glob("*/SKILL.md")):
        try:
            stat = skill_file.stat()
        except OSError:
            continue
        name = skill_file.parent.name
        records.append(
            {
                "name": name,
                "path": str(skill_file),
                "resource_uri": f"{NATIVE_SKILLS_URI}/{name}/SKILL.md",
                "description": parse_description(skill_file),
                "bytes": stat.st_size,
                "modified_at": file_mtime(skill_file),
                "has_scripts": (skill_file.parent / "scripts").is_dir(),
            }
        )
    return {
        "status": status,
        "root": str(skills_root),
        "count": len(records),
        "skills": records,
    }


def format_schedule(value: Any) -> Optional[str]:
    if isinstance(value, list):
        return "; ".join(item for item in (format_schedule(entry) for entry in value) if item)
    if isinstance(value, dict):
        parts: List[str] = []
        weekday = value.get("Weekday")
        if weekday is not None:
            weekday_label = {
                0: "周日",
                1: "周一",
                2: "周二",
                3: "周三",
                4: "周四",
                5: "周五",
                6: "周六",
                7: "周日",
            }.get(int(weekday), f"星期 {weekday}")
            parts.append(weekday_label)
        if value.get("Hour") is not None or value.get("Minute") is not None:
            parts.append(f"{int(value.get('Hour', 0)):02d}:{int(value.get('Minute', 0)):02d}")
        if weekday is None and parts:
            parts.insert(0, "每天")
        return " ".join(parts) or "calendar"
    return None


def probe_launchctl(label: str) -> Dict[str, Any]:
    """Observe one user LaunchAgent without printing its environment."""
    target = f"gui/{os.getuid()}/{label}"
    try:
        result = subprocess.run(
            ["launchctl", "print", target],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"state": "probe_inconclusive", "detail": type(exc).__name__}
    if result.returncode != 0:
        return {"state": "not_loaded", "returncode": result.returncode}
    state_match = re.search(r"^\s*state = ([^\n]+)", result.stdout, re.MULTILINE)
    exit_match = re.search(r"last exit code = ([^\n]+)", result.stdout)
    runs_match = re.search(r"^\s*runs = ([^\n]+)", result.stdout, re.MULTILINE)
    return {
        "state": state_match.group(1).strip() if state_match else "loaded",
        "last_exit_code": exit_match.group(1).strip() if exit_match else None,
        "runs": runs_match.group(1).strip() if runs_match else None,
    }


def collect_launchd_jobs(root: Optional[Path] = None) -> Dict[str, Any]:
    root = root or Path.home() / "Library" / "LaunchAgents"
    jobs: List[Dict[str, Any]] = []
    for plist_path in sorted(root.glob("com.zhujie14.*.plist")):
        try:
            with plist_path.open("rb") as stream:
                plist = plistlib.load(stream)
        except (OSError, plistlib.InvalidFileException):
            continue
        label = str(plist.get("Label", plist_path.stem))
        arguments = [str(value) for value in plist.get("ProgramArguments", [])]
        log_paths = [plist.get("StandardOutPath"), plist.get("StandardErrorPath")]
        log_info = []
        for raw_path in log_paths:
            if not raw_path:
                continue
            path = Path(str(raw_path)).expanduser()
            log_info.append({"path": str(path), "modified_at": file_mtime(path)})
        jobs.append(
            {
                "label": label,
                "plist": str(plist_path),
                "program": arguments,
                "working_directory": plist.get("WorkingDirectory"),
                "schedule": format_schedule(plist.get("StartCalendarInterval")),
                "interval_seconds": plist.get("StartInterval"),
                "run_at_load": bool(plist.get("RunAtLoad", False)),
                "keep_alive": bool(plist.get("KeepAlive", False)),
                "launchctl": probe_launchctl(label),
                "logs": log_info,
            }
        )
    status = "unavailable" if not root.is_dir() else "healthy"
    if status == "healthy" and any(job.get("launchctl", {}).get("state") == "probe_inconclusive" for job in jobs):
        status = "degraded"
    return {"status": status, "root": str(root), "count": len(jobs), "jobs": jobs}


class OpenVikingClient:
    def __init__(self, config_path: Path = OPENVIKING_CONFIG) -> None:
        self.config_path = config_path
        self.base_url = "http://127.0.0.1:1933"
        self.api_key = ""
        if config_path.is_file():
            try:
                config = json.loads(config_path.read_text(encoding="utf-8"))
                self.base_url = str(config.get("url") or self.base_url).rstrip("/")
                self.api_key = str(config.get("api_key") or "")
            except (OSError, json.JSONDecodeError):
                pass

    def request(self, path: str, method: str = "GET", body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {"Accept": "application/json"}
        payload = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.base_url + path, data=payload, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read().decode("utf-8")
        value = json.loads(raw)
        return value if isinstance(value, dict) else {"value": value}

    def snapshot(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"base_url": self.base_url, "status": "unknown", "skill_search": []}
        try:
            health = self.request("/health")
            result["health"] = health
            result["status"] = "healthy"
        except (OSError, ValueError, urllib.error.URLError, urllib.error.HTTPError) as exc:
            result["status"] = "probe_inconclusive"
            result["error"] = type(exc).__name__
            return result
        try:
            search = self.request(
                "/api/v1/search/find",
                "POST",
                {"query": "PM Loop Control Plane", "target_uri": NATIVE_SKILLS_URI, "limit": 8},
            )
            payload = search.get("result", search)
            resources = payload.get("resources", []) if isinstance(payload, dict) else []
            result["skill_search"] = [
                {
                    "uri": item.get("uri"),
                    "score": item.get("score"),
                    "abstract": item.get("abstract"),
                }
                for item in resources
                if isinstance(item, dict)
            ]
        except (OSError, ValueError, urllib.error.URLError, urllib.error.HTTPError) as exc:
            result["search_error"] = type(exc).__name__
        return result


def collect_timeline(timeline_dir: Path, limit: int = 8) -> Dict[str, Any]:
    files = sorted(timeline_dir.glob("*.jsonl"), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    if not files:
        return {"status": "unavailable", "directory": str(timeline_dir), "file": None, "events": []}
    path = files[0]
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]
    except OSError:
        lines = []
    events: List[Dict[str, Any]] = []
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        events.append(
            {
                "ts": event.get("ts"),
                "type": event.get("type"),
                "customer": event.get("customer"),
                "topic": event.get("topic"),
                "conclusion": event.get("conclusion"),
                "doc": event.get("doc"),
            }
        )
    return {"status": "healthy", "directory": str(timeline_dir), "file": str(path), "modified_at": file_mtime(path), "events": events}


def collect_snapshot(project_root: Path, codex_root: Path) -> Dict[str, Any]:
    skills = collect_skills(codex_root)
    jobs = collect_launchd_jobs()
    openviking = OpenVikingClient().snapshot()
    timeline = collect_timeline(codex_root / "skills" / "pm-timeline" / "state" / "timeline")
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": datetime.now(timezone.utc).strftime("snapshot-%Y%m%dT%H%M%SZ"),
        "collected_at": now_iso(),
        "scope": {"project_root": str(project_root), "codex_root": str(codex_root)},
        "summary": {
            "launchd_jobs": jobs["count"],
            "skills": skills["count"],
            "timeline_events": len(timeline["events"]),
            "openviking_status": openviking["status"],
            "openviking_skill_hits": len(openviking.get("skill_search", [])),
        },
        "sources": {
            "launchd": jobs,
            "skills": skills,
            "openviking": openviking,
            "pm_timeline": timeline,
        },
    }


def record_run(snapshot: Dict[str, Any], output_dir: Path) -> Dict[str, str]:
    run_id = snapshot["snapshot_id"].replace("snapshot-", "run-")
    run_dir = output_dir / "runs"
    run_path = run_dir / f"{run_id}.json"
    event_path = output_dir / "events.jsonl"
    event_started = {"ts": snapshot["collected_at"], "event": "run_started", "run_id": run_id, "loop_id": "local-source-snapshot"}
    event_closed = {"ts": now_iso(), "event": "run_closed", "run_id": run_id, "loop_id": "local-source-snapshot", "status": "completed"}
    run_record = {"run_id": run_id, "loop_id": "local-source-snapshot", "status": "completed", "snapshot": snapshot}
    output_dir.mkdir(parents=True, exist_ok=True)
    with event_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event_started, ensure_ascii=False) + "\n")
        stream.write(json.dumps(event_closed, ensure_ascii=False) + "\n")
    json_safe_write(run_path, run_record)
    return {"run_id": run_id, "run_path": str(run_path), "event_path": str(event_path)}


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read local PM Loop sources into a replayable snapshot")
    parser.add_argument("command", choices=["snapshot"], help="collect one bounded source snapshot")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--codex-root", type=Path, default=CODEX_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--record", action="store_true", help="append a local run ledger entry")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    snapshot = collect_snapshot(args.project_root.expanduser().resolve(), args.codex_root.expanduser().resolve())
    output_dir = args.out.expanduser().resolve()
    snapshot_path = output_dir / f"{snapshot['snapshot_id']}.json"
    json_safe_write(snapshot_path, snapshot)
    ledger: Optional[Dict[str, str]] = record_run(snapshot, output_dir) if args.record else None
    result = {"status": "ok", "snapshot_path": str(snapshot_path), "summary": snapshot["summary"]}
    if ledger:
        result["ledger"] = ledger
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
