#!/usr/bin/env python3
"""S8.4-M3 read-only reconciliation across source, ledger and OpenViking.

The audit deliberately treats each external projection as evidence, not as a
single source of truth.  Source plans are executed serially because the
canonical sync entry point owns a shared lock.  Only the supplied audit DB is
written; production ledger, task files and OpenViking are never mutated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYNC = Path.home() / ".codex/skills/shengsuan-sync/scripts/sync.sh"
DEFAULT_LEDGER = Path.home() / ".codex/skills/shengsuan-sync/state/ledger.json"
DEFAULT_TASK_DIR = Path.home() / ".openviking/data/viking/default/_system/tasks/default"
DEFAULT_RESOURCE_ROOT = Path.home() / ".openviking/data/viking/default"
DEFAULT_OV_URL = "http://127.0.0.1:1933"
SOURCES = (
    "databuilder-internal",
    "feature-list",
    "ontology",
    "data-agent",
    "datasearch",
    "pipeline-logic-fde",
    "product-management",
)
M2_SOURCES = tuple(source for source in SOURCES if source != "databuilder-internal")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def digest(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default


def read_runtime_flag(name: str) -> Dict[str, Any]:
    """Read a launchd-scoped flag, falling back to the current environment.

    The audit often runs outside the launchd session that owns the PM jobs, so
    ``os.environ`` alone can report a false ``null`` even while the host has
    the freeze gate enabled.  Keep the fallback for Linux/tests and record the
    evidence source without exposing unrelated launchd environment values.
    """
    try:
        completed = subprocess.run(
            ["launchctl", "getenv", name],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        completed = None
    if completed is not None and completed.returncode == 0:
        value = (completed.stdout or "").strip()
        if value:
            return {"value": value, "source": "launchctl"}
    value = os.environ.get(name)
    if value is not None and str(value).strip():
        return {"value": str(value).strip(), "source": "process_environment"}
    return {"value": None, "source": "unavailable"}


def extract_last_json(text: str) -> Optional[Mapping[str, Any]]:
    decoder = json.JSONDecoder()
    candidates: list[tuple[int, Mapping[str, Any]]] = []
    for match in re.finditer(r"\{", text):
        try:
            value, end = decoder.raw_decode(text[match.start() :])
        except ValueError:
            continue
        if isinstance(value, Mapping) and "discovered" in value and "kept" in value:
            candidates.append((match.start() + end, value))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def normal_uri(value: Any) -> str:
    return str(value or "").strip().rstrip("/")


def run_source_plan(source: str, sync_path: Path, timeout: int) -> Dict[str, Any]:
    command = ["bash", str(sync_path), "plan", source]
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout)),
            check=False,
        )
        output = (completed.stdout or "") + (completed.stderr or "")
        totals = extract_last_json(output)
        uris = sorted(
            {
                normal_uri(line.split("→", 1)[1])
                for line in output.splitlines()
                if "→ viking://" in line
            }
        )
        return {
            "source": source,
            "exit_code": completed.returncode,
            "totals": dict(totals or {}),
            "kept_uris": uris,
            "locked": completed.returncode == 75,
            "output_tail": output[-1200:],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "source": source,
            "exit_code": None,
            "totals": {},
            "kept_uris": [],
            "locked": False,
            "error": f"{type(exc).__name__}: {exc}",
            "output_tail": "",
        }


def ledger_summary(path: Path) -> Dict[str, Any]:
    value = load_json(path)
    rows = value if isinstance(value, Mapping) else {}
    by_source: Counter[str] = Counter()
    verified_by_source: Counter[str] = Counter()
    targets_by_source: Dict[str, set[str]] = {}
    missing_hash = 0
    malformed = 0
    for key, row in rows.items():
        if not isinstance(row, Mapping):
            malformed += 1
            continue
        source = str(row.get("source") or "unknown")
        by_source[source] += 1
        targets_by_source.setdefault(source, set()).add(normal_uri(row.get("target_uri")))
        verified = bool(
            re.fullmatch(r"(?:sha256:)?[0-9a-fA-F]{64}", str(row.get("sha256") or ""))
            and row.get("sha256_verified_by") == "content_sha256"
            and str(row.get("sha256_verified_at") or "").strip()
        )
        verified_by_source[source] += int(verified)
        if not verified:
            missing_hash += 1
    return {
        "path": str(path),
        "exists": path.is_file(),
        "sha256": digest(path),
        "rows": len(rows),
        "by_source": dict(sorted(by_source.items())),
        "verified_by_source": dict(sorted(verified_by_source.items())),
        "missing_verified_hash": missing_hash,
        "malformed_rows": malformed,
        "targets_by_source": {key: sorted(value) for key, value in targets_by_source.items()},
    }


def aggregate_directory(path: Path) -> Dict[str, Any]:
    files = sorted(path.glob("*.json")) if path.is_dir() else []
    hasher = hashlib.sha256()
    for item in files:
        hasher.update(item.name.encode("utf-8"))
        hasher.update(b"\0")
        try:
            hasher.update(item.read_bytes())
        except OSError:
            hasher.update(b"<unreadable>")
        hasher.update(b"\0")
    return {"path": str(path), "files": len(files), "sha256": hasher.hexdigest()}


def scan_tasks(task_dir: Path, resource_root: Path) -> tuple[list[Mapping[str, Any]], Dict[str, Any]]:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from pm_system_task_reconcile import scan_task_directory, summarize

    observations = scan_task_directory(task_dir, stale_after_seconds=3600, resource_root=resource_root)
    summary = summarize(observations)
    summary["stale_details"] = [
        {
            "task_id": item.get("task_id"),
            "resource_uri": item.get("resource_uri"),
            "external_status": item.get("external_status"),
            "reason": item.get("reason"),
        }
        for item in observations
        if item.get("classification") == "stale"
    ]
    return observations, summary


def write_isolated_observations(db_path: Path, observations: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from pm_system_store import PMSystemStore
    from pm_system_task_reconcile import observe_tasks

    store = PMSystemStore(db_path)
    counts = observe_tasks(store, observations)
    with store.connect() as connection:
        row_count = int(connection.execute("SELECT COUNT(*) FROM external_task_observations").fetchone()[0])
    return {
        "path": str(db_path),
        "schema_version": store.schema_version(),
        "observation_rows": row_count,
        "written_by_classification": dict(sorted(counts.items())),
        "pragmas": store.pragmas(),
    }


def checkpoint_summary(report_dir: Path) -> Dict[str, Any]:
    files = sorted(report_dir.glob("*hash-checkpoint.json"))
    by_source: Dict[str, Dict[str, Any]] = {}
    invalid: list[str] = []
    for path in files:
        value = load_json(path)
        if not isinstance(value, Mapping) or not isinstance(value.get("sources"), Mapping):
            invalid.append(path.name)
            continue
        for source, row in value["sources"].items():
            if isinstance(row, Mapping):
                by_source[str(source)] = {
                    "file": path.name,
                    "status": row.get("status"),
                    "total": row.get("total"),
                    "processed": row.get("processed"),
                    "hashed": row.get("hashed"),
                    "unchanged": row.get("unchanged"),
                    "completed": len(row.get("completed_doc_guids") or []),
                    "failed": len(row.get("failed_documents") or []),
                    "quarantined": len(row.get("quarantined_documents") or []),
                    "checkpoint_sha256": digest(path),
                }
    expected = set(SOURCES)
    return {
        "report_dir": str(report_dir),
        "files": [path.name for path in files],
        "invalid_files": invalid,
        "by_source": by_source,
        "missing_expected_sources": sorted(expected - set(by_source)),
        "unexpected_sources": sorted(set(by_source) - expected),
    }


def isolated_store_summary(paths: Iterable[Path]) -> list[Dict[str, Any]]:
    values: list[Dict[str, Any]] = []
    for state_dir in sorted({path.resolve() for path in paths if path.is_dir()}):
        ledger_path = state_dir / "ledger.json"
        checkpoint_path = state_dir / "hash-only-checkpoint.json"
        pending_path = state_dir / "pending-uploads.json"
        ledger = ledger_summary(ledger_path)
        checkpoint = load_json(checkpoint_path, {})
        pending = load_json(pending_path, {})
        values.append(
            {
                "state_dir": str(state_dir),
                "ledger": {key: value for key, value in ledger.items() if key != "targets_by_source"},
                "checkpoint": {
                    "exists": checkpoint_path.is_file(),
                    "sha256": digest(checkpoint_path),
                    "run_id": checkpoint.get("run_id") if isinstance(checkpoint, Mapping) else None,
                    "status": checkpoint.get("status") if isinstance(checkpoint, Mapping) else None,
                    "sources": sorted((checkpoint.get("sources") or {}).keys()) if isinstance(checkpoint, Mapping) else [],
                },
                "pending": {
                    "exists": pending_path.is_file(),
                    "count": len((pending or {}).get("items", [])) if isinstance(pending, Mapping) else None,
                    "by_status": dict(Counter(str(item.get("status")) for item in ((pending or {}).get("items", []) if isinstance(pending, Mapping) else []) if isinstance(item, Mapping))),
                },
            }
        )
    return values


def health(url: str, timeout: int = 10) -> Dict[str, Any]:
    endpoint = url.rstrip("/") + "/health"
    request = urllib.request.Request(endpoint, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=max(1, int(timeout))) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return {"url": endpoint, "http_status": response.status, "response": payload}
    except (OSError, urllib.error.URLError, ValueError) as exc:
        return {"url": endpoint, "http_status": None, "error": f"{type(exc).__name__}: {exc}"}


def reconcile(plans: Mapping[str, Mapping[str, Any]], ledger: Mapping[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    ledger_targets = ledger.get("targets_by_source", {}) if isinstance(ledger, Mapping) else {}
    for source in SOURCES:
        plan = plans.get(source) or {}
        planned = set(plan.get("kept_uris") or [])
        existing = set(ledger_targets.get(source) or [])
        result[source] = {
            "inventory_kept": len(planned),
            "ledger_rows": int((ledger.get("by_source") or {}).get(source, 0)),
            "inventory_hits": len(planned & existing),
            "inventory_missing_from_ledger": len(planned - existing),
            "ledger_surplus_not_in_inventory": len(existing - planned),
            "missing_sample": sorted(planned - existing)[:10],
            "surplus_sample": sorted(existing - planned)[:10],
        }
    return result


def audit(args: argparse.Namespace) -> Dict[str, Any]:
    audit_dir = Path(args.audit_dir).expanduser().resolve() if args.audit_dir else Path(tempfile.mkdtemp(prefix="v44-s84-m3.", dir="/private/tmp"))
    audit_dir.mkdir(parents=True, exist_ok=True)
    plans: Dict[str, Dict[str, Any]] = {}
    if args.skip_plans:
        for source in SOURCES:
            plans[source] = {"source": source, "exit_code": None, "totals": {}, "kept_uris": [], "skipped": True}
    else:
        for source in SOURCES:
            plans[source] = run_source_plan(source, Path(args.sync), args.plan_timeout)

    ledger_before = ledger_summary(Path(args.ledger))
    task_before = aggregate_directory(Path(args.task_dir))
    observations, task_summary = scan_tasks(Path(args.task_dir), Path(args.resource_root))
    isolated_db = write_isolated_observations(audit_dir / "pm-system.db", observations)
    task_after = aggregate_directory(Path(args.task_dir))
    checkpoints = checkpoint_summary(Path(args.report_dir))
    isolation = isolated_store_summary([Path(item) for item in args.m2_state_dir])
    m3 = reconcile(plans, ledger_before)
    ledger_after = ledger_summary(Path(args.ledger))
    plan_failures = [source for source, item in plans.items() if item.get("exit_code") not in (0, None) or item.get("locked")]
    health_probe = health(args.openviking_url)
    ov_healthy = bool((health_probe.get("response") or {}).get("healthy"))
    freeze_flag = read_runtime_flag("PM_V44_AUTOMATION_FREEZE")
    admission_flag = read_runtime_flag("PM_V44_ADMISSION")
    report_status = (
        "PASS"
        if (
            not args.skip_plans
            and not plan_failures
            and not checkpoints["invalid_files"]
            and not checkpoints["missing_expected_sources"]
            and ledger_before.get("sha256") == ledger_after.get("sha256")
            and task_before == task_after
            and ov_healthy
        )
        else "HOLD_CONTINUE"
    )
    return {
        "schema_version": "pm-system.s8.4-m3-audit.v1",
        "phase_id": "S8.4-M3",
        "observed_at": now_iso(),
        "audit_dir": str(audit_dir),
        "audit_db": str(audit_dir / "pm-system.db"),
        "isolated_observations": isolated_db,
        "status": report_status,
        "read_only": True,
        "sources": plans,
        "ledger_before": ledger_before,
        "ledger_after": ledger_after,
        "ledger_unchanged": ledger_before.get("sha256") == ledger_after.get("sha256"),
        "inventory_ledger_reconciliation": m3,
        "checkpoints": checkpoints,
        "task_files_before": task_before,
        "task_files_after": task_after,
        "task_files_unchanged": task_before == task_after,
        "task_summary": task_summary,
        "isolated_m2_state": isolation,
        "openviking_health": {"healthy": ov_healthy, "probe": health_probe},
        "freeze": {
            "PM_V44_AUTOMATION_FREEZE": freeze_flag["value"],
            "PM_V44_ADMISSION": admission_flag["value"],
            "evidence_source": {
                "PM_V44_AUTOMATION_FREEZE": freeze_flag["source"],
                "PM_V44_ADMISSION": admission_flag["source"],
            },
        },
        "plan_failures": plan_failures,
        "notes": [
            "Inventory and ledger counts are kept separate because the ledger retains historical surplus.",
            "Task files are observed as a bounded projection; stale and quarantine items are not auto-retried or mutated.",
            "Missing checkpoint artifacts remain HOLD_CONTINUE evidence gaps; no hash or Generation is fabricated.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync", default=str(DEFAULT_SYNC))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR))
    parser.add_argument("--resource-root", default=str(DEFAULT_RESOURCE_ROOT))
    parser.add_argument("--report-dir", default=str(PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告"))
    parser.add_argument("--openviking-url", default=os.environ.get("OPENVIKING_URL", DEFAULT_OV_URL))
    parser.add_argument("--m2-state-dir", action="append", default=[])
    parser.add_argument("--audit-dir")
    parser.add_argument("--plan-timeout", type=int, default=900)
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument("--output", help="write the machine-readable manifest to this path")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.m2_state_dir:
        args.m2_state_dir = [str(path / "state") for path in Path("/private/tmp").glob("v44-s84-m2.*/state")]
    value = audit(args)
    output = Path(args.output).expanduser().resolve() if args.output else Path(value["audit_dir"]) / "manifest.json"
    output.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": value["status"],
        "observed_at": value["observed_at"],
        "audit_dir": value["audit_dir"],
        "audit_db": value["audit_db"],
        "plan_failures": value["plan_failures"],
        "ledger_unchanged": value["ledger_unchanged"],
        "task_files_unchanged": value["task_files_unchanged"],
        "checkpoint_files": value["checkpoints"]["files"],
        "missing_checkpoint_sources": value["checkpoints"]["missing_expected_sources"],
        "task_files": value["task_summary"]["files"],
        "task_by_classification": value["task_summary"]["by_classification"],
        "isolated_observation_rows": value["isolated_observations"]["observation_rows"],
        "openviking_healthy": value["openviking_health"]["healthy"],
    }, ensure_ascii=False, indent=2))
    return 0 if value["status"] == "PASS" else 10


if __name__ == "__main__":
    raise SystemExit(main())
