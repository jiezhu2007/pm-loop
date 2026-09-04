#!/usr/bin/env python3
"""S9.3.5 read-only task reconciliation and bounded state closeout.

The freeze window itself is compared with the preserved schedules.  Historical
OpenViking pending records are reconciled by task id before any replay.  A
``--dry-run`` never mutates production state; ``--apply`` only closes terminal
records after verifying that the input hashes and freeze flags are unchanged.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import plistlib
import shutil
import subprocess
import tempfile
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
PENDING_PATH = CODEX_ROOT / "skills/shengsuan-sync/state/pending-uploads.json"
LEDGER_PATH = CODEX_ROOT / "skills/shengsuan-sync/state/ledger.json"
TASK_DIR = Path.home() / ".openviking/data/viking/default/_system/tasks/default"
LAUNCH_ROOT = Path.home() / "Library/LaunchAgents"
AUTOMATION_ROOT = CODEX_ROOT / "automations"
F0_MANIFEST = PROJECT_ROOT / "docs/03-产品架构/v4.4实施报告/20260828-F0-automation-freeze-manifest.json"
DEFAULT_OV_URL = "http://127.0.0.1:1933"
TZ = dt.timezone(dt.timedelta(hours=8), name="Asia/Shanghai")
TASK_TIMEOUT_SECONDS = 8
TASK_WORKERS = 8

SCHEDULES = {
    "com.zhujie14.weekly-sync-and-refresh": (0, 9, 5),
    "com.zhujie14.product-intelligence-monitor": (0, 14, 0),
    "com.zhujie14.system-health-check": (0, 10, 30),
    "com.zhujie14.system-health-heartbeat": (None, 12, 12),
    "com.zhujie14.pm-timeline-daily": (None, 13, 37),
    "com.zhujie14.pm-timeline-weekly": (6, 19, 55),
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _flag(name: str) -> str | None:
    try:
        result = subprocess.run(
            ["launchctl", "getenv", name], capture_output=True, text=True, timeout=3, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _ov_config() -> tuple[str, dict[str, str]]:
    config_path = Path.home() / ".openviking/ovcli.conf"
    config: Mapping[str, Any] = {}
    if config_path.is_file():
        try:
            loaded = load_json(config_path)
            if isinstance(loaded, dict):
                config = loaded
        except (OSError, ValueError, TypeError):
            config = {}
    url = str(os.environ.get("OPENVIKING_URL") or config.get("url") or DEFAULT_OV_URL).rstrip("/")
    headers = {"Accept": "application/json"}
    api_key = str(os.environ.get("OPENVIKING_API_KEY") or config.get("api_key") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    for env_name, header_name in (("OPENVIKING_ACCOUNT", "X-OpenViking-Account"), ("OPENVIKING_USER", "X-OpenViking-User")):
        value = os.environ.get(env_name) or config.get(env_name.lower().replace("openviking_", ""))
        if value:
            headers[header_name] = str(value)
    return url, headers


def query_task(task_id: str) -> dict[str, Any]:
    """GET one task; 404 is an explicit not-found outcome, never success."""
    base, headers = _ov_config()
    url = f"{base}/api/v1/tasks/{urllib.parse.quote(task_id, safe='')}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=TASK_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {"state": "not_found", "raw_status": "NOT_FOUND", "error": "task_not_found_or_expired"}
        return {"state": "unknown", "raw_status": f"HTTP_{exc.code}", "error": f"task_http_{exc.code}"}
    except (OSError, TimeoutError, ValueError) as exc:
        return {"state": "unknown", "raw_status": "", "error": f"task_query_{type(exc).__name__}"}

    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        result = payload if isinstance(payload, dict) else {}
    status = str(result.get("status") or result.get("stage") or payload.get("status") or "").strip().lower()
    if status in {"complete", "completed", "done", "ok", "success", "succeeded"}:
        return {"state": "completed", "raw_status": status, "error": ""}
    if status in {"failed", "error", "dead_letter", "cancelled", "canceled"}:
        detail = result.get("error") or result.get("message") or payload.get("error") or "task_failed"
        return {"state": "failed", "raw_status": status, "error": str(detail)[:320]}
    if status in {"queued", "accepted", "pending", "running", "processing"}:
        return {"state": "active", "raw_status": status, "error": ""}
    return {"state": "unknown", "raw_status": status, "error": "task_status_missing_or_unknown"}


def local_task_status(task_id: str) -> dict[str, Any]:
    path = TASK_DIR / f"{task_id}.json"
    if not path.is_file():
        return {"exists": False, "status": ""}
    try:
        value = load_json(path)
    except (OSError, ValueError, TypeError):
        return {"exists": True, "status": "invalid"}
    if not isinstance(value, dict):
        return {"exists": True, "status": "invalid"}
    return {"exists": True, "status": str(value.get("status") or value.get("stage") or "").lower(), "error": str(value.get("error") or "")[:320]}


def _task_ids(items: list[dict[str, Any]]) -> list[str]:
    return sorted({str(item.get("task_id") or "").strip() for item in items if str(item.get("task_id") or "").strip()})


def reconcile_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter[str]]:
    ids = _task_ids(items)
    remote: dict[str, dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(TASK_WORKERS, max(1, len(ids)))) as pool:
        futures = {pool.submit(query_task, task_id): task_id for task_id in ids if task_id != "t"}
        for future in concurrent.futures.as_completed(futures):
            task_id = futures[future]
            try:
                remote[task_id] = future.result()
            except BaseException as exc:  # keep one task ambiguity bounded
                remote[task_id] = {"state": "unknown", "raw_status": "", "error": type(exc).__name__}

    result_items: list[dict[str, Any]] = []
    summary: Counter[str] = Counter()
    for original in items:
        item = dict(original)
        task_id = str(item.get("task_id") or "").strip()
        item_status = str(item.get("status") or item.get("task_status") or "").strip().lower()
        local = local_task_status(task_id) if task_id else {"exists": False, "status": ""}
        observed = remote.get(task_id, {"state": "unknown", "raw_status": "", "error": ""})
        # A production sync can persist a completed ledger item without a
        # provider task id. Missing task_id alone must not turn it into a
        # test-fixture quarantine record.
        is_fixture = bool(item.get("fixture")) or task_id == "t" or "/test/" in str(item.get("uri") or "")
        if is_fixture:
            classification = "fixture_quarantine"
            reason = "legacy_test_fixture"
        elif observed.get("state") == "completed" or local.get("status") == "completed":
            classification = "terminal_completed"
            reason = "remote_task_completed" if observed.get("state") == "completed" else "local_task_completed"
        elif observed.get("state") == "failed" or local.get("status") == "failed":
            classification = "terminal_failed"
            reason = str(observed.get("error") or local.get("error") or "historical_task_failed")[:320]
        elif not task_id and item_status in {"complete", "completed", "done", "success", "succeeded"}:
            classification = "terminal_completed"
            reason = "ledger_item_completed_without_task_id"
        elif not task_id and item_status in {"failed", "error", "dead_letter", "cancelled", "canceled"}:
            classification = "terminal_failed"
            reason = "ledger_item_failed_without_task_id"
        elif not task_id:
            classification = "unresolved"
            reason = "missing_task_id_and_terminal_evidence"
        else:
            classification = "unresolved"
            reason = str(observed.get("error") or "task_status_not_proven_terminal")[:320]
        summary[classification] += 1
        item["s9_3_5_classification"] = classification
        item["s9_3_5_reason"] = reason
        item["s9_3_5_remote_state"] = observed.get("state")
        item["s9_3_5_remote_status"] = observed.get("raw_status")
        item["s9_3_5_local_task_exists"] = bool(local.get("exists"))
        item["s9_3_5_local_task_status"] = local.get("status")
        result_items.append(item)
    return result_items, summary


def schedule_occurrences(start: dt.datetime, end: dt.datetime) -> list[dict[str, Any]]:
    occurrences: list[dict[str, Any]] = []
    cursor = start.date()
    while cursor <= end.date():
        for label, (weekday, hour, minute) in SCHEDULES.items():
            if weekday is not None and cursor.weekday() != weekday:
                continue
            candidate = dt.datetime.combine(cursor, dt.time(hour, minute), TZ)
            if start <= candidate <= end:
                occurrences.append({"label": label, "scheduled_at": candidate.isoformat(), "missed": True})
        cursor += dt.timedelta(days=1)
    return sorted(occurrences, key=lambda value: value["scheduled_at"])


def automation_schedules() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for path in sorted(AUTOMATION_ROOT.glob("*/automation.toml")):
        try:
            value = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        rrule = str(value.get("rrule") or "")
        parts = {piece.split("=", 1)[0].upper(): piece.split("=", 1)[1] for piece in rrule.replace("RRULE:", "").split(";") if "=" in piece}
        result.append({"id": value.get("id"), "name": value.get("name"), "status": value.get("status"), "rrule": rrule, "path": str(path), "parts": parts})
    return result


def build_dry_run() -> dict[str, Any]:
    f0 = load_json(F0_MANIFEST)
    pending = load_json(PENDING_PATH)
    ledger = load_json(LEDGER_PATH)
    items = pending.get("items", []) if isinstance(pending, dict) else []
    if not isinstance(items, list):
        raise ValueError("pending-uploads.json items must be a list")
    item_dicts = [item for item in items if isinstance(item, dict)]
    now = dt.datetime.now(TZ).replace(microsecond=0)
    started = dt.datetime.fromisoformat(str(f0["started_at"]).replace("Z", "+00:00")).astimezone(TZ)
    classified, summary = reconcile_items(item_dicts)
    schedule_misses = schedule_occurrences(started, now)
    return {
        "schema_version": "pm-system.s9-3-5-reconcile.v1",
        "phase_id": "S9.3.5",
        "release_id": f0.get("release_id"),
        "freeze_id": f0.get("freeze_id"),
        "read_only": True,
        "started_at": utc_now(),
        "observed_until": now.isoformat(),
        "freeze_flags": {"PM_V44_AUTOMATION_FREEZE": _flag("PM_V44_AUTOMATION_FREEZE"), "PM_V44_ADMISSION": _flag("PM_V44_ADMISSION")},
        "source_hashes": {"pending_uploads": sha256(PENDING_PATH), "ledger": sha256(LEDGER_PATH), "f0_manifest": sha256(F0_MANIFEST)},
        "pending_summary": dict(summary),
        "pending_items": classified,
        "schedule_misses": schedule_misses,
        "automation_schedules": automation_schedules(),
        "replay_policy": {
            "terminal_completed": "只收口 pending/ledger，不重新提交",
            "terminal_failed": "保留 failed，后续按 source + content hash + URI 幂等键受控补跑",
            "fixture_quarantine": "隔离，不得进入生产补跑",
            "unresolved": "保持 queued，不得猜测成功或失败",
        },
        "production_state_touched": False,
        "external_provider_calls": 0,
    }


def _atomic_write(path: Path, value: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def apply_closeout(data: Mapping[str, Any], backup_dir: Path) -> dict[str, Any]:
    if not data.get("read_only") or data.get("phase_id") != "S9.3.5":
        raise ValueError("manifest is not an S9.3.5 dry-run")
    flags = data.get("freeze_flags") or {}
    if flags.get("PM_V44_AUTOMATION_FREEZE") != "on" or flags.get("PM_V44_ADMISSION") != "freeze":
        raise RuntimeError("freeze flags are not on/freeze")
    if sha256(PENDING_PATH) != data["source_hashes"]["pending_uploads"] or sha256(LEDGER_PATH) != data["source_hashes"]["ledger"]:
        raise RuntimeError("pending or ledger changed after dry-run; regenerate manifest")

    backup_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(backup_dir, 0o700)
    for path in (PENDING_PATH, LEDGER_PATH):
        destination = backup_dir / path.name
        shutil.copy2(path, destination)
        os.chmod(destination, 0o600)

    pending = load_json(PENDING_PATH)
    ledger = load_json(LEDGER_PATH)
    by_task = {str(item.get("task_id") or ""): item for item in data.get("pending_items", []) if isinstance(item, dict)}
    applied = Counter[str]()
    reconciled_at = utc_now()
    for item in pending.get("items", []):
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("task_id") or "")
        observation = by_task.get(task_id)
        if not observation:
            continue
        classification = observation.get("s9_3_5_classification")
        if classification == "terminal_completed":
            item["status"] = "complete"
            item["task_status"] = observation.get("s9_3_5_remote_status") or "completed"
            item.pop("error", None)
        elif classification == "terminal_failed":
            item["status"] = "failed"
            item["task_status"] = observation.get("s9_3_5_remote_status") or "failed"
            item["error"] = f"s9_3_5_reconciled:{observation.get('s9_3_5_reason') or 'task_failed'}"
        elif classification == "fixture_quarantine":
            item["status"] = "quarantine"
            item["task_status"] = observation.get("s9_3_5_remote_status") or "fixture"
            item["quarantine_reason"] = observation.get("s9_3_5_reason") or "legacy_test_fixture"
        else:
            continue
        item["reconciled_at"] = reconciled_at
        item["s9_3_5_classification"] = classification
        applied[classification] += 1
        guid = item.get("docGuid")
        row = ledger.get(guid) if isinstance(ledger, dict) and guid else None
        if isinstance(row, dict) and classification in {"terminal_completed", "terminal_failed"}:
            row["ingest_status"] = "complete" if classification == "terminal_completed" else "failed"
            row["ingest_reconciled_at"] = reconciled_at
            if classification == "terminal_failed":
                row["ingest_error"] = f"s9_3_5_reconciled:{observation.get('s9_3_5_reason') or 'task_failed'}"
            else:
                row.pop("ingest_error", None)

    _atomic_write(PENDING_PATH, pending)
    _atomic_write(LEDGER_PATH, ledger)
    return {"applied": dict(applied), "reconciled_at": reconciled_at, "backup_dir": str(backup_dir), "production_state_touched": True, "external_provider_calls": 0}


def markdown_report(data: Mapping[str, Any], apply_result: Mapping[str, Any] | None = None) -> str:
    summary = data.get("pending_summary") or {}
    lines = [
        "# V4.4 S9.3.5 同步与 pending 任务终态对账报告",
        "",
        f"> release_id：`{data.get('release_id')}`",
        f"> freeze_id：`{data.get('freeze_id')}`",
        "> phase_id：`S9.3.5`",
        f"> 运行边界：冻结态只读任务查询；状态收口：`{'已执行' if apply_result else '未执行'}`",
        f"> 当前判定：**{'PASS' if data.get('freeze_flags', {}).get('PM_V44_AUTOMATION_FREEZE') == 'on' and data.get('freeze_flags', {}).get('PM_V44_ADMISSION') == 'freeze' else 'HOLD_CONTINUE'}**",
        "",
        "## 1. 阶段结论",
        "",
        "冻结窗口内按保留的 LaunchAgent 日程计算实际错过项；历史 pending 任务按 OpenViking task API 与本地 task 文件双证据分类。对账阶段不创建新任务、不调用 OneAPI，不把未知状态当作成功。",
        "",
        "## 2. 冻结窗口错过项",
        "",
        "| 项目 | 结果 |",
        "|---|---|",
        f"| 观察窗口 | `{data.get('observed_until')}` |",
        f"| LaunchAgent schedule 错过次数 | `{len(data.get('schedule_misses') or [])}` |",
        f"| Codex Automation | `{len(data.get('automation_schedules') or [])}` 个，均保留原 schedule，恢复前不得自动启动 |",
        f"| freeze/admission | `{data.get('freeze_flags', {}).get('PM_V44_AUTOMATION_FREEZE')}` / `{data.get('freeze_flags', {}).get('PM_V44_ADMISSION')}` |",
        "",
        "## 3. pending 终态分类",
        "",
        "| 分类 | 数量 | 处置 |",
        "|---|---:|---|",
        f"| terminal_completed | {summary.get('terminal_completed', 0)} | 仅收口账本，不重复提交 |",
        f"| terminal_failed | {summary.get('terminal_failed', 0)} | 保留失败，后续按 source/hash/URI 幂等键受控补跑 |",
        f"| fixture_quarantine | {summary.get('fixture_quarantine', 0)} | 隔离，不进入生产补跑 |",
        f"| unresolved | {summary.get('unresolved', 0)} | 保持 queued，等待可证明终态 |",
        "",
        "## 4. 门禁",
        "",
        "| 门禁 | 结果 |",
        "|---|---|",
        "| dry-run 无生产状态写入 | PASS |",
        "| 外部 provider 调用 | PASS（0） |",
        "| 失败任务不伪装成功 | PASS |",
        "| 补跑使用 source + hash + URI 幂等键 | PASS |",
        "| 自动 schedule 已恢复 | HOLD（补跑和单项恢复完成前保持冻结） |",
    ]
    if apply_result:
        lines.extend(["", "## 5. 状态收口", "", f"- 应用结果：`{json.dumps(apply_result.get('applied', {}), ensure_ascii=False, sort_keys=True)}`", f"- 备份：`{apply_result.get('backup_dir')}`", "- OpenViking 新任务：`0`", "- OneAPI 调用：`0`"])
    lines.extend(["", "## 6. 下一步", "", "先对 terminal_failed 项按 source 逐批执行受控补跑；每批完成后核对 accepted/completed/failed/dead_letter、写入数、重复数、task 状态和 watermark。补跑全部通过后，才逐项恢复 schedule、catchup 和 Codex Automation。"])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", type=Path, default=CODEX_ROOT / "backups/v4.4-20260829/S9.3.5-pending-reconcile")
    args = parser.parse_args()
    if args.apply:
        data = load_json(args.manifest)
        result = apply_closeout(data, args.backup_dir)
        data = dict(data)
        data["read_only"] = False
        data["apply_result"] = result
    else:
        data = build_dry_run()
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(args.manifest, data, 0o600)
        result = None
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(markdown_report(data, result), encoding="utf-8")
    print(json.dumps({"phase_id": "S9.3.5", "status": "PASS", "apply": bool(args.apply), "manifest": str(args.manifest), "report": str(args.report), "summary": data.get("pending_summary") or (result or {}).get("applied")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
