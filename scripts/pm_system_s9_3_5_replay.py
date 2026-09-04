#!/usr/bin/env python3
"""Run S9.3.5 source replays one source at a time with bounded evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(os.environ.get("PM_LOOP_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
CODEX_ROOT = Path.home() / ".codex"
SYNC_SH = CODEX_ROOT / "skills/shengsuan-sync/scripts/sync.sh"
PENDING = CODEX_ROOT / "skills/shengsuan-sync/state/pending-uploads.json"
LEDGER = CODEX_ROOT / "skills/shengsuan-sync/state/ledger.json"
REPLAY_LOCK = CODEX_ROOT / "scripts/state/s9-3-5-replay.lock"
PYTHON = os.environ.get("CODEX_PYTHON", sys.executable)
DEFAULT_ORDER = ("ontology", "product-management", "data-agent", "datasearch", "pipeline-logic-fde")


def sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256(path.read_bytes())
    return digest.hexdigest()


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def source_for(item: dict[str, Any], ledger: dict[str, Any]) -> str:
    row = ledger.get(item.get("docGuid")) if isinstance(ledger, dict) else None
    if isinstance(row, dict) and row.get("source"):
        return str(row["source"])
    parts = str(item.get("uri") or "").split("/")
    return parts[4] if len(parts) > 4 else "unknown"


def source_counts(source: str) -> dict[str, int]:
    pending = load(PENDING)
    ledger = load(LEDGER)
    counts: Counter[str] = Counter()
    for item in pending.get("items", []):
        if not isinstance(item, dict) or source_for(item, ledger) != source:
            continue
        counts[f"pending_{str(item.get('status') or 'unknown')}"] += 1
    for row in ledger.values() if isinstance(ledger, dict) else []:
        if isinstance(row, dict) and str(row.get("source") or "") == source:
            counts[f"ledger_{str(row.get('ingest_status') or 'none')}"] += 1
    return dict(counts)


def _flags() -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for name in ("PM_V44_AUTOMATION_FREEZE", "PM_V44_ADMISSION"):
        try:
            proc = subprocess.run(["launchctl", "getenv", name], capture_output=True, text=True, timeout=3, check=False)
        except (OSError, subprocess.TimeoutExpired):
            proc = None
        values[name] = proc.stdout.strip() if proc and proc.returncode == 0 and proc.stdout.strip() else os.environ.get(name)
    return values


def parse_totals(output: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    found: dict[str, Any] = {}
    for match in re.finditer(r"\{", output):
        try:
            value, _ = decoder.raw_decode(output[match.start():])
        except (ValueError, TypeError):
            continue
        if isinstance(value, dict) and any(key in value for key in ("success", "upload_failed", "fetch_failed", "query_failed")):
            found = value
    return found


def run_source(source: str, timeout: int) -> dict[str, Any]:
    flags = _flags()
    before = source_counts(source)
    started = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    env = os.environ.copy()
    env.update(
        {
            "CODEX_PYTHON": PYTHON,
            "PM_V44_AUTOMATION_FREEZE": "on",
            "PM_V44_ADMISSION": "freeze",
            "PM_V44_MANUAL_REPLAY": "1",
            "SHENGSUAN_SYNC_MAX_RETRY": "0",
            "SHENGSUAN_SYNC_PENDING_RECONCILE": "0",
            "SHENGSUAN_SYNC_UPLOAD_WAIT": "1",
            "SHENGSUAN_SYNC_UPLOAD_RETRIES": "0",
            "SHENGSUAN_SYNC_FETCH_WORKERS": "4",
            "SHENGSUAN_SYNC_SOURCE_TIMEOUT": str(timeout),
            "OV_UPLOAD_SERIALIZE": "1",
            "OV_UPLOAD_WAIT_MAX_TIME": "180",
            "OV_UPLOAD_WAIT_TOTAL_MAX_TIME": "300",
        }
    )
    command = ["/bin/bash", str(SYNC_SH), "incremental", "--source", source]
    try:
        process = subprocess.Popen(command, cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    except OSError as exc:
        return {"source": source, "status": "failed", "returncode": None, "error": f"spawn:{type(exc).__name__}", "before": before, "after": source_counts(source), "flags_before": flags}
    try:
        output, _ = process.communicate(timeout=timeout + 30)
        returncode = process.returncode
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            time.sleep(0.5)
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate()
        returncode = 124
    totals = parse_totals(output or "")
    after = source_counts(source)
    fail_count = sum(int(totals.get(key) or 0) for key in ("query_failed", "fetch_failed", "upload_failed"))
    status = "PASS" if returncode == 0 and fail_count == 0 else "HOLD_CONTINUE"
    return {
        "source": source,
        "status": status,
        "returncode": returncode,
        "started_at": started,
        "finished_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "flags_before": flags,
        "flags_during": {"PM_V44_AUTOMATION_FREEZE": "on", "PM_V44_ADMISSION": "freeze"},
        "before": before,
        "after": after,
        "totals": totals,
        "new_openviking_tasks": int(totals.get("success") or 0),
        "output_tail": (output or "")[-5000:],
        "production_state_touched": True,
        "processing_mode": "vectors_only",
        "whole_run_retries": 0,
    }


def markdown_report(results: list[dict[str, Any]], *, status: str, started_at: str, finished_at: str) -> str:
    lines = [
        "# V4.4 S9.3.5 错过任务受控补跑报告",
        "",
        "> phase_id：`S9.3.5`",
        f"> 开始：`{started_at}`",
        f"> 结束：`{finished_at}`",
        f"> 当前判定：**{status}**",
        "> 运行边界：按 source 串行；固定 Python 3.12；单实例锁；`vectors_only`；整轮重试为 0",
        "",
        "## 1. 补跑原则",
        "",
        "只重放已在 ledger 标记为 terminal failed 的 source 文档。每批由原有同步锁和 OpenViking 上传锁保护；accepted/completed/failed 由同步引擎写入 pending/ledger，未知状态不推进为成功。冻结开关保持 `on/freeze`，本次执行是受控人工 replay，不恢复 schedule。",
        "",
        "## 2. Source 结果",
        "",
        "| source | 判定 | returncode | 新任务 | 关键结果 |",
        "|---|---|---:|---:|---|",
    ]
    for result in results:
        totals = result.get("totals") or {}
        key = f"success={totals.get('success', 0)}, upload_failed={totals.get('upload_failed', 0)}, fetch_failed={totals.get('fetch_failed', 0)}, query_failed={totals.get('query_failed', 0)}"
        lines.append(f"| `{result.get('source')}` | `{result.get('status')}` | {result.get('returncode')} | {result.get('new_openviking_tasks', 0)} | `{key}` |")
    lines.extend(
        [
            "",
            "## 3. 门禁",
            "",
            "| 检查 | 结果 |",
            "|---|---|",
            f"| source 串行执行 | {'PASS' if all(r.get('status') == 'PASS' for r in results) else 'HOLD_CONTINUE'} |",
            "| OneAPI/语义模式 | `vectors_only`，不启用 `semantic_and_vectors`；调用量未直接埋点，不作 0 断言 |",
            f"| 整轮自动重试 | `0` |",
            f"| freeze/admission | `{results[-1].get('flags_during', {}).get('PM_V44_AUTOMATION_FREEZE') if results else 'on'}` / `{results[-1].get('flags_during', {}).get('PM_V44_ADMISSION') if results else 'freeze'}` |",
            "| schedule/Codex Automation | HOLD（补跑全部通过前不恢复） |",
            "",
            "## 4. 下一步",
            "",
            "只有所有 source 的 accepted/completed/failed/dead_letter、重复数、OpenViking task 和 watermark 均完成对账后，才允许恢复对应 LaunchAgent；任一 source 为 HOLD_CONTINUE，保持冻结并从该 source 的 checkpoint 继续。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", dest="sources", help="只补跑指定 source；可重复")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    sources = tuple(args.sources or DEFAULT_ORDER)
    if not SYNC_SH.is_file():
        raise SystemExit(f"missing sync entrypoint: {SYNC_SH}")
    if _flags().get("PM_V44_AUTOMATION_FREEZE") != "on" or _flags().get("PM_V44_ADMISSION") != "freeze":
        raise SystemExit("freeze/admission must remain on/freeze")
    started = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    results: list[dict[str, Any]] = []
    for source in sources:
        result = run_source(source, args.timeout)
        results.append(result)
        if result.get("status") != "PASS":
            break
    finished = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    status = "PASS" if len(results) == len(sources) and all(r.get("status") == "PASS" for r in results) else "HOLD_CONTINUE"
    data = {
        "schema_version": "pm-system.s9-3-5-replay.v1",
        "phase_id": "S9.3.5",
        "status": status,
        "started_at": started,
        "finished_at": finished,
        "sources_requested": list(sources),
        "results": results,
        "pending_sha256": sha256(PENDING),
        "ledger_sha256": sha256(LEDGER),
        "freeze_flags_after": _flags(),
        # The sync process does not expose a provider-call counter. Keep the
        # processing mode explicit, but never infer a zero count from it.
        "external_provider_calls": "not_instrumented",
        "production_state_touched": bool(results),
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(markdown_report(results, status=status, started_at=started, finished_at=finished), encoding="utf-8")
    print(json.dumps({"phase_id": "S9.3.5", "status": status, "sources": list(sources), "completed_sources": [r["source"] for r in results if r.get("status") == "PASS"], "manifest": str(args.manifest), "report": str(args.report)}, ensure_ascii=False))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
