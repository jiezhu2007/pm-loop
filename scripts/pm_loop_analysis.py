#!/usr/bin/env python3
"""Structured Codex analysis for one PM Loop run.

The module keeps model invocation behind a small callable boundary so tests can
exercise the full artifact contract without starting another Codex process.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from pm_loop_runtime import RunStore, atomic_json_write, now_iso


PROMPT_VERSION = "pm-loop-analysis-v2.1"
ANALYSIS_SCHEMA = "pm-loop.analysis.v2"
DECISION_SCHEMA = "pm-loop.decision.v2"
MANIFEST_SCHEMA = "pm-loop.analysis-manifest.v1"
ANSWERABILITY = {"answerable", "partial", "insufficient"}
SEVERITIES = {"info", "low", "medium", "high"}


def canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_json_object(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", cleaned, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        cleaned = fenced.group(1).strip()
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for index, char in enumerate(cleaned):
            if char != "{":
                continue
            try:
                value, _ = decoder.raw_decode(cleaned[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
        raise ValueError("Codex response did not contain a JSON object")
    if not isinstance(value, dict):
        raise ValueError("Codex response must be a JSON object")
    return value


def _safe_text(value: Any, limit: int = 4000) -> str:
    return str(value or "").strip()[:limit]


def _list_of_text(value: Any, limit: int = 20) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_safe_text(item, 1000) for item in value[:limit] if _safe_text(item, 1000)]


def evidence_catalog(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    catalog: Dict[str, Dict[str, Any]] = {}
    for source_id, source in (snapshot.get("sources") or {}).items():
        if not isinstance(source, dict):
            continue
        catalog[f"source:{source_id}"] = {
            "source_id": str(source_id),
            "status": source.get("status") or "unknown",
            "summary": {key: value for key, value in source.items() if key not in {"content", "raw", "token", "api_key"}},
        }
    return catalog


def build_prompt(request: Dict[str, Any], snapshot: Dict[str, Any]) -> str:
    catalog = evidence_catalog(snapshot)
    payload = {
        "run_id": request.get("run_id"),
        "loop_id": request.get("loop_id"),
        "permission_mode": request.get("permission_mode"),
        "scope": request.get("scope") or {},
        "loop_contract": request.get("loop_contract") or {},
        "snapshot_id": snapshot.get("snapshot_id"),
        "summary": snapshot.get("summary") or {},
        "evidence_catalog": catalog,
    }
    return f"""你是个人 PM Loop 的只读分析 Agent。请只依据下方 INPUT，不补造客户、数字、状态或证据。

任务规则：
1. 根据 loop_id 和 scope 形成可判断的 PM 结论；证据不足时 answerability 必须为 insufficient 或 partial。
2. 每条 finding 的 evidence_refs 只能使用 INPUT.evidence_catalog 中存在的 key。
3. permission_mode=approved_action 时，只能提出 kind=safe_draft 的本地草稿动作；不得发送消息、修改真源、发布或变更权限。
4. 输出必须是单个 JSON object，不要 Markdown fence，不要解释文字。

OUTPUT SCHEMA：
{{
  "answerability": "answerable|partial|insufficient",
  "confidence": 0.0,
  "conclusion": {{"headline": "", "rationale": [""]}},
  "findings": [{{"id":"finding-001","title":"","summary":"","severity":"info|low|medium|high","evidence_refs":["source:..."]}}],
  "gaps": [""],
  "proposed_actions": [{{"id":"action-001","title":"","kind":"safe_draft","requires_gate":true,"payload":{{"draft_type":"followup"}}}}]
}}

INPUT：
{json.dumps(payload, ensure_ascii=False, sort_keys=True)}
"""


def normalize_analysis(raw: Dict[str, Any], request: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    allowed_refs = set(evidence_catalog(snapshot))
    answerability = str(raw.get("answerability") or "insufficient")
    if answerability not in ANSWERABILITY:
        answerability = "insufficient"
    try:
        confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.0))))
    except (TypeError, ValueError):
        confidence = 0.0

    conclusion_raw = raw.get("conclusion") if isinstance(raw.get("conclusion"), dict) else {}
    findings = []
    for index, item in enumerate(raw.get("findings") or []):
        if not isinstance(item, dict):
            continue
        refs = [ref for ref in _list_of_text(item.get("evidence_refs"), 20) if ref in allowed_refs]
        severity = str(item.get("severity") or "info")
        findings.append(
            {
                "id": _safe_text(item.get("id"), 100) or f"finding-{index + 1:03d}",
                "title": _safe_text(item.get("title"), 300) or "未命名发现",
                "summary": _safe_text(item.get("summary"), 2000),
                "severity": severity if severity in SEVERITIES else "info",
                "evidence_refs": refs,
            }
        )

    permission_mode = str(request.get("permission_mode") or "report")
    actions = []
    for index, item in enumerate(raw.get("proposed_actions") or []):
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "safe_draft")
        if kind != "safe_draft":
            continue
        action = {
            "id": _safe_text(item.get("id"), 100) or f"action-{index + 1:03d}",
            "title": _safe_text(item.get("title"), 300) or "生成本地跟进草稿",
            "kind": "safe_draft",
            "requires_gate": permission_mode == "approved_action" or bool(item.get("requires_gate")),
            "payload": item.get("payload") if isinstance(item.get("payload"), dict) else {},
        }
        actions.append(action)

    # A model cannot turn a report/draft Loop into an approved external action.
    if permission_mode != "approved_action":
        for action in actions:
            action["requires_gate"] = False

    return {
        "schema_version": ANALYSIS_SCHEMA,
        "available": True,
        "run_id": request.get("run_id"),
        "loop_id": request.get("loop_id"),
        "loop_contract": request.get("loop_contract") or {},
        "snapshot_id": snapshot.get("snapshot_id"),
        "generated_at": now_iso(),
        "answerability": answerability,
        "confidence": confidence,
        "conclusion": {
            "headline": _safe_text(conclusion_raw.get("headline"), 500) or "证据不足，暂不形成确定结论",
            "rationale": _list_of_text(conclusion_raw.get("rationale"), 20),
        },
        "findings": findings,
        "gaps": _list_of_text(raw.get("gaps"), 30),
        "evidence_catalog": evidence_catalog(snapshot),
        "proposed_actions": actions,
    }


def build_decision(analysis: Dict[str, Any], request: Dict[str, Any]) -> Dict[str, Any]:
    actions = []
    for item in analysis.get("proposed_actions") or []:
        action = dict(item)
        action["action_hash"] = canonical_hash(
            {
                "run_id": request.get("run_id"),
                "snapshot_id": analysis.get("snapshot_id"),
                "action": item,
            }
        )
        action["status"] = "awaiting_human" if action.get("requires_gate") else "draft_only"
        actions.append(action)
    gated = [item for item in actions if item.get("requires_gate")]
    gate_binding = {
        "run_id": request.get("run_id"),
        "snapshot_id": analysis.get("snapshot_id"),
        "actor": "zhujie14",
        "actions": [{"action_id": item.get("id"), "action_hash": item.get("action_hash")} for item in gated],
    }
    expires_at = (datetime.now(timezone.utc) + timedelta(days=7)).replace(microsecond=0).isoformat().replace("+00:00", "Z") if gated else None
    gate_token = canonical_hash({**gate_binding, "expires_at": expires_at}) if gated else None
    return {
        "schema_version": DECISION_SCHEMA,
        "available": True,
        "run_id": request.get("run_id"),
        "loop_id": request.get("loop_id"),
        "snapshot_id": analysis.get("snapshot_id"),
        "generated_at": now_iso(),
        "gate": {
            "required": bool(gated),
            "gate_id": request.get("run_id") if gated else None,
            "actor": "zhujie14" if gated else None,
            "expires_at": expires_at,
            "token": gate_token,
            "binding": gate_binding if gated else None,
        },
        "proposed_actions": actions,
    }


def write_report(store: RunStore, run_id: str, analysis: Dict[str, Any]) -> Path:
    conclusion = analysis.get("conclusion") or {}
    lines = [
        f"# {analysis.get('loop_id', 'PM Loop')} 分析报告",
        "",
        f"- run_id：`{run_id}`",
        f"- snapshot_id：`{analysis.get('snapshot_id')}`",
        f"- answerability：`{analysis.get('answerability')}`",
        f"- confidence：`{analysis.get('confidence')}`",
        "",
        "## 结论",
        "",
        str(conclusion.get("headline") or "—"),
    ]
    for reason in conclusion.get("rationale") or []:
        lines.append(f"- {reason}")
    lines.extend(["", "## 发现", ""])
    for finding in analysis.get("findings") or []:
        refs = ", ".join(finding.get("evidence_refs") or []) or "无可绑定证据"
        lines.extend([f"### {finding.get('title')}", "", str(finding.get("summary") or "—"), "", f"证据：`{refs}`", ""])
    path = store.paths(run_id).draft
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def _load_llm_runner(codex_root: Path) -> Any:
    path = codex_root / "skills" / "shengsuan-concepts" / "scripts" / "llm_runner.py"
    if not path.is_file():
        raise FileNotFoundError(f"shared Codex runner is missing: {path}")
    spec = importlib.util.spec_from_file_location("pm_loop_shared_llm_runner", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load shared Codex runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def invoke_codex(prompt: str, timeout: int, codex_root: Path) -> Tuple[int, str, str, str]:
    module = _load_llm_runner(codex_root)
    result, output = module.run_prompt(prompt, timeout)
    return int(result.returncode), str(output or ""), str(result.stderr or ""), str(module.LLM_CLI)


def execute_analysis(
    store: RunStore,
    run_id: str,
    snapshot: Dict[str, Any],
    codex_root: Path,
    invoker: Optional[Callable[[str, int, Path], Tuple[int, str, str, str]]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Path]:
    request = store.request(run_id)
    prompt = build_prompt(request, snapshot)
    timeout = int((request.get("budget") or {}).get("max_seconds") or 900)
    manifest_path = store.paths(run_id).root / "analysis" / "manifest.json"
    input_path = store.paths(run_id).root / "analysis" / "input.json"
    attempt_path = store.paths(run_id).root / "analysis" / "attempt.json"
    input_value = {
        "run_id": run_id,
        "loop_id": request.get("loop_id"),
        "scope": request.get("scope") or {},
        "snapshot_id": snapshot.get("snapshot_id"),
        "evidence_refs": sorted(evidence_catalog(snapshot)),
    }
    atomic_json_write(input_path, input_value)
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "run_id": run_id,
        "loop_id": request.get("loop_id"),
        "prompt_version": PROMPT_VERSION,
        "snapshot_id": snapshot.get("snapshot_id"),
        "permission_mode": request.get("permission_mode"),
        "executor_mode": "codex",
        "input_hash": canonical_hash(input_value),
        "started_at": now_iso(),
    }
    atomic_json_write(manifest_path, manifest)
    caller = invoker or invoke_codex
    returncode, output, stderr, cli_path = caller(prompt, timeout, codex_root)
    atomic_json_write(
        attempt_path,
        {
            "schema_version": "pm-loop.analysis-attempt.v1",
            "run_id": run_id,
            "returncode": returncode,
            "cli_path": cli_path,
            "prompt_version": PROMPT_VERSION,
            "completed_at": now_iso(),
            "stderr_tail": _safe_text(stderr, 2000),
        },
    )
    manifest["cli_path"] = cli_path
    manifest["returncode"] = returncode
    manifest["completed_at"] = now_iso()
    atomic_json_write(manifest_path, manifest)
    if returncode != 0:
        raise RuntimeError(f"Codex analysis exited with code {returncode}: {_safe_text(stderr, 1000)}")
    analysis = normalize_analysis(parse_json_object(output), request, snapshot)
    decision = build_decision(analysis, request)
    atomic_json_write(store.paths(run_id).root / "analysis" / "analysis.json", analysis)
    atomic_json_write(store.paths(run_id).root / "decision" / "decision.json", decision)
    report_path = write_report(store, run_id, analysis)
    return analysis, decision, report_path
