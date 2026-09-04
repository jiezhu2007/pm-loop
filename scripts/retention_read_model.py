#!/usr/bin/env python3
"""Read-only Control Plane projection for PM Loop retention artifacts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Mapping, Optional

from pm_schedule_registry import RegistryError, latest_scheduled_at, load_registry, next_scheduled_at
from retention_registry import DEFAULT_SOURCE_REGISTRY, RetentionConfigError, canonical_hash, load_bundle, normalize_relative_path


READ_MODEL_SCHEMA = "pm-loop.retention-read-model.v1"
REASON_TEXT = {
    "unregistered_source": ("发现尚未登记的数据域", "该数据不会自动回收，容量会继续增长。", "确认注册为新 source、归入已有 source，或明确排除并记录理由。"),
    "unclassified_object": ("已登记来源中存在未分类对象", "没有匹配的 R0-R5 策略，删除已抑制。", "补充对象分类证据和 observe-only 策略后重跑 observer。"),
    "unsupported_object_contract": ("当前 adapter 不支持该对象结构", "无法证明对象边界和恢复方式，删除已抑制。", "评估是否补固定 object contract；新存储协议需新增 ADR。"),
    "excluded_object": ("对象被来源解析规则排除", "容量仍被计入，但当前不会进入回收计划。", "确认排除依据，必要时调整 include/exclude 后重跑 observer。"),
    "reference_graph_incomplete": ("引用关系无法闭合", "无法证明没有活动消费者，删除已抑制。", "只读核对 reference provider、latest/current 指针和活动 lease。"),
    "source_stale": ("来源盘点已过 freshness SLA", "过期来源不能提供删除结论。", "先恢复来源刷新，再重跑 observer。"),
    "inventory_partial": ("来源盘点不完整", "只可使用明确可见事实，潜在缺失保持未知。", "核对权限、并发写入和 adapter 读取错误后重跑 observer。"),
    "path_policy_violation": ("受信路径边界校验失败", "该来源已熔断，任何回收计划都不可执行。", "核对 symlink、mount、root identity 和 canonical registry；不要移动或删除对象。"),
    "registry_hash_mismatch": ("canonical 与 runtime registry 不一致", "全局 reclaimer 已禁用。", "比较三份配置 hash，原子同步 runtime 后做只读 smoke。"),
    "restore_verification_failed": ("恢复验证失败", "同类 R1 替换必须停止。", "保留原件，核对压缩包、manifest、条目数和消费者 smoke。"),
    "post_check_failed": ("处置后业务检查失败", "系统应恢复原件并熔断同类动作。", "只读核对 quarantine manifest、CAS 状态和恢复报告。"),
    "active_reference_or_lease": ("对象仍有活动引用或 lease", "TTL 需重新计时，当前不可回收。", "等待活动使用结束并重新观察，不要强制解除 lease。"),
    "insufficient_headroom": ("临时空间不足", "当前批次无法安全完成恢复验证或隔离。", "核对可用空间和批次额度，缩小批次后重新 dry-run。"),
    "capability_not_granted": ("没有精确物理回收授权", "对象可观察但不能移动、隔离或删除。", "确认是否需要 ADR、恢复演练和单对象 canary；不要仅修改 registry 扩权。"),
    "backup_snapshot_manifest_missing": ("Runtime 备份未提交完整快照清单", "无法证明该目录可恢复，删除已抑制。", "仅在逐个补录并校验 manifest 后，才可进入快照级保留集。"),
    "snapshot_action_not_enabled": ("Runtime 历史完整快照可识别但未启用组级回收", "通用文件回收器不能安全删除快照的一部分。", "保持当前快照不动；单独启用 manifest 绑定的快照级 action 后再做 canary。"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_read(root: Path, relative: Any) -> Dict[str, Any]:
    value = normalize_relative_path(relative)
    target = root.joinpath(*PurePosixPath(value).parts)
    try:
        target.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise ValueError("retention artifact escapes state root") from exc
    if target.is_symlink():
        raise ValueError("retention artifact symlink is not allowed")
    data = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("retention artifact must be an object")
    return data


def _display_bytes(value: Any) -> str:
    if value is None:
        return "未知"
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number:.1f} {unit}" if unit != "B" else f"{int(number)} B"
        number /= 1024
    return "未知"


def _bounded(value: Any, maximum: int = 160) -> str:
    return "".join(char if ord(char) >= 32 else " " for char in str(value or ""))[:maximum].strip()


def _advice(item: Mapping[str, Any]) -> Dict[str, Any]:
    reason = str(item.get("reason_code") or "unclassified_object")
    if reason not in REASON_TEXT:
        reason = "unclassified_object"
    handles = [_bounded(value, 120) for value in item.get("evidence_handles", []) if str(value).startswith("retention://")][:5]
    facts = f"{REASON_TEXT[reason][0]}；对象数 {item.get('object_count') if item.get('object_count') is not None else '未知'}；当前占用 {_display_bytes(item.get('logical_bytes'))}。"
    prompt = "\n".join([
        "请处理 PM Loop retention 无法处理项。",
        f"对象：{_bounded(item.get('object_id') or item.get('unknown_id'), 80)}",
        f"来源：{_bounded(item.get('source_id') or 'unregistered', 80)}",
        f"阻塞原因：{reason}",
        f"当前占用/增长：{_display_bytes(item.get('logical_bytes'))} / {_display_bytes(item.get('growth_7d_bytes'))}",
        f"证据句柄：{', '.join(handles) if handles else '未记录'}",
        "",
        "<DATA>",
        facts,
        "</DATA>",
        "",
        "请先只读核对 source registry、retention policy、引用关系和消费者。",
        "输出：数据事实、风险级别、推荐 R0-R5、是否需要新增 adapter/ADR、建议的 observe-only 验证步骤。未经确认不得删除、移动或扩大权限。",
    ])
    return {"schema_version": "pm-loop.retention-advice.v1", "deterministic": True, "prompt": prompt, "evidence_handles": handles}


def _metric(value: Any, *, status: str, coverage: str, as_of: Optional[str], reason_codes: Optional[list[str]] = None) -> Dict[str, Any]:
    return {"value": value, "status": status, "coverage": coverage, "as_of": as_of, "reason_codes": reason_codes or []}


def _sum_unknown_logical_bytes(items: list[Mapping[str, Any]]) -> int:
    """Sum disjoint observer findings, including multiple unregistered roots."""
    return sum(int(item.get("logical_bytes") or 0) for item in items)


class RetentionReadModel:
    read_only = True

    def __init__(self, *, state_root: Path, registry_path: Path = DEFAULT_SOURCE_REGISTRY, schedule_registry_path: Optional[Path] = None, db_path: Optional[Path] = None) -> None:
        self.state_root = Path(state_root).expanduser().resolve()
        self.registry_path = Path(registry_path).expanduser().resolve()
        self.schedule_registry_path = Path(schedule_registry_path).expanduser().resolve() if schedule_registry_path else None
        self.db_path = Path(db_path).expanduser().resolve() if db_path else None

    def _actions(self) -> list[Dict[str, Any]]:
        actions: list[Dict[str, Any]] = []
        if self.db_path and self.db_path.is_file():
            try:
                connection = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=2)
                connection.row_factory = sqlite3.Row
                try:
                    rows = connection.execute(
                        """SELECT action_id,object_id,source_id,action_profile,state,reason_code,message,
                                  expected_reclaim_bytes,reclaimed_logical_bytes,reclaimed_allocated_bytes,updated_at
                           FROM retention_actions ORDER BY updated_at DESC,action_id LIMIT 5000"""
                    ).fetchall()
                finally:
                    connection.close()
                for row in rows:
                    item = dict(row)
                    actions.append({
                        "action_id": item["action_id"], "object_id": item["object_id"], "source_id": item["source_id"],
                        "action": item["action_profile"], "status": item["state"], "reason_code": item["reason_code"],
                        "observed_at": item["updated_at"], "expected_reclaim_bytes": item["expected_reclaim_bytes"],
                        "reclaimed_logical_bytes": item["reclaimed_logical_bytes"], "reclaimed_allocated_bytes": item["reclaimed_allocated_bytes"],
                        "message": _bounded(item["message"], 240), "evidence_handle": f"retention://action/{item['action_id']}",
                    })
                return actions
            except (OSError, sqlite3.DatabaseError):
                actions = []
        reclaimer_root = self.state_root / "reclaimer"
        try:
            candidates = sorted(reclaimer_root.glob("*/result.json"), key=lambda item: item.stat().st_mtime_ns, reverse=True)[:1000]
        except OSError:
            candidates = []
        for candidate in candidates:
            try:
                if candidate.is_symlink():
                    continue
                result = json.loads(candidate.read_text(encoding="utf-8"))
                if not isinstance(result, Mapping):
                    continue
                actions.append({
                    "action_id": _bounded(result.get("run_id") or "unknown", 120), "object_id": None, "source_id": None,
                    "action": "reclaimer_run", "status": result.get("status") or "unknown", "reason_code": result.get("reason_code"),
                    "observed_at": result.get("observed_at"), "expected_reclaim_bytes": result.get("planned_allocated_bytes", 0),
                    "reclaimed_logical_bytes": result.get("reclaimed_logical_bytes", 0),
                    "reclaimed_allocated_bytes": result.get("reclaimed_allocated_bytes", 0),
                    "message": _bounded(result.get("message"), 240),
                    "evidence_handle": f"retention://reclaimer/{_bounded(result.get('run_id') or 'unknown', 120)}",
                })
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return actions

    def _next_runs(self, current: datetime) -> Dict[str, Any]:
        result = {"observer": None, "reclaimer": None, "source_status": "unknown"}
        if not self.schedule_registry_path or not self.schedule_registry_path.is_file():
            return result
        try:
            registry = load_registry(self.schedule_registry_path)
            for key, target in (("retention-observer", "observer"), ("retention-reclaimer", "reclaimer")):
                task = registry.task(key)
                latest = latest_scheduled_at(task, current, timezone_name=registry.timezone_name)
                scheduled = next_scheduled_at(task, latest, timezone_name=registry.timezone_name)
                result[target] = scheduled.isoformat(timespec="seconds").replace("+00:00", "Z")
            result["source_status"] = "observed"
        except (OSError, RegistryError, KeyError, ValueError):
            pass
        return result

    def _not_recorded(self, *, error: Optional[str] = None) -> Dict[str, Any]:
        read_at = _now()
        return {
            "schema_version": READ_MODEL_SCHEMA, "read_only": True, "read_at": read_at, "as_of": None,
            "source_status": "unavailable" if error else "not_recorded", "freshness": "unknown", "evidence_status": "unknown",
            "source_version": canonical_hash(["not_recorded", bool(error)]), "source_cursor": canonical_hash(["not_recorded", bool(error)]),
            "mode": "unknown", "summary": {
                "managed_logical_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "managed_allocated_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "reclaimed_7d_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "reclaimed_30d_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "reclaimed_90d_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "expected_7d_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "expected_30d_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "quarantined_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
                "unknown_count": _metric(None, status="unknown", coverage="none", as_of=None),
                "unknown_logical_bytes": _metric(None, status="unknown", coverage="none", as_of=None),
            }, "sources": [], "actions": [], "unknowns": [], "plans": [], "next_runs": self._next_runs(datetime.now(timezone.utc)),
            "alerts": [{"severity": "P1", "reason_code": "retention_not_recorded", "title": "尚无 Retention 观察产物", "detail": "请等待或受控重跑 retention-observer。"}],
            "error": _bounded(error, 300) if error else None,
        }

    def snapshot(self) -> Dict[str, Any]:
        pointer_path = self.state_root / "latest-observer.json"
        if not pointer_path.is_file():
            return self._not_recorded()
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            result = _safe_read(self.state_root, pointer["result"])
            artifacts = result["artifacts"]
            inventory = _safe_read(self.state_root, artifacts["inventory"])
            unknown_doc = _safe_read(self.state_root, artifacts["unknowns"])
            plan = _safe_read(self.state_root, artifacts["plan"])
            bundle = load_bundle(self.registry_path, self.registry_path.with_name("retention-policy.v3.json"), self.registry_path.with_name("retention-deletion-capabilities.json"))
        except (OSError, KeyError, ValueError, json.JSONDecodeError, RetentionConfigError) as exc:
            return self._not_recorded(error=f"{type(exc).__name__}: {exc}")
        observed_at = result.get("observed_at")
        unknowns = []
        for raw in unknown_doc.get("items", []):
            if not isinstance(raw, Mapping):
                continue
            item = dict(raw)
            reason = str(item.get("reason_code") or "unclassified_object")
            title, impact, decision = REASON_TEXT.get(reason, REASON_TEXT["unclassified_object"])
            item.update({"title": title, "impact": impact, "decision_required": decision, "advice": _advice(item)})
            unknowns.append(item)
        sources = [dict(item) for item in inventory.get("sources", []) if isinstance(item, Mapping)]
        source_status = "partial" if any(item.get("inventory_complete") is not True for item in sources) else "observed" if sources else "not_recorded"
        complete = all(item.get("inventory_complete") is True for item in sources) if sources else False
        coverage = "complete" if complete else "partial" if sources else "none"
        managed_logical = sum(int(item.get("logical_bytes") or 0) for item in sources) if sources else None
        managed_allocated = sum(int(item.get("allocated_bytes") or 0) for item in sources) if sources else None
        due_7 = due_30 = 0
        current = datetime.now(timezone.utc)
        plan_usable = bool(plan.get("signature")) and bundle.global_mode == "enabled"
        for item in plan.get("items", []) if plan_usable else []:
            try:
                due = datetime.fromisoformat(str(item.get("due_at")).replace("Z", "+00:00")).astimezone(timezone.utc)
            except ValueError:
                continue
            if due <= current + timedelta(days=30):
                due_30 += int(item.get("expected_reclaim_bytes") or 0)
            if due <= current + timedelta(days=7):
                due_7 += int(item.get("expected_reclaim_bytes") or 0)
        unknown_bytes = _sum_unknown_logical_bytes(unknowns)
        actions = self._actions()
        def reclaimed_since(days: int) -> int:
            floor = current - timedelta(days=days)
            total = 0
            for item in actions:
                try:
                    action_at = datetime.fromisoformat(str(item.get("observed_at")).replace("Z", "+00:00")).astimezone(timezone.utc)
                except ValueError:
                    continue
                if action_at >= floor and item.get("status") in {"verified", "reclaimed", "post_verified"}:
                    total += int(item.get("reclaimed_allocated_bytes") or 0)
            return total
        reclaimed_7, reclaimed_30, reclaimed_90 = reclaimed_since(7), reclaimed_since(30), reclaimed_since(90)
        quarantined = sum(int(item.get("expected_reclaim_bytes") or 0) for item in actions if item.get("status") in {"applied", "quarantined"})
        alerts = []
        if bundle.global_mode != "enabled":
            alerts.append({"severity": "P2", "reason_code": "automatic_reclaim_disabled", "title": "自动物理回收尚未启用", "detail": "当前仅执行盘点、分类和 dry-run；注册表存在不等于获得删除授权。"})
        critical = [item for item in unknowns if item.get("severity") in {"P0", "P1"}]
        if critical:
            alerts.append({"severity": "P1" if not any(item.get("severity") == "P0" for item in critical) else "P0", "reason_code": "retention_needs_attention", "title": f"{len(critical)} 条无法处理项需要核对", "detail": "来源不完整、过期或未获得物理动作授权，相关删除结论已抑制。"})
        source_version = canonical_hash({"observer": pointer.get("artifact_digest"), "reclaimer": actions, "registry": bundle.source_registry_hash})
        return {
            "schema_version": READ_MODEL_SCHEMA, "read_only": True, "read_at": _now(), "as_of": observed_at,
            "source_status": source_status, "freshness": "fresh" if source_status == "observed" else "partial" if source_status == "partial" else "unknown", "evidence_status": "observed",
            "source_version": source_version, "source_cursor": source_version, "mode": bundle.global_mode,
            "policy_version": bundle.policy.get("policy_version"), "registry_version": bundle.registry.get("registry_version"),
            "summary": {
                "managed_logical_bytes": _metric(managed_logical, status="observed" if sources else "unknown", coverage=coverage, as_of=observed_at),
                "managed_allocated_bytes": _metric(managed_allocated, status="observed" if sources else "unknown", coverage=coverage, as_of=observed_at),
                "reclaimed_7d_bytes": _metric(reclaimed_7, status="observed", coverage="retention_action_ledger", as_of=observed_at),
                "reclaimed_30d_bytes": _metric(reclaimed_30, status="observed", coverage="retention_action_ledger", as_of=observed_at),
                "reclaimed_90d_bytes": _metric(reclaimed_90, status="observed", coverage="retention_action_ledger", as_of=observed_at),
                "expected_7d_bytes": _metric(due_7, status="observed", coverage="signed_plan", as_of=observed_at),
                "expected_30d_bytes": _metric(due_30, status="observed", coverage="signed_plan", as_of=observed_at),
                "quarantined_bytes": _metric(quarantined, status="observed", coverage="retention_action_ledger", as_of=observed_at),
                "unknown_count": _metric(len(unknowns), status="observed", coverage=coverage, as_of=observed_at),
                "unknown_logical_bytes": _metric(unknown_bytes, status="observed", coverage=coverage, as_of=observed_at),
            },
            "sources": sources, "actions": actions, "unknowns": unknowns,
            "plans": [{"plan_id": plan.get("plan_id"), "status": "signed" if plan.get("signature") else "unsigned", "issued_at": plan.get("issued_at"), "expires_at": plan.get("expires_at"), "item_count": len(plan.get("items", [])), "artifact_digest": pointer.get("artifact_digest"), "evidence_handle": f"retention://plan/{plan.get('plan_id')}"}],
            "next_runs": self._next_runs(current), "alerts": alerts,
            "data_quality": {"inventory_complete": complete, "source_count": len(sources), "partial_source_count": sum(1 for item in sources if not item.get("inventory_complete")), "deletion_conclusion_allowed": complete and bundle.global_mode == "enabled"},
        }

    def summary(self) -> Dict[str, Any]:
        value = self.snapshot()
        return {key: value[key] for key in ("schema_version", "read_only", "read_at", "as_of", "source_status", "freshness", "evidence_status", "source_version", "source_cursor", "mode", "policy_version", "registry_version", "summary", "next_runs", "alerts", "data_quality") if key in value}

    def resource(self, name: str, query: Optional[Mapping[str, list[str]]] = None) -> Dict[str, Any]:
        value = self.snapshot()
        rows = [dict(item) for item in value.get(name, []) if isinstance(item, Mapping)]
        parameters = query or {}
        needle = _bounded((parameters.get("q") or parameters.get("query") or [""])[0], 100).lower()
        reason = _bounded((parameters.get("reason") or [""])[0], 80)
        source = _bounded((parameters.get("source") or [""])[0], 80)
        status = _bounded((parameters.get("status") or [""])[0], 40)
        action = _bounded((parameters.get("action") or [""])[0], 80)
        if needle:
            rows = [item for item in rows if needle in " ".join(_bounded(item.get(field), 160).lower() for field in ("display_name", "source_label", "source_id", "object_id", "unknown_id", "action_id", "reason_code"))]
        if reason:
            rows = [item for item in rows if str(item.get("reason_code") or "") == reason]
        if source:
            rows = [item for item in rows if str(item.get("source_id") or "") == source]
        if status:
            rows = [item for item in rows if str(item.get("status") or "") == status]
        if action:
            rows = [item for item in rows if str(item.get("action") or "") == action]
        try:
            page = max(1, int((parameters.get("page") or ["1"])[0]))
            page_size = max(1, min(100, int((parameters.get("page_size") or parameters.get("limit") or ["50"])[0])))
        except (TypeError, ValueError):
            page, page_size = 1, 50
        total = len(rows)
        start = (page - 1) * page_size
        rows = rows[start:start + page_size]
        return {
            "schema_version": value["schema_version"], "read_only": True, "read_at": value["read_at"], "as_of": value["as_of"],
            "source_status": value["source_status"], "freshness": value["freshness"], "evidence_status": value["evidence_status"],
            "source_version": value["source_version"], "source_cursor": value["source_cursor"], name: rows,
            "pagination": {"page": page, "page_size": page_size, "total": total, "total_pages": max(1, (total + page_size - 1) // page_size)},
        }

    def plan(self, plan_id: str) -> Dict[str, Any]:
        value = self.snapshot()
        for item in value.get("plans", []):
            if item.get("plan_id") == plan_id:
                return {"schema_version": value["schema_version"], "read_only": True, "read_at": value["read_at"], "source_version": value["source_version"], "source_cursor": value["source_cursor"], "plan": item}
        raise KeyError(plan_id)


__all__ = ["READ_MODEL_SCHEMA", "RetentionReadModel"]
