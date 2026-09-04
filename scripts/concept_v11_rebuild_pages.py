#!/usr/bin/env python3
"""Rebuild the eight legacy concept pages from their verified current sources.

This is a deterministic content migration, not an LLM compiler.  Each page is
limited to claims present in the current P3 ``mapped`` evidence.  In
particular, where the current source proves a narrower capability than the
legacy page, the rebuilt page states that boundary instead of carrying forward
the old claim.  The script fails closed when the coverage report changes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import yaml


COVERAGE_SCHEMA = "concept-v11.source-coverage-report.v1"
REBUILD_SCHEMA = "concept-v11.current-source-rebuild.v1"
DEFAULT_COVERAGE = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "source-coverage-current.json"
DEFAULT_CONCEPT_ROOT = Path.home() / ".codex" / "skills" / "shengsuan-concepts"
DEFAULT_BACKUP_ROOT = Path.home() / ".codex" / "pm-loop" / "state" / "concept-v11" / "page-rebuild-backups"


SOURCES: dict[str, tuple[str, ...]] = {
    "API生成": (
        "viking://resources/shengsuan/public-docs/operations-guide/搜索服务-高代码开发指南/依赖服务使用指南.html/依赖服务使用指南/依赖服务使用指南_1.md",
        "viking://resources/shengsuan/public-docs/best-practices/符合OpenAI标准的第三方模型服务调用.html/Ymqurwelt.md",
        "viking://resources/shengsuan/public-docs/api-reference/API更新记录.html/hmdzlofc6.md",
    ),
    "多模态检索": (
        "viking://resources/shengsuan/public-docs/operations-guide/搜索服务-高代码开发指南/预置Data Search设计.html/Pmocshd0y.md",
    ),
    "审批流": (
        "viking://resources/shengsuan/product-management/prd/【PRD】DataBuilder-版本管理（Global-Branching）/PRD-版本管理-本体建模/PRDDataBuilder_版本管理本体建模模块/3._功能详述/3.6_Review_提案与审批_2more_b0610ace.md",
        "viking://resources/shengsuan/product-management/prd/【PRD】DataBuilder-版本管理（Global-Branching）/PRD-版本管理-数据管道/PRDDataBuilder_版本管理数据管道模块/4._API_设计/4.7_Proposal_与_Re_3more_a08a6a6d.md",
    ),
    "数据安全": (
        "viking://resources/shengsuan/public-docs/introduction/数据安全.html/9950fc6694ae82420482e5a87427e67548d243b34_81e7f156.md",
    ),
    "数据授权": (
        "viking://resources/shengsuan/public-docs/operations-guide/数据架构/模型.html/umocq87rg_2.md",
        "viking://resources/shengsuan/pipeline-logic-fde/pipeline/【逆向验证】非结构化数据和本体关联（周报、知识库验证）/逆向验证非结构化数据和本体关联周报知识库验证.md",
    ),
    "数据表": (
        "viking://resources/shengsuan/public-docs/operations-guide/元数据/数据表.html/Emsir27l5_1.md",
    ),
    "数据资产发布": (
        "viking://resources/shengsuan/product-management/prd/【产品融合】DataBuilder、EDAP产品融合设计/【PRD】DAMP数据资产上架DB数据表-集/1.背景.md",
    ),
    "文件管理": (
        "viking://resources/shengsuan/product-management/prd/Databuilder2026-7月迭代/【PRD】数据卷文件管理优化/PRD数据卷文件管理优化.md",
    ),
}


BODY: dict[str, str] = {
    "API生成": """# API生成

## 定义
当前可验证范围是平台对外 API 与 OpenAI 兼容第三方模型服务的接入和调用：可通过服务地址、API Key 与模型名完成配置，并在工作流、Notebook 或多模态处理算子中调用。当前来源不证明旧页面中“数据表自动生成数据服务 API”的能力仍然有效。

## 能力边界（能做什么）
- 支持配置兼容 OpenAI 规范的第三方模型服务，并在工作流、Notebook 与多模态处理算子中统一调用。
- 可配置服务地址、API Key、模型名称、提示词、采样参数、超时及扩展参数。
- 已发布的 REST 接口覆盖工作流操作和运维接口；接口发布记录的当前可验证时间为 2025-08-04。

## 已知限制（不能做什么/需定制）
- 当前证据不包含数据源表自动生成查询 API、API 网关发布、限流、版本回滚或调用监控的有效产品说明；这些旧能力不得据此页承诺。
- 第三方模型服务是否可用取决于服务商凭证、模型兼容性和调用配额，需在目标环境验证。

## 版本演进
| 时间 | 当前可验证变化 | 出处 |
|---|---|---|
| 2025-08-04 | 首批 REST 接口发布工作流操作和运维能力 | API 更新记录 |

## 关联概念
数据服务、工作流、Notebook、多模态检索

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "多模态检索": """# 多模态检索

## 定义
当前来源验证的是预置 Data Search 的混合检索设计：全文检索处理词法匹配，向量检索处理语义匹配，并以两阶段融合完成召回与排序。页面名称保留“多模态检索”，但当前证据未证明图片、音频或视频等多模态输入的完整检索能力。

## 能力边界（能做什么）
- 以 BM25 全文检索与向量检索互补，提高术语精确匹配和语义召回的平衡。
- 使用 Paragraph、Sentence 和 Custom Sentence 的父子结构组织检索证据。
- 在句级和段级分别融合向量与 BM25 分数，并可通过向量权重和召回倍数调整策略。

## 已知限制（不能做什么/需定制）
- 当前来源没有提供图像、音频、视频等输入模态的索引、召回或排序契约，不能将本页作为这些能力的交付承诺。
- 缺少子句结构时系统采用回退行为并在调试场景暴露索引质量问题，实际效果需以索引质量和调参验证为准。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | 预置 Data Search 采用全文与向量两阶段混合检索 | 预置Data Search设计 |

## 关联概念
数据搜索、向量化、Rerank、知识增强

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "审批流": """# 审批流

## 定义
审批流是版本管理中 Proposal 与 Review 的审核闭环：变更以 Proposal 提交，审核任务、检查项与合并入口在 Review 中统一呈现，审批和必要检查满足后才允许合并。

## 能力边界（能做什么）
- Review 展示 Proposal 概览、审批任务、检查项、资源变更和合并入口。
- 审核人可批准、请求变更、拒绝或评论；审批人须具备对应资源查看权限并被加入评审人。
- 本体或数据管道管理员可在紧急修复、审批人不可用或线下确认等场景跳过审批，但必须填写原因，并记录为 `SKIPPED`。
- 合并前检查覆盖审批、Rebase、Schema、权限、依赖数据与关联资源发布状态；任一阻断检查失败均不可合并。
- Proposal 与 Review API 支持幂等键、变更说明校验及只读 Diff 查询。

## 已知限制（不能做什么/需定制）
- Review 只用于比较、评论和审批，不在 Review 页面直接编辑资源。
- 跳过审批不是普通审批通过，前端和 API 必须保留 `SKIPPED` 状态与原因。
- 本页仅覆盖当前来源中的本体和数据管道 Proposal/Review 契约，其他资源类型需单独确认。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | Review 检查项与 Proposal/Review API 契约完成定义 | 版本管理 PRD |

## 关联概念
工作流、权限体系、数据管道、Ontology

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "数据安全": """# 数据安全

## 定义
百度胜算的数据安全能力围绕身份认证、资源隔离、细粒度访问控制、敏感数据防护、API 安全与日志追溯展开，支持工作空间、项目和资源层级的访问保护。

## 能力边界（能做什么）
- 基于主/子账号和 IAM 授权区分管理员、开发者、只读和审计等角色。
- 通过工作空间、项目、资源组和文件夹实施逻辑访问隔离。
- 权限可下沉到 Catalog、Schema、数据表、数据卷、数据集和本体对象，并覆盖查看、读取、编辑、管理等操作。
- 支持行列组合权限和字段级动态脱敏；动态脱敏仅改变查询返回结果，不改变底层原始数据。
- 数据服务 API 支持 AK/SK 签名和 API-Key 凭证管理，平台提供作业运行与实例日志查询、下载能力。

## 已知限制（不能做什么/需定制）
- 工作空间与项目隔离是逻辑隔离；物理隔离或独占集群须结合部署方案评估。
- Ray 引擎不在当前动态脱敏覆盖范围；API 导出、Data Agent 等旁路访问路径需按项目验收确认。
- 平台能力不等同于自动满足特定法规或已取得特定认证，具体范围应按对象类型、查询引擎、版本和部署形态验收。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | 形成身份、隔离、权限、脱敏、API 与日志的安全能力说明 | 数据安全产品介绍 |

## 关联概念
权限体系、数据授权、数据脱敏、监控告警、私有化部署

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "数据授权": """# 数据授权

## 定义
当前来源能够验证数据模型的创建、发布、废弃和物化生命周期，以及数据表物化时与 Catalog、Schema、数据源类型和表类型的映射。它没有给出独立的数据授权申请、审批或授权回收契约，因此本页只保留当前已验证范围。

## 能力边界（能做什么）
- 数据模型具有草稿、已发布、已废弃三种生命周期；已发布模型可物化为同名物理表。
- 模型和字段支持定义名称、类型、主键、非空约束、描述以及关联标准或维度/指标。
- 物化配置支持 Doris 和 Iceberg，数据库映射对齐 Catalog.Schema；Iceberg 外部表可配置 BOS 存储位置和访问凭证。

## 已知限制（不能做什么/需定制）
- 当前来源未证明独立的数据授权申请、审批、授权时效、回收或行列授权 API；不得把本页作为授权流程交付承诺。
- 已被其他模型或指标引用的已发布模型不能废弃；同名物理表已存在时首次物化失败。
- 外部表涉及外部存储、凭证和部署边界，需按实际数据源与环境验证。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | 模型生命周期和物化配置按当前操作指南定义 | 数据架构模型操作指南 |

## 关联概念
数据安全、权限体系、数据表、Catalog、Schema

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "数据表": """# 数据表

## 定义
数据表按所属数据模式分为 Datalake Iceberg/Lance 表、外部数据模式数据表和分析与 AI 搜索实例 Doris 表。不同类型的创建入口、可编辑项、详情页签和存储边界不同。

## 能力边界（能做什么）
- Datalake 支持创建 Iceberg 和 Lance 表；Doris 实例支持创建 Doris 表；外部表由外部数据源采集生成。
- 可配置表名、表类型、BOS 路径、字段、非空约束、描述与高级表属性；Iceberg 可配置分区规则。
- Iceberg、Lance 和 Doris 支持可视化表结构与 DDL 查看；Doris 还支持明细、唯一和聚合模型。
- 详情页按表类型提供概览、数据预览、详情、权限、血缘、质量、DDL 或自动运维等实际可用页签。
- Iceberg 和 Doris 表支持数据标准关联和数据脱敏配置，需具备数据表管理权限。

## 已知限制（不能做什么/需定制）
- 外部数据表数据仍存储在源端，平台侧仅提供元数据、预览、权限和质量等能力，不提供表结构编辑、重命名、删除或 DDL。
- Lance 表不配置 Iceberg 分区转换函数；其自动运维能力以实际版本为准。
- 数据脱敏删除不可恢复，且仅在当前来源列出的表类型与权限范围内可配置。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | 统一说明四类数据表的创建、管理与详情能力 | 数据表操作指南 |

## 关联概念
Catalog、Schema、外部表、数据质量、数据血缘、数据脱敏

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "数据资产发布": """# 数据资产发布

## 定义
数据资产发布是将已处理的数据表或数据集上架到 DAMP、纳入数据目录并赋予业务属性的过程，面向资产管理人员完成上架，面向业务人员完成发现、申请和使用。

## 能力边界（能做什么）
- 支持将 DB 数据表或数据集上架到资产目录，形成开发到资产管理的闭环。
- 数据表上架可设置字段业务属性、字段脱敏信息和行级权限；数据集不需要这些字段级配置。
- 数据表支持按行、列细粒度授权，可查看元信息、字段、样例、行级权限和关联 API 信息。
- 数据表可通过 SQL 分析查询、导出结果并上架为数据服务；数据集支持单文件下载和整体导出。

## 已知限制（不能做什么/需定制）
- 数据集只支持整体数据集级别授权，不支持行列细粒度权限。
- 数据集暂不支持上架为数据服务；其使用能力以数据文件下载和导出为主。
- 本页覆盖 DAMP 数据表/集上架范围，其他资产类型或跨系统发布链路需单独确认。

## 版本演进
| 状态 | 当前可验证变化 | 出处 |
|---|---|---|
| 当前 | 定义数据表/数据集上架、授权、查看和使用的差异 | DAMP 数据资产上架 PRD |

## 关联概念
数据表、数据集、数据服务、数据授权、数据安全

## 出现过的客户/评估
本轮仅完成当前来源重编译，历史客户评估不作为能力证据。
""",
    "文件管理": """# 文件管理

## 定义
文件管理覆盖数据卷中的文件搜索、批量下载和文件移动，用于提升数据工程师和业务分析师在非结构化文件场景下的检索、获取与组织效率。

## 能力边界（能做什么）
- 在当前路径支持前缀搜索和模糊搜索；前缀搜索由服务端执行，模糊搜索在当前已加载对象中执行，最多 1000 个对象。
- 支持选择多个文件或文件夹批量下载，后端将对象打包为 ZIP 并保留目录结构。
- 支持将单个文件跨数据卷移动，并校验目标路径、同名冲突和目标路径写入权限。
- 文件搜索和批量下载同样适用于数据集版本、模型版本和媒体集下的文件场景。

## 已知限制（不能做什么/需定制）
- 搜索仅限当前路径，不递归搜索子目录；模糊搜索仅覆盖当前已加载对象，最多 1000 个。
- 文件移动要求源数据卷管理权限；目标路径同名、无写权限、路径不存在或源文件已删除时会拒绝操作。
- 当前 PRD 定义的是单文件移动，未承诺文件夹批量移动。

## 版本演进
| 版本 | 当前可验证变化 | 出处 |
|---|---|---|
| v1.0 | 新增前缀/模糊搜索、批量下载和跨数据卷单文件移动 | 数据卷文件管理优化 PRD |

## 关联概念
数据卷、数据集、权限体系、数据安全

## 出现过的客户/评估
中铁科研院、国机提出文件搜索、批量下载和文件移动需求；具体交付以项目验收为准。
""",
}


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_coverage(path: Path) -> Mapping[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("coverage_not_object")
    if str(raw.get("schema") or "") != COVERAGE_SCHEMA or str(raw.get("status") or "") != "PASS":
        raise ValueError("coverage_not_pass_or_wrong_schema")
    return raw


def _current_sources(coverage: Mapping[str, Any], concept: str) -> tuple[str, ...]:
    rows = coverage.get("concepts")
    if not isinstance(rows, list):
        raise ValueError("coverage_concepts_missing")
    record = next((row for row in rows if isinstance(row, Mapping) and str(row.get("concept") or "") == concept), None)
    if not isinstance(record, Mapping):
        raise ValueError(f"coverage_concept_missing:{concept}")
    if str(record.get("coverage_status") or "") not in {"refreshable", "substituted"}:
        raise ValueError(f"coverage_concept_not_refreshable:{concept}")
    refs = record.get("references") if isinstance(record.get("references"), list) else []
    result = tuple(
        sorted(
            {
                str(row.get("source_uri") or "")
                for row in refs
                if isinstance(row, Mapping)
                and str(row.get("disposition") or "") in {"mapped", "substituted"}
                and str(row.get("source_map_status") or "") == "mapped"
                and str(row.get("source_uri") or "")
            }
        )
    )
    if not result:
        raise ValueError(f"coverage_current_sources_missing:{concept}")
    return result


def _frontmatter(path: Path) -> dict[str, Any]:
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        raise ValueError(f"page_frontmatter_missing:{path.name}")
    closing = content.find("\n---\n", 4)
    if closing < 0:
        raise ValueError(f"page_frontmatter_unclosed:{path.name}")
    metadata = yaml.safe_load(content[4:closing]) or {}
    if not isinstance(metadata, dict):
        raise ValueError(f"page_frontmatter_invalid:{path.name}")
    return metadata


def _content(concept: str, metadata: Mapping[str, Any], sources: tuple[str, ...], observed_at: str) -> str:
    output = dict(metadata)
    output["concept"] = concept
    output["sources"] = list(sources)
    output["last_updated"] = observed_at
    output["latest_version"] = "current-source-rebuild-v1"
    header = yaml.safe_dump(output, allow_unicode=True, sort_keys=False, width=160).strip()
    evidence = "\n".join(f"- {uri}" for uri in sources)
    return f"---\n{header}\n---\n\n{BODY[concept].rstrip()}\n\n## 证据与待确认点\n- 当前页面只以以下 `mapped/substituted` 来源为证据：\n{evidence}\n- 历史 `historical_exclusion` 引用保留在来源账本中，但不再用作当前能力依据。\n"


def build_plan(*, coverage_path: Path, concept_root: Path, observed_at: Optional[str] = None) -> dict[str, Any]:
    coverage = _read_coverage(coverage_path)
    timestamp = observed_at or _now()
    changes: list[dict[str, Any]] = []
    errors: list[str] = []
    for concept in sorted(SOURCES):
        page = concept_root / "state" / "pages" / f"{concept}.md"
        try:
            if not page.is_file():
                raise ValueError(f"page_missing:{concept}")
            expected = tuple(sorted(SOURCES[concept]))
            current = _current_sources(coverage, concept)
            if current != expected:
                raise ValueError(f"coverage_sources_drift:{concept}")
            metadata = _frontmatter(page)
            if str(metadata.get("concept") or "") != concept:
                raise ValueError(f"page_concept_mismatch:{concept}")
            rebuilt = _content(concept, metadata, expected, timestamp)
            changes.append(
                {
                    "concept": concept,
                    "page_path": str(page),
                    "before_sha256": _file_hash(page),
                    "after_sha256": "sha256:" + hashlib.sha256(rebuilt.encode("utf-8")).hexdigest(),
                    "sources": list(expected),
                    "content": rebuilt,
                }
            )
        except (OSError, ValueError, yaml.YAMLError) as exc:
            errors.append(f"{type(exc).__name__}:{exc}")
    body = {
        "schema": REBUILD_SCHEMA,
        "status": "PASS" if not errors and len(changes) == len(SOURCES) else "HOLD",
        "read_only": True,
        "external_calls": {"oneapi": 0, "openviking": 0},
        "observed_at": timestamp,
        "coverage_path": str(coverage_path),
        "coverage_report_hash": str(coverage.get("report_hash") or ""),
        "concept_root": str(concept_root),
        "change_count": len(changes),
        "changes": changes,
        "errors": sorted(set(errors)),
    }
    return {**body, "plan_hash": _hash(body)}


def apply_plan(plan: Mapping[str, Any], *, backup_root: Path) -> dict[str, Any]:
    if str(plan.get("status") or "") != "PASS":
        return {"status": "HOLD", "applied": False, "errors": ["plan_not_pass"]}
    changes = plan.get("changes") if isinstance(plan.get("changes"), list) else []
    validated: list[tuple[Path, str]] = []
    for change in changes:
        if not isinstance(change, Mapping):
            return {"status": "HOLD", "applied": False, "errors": ["change_invalid"]}
        page = Path(str(change.get("page_path") or ""))
        if not page.is_file() or _file_hash(page) != str(change.get("before_sha256") or ""):
            return {"status": "HOLD", "applied": False, "errors": [f"page_changed_since_plan:{page.name}"]}
        content = str(change.get("content") or "")
        validated.append((page, content))
    backup_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + str(plan.get("plan_hash") or "").split(":")[-1][:12]
    backup_dir = backup_root / backup_id
    if backup_dir.exists():
        return {"status": "HOLD", "applied": False, "errors": [f"backup_id_exists:{backup_id}"]}
    backup_dir.mkdir(parents=True)
    try:
        for page, _ in validated:
            shutil.copy2(page, backup_dir / page.name)
        _write_json(
            backup_dir / "manifest.json",
            {
                "schema": REBUILD_SCHEMA + ".backup.v1",
                "plan_hash": plan.get("plan_hash"),
                "observed_at": plan.get("observed_at"),
                "pages": [{"name": page.name, "sha256": _file_hash(page)} for page, _ in validated],
            },
        )
    except OSError as exc:
        return {"status": "HOLD", "applied": False, "backup_dir": str(backup_dir), "errors": [f"backup_failed:{type(exc).__name__}"]}

    written: list[str] = []
    try:
        for page, content in validated:
            temporary = page.with_name(f".{page.name}.current-source-rebuild.tmp")
            temporary.write_text(content, encoding="utf-8")
            os.replace(temporary, page)
            written.append(str(page))
    except OSError as exc:
        restored: list[str] = []
        for written_path in written:
            page = Path(written_path)
            backup = backup_dir / page.name
            temporary = page.with_name(f".{page.name}.current-source-rollback.tmp")
            try:
                shutil.copy2(backup, temporary)
                os.replace(temporary, page)
                restored.append(written_path)
            except OSError:
                pass
        return {
            "status": "HOLD",
            "applied": False,
            "backup_dir": str(backup_dir),
            "written": written,
            "restored": restored,
            "errors": [f"write_failed:{type(exc).__name__}"],
        }
    return {"status": "PASS", "applied": True, "backup_dir": str(backup_dir), "written": written, "errors": []}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--concept-root", type=Path, default=DEFAULT_CONCEPT_ROOT)
    parser.add_argument("--observed-at")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        plan = build_plan(
            coverage_path=args.coverage.expanduser().resolve(),
            concept_root=args.concept_root.expanduser().resolve(),
            observed_at=args.observed_at,
        )
        result = {key: value for key, value in plan.items() if key != "changes"}
        result["apply"] = bool(args.apply)
        result["apply_result"] = apply_plan(plan, backup_root=args.backup_root.expanduser().resolve()) if args.apply else {"status": "DRY_RUN", "applied": False, "errors": []}
        if result["apply_result"].get("status") != "PASS" and args.apply:
            result["status"] = "HOLD"
    except Exception as exc:
        result = {"schema": REBUILD_SCHEMA, "status": "HOLD", "apply": bool(args.apply), "errors": [f"{type(exc).__name__}:{exc}"]}
    _write_json(args.report.expanduser().resolve(), result)
    print(json.dumps({key: result.get(key) for key in ("schema", "status", "change_count", "errors", "plan_hash", "apply_result")}, ensure_ascii=False))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
