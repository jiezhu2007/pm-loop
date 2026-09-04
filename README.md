# PM Loop

PM Loop 是一个面向个人 PM 工作流的本地控制面：把来源采集、决策分析、概念学习、竞品雷达、运行调度、OpenViking 语义队列、产物登记和历史留存收敛到一套可审计的运行模型中。

本仓库只包含源码、测试、配置模板和第三方依赖声明。运行数据库、时间轴、客户资料、知识库内容、凭证、日志和 macOS LaunchAgent 状态均不在仓库内。

## 目录

- `scripts/pm_loop_*`：PM Loop 按次运行、控制面快照和调度器。
- `scripts/pm_system_*`：SQLite 协调库、Gateway、Worker、Admission、Cockpit、证据与恢复门禁。
- `scripts/concept_*`：Concept Learning Loop 的发现、审核、编译和回读流程。
- `scripts/competitive_radar*`：竞品雷达采集与只读读模型。
- `scripts/retention_*`：历史数据保留、观察、回收和恢复演练。
- `scripts/artifact_*`：产物登记和文件树索引。
- `web/`：PM Loop Control Plane 的静态展示层。
- `config/`：调度、留存和能力声明模板。
- `vendor/openviking/`：OpenViking 固定版本、补丁和第三方声明。

## 快速开始

```bash
cd pm-loop
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
python -m pytest
```

也可使用已提交的锁文件执行可复现安装：`uv sync --all-extras`。默认测试仅使用临时目录；部署机 LaunchAgent 核验需要明确执行 `PM_LOOP_VERIFY_INSTALLED_RUNTIME=1 python -m pytest tests/test_pm_system_s9_writer_preflight.py`。

只读创建并运行一条 PM Loop：

```bash
PYTHONPATH=scripts python scripts/pm_loop_runner.py create \
  --loop-id daily-radar --permission-mode report --json
PYTHONPATH=scripts python scripts/pm_loop_runner.py list
```

控制面服务：

```bash
PYTHONPATH=scripts python scripts/pm_loop_control_plane_server.py \
  --project-root "$PWD" --state-dir "$HOME/.codex/pm-loop"
```

控制面默认只读；任何外部写入、时间轴写入或内容回收都需要显式的人工 Gate 和独立证据。

## 本机运行配置

生产运行仍依赖 Codex 本机运行时和 OpenViking 服务。可以通过环境变量覆盖项目根目录、Codex 根目录、协调库路径和 OpenViking 地址；不得把 `ovcli.conf`、API key、SQLite 数据库或本机 `state/` 目录提交到 Git。

常用变量：

- `PM_LOOP_PROJECT_ROOT`：PM Loop 工作区根目录。
- `CODEX_ROOT`：Codex 运行时根目录，默认 `~/.codex`。
- `PM_LOOP_CANONICAL_REGISTRY`：调度 canonical registry 路径。
- `PM_V44_ADMISSION` / `PM_V44_MAX_CODEX_SLOTS`：Admission 开关和并发槽位。

部分迁移/恢复脚本保留了 macOS LaunchAgent 的本机契约，只适合在配置完成的本机执行；源码仓库本身不提供任何客户数据或服务凭证。
`PM_LOOP_LAUNCH_ROOT` 可覆盖 LaunchAgent 目录，便于隔离测试或非默认部署目录。

`scripts/com.zhujie14.pm-scheduler.plist` 是不含机器路径的模板。安装前必须把 `__CODEX_PYTHON__`、`__PM_LOOP_RUNTIME_ROOT__`、`__PM_LOOP_STATE_ROOT__` 和 `__PM_LOOP_PROJECT_ROOT__` 替换成部署机的绝对路径，再交由 `launchctl bootstrap` 加载。

## OpenViking 依赖

PM Loop 使用 OpenViking `0.4.16` 兼容接口，并依赖 `jiezhu2007/OpenViking` 的固定 fork 提交 `cf3633f70836bc4cb6867ed9aae7c490d3f62ee6`。该 fork 基于 `volcengine/OpenViking`，许可证为 AGPL-3.0。

本仓库不复制 OpenViking 的完整源码和 `.venv`。`vendor/openviking/pm-queue-reliability-v1.1.patch` 保存 PM Loop 所需的运行时可靠性补丁，`config/openviking-dependency.json` 保存基线 tag、fork commit、补丁 SHA-256 和五个修改文件。安装或升级 OpenViking 后，应先按清单校验版本，再应用补丁；补丁脚本是 `scripts/openviking_runtime_patch.py`。

OpenViking 源码通过 `vendor/openviking/source` Git submodule 随仓库提供，固定到 `jiezhu2007/OpenViking` 的 `cf3633f70836bc4cb6867ed9aae7c490d3f62ee6`。克隆后执行 `git submodule update --init --recursive`，再按清单验证并应用补丁；父仓库只记录 submodule 指针，不把 OpenViking 历史复制到 PM Loop。

## 许可证

PM Loop 自有代码采用 MIT，第三方 OpenViking 仍按其 AGPL-3.0 条款使用。详见 `vendor/openviking/THIRD_PARTY_NOTICES.md`。
