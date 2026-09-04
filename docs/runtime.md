# 运行与发布边界

仓库内文件是可审查源码；以下内容必须由部署环境提供，不能提交：

- `~/.codex`、`~/.openviking` 下的配置、数据库、知识库数据和日志；
- PM Timeline、客户评估、内部文档、Ku 内容和任何凭证；
- macOS `~/Library/LaunchAgents` 下的机器级 plist；
- OpenViking `.venv`、缓存、Rust 构建产物和临时目录。

部署时先安装固定版本 OpenViking，再读取 `config/openviking-dependency.json` 校验版本和补丁 SHA-256，最后运行补丁入口。PM Loop 的状态目录应指向部署机上的独立路径；测试使用临时目录，不读取生产数据库。

GitHub 发布前应检查：

- `git status --short` 只包含预期源码和文档；
- `rg` 扫描没有 token、私有 URL、数据库和运行日志；
- `python -m pytest` 全部通过；
- OpenViking 依赖清单中的 commit、补丁 hash 和 changed files 与实际文件一致。

