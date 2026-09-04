# OpenViking 依赖说明

PM Loop 不把 OpenViking 的完整仓库、虚拟环境或本机数据目录复制进来。依赖通过固定 fork commit 加补丁的方式锁定：

- fork：`https://github.com/jiezhu2007/OpenViking`
- commit：`cf3633f70836bc4cb6867ed9aae7c490d3f62ee6`
- 兼容基线：`volcengine/OpenViking` `v0.4.16`
- 补丁：`pm-queue-reliability-v1.1.patch`

补丁覆盖模型 Retry-After、Provider 半开探针、语义队列重试/锁竞争、集合 schema 兼容和 Studio skill inventory 口径。完整文件列表、基线 commit 和校验值见 `config/openviking-dependency.json`。

本目录中的补丁是源码分发物，不包含任何 API key、OpenViking 存储数据或运行日志。

