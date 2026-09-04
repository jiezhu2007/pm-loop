# PM Loop 架构

PM Loop 的核心边界是“本地可审计协调 + 外部能力适配”。SQLite 协调库记录作业、Run、事件、Admission、Provider 退避、Outbox、语义任务、证据和时间轴索引；OpenViking 负责知识资源检索和异步语义处理，但不是唯一主存。

## 运行链路

1. Source Adapter 采集本机运行状态或外部来源，生成带 schema 的快照。
2. PM Loop Runner 创建 Run，追加 append-only 事件并生成只读草稿。
3. Scheduler/Admission 按 profile、并发槽位、Provider bucket 和 deadline 发放 lease。
4. Worker 消费固定 handler，Gateway 把资源/概念请求写入 durable Outbox。
5. OpenViking 处理向量或语义任务；失败按错误类别、Retry-After、Circuit Breaker 和总墙钟收敛。
6. Cockpit 只读聚合健康、新鲜度、失败、留存和产物状态；高风险动作回到人工 Gate。

## 设计约束

- canonical schedule registry 是调度唯一来源，runtime mirror 必须 hash 一致。
- 状态和证据可恢复、可重放、可去重；外部写入不作为本地主状态。
- `vectors_only` 与语义处理解耦，Provider 不可用时不阻塞本地协调库。
- 任意物理回收默认 deny，需要范围、hash、过期时间、单次 claim 和 post-check。
- OpenViking 补丁只针对五个明确文件，版本或源码锚点不匹配时拒绝自动修改。

