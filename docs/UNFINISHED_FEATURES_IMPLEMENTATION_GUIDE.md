# SuperSQL 未完成功能实现指南（已与代码状态对齐）

更新时间：2026-04-18

本文档用于记录“当前仍未完成的能力”与推荐落地顺序。
说明：本文件是执行指南，不再保留历史阶段的过时判断；如需实时进展请同时参考 `IMPLEMENTATION_STATUS.md`。

相关参考：
- [DEVELOPMENT_PLAN.md](../DEVELOPMENT_PLAN.md)
- [MODULE_TASKS.md](../MODULE_TASKS.md)
- [IMPLEMENTATION_STATUS.md](../IMPLEMENTATION_STATUS.md)

---

## 1. 严格对照结论

本次对照范围：`java-master` / `java-regionserver` / `java-client` 的主干实现与测试现状。

### 1.1 已实现但历史文档曾写“未实现”的内容（已纠正）

- Client 已具备真实 RPC 链路（`MasterRpcClient` / `RegionRpcClient` + REPL 路由），不再是“演示模式”。
- Client 已支持 `NOT_LEADER` 重定向、`REDIRECT` 自愈、`MOVING` 有界重试、主副本瞬时不可达重试、`SELECT` 读故障转移（副本降级）与 `/meta/tables` 主动失效 watch。
- Master 的 `createTable/dropTable` 已通过真实 Thrift 下发 DDL，不是“仅写 ZK”。
- Master 的 `triggerRebalance()` 已实现最小可用迁移闭环，并且新增了失败回滚与补偿路径。
- RegionServer 的 `RegionAdminServiceImpl` 核心管理方法已具备基础实现（pause/resume/delete/transfer/copy/invalidate）。
- RegionServer 的 `executeBatch/createIndex/dropIndex` 已在 `RegionServiceImpl` 落地基础实现。

### 1.2 仍未完成且需继续推进的内容

- Master：完整动态调度与自治恢复闭环仍待完善（基础定时重均衡已落地）。
- RegionServer：WAL/复制/恢复的最终形态（更强一致语义与恢复协议）。
- RegionServer：完整迁移协议（包括更可靠的数据校验、幂等补偿与可确认完成语义）。
- Client：更细粒度可观测能力的外部导出与持久化仍待完善（按表内存指标统计已落地）。

---

## 2. 当前未完成项（按模块）

## 2.1 Master

### 已具备基础

- Leader 选举 + `/active-master` epoch CAS + Active 心跳。
- `createTable/dropTable/getTableLocation/list*` 主链路。
- `triggerRebalance()` 最小迁移闭环。
- 已有基础 `RebalanceScheduler`（定时触发 + 开关 + 最小触发间隔节流）。
- rebalance 的元数据回滚、target 残留清理补偿、cache invalidation/resume best-effort。

### 仍待实现

- `RegionMigrator`：迁移状态机化（准备/传输/切换/收尾）和幂等恢复。
- 故障闭环：RegionServer 下线后的自动副本修复、主副本晋升、路由稳定切换。
- 混沌与分区场景验证：确保 epoch/主从切换在网络抖动下行为可预测。

## 2.2 RegionServer

### 已具备基础

- 注册/心跳、MiniSQL 进程管理（含自动重启）。
- `RegionServiceImpl` 读写基础路径、`executeBatch`、索引相关接口。
- WAL 文件基础读写与恢复、ReplicaSync 基础同步与回放。
- RegionAdmin 基础管理路径；`deleteLocalTable` 对 assignment 已修复为“仅移除当前 RS”。

### 仍待实现

- WAL 最终协议：更明确的 PREPARE/COMMIT/ABORT 语义与恢复边界。
- 多数派复制最终语义：失败重试、超时降级、追赶一致性与幂等保障。
- 迁移协议完善：文件传输完整性校验、断点续传/重试、完成确认与回滚协议。
- 自治恢复：主副本晋升、补副本、恢复后自动追赶到一致状态。

## 2.3 Client

### 已具备基础

- DDL -> Master、DML -> Region 的真实路由。
- `NOT_LEADER` 与 `REDIRECT` 处理。
- `MOVING` 有界重试（参数可配）+ route cache 失效后回源。
- `SELECT` 在主副本不可达时支持按副本列表降级读取。
- 已支持读一致性分级：`EVENTUAL`（允许副本降级）与 `STRONG`（仅主副本）。
- 已支持基于 `/meta/tables` 事件的 route cache 主动失效（create/change/delete）。

### 仍待实现

- 可观测能力外部化：将按表内存指标接入统一监控/日志出口，并补充长期趋势统计。

---

## 3. 建议实施顺序（以低返工为目标）

1. 完善 Master 调度与迁移状态机
- 先补可观测与状态机，再扩自治恢复，避免“能跑但不可诊断”。

2. 完善 RegionServer 迁移协议与恢复语义
- 优先做“可校验、可恢复、可回滚”，再做性能优化。

3. 完善 Client 的读故障转移与主动失效
- 与 Master/RegionServer 的稳定协议配套推进，避免策略冲突。

4. 启用并收敛剩余 Disabled 规格测试
- 当前仍保留：
  - `java-master/.../MasterPlannedFeaturesTddTest`
  - `java-regionserver/.../RegionPlannedFeaturesTddTest`

---

## 4. 文档维护规则

- 当功能从“未实现”变为“已实现”时，同步更新本文件与 `IMPLEMENTATION_STATUS.md`。
- 本文件只保留“仍未完成项”与推进策略，不再描述已落地细节。
- 任何含“未实现/未落地”的条目都需要能在当前代码或测试中找到对应证据。
