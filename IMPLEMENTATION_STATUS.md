# SuperSQL 当前实现状态（2026-04-19）

本文档记录已落地并通过测试的 Java 分布式层功能。

## 1. Master 侧已实现内容

实现文件：
- java-master/src/main/java/edu/zju/supersql/master/election/LeaderElector.java

已落地能力：
- `/masters` Watcher：CuratorCache 监听在线 Master 节点变化。
- `/region_servers` Watcher：已通过 `RegionServerWatcher` 监听 RS 上线、更新、下线事件。
- HTTP 管理端点：`GET /health` 与 `GET /status` 返回 JSON（含角色信息）。
- createTable / dropTable 已通过真实 Thrift 调用把 DDL 转发到 RegionServer，再在成功后更新 ZooKeeper 元数据。
- `triggerRebalance()` 已具备最小可用迁移闭环：可把热点节点上的一个非主副本迁往更空闲节点，并更新 `/meta/tables` 与 `/assignments`。
- `triggerRebalance()` 过程中已显式写入 `PREPARING -> MOVING -> FINALIZING -> ACTIVE` 状态迁移，并在失败时进入 `ROLLBACK` 后恢复元数据。
- `triggerRebalance()` 在 `transferTable` 失败路径也会立即回滚到原始元数据（含 `ACTIVE` 状态），避免卡在 `MOVING`。
- `triggerRebalance()` 在 `pauseTableWrite` 失败路径也会回滚元数据，避免停留在 `PREPARING` 中间态。
- `triggerRebalance()` 在 `deleteLocalTable` 前会进入 `FINALIZING`，使“数据迁移已完成、待源端清理”阶段可观测。
- `getTableLocation/listTables/repairTableRoutesWithConfirmation` 已接入 `recoverStuckMigrationWithConfirmation`：当迁移状态超时且存在 `migrationAttemptId` 时，会按补偿确认协议执行恢复，而非直接回写 `ACTIVE`（`repairTableRoutesBestEffort` 仅作兼容别名）。
- `triggerRebalance()` 调度入口已接入卡死迁移预恢复：即使本轮负载均衡最终被跳过，也会先回收超时迁移状态，降低“长期无人读写表”卡死风险。
- `triggerRebalance()` 在被跳过（如 cluster balanced）时会在响应消息中附带本轮预恢复数量，便于上层调度/日志快速感知是否发生了状态回收。
- `triggerRebalance()` 状态推进会刷新 `/meta/tables/{table}` 的 `statusUpdatedAt` 时间戳，便于观测迁移阶段更新时间。
- `triggerRebalance()` 迁移中会写入 `migrationAttemptId`（PREPARING/MOVING 可观测），成功与回滚后会清理该字段，便于后续幂等恢复扩展。
- `triggerRebalance()` 已补充迁移上下文字段（`migrationSourceReplicaId` / `migrationTargetReplicaId`）；卡死迁移超时恢复不再仅回写状态，会按阶段执行补偿：`FINALIZING` 优先清理 source，`PREPARING/MOVING/ROLLBACK` 优先清理 target，再恢复为 `ACTIVE`。
- 卡死迁移恢复已升级为“可确认完成协议”：必要清理由 `deleteLocalTable` 多次重试确认（`OK/TABLE_NOT_FOUND` 视为确认完成），仅在补偿确认后才回到 `ACTIVE`。
- 当补偿确认失败或补偿对象不可解析时，状态会进入 `COMPENSATING`（而非继续停留在历史迁移态），并保留 `migrationCompensationRole/migrationCompensationBlocked/migrationCompensationLastError/migrationCompensationUpdatedAtMs` 以支持后续自动重试收敛。
- 对于 `MOVING/PREPARING/ROLLBACK` 且缺少 target 上下文的历史残留场景，恢复逻辑保留安全兜底（可直接回收为 `ACTIVE`），避免因上下文缺失导致永久卡死；其余场景坚持“补偿确认优先”。
- Master 迁移主流程已抽离到独立组件 `RegionMigrator`（`PREPARING -> MOVING -> FINALIZING/ROLLBACK -> ACTIVE`），`MasterServiceImpl` 调度入口改为调用该组件执行迁移编排。
- 卡死迁移恢复逻辑已进一步下沉到 `RegionMigrator`：`MasterServiceImpl` 仅保留迁移上下文读写与副本解析回调，迁移编排与超时恢复收敛到统一组件。
- 迁移补偿相关回调与调度入口命名已去除 `best-effort` 语义（如 `set/clear/readMigrationContext`、`recoverStuckMigrationsForRebalanceWithConfirmation`），与“确认后完成”的协议语义保持一致。
- `triggerRebalance()` 候选选择已限制为 `ACTIVE` 表，避免对 `PREPARING/MOVING` 表重复触发迁移。
- `triggerRebalance()` 候选选择已引入轮转游标（round-robin），连续调度时会轮换候选起点，降低同一表在失败重试场景下被重复优先命中的风险。
- `createTable` 成功落盘元数据后也会初始化 `statusUpdatedAt`，保证新表从创建时起具备状态时间戳。
- 已落地基础 RebalanceScheduler：按配置周期定时触发 `triggerRebalance()`，支持开关与最小触发间隔节流。
- Master `/status` 已输出 RebalanceScheduler 运行快照（tick/trigger/throttle/success/failure、最近执行时间、最近触发原因）。
- Master `/status` 已输出 route repair 运行指标（修复运行次数、累计修复表数、最近修复时间、最近修复表、最近错误），并新增近 N 次运行窗口统计（成功率、平均修复数）。
- Master `/status` 的 route repair 指标已补充最近一次扫描范围观测（`lastRunTotalTables`/`lastRunCandidateTables`/`lastRunFilterRegionServerId`），便于区分全量修复与按 RS 定向修复的覆盖面。
- Master `/status` 已新增并细化迁移编排指标快照：在总量指标（`attemptCount/successCount/failureCount`）之外，提供 `rebalance*` 与 `recovery*` 分项计数，以及 `lastRebalanceError/lastRecoveryError`，便于区分迁移主流程失败与卡死恢复失败。
- RegionServer 上下线事件已接入调度器外部触发（`rs_up` / `rs_down`），在节流保护下可即时请求一次 rebalance。
- RebalanceScheduler 定时 tick 已接入 route repair 预扫描：即使没有读流量和 membership 事件，也会周期性后台修复离线路由再进入 rebalance。
- RebalanceScheduler 已覆盖外部触发节流测试：连续 membership 事件会受 `minGapMs` 保护，避免抖动时触发风暴。
- RegionServer membership 事件现会触发后台 `repairTableRoutesWithConfirmation` 扫描，主动修复离线主副本路由并减少对读路径触发修复的依赖。
- RegionServer membership 事件触发的 route repair 已支持携带 `rsId` 定向扫描受影响表，降低大表量场景下的全表修复开销。
- membership 事件链路在 route repair 触发异常时会吞并异常并保留 rebalance 外部触发，避免修复异常阻断调度。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。
- `getTableLocation` 在检测到主副本离线且存在在线副本时，会自动晋升在线副本并回写元数据（lazy failover）。
- `listTables` 同样会在返回前执行主副本离线检测与 lazy failover，减少批量查询场景下的陈旧主路由。
- lazy failover 补副本已升级为“先数据迁移后元数据落盘”：Master 会先触发 RegionAdmin `transferTable` 把表数据复制到新副本，成功后才回写 `/meta/tables` 与 `/assignments`。
- lazy failover 的补副本迁移失败时会保持降副本状态，并对目标残留执行确认重试清理（`OK/TABLE_NOT_FOUND` 视为完成），避免出现“元数据已宣告副本存在但数据未到位”的假副本。
- 当表的所有副本都离线时，路由自愈会将表状态标记为 `UNAVAILABLE`；有副本恢复在线后会自动回升到 `ACTIVE`。
- 路由自愈写回已增加按表去抖节流（相同目标拓扑在最小间隔内不重复写 ZK），降低高频查询下写放大。

当前限制：
- 当前 rebalance 调度器仍是基础版；虽已形成 `RegionMigrator` 统一迁移/恢复编排并补齐阶段状态与超时回收，但跨节点故障自治恢复闭环仍待完善。
- rebalance 与卡死恢复已形成“补偿确认后完成”协议：必要副本清理采用确认重试，失败时显式进入 `COMPENSATING` 并保留上下文，避免假恢复。
- 当前选主与脑裂防护已跑通基础路径，尚未补全网络分区/抖动场景下的混沌验证。

## 2. RegionServer 侧已实现内容

实现文件：
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/RegionServerRegistrar.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/RegionServerMain.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/WalManager.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/WriteGuard.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/ReplicaManager.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/rpc/RegionServiceImpl.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/rpc/RegionAdminServiceImpl.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/rpc/ReplicaSyncServiceImpl.java

已落地能力：
- 启动时在 `/region_servers/{rsId}` 注册节点，每 10 秒上报心跳。
- `MiniSqlProcess` 已支持崩溃后自动重启和显式 restart。
- RegionServer HTTP 端点已支持 `/status` JSON 运行态输出（含 rsId/端口/zk/dataDir/walDir/miniSqlAlive/timestamp）。
- RegionServer `/status` 已补充迁移 manifest 校验统计（total/success/failure/duplicateAcks/lastSuccessTs/lastFailureTs/lastFailureMessage），便于定位迁移校验异常。
- RegionServer `/status` 的 manifest 校验统计已补充失败原因维度（`failureReasons` + `lastFailureReason`），可区分格式错误、范围违规、缺文件、size/checksum 不一致等失败类型。
- RegionServer `/status` 的 manifest 校验统计已补充 `recentFailures`（最多 8 条）与 `recentFailuresDropped`，用于观测近期失败序列并反映窗口裁剪量。
- RegionServer `/status` 的 manifest 校验统计已补充 `duplicateAcksByTable`，可按表观察重复清单确认分布。
- `duplicateAcksByTable` 已增加有界上限与 `duplicateAcksByTableDropped` 计数，避免高基数表名导致统计结构无限增长。
- transferTable 失败观测已补充 `lastFailureTable`，并在 `recentFailures` 事件中记录 `table` 字段，便于按表快速定位失败样本。
- manifest 校验失败观测同样补充 `lastFailureTable`，并在 `recentFailures` 事件中记录 `table` 字段，保持 transfer/manifest 双栈语义一致。
- RegionServer `/status` 已补充 `transferTable` 统计（total/success/failure/lastSuccessTs、失败原因分类与最近失败信息），便于快速定位迁移失败类型。
- `transferTable` 失败原因分类已新增 `source_io_error`（源端文件读取/流式发送阶段 I/O 异常），便于区分网络故障与本地数据面故障。
- `transferTable` 统计已补充 `recentFailures` 窗口（最多 8 条，含 ts/reason/code/message），便于查看最近失败序列而不放大 `/status` 载荷。
- `transferTable` 统计新增 `recentFailuresDropped` 计数，用于反映有界窗口裁剪的历史失败条目数量。
- RegionServer `/status` 的 `transferTable.lastFailureMessage` 已做单行归一化与长度上限控制（256），避免异常长错误撑大状态接口。
- 持久化 WAL 基础能力：按表写入 `.wal` 文件，启动时扫描 WAL 恢复全局 LSN。
- WriteGuard：per-table 写入暂停/恢复（用于迁移中的 MOVING 保护）。
- ReplicaManager：主侧通过 Thrift 并发 sync 到副本，支持半同步等待与异步 commit。
- ReplicaManager 半同步等待已按 requiredAcks 收敛：会在超时窗口内等待达到所需 ACK 数（而非固定等待首个返回）。
- RegionServiceImpl 已实现基础写路径：WriteGuard 检查、WAL append、副本 sync、本地 MiniSQL 执行、异步 commit、checkpoint 计数。
- RegionServiceImpl 写路径已支持最小副本 ACK 门槛（`RS_MIN_REPLICA_ACKS`，默认 1）：ACK 不足时拒绝本地执行并返回错误。
- RegionServiceImpl 写路径在“可见副本数低于 `RS_MIN_REPLICA_ACKS`”时会直接拒绝写入并将 WAL 条目标记为 ABORT，不再按可见副本数自动降低 ACK 门槛。
- RegionServer 运行时配置会将环境变量 `RS_MIN_REPLICA_ACKS` 下限钳制为 1，避免线上通过配置把写入 ACK 门槛降为 0。
- ACK 不足拒绝写入时，WAL 条目会标记为 ABORT，避免 PREPARE 条目长期滞留。
- WAL 状态语义已收敛为明确常量（PREPARE/COMMITTED/ABORTED），恢复路径仅回放 COMMITTED，且已补充边界测试覆盖。
- WAL checkpoint 已增加按文件压缩：会清理 ABORT 与已持久化的旧 COMMITTED 记录，保留 PREPARE/未持久化 COMMITTED，减少日志膨胀与恢复噪音。
- WAL checkpoint/recover 清理语义已增强：会剔除 `LSN<=checkpointLsn` 的陈旧 PREPARE 条目，避免异常中断后悬挂 PREPARE 长期滞留。
- RegionServiceImpl 已实现基础读路径：直接 MiniSQL 执行，跳过 WAL 与 replica sync。
- executeBatch：按序执行，遇错即停。
- RegionAdminServiceImpl 已实现 pause/resume、deleteLocalTable、invalidateClientCache、transferTable、copyTableData 的基础路径。
- RegionAdminServiceImpl 的 `registerRegionServer/heartbeat` 已支持回写 `/region_servers/{rsId}` 节点负载与 `lastHeartbeat`。
- RegionAdminServiceImpl 的 `invalidateClientCache` 已支持通过更新 `/meta/tables/{table}` version 广播失效信号，驱动 Client 端主动失效。
- RegionAdminServiceImpl 的 `deleteLocalTable` 已增强 assignment 安全更新：只移除当前 RS 的副本条目，不再误删整条 `/assignments/{table}`；仅在副本列表为空时删除节点。
- `transferTable` 已增加逐块响应校验：`copyTableData` 若返回非 OK 会立即中断并返回 ERROR，避免静默迁移失败。
- `copyTableData` 已增加 per-file 连续 offset 约束：拒绝乱序/跳跃 chunk，降低迁移文件损坏风险。
- `copyTableData` 已改为先写入临时分块文件，只有在最后一块（`isLast=true`）时才原子发布正式文件，避免半传输数据被误读。
- `transferTable` 已补充迁移完成 manifest 确认：源端在分块传输结束后发送文件清单与大小，目标端逐项校验后才确认完成。
- manifest 校验已拒绝 `.part` 临时文件与 manifest 控制文件自身作为数据条目，避免异常输入误确认非业务文件。
- manifest 条目已增加 `crc32` 校验和，目标端按“文件大小 + CRC32”双重校验确认迁移完整性。
- manifest 校验已拒绝跨表文件项与重复文件项，避免错误文件混入或重复条目掩盖异常传输。
- manifest 校验会拒绝空文件列表，避免异常空清单被误判为迁移成功。
- manifest 校验已支持重复清单幂等确认：同表同内容重放会直接 ACK 并记录 `duplicateAcks`，避免链路重试造成误报失败。
- `transferTable` 对 0 字节文件已支持发送最终完成块，保证空文件迁移也能触发目标端发布。
- `copyTableData` 已增加 fileName 安全约束，禁止路径穿越写入 dataDir 外部文件。
- `transferTable` 发送分块时已支持有界重试（默认 3 次），可吸收短暂目标端抖动导致的单次 chunk 失败。
- `copyTableData` 在 offset 校验失败时会自动清理该文件的临时分块状态（`.part` + 内存偏移），允许后续从 0 干净重传。
- `copyTableData` 的 offset 跟踪已按 `tableName + fileName` 作用域隔离，避免跨表同名文件传输时的状态串扰。
- `copyTableData` 在内存 offset 状态缺失时可从磁盘 `.part` 文件长度恢复期望偏移，支持目标端重启后的迁移续传。
- `copyTableData` 已支持重复 chunk 幂等确认：当目标端已写入同偏移同内容时，会返回成功而非触发 offset reset，提升链路抖动下重试成功率。
- `copyTableData` 对“同偏移但内容不一致”的重复包会显式拒绝且不重置当前传输进度，避免因异常重试包覆盖/清空已完成分块。
- `transferTable` 已过滤源端 `.part` 临时文件，只迁移已完成文件，避免把未完成分块产物继续扩散。
- ReplicaSyncServiceImpl 已支持内存/本地结合的基础同步路径、pullLog 与 commitLog 回放。
- ReplicaSyncServiceImpl 的 `commitLog` 已具备幂等语义：重复 COMMIT 不再重复回放 SQL。
- ReplicaSyncServiceImpl 的 `pullLog/getMaxLsn` 已收敛为仅暴露 COMMITTED 日志（并在启动时恢复 committed 索引），避免将未提交 PREPARE 暴露给追赶链路。
- ReplicaSyncServiceImpl 已新增超时 PREPARE 自动决议（超时后本地标记 ABORT 并从内存待提交集合移除），降低跨节点提交确认丢失时 PREPARE 长期滞留风险。
- ReplicaSyncService/ReplicaManager 已落地显式最终决议 RPC 语义：新增 `finalizeLogDecision` 与 `getLogDecisionState`，主副本可显式下发 COMMIT/ABORT 终局并查询副本决议状态。
- ReplicaSyncServiceImpl 已支持“legacy commit + 显式决议”双轨兼容：`commitLog` 保持历史幂等语义（COMMITTED/ALREADY_COMMITTED），显式 ABORT 冲突会拒绝后续提交，超时自动 ABORT 保持 TABLE_NOT_FOUND 兼容行为。
- ReplicaSyncServiceImpl 启动恢复已支持从 WAL 状态重建决议状态（COMMITTED/ABORTED），保证重启后决议查询与重复决议幂等行为一致。
- 主副本对副本 `commitLog` 通知已增加有界重试（best-effort），降低短暂网络抖动下的提交通知丢失概率。
- 当 `commitLog` 因 `TABLE_NOT_FOUND` 失败进入待重试队列时，ReplicaManager 会基于 donor 的 `pullLog` 定向回填缺失日志后再重试 commit，提升无新写入场景下的自愈收敛能力。
- ReplicaManager 待重试队列已引入退避节流窗口，避免对故障副本持续高频无效重试；并新增 `throttledSkipCount/stalledCount/oldestPendingAgeMs` 观测字段用于识别重试停滞。
- ReplicaManager 对连续 `transport_error` 已增加分级降频策略：达到阈值后进入升级重试窗口（更长冷却），并输出 `escalatedCount/activeEscalatedCount/maxConsecutiveTransportFailures` 观测字段。
- 当升级态 pending commit 最终成功收敛时，ReplicaManager 会记录升级恢复信号（`recoveredFromEscalationCount/lastRecoveredFromEscalationAtMs`），用于确认降频后是否真实恢复。
- 对于长时间处于升级态且重试次数持续累积的 pending commit，ReplicaManager 会标记为决议候选并输出 `decisionCandidateCount/activeDecisionCandidateCount/lastDecisionCandidateAtMs`，作为自动最终决议前置阶段。
- 决议候选项已提供预览视图 `decisionCandidatesPreview`（table/lsn/address/attempts/consecutiveTransportFailures/ageMs，最多5条），便于快速定位自动最终决议收敛中的异常对象。
- 决议候选项引入二级冷却窗口（`decisionCandidateCooldownMs`，默认 300s），触发时记录 `decisionCandidateCooldownAppliedCount`，并在预览项中输出 `nextRetryAtMs`，用于降低长故障期重试噪音。
- 在决议候选基础上新增“decision-ready”阶段：持续失败达到阈值后记录 `decisionReadyTransitionCount/lastDecisionReadyAtMs`，并输出 `activeDecisionReadyCount/decisionReadyAttemptsThreshold`，用于触发最终决议流程。
- 进入 `decision-ready` 后，待提交重试会切换到更长冷却窗口（15 分钟）并上报 `decisionReadyCooldownAppliedCount/decisionReadyCooldownMs`，降低长故障期间的无效重试噪音。
- 对进入 `decision-ready` 的待提交项，系统会持续自动重试并周期评估多数派提交条件，不再分流到人工确认队列。
- `replicaCommitRetry` 统计已收敛为自动决议观测字段（如 `activeDecisionReadyCount`、`decisionReadyOldestAgeMs`、`finalDecisionCommittedCount`），用于判断自动收敛进度。
- ReplicaManager 已补充 suspected 副本观测：当同步阶段 ACK 不足或 commit 重试阶段出现连续 `transport_error` 时，会记录 `suspectedReplicaCount/suspectedReplicaMarkCount/suspectedReplicaPreview` 等字段，用于识别可疑副本并辅助决议收敛定位。
- suspected 副本在后续提交通知恢复成功时会自动从 suspected 集合移除，并上报 `suspectedReplicaRecoveredCount/suspectedReplicaLastRecoveredAtMs`，便于区分短暂抖动与持续异常。
- RegionServer 心跳不再携带人工确认终态队列字段；Master `/status` 的 `replicaDecision` 聚合段不再暴露人工终态队列信号。
- ReplicaManager 已新增基于 `getMaxLsn + pullLog` 的落后副本追赶编排：会选择最新副本作为 donor，向落后副本重放缺失日志并补发 commit（best-effort）。
- ReplicaManager 追赶编排已支持 donor 回退：首选 donor 无法提供 backlog 时会自动尝试下一候选 donor，提升追赶收敛稳定性。
- ReplicaManager 追赶编排已增加连续 LSN 回放约束：对 donor 返回的非连续 backlog 会跳过并回退到下一 donor，避免跨缺口回放导致的日志洞。
- RegionServiceImpl 写成功后会异步触发副本追赶编排（`reconcileReplicasAsync`），使 `pullLog/getMaxLsn` 从“能力预留”进入主写链路后的收敛路径。
- RegionServer `/status` 已补充 `prepareDecision` 与 `replicaCommitRetry` 统计，支持观测 PREPARE 超时决议与副本提交通知重试收敛情况；其中 `replicaCommitRetry` 进一步包含 repair 计数与错误分类分布（如 `table_not_found`/`transport_error`）。

当前限制：
- WAL、ReplicaManager 与 ReplicaSyncService 已落地显式最终决议 RPC 与自动终局主链路；后续仍需继续加强极端网络分区下的跨节点一致性混沌验证与运维处置自动化。
- Region 迁移、主副本晋升、恢复 3 副本等自治能力还未打通完整闭环。
- transfer/copyTableData 已有基础实现，但尚未形成完整迁移协议。

## 3. Client 侧已实现内容

实现文件：
- java-client/src/main/java/edu/zju/supersql/client/SqlClient.java
- java-client/src/main/java/edu/zju/supersql/client/RouteCache.java
- java-client/src/main/java/edu/zju/supersql/client/MasterRpcClient.java
- java-client/src/main/java/edu/zju/supersql/client/RegionRpcClient.java

已落地能力：
- MasterRpcClient：已封装 getTableLocation / createTable / dropTable / listTables / listRegionServers / getActiveMaster。
- RegionRpcClient：已封装 execute / executeBatch / createIndex / dropIndex / ping。
- SqlClient REPL 已具备基础真实路由：
  - DDL -> MasterRpcClient（createTable / dropTable）
  - DML -> RouteCache -> RegionRpcClient.execute
  - SHOW TABLES -> MasterRpcClient.listTables
- 已支持 Master `NOT_LEADER` 自动重定向。
- 已支持 DML `REDIRECT` 自动路由自愈：失效本地 route cache 后回源查询并重试。
- 已支持 DML `MOVING` 有界重试（可配置次数与退避间隔）：
  - `CLIENT_MOVING_RETRY_MAX_ATTEMPTS`
  - `CLIENT_MOVING_RETRY_INITIAL_BACKOFF_MS`
  - `CLIENT_MOVING_RETRY_BACKOFF_STEP_MS`
- 主副本短暂不可达时，DML 可自动失效缓存并重试，提升瞬时抖动下可用性。
- SELECT 在主副本不可达时支持按副本列表降级读取，降低读请求对单主副本的依赖。
- 已支持读一致性分级（`CLIENT_READ_CONSISTENCY`）：
  - `EVENTUAL`：允许 SELECT 在主副本失败时降级到副本读取。
  - `STRONG`：SELECT 仅访问主副本，不进行副本降级读取。
- 已支持按表路由指标统计（内存态）：重定向次数、MOVING 重试次数、异常重试次数、路由回源次数、读降级命中次数。
- REPL 已支持 `SHOW ROUTING METRICS` 命令，可直接查看当前进程内按表路由指标快照。
- REPL 已支持 `SHOW ROUTING METRICS JSON`，可输出结构化 JSON 结果供日志采集或外部脚本消费。
- `SHOW ROUTING METRICS JSON` 与 `/metrics/json` 输出已补充运行态元数据（`generatedAtEpochMs`/`processStartEpochMs`/`processUptimeSeconds`），便于监控侧做时间对齐与重启识别。
- REPL 已支持 `SHOW ROUTING METRICS PROMETHEUS`，可输出 Prometheus 文本格式指标，便于接入监控抓取。
- 路由指标在文本/JSON/Prometheus 输出中已补充“全表汇总总量”计数，便于统一监控直接抓取全局趋势而无需外部二次聚合。
- Prometheus 输出已补充客户端进程级指标（启动时间、运行时长），便于外部监控识别重启与计算趋势窗口。
- REPL 已支持 `SHOW ROUTING METRICS EXPORT <path>`，可将当前路由指标快照导出到 JSON 文件。
- Client 已支持可选 HTTP `/metrics` 暴露（`CLIENT_METRICS_HTTP_ENABLED=true`），可持续输出 Prometheus 文本指标供外部抓取。
- Client metrics HTTP 服务已支持 `/metrics/json` 结构化快照端点，便于日志平台与采集器直接消费 JSON 指标。
- Client metrics HTTP 服务已支持 `/healthz` 探活端点，便于容器探针与进程存活检测。
- REPL 已支持 `execfile <path>`：会解析 SQL 脚本并按顺序执行；同表连续 DML 会走 Region `executeBatch` + MOVING/REDIRECT 重试闭环。
- 路由缓存支持 TTL 与版本失效。
- 已支持基于 ZooKeeper `/meta/tables` 事件的主动 route cache 失效（create/change/delete）。
- 读取 `/active-master` 时支持 address 优先、masterId 回退、坏数据 fallback。

当前限制：
- MOVING 已支持有界重试；当 `CLIENT_MOVING_RETRY_MAX_ATTEMPTS<=0` 时启用透明持续重试模式。
- 可观测性已支持命令行/JSON/文件导出，但尚未接入统一监控系统与长期趋势聚合。

## 4. 已落地测试

Master：
- LeaderElectorTest
- MasterHeartbeatIntegrationTest
- MasterConfigTest
- MasterServerHttpPayloadTest
- MasterServiceImplTest
- MasterServiceMetadataIntegrationTest
- MasterRuntimeContextIntegrationTest
- RegionServerWatcherIntegrationTest
- MetaManagerIntegrationTest
- LoadBalancerTest
- MasterRegionDdlForwardingIntegrationTest
- RebalanceLoggingConsistencyTest

RegionServer：
- MiniSqlProcessTest
- WalManagerTest
- RegionServerConfigTest
- WriteGuardTest
- ReplicaManagerTest
- RegionServiceImplTest
- RegionAdminServiceImplTest
- RegionAdminServiceAssignmentIntegrationTest
- ReplicaSyncServiceImplTest
- RegionServerRegistrarIntegrationTest
- OutputParserTest

Client：
- ClientConfigTest
- ClientMetricsHttpServerTest
- MasterRpcClientTest
- RegionRpcClientTest
- SqlClientRedirectTest
- CreateInsertSelectFunctionalTest
- SqlClientRoutingTest
- RouteCacheAndDiscoveryTest
- SqlClientDmlRetryTest
- SqlClientExecFileTest
- SqlClientRoutingMetricsCommandTest
- SqlClientPlannedFeaturesTddTest（已改为可执行测试，不再 `@Disabled`）
- MasterPlannedFeaturesTddTest（已改为可执行测试）
- RegionPlannedFeaturesTddTest（已改为可执行测试）

执行方式：
```bash
# 从仓库根目录运行全部测试
mvn test -DskipTests=false
```

说明：
- 2026-04-18 已完成文档与实现状态逐项对照校验，并修正文档中历史遗留的过时结论。
- 集成测试使用 Curator TestingServer 启动内嵌 ZooKeeper，验证真实节点读写语义。
- 2026-04-10 已补充覆盖：active-master bootstrap/回退、三副本元数据分配与 assignments 持久化、OutputParser 成功/错误/结果集解析、RegionService 执行与 checkpoint 触发、Client discovery 回退路径、Master->RS DDL 转发、CREATE->INSERT->SELECT 功能流、rebalance 日志与元数据一致性、MiniSqlProcess 自动重启。
- 2026-04-18 已补充覆盖：rebalance 在 source 清理失败时的元数据回滚一致性、以及缓存失效失败场景下的 best-effort 语义验证。
- 2026-04-18 已补充覆盖：rebalance 在 transfer 失败和 source 清理失败场景下的 target 残留数据清理补偿路径。
- 2026-04-18 已补充覆盖：RegionAdmin `deleteLocalTable` 对 assignment 的安全更新语义（移除当前 RS / 空列表删节点 / 非成员保持不变）。
- 2026-04-19 已将 Master/RegionServer 规划 TDD 规格测试切换为可执行测试，覆盖 rebalance 与迁移基础路径的回归检查。
- 2026-04-19 已补充覆盖：rebalance `FINALIZING` 阶段可观测性，以及超时 `MOVING` 状态在读路径触发下自动恢复为 `ACTIVE` 的自恢复语义。
- 2026-04-19 已补充覆盖：checkpoint 与 recover 对陈旧 PREPARE 的自动裁剪（`LSN<=checkpointLsn`）语义，避免悬挂 PREPARE 污染后续恢复与统计。
- 2026-04-19 已补充覆盖：`copyTableData` 在“进行中重复 chunk”与“完成后重复末块”场景下的幂等确认语义，避免误触发 offset reset。
- 2026-04-19 已补充覆盖：显式最终决议 RPC（`finalizeLogDecision/getLogDecisionState`）与 legacy `commitLog` 兼容语义（显式 ABORT 冲突拒绝、超时自动 ABORT 返回 `TABLE_NOT_FOUND`）的回归验证。
- 2026-04-19 已补充覆盖：`copyTableData` 在“重复 offset 但内容冲突”场景下会返回错误且保持传输进度，后续正确 chunk 可继续完成迁移。
- 2026-04-19 已补充覆盖：`triggerRebalance` 在“集群已平衡”返回前会先执行卡死迁移预恢复，验证调度路径不再依赖读路径才能回收超时状态。
- 2026-04-19 已补充覆盖：`RegionMigrator` 迁移指标快照（总量 + `rebalance/recovery` 分项 + 分项最近错误）与 Master `/status` 中 `migration` 字段契约。
- 已补充基础版网络分区混沌脚本：`scripts/chaos_test.sh network_partition` 会对 `rs-1` / `rs-2` 注入双向网络阻断，归档分区期间 SQL 返回与 `/status` 观测结果，用于推进 S7-05 的脚本、注入方式与结果归档闭环。
- 2026-04-10 在仓库根目录执行 `mvn test -DskipTests=false`，当前结果为 `BUILD SUCCESS`。
- 2026-04-10 `docker compose build` 已验证 master 与 regionserver 关键阶段可正常推进；client 镜像构建稳定性已通过切换官方源并增加 apt 重试得到改善，但完整 build 仍受外部 apt 仓库可用性影响。

## 5. Docker 集群启动

```bash
docker compose up -d --build
docker compose ps

# 连接交互式 REPL
docker exec -it client java -jar /app/client.jar
```
