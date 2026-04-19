# SuperSQL 当前实现状态（2026-04-18）

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
- `getTableLocation/listTables/repairTableRoutesBestEffort` 已接入卡死迁移自恢复：当 `PREPARING/MOVING/FINALIZING/ROLLBACK` 状态超时且存在 `migrationAttemptId` 时，自动回收为 `ACTIVE` 并清理尝试标记。
- `triggerRebalance()` 调度入口已接入卡死迁移预恢复：即使本轮负载均衡最终被跳过，也会先回收超时迁移状态，降低“长期无人读写表”卡死风险。
- `triggerRebalance()` 在被跳过（如 cluster balanced）时会在响应消息中附带本轮预恢复数量，便于上层调度/日志快速感知是否发生了状态回收。
- `triggerRebalance()` 状态推进会刷新 `/meta/tables/{table}` 的 `statusUpdatedAt` 时间戳，便于观测迁移阶段更新时间。
- `triggerRebalance()` 迁移中会写入 `migrationAttemptId`（PREPARING/MOVING 可观测），成功与回滚后会清理该字段，便于后续幂等恢复扩展。
- `triggerRebalance()` 候选选择已限制为 `ACTIVE` 表，避免对 `PREPARING/MOVING` 表重复触发迁移。
- `createTable` 成功落盘元数据后也会初始化 `statusUpdatedAt`，保证新表从创建时起具备状态时间戳。
- 已落地基础 RebalanceScheduler：按配置周期定时触发 `triggerRebalance()`，支持开关与最小触发间隔节流。
- Master `/status` 已输出 RebalanceScheduler 运行快照（tick/trigger/throttle/success/failure、最近执行时间、最近触发原因）。
- Master `/status` 已输出 route repair 运行指标（修复运行次数、累计修复表数、最近修复时间、最近修复表、最近错误），并新增近 N 次运行窗口统计（成功率、平均修复数）。
- Master `/status` 的 route repair 指标已补充最近一次扫描范围观测（`lastRunTotalTables`/`lastRunCandidateTables`/`lastRunFilterRegionServerId`），便于区分全量修复与按 RS 定向修复的覆盖面。
- RegionServer 上下线事件已接入调度器外部触发（`rs_up` / `rs_down`），在节流保护下可即时请求一次 rebalance。
- RebalanceScheduler 定时 tick 已接入 route repair 预扫描：即使没有读流量和 membership 事件，也会周期性后台修复离线路由再进入 rebalance。
- RebalanceScheduler 已覆盖外部触发节流测试：连续 membership 事件会受 `minGapMs` 保护，避免抖动时触发风暴。
- RegionServer membership 事件现会触发后台 `repairTableRoutesBestEffort` 扫描，主动修复离线主副本路由并减少对读路径触发修复的依赖。
- RegionServer membership 事件触发的 route repair 已支持携带 `rsId` 定向扫描受影响表，降低大表量场景下的全表修复开销。
- membership 事件链路在 route repair 触发异常时会吞并异常并保留 rebalance 外部触发，避免修复异常阻断调度。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。
- `getTableLocation` 在检测到主副本离线且存在在线副本时，会自动晋升在线副本并回写元数据（lazy failover）。
- `listTables` 同样会在返回前执行主副本离线检测与 lazy failover，减少批量查询场景下的陈旧主路由。
- lazy failover 补副本已升级为“先数据迁移后元数据落盘”：Master 会先触发 RegionAdmin `transferTable` 把表数据复制到新副本，成功后才回写 `/meta/tables` 与 `/assignments`。
- lazy failover 的补副本迁移失败时会保持降副本状态并清理目标残留（best-effort），避免出现“元数据已宣告副本存在但数据未到位”的假副本。
- 当表的所有副本都离线时，路由自愈会将表状态标记为 `UNAVAILABLE`；有副本恢复在线后会自动回升到 `ACTIVE`。
- 路由自愈写回已增加按表去抖节流（相同目标拓扑在最小间隔内不重复写 ZK），降低高频查询下写放大。

当前限制：
- 当前 rebalance 调度器仍是基础版，虽已补齐阶段状态与超时回收，但尚未形成完整 RegionMigrator 编排与跨节点故障恢复闭环。
- rebalance 对数据面迁移回滚当前仍以元数据回滚为主；target 清理虽已支持 best-effort，但尚未形成强一致、可确认完成的补偿协议。
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
- 主副本对副本 `commitLog` 通知已增加有界重试（best-effort），降低短暂网络抖动下的提交通知丢失概率。
- ReplicaManager 已新增基于 `getMaxLsn + pullLog` 的落后副本追赶编排：会选择最新副本作为 donor，向落后副本重放缺失日志并补发 commit（best-effort）。
- ReplicaManager 追赶编排已支持 donor 回退：首选 donor 无法提供 backlog 时会自动尝试下一候选 donor，提升追赶收敛稳定性。
- ReplicaManager 追赶编排已增加连续 LSN 回放约束：对 donor 返回的非连续 backlog 会跳过并回退到下一 donor，避免跨缺口回放导致的日志洞。
- RegionServiceImpl 写成功后会异步触发副本追赶编排（`reconcileReplicasAsync`），使 `pullLog/getMaxLsn` 从“能力预留”进入主写链路后的收敛路径。

当前限制：
- WAL、ReplicaManager 与 ReplicaSyncService 仍未达到最终语义（例如 PREPARE 的跨节点恢复闭环与多数派故障收敛策略）。
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
- 2026-04-19 已补充覆盖：`copyTableData` 在“重复 offset 但内容冲突”场景下会返回错误且保持传输进度，后续正确 chunk 可继续完成迁移。
- 2026-04-19 已补充覆盖：`triggerRebalance` 在“集群已平衡”返回前会先执行卡死迁移预恢复，验证调度路径不再依赖读路径才能回收超时状态。
- 2026-04-10 在仓库根目录执行 `mvn test -DskipTests=false`，当前结果为 `BUILD SUCCESS`。
- 2026-04-10 `docker compose build` 已验证 master 与 regionserver 关键阶段可正常推进；client 镜像构建稳定性已通过切换官方源并增加 apt 重试得到改善，但完整 build 仍受外部 apt 仓库可用性影响。

## 5. Docker 集群启动

```bash
docker compose up -d --build
docker compose ps

# 连接交互式 REPL
docker exec -it client java -jar /app/client.jar
```
