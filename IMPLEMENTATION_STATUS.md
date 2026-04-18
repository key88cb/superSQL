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
- `triggerRebalance()` 过程中已显式写入 `PREPARING -> MOVING -> ACTIVE` 状态迁移，并在失败时回滚元数据状态。
- `triggerRebalance()` 在 `transferTable` 失败路径也会立即回滚到原始元数据（含 `ACTIVE` 状态），避免卡在 `MOVING`。
- `triggerRebalance()` 在 `pauseTableWrite` 失败路径也会回滚元数据，避免停留在 `PREPARING` 中间态。
- `triggerRebalance()` 状态推进会刷新 `/meta/tables/{table}` 的 `statusUpdatedAt` 时间戳，便于观测迁移阶段更新时间。
- `triggerRebalance()` 候选选择已限制为 `ACTIVE` 表，避免对 `PREPARING/MOVING` 表重复触发迁移。
- 已落地基础 RebalanceScheduler：按配置周期定时触发 `triggerRebalance()`，支持开关与最小触发间隔节流。
- Master `/status` 已输出 RebalanceScheduler 运行快照（tick/trigger/throttle/success/failure、最近执行时间、最近触发原因）。
- RegionServer 上下线事件已接入调度器外部触发（`rs_up` / `rs_down`），在节流保护下可即时请求一次 rebalance。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。

当前限制：
- 当前 rebalance 调度器仍是基础版，尚未形成完整 RegionMigrator 状态机 + 故障恢复闭环。
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
- 持久化 WAL 基础能力：按表写入 `.wal` 文件，启动时扫描 WAL 恢复全局 LSN。
- WriteGuard：per-table 写入暂停/恢复（用于迁移中的 MOVING 保护）。
- ReplicaManager：主侧通过 Thrift 并发 sync 到副本，支持半同步等待与异步 commit。
- ReplicaManager 半同步等待已按 requiredAcks 收敛：会在超时窗口内等待达到所需 ACK 数（而非固定等待首个返回）。
- RegionServiceImpl 已实现基础写路径：WriteGuard 检查、WAL append、副本 sync、本地 MiniSQL 执行、异步 commit、checkpoint 计数。
- RegionServiceImpl 写路径已支持最小副本 ACK 门槛（`RS_MIN_REPLICA_ACKS`，默认 1）：ACK 不足时拒绝本地执行并返回错误。
- ACK 不足拒绝写入时，WAL 条目会标记为 ABORT，避免 PREPARE 条目长期滞留。
- WAL 状态语义已收敛为明确常量（PREPARE/COMMITTED/ABORTED），恢复路径仅回放 COMMITTED，且已补充边界测试覆盖。
- RegionServiceImpl 已实现基础读路径：直接 MiniSQL 执行，跳过 WAL 与 replica sync。
- executeBatch：按序执行，遇错即停。
- RegionAdminServiceImpl 已实现 pause/resume、deleteLocalTable、invalidateClientCache、transferTable、copyTableData 的基础路径。
- RegionAdminServiceImpl 的 `invalidateClientCache` 已支持通过更新 `/meta/tables/{table}` version 广播失效信号，驱动 Client 端主动失效。
- RegionAdminServiceImpl 的 `deleteLocalTable` 已增强 assignment 安全更新：只移除当前 RS 的副本条目，不再误删整条 `/assignments/{table}`；仅在副本列表为空时删除节点。
- `transferTable` 已增加逐块响应校验：`copyTableData` 若返回非 OK 会立即中断并返回 ERROR，避免静默迁移失败。
- `copyTableData` 已增加 per-file 连续 offset 约束：拒绝乱序/跳跃 chunk，降低迁移文件损坏风险。
- `copyTableData` 已改为先写入临时分块文件，只有在最后一块（`isLast=true`）时才原子发布正式文件，避免半传输数据被误读。
- `transferTable` 对 0 字节文件已支持发送最终完成块，保证空文件迁移也能触发目标端发布。
- `copyTableData` 已增加 fileName 安全约束，禁止路径穿越写入 dataDir 外部文件。
- `transferTable` 发送分块时已支持有界重试（默认 3 次），可吸收短暂目标端抖动导致的单次 chunk 失败。
- `copyTableData` 在 offset 校验失败时会自动清理该文件的临时分块状态（`.part` + 内存偏移），允许后续从 0 干净重传。
- `transferTable` 已过滤源端 `.part` 临时文件，只迁移已完成文件，避免把未完成分块产物继续扩散。
- ReplicaSyncServiceImpl 已支持内存/本地结合的基础同步路径、pullLog 与 commitLog 回放。
- ReplicaSyncServiceImpl 的 `commitLog` 已具备幂等语义：重复 COMMIT 不再重复回放 SQL。
- 主副本对副本 `commitLog` 通知已增加有界重试（best-effort），降低短暂网络抖动下的提交通知丢失概率。

当前限制：
- WAL、ReplicaManager 与 ReplicaSyncService 还没有完全达到计划中的 Crash Recovery 与多数派复制最终形态。
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
- 路由缓存支持 TTL 与版本失效。
- 已支持基于 ZooKeeper `/meta/tables` 事件的主动 route cache 失效（create/change/delete）。
- 读取 `/active-master` 时支持 address 优先、masterId 回退、坏数据 fallback。

当前限制：
- MOVING 已支持有界重试，但在重试窗口内若迁移仍未完成，客户端仍会返回 MOVING 给上层；尚未实现无限透明等待策略。

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
- MasterRpcClientTest
- RegionRpcClientTest
- SqlClientRedirectTest
- CreateInsertSelectFunctionalTest
- SqlClientRoutingTest
- RouteCacheAndDiscoveryTest
- SqlClientDmlRetryTest
- SqlClientRoutingMetricsCommandTest
- SqlClientPlannedFeaturesTddTest（已改为可执行测试，不再 `@Disabled`）

新增 TDD 规格测试（默认 `@Disabled`）：
- java-master/src/test/java/edu/zju/supersql/master/rpc/MasterPlannedFeaturesTddTest.java
- java-regionserver/src/test/java/edu/zju/supersql/regionserver/rpc/RegionPlannedFeaturesTddTest.java

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
- 2026-04-10 已新增一批 `@Disabled` 的 TDD 规格测试，用于提前钉住未实现功能的期望行为，包括 rebalance、executeBatch、索引分布式传播、Client redirect/MOVING 重试等。
- 2026-04-10 在仓库根目录执行 `mvn test -DskipTests=false`，当前结果为 `BUILD SUCCESS`。
- 2026-04-10 `docker compose build` 已验证 master 与 regionserver 关键阶段可正常推进；client 镜像构建稳定性已通过切换官方源并增加 apt 重试得到改善，但完整 build 仍受外部 apt 仓库可用性影响。

## 5. Docker 集群启动

```bash
docker compose up -d --build
docker compose ps

# 连接交互式 REPL
docker exec -it client java -jar /app/client.jar
```
