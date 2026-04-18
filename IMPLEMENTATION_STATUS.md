# SuperSQL 当前实现状态（2026-04-18）

本文档记录已落地并通过测试的 Java 分布式层功能。

## 1. Master 侧已实现内容

实现文件：
- java-master/src/main/java/edu/zju/supersql/master/MasterRuntimeContext.java
- java-master/src/main/java/edu/zju/supersql/master/MasterServer.java
- java-master/src/main/java/edu/zju/supersql/master/rpc/MasterServiceImpl.java
- java-master/src/main/java/edu/zju/supersql/master/election/LeaderElector.java

已落地能力：
- 启动时初始化 Master 运行上下文（含本机 masterId、address、zkClient）。
- LeaderLatch 选主：当选时更新 `/active-master`，epoch CAS 防脑裂。
- Active Master 心跳：每 5 秒更新 `/masters/active-heartbeat`。
- `/masters` Watcher：CuratorCache 监听在线 Master 节点变化。
- `/region_servers` Watcher：已通过 `RegionServerWatcher` 监听 RS 上线、更新、下线事件。
- HTTP 管理端点：`GET /health` 与 `GET /status` 返回 JSON（含角色信息）。
- 已抽出 `MetaManager` / `AssignmentManager` / `LoadBalancer`，支持 Master 元数据与分配读写。
- getTableLocation / createTable / dropTable / listRegionServers / listTables 已支持 ZooKeeper 元数据读写。
- createTable / dropTable 已通过真实 Thrift 调用把 DDL 转发到 RegionServer，再在成功后更新 ZooKeeper 元数据。
- `triggerRebalance()` 已具备最小可用迁移闭环：可把热点节点上的一个非主副本迁往更空闲节点，并更新 `/meta/tables` 与 `/assignments`。
- `triggerRebalance()` 已增强失败一致性：当迁移后置阶段（如 source 清理）失败时，会自动回滚 `/meta/tables` 与 `/assignments` 到迁移前快照，避免路由元数据悬空。
- `triggerRebalance()` 的缓存失效与恢复写入步骤改为 best-effort，不再因为非关键后置动作失败而破坏主迁移结果返回。
- `triggerRebalance()` 已补偿目标副本清理：当 transfer/后置阶段失败时，会 best-effort 清理 target 上可能残留的临时表数据，降低脏副本残留概率。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。

当前限制：
- 当前 rebalance 仍是最小可用版本，尚未形成完整定时调度器 + RegionMigrator + 故障恢复闭环。
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
- RegionServiceImpl 已实现基础写路径：WriteGuard 检查、WAL append、副本 sync、本地 MiniSQL 执行、异步 commit、checkpoint 计数。
- RegionServiceImpl 已实现基础读路径：直接 MiniSQL 执行，跳过 WAL 与 replica sync。
- executeBatch：按序执行，遇错即停。
- RegionAdminServiceImpl 已实现 pause/resume、deleteLocalTable、invalidateClientCache、transferTable、copyTableData 的基础路径。
- RegionAdminServiceImpl 的 `deleteLocalTable` 已增强 assignment 安全更新：只移除当前 RS 的副本条目，不再误删整条 `/assignments/{table}`；仅在副本列表为空时删除节点。
- ReplicaSyncServiceImpl 已支持内存/本地结合的基础同步路径、pullLog 与 commitLog 回放。

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
- 路由缓存支持 TTL 与版本失效。
- 读取 `/active-master` 时支持 address 优先、masterId 回退、坏数据 fallback。

当前限制：
- MOVING 已支持有界重试，但在重试窗口内若迁移仍未完成，客户端仍会返回 MOVING 给上层；尚未实现无限透明等待策略。
- 主副本不可达后的读故障转移、缓存主动失效广播仍未落地。

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
