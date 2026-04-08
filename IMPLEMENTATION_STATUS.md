# SuperSQL 当前实现状态（2026-04-09）

本文档记录近期已经落地并通过测试的 Java 分布式层功能，避免 README 与代码状态偏差。

## 1. Master 侧已实现内容

实现文件：
- java-master/src/main/java/edu/zju/supersql/master/MasterRuntimeContext.java
- java-master/src/main/java/edu/zju/supersql/master/MasterServer.java
- java-master/src/main/java/edu/zju/supersql/master/rpc/MasterServiceImpl.java
- java-master/src/main/java/edu/zju/supersql/master/election/LeaderElector.java

已落地能力：
- 启动时初始化 Master 运行上下文（含本机 masterId、address、zkClient）。
- 启动时尝试引导 active-master 节点。
- Active Master 心跳：每 5 秒更新 `/masters/active-heartbeat`（仅 ACTIVE 节点写入）。
- LeaderLatch 选主：Master 启动后参与 `/masters` 选主并在当选时更新 `/active-master`。
- `/masters` Watcher：监听在线 Master 节点变化并记录事件日志。
- HTTP 管理端点：`GET /health` 与 `GET /status` 返回 JSON（含角色信息）。
- getTableLocation 基于 ZooKeeper 元数据返回表路由。
- createTable 支持：DDL 提取表名、按 tableCount 选择副本、写入 /meta/tables 与 /assignments。
- dropTable 支持清理 /meta/tables 与 /assignments。
- listRegionServers 与 listTables 支持从 ZooKeeper 枚举。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。

当前限制：
- 还没有与 RegionAdminService 联动执行真正的远端建表/删表。
- Master 选主与脑裂防护目前仍是基础骨架，尚未完整实现 LeaderLatch 流程。

## 2. RegionServer 侧已实现内容

实现文件：
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/RegionServerRegistrar.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/RegionServerMain.java
- java-regionserver/src/main/java/edu/zju/supersql/regionserver/rpc/ReplicaSyncServiceImpl.java

已落地能力：
- 启动时在 /region_servers/{rsId} 注册节点。
- 每 10 秒上报心跳（tableCount/qps/cpu/mem/lastHeartbeat）。
- ReplicaSyncService 支持内存 WAL 的 syncLog/pullLog/getMaxLsn/commitLog 基础路径。

当前限制：
- Replica WAL 当前为内存实现，尚未持久化到 WalManager 文件格式。
- commitLog 目前只做提交标记，尚未接入真实可见性切换流程。

## 3. Client 侧已实现内容

实现文件：
- java-client/src/main/java/edu/zju/supersql/client/SqlClient.java
- java-client/src/main/java/edu/zju/supersql/client/RouteCache.java

已落地能力：
- SQL 分类（DDL/DML/UNKNOWN）。
- 基础表名提取（from/into/table/update）。
- 从 /active-master 读取主节点地址（address 优先，masterId 次之，失败回退 fallback）。
- 路由缓存支持 TTL 与版本失效。

当前限制：
- 仍是路由骨架输出，尚未接入 MasterRpcClient/RegionRpcClient 做真实 RPC 执行。

## 4. 已落地测试（新增与扩展）

Master：
- java-master/src/test/java/edu/zju/supersql/master/election/LeaderElectorTest.java
- java-master/src/test/java/edu/zju/supersql/master/MasterHeartbeatIntegrationTest.java
- java-master/src/test/java/edu/zju/supersql/master/MasterServerHttpPayloadTest.java
- java-master/src/test/java/edu/zju/supersql/master/rpc/MasterServiceImplTest.java
- java-master/src/test/java/edu/zju/supersql/master/rpc/MasterServiceMetadataIntegrationTest.java

RegionServer：
- java-regionserver/src/test/java/edu/zju/supersql/regionserver/rpc/ReplicaSyncServiceImplTest.java
- java-regionserver/src/test/java/edu/zju/supersql/regionserver/RegionServerRegistrarIntegrationTest.java

Client：
- java-client/src/test/java/edu/zju/supersql/client/SqlClientRoutingTest.java

执行方式：
- 仓库根目录执行：mvn test -DskipTests=false

说明：
- 集成测试使用 Curator TestingServer 启动内嵌 ZooKeeper，验证真实节点读写语义。
