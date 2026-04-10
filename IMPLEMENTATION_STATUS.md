# SuperSQL 当前实现状态（2026-04-10）

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
- HTTP 管理端点：`GET /health` 与 `GET /status` 返回 JSON（含角色信息）。
- getTableLocation / createTable / dropTable / listRegionServers / listTables — 完整 ZooKeeper 元数据读写。
- 非 Active Master 下，createTable/dropTable 返回 NOT_LEADER 并带 redirectTo。

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
- **完整 WAL 持久化**：二进制格式 `[8B lsn][8B txnId][1B opType][4B sqlLen][payload]`，每表一个 `.wal` 文件，`FileChannel.force(false)` 保证落盘。启动时扫描 `.wal` 文件恢复全局 LSN。
- **WriteGuard**：per-table 写入暂停（用于 Region 迁移），支持 `pause/resume/awaitWritable`。
- **ReplicaManager**：主侧通过 `TFramedTransport + TMultiplexedProtocol` 并发 sync 到副本（CompletableFuture 半同步），commitOnReplicas 异步 fire-and-forget。
- **RegionServiceImpl 完整写路径**：
  1. WriteGuard 检查（返回 MOVING 时不执行）
  2. WAL append（nextLsn → appendEntry）
  3. ReplicaManager.syncToReplicas（等待 ≥1 ACK）
  4. MiniSQL 本地执行
  5. ReplicaManager.commitOnReplicas（异步）
  6. WAL 写计数检查，触发异步 Checkpoint
- **RegionServiceImpl 读路径**：直接 MiniSQL 执行，跳过 WAL 与 replica sync。
- **executeBatch**：按序执行，遇到错误立即停止。
- **RegionAdminServiceImpl**：pauseTableWrite / resumeTableWrite / deleteLocalTable（删本地文件 + ZK assignment 节点）/ invalidateClientCache / transferTable（流式传输 4KB DataChunk） / copyTableData（写入指定 offset）。
- **ReplicaSyncServiceImpl**：内存 WAL（ConcurrentSkipListMap）+ commitLog 回放 SQL 到副本 MiniSQL。

## 3. Client 侧已实现内容

实现文件：
- java-client/src/main/java/edu/zju/supersql/client/SqlClient.java
- java-client/src/main/java/edu/zju/supersql/client/RouteCache.java
- java-client/src/main/java/edu/zju/supersql/client/MasterRpcClient.java
- java-client/src/main/java/edu/zju/supersql/client/RegionRpcClient.java

已落地能力：
- **MasterRpcClient**：AutoCloseable Thrift 封装，`fromAddress("host:port", timeoutMs)`，支持 getTableLocation / createTable / dropTable / listTables / listRegionServers / getActiveMaster。
- **RegionRpcClient**：AutoCloseable Thrift 封装，`fromInfo(RegionServerInfo, timeoutMs)` / `fromAddress(hostPort, timeoutMs)`，支持 execute / executeBatch / createIndex / dropIndex / ping。
- **SqlClient REPL 完整路由**：
  - DDL → MasterRpcClient（createTable / dropTable，其他 DDL 转发到 RegionRpcClient）
  - DML → RouteCache 查 TableLocation → RegionRpcClient.execute；REDIRECT 时自动失效缓存重试；MOVING 时提示用户重试。
  - SHOW TABLES → MasterRpcClient.listTables()，格式化打印表列表。
- printQueryResult 支持打印列名、分隔符、行数据、affected rows。
- 路由缓存 TTL = 30 秒，版本失效支持。

## 4. 已落地测试

Master（14 个测试）：
- LeaderElectorTest, MasterHeartbeatIntegrationTest, MasterServerHttpPayloadTest
- MasterServiceImplTest, MasterServiceMetadataIntegrationTest

RegionServer（41 个测试）：
- WalManagerTest（round-trip / LSN 恢复 / checkpoint 检测 / per-table 隔离）
- WriteGuardTest（pause/resume / awaitWritable 解除阻塞）
- ReplicaManagerTest（内嵌 Thrift 服务端 / ACK 计数 / 超时处理）
- RegionServiceImplTest（写路径 WAL+sync+execute / 读路径跳过 WAL / MOVING / LSN 单调 / batch）
- RegionAdminServiceImplTest（pause/resume / 文件删除 / copyTableData offset 写入）
- ReplicaSyncServiceImplTest（syncLog / commitLog SQL 回放 / pullLog 排序）
- RegionServerRegistrarIntegrationTest

Client（15 个测试）：
- MasterRpcClientTest（内嵌 Thrift 服务端验证所有方法）
- RegionRpcClientTest（ping / execute / executeBatch / fromInfo）
- SqlClientRoutingTest（classifySql / extractTableName / ZK active-master 读取 / RouteCache TTL）

执行方式：
```bash
# 从仓库根目录运行全部测试（先安装依赖）
mvn install -N -DskipTests
mvn install -pl rpc-proto,test-common -DskipTests
mvn test -pl java-master,java-regionserver,java-client
```

## 5. Docker 集群启动

```bash
docker compose up -d --build
docker compose ps   # 检查各容器健康状态

# 连接交互式 REPL
docker exec -it client java -jar /app/client.jar

# REPL 示例命令
SuperSQL> create table test(id int, name char(20), primary key(id));
SuperSQL> insert into test values(1, "hello");
SuperSQL> select * from test where id = 1;
SuperSQL> show tables;
SuperSQL> drop table test;
```
