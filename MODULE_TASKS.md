# SuperSQL 各模块具体实现任务说明

> 本文档是对 `DEVELOPMENT_PLAN.md` 的补充，**聚焦于每个必须实现模块的具体代码任务**，
> 说明需要创建哪些类、实现哪些方法、如何验证。
>
> 说明：本文档主要作为实现清单与验收基线，代码实时进展请以 `IMPLEMENTATION_STATUS.md` 为准。
>
> 必须实现：数据分布 · 集群管理 · 分布式查询 · 副本管理 · 容错容灾 · 负载均衡 · 缓存机制 · 日志机制
> Bonus：复杂查询（Join 等）· Table 切分

---

## 环境现状确认

### 已就绪

| 内容 | 状态 | 位置 |
|---|---|---|
| miniSQL C++ 引擎 | ✅ 全部源码完整，测试通过 | `minisql/cpp-core/` |
| Docker Compose 集群编排 | ✅ 3ZK+3Master+3RS+1Client | `docker-compose.yml` |
| 三套 Dockerfile | ✅ Master/RegionServer/Client | `docker/` |
| Thrift IDL（4 套 service） | ✅ 完整定义 | `rpc-proto/supersql.thrift` |
| Maven 多模块目录结构 | ✅ 目录已创建 | `java-master/` `java-regionserver/` `java-client/` |
| 环境变量模板 | ✅ | `.env` |

### 当前状态（基础骨架已落地，以下模块仍需按任务继续补全）

| 模块 | 目录 |
|---|---|
| java-master | `java-master/src/main/java/`（已有 Sprint1 主干实现） |
| java-regionserver | `java-regionserver/src/main/java/`（已有注册/心跳/副本同步基础实现） |
| java-client | `java-client/src/main/java/`（已有路由与缓存骨架实现） |

### miniSQL 输出格式（Java 解析必须对齐）

```
启动提示：>>> Welcome to MiniSQL
每行提示：>>> 
成功输出：>>> SUCCESS
错误输出：>>> Error: <message>
SELECT 结果头：列名居中，列间用 | 分隔，整行以 \n 结尾
分隔线：  ---...--- （连字符数 = 列数 × (最长值+1)）
数据行：  值居中，列间 |，最后一列换行
退出输出：>>> Bye bye~
```

---

## 模块一：集群管理（Cluster Management）

**目标**：ZooKeeper 负责集群拓扑感知；Master 通过领导者选举保证唯一 Active；RegionServer 通过临时节点实现自动注册与下线感知。

### 需要创建的文件

```
java-master/src/main/java/edu/zju/supersql/
├── MasterMain.java                          # 主入口，启动 Thrift 服务 + 选举
├── cluster/
│   ├── ZkClient.java                        # Curator 封装（连接、重试、基础 CRUD）
│   ├── LeaderElector.java                   # 领导者选举
│   ├── HeartbeatService.java                # Active Master 心跳写入
│   └── RegionServerWatcher.java             # 监听 /region_servers 子节点变更

java-regionserver/src/main/java/edu/zju/supersql/
├── RegionServerMain.java                    # 主入口
└── cluster/
    └── RegionServerRegistrar.java           # 向 ZK 注册临时节点，维持心跳
```

### 具体方法任务

#### `ZkClient.java`
```java
// 必须实现以下方法：
void connect(String zkConnect)             // Curator RetryNTimes(3,1000) 连接
void createEphemeral(String path, byte[] data)
void createEphemeralSequential(String path, byte[] data) → String  // 返回实际路径
void createPersistent(String path, byte[] data)
byte[] getData(String path)
void setData(String path, byte[] data)
void setData(String path, byte[] data, int version)   // CAS，用于 epoch 防脑裂
List<String> getChildren(String path)
void watchChildren(String path, ChildrenWatcher watcher)  // 注册 + 自动重注册
void watchData(String path, DataWatcher watcher)
boolean exists(String path)
void delete(String path)
```

#### `LeaderElector.java`
```java
// 必须实现：
void start()    // 在 /masters 下创建临时顺序节点，启动选举循环
// 内部逻辑：
//   1. createEphemeralSequential("/masters/master-", myAddress.getBytes())
//   2. getChildren("/masters") 排序，判断自己是否最小
//   3. 是 → becomeActive()；否 → watchData(前一个节点) 等待触发
void becomeActive()
//   1. 读 /active-master 的 epoch
//   2. epoch 在 15s 内 → 延迟 5s 后重新检查（防脑裂）
//   3. epoch 超时 → setData("/active-master", {epoch+1, myId, now}) with version CAS
//   4. 启动 HeartbeatService
//   5. 从 ZK 同步全量元数据到本地内存（MetaManager.loadFromZk()）
void becomeStandby()
//   停止 HeartbeatService，切换为只读状态，拒绝写请求返回 NOT_LEADER
```

#### `HeartbeatService.java`
```java
// 每 5 秒执行一次：
void tick()   // setData("/masters/active-heartbeat", System.currentTimeMillis())
// 同时：检查 /region_servers 下所有节点的 lastHeartbeat，标记超时节点
```

#### `RegionServerRegistrar.java`
```java
void register(RegionServerInfo info)
//   createEphemeral("/region_servers/" + rsId, JSON.toBytes(info))
void updateHeartbeat(RegionServerInfo latestInfo)
//   setData("/region_servers/" + rsId, JSON.toBytes(latestInfo))
//   每 10 秒调用一次，latestInfo 包含最新 tableCount/qps/cpu/mem
```

#### `RegionServerWatcher.java`（在 Master 中运行）
```java
void startWatching()
//   watchChildren("/region_servers", (addedNodes, removedNodes) -> {
//       removedNodes.forEach(rsId -> onRegionServerDown(rsId));
//       addedNodes.forEach(rsId -> onRegionServerUp(rsId));
//   })
void onRegionServerDown(String rsId)   // 触发容错容灾流程（见模块五）
void onRegionServerUp(String rsId)     // 更新 RS 列表，触发重均衡检查
```

### 验收标准
- `docker compose up -d`，`docker exec zk1 zkCli.sh ls /masters` 显示 3 个节点
- `docker exec zk1 zkCli.sh ls /region_servers` 显示 rs-1/rs-2/rs-3
- `docker stop master-1`，15 秒内 master-2 日志输出 `"became ACTIVE, epoch=2"`
- 旧 master-1 重启后日志输出 `"epoch stale, becoming STANDBY"`

---

## 模块二：数据分布（Data Distribution）

**目标**：CREATE TABLE 时 Master 选择最空闲的 RS 作为主副本，同时将另外 2 个 RS 分配为从副本，并在 ZK 写入路由元数据。RegionServer 在本地通过 miniSQL 建表。

### 需要创建的文件

```
java-master/src/main/java/edu/zju/supersql/
├── meta/
│   ├── MetaManager.java          # ZK 元数据读写封装
│   ├── TableLocation.java        # POJO：tableName + primaryRS + replicas + status + version
│   └── AssignmentManager.java    # /assignments 节点读写
├── rpc/
│   └── MasterServiceImpl.java    # Thrift MasterService 实现（含 createTable/dropTable/getTableLocation）

java-regionserver/src/main/java/edu/zju/supersql/
├── minisql/
│   ├── MiniSqlProcess.java       # ProcessBuilder 管理 miniSQL 进程
│   └── OutputParser.java         # miniSQL stdout → QueryResult
└── rpc/
    └── RegionServiceImpl.java    # Thrift RegionService 实现（execute/ping）
```

### 具体方法任务

#### `MetaManager.java`
```java
// ZK 路径约定：
//   /meta/tables/{tableName}   → JSON: {primaryRS:"rs-1:9090", status:"ACTIVE", version:3}
//   /assignments/{tableName}   → JSON: ["rs-1:9090","rs-2:9090","rs-3:9090"]

void createTableMeta(String tableName, String primaryRS, List<String> allReplicas)
//   createPersistent("/meta/tables/" + tableName, ...)
//   createPersistent("/assignments/" + tableName, ...)

TableLocation getTableLocation(String tableName)
//   getData("/meta/tables/" + tableName) → 解析 JSON

void updateTableStatus(String tableName, String status)   // ACTIVE / MOVING / OFFLINE
void updatePrimaryRS(String tableName, String newPrimaryRS)
void updateAssignment(String tableName, List<String> newReplicas)
void deleteTableMeta(String tableName)
List<String> listTables()
void loadFromZk()    // Master 选举成功后调用，将 ZK 元数据加载到本地 ConcurrentHashMap
```

#### `MasterServiceImpl.java` — createTable
```java
Response createTable(String ddl) {
    // 1. 解析 DDL 中的 tableName（简单字符串解析）
    String tableName = parseDDL(ddl);

    // 2. 检查表是否已存在
    if (metaManager.exists(tableName)) return Response(TABLE_EXISTS);

    // 3. 调用 LoadBalancer 选择主副本 RS 和 2 个从副本 RS
    List<RegionServerInfo> rsList = regionServerWatcher.getLiveRS();
    RegionServerInfo primary = loadBalancer.selectPrimary(rsList);
    List<RegionServerInfo> replicas = loadBalancer.selectReplicas(rsList, primary, 2);

    // 4. 向主副本 RS 发送 RegionAdminService.createTableLocal(ddl)
    //    （RS 内部调用 miniSqlProcess.execute(ddl)）
    adminClient(primary).createTableLocal(ddl);

    // 5. 向 2 个从副本 RS 也发送 createTableLocal(ddl)（建空表结构）
    replicas.forEach(rs -> adminClient(rs).createTableLocal(ddl));

    // 6. 写 ZK 元数据
    metaManager.createTableMeta(tableName, primary.address(), toAddresses(replicas));

    return Response(OK);
}
```

#### `MiniSqlProcess.java`
```java
// 关键实现细节：
void start(String dataDir)
//   pb = new ProcessBuilder("/opt/minisql/main")
//   pb.environment().put("HOME", dataDir)   // miniSQL 用相对路径，需设置工作目录
//   pb.directory(new File(dataDir))
//   process = pb.start()
//   启动独立线程持续读取 stderr，避免管道满导致死锁

synchronized String executeSql(String sql)
//   stdin.write(sql + "\n"); stdin.flush();
//   readUntilPrompt() — 读取到 ">>> " 为止
//   返回原始输出字符串

// 提示符：miniSQL 每次等待输入时输出 ">>> "（见 interpreter.cc:11）
// 注意：miniSQL 可能跨多行输入（以 ; 结尾），此处每次只发送完整 SQL
```

#### `OutputParser.java`
```java
// 根据 interpreter.cc 的实际输出格式解析：
QueryResult parse(String raw)
//   若含 ">>> SUCCESS"        → affectedRows=1, code=OK
//   若含 ">>> Error:"         → code=ERROR, errorMessage=...
//   若含 "---" 分隔线         → 解析表格：
//     第一段（---之前）       → 列名（按 | 分割，trim）
//     第二段（---之后各行）   → 数据行（按 | 分割，trim）
//   返回 QueryResult{columnNames, rows, affectedRows}

List<String> parseHeaderLine(String line)   // "  id  | name  | age  " → ["id","name","age"]
Row parseDataLine(String line)              // "  1  | Alice  | 25  " → Row(["1","Alice","25"])
```

### 验收标准
- Client 执行 `CREATE TABLE users(id int, name char(32), primary key(id));`
- ZK `/meta/tables/users` 节点存在，内容含 primaryRS
- ZK `/assignments/users` 节点存在，含 3 个 RS 地址
- 3 个 RS 容器内 `ls /data/db/users/` 均存在对应数据目录

---

## 模块三：日志机制（WAL Log）

**目标**：所有写操作（INSERT/DELETE/CREATE TABLE/DROP TABLE）在执行前先写 WAL；RS 重启后通过 WAL 完成 Crash Recovery；WAL 作为副本同步的数据载体。

### 需要创建的文件

```
java-regionserver/src/main/java/edu/zju/supersql/
└── wal/
    ├── WalManager.java      # WAL 文件写入、读取、恢复
    ├── WalEntry.java        # POJO，对应 Thrift WalEntry 结构
    ├── WalStatus.java       # enum: PREPARE, COMMITTED, ABORTED
    └── CrashRecovery.java   # 重启时读取 WAL 重放
```

### 具体方法任务

#### `WalEntry.java`
```java
// 字段（对应 Thrift IDL 定义）：
long lsn;           // 单调递增，AtomicLong 生成
long txnId;         // 本系统：lsn == txnId（单条语句即一个事务）
String tableName;
WalOpType opType;   // INSERT=1, DELETE=2, CREATE_TABLE=4, DROP_TABLE=5, ...
byte[] beforeRow;   // DELETE 时填写原始行（用于 UNDO，可选实现）
byte[] afterRow;    // INSERT/UPDATE 时的完整行 bytes（SQL 语句本身即可）
long timestamp;
WalStatus status;   // PREPARE / COMMITTED
```

#### `WalManager.java`
```java
// WAL 文件存储路径：$RS_WAL_DIR/{tableName}/wal-{startLSN}.log
// 文件格式（每条记录二进制序列化）：
//  [LSN:8B][TxnID:8B][OpType:1B][Status:1B][PayloadLen:4B][Payload:varlen][CRC32:4B]
// Payload = afterRow（对于写操作，直接存 SQL 语句的 UTF-8 bytes 最简单）

void init(String walDir)                     // 扫描已有 WAL 文件，确定 nextLsn
long append(WalEntry entry)                  // 写入 PREPARE 状态，返回 lsn
void commit(long lsn)                        // 将对应条目的 Status 改为 COMMITTED（原地覆写 1 字节）
void abort(long lsn)                         // 将 Status 改为 ABORTED
List<WalEntry> readAfter(String tableName, long startLsn)   // 供副本拉取日志
long getMaxLsn(String tableName)             // 供主副本查询从副本进度
void checkpoint(String tableName)            // 删除 COMMITTED 条目早于 checkpointLsn 的旧文件
AtomicLong nextLsn                           // 全局单调递增
```

#### `CrashRecovery.java`
```java
// RegionServerMain 启动时调用：
void recover(String tableName)
//   1. 读取 walDir/{tableName}/ 下所有 WAL 文件，按 startLSN 排序
//   2. 对每条 COMMITTED 状态的 WalEntry：
//        检查该操作是否已在 miniSQL 中执行
//        → 简单判断方式：对 INSERT 条目，用 SELECT 验证主键是否存在
//        → 若不存在，重放：miniSqlProcess.executeSql(entry.getSql())
//   3. 对 PREPARE（未提交）状态的条目：忽略（等效回滚）
//   4. 恢复完成后，更新 nextLsn = max(已有 LSN) + 1
```

#### WAL 在写路径中的位置（集成到 RegionServiceImpl）
```java
// RegionServiceImpl.execute() 写操作流程：
QueryResult execute(String tableName, String sql) {
    boolean isWrite = isWriteOperation(sql);   // INSERT / DELETE / DROP TABLE
    if (isWrite) {
        // Step 1: 写 WAL PREPARE
        WalEntry entry = new WalEntry(tableName, opType, sql.getBytes(), now);
        long lsn = walManager.append(entry);   // fsync

        // Step 2: 并行同步副本（见模块四）
        replicaManager.syncToReplicas(entry);

        // Step 3: COMMIT WAL
        walManager.commit(lsn);

        // Step 4: 执行 miniSQL
        String raw = miniSqlProcess.executeSql(sql);
        return outputParser.parse(raw);
    } else {
        // 只读：直接执行
        return outputParser.parse(miniSqlProcess.executeSql(sql));
    }
}
```

### 验收标准
- INSERT 100 条后，`ls /data/wal/users/` 有 WAL 文件，hexdump 可见 lsn 字段
- `docker kill rs-1`（-9 杀死），重启后 `SELECT COUNT(*)` 结果与杀死前一致
- WAL 每 1000 条触发 checkpoint，旧 WAL 文件被删除，目录不无限增长

---

## 模块四：副本管理（Replica Management）

**目标**：每张表维护 3 副本（1 主 + 2 从）；写操作采用半同步协议（主 + ≥1 从 ACK 后提交）；从副本落后时可拉取日志追赶；Master 可查询各副本 LSN 进度。

### 需要创建的文件

```
java-regionserver/src/main/java/edu/zju/supersql/
└── replica/
    ├── ReplicaManager.java              # 主副本协调（syncToReplicas / waitForAck）
    ├── ReplicaSyncServiceImpl.java      # Thrift ReplicaSyncService 实现（从副本端）
    └── ReplicaStatus.java               # enum: ACTIVE, SUSPECTED, OFFLINE
```

### 具体方法任务

#### `ReplicaManager.java`（运行在主副本 RS）
```java
// 初始化时从 ZK 读取本表的副本列表
void init(String tableName, List<String> replicaAddresses)

void syncToReplicas(WalEntry entry)
//   1. 获取所有从副本地址（排除自己）
//   2. 并行发起 ReplicaSyncService.syncLog(entry) 调用
//      使用 CompletableFuture + 超时 3 秒
//   CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> syncClient(rs2).syncLog(entry));
//   CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> syncClient(rs3).syncLog(entry));
//   3. 等待任意一个成功（anyOf），超时则标记超时副本为 SUSPECTED
//   CompletableFuture.anyOf(f1, f2).get(3, TimeUnit.SECONDS)

void markSuspected(String rsAddress)
//   本地标记 replicaStatus.put(rsAddress, SUSPECTED)
//   异步通知 Master：masterAdminClient.reportSuspectedReplica(tableName, rsAddress)

void onReplicaBack(String rsAddress)
//   replicaStatus.put(rsAddress, ACTIVE)
//   触发日志追赶：检查对方 LSN，若落后则推送差量

// 定期任务（每 30s）：检查各从副本 LSN 是否落后
void checkReplicaProgress()
//   for each active replica:
//     long theirLsn = syncClient(rs).getMaxLsn(tableName)
//     long myLsn = walManager.getMaxLsn(tableName)
//     if (myLsn - theirLsn > THRESHOLD) 触发日志推送
```

#### `ReplicaSyncServiceImpl.java`（运行在从副本 RS）
```java
Response syncLog(WalEntry entry)
//   1. walManager.append(entry)   // 写入本地 WAL（PREPARE 状态）
//   2. walManager.commit(entry.lsn)  // 立即 COMMIT（从副本信任主副本的决策）
//   3. miniSqlProcess.executeSql(entry.getSql())  // 本地执行
//   4. return Response(OK)

List<WalEntry> pullLog(String tableName, long startLsn)
//   return walManager.readAfter(tableName, startLsn)

long getMaxLsn(String tableName)
//   return walManager.getMaxLsn(tableName)

Response commitLog(String tableName, long lsn)
//   walManager.commit(lsn)
//   return Response(OK)
```

#### Thrift 多服务复用（RegionServer 端）
```java
// RegionServerMain 中的 Thrift 服务器初始化：
TMultiplexedProcessor processor = new TMultiplexedProcessor();
processor.registerProcessor("RegionService",
    new RegionService.Processor<>(new RegionServiceImpl(...)));
processor.registerProcessor("RegionAdminService",
    new RegionAdminService.Processor<>(new RegionAdminServiceImpl(...)));
processor.registerProcessor("ReplicaSyncService",
    new ReplicaSyncService.Processor<>(new ReplicaSyncServiceImpl(...)));

TServerTransport transport = new TServerSocket(RS_PORT);
TThreadPoolServer server = new TThreadPoolServer(
    new TThreadPoolServer.Args(transport)
        .processor(processor)
        .transportFactory(new TFramedTransport.Factory())
        .minWorkerThreads(5)
        .maxWorkerThreads(50));
server.serve();
```

### 验收标准
- INSERT 一条数据后，分别对 rs-1/rs-2/rs-3 执行 SELECT，3 个结果相同
- 人为停止 rs-2（`docker pause rs-2`），写入 10 条，恢复 rs-2，rs-2 日志追赶到最新
- syncLog 超时（`docker exec rs-2 tc netem delay 5000ms`），rs-1 日志输出 `"rs-2 suspected"`，写操作仍返回 OK

---

## 模块五：容错容灾（Fault Tolerance）

**目标**：RS 宕机后，ZK Watcher 在 10 秒内通知 Master；Master 自动从存活副本重建受影响表的第 3 副本；Master 宕机后 Standby 自动接管。

### 需要创建的文件

```
java-master/src/main/java/edu/zju/supersql/
└── failover/
    ├── FailoverCoordinator.java    # 容灾总协调（被 RegionServerWatcher 触发）
    └── ReplicaRebuilder.java       # 副本重建：从存活副本拷贝数据到新 RS
```

### 具体方法任务

#### `FailoverCoordinator.java`
```java
// 被 RegionServerWatcher.onRegionServerDown(rsId) 调用
void handleRsDown(String offlineRsId) {
    // 1. 查找所有以 offlineRsId 为副本的表
    List<String> affectedTables = metaManager.listTables().stream()
        .filter(t -> metaManager.getAssignment(t).contains(offlineRsId))
        .collect(toList());

    // 2. 并发处理（限流：最多同时恢复 2 张表，防止恢复风暴）
    Semaphore limit = new Semaphore(2);
    affectedTables.forEach(table -> executor.submit(() -> {
        limit.acquire();
        try { replicaRebuilder.rebuild(table, offlineRsId); }
        finally { limit.release(); }
    }));
}

// 处理主副本宕机的特殊逻辑：
void promotePrimary(String tableName, String offlinePrimary) {
    // 从存活的从副本中选 LSN 最高的作为新主副本
    List<String> survivors = metaManager.getAssignment(tableName).stream()
        .filter(rs -> !rs.equals(offlinePrimary) && isAlive(rs))
        .collect(toList());
    String newPrimary = selectHighestLsn(survivors, tableName);
    metaManager.updatePrimaryRS(tableName, newPrimary);
    // 通知所有客户端缓存失效
    broadcastCacheInvalidation(tableName);
}
```

#### `ReplicaRebuilder.java`
```java
void rebuild(String tableName, String offlineRsId) {
    // 1. 找一个存活的副本作为数据源
    String sourceRs = findSurvivorReplica(tableName, offlineRsId);
    if (sourceRs == null) {
        log.error("No survivor for table {}, data lost!", tableName);
        return;
    }

    // 2. 选一个新 RS 作为目标（表数最少且不在当前副本列表中）
    List<String> currentReplicas = metaManager.getAssignment(tableName);
    String targetRs = loadBalancer.selectForRebuild(getLiveRS(), currentReplicas);

    // 3. 调用 RegionAdminService.transferTable(tableName, targetHost, targetPort)
    //    （源 RS 负责把数据文件流式发送到目标 RS）
    adminClient(sourceRs).transferTable(tableName,
        rsInfo(targetRs).host, rsInfo(targetRs).port);

    // 4. 更新 ZK /assignments/tableName
    List<String> newReplicas = new ArrayList<>(currentReplicas);
    newReplicas.remove(offlineRsId);
    newReplicas.add(targetRs);
    metaManager.updateAssignment(tableName, newReplicas);

    // 5. 若下线的是主副本，还需晋升新主
    if (offlineRsId.equals(metaManager.getPrimaryRS(tableName))) {
        promotePrimary(tableName, offlineRsId);
    }

    log.info("Table {} replica rebuilt on {}", tableName, targetRs);
}
```

#### `RegionAdminServiceImpl.java`（RS 端数据传输）
```java
// 实现 transferTable：将本地 miniSQL 数据目录打包流式发给目标 RS
Response transferTable(String tableName, String targetHost, int targetPort) {
    // miniSQL 数据文件在 $RS_DATA_DIR/database/data/{tableName}
    //              索引文件在 $RS_DATA_DIR/database/index/INDEX_FILE_*{tableName}
    //              catalog 在 $RS_DATA_DIR/database/catalog/catalog_file
    File dataDir = new File(rsDataDir + "/database");
    ReplicaSyncService.Client target = syncClient(targetHost, targetPort);
    // 逐文件、逐块发送（每块 64KB）
    for (File file : collectTableFiles(dataDir, tableName)) {
        sendFileInChunks(file, tableName, target);
    }
    return Response(OK);
}

Response copyTableData(DataChunk chunk) {
    // 接收端：将 chunk.data 写入本地对应路径
    File dest = new File(rsDataDir + "/" + chunk.fileName);
    dest.getParentFile().mkdirs();
    try (FileOutputStream fos = new FileOutputStream(dest, chunk.offset > 0)) {
        fos.write(chunk.data);
    }
    if (chunk.isLast) {
        // 所有文件接收完毕，通知 miniSQL 进程重新加载（重启子进程）
        miniSqlProcess.restart();
    }
    return Response(OK);
}
```

### 验收标准
- `docker stop rs-1`，10 秒内 master-1 日志显示 `"rebuilding replica for table X on rs-new"`
- 30 秒内，`/assignments/X` 不再含 rs-1，新 RS 可提供查询服务
- `docker stop master-1`（Active），15 秒内 master-2 成为 Active，`/active-master` epoch 递增
- 同时 `docker stop rs-1 rs-2`（仅剩 1 副本），系统日志警告但不崩溃，仅剩副本仍可读

---

## 模块六：负载均衡（Load Balancing）

**目标**：建表时静态分配到最空闲节点；每 30 秒检查动态均衡；热点 RS（tableCount > 1.5×avg 或 QPS > 2×avg）触发 Region 迁移；迁移过程对客户端透明（最多 MOVING 重试）。

### 需要创建的文件

```
java-master/src/main/java/edu/zju/supersql/
└── balance/
    ├── LoadBalancer.java           # 节点选择算法（静态分配 + 重建选择）
    ├── RebalanceScheduler.java     # 定时检查 + 触发迁移
    └── RegionMigrator.java         # Region 迁移 6 步流程
```

### 具体方法任务

#### `LoadBalancer.java`
```java
// 静态分配：建表时调用
RegionServerInfo selectPrimary(List<RegionServerInfo> candidates)
//   按 tableCount 升序排，取第一个（最空闲）

List<RegionServerInfo> selectReplicas(List<RegionServerInfo> candidates,
                                       RegionServerInfo primary, int count)
//   从剩余节点中按 tableCount 升序取前 count 个
//   确保：每个副本在不同物理节点（不同 rsId）

RegionServerInfo selectForRebuild(List<RegionServerInfo> liveRS,
                                   List<String> existingReplicas)
//   从不在 existingReplicas 中的 RS 里选 tableCount 最少的

// 动态均衡判断
boolean isHotspot(RegionServerInfo rs, List<RegionServerInfo> allRS)
//   double avgTableCount = allRS.stream().mapToInt(r->r.tableCount).average()
//   double avgQps = allRS.stream().mapToDouble(r->r.qps1min).average()
//   return rs.tableCount > avgTableCount * REBALANCE_RATIO        // 1.5
//       || rs.qps1min > avgQps * 2
//       || rs.cpuUsage > 0.8

String selectTableToMigrate(RegionServerInfo hotRS, Map<String,TableStats> stats)
//   选该 RS 上表数最少（迁移代价最小）或 QPS 贡献最大的表

RegionServerInfo selectMigrationTarget(List<RegionServerInfo> allRS,
                                        List<String> tableCurrentReplicas)
//   从不持有该表的 RS 中选 tableCount 最少的
```

#### `RebalanceScheduler.java`
```java
// 使用 ScheduledExecutorService，每 30 秒执行一次
void checkAndRebalance() {
    if (!leaderElector.isActive()) return;   // Standby 不执行

    List<RegionServerInfo> allRS = regionServerWatcher.getLiveRS();
    if (allRS.size() < 2) return;            // 节点数不足，跳过

    for (RegionServerInfo rs : allRS) {
        if (loadBalancer.isHotspot(rs, allRS)) {
            String table = loadBalancer.selectTableToMigrate(rs, getTableStats());
            RegionServerInfo target = loadBalancer.selectMigrationTarget(allRS,
                metaManager.getAssignment(table));
            if (target != null) {
                log.info("Rebalance: migrate {} from {} to {}", table, rs.id, target.id);
                regionMigrator.migrate(table, rs.id, target.id);
                break;   // 每次只迁移一张，等下个周期再检查
            }
        }
    }
}
```

#### `RegionMigrator.java` — 完整 6 步迁移
```java
void migrate(String tableName, String sourceRsId, String targetRsId) {
    log.info("Migration START: {} {} → {}", tableName, sourceRsId, targetRsId);

    // Step 1: 标记 MOVING（客户端见到此状态返回 MOVING，等待重试）
    metaManager.updateTableStatus(tableName, "MOVING");

    try {
        // Step 2: 暂停源 RS 对该表的写入
        adminClient(sourceRsId).pauseTableWrite(tableName);

        // Step 3: 流式传输数据文件（含 WAL）
        RegionServerInfo target = rsInfo(targetRsId);
        adminClient(sourceRsId).transferTable(tableName, target.host, target.port);

        // Step 4: 更新 ZK 路由（原子更新，先 assignment 再 meta）
        List<String> newReplicas = new ArrayList<>(metaManager.getAssignment(tableName));
        newReplicas.remove(sourceRsId + ":" + rsInfo(sourceRsId).port);
        newReplicas.add(targetRsId + ":" + target.port);
        metaManager.updateAssignment(tableName, newReplicas);
        metaManager.updateTableStatus(tableName, "ACTIVE");

        // Step 5: 删除源 RS 本地数据
        adminClient(sourceRsId).deleteLocalTable(tableName);

        // Step 6: 广播所有 RS 通知客户端缓存失效
        for (RegionServerInfo rs : regionServerWatcher.getLiveRS()) {
            adminClient(rs.id).invalidateClientCache(tableName);
        }

        log.info("Migration DONE: {} → {}", tableName, targetRsId);

    } catch (Exception e) {
        // 回滚：恢复写入 + 还原状态
        log.error("Migration FAILED, rolling back: {}", e.getMessage());
        try { adminClient(sourceRsId).resumeTableWrite(tableName); } catch (Exception ignored) {}
        metaManager.updateTableStatus(tableName, "ACTIVE");
        throw new RuntimeException("Migration failed", e);
    }
}
```

#### RS 端：暂停/恢复写入实现
```java
// RegionAdminServiceImpl.java 中：
private final Map<String, Boolean> pausedTables = new ConcurrentHashMap<>();

Response pauseTableWrite(String tableName) {
    pausedTables.put(tableName, true);
    return Response(OK);
}

Response resumeTableWrite(String tableName) {
    pausedTables.remove(tableName);
    return Response(OK);
}

// RegionServiceImpl.execute() 开头检查：
if (pausedTables.containsKey(tableName) && isWriteOperation(sql)) {
    return QueryResult{status: Response(MOVING, "Table is migrating, please retry")};
}
```

### 验收标准
- 创建 6 张表，全部分配到 3 个 RS，每个 RS 各约 2 张（±1）
- 手动将 rs-1 的 tableCount 指标提高到 rs-2 的 2 倍，30s 内触发迁移，日志显示 `"Rebalance: migrate"`
- 迁移期间 Client 收到 `MOVING` 后自动重试，最终 SELECT 成功
- `GET http://localhost:8880/admin/metrics` 显示各 RS 负载指标

---

## 模块七：缓存机制（Client Cache）

**目标**：Client 本地维护 `tableName → TableLocation` 的 HashMap，TTL=30s；版本号不匹配时立即失效；主副本不可达时自动切换从副本；RS 迁移后收到失效通知或等待 TTL 超期。

### 需要创建的文件

```
java-client/src/main/java/edu/zju/supersql/
├── SqlClient.java              # REPL 主入口
├── cache/
│   ├── RouteCache.java         # 路由缓存核心
│   └── CachedTableLocation.java  # POJO
└── rpc/
    ├── MasterRpcClient.java    # 连接 Active Master 的 Thrift 客户端
    └── RegionRpcClient.java    # 连接 RS 的 Thrift 客户端池
```

### 具体方法任务

#### `CachedTableLocation.java`
```java
String tableName;
String primaryRS;          // "rs-1:9090"
List<String> allReplicas;  // ["rs-1:9090","rs-2:9090","rs-3:9090"]
long expireTime;           // System.currentTimeMillis() + TTL_MS (30s)
long version;              // 从 /meta/tables/{name} 读到的 version 字段

boolean isExpired()  { return System.currentTimeMillis() > expireTime; }
boolean isValid(long zkVersion) { return this.version == zkVersion && !isExpired(); }
```

#### `RouteCache.java`
```java
private final ConcurrentHashMap<String, CachedTableLocation> cache = new ConcurrentHashMap<>();
private static final long TTL_MS = 30_000;

CachedTableLocation get(String tableName)
//   entry = cache.get(tableName)
//   if entry == null || entry.isExpired() → return null（调用方去 Master 查）

void put(String tableName, TableLocation loc)
//   cache.put(tableName, new CachedTableLocation(loc, now + TTL_MS))

void invalidate(String tableName)
//   cache.remove(tableName)

void invalidateAll()
//   cache.clear()
```

#### `MasterRpcClient.java`
```java
// 连接时通过 ZK 找 Active Master（不依赖固定地址）
void connectViaZk(String zkConnect)
//   zkClient.connect(zkConnect)
//   activeMasterAddr = JSON.parse(zkClient.getData("/active-master")).masterId
//   连接 activeMasterAddr 对应的 Thrift 服务

// 处理 NOT_LEADER 响应（Standby 收到请求时返回）：
TableLocation getTableLocation(String tableName) {
    try {
        return masterService.getTableLocation(tableName);
    } catch (NotLeaderException e) {
        // 切换到新 Active Master 重试（最多 3 次）
        reconnectToActiveMaster();
        return masterService.getTableLocation(tableName);
    }
}
```

#### `RegionRpcClient.java`
```java
// 连接池：每个 RS 地址维护一个 Thrift 连接（或简单每次重建）
QueryResult execute(String primaryRS, String tableName, String sql) {
    try {
        return getConnection(primaryRS).execute(tableName, sql);
    } catch (TTransportException e) {
        // 主副本连接失败：从缓存中选一个从副本重试（读操作）
        String backup = pickBackupReplica(tableName, primaryRS);
        if (backup != null) {
            cache.invalidate(tableName);  // 主副本可能已下线，清缓存
            return getConnection(backup).execute(tableName, sql);
        }
        throw e;
    } catch (MovingException e) {
        // 表在迁移中：等待 500ms 后重试，最多 5 次
        Thread.sleep(500);
        return execute(primaryRS, tableName, sql);   // 递归重试（实际用循环）
    }
}
```

#### `SqlClient.java` — REPL 主循环
```java
void run() {
    Scanner sc = new Scanner(System.in);
    System.out.print("SuperSQL> ");
    StringBuilder buf = new StringBuilder();

    while (sc.hasNextLine()) {
        String line = sc.nextLine().trim();
        buf.append(line).append(" ");

        if (line.endsWith(";")) {
            String sql = buf.toString().trim();
            buf.setLength(0);

            try {
                QueryResult result = dispatch(sql);    // 路由 + 执行
                printResult(result);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
            System.out.print("SuperSQL> ");
        }
    }
}

QueryResult dispatch(String sql) {
    String lowerSql = sql.toLowerCase();

    // DDL 走 Master
    if (lowerSql.startsWith("create table") || lowerSql.startsWith("drop table")) {
        return masterClient.executeDDL(sql);
    }

    // DML/DQL 走 RegionServer（通过路由缓存定位）
    String tableName = extractTableName(sql);   // 从 SQL 中解析表名
    CachedTableLocation loc = routeCache.get(tableName);
    if (loc == null) {
        TableLocation fresh = masterClient.getTableLocation(tableName);
        routeCache.put(tableName, fresh);
        loc = routeCache.get(tableName);
    }
    return rsClient.execute(loc.primaryRS, tableName, sql);
}
```

### 验收标准
- 连续执行 10 次 `SELECT * FROM users`，只有第 1 次访问 Master（后 9 次缓存命中，Master 日志无新 `getTableLocation` 请求）
- 迁移表后，Client 在 30s 内（TTL 超期）或立即（收到失效通知）路由到新 RS
- 主副本宕机（`docker stop rs-1`），读请求自动切换从副本，SELECT 继续成功

---

## 模块八：分布式查询（Distributed Query）

**目标**：Client 路由到正确的 RS，RS 调用 miniSQL 执行 SQL，结果直接返回。支持 CREATE TABLE/INSERT/DELETE/SELECT（含 WHERE 多条件）、CREATE INDEX/DROP INDEX/execfile。Standby Master 正确返回 REDIRECT。

### 需要创建/完善的文件

```
java-master/src/main/java/edu/zju/supersql/rpc/
└── MasterServiceImpl.java     # 完整实现所有 MasterService 方法

java-regionserver/src/main/java/edu/zju/supersql/rpc/
└── RegionServiceImpl.java     # 完整实现 execute / executeBatch / createIndex / dropIndex

java-client/src/main/java/edu/zju/supersql/
└── SqlClient.java             # dispatch 逻辑完整实现
```

### 具体方法任务

#### SQL 路由规则（Client 端）

| SQL 类型 | 路由目标 | 说明 |
|---|---|---|
| `CREATE TABLE` | Master | Master 选 RS，通知各副本建表 |
| `DROP TABLE` | Master | Master 协调各副本删除 |
| `CREATE INDEX` | 主副本 RS，Master 广播 | 先在主副本建，再同步到从副本 |
| `DROP INDEX` | 同上 | |
| `INSERT / DELETE` | 主副本 RS | 写操作，走副本同步路径 |
| `SELECT` | 主副本 RS（默认）| 也可路由到任意副本（弱一致读） |
| `execfile` | Client 本地展开 | 逐行解析，按上述规则分发 |

#### `RegionServiceImpl.executeBatch()` — execfile 实现
```java
QueryResult executeBatch(String tableName, List<String> sqls) {
    List<QueryResult> results = new ArrayList<>();
    for (String sql : sqls) {
        results.add(execute(tableName, sql));
    }
    // 合并结果（所有 affectedRows 求和，任一 ERROR 则整体返回 ERROR）
    return mergeResults(results);
}
```

#### CREATE INDEX 分布式传播（MasterServiceImpl）
```java
// 由 Client 将 CREATE INDEX 发给 Master，Master 广播到 3 个副本
// （简化：Client 也可直接发给主副本 RS，RS 在 syncLog 中携带 CREATE_INDEX 操作类型）
Response broadcastIndex(String tableName, String ddl) {
    List<String> replicas = metaManager.getAssignment(tableName);
    List<Exception> errors = new ArrayList<>();
    for (String rs : replicas) {
        try { rsClient(rs).createIndex(tableName, ddl); }
        catch (Exception e) { errors.add(e); }
    }
    return errors.isEmpty() ? Response(OK) : Response(ERROR, errors.get(0).getMessage());
}
```

#### 表名解析工具（Client 端）
```java
// 从 SQL 中提取操作的表名，用于路由
String extractTableName(String sql) {
    // SELECT * FROM users WHERE ...   → "users"
    // INSERT INTO users VALUES ...    → "users"
    // DELETE FROM users WHERE ...     → "users"
    // CREATE INDEX idx ON users (id)  → "users"
    // 简单实现：按关键字定位
    String lower = sql.toLowerCase().trim();
    if (lower.startsWith("select"))  return extractAfter(lower, "from");
    if (lower.startsWith("insert"))  return extractAfter(lower, "into");
    if (lower.startsWith("delete"))  return extractAfter(lower, "from");
    if (lower.startsWith("create index")) return extractAfter(lower, " on ");
    throw new IllegalArgumentException("Cannot extract table name from: " + sql);
}
```

### 验收标准
- 完整执行设计用例 TC-01 ~ TC-06，全部通过
- `execfile test_file1.sql`（1000 条 SQL）在 3 副本间一致
- Standby Master 收到 Client 请求时，返回 REDIRECT，Client 自动切换到 Active Master

---

## Bonus A：复杂查询（Join 等）

> 仅在必须模块全部完成后实现。

**目标**：支持两表 JOIN（INNER JOIN 语义），由 Client 或 Master 协调跨 RS 的查询分发与结果合并。

### 实现方案（推荐：Client 端 Hash Join）

```
Client 收到：SELECT * FROM A JOIN B ON A.id = B.aid WHERE ...

Step 1: 分别定位 A 所在 RS（rs-1）和 B 所在 RS（rs-2）
Step 2: 向 rs-1 发送 SELECT * FROM A（全表或带 WHERE 过滤）
Step 3: 向 rs-2 发送 SELECT * FROM B
Step 4: Client 内存 Hash Join：
        Map<String, List<Row>> hashMap = buildHashMap(resultA, joinKey);
        List<Row> joined = resultB.rows.stream()
            .flatMap(row -> hashMap.getOrDefault(row.get(joinKey), emptyList()).stream()
                .map(aRow -> merge(aRow, row)))
            .collect(toList());
Step 5: 输出合并后的结果集
```

### 需要新增的文件
```
java-client/src/main/java/edu/zju/supersql/
└── query/
    ├── JoinPlanner.java      # 解析 JOIN SQL，拆分为子查询
    ├── HashJoinExecutor.java # 两表 Hash Join 实现
    └── ResultMerger.java     # 结果集合并、列名对齐
```

### SQL 解析范围（最小可行实现）
```sql
-- 支持：
SELECT A.col1, B.col2 FROM A JOIN B ON A.id = B.aid;
SELECT * FROM A INNER JOIN B ON A.id = B.aid WHERE A.age > 18;

-- 不需要支持：
-- 三表 JOIN、子查询、聚合函数（SUM/COUNT）、ORDER BY
```

---

## Bonus B：Table 切分（Table Sharding）

> 仅在必须模块全部完成后实现。

**目标**：单张大表按主键范围水平切分为多个 Region Shard，分布到不同 RS；查询时 Client 根据 WHERE 条件的主键范围定位到对应 Shard。

### 设计要点

1. **切分粒度**：按主键（int 类型）范围划分，如 `[0,1000), [1000,2000), ...`
2. **元数据扩展**：`/meta/tables/{tableName}/shards/{shardId}` 存储各 Shard 的主键范围和所在 RS
3. **miniSQL 适配**：每个 Shard 在 RS 上以独立表名存储（如 `users_shard_0`, `users_shard_1`）
4. **路由**：Client 根据 WHERE `id = X` 或 `id BETWEEN A AND B` 定位 Shard；全表扫描则广播到所有 Shard 并合并

### 元数据结构扩展
```
/meta/tables/users/
    primary         → "rs-1:9090"      (兼容非切分模式)
    sharded         → "true"
    shards/
        shard-0     → {range:[0,1000), primaryRS:"rs-1:9090", replicas:[...]}
        shard-1     → {range:[1000,2000), primaryRS:"rs-2:9090", replicas:[...]}
        shard-2     → {range:[2000,+∞), primaryRS:"rs-3:9090", replicas:[...]}
```

### 需要新增的文件
```
java-master/src/main/java/edu/zju/supersql/
└── sharding/
    ├── ShardingManager.java     # Shard 创建、分裂、元数据管理
    └── ShardPlanner.java        # 根据主键范围选择 Shard

java-client/src/main/java/edu/zju/supersql/
└── sharding/
    └── ShardRouter.java         # 根据 WHERE 主键条件定位 Shard，广播或单点查询
```

---

## 实现顺序建议

```
周次      必须实现                              Bonus（可选）
W1        集群管理基础（ZK连接、选举、注册）
W2        日志机制（WAL写入、Crash Recovery）
W3        数据分布（createTable路由、miniSQL进程管理）
W4        副本管理（syncLog、pullLog、半同步写）
W5        容错容灾（RS宕机感知、副本重建）
          负载均衡（静态分配、动态重均衡、迁移）
W6        缓存机制（RouteCache、失效通知）
          分布式查询（完整链路、execfile、createIndex广播）
          ─────────── 全部 TC-01~TC-13 验收 ───────────
W7        文档 + 演示                           复杂查询 / Table切分
```

---

## 快速验收检查清单

```bash
# 集群管理
docker exec zk1 zkCli.sh ls /masters          # 3 个临时顺序节点
docker exec zk1 zkCli.sh ls /region_servers   # rs-1 rs-2 rs-3
docker stop master-1 && sleep 15 && docker logs master-2 | grep "ACTIVE"

# 数据分布
docker exec client java -jar /app/client.jar  # 进入 REPL
>>> CREATE TABLE users(id int, name char(32), primary key(id));
docker exec zk1 zkCli.sh get /meta/tables/users
docker exec zk1 zkCli.sh get /assignments/users

# WAL
>>> INSERT INTO users VALUES(1, 'Alice');
docker exec rs-1 ls /data/wal/users/          # 有 WAL 文件
docker kill rs-1 && docker start rs-1
docker exec rs-1 docker logs rs-1 | grep "recovery"

# 副本一致性
>>> INSERT INTO users VALUES(2, 'Bob');
# 分别进入 rs-1/rs-2/rs-3 容器，执行 /opt/minisql/main，select * from users;
# 3 个结果相同

# 容错容灾
docker stop rs-1
sleep 15 && docker logs master-1 | grep "rebuild"
docker exec zk1 zkCli.sh get /assignments/users   # rs-1 已被替换

# 负载均衡
# 创建 6 张表后查看分配
docker exec zk1 zkCli.sh ls /assignments          # 列出所有表
# 每个 RS 约各 2 张

# 缓存
# 连续 10 次 SELECT，观察 master-1 日志中 getTableLocation 调用次数

# 分布式查询完整链路
>>> CREATE TABLE t1(id int, val char(10), primary key(id));
>>> INSERT INTO t1 VALUES(1, 'hello');
>>> SELECT * FROM t1 WHERE id = 1;
>>> DELETE FROM t1 WHERE id = 1;
>>> DROP TABLE t1;
```
