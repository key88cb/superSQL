# SuperSQL 详细开发计划

> 项目周期：2025/4/29 — 2025/6/16（7 周，Sprint 0-8）
> 团队：5 人 | 技术栈：Java 17 + Maven + ZooKeeper + Thrift + Docker

---

## 全局约定

| 符号 | 含义 |
|---|---|
| P0 | 阻塞后续任务，本 Sprint 必须完成 |
| P1 | 本 Sprint 核心交付，可延至下一 Sprint 首日 |
| P2 | 优化项，不影响主干流程 |
| → | 依赖前置任务 |

---

## Sprint 0：环境与基础框架（W1: 4/29 ~ 5/5）

**里程碑**：ZooKeeper 3 节点集群可连接，Thrift IDL 编译通过，Maven 多模块工程可 `mvn install`，Docker Compose 一键启动所有服务。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S0-01 | 初始化 Maven 多模块工程（父 pom + java-master/java-regionserver/java-client/rpc-proto 子模块） | P0 | 全员 | `mvn install -DskipTests` 通过 |
| S0-02 | 编写 `rpc-proto/supersql.thrift`，运行 `thrift --gen java` 生成所有 stub | P0 | 师东祺 | 生成代码无编译错误，4 套 service 接口齐全 |
| S0-03 | 完成 `docker-compose.yml`（3 ZK + 3 Master + 3 RS + 1 Client） | P0 | 李业 | `docker compose up -d` 后 `docker compose ps` 全部 healthy |
| S0-04 | 验证 miniSQL C++ 编译（在 Dockerfile.regionserver 中） | P0 | 周子安 | 容器内 `/opt/minisql/main` 可执行，`echo "exit;" | /opt/minisql/main` 正常退出 |
| S0-05 | 配置 `.env` 和 `.gitignore` | P1 | 李业 | 敏感配置不提交，`.env` 有完整注释 |
| S0-06 | 建立 ZK 基础目录（启动时由 Master 自动创建 `/masters` `/region_servers` `/meta/tables` `/assignments` `/active-master`） | P1 | 李业 | `zkCli.sh ls /` 可见以上节点 |
| S0-07 | 配置 GitHub Actions CI（编译检查 + `thrift --gen java` 验证） | P2 | 任意 | Push 后 CI 绿灯 |

### 关键技术点

- **Maven 多模块**：父 pom 中 `<modules>` 引入 rpc-proto 为第一个子模块，确保 Thrift 生成代码先于其他模块被编译。推荐用 `maven-thrift-plugin` 或 `exec-maven-plugin` 在 `generate-sources` 阶段自动运行 `thrift`。
- **Curator 依赖**：在父 pom 中统一声明 `curator-recipes:5.5.0`（对应 ZK 3.8），避免各模块版本冲突。
- **ZK Healthcheck**：`bitnami/zookeeper` 镜像内置 `zkServer.sh status`，直接用于 Docker healthcheck。

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| Windows 宿主机 Docker 构建 C++ 速度慢 | 预先在本地用 WSL 编译好 `main` 二进制，通过 `COPY` 直接打入镜像，跳过 C++ 构建阶段 |
| Thrift 版本与 Java 17 不兼容 | 使用 `org.apache.thrift:libthrift:0.20.0`，该版本已适配 Java 17 模块系统 |

---

## Sprint 1：ZooKeeper 集群管理 + Master 高可用（W2: 5/6 ~ 5/12）

**里程碑**：3 个 Master 启动后通过 ZK 选出唯一 Active，Active 故障后 Standby 10 秒内自动接管，防脑裂 epoch 机制验证通过。

**前置**：Sprint 0 全部 P0 任务完成。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S1-01 | 实现 `LeaderElector`：Curator `LeaderLatch` 或手动临时顺序节点选举 | P0 | 徐浩然 | 3 个 Master 进程中只有 1 个输出 "I am Active" |
| S1-02 | 实现 Active Master 心跳写入（每 5s 更新 `/masters/active-heartbeat`） | P0 | 徐浩然 | `zkCli.sh get /masters/active-heartbeat` 时间戳每 5s 变化 |
| S1-03 | 实现 Standby 监听逻辑（Watcher 监听 `/masters`，active 节点消失后重新选举） | P0 | 徐浩然 | `docker stop master-1` 后，master-2 在 15s 内成为 Active |
| S1-04 | 实现防脑裂 epoch 机制（`/active-master` 持久节点，格式 `{epoch, masterId}`） | P1 | 徐浩然 | 旧 Master 恢复后读到更大 epoch，自动降为 Standby |
| S1-05 | 实现 `MasterService` Thrift 服务端框架（`TThreadPoolServer`，端口 8080） | P0 | 徐浩然 | `thrift` 客户端工具可连接 8080 并调用 `getActiveMaster()` |
| S1-06 | 实现 Master HTTP 健康检查端点（`GET /health` → 200，`GET /status` → JSON） | P1 | 徐浩然 | `curl http://localhost:8880/health` 返回 `{"status":"ok","role":"ACTIVE"}` |
| S1-07 | 单元测试：`LeaderElectorTest`（使用 Curator TestingServer 模拟 ZK） | P1 | 徐浩然 | 测试覆盖：正常选举、Active 宕机后重选、脑裂 epoch 防护 |

### ZooKeeper 目录设计

```
/masters/
  master-0000000001  (临时顺序，content: "master-1:8080")
  master-0000000002  (临时顺序，content: "master-2:8080")
/active-master       (持久，content: {"epoch":1,"masterId":"master-1","ts":1234567890})
```

### 关键技术点

```java
// 使用 Curator LeaderLatch（推荐，比手写顺序节点更健壮）
LeaderLatch latch = new LeaderLatch(curatorClient, "/masters", masterId);
latch.addListener(new LeaderLatchListener() {
    @Override public void isLeader() {
        // 1. 递增 epoch 并写入 /active-master
        // 2. 从 ZK 同步最新元数据
        // 3. 启动心跳线程
        becomeActive();
    }
    @Override public void notLeader() {
        becomeStandby();
    }
});
latch.start();
```

**防脑裂关键逻辑**：
```java
// Standby 监听到 active 节点消失后，在重选前检查 epoch
ActiveMasterInfo current = readActiveMaster();
if (current.timestamp > System.currentTimeMillis() - 15_000) {
    // 心跳未超时，可能是网络抖动，延迟 5s 再重试
    Thread.sleep(5000);
    return;
}
// 超时，递增 epoch 并写入自己信息
writeActiveMaster(new ActiveMasterInfo(current.epoch + 1, myId));
```

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| Curator LeaderLatch 在 ZK 会话超时时行为不稳定 | 改用手写临时顺序节点 + Watcher，逻辑更透明，便于调试 |
| epoch 写入与 ZK 节点消失存在竞态 | 使用 `ZooKeeper.setData` + `version` CAS 操作，失败则重试 |

---

## Sprint 2：RegionServer 注册/心跳 + miniSQL 进程管理（W3: 5/13 ~ 5/19）

**里程碑**：RegionServer 启动后成功注册到 ZK，Master 感知 RS 加入/退出，miniSQL 进程可通过 stdin/stdout 执行 SQL 并解析结果。

**前置**：S1-01（Leader 选举）、S1-05（MasterService 框架）。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S2-01 | 实现 `RegionServerRegistrar`：启动时在 `/region_servers/rs-{id}` 创建临时节点，内容含 host/port/tableCount | P0 | 师东祺 | `zkCli.sh ls /region_servers` 显示 3 个节点 |
| S2-02 | 实现 RS 心跳上报（每 10s 更新节点内容，含 qps/cpu/mem 指标） | P1 | 师东祺 | Master 日志打印最新 RS 负载数据 |
| S2-03 | 实现 Master 端 RS Watcher（监听 `/region_servers`，节点增删触发回调） | P0 | 李业 | `docker stop rs-1` 后 Master 日志 10s 内输出 "rs-1 offline" |
| S2-04 | 实现 `MiniSqlProcess`：用 `ProcessBuilder` 启动 miniSQL，通过 stdin 发送 SQL，解析 stdout | P0 | 师东祺 | `process.execute("select * from t1;")` 返回正确行数据 |
| S2-05 | 实现 stdout 解析器（将 miniSQL 文本输出转换为 `QueryResult`） | P0 | 周子安 | 测试 SELECT / INSERT / DELETE / CREATE TABLE / DROP TABLE 各场景的输出解析 |
| S2-06 | 实现进程守护：miniSQL 崩溃时自动重启，记录错误日志 | P1 | 周子安 | 用 `kill -9` 杀死 miniSQL 子进程后，5s 内自动重启 |
| S2-07 | 实现 `RegionAdminService` Thrift 服务端（端口 9090 同端口，使用 Thrift Multiplexed Processor 复用） | P0 | 师东祺 | Master 可通过 RPC 调用 `heartbeat()` 和 `registerRegionServer()` |

### MiniSqlProcess 实现要点

```java
public class MiniSqlProcess {
    private Process process;
    private BufferedWriter stdin;
    private BufferedReader stdout;

    public void start(String dataDir) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("/opt/minisql/main");
        pb.environment().put("MINISQL_DATA_DIR", dataDir);
        pb.redirectErrorStream(false);
        process = pb.start();
        stdin  = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    /**
     * 发送一条 SQL（必须以 ; 结尾），读取输出直到出现提示符 "miniSQL> "
     */
    public synchronized String execute(String sql) throws IOException {
        stdin.write(sql.endsWith(";") ? sql : sql + ";");
        stdin.newLine();
        stdin.flush();
        return readUntilPrompt();  // 读取到 "miniSQL> " 为止
    }
}
```

**注意**：miniSQL 的 `main.cc` 循环等待 stdin 输入，输出格式固定。解析时按行扫描，遇到 `-------` 分隔线标志结果集开始，遇到 `miniSQL> ` 标志本次 SQL 结束。

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| miniSQL stdout 解析因输出格式不规范导致误判 | 修改 miniSQL `interpreter.cc`，在结果集首尾增加机器可读标记（如 `##RESULT_BEGIN##` / `##RESULT_END##`）；或以 JSON 格式输出 |
| ProcessBuilder 在容器内找不到 miniSQL 二进制 | 容器启动脚本中先检查 `$MINISQL_BIN`，不存在则从源码编译 |

---

## Sprint 3：RPC 接口实现 + 客户端与服务端通信（W3-4: 5/13 ~ 5/26）

**里程碑**：客户端可通过 Thrift 向 Master 查询表位置，拿到地址后直连 RegionServer 执行 SQL，完整链路端到端验证通过。

**前置**：S2-04（MiniSqlProcess），S1-05（MasterService 框架）。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S3-01 | 实现 `RegionServiceImpl.execute()`（调 MiniSqlProcess → 封装 QueryResult） | P0 | 师东祺 | `thrift` 客户端工具调用 `execute("t1","select * from t1;")` 返回正确数据 |
| S3-02 | 实现 `MasterServiceImpl.createTable()`（选 RS → 通知 RS 建表 → 写 ZK 元数据） | P0 | 李业 | `createTable("CREATE TABLE t1(id int,primary key(id))")` 成功后 `/meta/tables/t1` 节点可见 |
| S3-03 | 实现 `MasterServiceImpl.getTableLocation()` | P0 | 李业 | 返回正确的 primaryRS 地址和 3 个副本地址 |
| S3-04 | 实现 Client SDK：`SqlClient` REPL 主循环 | P0 | 周子安 | 用户输入 SQL → 路由到正确 RS → 打印结果 |
| S3-05 | 实现 Client `RouteCache`：`ConcurrentHashMap<String, CachedTableLocation>`，TTL=30s，版本号失效 | P1 | 周子安 | 缓存命中时不发起 Master RPC；缓存过期后自动重查 |
| S3-06 | 实现 NOT_LEADER 重定向：Standby Master 收到请求返回 REDIRECT，Client 自动切换 | P1 | 周子安 | `docker stop master-1`（Active）后，Client 自动切换到 master-2 |
| S3-07 | 实现 `MasterServiceImpl.dropTable()`（协调所有副本删除 → 清理 ZK 元数据） | P1 | 李业 | `DROP TABLE t1` 后 `/meta/tables/t1` 节点消失，所有 RS 本地数据删除 |

### Client 路由逻辑

```java
public QueryResult execute(String tableName, String sql) {
    CachedTableLocation loc = cache.get(tableName);
    if (loc == null || loc.isExpired()) {
        loc = fetchFromMaster(tableName);  // Thrift → MasterService.getTableLocation
        cache.put(tableName, loc);
    }
    try {
        return rsClient(loc.primaryRS).execute(tableName, sql);
    } catch (TException e) {
        // 主副本不可达：清除缓存，重查 Master
        cache.invalidate(tableName);
        throw e;
    }
}
```

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| Thrift TThreadPoolServer 在高并发下线程耗尽 | 配置合理线程池大小（maxWorkerThreads=50），或改用 TNonblockingServer（需 TFramedTransport） |
| 客户端连接 RS 失败时无重试逻辑 | 添加指数退避重试（最多 3 次，间隔 200/400/800ms） |

---

## Sprint 4：副本维护（3 副本强一致写）+ WAL（W4: 5/20 ~ 5/26）

**里程碑**：INSERT/DELETE 写操作完成 WAL 记录 + 副本同步，3 副本数据一致性验证通过，RS 重启后通过 WAL 完成 Crash Recovery。

**前置**：S3-01（RegionServiceImpl），S2-04（MiniSqlProcess）。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S4-01 | 设计 WAL 文件格式，实现 `WalManager.append(WalEntry)` | P0 | 周子安 | WAL 文件以二进制 Append 写入，每条含 LSN/TxnID/OpType/AfterRow/Timestamp |
| S4-02 | 实现 `ReplicaSyncServiceImpl.syncLog(entry)`（从副本写本地 WAL → 返回 ACK） | P0 | 徐浩然 | 主副本发送 syncLog 后，从副本 WAL 文件可见对应条目 |
| S4-03 | 实现主副本写路径：`execute()` 写 → WAL(PREPARE) → 并行 syncLog → 等 ACK → COMMIT → 调 miniSQL | P0 | 徐浩然 | INSERT 完成后，3 副本 miniSQL 数据一致（通过各 RS 的 select 验证） |
| S4-04 | 实现从副本 `pullLog` 追赶（从副本落后时主动向主副本拉取 WAL 批量重放） | P1 | 李浩博 | 故意让从副本落后 10 条日志，重启后 10s 内通过 pullLog 追上 |
| S4-05 | 实现 Crash Recovery：RS 重启后读取 WAL，比较 PageLSN，REDO 未落盘条目 | P0 | 李浩博 | 写入 100 条数据后 `kill -9` RS，重启后数据完整 |
| S4-06 | 实现副本超时标记：syncLog 超过 3s 无 ACK → 标记为可疑 → 通知 Master | P1 | 徐浩然 | 人为延迟从副本网络（`tc netem`），主副本日志输出 "replica rs-2 suspected" |
| S4-07 | WAL Checkpoint：每 1000 条日志或 5 分钟触发一次 Checkpoint，清理旧 WAL 文件 | P2 | 李浩博 | WAL 目录大小不无限增长 |

### WAL 文件格式

```
WAL 文件名：wal-{tableName}-{startLSN}.log
每条记录（固定头 + 变长体）：

┌──────────┬──────────┬─────────┬──────────┬─────────────────┬──────────┐
│ LSN(8B)  │TxnID(8B) │TableId  │ OpType   │ Payload length  │ Payload  │
│  long    │  long    │ (4B)int │ (1B)byte │    (4B)int      │ (varlen) │
└──────────┴──────────┴─────────┴──────────┴─────────────────┴──────────┘

Payload = AfterRow bytes（INSERT/UPDATE）或 BeforeRow bytes（DELETE）
```

```java
// WalManager 核心写路径
public synchronized long append(WalEntry entry) throws IOException {
    long lsn = nextLsn.getAndIncrement();
    entry.setLsn(lsn);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(buf);
    dos.writeLong(lsn);
    dos.writeLong(entry.getTxnId());
    dos.writeInt(entry.getTableId());
    dos.writeByte(entry.getOpType().getValue());
    byte[] payload = entry.getAfterRow();
    dos.writeInt(payload.length);
    dos.write(payload);
    walChannel.write(ByteBuffer.wrap(buf.toByteArray()));
    walChannel.force(false);  // fsync，确保落盘
    return lsn;
}
```

### 副本同步协议（半同步）

```
主副本收到写请求：
  1. WalManager.append(entry, PREPARE)   // 本地落盘
  2. 并行发起 syncLog(entry) to [rs-2, rs-3]
  3. CompletableFuture.anyOf(rs2Ack, rs3Ack).get(3, SECONDS)
     → 成功：继续步骤 4
     → 超时：标记超时副本为 SUSPECTED，继续步骤 4（保证可用性）
  4. WalManager.updateStatus(lsn, COMMITTED)
  5. miniSqlProcess.execute(sql)         // 写入本地存储引擎
  6. 返回客户端 OK
  7. 异步通知剩余副本 commitLog(lsn)
```

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| miniSQL 进程执行 SQL 与 WAL 写入不是原子操作，崩溃可能导致 WAL 有记录但 miniSQL 未执行 | Crash Recovery 中 REDO 逻辑：对 WAL 中 COMMITTED 但 miniSQL 未执行的条目重放 |
| syncLog RPC 调用链导致写延迟过高（>100ms） | 将 syncLog 改为异步批量发送（每 10ms 或 100 条打包一次），降低网络往返次数 |

---

## Sprint 5：负载均衡（静态 + 动态）+ Region 迁移（W5: 5/27 ~ 6/2）

**里程碑**：CREATE TABLE 时自动选择最空闲节点，动态重均衡触发 Region 迁移，迁移期间客户端请求自动重试，迁移完成后路由正确更新。

**前置**：S3-02（createTable），S2-03（RS Watcher），S4-03（副本写路径）。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S5-01 | 实现静态分配：`LoadBalancer.selectPrimary()`，选表数最少的 RS 为主副本，另选 2 个不同节点为从副本 | P0 | 李业 | `CREATE TABLE` 后，3 个副本分布在 3 个不同 RS 上 |
| S5-02 | 实现 RS 负载指标收集（每 10s 上报 `tableCount/qps/cpu/mem`） | P0 | 师东祺 | `GET /metrics` 返回各 RS 最新指标 |
| S5-03 | 实现动态重均衡调度器（每 30s 检查，tableCount > avg × 1.5 触发迁移） | P1 | 李业 | 手动增大 rs-1 的表数到 rs-2 的 2 倍，30s 内触发迁移 |
| S5-04 | 实现 `RegionMigrator`：完整 6 步迁移流程（MOVING → 暂停写 → 传输 → 更新路由 → 删源 → ACTIVE） | P0 | 李业 | 迁移完成后，`/assignments/tableName` 更新到目标 RS |
| S5-05 | 实现迁移期间客户端 MOVING 响应处理（收到 MOVING 后等待 500ms 重试，最多 5 次） | P1 | 周子安 | 迁移期间客户端请求最终成功，日志显示 "retry after MOVING" |
| S5-06 | 实现迁移后缓存失效广播（Master 通知所有 RS，RS 通知已连接的 Client） | P1 | 李业 | 迁移完成后，Client 缓存失效，下次请求走新路由 |

### Region 迁移流程（完整实现）

```java
public void migrate(String tableName, String sourceRsId, String targetRsId) {
    // Step 1: 标记 MOVING
    zkClient.setData("/meta/tables/" + tableName, "MOVING");

    // Step 2: 暂停源 RS 写入
    adminClient(sourceRsId).pauseTableWrite(tableName);

    try {
        // Step 3: 流式传输数据文件
        adminClient(sourceRsId).transferTable(tableName,
            rsInfo(targetRsId).host, rsInfo(targetRsId).port);

        // Step 4: 更新路由
        updateAssignment(tableName, targetRsId);    // /assignments/tableName
        updateMetaTable(tableName, targetRsId);     // /meta/tables/tableName → ACTIVE

        // Step 5: 删除源数据
        adminClient(sourceRsId).deleteLocalTable(tableName);

        // Step 6: 广播缓存失效
        broadcastCacheInvalidation(tableName);

    } catch (Exception e) {
        // 回滚：恢复源 RS 写入，清除 MOVING 状态
        adminClient(sourceRsId).resumeTableWrite(tableName);
        zkClient.setData("/meta/tables/" + tableName, "ACTIVE");
        throw e;
    }
}
```

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| 数据传输期间源 RS 崩溃，目标 RS 数据不完整 | 目标 RS 接收完整后写入临时目录，校验 checksum 后原子性 rename；源崩溃则从其他副本重新拷贝 |
| 迁移期间客户端持续收到 MOVING 导致长时间不可用 | 设置最大迁移时间（60s），超时后中止迁移并恢复服务 |

---

## Sprint 6：容错容灾 + 客户端缓存失效（W5-6: 5/27 ~ 6/9）

**里程碑**：单个 RS 宕机后，10s 内系统自动恢复副本数到 3，客户端缓存正确失效，读写请求无永久错误。

**前置**：S4-03（副本写），S5-04（Region 迁移），S2-03（RS Watcher）。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S6-01 | 实现 RS 宕机感知（ZK Watcher 触发 → Master 查找受影响表 → 启动副本恢复） | P0 | 徐浩然 | `docker stop rs-1` 后，Master 日志显示 "recovering replicas for rs-1" |
| S6-02 | 实现副本恢复流程（从存活副本拷贝数据到新 RS → 更新路由 → 恢复 3 副本） | P0 | 徐浩然 | RS-1 宕机后，原本在 RS-1 的表在 RS-2/RS-3 + 新节点上重建 3 副本 |
| S6-03 | 实现客户端缓存失效通知（Master → RS → Client 的推送链路，或 Client ZK Watcher） | P1 | 周子安 | 路由变更后，Client 30s 内（TTL 或推送）感知并更新缓存 |
| S6-04 | 实现读请求故障转移（主副本不可达时，自动切换到从副本读取） | P1 | 徐浩然 | 主副本宕机后，Client 自动切换到从副本，SELECT 继续成功 |
| S6-05 | 实现可疑副本摘除（副本超时被标记 SUSPECTED 后，Master 调度重新创建替换副本） | P2 | 李浩博 | 3s 内无 ACK 的副本被替换为新副本 |
| S6-06 | 集成测试：场景覆盖"1 RS 宕机"、"2 RS 宕机"、"Active Master 宕机" | P0 | 全员 | 所有场景测试通过，系统最终恢复到健康状态 |

### 容灾感知关键代码

```java
// Master 中的 RS Watcher
client.getChildren().usingWatcher((WatchedEvent event) -> {
    if (event.getType() == EventType.NodeDeleted) {
        String rsId = ZKPaths.getNodeFromPath(event.getPath());
        log.warn("RegionServer {} offline, starting recovery", rsId);
        recoveryExecutor.submit(() -> recoverReplicas(rsId));
    }
    // 重新注册 Watcher（单次触发语义）
    registerRsWatcher();
}).forPath("/region_servers");

private void recoverReplicas(String offlineRsId) {
    // 1. 查找所有以 offlineRsId 为主/从副本的表
    List<String> affectedTables = findTablesByRs(offlineRsId);
    // 2. 对每张表，从存活副本拷贝到空闲节点，补全副本数到 3
    for (String table : affectedTables) {
        String targetRs = loadBalancer.selectLeastLoaded(excludeOffline(offlineRsId));
        String sourceRs = findSurvivorReplica(table, offlineRsId);
        regionMigrator.cloneReplica(table, sourceRs, targetRs);
    }
}
```

### 风险与备选

| 风险 | 备选方案 |
|---|---|
| 副本恢复期间再发生第二个 RS 宕机（雪崩场景） | 设置并发恢复限流（最多同时恢复 2 张表），优先恢复副本数为 1 的表 |
| ZK Watcher 注册丢失（会话超时后 Watcher 自动失效） | 所有 Watcher 注册逻辑封装在 `registerXxxWatcher()` 方法中，每次触发后立即重新注册 |

---

## Sprint 7：分布式查询 + 完整测试用例（W6: 6/3 ~ 6/9）

**里程碑**：13 个设计用例全部通过，额外补充混沌测试（节点随机宕机 + 网络分区模拟），测试覆盖率 > 70%。

**前置**：Sprint 3-6 全部 P0 任务。

### 任务清单

| # | 任务 | 优先级 | 负责人 | 验收标准 |
|---|---|---|---|---|
| S7-01 | 实现 `executeBatch`（execfile 语义，顺序执行 SQL 文件中的语句） | P0 | 师东祺 | `execfile test_file1.sql`（1000 条）在容器内执行正确 |
| S7-02 | 实现 CREATE INDEX / DROP INDEX 的分布式传播（Master 协调 3 副本同步执行） | P1 | 师东祺 | 在有 3 副本的表上建索引后，3 个 RS 都有对应索引 |
| S7-03 | 实现设计用例 01-13 的自动化测试脚本 | P0 | 全员 | 所有用例脚本在 `docker compose up` 后可一键运行并全部通过 |
| S7-04 | 补充混沌测试：随机 `docker stop` RS，验证系统自愈 | P1 | 李浩博 | 连续宕机 3 次，每次恢复后数据完整，无永久性错误 |
| S7-05 | 补充混沌测试：网络分区模拟（`iptables` 阻断 RS 间通信），验证副本同步失败后系统行为 | P2 | 李浩博 | 分区期间写入失败或超时，分区恢复后数据最终一致 |
| S7-06 | 补充性能测试：1000 INSERT 的端到端吞吐量和延迟 | P2 | 徐浩然 | P99 延迟 < 500ms（Docker 本地环境） |

### 完整测试用例清单

| 用例 | SQL/操作 | 预期 |
|---|---|---|
| TC-01 | `SELECT * FROM users WHERE id = 1` | 返回 id=1 的行 |
| TC-02 | `SELECT * FROM products WHERE price > 100 AND price < 200` | 返回价格区间内的行 |
| TC-03 | `SELECT * FROM students WHERE name = '张三'` | 返回 char 类型匹配行 |
| TC-04 | `INSERT INTO users VALUES (1, 'Alice', 25)` | 成功；3 副本均可查到 |
| TC-05 | `DELETE FROM users WHERE id = 1` | 成功；3 副本均不可查到 |
| TC-06 | `execfile test.sql`（10 条 SQL） | 依次执行，全部成功 |
| TC-07 | CREATE TABLE 后检查 ZK `/assignments` | 表分配到表数最少的 RS |
| TC-08 | INSERT 后查询 3 个副本 | 3 副本数据完全一致 |
| TC-09 | `docker stop rs-1`，等待恢复后查询 | 10s 内副本恢复，查询成功 |
| TC-10 | `docker stop master-1`（Active），等待 | 15s 内 master-2 成为 Active |
| TC-11 | 迁移表后立即查询 | 缓存失效，路由重定向成功 |
| TC-12 | `DROP TABLE users` | ZK 元数据清理，RS 数据删除 |
| TC-13 | `DROP INDEX idx_name` | 3 副本索引删除成功 |
| TC-14 | 连续随机宕机 rs-1/rs-2，各恢复 | 数据始终完整，系统自愈 |
| TC-15 | `iptables` 阻断 rs-1↔rs-2，写入后恢复 | 分区期间 REPLICA_TIMEOUT，恢复后数据一致 |
| TC-16 | 1000 INSERT 压力测试 | P99 < 500ms，无数据丢失 |

---

## Sprint 8：集成测试 + 演示优化 + 文档（W7: 6/10 ~ 6/16）

**里程碑**：报告定稿，演示视频录制完成，所有测试通过，GitHub 仓库整洁。

### 任务清单

| # | 任务 | 优先级 | 负责人 |
|---|---|---|---|
| S8-01 | 演示脚本编写（覆盖：一键启动 → 建表 → 写入 → 宕机 → 自愈 → 负载均衡）| P0 | 全员 |
| S8-02 | 录制演示视频（≥5 分钟，含旁白）| P0 | 任意 |
| S8-03 | 完善 README（快速启动、架构图、用例截图）| P1 | 周子安 |
| S8-04 | 完善课程报告（终期，覆盖实现细节、测试结果、性能数据）| P0 | 全员 |
| S8-05 | 代码整理（删除 debug 日志、统一代码风格、补充 Javadoc）| P2 | 任意 |
| S8-06 | GitHub Release 打包（`docker compose` 可离线运行的完整包）| P2 | 师东祺 |

---

## 全局依赖关系图

```
Sprint 0 (S0-01~S0-07)
    │
    ├─→ Sprint 1: Leader Election (S1-01~S1-07)
    │       │
    │       └─→ Sprint 2: RS Registration + MiniSQL (S2-01~S2-07)
    │               │
    │               └─→ Sprint 3: RPC End-to-End (S3-01~S3-07)
    │                       │
    │                       ├─→ Sprint 4: WAL + Replica Sync (S4-01~S4-07)
    │                       │       │
    │                       │       └─→ Sprint 6: Fault Tolerance (S6-01~S6-06)
    │                       │
    │                       └─→ Sprint 5: Load Balance + Migration (S5-01~S5-06)
    │                               │
    │                               └─→ Sprint 7: Full Test Suite (S7-01~S7-06)
    │                                       │
    │                                       └─→ Sprint 8: Demo + Docs
    └─→ Sprint 2: MiniSQL Process (S2-04, 可并行)
```

---

## 关键技术选型速查

| 技术点 | 推荐方案 | 注意事项 |
|---|---|---|
| ZK 客户端 | `curator-recipes:5.5.0` | 使用 `RetryNTimes(3, 1000)` 重试策略 |
| 领导者选举 | `LeaderLatch` | 比 `LeaderSelector` 更简单，适合本项目 |
| Thrift 服务端 | `TThreadPoolServer` + `TFramedTransport` | minWorkers=5, maxWorkers=50 |
| Thrift 多服务复用 | `TMultiplexedProcessor` | RegionServer 在同一端口同时提供 RegionService + RegionAdminService + ReplicaSyncService |
| WAL 文件 IO | `FileChannel` + `ByteBuffer`，`force(false)` | 不需要 fdatasync metadata，`force(false)` 足够 |
| miniSQL 进程管理 | `ProcessBuilder` + `synchronized` 方法锁 | miniSQL 是单线程进程，禁止并发调用 `execute()` |
| 副本同步 | `CompletableFuture.anyOf()` 等待 ≥1 ACK | 超时用 `get(3, TimeUnit.SECONDS)` |
| 负载指标 | `OperatingSystemMXBean` (CPU) + `MemoryMXBean` (堆) | 注意 Docker 容器内 CPU 核数识别问题，用 `availableProcessors()` |
| 网络拓扑 | 所有服务通过 hostname 互访（`supersql-net` bridge 网络） | 不要用 `localhost`，用环境变量 `RS_HOST` 等 |
