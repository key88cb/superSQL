# SuperSQL 综合测试手册

本文档介绍了 SuperSQL 存储引擎及核心组件的所有测试方法、指令及其验证标准。

---

## 1. 测试环境准备

在运行任何测试之前，请确保处于 `minisql/cpp-core/` 目录，并关闭所有正在运行的 `main.exe` 实例。

```powershell
cd minisql/cpp-core/
taskkill /F /IM main.exe /T 2>$null
```

---

## 2. 系统级持久化与回归测试 (Manual Regression)

这是验证存储引擎“闭环”能力的最高有效测试，涵盖了数据创建、插入、刷盘及重启恢复。

### 运行指令：
```powershell
# 清理旧数据并运行第一阶段
rm -rf reg_test; $env:MINISQL_DATA_DIR="reg_test"; ./main.exe

# 在 MiniSQL 交互界面依次输入：
create table reg(id int);
insert into reg values(100);
checkpoint;
exit;

# 运行第二阶段（验证持久化）
$env:MINISQL_DATA_DIR="reg_test"; ./main.exe
# 输入：
select * from reg;
exit;
```

### 验证标准：
- 看到 `REDO Recovery Complete up to LSN: X`。
- `select` 结果正确显示 `100`。
- `reg_test/database/catalog/` 目录下存在 `catalog_file`。

---

## 3. C++ Core 单元测试 (Unit Tests)

这些测试位于 `tests/` 目录下，用于验证各底层模块的逻辑正确性。

### 编译与运行通用模板：
```powershell
# 编译指令模板 (需链接依赖的所有 .cc 文件)
g++ -o [测试程序名] -g -Wall tests/[源文件名].cc record_manager.cc buffer_manager.cc catalog_manager.cc index_manager.cc basic.cc log_manager.cc api.cc

# 运行指令
mkdir [测试目录]; $env:MINISQL_DATA_DIR="[测试目录]"; ./[测试程序名]
```

### 主要测试程序编译参考：

```powershell
# Buffer Manager 测试
g++ -o test_buffer -g -Wall tests/test_buffer_manager.cc record_manager.cc buffer_manager.cc catalog_manager.cc index_manager.cc basic.cc log_manager.cc api.cc

# WAL 故障恢复测试
g++ -o test_recovery -g -Wall tests/test_wal_crash_recovery.cc record_manager.cc buffer_manager.cc catalog_manager.cc index_manager.cc basic.cc log_manager.cc api.cc
```

### 主要测试用例列表：

| 源文件名 | 测试目标 |
| :--- | :--- |
| `test_buffer_manager.cc` | 验证页面读取、Pin/Unpin 计数以及 LRU/Clock 替换策略。 |
| `test_record_manager.cc` | 验证堆表的记录插入、定长字符处理及磁盘块管理。 |
| `test_wal.cc` | 验证日志的追加、刷盘（Fsync）及 LSN 的单调性。 |
| `test_wal_crash_recovery.cc` | **核心**：模拟系统崩溃，验证 redo 逻辑是否能回放已提交事务。 |
| `test_catalog_manager.cc` | 验证表元数据的存储、列定义以及表名查找。 |
| `test_index_manager.cc` | 验证 B+ 树索引的创建、查找和范围扫描。 |
| `test_pin_count.cc` | 专门针对高并发缓冲区死锁进行的 Pin 压力测试。 |

---

## 4. 稳定性与压力测试 (Stress Testing)

### 穷举测试 (Exhaustive Test)
该测试通过大量的随机数据插入和查询，模拟存储引擎在极端情况下的表现。

#### 指令：
```powershell
g++ -o test_exh -g -Wall tests/test_exhaustive.cc record_manager.cc buffer_manager.cc catalog_manager.cc index_manager.cc basic.cc log_manager.cc api.cc
mkdir exh_db; $env:MINISQL_DATA_DIR="exh_db"; ./test_exh
```

#### 验证标准：
- 程序在运行数千次操作后不崩溃。
- 查询结果与预期一致（无数据损坏）。

---

## 5. Java 侧集成测试 (Integration)

在分布式环境中，可以使用 Maven 运行 Java 侧的验证逻辑。

### 5.1 测试框架统一约定（必须遵循）

- 所有需要内嵌 ZooKeeper 的 Java 测试，必须通过共享工具类创建实例：
	- `test-common/src/main/java/edu/zju/supersql/testutil/EmbeddedZkServerFactory.java`
- 测试代码应使用 `EmbeddedZkServer`（test-common 提供的包装类型），不要在模块测试中直接声明 `TestingServer` 字段类型。
- 禁止在测试里直接写 `new TestingServer(true)`。
- 原因：共享工厂已统一设置 `maxCnxns` 与 `maxClientCnxns`，用于消除 CI 常见告警：
	- `[zkservermainrunner] WARN  o.a.z.server.ServerCnxnFactory - maxCnxns is not configured, using default value 0.`
- 依赖关系要求：
	- `java-master` / `java-regionserver` / `java-client` 的测试依赖 `test-common`，不要在各模块重复实现同名工厂类。
	- 同时这三个模块都应显式声明 `org.apache.curator:curator-test`（`test` scope），避免单模块执行或 IDE 直跑测试时出现 `NoClassDefFoundError: org/apache/curator/test/InstanceSpec`。

### 推荐执行顺序（仓库根目录）

```powershell
# 全模块测试（推荐）
mvn test -DskipTests=false
```

```powershell
# 只跑 Master 测试
mvn -pl java-master test

# 只跑 RegionServer 测试
mvn -pl java-regionserver test

# 只跑 Client 测试
mvn -pl java-client test
```

### 可按类定向执行

```powershell
# Master 元数据集成测试（内嵌 ZooKeeper）
mvn -pl java-master -Dtest=MasterServiceMetadataIntegrationTest test

# RegionServer 注册器集成测试（内嵌 ZooKeeper）
mvn -pl java-regionserver -Dtest=RegionServerRegistrarIntegrationTest test

# Client 路由与 active-master 解析测试
mvn -pl java-client -Dtest=SqlClientRoutingTest test
```

### 当前 Java 侧重点用例

| 测试类 | 覆盖目标 |
| :--- | :--- |
| `LeaderElectorTest` | LeaderLatch 选主唯一性、主节点退出后的自动接管、epoch 递增与已有 epoch 延续 |
| `MasterServiceMetadataIntegrationTest` | create/get/list/drop 元数据链路、NOT_LEADER 重定向、无 RS 分支 |
| `MasterHeartbeatIntegrationTest` | Active Master 心跳写入 `/masters/active-heartbeat` 及非主不覆盖行为 |
| `MasterServerHttpPayloadTest` | Master `/health` `/status` JSON 载荷字段与角色信息 |
| `RegionServerRegistrarIntegrationTest` | RS 注册、心跳更新、节点丢失后的重注册 |
| `ReplicaSyncServiceImplTest` | syncLog/pullLog/getMaxLsn/commitLog 路径及边界 |
| `SqlClientRoutingTest` | SQL 分类、表名提取、TTL 过期、active-master 解析回退 |

### 旧示例（仍可用）：
```powershell
cd java-regionserver
mvn test
```

# 6. 分布式混沌测试 (Chaos Testing)
 
混沌测试用于验证系统在分布式故障（主节点宕机、RS 崩溃、网络分区）下的鲁棒性。
 
### 脚本位置：
`scripts/chaos_test.sh`
 
### 运行前提：
- 集群已通过 `docker compose up -d` 启动并处于 Healthy 状态。
- 环境具备 `bash` 和 `curl`。
 
### 执行指令：
```bash
# 运行全量测试
chmod +x scripts/chaos_test.sh
./scripts/chaos_test.sh all
 
# 仅测试 Master 选主切换
./scripts/chaos_test.sh master_failover
 
# 仅测试 RS 宕机恢复与数据一致性
./scripts/chaos_test.sh rs_crash

# 仅测试 S7-04 随机 RS 宕机混沌场景
./scripts/chaos_test.sh random_rs_crash

# 仅测试 S7-05 基础版网络分区场景
./scripts/chaos_test.sh network_partition

# 自定义随机宕机轮数与等待时间
CHAOS_RANDOM_ROUNDS=5 \
CHAOS_STOP_WAIT_SECONDS=10 \
CHAOS_RECOVERY_WAIT_SECONDS=30 \
./scripts/chaos_test.sh random_rs_crash

# 自定义网络分区持续时间，并把结果归档到指定目录
CHAOS_PARTITION_DURATION_SECONDS=15 \
CHAOS_LOG_DIR=artifacts/chaos \
./scripts/chaos_test.sh network_partition
```
 
### 验证项：
1. **Master Failover**：停止 Active Master 后，Standby 节点应在 20s 内接管。
2. **RS Crash Recovery**：
   - 即使其中一个副本宕机，查询仍应能从剩余副本成功返回。
   - 宕机节点重启后，应能通过 WAL 回放恢复丢失的数据。
3. **数据完整性**：对比故障前后的 `SELECT` 结果，确保无数据丢失。

### S7-04 随机宕机混沌测试说明

该场景用于覆盖开发计划中的 `S7-04`：

- 随机选择 `rs-1 / rs-2 / rs-3` 中的一个节点执行 `docker stop`
- 故障期间持续执行 `SELECT` 与 `INSERT`
- 重启故障节点并等待恢复
- 校验故障期间写入的数据在恢复后仍可读
- 默认执行 3 轮，可通过 `CHAOS_RANDOM_ROUNDS` 调整

脚本实现位置：

- [scripts/chaos_test.sh](C:/Users/Lenovo/Desktop/新建文件夹/superSQL/scripts/chaos_test.sh)

默认验收标准：

1. 任一轮随机停机期间，种子数据 `seed` 仍可被查询到。
2. 任一轮随机停机期间，新的 `INSERT` 请求仍能成功返回。
3. 故障节点恢复 healthy 后，故障期间写入的记录仍可通过客户端查询到。
4. 连续多轮执行后，脚本退出码为 `0`。

推荐执行前检查：

```bash
docker compose up -d
docker compose ps
curl -sf http://localhost:8880/health
curl -sf http://localhost:8881/health
curl -sf http://localhost:8882/health
```

推荐执行后补充观察：

```bash
docker logs --tail=200 master-1
docker logs --tail=200 master-2
docker logs --tail=200 master-3
docker logs --tail=200 rs-1
docker logs --tail=200 rs-2
docker logs --tail=200 rs-3
```

### S7-04 验收记录模板

可直接在测试报告中填写如下记录：

| 字段 | 内容 |
| :--- | :--- |
| 测试日期 |  |
| 测试人 |  |
| 脚本命令 | `CHAOS_RANDOM_ROUNDS=3 ./scripts/chaos_test.sh random_rs_crash` |
| 集群版本/提交 |  |
| 轮数 |  |
| 实际故障节点序列 |  |
| 结果 | 通过 / 失败 |
| 失败轮次 |  |
| 现象摘要 |  |
| 日志位置 |  |

建议在“现象摘要”中至少记录：

- 每轮被停止的 RS
- 故障期间 `SELECT` 是否成功
- 故障期间 `INSERT` 是否成功
- 重启后数据是否仍可查询

### S7-05 基础版网络分区混沌测试说明

该场景用于先完成 `S7-05` 的基础版验证，重点不是证明最终一致性协议已经完全收敛，而是先把注入方式、观测指标和结果归档跑通：

- 对 `rs-1` 与 `rs-2` 注入双向网络阻断
- 分区期间执行 `INSERT`
- 采集 `rs-1` / `rs-2` 的 `/status` 结果并归档到日志文件
- 重点观察 `transport_error`、`suspectedReplica*`、`replicaCommitRetry` 等字段
- 分区恢复后再次查询，观察数据是否追赶成功、是否有明显脏状态

脚本执行后的归档日志默认落在：

- `artifacts/chaos/`

基础版验收标准：

1. 能稳定注入并恢复 `rs-1` 与 `rs-2` 的网络分区。
2. 分区期间的 SQL 返回结果被完整记录到 chaos 日志。
3. 至少成功抓取一份包含 `replicaCommitRetry` 的 `/status` 输出。
4. 恢复后种子数据仍可查询。
5. 如果分区期间写入返回成功，则恢复后该行应最终可查询；如果返回失败或超时，则允许只做现象记录。

建议在测试记录中额外填写：

- 分区注入开始时间 / 恢复时间
- 分区期间写入返回值
- `suspectedReplicaCount` 是否大于 0
- `transport_error` 是否出现
- 恢复后数据快照
