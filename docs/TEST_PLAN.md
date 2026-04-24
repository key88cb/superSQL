# SuperSQL 测试总体规划

**更新时间：2026-04-24**

本文档给出 SuperSQL 从「单机存储引擎」到「分布式集群」全链路的测试计划，回答三个问题：

1. **要测什么？**（分层 + 关注点 + 验收标准）
2. **怎么测？**（套件入口 + 工具链 + 产物规范）
3. **谁来跑？**（本地 / CI / 长时 agent，各自边界）

状态看板与 Bug 清单见：[`docs/TEST_STATUS.md`](./TEST_STATUS.md)。

---

## 1. 第一性原理

- **每个测试必须对应一个明确的失败假设**（「如果 X 坏了，这个测试会红」）。没有假设的冒烟测试不登记。
- **快失败优于重试堆叠**：单个命令重试 ≤ 3，不用 sleep 等待掩盖问题；超时≥预期两倍即判 FAIL。
- **产物优先于日志**：每个套件必须产出结构化 `summary.json`，stdout 仅作附件。
- **状态收敛在一处**：`docs/TEST_STATUS.md` 是唯一事实源，Bug 只登记于 §5；ad-hoc 笔记禁止散落。
- **修根因不改测试**：如果测试失败由设计缺陷引起，去修代码而不是放宽断言；确实需要放宽的在 Bug 表中标注 `降级(known-issue)`。

---

## 2. 测试分层

```
┌─────────────────────────────────────────────────────────┐
│ L5 End-to-End     e2e.client (SQL 脚本经 docker client)│
├─────────────────────────────────────────────────────────┤
│ L4 Chaos          cluster.chaos (故障注入)              │
├─────────────────────────────────────────────────────────┤
│ L3 Cluster        cluster.smoke / cluster.stress        │
│                   zk.topology                           │
├─────────────────────────────────────────────────────────┤
│ L2 Module Int.    java.integration (嵌入 ZK)            │
├─────────────────────────────────────────────────────────┤
│ L1 Unit           java.unit + cpp.unit                  │
├─────────────────────────────────────────────────────────┤
│ L0 Engine Core    cpp.wal / cpp.stress / cpp.coverage   │
└─────────────────────────────────────────────────────────┘
```

上层失败时首先回查下层。agent 运行顺序必须自底向上（见 §5）。

---

## 3. 分层详细计划

### 3.1 L0 — MiniSQL 存储引擎（C++）

| 目标 | 套件 | 失败假设 |
| --- | --- | --- |
| 基本类型编解码 | `cpp.unit/test_basic` | `Data` / `Attribute` / `Tuple` 在大小端、char(N) 截断上出错 |
| 缓冲池 | `cpp.unit/test_buffer_manager` + `test_pin_count` | 512 帧满 → pin/unpin 失衡导致死锁 |
| 目录元数据 | `cpp.unit/test_catalog_manager` | catalog 文本编码破坏、字段上限（32 列/10 索引）越界 |
| 索引 | `cpp.unit/test_index_manager` | B+ 树分裂/合并、叶子链断开导致 range 丢数据 |
| 记录 | `cpp.unit/test_record_manager` | 软删除标记失效、定长与变长混存 |
| API / SQL | `cpp.unit/test_api` | 多条件 AND/OR（`unionTable`/`joinTable`）结果错 |
| 压力 | `cpp.stress/test_exhaustive` + `execfile test_file.sql` | 4000+ / 10000+ 行规模下页淘汰错误 |
| WAL | `cpp.wal/test_wal` + `test_wal_crash_recovery` | `FileChannel.force` 失效、redo 不幂等 |
| 覆盖率 | `cpp.coverage` | 核心模块覆盖低于基线 |

**验收**：`make test` 全绿；`cpp.coverage` 输出的 `*.gcov` 存档；`test_pin_count` 在 200 次 pin 抖动下不死锁。

### 3.2 L1 — Java 模块单测

无 ZK 依赖，纯内存。按模块分：

| 模块 | 关键用例 | 失败假设 |
| --- | --- | --- |
| `java-master` | `MasterConfigTest`, `LoadBalancerTest`, `RebalanceSchedulerTest`, `MasterServerHttpPayloadTest` | 配置解析错误、负载权重算错、HTTP payload 漏字段 |
| `java-regionserver` | `MiniSqlProcessTest`, `OutputParserTest`, `WriteGuardTest`, `WalManagerTest`, `ReplicaManagerTest` | MiniSQL 子进程启动/解析错、WAL LSN 倒退、写冻结未生效 |
| `java-client` | `ClientConfigTest`, `RouteCacheAndDiscoveryTest`, `SqlClientDmlRetryTest`, `SqlClientRoutingTest`, `ClientMetricsHttpServerTest` | 路由缓存失效、DML 重试放大、metrics endpoint 挂 |

**套件入口**：`scripts/run_tests.sh java unit` → `mvn -q -B -ntp test -Dtest='!*IntegrationTest'`。

**验收**：每模块 surefire 报告 0 failure；`target/surefire-reports` 存档。

### 3.3 L2 — Java 模块集成测（嵌入 ZK）

`*IntegrationTest` 使用 `EmbeddedZkServerFactory`。覆盖：

- `LeaderElectorTest`：选主 flapping、`epoch` 单调。
- `MetaManagerIntegrationTest`：`/meta/tables` 持久化与版本号。
- `RegionServerRegistrarIntegrationTest` / `RegionServerWatcherIntegrationTest`：心跳、上下线 Watcher。
- `MasterRegionDdlForwardingIntegrationTest` / `RegionAdminServiceAssignmentIntegrationTest`：Master ↔ RS 的 DDL / 迁移链路。
- `RouteInvalidationWatcherIntegrationTest`：客户端路由失效订阅。

**套件入口**：`scripts/run_tests.sh java integration` → `mvn -q -B -ntp test -Dtest='*IntegrationTest'`。

**验收**：0 failure；平均单测 < 30s；嵌入 ZK 端口冲突需自动退避。

### 3.4 L3 — 集群级别

前置：`docker compose up -d` 且 `cluster.smoke` 通过。

#### 3.4.1 `cluster.smoke`（≤ 60s）

- `docker compose ps` 全 healthy。
- Client 执行 `CREATE TABLE t(id int, primary key(id))` → `INSERT` → `SELECT` → `DROP` 闭环。
- 读取每个 Master `/status` 返回 `ACTIVE`/`STANDBY` 各存在。
- 读取每个 RS `/status` 含 `suspectedReplicaCount` 字段。

#### 3.4.2 `zk.topology`

- `docker exec zk1 zkCli.sh` 依次断言：`/masters` 顺序子节点数 == 3；`/active-master` 存在且 JSON 含 `epoch`；`/region_servers` 子节点数 == 3；每张 `/meta/tables/<name>` 含 `assignments`。

#### 3.4.3 `cluster.stress`

- 并发：N 个 client 协程，每个循环 INSERT 1000 行（自增 id），目标吞吐 > 200 rows/s。
- 大表：导入 10 万行，期间每 10 秒查询行数必须单调递增。
- 资源：导入结束后 buffer pool 命中率 > 80%（从 RS `/status` 的 `bufferHit` 读取，缺失则记录为观察项）。
- 失败判据：TPS 波动 > 50%、出现主动 Master 切换、任一 RS 被标记 suspected。

### 3.5 L4 — 混沌（`cluster.chaos`）

复用 `scripts/chaos_test.sh`，扩展场景目录 `scripts/chaos/scenarios.d/*.sh`（首次运行时 agent 可按需新增）：

| 场景 | 注入 | 验收 |
| --- | --- | --- |
| Master failover | `docker stop master-ACTIVE` | `/active-master.epoch` 单调递增；新主唯一 |
| RS 固定宕机 | `docker kill rs-1` | 读可用；写成功；重启后可读到期间写入 |
| RS 随机宕机 ×3 轮 | 每轮挑一台 | 同上 |
| 网络分区 | `iptables` 注入 | `suspectedReplicaCount > 0` 或 `commit_transport_error` 可观察；恢复后 seed 行仍在 |
| 可疑副本 | 分区 + 恢复 | `suspectedReplicaRecoveredCount > 0` |
| WAL 抖动（新增） | `docker exec rs-1 sh -c 'dd if=/dev/zero of=/tmp/fill bs=1M count=300'` 填满磁盘 | 写冻结；清理后恢复 |
| 多次反复切主（新增） | 轮流 stop/start 3 个 master 5 轮 | 仍然单主、元数据未损坏 |

### 3.6 L5 — 端到端（`e2e.client`）

- `scripts/e2e/fixtures/*.sql`：覆盖 DDL + DML + 索引 + 事务边界。
- 每条 fixture 附 `*.expected`（只校验关键行数/关键行）。
- 通过 `docker exec -i client java -jar /app/app.jar < fixture.sql` 执行，用 `diff --unified` 比对。

---

## 4. 产物与状态协议

见 `docs/TEST_STATUS.md` §7。核心要求：

- 每个套件必须写 `artifacts/<suite>/<timestamp>/summary.json`。
- agent 必须在**每次套件结束后立刻**把状态同步到 `TEST_STATUS.md` §3 / §5（不得等全部跑完再批处理）。
- `summary.json` 只含结构化数据；日志明细留在 `stdout.log` / `stderr.log`。

---

## 5. 执行顺序（供 agent）

下列顺序是强制的。前一层全绿才允许启动下一层；失败套件须在当前层重跑一次（至多一次）确认不是 flaky 后登记 Bug。

```
cpp.unit         ──►  cpp.wal  ──►  cpp.stress  ──►  cpp.coverage
     │
     ▼
java.unit        ──►  java.integration
     │
     ▼
cluster.smoke    ──►  zk.topology  ──►  cluster.stress
     │
     ▼
cluster.chaos    ──►  e2e.client
```

- `cpp.coverage` 与 `java.full` 可并行，但都要求其前置层绿。
- `cluster.chaos` 必须在 `cluster.smoke` 通过后才能跑，且跑完后**必须重新跑一次 `cluster.smoke`** 确认集群恢复。

---

## 6. 非目标（本计划不覆盖）

- 与外部监控系统（Prometheus/Grafana）的对接，属于上层运维。
- 纯性能 benchmark 的长期趋势聚合。
- 对 MiniSQL C++ 层的重构/新功能开发。

这些在 `docs/UNFINISHED_FEATURES_IMPLEMENTATION_GUIDE.md` 跟踪，不进 Bug 表。
