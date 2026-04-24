# SuperSQL 测试状态与 Bug 追踪

**更新时间：2026-04-24**
**维护者：测试 agent（详见 `scripts/run_tests.sh` 与 `docs/TEST_PLAN.md`）**

本文档是 SuperSQL 全量测试的**唯一状态看板**：

- 测试套件的最新执行状态（通过 / 失败 / 跳过 / 未执行）
- 发现的 bug 清单（含定位信息与修复状态）
- 每轮测试产物（日志、surefire 报告、混沌快照）的归档路径

> **规则**：agent 每完成一轮测试必须更新本文档的「最近一次执行结果」与「Bug 清单」两个章节，不得追加无效信息（例如执行花费时间、命令原样日志等均写到 `artifacts/` 下的附件中）。

---

## 1. 环境基线（2026-04-24 摸底）

| 项目 | 状态 | 备注 |
| --- | --- | --- |
| macOS Darwin 25.3.0 | ✅ | 开发机 |
| `g++` (aliased → g++-14) / `/usr/bin/g++` | ✅ | `make main` 可编译 |
| Java 21 (运行) + JDK 17 (编译目标) | ✅ | `mvn` OK，`pom.xml` 要求 Java 17 |
| Maven 3.9.9 | ✅ | 中央仓偶发 TLS 瞬时握手失败，重试即可 |
| Docker 28.4.0 | ✅ | `docker compose ps` 全 healthy（3 ZK + 3 Master + 3 RS + 1 Client） |
| `thrift` CLI | ❌ | 未安装；仅在修改 `supersql.thrift` 时需要，测试可跳过 |
| `pwsh` | ❌ | `scripts/check-unfinished-doc.ps1` 无法直接运行，相关校验可在 CI 跑 |

## 2. 已有测试盘点

### 2.1 C++ 存储引擎（`minisql/cpp-core/`）

| Target | 文件 | 说明 |
| --- | --- | --- |
| `test_basic` | `tests/test_basic.cc` | Data/Tuple/Table 基本类型 |
| `test_buffer_manager` | `tests/test_buffer_manager.cc` | 4KB 页、clock 替换、pin/unpin |
| `test_catalog_manager` | `tests/test_catalog_manager.cc` | 表/索引元数据持久化 |
| `test_index_manager` | `tests/test_index_manager.cc` | B+ 树 CRUD / range |
| `test_record_manager` | `tests/test_record_manager.cc` | heap file 插删查 |
| `test_api` | `tests/test_api.cc` | SQL 通过 API 层的端到端路径 |
| `test_exhaustive` | `tests/test_exhaustive.cc` | 4000+ 记录压力 |
| `test_wal` | `tests/test_wal.cc` | WAL append + fsync + LSN 单调 |
| `test_wal_crash_recovery` | `tests/test_wal_crash_recovery.cc` | 崩溃 redo |
| `test_pin_count` | `tests/test_pin_count.cc` | Pin 压力，需手工编译 |
| `unit_test_for_basic` | `unit_test/unit_test_for_basic.cc` | 补充单测 |
| `unit_test_for_buffer_manager` | `unit_test/unit_test_for_buffer_manager.cc` | 补充单测 |
| `unit_test_for_record_manager` | `unit_test/unit_test_for_record_manager.cc` | 补充单测 |
| REPL 脚本 | `test_file.sql`（1w 行）/ `test_file1.sql`（1k 行） | `execfile` 回归 |

Makefile 入口：`make test` 串行跑除 `test_pin_count` 外的 9 个 target 并收 gcov；`make coverage` 触发带覆盖率的全跑。

**摸底执行**：`test_basic` 本机通过（21/21 PASSED）。其它测试未在本轮手动执行；作为 baseline 视为「待首轮验证」。

### 2.2 Java 分布式层（Maven 模块）

| 模块 | 测试文件数 | 典型场景 |
| --- | --- | --- |
| `java-master` | 15 | 选主 flapping、meta 管理、Rebalance 调度、Region DDL 转发、HTTP payload |
| `java-regionserver` | 14 | WAL、WriteGuard、ReplicaManager、RegionAdmin / RegionService / ReplicaSync RPC |
| `java-client` | 15 | 路由缓存、重定向、DML 重试、度量 HTTP 导出、`execfile` 路径 |
| `test-common` | 0（仅工具） | `EmbeddedZkServerFactory` 共享 Curator TestingServer |
| `rpc-proto` | 0 | 仅 Thrift 生成 |

共 46 个 Java 测试类。Curator TestingServer（`curator-test`）在每个 `*IntegrationTest` 里启动嵌入式 ZK，无需依赖本地 docker。

**摸底执行**：`mvn -pl java-client test -Dtest=ClientConfigTest` BUILD SUCCESS。全量 `mvn test` 未在本轮运行，状态记为「待首轮验证」。

### 2.3 集群级别脚本

| 脚本 | 说明 |
| --- | --- |
| `scripts/chaos_test.sh` | 5 个场景：`master_failover` / `rs_crash` / `random_rs_crash` / `network_partition` / `suspected_replica`；依赖 `docker exec iptables` 注入分区 |
| `scripts/mvn-module-test.ps1` | Windows 环境下的 `mvn -pl` 封装 |
| `scripts/check-unfinished-doc.ps1` | 校验 `UNFINISHED_FEATURES_IMPLEMENTATION_GUIDE.md` 一致性 |

> 注：新增统一入口 `scripts/run_tests.sh` 见 §4。

### 2.4 CI

`.github/workflows` 目前仅跑 `mvn -B -ntp test` 与 `make test`（C++）。不覆盖 docker 集群与 chaos 场景。

---

## 3. 最近一次执行结果

> 每轮执行后由 agent 覆盖此表。字段含义：
> - `套件`：与 §4 的套件 ID 一致
> - `结果`：`PASS` / `FAIL` / `SKIP` / `N/A`
> - `耗时`：秒
> - `产物`：相对仓库根的日志/报告路径

| 轮次 | 时间 (UTC+8) | 套件 | 结果 | 耗时 (s) | 失败用例 | 产物 |
| --- | --- | --- | --- | --- | --- | --- |
| R0 (baseline) | 2026-04-24 15:55 | `cpp.test_basic` | PASS | 1 | — | — |
| R0 (baseline) | 2026-04-24 16:05 | `java.client.ClientConfigTest` | PASS | ~20 | — | — |
| R0 (baseline) | 2026-04-24 16:55 | `cluster.smoke` | FAIL | — | SQL CREATE→INSERT→SELECT 闭环未通过，详见 `BUG-20260424-01` | 见 docker logs |

> R1 起由 agent 按计划填写。若同一套件出现两轮以上回归失败，在 §5 开新 bug。

---

## 4. 套件编号对照

测试套件以「层 × 关注点」命名，与 `scripts/run_tests.sh` 的 subcommand 一一对应：

| 套件 ID | 入口 | 描述 |
| --- | --- | --- |
| `cpp.unit` | `scripts/run_tests.sh cpp unit` | C++ 所有 `test_*` + `unit_test_*`（不含 exhaustive / WAL crash） |
| `cpp.stress` | `scripts/run_tests.sh cpp stress` | `test_exhaustive` + `test_pin_count` + REPL `execfile test_file.sql` |
| `cpp.wal` | `scripts/run_tests.sh cpp wal` | `test_wal` + `test_wal_crash_recovery` + 手动持久化回归 |
| `cpp.coverage` | `scripts/run_tests.sh cpp coverage` | `make coverage`，产物 `*.gcov` |
| `java.unit` | `scripts/run_tests.sh java unit` | 所有模块的 `*Test`（非 `*IntegrationTest`） |
| `java.integration` | `scripts/run_tests.sh java integration` | 所有 `*IntegrationTest`（内嵌 ZK） |
| `java.full` | `scripts/run_tests.sh java full` | `mvn -B -ntp test`（单机全量） |
| `cluster.smoke` | `scripts/run_tests.sh cluster smoke` | `docker compose ps` + 基本 CREATE/INSERT/SELECT |
| `cluster.chaos` | `scripts/run_tests.sh cluster chaos` | `chaos_test.sh all` |
| `cluster.stress` | `scripts/run_tests.sh cluster stress` | 并发写入 + 大表导入（见 §4.5） |
| `zk.topology` | `scripts/run_tests.sh zk topology` | `/masters` `/active-master` `/region_servers` `/meta/tables` 结构断言 |
| `e2e.client` | `scripts/run_tests.sh e2e client` | 通过 docker client 执行 `.sql` 脚本，对照期望输出 |

### 4.1 套件执行约定

- 每个套件必须把输出写到 `artifacts/<suite>/<timestamp>/` 下，并将关键结果写回本文件 §3。
- 失败时必须同步更新 §5 的 bug 清单，并附 artifact 路径。
- 若套件尚无入口脚本，本文记为「TODO」并在 §6 跟进。

---

## 5. Bug 清单

> 新 bug 按发现时间倒序追加；修复后将 `状态` 置 `FIXED`，保留历史。
>
> **字段说明**：
> - `ID`：`BUG-YYYYMMDD-NN`
> - `位置`：`文件#行` 或「跨模块」
> - `复现`：单行命令，尽量最小化
> - `根因` / `修复动作`：最多两行

| ID | 严重度 | 状态 | 位置 | 现象 | 复现 | 根因 / 修复动作 | artifacts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BUG-20260424-01 | **P0** | OPEN | 跨模块（client → RS `RegionServiceImpl#executeWrite`） | 新建表后客户端 INSERT 立即触发 RS 侧 `Insufficient replica targets: required=1, available=0`；CREATE TABLE 响应被客户端翻译为 `Error [ERROR]: Failed to create table on replica rs-3: Insufficient replica targets...` 的 CREATE 也失败 | `printf "CREATE TABLE t1(id int, primary key(id));\nexit;\n" \| docker exec -i client java -jar /app/app.jar` | 待定位：疑似 CREATE 通过了 `executeWrite` 写路径，而此时 `/supersql/assignments/<table>` 尚未写入导致 `getReplicaAddresses` 返回空；或 `minReplicaAcks=1` 要求至少 1 个 peer 但 RS 只看到自己。需要确认：① master-1 日志是否出现 `createTable` 编排；② `/supersql/assignments/` 是否生成；③ `minReplicaAcks` 配置值与副本数目关系。 | `docker logs rs-3 --since 1h \| grep insufficient` |

### 5.1 已知待验证项（摘自 `UNFINISHED_FEATURES_IMPLEMENTATION_GUIDE.md`）

以下为项目已自述「未完成」的条目，**agent 不可当作 bug 登记**，而要作为专项测试场景补齐数据：

1. Master 选主与脑裂防护缺乏长时分区 / 抖动混沌验证。
2. RegionServer 副本追赶与迁移缺极端网络故障压测结论。
3. Client 路由指标尚未接入统一监控，长期趋势缺失。

若在测试中观察到这三类场景失败，按「复现可重复 + 触发条件明确」的标准升级为 §5 的正式 bug。

---

## 6. TODO / 未实现套件入口

- `cluster.stress`：当前没有官方压测脚本；首轮执行前需要 agent 新增并落盘到 `scripts/stress/cluster_stress.sh`。
- `zk.topology`：需要新增 `scripts/zk/assert_topology.sh`，使用 `docker exec zk1 zkCli.sh`。
- `e2e.client`：需要新增 `scripts/e2e/run_sql_fixtures.sh`，固定读入 `scripts/e2e/fixtures/*.sql`。

agent 首次运行这些套件时若脚本缺失，必须先补齐脚本（含幂等、清理、超时），再登记到 §4。

---

## 7. 产物归档规则

```
artifacts/
  <suite>/
    <YYYYMMDD-HHMMSS>/
      stdout.log
      stderr.log
      surefire-reports/   # Java 专用
      coverage/           # C++ 专用
      chaos_snapshot/     # 集群专用
      summary.json        # 套件级汇总（状态、耗时、失败用例）
```

`summary.json` schema：

```json
{
  "suite": "java.integration",
  "startedAt": "2026-04-24T16:10:00+08:00",
  "finishedAt": "2026-04-24T16:18:43+08:00",
  "result": "FAIL",
  "total": 23,
  "passed": 21,
  "failed": 2,
  "skipped": 0,
  "failures": [
    { "name": "...", "stack": "..." }
  ]
}
```

agent 每轮结束时把新出现的失败自动补登到 §5。
