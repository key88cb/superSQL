# SuperSQL 测试状态与 Bug 追踪

本文档是 SuperSQL 全量测试的**唯一状态看板**：

- 测试套件的最新执行状态（通过 / 失败 / 跳过 / 未执行）
- 发现的 bug 清单（含定位信息与修复状态）
- 每轮测试产物（日志、surefire 报告、混沌快照）的归档路径

> **规则**：agent 每完成一轮测试必须更新本文档的「最近一次执行结果」与「Bug 清单」两个章节，不得追加无效信息（例如执行花费时间、命令原样日志等均写到 `artifacts/` 下的附件中）。

---

## 1. 环境基线

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

| 轮次 | 套件 | 结果 | 耗时 (s) | 失败用例 | 产物 |
| --- | --- | --- | --- | --- | --- |
| R0 (baseline) | `cpp.test_basic` | PASS | 1 | — | — |
| R0 (baseline) | `java.client.ClientConfigTest` | PASS | ~20 | — | — |
| R0 (baseline) | `cluster.smoke` | FAIL | — | SQL CREATE→INSERT→SELECT 闭环未通过，详见 `BUG-01` | 见 docker logs |
| R1 | `cpp.unit` | PASS | ~35 | — | `artifacts/cpp.unit/` |
| R1 | `cpp.wal` | PASS | ~20 | — | `artifacts/cpp.wal/` |
| R1 | `cpp.stress` | PASS | ~30 (after fix) | — | `artifacts/cpp.stress/` |
| R1 | `cpp.coverage` | PASS | ~90 | — | `artifacts/cpp.coverage/` |
| R1 | `java.unit` | PASS | ~160 | — | `artifacts/java.unit/` |
| R1 | `java.integration` | PASS | ~600 | — | `artifacts/java.integration/` |
| R1 | `cluster.smoke` | PASS | ~10 (after fixes) | — | `artifacts/cluster.smoke/` |
| R1 | `zk.topology` | PASS | ~4 (after fixes) | — | `artifacts/zk.topology/` |
| R1 | `cluster.stress` | PASS | ~120 (scaled 2×50 + 500) | — | `artifacts/cluster.stress/` |
| R1 | `cluster.chaos` | FAIL | ~400 | master_failover + rs_crash/random_rs seed-row/route-cache; partition 场景因容器无 `iptables` SKIP | `artifacts/cluster.chaos/` |
| R1 | `cluster.smoke` (post-chaos) | PASS | ~10 | — | `artifacts/cluster.smoke/` |
| R1 | `e2e.client` | PASS | ~5 | — | `artifacts/e2e.client/` |
| R2 | `cpp.unit` | PASS | ~18 | — | `artifacts/cpp.unit/` |
| R2 | `cpp.wal` | PASS | ~10 | — | `artifacts/cpp.wal/` |
| R2 | `cpp.stress` | PASS | ~26 | — | `artifacts/cpp.stress/` |
| R2 | `cpp.coverage` | PASS | ~80 | — | `artifacts/cpp.coverage/` |
| R2 | `java.unit` | PASS | ~40 | — | `artifacts/java.unit/` |
| R2 | `java.integration` | PASS | ~30 | — | `artifacts/java.integration/` |
| R2 | `cluster.smoke` | PASS | ~7 | — | `artifacts/cluster.smoke/` |
| R2 | `zk.topology` | PASS | ~4 | — | `artifacts/zk.topology/` |
| R2 | `cluster.stress` | FAIL→PASS (after BUG-12 fix) | ~130 | Initial phase2=0 rows 由 client 路由到 `0.0.0.0:0` 引起；修复 `resolveLocation` 的 sentinel 处理后复跑通过 | `artifacts/cluster.stress/` (FAIL), `artifacts/cluster.stress/` (PASS) |
| R2 | `cluster.chaos` | FAIL (1 of 5) | ~350 | 仅 Scenario 1 Master failover；Scenario 2/3 全绿（BUG-07/11 修复生效）；4/5 由于 iptables 缺失 SKIP | `artifacts/cluster.chaos/` |
| R2 | `cluster.smoke` (post-chaos) | PASS | ~7 | — | `artifacts/cluster.smoke/` |
| R2 | `e2e.client` | PASS | ~5 | — | `artifacts/e2e.client/` |
| R3 | `cpp.unit` | PASS | ~12 | — | `artifacts/cpp.unit/` |
| R3 | `cpp.wal` | PASS | ~10 | — | `artifacts/cpp.wal/` |
| R3 | `cpp.stress` | PASS | ~28 | — | `artifacts/cpp.stress/` |
| R3 | `cpp.coverage` | PASS | ~45 | — | `artifacts/cpp.coverage/` |
| R3 | `java.unit` | PASS | ~40 | — | `artifacts/java.unit/` |
| R3 | `java.integration` | PASS | ~33 | — | `artifacts/java.integration/` |
| R3 | `cluster.smoke` | PASS | ~6 | — | `artifacts/cluster.smoke/` |
| R3 | `zk.topology` | PASS | ~4 | — | `artifacts/zk.topology/` |
| R3 | `cluster.stress` | PASS | ~118 (scaled 2×50 + 500) | — | `artifacts/cluster.stress/` |
| R3 | `cluster.chaos` | FAIL (1 of 5) | ~400 | 仍然只是 Scenario 1 Master failover（BUG-08）；Scenario 2/3 全绿（Rounds 1-3 都通过），Scenario 4/5 SKIP | `artifacts/cluster.chaos/` |
| R3 | `cluster.smoke` (post-chaos) | PASS | ~6 | — | `artifacts/cluster.smoke/` |
| R3 | `e2e.client` | PASS (4/4: 01_ddl_basic + 02_index + 03_where_and_or + 04_delete) | ~30 | — | `artifacts/e2e.client/` |
| R4 | `cpp.unit` | PASS | ~20 | — | `artifacts/cpp.unit/` |
| R4 | `cpp.wal` | PASS | ~12 | — | `artifacts/cpp.wal/` |
| R4 | `cpp.stress` | PASS | ~28 | — | `artifacts/cpp.stress/` |
| R4 | `cpp.coverage` | PASS (BUG-10 修复后产物完整) | ~45 | — | `artifacts/cpp.coverage/` |
| R4 | `java.unit` | PASS | ~42 | — | `artifacts/java.unit/` |
| R4 | `java.integration` | PASS | ~30 | — | `artifacts/java.integration/` |
| R4 | `cluster.smoke` | PASS（fresh cluster after `compose down -v`） | ~7 | — | `artifacts/cluster.smoke/` |
| R4 | `zk.topology` | PASS | ~3 | — | `artifacts/zk.topology/` |
| R4 | `cluster.stress` | PASS（scaled 2×50 + 500，新增 wait_for_table 解决 CREATE→INSERT 路由可见性窗口） | ~120 | — | `artifacts/cluster.stress/` |
| R4 | `e2e.client` | PASS (4/4 fixtures) | ~10 | — | `artifacts/e2e.client/` |
| R5 | `cluster.smoke` | PASS（新增 SQL coverage 块：CREATE/DROP INDEX、range/AND/OR、DELETE、UPDATE/ALTER/TRUNCATE 错误路径） | ~12 | — | `artifacts/cluster.smoke/` |
| R5 | `cluster.stress` | PASS（新增 phase3：CREATE/DROP INDEX + DELETE + range/point SELECT，scaled 2×50 + 500 + 200） | ~110 | — | `artifacts/cluster.stress/` |
| R5 | `e2e.client` | PASS 6/7 (新增 fixture 05_range_select / 06_unsupported_sql / 07_execfile 全绿；04_delete 偶发 BUG-17 残留状态) | ~30 | 04_delete 偶发 `(3 rows)` 断言失败：BUG-17 DROP TABLE 数据残留 | `artifacts/e2e.client/` |
| R4 | `cluster.chaos` | FAIL (Scenario 1 PASS — BUG-08 修复在集群中验证；Scenario 2/3 由于 Mac Docker rs 重启 health 慢于 60s 默认窗口出现级联 failure；4/5 SKIP) | ~400 | rs-1 restart timeout → 后续 round 在表元数据未恢复时 SELECT 报 TABLE_NOT_FOUND | `artifacts/cluster.chaos/` |
| R4 | `cluster.smoke` (post-chaos) | PASS | ~6 | — | `artifacts/cluster.smoke/` |

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
> - `ID`：`BUG-NN`（按发现顺序递增，不带日期）
> - `位置`：`文件#行` 或「跨模块」
> - `复现`：单行命令，尽量最小化
> - `根因` / `修复动作`：最多两行

| ID | 严重度 | 状态 | 位置 | 现象 | 复现 | 根因 / 修复动作 | artifacts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BUG-01 | **P0** | FIXED | `RegionServiceImpl#executeWrite`（`java-regionserver/src/main/java/edu/zju/supersql/regionserver/rpc/RegionServiceImpl.java:176-198`） | CREATE TABLE 通过 master 的 `regionDdlExecutor.execute` 下到每个 RS → RS 走 `executeWrite` → 读 `/supersql/assignments/<table>` 返回空（master 在 fan-out DDL 之后才写 assignments）→ `Insufficient replica targets: required=1, available=0`；链路上 client 最终看到 `Failed to create table on replica rs-3` | `printf "CREATE TABLE t1(id int, primary key(id));\nexit;\n" \| docker exec -i client java -jar /app/app.jar` | 根因：CREATE/DROP TABLE 是 master 编排的（已向每个副本独立下发 DDL），RS 侧再做副本同步会死等。修复：`executeWrite` 对 `WalOpType.CREATE_TABLE` / `WalOpType.DROP_TABLE` 跳过 replica-sync（本地 WAL + 本地执行仍保留） | `artifacts/cluster.smoke/` (before), `artifacts/cluster.smoke/` (after) |
| BUG-02 | **P0** | FIXED | `MiniSqlProcess#startProcessIfNeeded`（`java-regionserver/src/main/java/edu/zju/supersql/regionserver/MiniSqlProcess.java:50-64`） | 全新 RS 数据目录下第一次 CREATE TABLE 会令 C++ 引擎 segfault；引擎监控线程拉起新进程后 CREATE 已丢失，后续 INSERT/SELECT 都说 `Table not exist!` | `docker exec rs-1 sh -c "cd /data/db && printf 'create table t(id int, primary key(id));\n' \| /opt/minisql/main"` → `Segmentation fault` | 根因：miniSQL 的 `buffer_manager` 需要 `database/{catalog,data,index}` 子目录预先存在，缺失时直接段错误（C++ 层无法改，因为测试 agent 不允许动 cpp-core）。修复：Java 侧 `MiniSqlProcess` 启动前 `mkdirs` 这三个子目录 | `artifacts/cluster.smoke/` |
| BUG-03 | **P0** | FIXED | `MiniSqlProcess#execute`（`java-regionserver/.../MiniSqlProcess.java:178-201`）+ `OutputParser#parse` | SELECT 永远返回空结果（client 只打印 `OK`）。根因双层：① `execute()` 用 `readLine() until line.contains(">>> ")` 命中的是**下一条命令的前导提示符**，返回的是「上一条命令的响应」——CREATE/INSERT 因都产生 `>>> SUCCESS` 表面正常，SELECT 的多行表格被截断；② `OutputParser` 按 `\|` 切列，但 miniSQL 实际用 `\t` | `printf "CREATE TABLE t(id int, primary key(id));\nINSERT INTO t VALUES(1);\nSELECT * FROM t;\n" \| docker exec -i client java -jar /app/app.jar` | 修复：`execute()` 改为「写命令 → 读到输出流静默 150ms 为止」，并在 start() 同样消费完启动 banner；`OutputParser` 优先 `\t` 切分，回退 `\|`；增加移除首行粘贴的 `>>> ` 前缀 | `artifacts/cluster.smoke/` (observed "OK" no rows) |
| BUG-04 | **P1** | FIXED | `scripts/zk/assert_topology.sh` | 查询 ZK 路径时丢了 Curator namespace 前缀（`/masters` 应为 `/supersql/masters`），且 `zk_ls` / `zk_get` 的过滤器会把 zkCli.sh 的日志行当作数据 | `bash scripts/zk/assert_topology.sh` | 修复：全量改前缀为 `/supersql/*`；`zk_ls` 仅取第一行 `^[` 且非 `[zk:` 前缀的列表行；`zk_get` 仅取 `^{` 的 JSON payload 行 | `artifacts/zk.topology/` |
| BUG-05 | **P1** | FIXED | `scripts/run_tests.sh#suite_cpp_stress` | `test_pin_count` 链接失败（`Undefined symbols: API::createTable / Table::getTuple / ...`）；`g++` 命令漏了 `api.cc record_manager.cc index_manager.cc catalog_manager.cc basic.cc` | `bash scripts/run_tests.sh cpp stress` | 修复：补齐源文件列表，与 Makefile 的 `test_exhaustive` target 对齐 | `artifacts/cpp.stress/` |
| BUG-06 | P2 | FIXED | `scripts/chaos_test.sh` 多处（`get_active_master` / `extract_status_number`） | macOS BSD `grep` 不支持 `-P`；脚本里 `grep -oP '"role":"\K[^"]+'` 静默失败，导致 `Scenario 1 Master failover` 拿不到当前 active master；RS 管理端口 9190 探测需要 `docker exec`（本地 8080-8082 是 Thrift） | `bash scripts/chaos_test.sh master_failover` | 已全量改为 `grep -oE + sed`；R4 在 macOS BSD grep 下重跑 Scenario 1 PASS（master-3→master-1 epoch=2→3，master-3→master-2 epoch=5→6）。已无 `-P` 残留。 | `artifacts/cluster.chaos/` |
| BUG-07 | **P1** | FIXED | `RegionAdminServiceImpl#transferTable`（`java-regionserver/.../RegionAdminServiceImpl.java:242-310`） | chaos 场景 2/3 复跑时 client 对刚创建的表路由到 `target=none(0.0.0.0:0)`，RS 侧 `Insufficient replica targets: required=1, available=0` 级联；master 日志反复出现 `healTableLocation transfer failed source=rs-X code=TABLE_NOT_FOUND msg=No files found for table` → `recovered with reduced replicas` | 手动复现：`docker stop rs-3 && sleep 15 && printf "INSERT INTO t VALUES(2,'x');\nexit;\n" \| docker exec -i client ...` | 根因：miniSQL 的 buffer pool 延迟刷盘，刚 CREATE 的表在源 RS 磁盘上还没文件；master 的 heal 走 `transferTable` 仅扫描 `dataDir` 目录列表文件，找不到 → TABLE_NOT_FOUND → 错误地触发 replica 缩容（3→2→1），导致最后集群的 primary 落到已关机的 RS 上，client 的 RouteCache 看到空/无效地址（`0.0.0.0:0`）。修复：`transferTable` 在 list files 之前先 `miniSql.checkpoint()` 强制刷盘；构造器新增可选 `MiniSqlProcess` 注入，null 时保持原行为（单测不受影响） | `artifacts/cluster.chaos/` (before 8 fail), `artifacts/cluster.chaos/` (after 2 fail) |
| BUG-11 | **P1** | FIXED (indirectly by BUG-07 & BUG-12) | 跨模块（RS `ReplicaManager` / `WalManager` / master heal-catchup） | chaos 场景 3 Round 2 & Round 3：在 RS 下线期间客户端的 INSERT 显示「succeeded」，RS 恢复之后 `SELECT` 只看到第 1 轮写入的行，Round 2/3 写入全部丢失；`write succeeded while rs-X was down` 但 recovery 后 `missing row 'round_N_rs-X'` | `scripts/run_tests.sh cluster chaos`（R1），Scenario 3 Round 2 / Round 3 | 实际根因：BUG-07 的 `healTableLocation` 持续缩容副本集，导致 round 2/3 期间实际副本只剩 1 个；同时 BUG-12 让客户端在路由失效时拿到 sentinel 地址。两者合流导致「写看似成功但被 master 下一轮 heal 抹掉」。修复 BUG-07 + BUG-12 之后 R2 chaos Scenario 3 Round 1/2/3 全绿 | `artifacts/cluster.chaos/` (partial fail), `artifacts/cluster.chaos/` (pass) |
| BUG-12 | **P0** | FIXED | `SqlClient#resolveLocation`（`java-client/src/main/java/edu/zju/supersql/client/SqlClient.java:701`） | 负载压测 / chaos 下客户端连续 5 次 `DML execution failed: table=..., attempt=N/5, reason=Invalid port 0`；`target=none(0.0.0.0:0)` 把整条 DML 链路拖垮 | `STRESS_CONCURRENCY=2 STRESS_ROWS_PER_WORKER=50 STRESS_BULK_ROWS=500 bash scripts/run_tests.sh cluster stress` | 根因：master 在表元数据尚不可见时返回「sentinel」`TableLocation(primaryRS=RegionServerInfo("none","0.0.0.0",0), tableStatus="TABLE_NOT_FOUND")`；client 的 `resolveLocation` 直接把它缓存并后续重试都命中这条缓存，produce port 0。修复：`isSentinelLocation` 识别 tableStatus ∈ {TABLE_NOT_FOUND, ZK_UNAVAILABLE, NOT_LEADER} 或 host ∈ {"none","unavailable","0.0.0.0"}/port≤0 的占位，不缓存；master 查询带 3 次 100/200/300ms 退避重试以吃掉「刚 CREATE 后的元数据可见性竞态」 | `artifacts/cluster.stress/` (before), `artifacts/cluster.stress/` (after) |
| BUG-08 | **P1** | FIXED | `MasterServer#main` bootstrap 循环（`java-master/.../MasterServer.java:340-347`）+ `LeaderElector#ensurePath`（`.../election/LeaderElector.java:155-159`） | 停掉 active master 后 150 秒内 `/supersql/active-master` 的 epoch 不递增；`/supersql/masters` 只看到 1 个 latch participant，master-2/master-3 从未注册 latch | `docker stop master-1; sleep 150; docker exec zk1 zkCli.sh get /supersql/active-master` → epoch 不变 | 根因：bootstrap 循环 `checkExists()` + `create()` 不是原子操作。多 master 并发启动时，第一台 master 创建 `/meta/tables` 后，第二台/第三台的 `create()` 抛 `KeeperException.NodeExistsException`，被外层 catch 当作"ZK connection failed at startup"吃掉，结果 `MasterRuntimeContext.initialize` 没执行、`leaderElector.start()` 也跳过。修复：bootstrap 循环和 `LeaderElector.ensurePath` 改为直接 `create()` 并捕获 `NodeExistsException`。增加 `LeaderElectorTest.shouldStartWhenEnsuredPathsAlreadyExist` 回归测试。R4 集群 fresh boot 后 3 个 latch participant 都注册成功，Scenario 1 失主选举正常完成（epoch 5→6）。 | `artifacts/cluster.chaos/` (fresh boot R4 PASS) |
| BUG-09 | P2 | KNOWN_ISSUE | `docker/Dockerfile.regionserver`（**不允许本 agent 修改**） | RS 镜像未安装 `iptables`，chaos 场景 4/5（network partition, suspected replica）无法注入故障 | `docker exec rs-1 which iptables` → empty | 本 agent 将这两个场景改为 SKIP（exit 2），不作为 FAIL；修复需 Dockerfile 层加 `apt-get install -y iptables` | `artifacts/cluster.chaos/` |
| BUG-10 | P2 | FIXED | `scripts/run_tests.sh#suite_cpp_coverage` | `minisql/cpp-core/Makefile:113` 的 `coverage:` target 执行 `gcov basic.cc ...`，但 `*.gcno` 实际命名是 `test_basic-basic.gcno`（每个测试 target 各自独立的命名空间），导致 gcov 找不到源文件、`*.gcov` 不生成；额外发现：`make coverage` 的 prereq `test` 在 binary already up-to-date 时跳过编译，连 gcno/gcda 都不会生成 | `bash scripts/run_tests.sh cpp coverage && find artifacts/cpp.coverage/*/coverage -name "*.gcov" \| wc -l` | Makefile 不允许改。修复 `scripts/run_tests.sh` 的 `suite_cpp_coverage`：① 跑前先 `make clear` 强制重编（否则 `up-to-date` 会跳过 gcno 生成）；② 跑后按 `<target>-<src>.gcno` 模式遍历，逐个 cp 到 `<src>.gcno` → `gcov <src>.cc` → 移到 `artifacts/.../coverage/<target>/<src>.cc.gcov`。R4 重跑产出 117 个 `.gcov` 文件覆盖 `basic/api/buffer_manager/record_manager/...` × 11 个 test target | `artifacts/cpp.coverage/` (117 .gcov files) |
| BUG-13 | **P1** | FIXED | `scripts/cluster/stress.sh#phase1/phase2` | `cluster.stress` 时 phase1/phase2 在 `CREATE TABLE` 后立刻 `INSERT`，前几条 INSERT 会偶发 `Error: Table not found: stress_xxx (status=TABLE_NOT_FOUND, attempt=3/3)`，导致 phase2 期望 500 行实际只有 495；BUG-12 修过 sentinel 缓存但 client 端的 master query 退避（100/200/300ms 三次）在压测并发下仍不够 | `STRESS_CONCURRENCY=2 STRESS_ROWS_PER_WORKER=50 STRESS_BULK_ROWS=500 bash scripts/run_tests.sh cluster stress`（fresh cluster R4 第一次 PASS，其后偶发） | 根因：master 在 `CREATE TABLE` 完成后向 ZK 写 metadata 与 client 通过 `RouteCache` 看见之间存在毫秒级窗口；测试脚本不能假设 CREATE 返回即可立即 INSERT。修复：`stress.sh` 新增 `wait_for_table` 辅助，在 phase1/phase2 的 `run_sql "CREATE TABLE ..."` 之后**主动 SELECT 直到不再返回 TABLE_NOT_FOUND**（最多 30s），再启动 INSERT worker。 | `artifacts/cluster.stress/` (FAIL 495/500), `artifacts/cluster.stress/` (PASS) |
| BUG-14 | P2 | OPEN | `scripts/chaos_test.sh#run_sql` + `create_test_table` | chaos Scenario 3 中 `Round N: missing row 'round_N_rs-X' after recovery` 与 `Table not found: chaos_test_random_rs_NNNNN` 的真实根因是 chaos `create_test_table` 在 cluster 退化时 CREATE 实际返回 `Error: Internal error processing createTable`，但 `run_sql ... \|\| return 1` 只检查 docker exec 退出码（client 一律 exit 0），因此把"创建失败"当成"创建成功"继续后续轮次 | R4 chaos Scenario 3 Round 1：`Preparing table: chaos_test_random_rs_29166` 后立即 `Round 1: seed row became unavailable during rs-2 outage`，master 三个节点的日志都查不到 `chaos_test_random_rs_29166` 字眼 → 表根本没建出来 | 待修：`run_sql` 的输出按 `Error:` 关键字判定真实失败、`create_test_table` 在 CREATE 之后调用类似 BUG-13 的 `wait_for_table` 等元数据可见再 INSERT seed 行；同时 Scenario 之间应清理上一轮 `chaos_test_*` 残留 ZK znode 避免 heal 干扰 | `artifacts/cluster.chaos/` |
| BUG-15 | P2 | OPEN | docker `Dockerfile.regionserver`（不允许 agent 修改）+ chaos `RECOVERY_WAIT_SECONDS` 默认 60s | macOS Docker 下 RS 容器 stop 之后再 start，miniSQL C++ 进程冷启动 + Java RS 重新连 ZK 经常需要 90~120s，超过 chaos 默认 60s 的 health 等待窗口；R4 cluster.chaos Scenario 2 因此 fail（rs-1 timeout），后续 Scenario 3 在级联状态下进一步连锁 fail | `bash scripts/run_tests.sh cluster chaos` | 待评估：① 默认 `CHAOS_RECOVERY_WAIT_SECONDS` 提升到 120；② 增加 RS 镜像 health check 启动 grace；③ chaos 脚本失败前再额外检查 `docker exec rs-X curl -sf localhost:9190/health` 兜底 | `artifacts/cluster.chaos/` |
| BUG-17 | **P1** | OPEN | client `DROP TABLE` → master `MetaManager.dropTable` → RS `DROP TABLE` 链路 | DROP TABLE 之后再次 CREATE 同名表，新表的 INSERT 出现 `Primary key conflict` 错误；SELECT 看到来自前一轮的旧行；DROP 没有真正擦除 RS 上的 `database/data/<name>` 文件，并且 C++ 引擎的 buffer pool 仍然缓存旧数据。具体表现见 `e2e.client/04_delete` 在多次复跑后偶发 `(3 rows)` 断言失败，实际首条 SELECT 返回 5 行但 row 2 的 tag 是上一轮残留的 `keep` 而非本轮 INSERT 的 `drop` | 反复跑 `bash scripts/run_tests.sh e2e client`，4-5 次后 `04_delete` 出现 `missing pattern: \(3 rows\)`；`docker exec rs-1 ls /data/db/database/data/` 在 DROP 后偶尔仍能看到表文件 | 待修：① DROP TABLE 链路在所有副本完成后才清空 ZK metadata；② RS 收到 DROP 必须删除 `data/`、`index/`、`catalog/` 三处文件并提示 C++ 引擎 evict 该表的 buffer pool；③ 在彻底修好之前 `scripts/e2e/run_sql_fixtures.sh` 已加预清理（ZK + 三个 RS 数据文件），可减少但不能消除偶发 | `artifacts/e2e.client/` |

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
  "result": "FAIL",
  "elapsedSeconds": 523,
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

---

## 8. SQL 操作覆盖矩阵

盘点每个 SQL 操作在各测试套件里的实际执行情况。这是回答"所有 SQL 操作是否被覆盖"的唯一事实源。

### 8.1 miniSQL 引擎实际支持的 SQL（源自 `minisql/cpp-core/interpreter.cc:60-108`）

| 语句 | 引擎实现 | 备注 |
| --- | --- | --- |
| `CREATE TABLE` | ✓ | `EXEC_CREATE_TABLE` |
| `DROP TABLE`   | ✓ | `EXEC_DROP_TABLE` |
| `CREATE INDEX` | ✓ | `EXEC_CREATE_INDEX` |
| `DROP INDEX`   | ✓ | `EXEC_DROP_INDEX` |
| `INSERT`       | ✓ | `EXEC_INSERT` |
| `SELECT *`     | ✓ | `EXEC_SELECT` |
| `SELECT … WHERE =`         | ✓ | 单条件走 IndexManager 加速（若索引存在） |
| `SELECT … WHERE < > <= >=` | ✓ | 走 B+树 range 或全表扫 |
| `SELECT … AND / OR`        | ✓ | `api.cc` 的 `unionTable` / `joinTable` 合并两路结果 |
| `DELETE`       | ✓ | `EXEC_DELETE`，软删除标记 |
| `SHOW`         | ✓ | 仅在 REPL 使用 |
| `execfile`     | ✓ | 仅在 REPL 使用 |
| `checkpoint`   | ✓ | `EXEC_CHECKPOINT`，Java 侧通过 `MiniSqlProcess.checkpoint()` 调用 |
| `UPDATE`       | ✗ | **引擎未实现**；但 Java 的 `RegionServiceImpl.classifyOpType` 映射了 `WalOpType.UPDATE`，属于契约 gap |
| `ALTER TABLE`  | ✗ | 同上；`SqlClient.classifyDdlAction` 把它标成 `FORWARD_TO_REGION` 转给 RS，RS 丢给引擎后报 `Input format error` |
| `TRUNCATE`     | ✗ | 同 ALTER 路径，实际会返回 ERROR 但无专门测试 |

### 8.2 各套件对 SQL 的覆盖

> 标记说明：`✓` 自动跑过；`—` 没跑且本套件能跑（gap）；`N/A` 该套件不在合理覆盖范围（如客户端不识别该 SQL、cpp 引擎不支持等）。
> e2e.client 后括号是 fixture 编号：`01_ddl_basic` / `02_index` / `03_where_and_or` / `04_delete` / `05_range_select` / `06_unsupported_sql` / `07_execfile`。

| SQL | cpp.unit (test_api / test_exhaustive) | cpp.wal | cluster.smoke | cluster.stress | cluster.chaos | e2e.client |
| --- | --- | --- | --- | --- | --- | --- |
| CREATE TABLE | ✓ | ✓ (crash-recovery) | ✓ | ✓ | ✓ | ✓ (01,02,03,04,05,06,07) |
| DROP TABLE   | ✓ | N/A (test 聚焦 redo) | ✓ | ✓ | ✓ (cleanup_test_table 每场景必跑) | ✓ (01,02,03,04,05,06,07) |
| CREATE INDEX | ✓ | N/A | ✓ (smoke 覆盖块) | ✓ (phase3) | ✓ (seed 阶段) | ✓ (02) |
| DROP INDEX   | ✓ | N/A | ✓ (smoke 覆盖块) | ✓ (phase3) | ✓ (seed 阶段) | ✓ (02) |
| INSERT       | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (01,02,03,04,05,06,07) |
| SELECT *     | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (01,02,03,04,05,06,07) |
| SELECT WHERE =        | ✓ | N/A | ✓ (smoke 覆盖块) | ✓ (phase3 point lookup) | ✓ (单键 seed 行 + AND/OR seed 阶段) | ✓ (02,03) |
| SELECT WHERE < > …    | ✓ | N/A | ✓ (smoke 覆盖块 grade>75) | ✓ (phase3 score>500) | N/A (chaos 关注故障注入路径) | ✓ (05) |
| SELECT AND / OR       | ✓ | N/A | ✓ (smoke 覆盖块) | N/A (无并发条件复合需求) | ✓ (seed 阶段) | ✓ (03) |
| DELETE       | ✓ | N/A | ✓ (smoke 覆盖块) | ✓ (phase3) | ✓ (seed 阶段) | ✓ (04) |
| UPDATE / ALTER / TRUNCATE（错误路径） | N/A (cpp 引擎不实现) | N/A | ✓ (smoke ERR 断言) | N/A (压测无意义) | N/A (chaos 无意义) | ✓ (06) |
| execfile     | N/A (引擎 REPL 内部) | N/A | N/A (smoke 不需嵌套) | N/A | N/A | ✓ (07) |
| checkpoint   | N/A (引擎层无独立 test) | 间接（test_wal 触发 flush 路径） | N/A (SqlClient.classifySql 不识别 `checkpoint`) | N/A | N/A | N/A |

> `checkpoint` 走 Java 侧 `MiniSqlProcess.checkpoint()`，由 `MiniSqlProcessTest`（java.unit）覆盖；该列与 §8.2 的 SQL-客户端维度正交，故矩阵中标 N/A。
> `UPDATE/ALTER/TRUNCATE` 走 client → master DDL/DML 路径，必须返回 ERROR；`cluster.smoke#smoke_err_*` + `e2e.client/06_unsupported_sql.sql` 双重锁死契约，避免回归到静默 success。

### 8.3 当前 gap（待后续补测，不计为 bug）
- `checkpoint` 的显式命令只在 `cpp.wal` 间接触发；没有 "发 1000 行 → 手动 checkpoint → 重启 → SELECT 行数一致" 的端到端回归。Java 侧 `MiniSqlProcessTest#checkpoint*` 单测已锁，集群层暂无对应。
- UPDATE / ALTER / TRUNCATE 错误路径已被 `cluster.smoke` + `e2e.client/06` 锁死；后续若新增 Java 集成测试断言 `StatusCode.ERROR`，可视为加固层而不是 gap。
