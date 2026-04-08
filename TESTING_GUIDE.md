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

> [!TIP]
> **测试建议**：优先运行 `test_wal_crash_recovery`。如果该测试通过，说明你的存储引擎在意外断电等极端情况下也能保证数据不丢失。
