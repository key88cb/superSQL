# SuperSQL 未完成功能实现指南

更新时间：2026-04-10

本文档基于当前仓库真实代码状态整理，不以 README 的目标描述为准，而以 `java-master`、`java-regionserver`、`java-client` 的实际实现为准。目标是把“还没完成的功能”拆成可执行的实现顺序，并提前理顺依赖关系，方便后续分阶段落地。

相关参考：
- [DEVELOPMENT_PLAN.md](../DEVELOPMENT_PLAN.md)
- [MODULE_TASKS.md](../MODULE_TASKS.md)
- [IMPLEMENTATION_STATUS.md](../IMPLEMENTATION_STATUS.md)

## 1. 当前真实状态

### 1.1 已经比较扎实的部分

- Master 已有 ZooKeeper 初始化、LeaderLatch 选主、`/active-master` epoch CAS、Active 心跳、HTTP `/health` `/status`、基本元数据读写。
- RegionServer 已有 ZooKeeper 注册与心跳、MiniSQL 进程启动、`RegionService.execute()` 基础执行路径、`ReplicaSyncService` 的内存版同步。
- Client 已有 SQL 分类、表名提取、`/active-master` 发现、路由缓存 TTL 与版本失效骨架。
- 现有自动化测试覆盖了上述主干，仓库当前 `mvn test -DskipTests=false` 为绿色。

### 1.2 明确还没完成的部分

按代码真实状态，以下功能仍未完成：

- Client 到 Master/RegionServer 的真实 RPC 执行链路。
- Master 联动 RegionAdmin 的远端建表、删表、迁移、缓存失效广播。
- `triggerRebalance()`、动态均衡、热点识别、Region 迁移。
- RegionAdmin 大部分管理 RPC。
- `executeBatch`、`createIndex`、`dropIndex`。
- 持久化 WAL、Crash Recovery、LSN 驱动的副本追赶。
- 主副本写路径中的“WAL PREPARE -> syncLog -> COMMIT -> miniSQL”完整闭环。
- RegionServer 宕机后的副本恢复、主副本晋升、客户端缓存失效推送。
- MOVING/NOT_LEADER/主副本不可达时的客户端自动重试与故障转移。

## 2. 先解决的共性依赖

这一部分不先解决，后面各模块会不断重复返工。

### 2.1 配置项统一

当前 `.env.example` 和代码使用的环境变量并不完全一致：

- `.env.example` 提供了 `MASTER_ZK_CONNECT`、`RS_ZK_CONNECT`、`CLIENT_ZK_CONNECT`
- 实际代码统一读取的是 `ZK_CONNECT`
- `.env.example` 提供了多种 `*_INTERVAL_MS`、`*_TIMEOUT_MS`
- 实际代码里很多值仍然是硬编码

建议：

- 保留当前 `ZK_CONNECT` 作为运行时统一入口，避免 Docker Compose 再分裂变量。
- 为三个模块各自增加一个轻量配置类：
  - `java-master/.../MasterConfig`
  - `java-regionserver/.../RegionServerConfig`
  - `java-client/.../ClientConfig`
- 统一从配置类读取：
  - ZK 地址
  - 心跳间隔
  - 重均衡间隔
  - Client TTL / RPC 超时 / MOVING 重试参数
  - Replica sync timeout

这样后续写重试、迁移和恢复逻辑时不会继续散落常量。

### 2.2 ZooKeeper 路径常量统一

当前 Master 代码里路径大量以内联字符串存在，例如：

- `/active-master`
- `/region_servers`
- `/meta/tables`
- `/assignments`

建议新增常量类：

- `java-master/.../ZkPaths`
- `java-regionserver/.../ZkPaths`
- `java-client/.../ZkPaths`

内容至少包括：

- `ACTIVE_MASTER`
- `ACTIVE_HEARTBEAT`
- `REGION_SERVERS`
- `META_TABLES`
- `ASSIGNMENTS`
- helper：`tableMeta(tableName)`、`assignment(tableName)`、`regionServer(rsId)`

原因：

- 后续 Master / RS / Client 都会依赖相同路径
- 迁移、恢复、缓存失效逻辑会频繁引用这些节点
- 统一常量后测试也更容易写

### 2.3 生产代码不要依赖 `test-common`

现有 `test-common` 只适合测试。

不要把生产代码里的共享逻辑塞进 `test-common`。后续如果确实出现大量重复生产代码，建议单独新增 Maven 模块，例如：

- `java-common`

但现阶段不建议立刻加新模块，先在各模块内用轻量配置类和常量类解决问题，避免工程复杂度上升。

### 2.4 RPC Client 层必须先补出来

当前缺的不是 Thrift 接口定义，而是调用端封装。

建议最先补以下客户端：

- `java-client/.../rpc/MasterRpcClient`
- `java-client/.../rpc/RegionRpcClient`
- `java-master/.../rpc/RegionAdminRpcClient`
- `java-regionserver/.../rpc/ReplicaSyncRpcClient`

统一职责：

- 创建 `TSocket` / `TFramedTransport`
- 处理超时、关闭、重试
- 将低层异常转成清晰的业务错误

如果不先做这层，后面每个调用点都会重复连线、超时和错误处理。

## 3. 模块级实现方案

## 3.1 Client

### 当前差距

`SqlClient` 还停留在“路由演示模式”：

- DDL/DML 只打印将要去哪，不真正发 RPC
- 没有处理 `NOT_LEADER`
- 没有处理 `MOVING`
- 没有处理主副本不可达时的缓存失效和重查

### 建议实现顺序

#### 第一步：补真实 RPC 执行链路

新增：

- `MasterRpcClient.getTableLocation/createTable/dropTable/getActiveMaster`
- `RegionRpcClient.execute/executeBatch/createIndex/dropIndex/ping`

改造：

- `SqlClient.main()`

落地方式：

- DDL：
  - 先找到 Active Master
  - 调 `MasterService.createTable/dropTable`
  - 收到 `NOT_LEADER` 时自动切换 `redirectTo`
- DML：
  - 先查 `RouteCache`
  - miss 时调 `MasterService.getTableLocation`
  - 再对 `primaryRS` 调 `RegionService.execute`

#### 第二步：补路由异常处理

需要支持：

- `NOT_LEADER`
  - 清除当前 Master 地址缓存
  - 改连新 leader
- `MOVING`
  - 等待 300ms / 500ms
  - 清路由缓存
  - 回源 Master 重查
  - 限制重试次数，建议 5 次
- `TException`
  - 清除对应 table 的缓存
  - 重查路由一次
  - 若仍失败再报错

#### 第三步：补缓存失效发现机制

先做低成本版本：

- 仍保留 TTL
- 额外让 Client 对 `/meta/tables/{table}` 版本变化做 watch 或在每次 Master 返回时对比 `version`

暂不建议先做复杂的推送链路，优先完成基于 ZK / 版本号的被动失效。

### 依赖前提

- `MasterService.getTableLocation` 返回可靠元数据
- `RegionService.execute` 支持真实 SQL 执行
- Master 的 `redirectTo` 行为稳定

### 测试建议

优先启用或扩展这些测试：

- `SqlClientPlannedFeaturesTddTest`
- `SqlClientRoutingTest`
- `RouteCacheAndDiscoveryTest`

目标覆盖：

- DDL 真正发到 Master
- DML 真正发到 Region
- `NOT_LEADER` 自动重定向
- `MOVING` 自动重试
- 主副本不可达时缓存失效并回源

## 3.2 Master

### 当前差距

Master 现在更像“元数据服务”，还不是“集群控制器”。

具体缺口：

- `createTable/dropTable` 只写 ZK，不联动 RegionAdmin
- 没有元数据管理抽象层，逻辑集中在 `MasterServiceImpl`
- `triggerRebalance()` 未实现
- 没有 RegionServer 下线恢复逻辑
- 没有迁移执行器和缓存失效广播

### 建议拆分的新类

建议新增：

- `meta/MetaManager`
- `meta/AssignmentManager`
- `balance/LoadBalancer`
- `balance/RebalanceScheduler`
- `balance/RegionMigrator`
- `failover/FailoverCoordinator`
- `failover/ReplicaRebuilder`
- `rpc/RegionAdminRpcClient`

### 实现顺序

#### 第一步：抽出元数据层

先把 `MasterServiceImpl` 里的这些职责拆出去：

- 读写 `/meta/tables`
- 读写 `/assignments`
- 读在线 RS 列表
- tableName -> `TableLocation` 的 JSON 序列化/反序列化

原因：

- 后续 rebalance、failover、client invalidation 都要复用这些读写能力
- 不先抽出来，后面逻辑会继续长在一个类里

#### 第二步：补 `createTable/dropTable` 的真实控制面闭环

目标行为：

- `createTable`
  - 选主副本 + 副本
  - 调主副本和从副本的 `RegionAdminService` 建表
  - 所有副本成功后再写 ZK 元数据
  - 若中途失败则回滚已成功建表的副本
- `dropTable`
  - 先读 `/assignments`
  - 并发调用所有副本删除本地表
  - 成功后清理 `/meta/tables` 与 `/assignments`
  - 部分失败时返回错误，不提前删元数据

当前 `RegionAdminService` 没有 `createTableLocal`，有两个可选路径：

方案 A：
- 扩展 Thrift IDL，新增 `createTableLocal/dropTableLocal`

方案 B：
- 复用 `RegionService.execute(tableName, ddl)` 执行 DDL

建议选方案 A，因为管理语义更清晰，也更符合当前 `RegionAdminService` 的职责。

#### 第三步：补 `triggerRebalance()` 和动态均衡

最小实现目标：

- 根据 `tableCount` 识别热点 RS
- 选出一个可迁移表
- 触发一次单表迁移
- 更新 `/assignments` 与 `/meta/tables`

不要一开始就做太复杂的多条件调度，先用：

- `tableCount > avg * ratio`

触发就够了。

#### 第四步：补 failover 与副本重建

Master 需要感知 `/region_servers` 删除事件后：

- 找受影响的表
- 判断下线节点是否为主副本
- 若是主副本，先选新主
- 再把缺的副本补到新的 RS

最小闭环：

- 可以先只恢复“副本数”
- 再补“主副本晋升”和“客户端缓存失效广播”

### 依赖前提

- RegionAdmin 支持本地建表/删表/暂停写/恢复写/文件传输
- RS 心跳数据能提供更真实的 `tableCount/qps`
- `ReplicaSyncService` 和 WAL 能支撑迁移后的追赶

### 测试建议

优先启用或扩展：

- `MasterPlannedFeaturesTddTest`
- `MasterServiceMetadataIntegrationTest`

后续应新增：

- rebalance 集成测试
- failover 恢复测试
- 多 Master + ZK + RegionAdmin 联动测试

## 3.3 RegionServer

### 当前差距

RegionServer 是当前欠债最多的地方。

具体缺口：

- `RegionAdminServiceImpl` 绝大多数方法未实现
- `ReplicaSyncServiceImpl` 仍是内存 WAL
- `WalManager` 只有 checkpoint 清理计数，不是真 WAL
- `executeBatch/createIndex/dropIndex` 未实现
- `MiniSqlProcess` 缺重启、stderr 独立 drain、迁移/恢复辅助能力

### 建议拆分的新类

建议新增：

- `wal/FileWalManager` 或直接增强当前 `WalManager`
- `wal/WalCodec`
- `wal/CrashRecovery`
- `replica/ReplicaManager`
- `replica/ReplicaStatus`
- `migration/TableTransferService`

### 实现顺序

#### 第一步：把 WAL 做成真正持久化

当前 `WalManager` 只有：

- 目录初始化
- 写次数计数
- checkpoint 文件清理

需要补成真正的：

- `append(entry)`
- `commit(lsn)`
- `abort(lsn)`
- `readAfter(table, startLsn)`
- `getMaxLsn(table)`
- `loadExistingWal()`

最小可行格式建议：

- 每条记录按 length-prefixed JSON 或二进制固定头序列化

为了开发速度，优先建议：

- 第一版先用 length-prefixed JSON
- 验证流程跑通后再考虑二进制压缩格式

因为当前课程项目更缺“正确闭环”，不缺“极致性能”。

#### 第二步：实现 `ReplicaSyncService` 持久化版本

现在的 `ReplicaSyncServiceImpl` 用静态内存 map，不适合重启恢复。

替换目标：

- `syncLog` 写本地 WAL
- `getMaxLsn` 从 WAL 文件或内存索引读取
- `pullLog` 从 WAL 读差量
- `commitLog` 更新持久状态

#### 第三步：在 `RegionService.execute()` 写路径接入 WAL + 副本同步

当前 `execute()` 只是：

- 识别写操作
- 增加计数
- 直接调 miniSQL

目标改为：

- 写操作：
  - 生成 `WalEntry`
  - `append(PREPARE)`
  - 主副本并发 `syncLog`
  - 等至少 1 个 ACK
  - 本地 `commit`
  - 执行 miniSQL
  - 异步通知其他副本 `commitLog`
- 读操作：
  - 直接执行 miniSQL

这一步是整个项目最关键的闭环之一。

#### 第四步：补 `executeBatch/createIndex/dropIndex`

建议策略：

- `executeBatch`
  - 顺序执行
  - 逐条汇总失败位置
  - 默认“前面成功的不回滚”
- `createIndex/dropIndex`
  - 本地先调 miniSQL
  - 后续由 Master 协调跨副本传播

#### 第五步：补 RegionAdmin 迁移能力

需要实现：

- `pauseTableWrite`
- `resumeTableWrite`
- `transferTable`
- `copyTableData`
- `deleteLocalTable`
- `invalidateClientCache`

实现建议：

- 先做“表级写锁表”
  - `ConcurrentHashMap<String, AtomicBoolean>` 标记 `table -> paused`
  - `RegionService.execute()` 写入前检查
- `transferTable/copyTableData`
  - 第一版先按文件流式传输
  - 只覆盖最小表文件集合
  - 接收完成后重启或 reload MiniSQL

### 依赖前提

- MiniSQL 数据目录和文件结构要稳定
- 需要搞清楚 C++ 侧 checkpoint / clear log / restart 的真实行为
- 主副本 / 从副本角色需要有清晰来源，建议从 Master 下发的 assignment 衍生

### 测试建议

优先启用或扩展：

- `RegionPlannedFeaturesTddTest`
- `RegionServiceImplTest`
- `ReplicaSyncServiceImplTest`
- `WalManagerTest`

后续应新增：

- Crash Recovery 集成测试
- 写路径多数派 ACK 测试
- transfer/copyTableData 集成测试

## 4. 跨模块功能的依赖顺序

以下顺序建议严格遵守，能明显减少返工。

### Phase 0：共性整理

先做：

- 配置类
- ZK path 常量
- RPC client 封装
- 元数据读写抽象

完成前不要急着写迁移、failover 或 push invalidation。

### Phase 1：端到端最小闭环

目标：

- Client 真正执行 DDL/DML
- Master 真正协调建表/删表
- RegionServer 真正执行 SQL

依赖：

- `MasterRpcClient`
- `RegionRpcClient`
- RegionAdmin 的最小建删表能力

完成标志：

- `CREATE TABLE -> INSERT -> SELECT -> DROP TABLE`
  真正经过 `Client -> Master -> RS`

### Phase 2：持久化 WAL + 半同步复制

目标：

- 写操作有持久化 WAL
- 至少 1 个副本 ACK 后提交
- 副本重启能通过 WAL 恢复

依赖：

- `WalManager` 真正可读写
- `ReplicaSyncService` 持久化
- `ReplicaManager`

### Phase 3：迁移与重均衡

目标：

- `triggerRebalance()` 生效
- 单表迁移闭环可跑通
- MOVING 状态对客户端可见

依赖：

- RegionAdmin 文件传输
- Client 的 MOVING 重试
- MetaManager / AssignmentManager

### Phase 4：故障恢复

目标：

- RS 下线后自动补副本
- 主副本宕机后能晋升新主
- Client 能重新发现新路由

依赖：

- `/region_servers` watcher
- 副本重建
- 路由失效
- WAL 追赶

## 5. 建议的实际落地清单

如果接下来准备真正开工，建议按下面顺序做。

### 第一批

- 统一配置与路径常量
- 抽出 Master 元数据层
- 补 Client 的 `MasterRpcClient/RegionRpcClient`
- 补真正的 `CREATE TABLE / DROP TABLE` 闭环

原因：

- 这批改动风险最低
- 一旦完成，项目就有真正可演示的端到端链路
- 还能直接启用一部分现有 TDD 测试

### 第二批

- `executeBatch/createIndex/dropIndex`
- RegionAdmin 的 pause/resume/delete/transfer/copy
- `triggerRebalance()`
- Client 的 `NOT_LEADER/MOVING` 重试

原因：

- 这批是控制面和体验层完善
- 可以在没有完整 WAL 的情况下先跑通“迁移框架”

### 第三批

- 持久化 WAL
- `ReplicaManager`
- `ReplicaSyncService` 文件版
- Crash Recovery

原因：

- 这是最复杂、最容易返工的一层
- 放在端到端控制面跑通之后再做，调试压力更小

### 第四批

- 动态重均衡调度器
- RS 宕机恢复
- 主副本晋升
- 客户端缓存主动失效

原因：

- 这是“系统自治”能力，依赖前面几乎所有模块

## 6. 与现有测试的对应关系

已经存在的 `@Disabled` TDD 测试可作为后续里程碑：

- `MasterPlannedFeaturesTddTest`
  - rebalance 行为
- `RegionPlannedFeaturesTddTest`
  - executeBatch
  - index 管理
  - table migration hooks
- `SqlClientPlannedFeaturesTddTest`
  - redirect
  - MOVING 重试
  - 路由版本失效

建议规则：

- 每完成一个功能点，就优先“解禁”对应测试
- 不要一上来把所有 `@Disabled` 测试都打开
- 每个阶段只解禁一小批，保证主干持续绿色

## 7. 结论

当前项目已经完成了“分布式控制面的基础骨架”，但距离完整目标还差四条关键链路：

- Client 的真实 RPC 调用链
- Master 的真实控制面闭环
- RegionServer 的持久化 WAL + 副本复制
- 迁移 / 重均衡 / 宕机恢复的自治能力

推荐后续实现顺序：

1. 先做配置统一、路径常量、RPC client、MetaManager。
2. 再做 `CREATE TABLE -> INSERT -> SELECT -> DROP TABLE` 的真实端到端闭环。
3. 然后做持久化 WAL 和半同步复制。
4. 最后做迁移、重均衡、故障恢复和客户端主动失效。

这样推进，代码结构、依赖关系和测试节奏都会更稳。
