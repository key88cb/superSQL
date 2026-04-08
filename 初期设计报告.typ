#import "@preview/bubble-zju:0.1.0": *

// and you can import any Typst package you want!

#import "@preview/fletcher:0.5.8" as fletcher: diagram, edge, node
#import fletcher.shapes: circle as fletcher_circle, hexagon, house

#show: bubble.with(

  title: text(font: ("Times New Roman", "Noto Sans CJK SC"))[分布式 MiniSQL],
  subtitle: text(font: ("Times New Roman", "Noto Sans CJK SC"))[大规模信息系统构建技术导论],
  
  affiliation: "浙江大学 计算机科学与技术学院",
  date: datetime.today().display("[year] 年 [month padding:none] 月 [day padding:none] 日"),
)

#set page(
  paper: "a4",
  margin: (x: 2.5cm, y: 2.5cm),
  numbering: "1",
)
#set text(
  font: ("Times New Roman", "Noto Serif CJK SC", "Noto Sans CJK SC"),
  size: 12pt,
  lang: "zh"
)
#show heading: set text(font: ("Times New Roman", "Noto Sans CJK SC"), lang: "zh")
#show raw: set text(font: ("Consolas", "Menlo", "Noto Sans CJK SC"))
#outline(title: "目录")
#pagebreak()
#import "@preview/tablex:0.0.8": tablex, cellx
#import "@preview/cetz:0.2.2": *

#set document(
  title: "分布式 MiniSQL 系统设计报告",
  author: "sdq"
)

#set par(
  leading: 1.2em,
  first-line-indent: (amount: 2em, all: true),
  justify: true
)

#show heading: set block(above: 1.4em, below: 1em)

= 引言

== 系统目标

本项目是《大规模信息系统构建技术导论》的课程项目。在大二春夏学期的《数据库系统》课程上学习的数据库基本知识与大三春学期的《大规模信息系统构建技术导论》课程上学习的分布式系统与大规模软件系统的构建的知识，完成一个分布式的 MiniSQL 系统，包含 Zookeeper 集群、多 Master 高可用架构、Region Server、Client 等模块，可以完成一个分布式的关系型数据库的各类基本操作，SQL 语句的执行，并且具有*表级数据分布*、*副本维护*、*负载均衡*、*容错容灾*、*集群动态管理*、*分布式查询*、*客户端缓存*、*WAL 日志机制*等功能。

本系统将使用 Java 作为编程语言进行构建，并使用 Maven 作为包管理工具，Zookeeper（一种分布式协调服务，用于管理大型主机。在分布式环境中协调和管理服务是一个复杂的过程。ZooKeeper 通过其简单的架构和 API 解决了这个问题。ZooKeeper 允许开发人员专注于核心应用程序逻辑，而不必担心应用程序的分布式特性。）作为集群管理工具。可以在各个操作系统平台中跨平台使用。

== 设计说明

本程序采用 Java 程序设计语言，在 IDEA 集成开发环境中下编辑、编译与调试。具体程序由 5 人组成的小组开发而成。小组成员的具体分工如表 1 所示：

#table(
  columns: (1fr, 1fr, auto),
  align: center + horizon,
  inset: 8pt,
  [*成员姓名*], [*学号*], [*分工*],
  [师东祺], [3230101967], [分布式通信与Region Server框架], 
  [周子安], [3230103169], [分布式通信与Region Server框架],
  [徐浩然], [3230105281], [一致性协议与容灾],
  [李浩博], [3230101962], [一致性协议与容灾],
  [李业], [3230102276], [Master 高可用与负载均衡],
)

#pagebreak()

= 总体设计

== 系统总体架构设计

系统的总体架构设计如下图所示（基于 HBase 思想优化为多 Master 高可用架构）：
#image("插图1.png")

系统整体架构分为 Zookeeper 集群管理、Master 集群（多 Master）、Region Server 和 Application 四个模块。其中，Region Server 底层由 miniSQL 提供服务，Application 底层由 Client 提供服务。

=== CAP 理论权衡
本系统在 CAP 三者中优先保证一致性(C)和分区容错性(P)，适当降低可用性(A)以换取数据正确性。
- 写操作采用“主副本写 WAL PREPARE → 并行同步至 ≥1 从副本 → 主提交 COMMIT”的半同步协议，若从副本超时则标记可疑，确保多数派一致。
- 分区发生时，ZooKeeper 临时节点机制可能导致部分 Region Server 被踢出集群，此时该节点的读写请求会失败，但集群整体仍能维持一致状态。
- 该取舍符合课程中“分布式系统无法同时达到 CA”的原则，适用于金融、账务等对数据错误零容忍的场景。

=== Zookeeper 集群管理

ZooKeeper 是一个分布式协调服务的开源框架。主要用来解决分布式集群中应用系统的一致性的问题，例如怎样避免同时操作同一数据造成脏读的问题。

ZooKeeper 本质上是一个分布式的小文件存储系统。提供基于类似于文件系统的目录树方式的数据存储，并且可以对树中的节点进行有效管理。从而来维护和监控你存储的数据的状态变化。将通过监控这些数据状态的变化，从而可以达到基于数据的集群管理。诸如：统一命名服务、分布式配置管理、分布式消息队列、分布式锁、分布式协调等功能。

本系统在 Zookeeper 中创建以下目录结构：
- `/masters`：临时节点，Active Master 和 Standby Master 在此注册。节点内容为 Master 的 IP:Port。所有 Master 监听此目录，通过序号最小的节点成为 Active。
- `/region_servers`：临时节点，每个 Region Server 启动时创建，内容为 RS 的 IP:Port 及当前表数量。Master 监听此目录以感知节点加入/退出。
- `/meta/tables`：持久节点，存储所有表的元数据（表名 → 主副本所在的 Region Server）。Master 更新此节点后，Client 可通过读取此处获得路由信息。
- `/assignments`：持久节点，记录每个表的副本分布（例如 `t1` 节点内容为 `["rs1:9090", "rs2:9090", "rs3:9090"]`）。

Watcher 机制：Master 在 `/region_servers` 上设置 Watcher，当节点消失时立即触发重新分配 Region 的流程。

=== Master 服务器（多 Master 高可用设计）

*领导者选举*：启动时在 Zookeeper `/masters` 下创建临时顺序节点，序号最小的节点成为 Active Master。Active Master 每 5 秒向 `/masters/active-heartbeat` 写入心跳时间戳，并维护 `/active-master` 领导信息。Standby Master 监听此节点，若 15 秒未更新则发起重新选举。

*负载均衡算法*：
- 静态分配：CREATE TABLE 时，Master 获取所有 Region Server 的当前表数量，选择表数最少的节点分配主副本。剩余两个副本分配到表数次少的两个不同节点。
- 动态重均衡：每 30 秒检查一次，若某 Region Server 的表数超过平均表数的 1.5 倍，则选择该节点上负载最低的一张表迁移到表数最少的节点。迁移过程见下文“Region 迁移”。

*Region 迁移流程*（以表 T1 从 RS-A 迁移到 RS-B 为例）：
1. Master 在 Zookeeper 中将 T1 的状态标记为 `MOVING`。
2. Master 通知 RS-A 暂停对 T1 的写请求（返回重定向响应）。
3. Master 命令 RS-B 从 RS-A 拷贝 T1 的 miniSQL 数据文件（通过 RPC 流式传输）。
4. 拷贝完成后，Master 更新 `/assignments/T1` 和 `/meta/tables` 指向 RS-B。
5. Master 通知 RS-A 删除 T1 本地数据，并将 T1 状态改为 `ACTIVE`。
6. 广播缓存失效消息给所有 Client。

*防脑裂机制*：
- Zookeeper 中维护 `/active-master` 持久节点，内容为 `{epoch: 5, masterId: master-001}`。
- Active Master 每 5 秒更新 `/masters/active-heartbeat` 临时节点的时间戳。
- Standby Master 监听 `/masters` 目录，当序号最小节点消失时，先检查 `/active-master` 的 epoch，若 epoch 未过期（心跳时间 < 15s），则延迟选举，否则递增 epoch 并写入新 Active 信息。
- 旧 Master 恢复后读取 `/active-master`，发现 epoch 更大，自动转为 Standby 并从 ZK 同步最新元数据。

=== Region 服务器

Region（从节点）的主要功能有：

- Region 服务器负责存储和维护分配给自己的 Region，处理来自客户端的读写请求。
- 简单起见，Region Server 利用 miniSQL 来管理 Region，负责 MiniSQL 的启动和管理，和 Client 的通信。
- Region 服务器提供缓存机制，对一些出现频率较高的查询进行缓存，可以优化一些高频率的查询的查询速度。

=== 客户端

客户端并不是直接从 Master 主服务器上读取数据，而是在获得 Region 的存储位置信息后，直接从 Region 服务器上读取数据。

客户端可以不依赖 Master，可以通过 Zookeeper 来获得 Region 位置信息（需要设计一套定位机制）或者从 Active Master 中获得，大多数客户端甚至从来不和 Master 通信，这种设计方式使得 Master 负载很小。

为减轻 Master 负担，在客户端采用了一定的缓存机制，保存 Table 定位信息。更详细的内容参见第三部分模块与功能详细设计部分。

== miniSQL 架构设计

miniSQL 是本分布式数据库系统项目中的核心，是运行在从节点中的单个数据库，并且可以执行一系列 SQL 语句并完成对应的操作，是一个精简的单用户 SQL 引擎，允许用户通过字符界面输入 SQL 语句实现数据库的一系列操作。

本项目中我们采用 Java 对原本的基于 C++ 的 miniSQL 数据库系统进行了重构，并做了如下架构设计：

主要需要实现的模块有 Interpreter、API、Record Manager、Buffer Manager 和 Catalog Manager 等，主要需要实现的功能和需求有：

- 数据类型：支持的数据类型是 int、float、char(n) 等三种类型，并且 char 类型的长度在 1 到 255 之间。
- 表的定义：一张表最多定义 32 个属性，属性可以设置为 unique 和 primary key。
- 索引的建立和删除：对于一张表的主属性自动建立 B+ 树索引，对于声明为 unique 的属性可以通过 SQL 语句来建立 B+ 树的索引，所有的索引都是单属性单值的。
- 查找记录：查找记录的过程中可以通过用 and 进行多个条件的连接。
- 插入和删除记录：插入只支持单条记录的插入，删除操作支持一条和多条记录的删除。
- 数据文件由一个或多个数据块组成，块大小应与缓冲区块大小相同。一个块中包含一条至多条记录，为简单起见，只要求支持定长记录的存储，且不要求支持记录的跨块存储。
- 为提高磁盘 I/O 操作的效率，缓冲区与文件系统交互的单位是块，块的大小应为文件系统与磁盘交互单位的整数倍，一般可定为 4KB 或 8KB。
- 本系统主要通过输入一系列 SQL 语句执行来完成相应的操作和功能，SQL 语句支持单行和多行的输入，最后必须用分号结尾作为 SQL 语句结束的标志。
- WAL（Write-Ahead Log）日志机制，用于 Crash Recovery 和副本同步。

#pagebreak()

= 模块与功能详细设计

== 主节点（多 Master）设计

主服务器 Master Server（多 Master 架构）主要负责管理和维护表的分区信息，维护 Region Server 列表，分配 Region，使得负载均衡。为减轻 Master 负担，在后续的设计提升中，客户端并不是直接从 Master 主服务器上读取数据，而是在获得 Region 的存储位置信息后，直接从 Region 服务器上读取数据。

具体来讲，Master Server 需要实现以下的设计功能。管理用户对表的增加、删除、修改、查询等操作。

- 实现不同 Region 服务器之间的负载均衡。
- 在 Region 分裂或合并后，负责重新调整 Region 的分布。
- 对发生故障失效的 Region 服务器上的 Region 进行迁移。
- 主节点中维护了数据表的分布信息，比如每张数据表所处的从节点，表中的行列信息等元信息。
- 维护一个从节点服务器列表，并管理从节点的元信息，负责从节点服务器的调度和分配。
- 有一定的容错容灾能力，当从节点发生故障的时候可以对失效的服务器进行数据的迁移。
- *多 Master 高可用*：Zookeeper 领导者选举 + 元数据实时同步。

== 从节点设计

Region server，即从节点服务器，可以说是分布式数据库中最核心的模块。Region server 负责存储和维护分配给自己的 Region，并利用 MiniSQL 来管理 Region。每个 Region server 负责 MiniSQL 的启动和管理，响应来自客户端的读写请求，将最终的结果返回客户端。

我们仿照 HBase 进行设计，在 HBase 中是一张大表按照行来进行分区，而在本分布式数据库中，每个 Region 都对应数据库中的一张表格，按表格来进行分区。所有表格的信息都存储在 Master 节点中的 META 元数据表中，Client 可以通过 Active Master 查询 META 元数据表快速的定位每个表格在哪个 Region 中，并随后访问对应的 Region server 发送读写请求。

为减轻 Master 负担，在客户端可以有缓存，保存每张表格的定位信息，从而直接访问对应的 Region server，减少客户端对 Master 的访问。

*客户端缓存结构*：
```java
// 缓存数据结构
Map<String, CachedTableLocation> cache = new ConcurrentHashMap<>();
class CachedTableLocation {
    String tableName;
    String primaryRSAddress;   // 主副本地址
    List<String> allReplicas;   // 所有副本地址
    long expireTime;            // 过期时间戳（默认 30 秒）
    long version;               // 元数据版本号，用于检测变更
}
```
== miniSQL 设计

=== Interpreter

Interpreter 模块直接与用户交互，主要实现以下功能：

- 程序流程控制，即启动并初始化 → 【接收命令、处理命令、显示命令结果】循环 → 退出流程。
- 接收并解释用户输入的命令，生成命令的内部数据结构表示，同时检查命令的语法正确性和语义正确性，对正确的命令调用 API 层提供的函数执行并显示执行结果，对不正确的命令显示错误信息。

=== API

API 模块是整个系统的核心，其主要功能为提供执行 SQL 语句的接口，供 Interpreter 层调用。

该接口以 Interpreter 层解释生成的命令内部表示为输入，根据 Catalog Manager 提供的信息确定执行规则，并调用 Record Manager、Index Manager 和 Catalog Manager 提供的相应接口进行执行，最后返回执行结果给 Interpreter 模块。

=== Catalog Manager

Catalog Manager 负责管理数据库的所有模式信息，包括：

- 数据库中所有表的定义信息，包括表的名称、表中字段（列）数、主键、定义在该表上的索引。
- 表中每个字段的定义信息，包括字段类型、是否唯一等。
- 数据库中所有索引的定义，包括所属表、索引建立在那个字段上等。

Catalog Manager 还必需提供访问及操作上述信息的接口，供 Interpreter 和 API 模块使用。

=== Record Manager

Record Manager 负责管理记录表中数据的数据文件。主要功能为实现数据文件的创建与删除（由表的定义与删除引起）、记录的插入、删除与查找操作，并对外提供相应的接口。其中记录的查找操作要求能够支持不带条件的查找和带一个条件的查找（包括等值查找、不等值查找和区间查找）。

数据文件由一个或多个数据块组成，块大小应与缓冲区块大小相同。一个块中包含一条至多条记录，为简单起见，只要求支持定长记录的存储，且不要求支持记录的跨块存储。

=== Index Manager

Index Manager 负责 B+ 树索引的实现，实现 B+ 树的创建和删除（由索引的定义与删除引起）、等值查找、插入键值、删除键值等操作，并对外提供相应的接口。

B+ 树中索引节点大小应与缓冲区的块大小相同(4096B)，B+ 树的叉数由节点大小与索引键大小计算得到。

=== Buffer Manager

Buffer Manager 负责缓冲区的管理，主要功能有：

- 根据需要，读取指定的数据到系统缓冲区或将缓冲区中的数据写出到文件。
- 实现缓冲区的替换算法，当缓冲区满时选择合适的页进行替换。
- 记录缓冲区中各页的状态，如是否被修改过等。
- 提供缓冲区页的 pin 功能，及锁定缓冲区的页，不允许替换出去。

为提高磁盘 I/O 操作的效率，缓冲区与文件系统交互的单位是块，块的大小应为文件系统与磁盘交互单位的整数倍，一般可定为 4KB 或 8KB，本项目中采用 4KB 作为块的大小。

=== 日志机制（WAL + 复制日志）

*WAL 日志条目格式*（二进制序列化）：

#table(
  columns: (auto, auto, 1fr),
  align: (center, center, left),
  [*字段*], [*类型*], [*说明*],
  [LSN], [long], [日志序列号，单调递增],
  [TxnID], [long], [事务ID（本系统为单条写入语句）],
  [TableID], [int], [表ID],
  [OpType], [byte], [1=INSERT, 2=UPDATE, 3=DELETE],
  [BeforeRow], [bytes], [更新前的行数据（用于 UNDO，可选）],
  [AfterRow], [bytes], [更新后的行数据],
  [Timestamp], [long], [写入时间戳],
)

*恢复流程（Crash Recovery）*：
1. Region Server 重启后，从磁盘读取最后一个 Checkpoint 之后的 WAL 文件。
2. 对于每条日志，检查该 LSN 对应的数据页是否已落盘（通过 Buffer Manager 的 PageLSN 比较）。
3. 若 PageLSN < 日志 LSN，则重做（REDO）该日志：调用 Record Manager 的 `applyInsert/Update/Delete`。
4. 恢复完成后，删除旧的 WAL 文件，新建 WAL。

*副本同步协议*：
- 主副本收到客户端的写请求后，先写入本地 WAL（状态=PREPARE），然后并行向 2 个从副本发送 `syncLog(WalEntry)` 请求。
- 等待至少 1 个从副本返回 ACK 后，主副本将本地日志状态改为 COMMITTED，并返回客户端成功。
- 若等待超时（3秒），则标记该从副本为可疑，后续读请求不再路由到它，并通知 Master 进行修复。

*一致性模型*：本系统提供线性一致性（Linearizability），即写操作一旦返回客户端成功，后续所有读操作（无论访问哪个副本）都能看到该写入。
- 主副本在收到 ≥1 个从副本 ACK 后，将本地日志状态改为 COMMITTED，并先返回客户端成功，再异步通知剩余从副本，以降低延迟。
- 若主副本在 COMMIT 后、返回前崩溃，客户端收到超时异常，但此时数据已在至少 2 个副本（主 + 1 从）上持久化，系统会通过新选举的主副本恢复该写入（由新主读取 WAL 并重放）。
- 从副本定期向主副本发送 `getLSN` 请求，发现落后时发起 `pullLog(startLSN)` 批量拉取。

=== 数据库文件系统

DB Files 也就是数据库的文件系统，指构成数据库的所有数据文件，主要由记录数据文件、索引数据文件、Catalog 数据文件和 WAL 日志文件组成。

=== 支持的 SQL 语句格式


- 创建表的语句：关键字为 `create table` 具体的语法格式如下
```sql 
create table table*name(
attribution1 date*type1,
attribution2 data*type2 (unique),
……
primary key(attribution*name)
);
```

创建索引的语句：关键字为 `create index`，具体的语法格式如下
```sql 
create index index*name on table*name (attribution*name);
```

 删除表的语句
 ```sql 
drop table table*name;
```
 删除索引的语句
 ```sql 
drop index index*name;
```
 选择语句，关键字为 `select`，只支持 `select *` 即显示全部属性
 ```sql 
select * from table*name;
select * from table*name where conditions;
 ```
  插入记录语句，关键字为 `insert into`
  ```sql
insert into table*name values (value1, value2, value3……);
```
 删除记录语句，关键字为 `delete from`
 ```sql
delete from table*name;
delete from table*name where conditions;
```
执行 SQL 脚本文件
```sql
execfile file_name
```
== 客户端设计

本项目中的客户端是提供给用户的操作界面，用于和用户进行交互，并和服务器建立稳定的网络通信连接，基于 RPC 通信协议进行通信。

=== 客户端工作原理

客户端并不是直接从 Master 主服务器上读取数据，而是在获得 Region 的存储位置信息后，直接从 Region 服务器上读取数据，也可以直接使用 Zookeeper 来获取 Region 的位置信息。

ZooKeeper 集群启动之后，将等待客户端连接。客户端将连接到 ZooKeeper 集合中的一个节点。它可以是领导或跟随者节点。一旦客户端被连接，节点将向特定客户端分配会话 ID 并向该客户端发送确认。如果客户端没有收到确认，它将尝试连接 ZooKeeper 集合中的另一个节点。一旦连接到节点，客户端将以有规律的间隔向节点发送心跳确认，以确保连接不会丢失。

=== RPC 通信协议

RPC 协议是一种网络通信协议，RPC 协议假定一些传输协议的存在，如 TCP 或 UDP，为通信程序之间携带信息数据。在 OSI 模型中，RPC 跨越了传输层和应用层。RPC 使得开发包括网络分布式多程序在内的应用程序更加容易。

RPC 采用 C/S 架构。本项目中客户端和服务器的通信拟采用 RPC 协议进行，同时采用 Thrift 语言来描述自定义通信格式的具体内容。

=== 客户端缓存机制

客户端来进行具体的查询之前需要获得对应的表的 Region Server 的相关信息，解析这一位置信息往往是比较耗时的。

因此我们为客户端设立了缓存机制，在内存中维护一个记录已知 Region Server 元信息的 HashMap，每次操作先从缓存中查询目标服务器是否已经获得其相关信息，这样可以较大地提高客户端的工作效率。缓存失效时自动向 Active Master 重新查询。

== 详细功能设计

本分布式数据库主要实现了数据表分区、副本维护、负载均衡、容错容灾、集群管理、分布式查询等分布式数据库所必须具备的基本功能。

=== 数据分布（核心新增）

每个 Region Server 负责维护若干个数据表，并使用 miniSQL 对其进行维护、管理、添加、删除、修改和查询等操作，同时 Master Server 上也维护了每个表中数据表的分布情况，并可以进行统一的管理和调度。

- *分布粒度*：以“整张表格（Table）”为单位（大幅降低难度，无需行级切分）。
- *负载均衡分配*：CREATE TABLE 时，Master 检查各 Region Server 当前负载（表数量、连接数），选择最空闲节点分配主副本，其余副本随机分布到不同节点（保证 3 副本跨节点）。
- *元数据路由*：Master 维护全局 META 表，客户端查询位置后直接访问对应 Region Server。

=== 副本维护

本项目采用主从复制的策略，在一系列 Region Server 中进行副本的保存和维护，并选择其中一个表为主副本，负责副本的复制操作，定期进行数据库文件副本的维护工作。

- *3 副本策略*：每张表维护 3 个副本，分布在不同物理节点。
- *写入确认策略*：主副本并行同步到从副本，收到至少 1 个从副本 ACK 后返回（2/3 多数派提交），其余副本异步追赶。
- *动态恢复*：副本数不足时，Master 指挥存活副本拷贝数据到新节点。

=== 负载均衡

Master Server 可以对 Region Server 进行统一的管理和调度，当检测到一台 Region Server 繁忙的时候，Master 会将其中的某些 Region 进行重新分配，同时对于客户端发出的请求也要进行适当的调度，防止出现部分 Region Server 过热而其他 Region Server 处于饥饿状态。

*负载检测指标*（每 10 秒由 Region Server 主动上报给 Master）：
- `table_count`：当前管理的表数量
- `qps_1min`：过去 1 分钟的平均查询数/秒
- `cpu_usage`：进程 CPU 使用率（通过 OperatingSystemMXBean 获取）
- `mem_usage`：JVM 堆内存使用率

*繁忙判定阈值*：
- 若某 RS 的 `qps_1min` > 集群平均 QPS × 2，或 `cpu_usage` > 80%，则判定为热点节点。
- 热点节点触发迁移：选择该节点上 QPS 贡献最大的表迁移到负载最低的节点。
- 迁移过程中，该表的写请求会被阻塞 0.5~2 秒（取决于数据量），需要在前端提示用户重试。

=== 容错容灾

当某个 Region Server 失效或者发生系统崩溃等情况时，Master Server 需要将 Region 进行重新分配。

- Zookeeper 临时节点 + Watcher 机制实时感知节点增删。
- 节点宕机 → 自动触发副本恢复 → 维持 3 副本。

=== 集群管理

- 动态感知机器加入与退出（临时节点 + 心跳）。
- Zookeeper 完全自动化，无需停机维护。

=== 分布式查询

- Client → Active Master（或 Zookeeper）查询位置 → 直连对应 Region Server 执行 SQL → 结果直接返回。
- 支持客户端位置缓存，失效自动重查。

#pagebreak()

= 项目计划与进度安排

== 系统测试用例设计
#align(center)[
  #table(
    columns: (auto, 1fr, 1fr, 1fr, auto),
    align: (center, left),
    stroke: 0.5pt,
    [*用例编号*], [*描述*], [*输入*], [*预期输出*], [*备注*],
    [01], [单表等值查询], [SELECT \* FROM users WHERE id = 1], [返回 id=1 的行], [验证基础查询],
    [02], [单表范围查询], [SELECT \* FROM products WHERE price > 100 AND price < 200], [返回价格在(100,200)的行], [验证多条件],
    [03], [字符串匹配查询], [SELECT \* FROM students WHERE name = '张三'], [返回姓名为张三的行], [验证 char 类型],
    [04], [插入记录], [INSERT INTO users VALUES (1, 'Alice', 25)], [成功，查询可见], [验证写入+副本同步],
    [05], [删除记录], [DELETE FROM users WHERE id = 1], [成功，查询不到 id=1], [验证删除+副本同步],
    [06], [SQL 脚本执行], [execfile test.sql], [依次执行文件中的 10 条 SQL], [验证批量执行],
    [07], [CREATE TABLE 负载均衡], [CREATE TABLE t1 ...], [表分配到负载最低节点], [验证数据分布],
    [08], [副本一致性], [INSERT 后查询 3 个副本], [3 副本数据完全一致], [验证副本维护],
    [09], [节点宕机容灾], [关闭一台 Region Server 后查询], [系统自动恢复副本，继续服务], [验证容错容灾],
    [10], [多 Master 切换], [关闭 Active Master], [Standby 自动成为新 Active], [验证多 Master 高可用],
    [11], [客户端缓存失效], [迁移表后查询], [缓存失效后重新路由], [验证缓存机制],
    [12-13], [DROP TABLE / DROP INDEX], [同模板], [成功], [验证 miniSQL],
  )
]
== 项目规划

#align(center)[
  #table(
  columns: (auto, auto, 1fr, 1fr),
  align: (center, center, left, left),
  [*周次*], [*时间*], [*任务*], [*可交付物/里程碑*],
  [W1], [4/29-5/5], [环境搭建 + 接口定义], [Zookeeper 集群跑通，Thrift IDL 定义完成],
  [W2], [5/6-5/12], [miniSQL 单机重构 + WAL], [单机版 miniSQL 通过所有 SQL 测试],
  [W3], [5/13-5/19], [Master 选举 + Region Server 注册], [多 Master 可切换，Region Server 可注册],
  [W4], [5/20-5/26], [数据分布 + 副本同步 + 负载均衡], [CREATE TABLE 可分配到多节点，3 副本强一致],
  [W5], [5/27-6/2], [Client 缓存 + 分布式查询], [客户端可直连 Region Server，缓存生效],
  [W6], [6/3-6/9], [容灾测试 + 性能调优], [节点宕机 10 秒内恢复，通过混沌测试],
  [W7], [6/10-6/16], [文档 + 演示准备], [报告定稿，演示视频录制],
)
]

#pagebreak()

= 总结


项目充分体现了软件工程的团队协作价值：从单机 miniSQL 到真正可扩展、可容错、可均衡的分布式数据库，我们在理论与实践的结合中收获巨大。最终系统将支持真实多机演示（笔记本局域网、多进程模拟），并通过老师现场测试来充分验证容错能力。

我们将在后续迭代中持续优化性能与一致性，交付一个稳定、高可用、符合设计预期的分布式 MiniSQL 系统。#set text(font: "TeX Gyre Termes Math")
