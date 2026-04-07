/**
 * SuperSQL — Thrift IDL 接口定义
 *
 * 覆盖以下所有 RPC 边界：
 *   1. Client  <──> Master        (MasterService)
 *   2. Client  <──> RegionServer  (RegionService)
 *   3. Master  <──> RegionServer  (RegionAdminService)
 *   4. RegionServer <──> RegionServer (ReplicaSyncService)
 *
 * 编译命令（生成 Java 代码）：
 *   thrift --gen java -out rpc-proto/src/main/java rpc-proto/supersql.thrift
 */

namespace java edu.zju.supersql.rpc
namespace cpp  supersql.rpc

// ================================================================
//  公共枚举与结构体
// ================================================================

/** 操作结果状态码 */
enum StatusCode {
    OK              = 0,
    ERROR           = 1,
    TABLE_NOT_FOUND = 2,
    TABLE_EXISTS    = 3,
    RS_NOT_FOUND    = 4,
    REDIRECT        = 5,   // 客户端应重定向到 redirectTo 地址
    MOVING          = 6,   // 表正在迁移，请稍后重试
    NOT_LEADER      = 7,   // 此 Master 不是 Active Master
    REPLICA_TIMEOUT = 8,   // 副本同步超时（已标记可疑）
}

/** 通用响应包装 */
struct Response {
    1: required StatusCode code,
    2: optional string     message,
    3: optional string     redirectTo,  // REDIRECT 时填写目标地址 host:port
}

/** RegionServer 节点信息 */
struct RegionServerInfo {
    1: required string id,          // e.g. "rs-1"
    2: required string host,
    3: required i32    port,
    4: optional i32    tableCount,
    5: optional double qps1min,
    6: optional double cpuUsage,
    7: optional double memUsage,
    8: optional i64    lastHeartbeat,
}

/** 表的路由元数据 */
struct TableLocation {
    1: required string       tableName,
    2: required RegionServerInfo primaryRS,     // 主副本
    3: required list<RegionServerInfo> replicas, // 全部3副本（含主）
    4: optional string       tableStatus,       // ACTIVE | MOVING | OFFLINE
    5: optional i64          version,           // 元数据版本号
}

/** SQL 执行结果（单条查询行）*/
struct Row {
    1: required list<string> values,
}

/** SQL 执行返回集合 */
struct QueryResult {
    1: required Response           status,
    2: optional list<string>       columnNames,
    3: optional list<Row>          rows,
    4: optional i64                affectedRows,
    5: optional string             errorMessage,
}

/** WAL 日志条目（用于副本同步）*/
enum WalOpType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CREATE_TABLE = 4,
    DROP_TABLE   = 5,
    CREATE_INDEX = 6,
    DROP_INDEX   = 7,
}

struct WalEntry {
    1: required i64        lsn,          // 日志序列号，单调递增
    2: required i64        txnId,        // 事务/语句 ID
    3: required string     tableName,
    4: required WalOpType  opType,
    5: optional binary     beforeRow,    // UNDO 数据（DELETE/UPDATE 用）
    6: optional binary     afterRow,     // REDO 数据（INSERT/UPDATE 用）
    7: required i64        timestamp,
}

/** 数据文件传输块（Region 迁移用）*/
struct DataChunk {
    1: required string  tableName,
    2: required string  fileName,       // 文件相对路径
    3: required i64     offset,         // 块起始偏移
    4: required binary  data,           // 实际字节内容
    5: required bool    isLast,         // 最后一块标志
}

// ================================================================
//  1. MasterService — Client 向 Master 发起的 RPC
// ================================================================
service MasterService {

    /** 查询指定表所在的 RegionServer（主副本地址）。客户端缓存 TTL=30s。 */
    TableLocation getTableLocation(1: string tableName),

    /** 创建表。Master 选择最空闲节点分配主副本，另选 2 个节点存放从副本。ddl 为完整 CREATE TABLE SQL。 */
    Response createTable(1: string ddl),

    /** 删除表。Master 协调所有副本删除数据后更新元数据。 */
    Response dropTable(1: string tableName),

    /** 获取当前 Active Master 地址（Standby 收到请求时响应中返回 REDIRECT）。 */
    string getActiveMaster(),

    /** 列出所有在线 RegionServer 及负载信息（管理接口）。 */
    list<RegionServerInfo> listRegionServers(),

    /** 列出所有表的路由信息（管理接口）。 */
    list<TableLocation> listTables(),

    /** 手动触发负载均衡（管理接口，正常由 Master 自动触发）。 */
    Response triggerRebalance(),
}

// ================================================================
//  2. RegionService — Client 向 RegionServer 发起的 SQL RPC
// ================================================================
service RegionService {

    /**
     * 执行任意 SQL 语句。RegionServer 转发给本地 miniSQL 进程，解析输出后封装为 QueryResult。
     * 写操作先写 WAL，同步到从副本收到 ≥1 个 ACK 后返回 OK。
     */
    QueryResult execute(1: string tableName, 2: string sql),

    /** 批量执行（execfile 语义）。 */
    QueryResult executeBatch(1: string tableName, 2: list<string> sqls),

    /** 创建索引（转发 CREATE INDEX 到 miniSQL）。 */
    Response createIndex(1: string tableName, 2: string ddl),

    /** 删除索引。 */
    Response dropIndex(1: string tableName, 2: string indexName),

    /** 健康检查。 */
    Response ping(),
}

// ================================================================
//  3. RegionAdminService — Master 向 RegionServer 发起的管理 RPC
// ================================================================
service RegionAdminService {

    /** Master 命令 RS 暂停对某张表的写入（迁移前）。超时返回 MOVING。 */
    Response pauseTableWrite(1: string tableName),

    /** Master 命令 RS 恢复某张表的写入。 */
    Response resumeTableWrite(1: string tableName),

    /** Master 命令源 RS 将数据文件流式传输到目标 RS。 */
    Response transferTable(1: string tableName, 2: string targetHost, 3: i32 targetPort),

    /** Master 命令 RS 接收数据块（transferTable 内部调用）。 */
    Response copyTableData(1: DataChunk chunk),

    /** Master 命令 RS 删除本地某张表的所有数据（迁移完成后）。 */
    Response deleteLocalTable(1: string tableName),

    /** RS 向 Master 注册自身（启动时调用一次）。 */
    Response registerRegionServer(1: RegionServerInfo info),

    /** RS 向 Master 发送心跳（含最新负载指标，每 10 秒一次）。 */
    Response heartbeat(1: RegionServerInfo info),

    /** Master 通知 RS 广播缓存失效消息（表迁移后）。 */
    Response invalidateClientCache(1: string tableName),
}

// ================================================================
//  4. ReplicaSyncService — RegionServer 之间副本同步 RPC
// ================================================================
service ReplicaSyncService {

    /** 主副本将 WAL 条目同步到从副本。从副本写入本地 WAL 后返回 ACK。 */
    Response syncLog(1: WalEntry entry),

    /** 从副本拉取日志（落后时批量追赶）。startLsn 为从副本当前最大 LSN + 1。 */
    list<WalEntry> pullLog(1: string tableName, 2: i64 startLsn),

    /** 从副本查询当前最大 LSN（主副本定期检查落后情况）。 */
    i64 getMaxLsn(1: string tableName),

    /** 主副本确认提交（COMMIT）通知从副本更新状态。 */
    Response commitLog(1: string tableName, 2: i64 lsn),
}
