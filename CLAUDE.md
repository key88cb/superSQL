# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SuperSQL is a distributed relational database built on top of a single-node MiniSQL engine (C++). The repository has two distinct layers:

- **`minisql/cpp-core/`** — C++ storage engine (B+ tree, buffer pool, heap files). Do **not** refactor; the distributed layer wraps it via `ProcessBuilder`.
- **`java-master/` / `java-regionserver/` / `java-client/`** — Java 17 distributed layer (ZooKeeper coordination, Thrift RPC, WAL, replica sync, load balancing).
- **`rpc-proto/supersql.thrift`** — Single source of truth for all 4 RPC service interfaces.

Key design docs: `初期设计报告.typ` (authoritative design) and `分布式MiniSQL作业要求.md`.

## Build & Run

All commands run from `minisql/cpp-core/`.

```bash
# Build the interactive REPL
make main

# Run the REPL
./main

# Clean object files and executables
make clear

# Wipe all persisted database files
make clear_data
```

Manual full build:
```bash
g++ -o main main.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc interpreter.cc
```

## Testing

```bash
make test                  # Run all test suites in sequence
make test_basic            # Data-type and tuple primitives
make test_buffer_manager   # Page eviction, pin/unpin
make test_catalog_manager  # Table/index metadata persistence
make test_index_manager    # B+ tree CRUD and range search
make test_record_manager   # Record insert/delete/select
make test_api              # Full SQL operations through the API layer
make test_exhaustive       # 4000+ record stress test

make coverage              # Build with gcov, run tests, generate .gcov reports
make clear_coverage        # Delete stale .gcda/.gcno coverage data before re-running
```

SQL test scripts can be run from inside the REPL with:
```
execfile test_file.sql     -- 10,000-row stress script
execfile test_file1.sql    -- 1,000-row script
```

## Architecture

The engine is a classic 5-layer database with a **single global `BufferManager`** instance (declared `extern` and shared across all modules):

```
stdin
  └── Interpreter      parse SQL, dispatch to API
        └── API        coordinate catalog + record ops, merge multi-condition results
              ├── CatalogManager   schema metadata → database/catalog/catalog_file
              ├── RecordManager    heap file CRUD → database/data/<table>
              │     └── IndexManager   per-table B+ tree cache
              │           └── BPlusTree<T>   templated, header-only, leaf-chained
              └── BufferManager    512-frame clock-replacement page cache (2 MB)
                    └── disk        4 KB pages, addressed as (filename, block_id)
```

### Key design points

- **`const.h`**: `PAGESIZE = 4096`, `MAXFRAMESIZE = 512` (2 MB buffer pool).
- **`basic.h`**: Core value type `Data` uses type encoding `-1` = int, `0` = float, `1–255` = char(N). `Attribute` caps at 32 columns; `Index` at 10 per table.
- **`BPlusTree<T>`** (`bplustree.h`): Header-only template. Loads entire index from disk on construction (`readFromDiskAll`), writes back on destruction (`writtenbackToDiskAll`). Leaf nodes are linked for range scans. Degree is computed from key size vs. page size by `IndexManager::getDegree`.
- **`RecordManager`**: Lazy-loads one `IndexManager*` per table. Supports index-accelerated lookup when an index exists on the filter column; otherwise full-scans.
- **`API`**: Multi-condition SELECT uses `unionTable` (AND = intersection) and `joinTable` (OR = union) to merge two `Table` results by sorted key comparison.
- **`Interpreter`**: One instance per query. Catches all typed exceptions from lower layers and prints error messages. `EXEC_FILE` runs a `.sql` script by re-instantiating the interpreter per line.
- **Soft deletes**: Records on disk carry a deleted flag byte; `RecordManager` filters them on read.
- **Exceptions** (`exception.h`): 12 typed classes (e.g., `table_not_exist`, `primary_key_conflict`, `unique_conflict`) used for control flow across all layers.

### Storage layout

```
database/
  catalog/catalog_file      -- all table schemas (text-encoded pages)
  data/<table_name>         -- heap file, 4 KB blocks
  index/<index_file_name>   -- B+ tree leaf serialization, one file per index
```

All storage access goes through the shared `BufferManager` — there is no direct `fread`/`fwrite` outside of `buffer_manager.cc`.

## Distributed Layer (Java)

### Java testing convention (important)

- Use the shared test utility in `test-common/src/main/java/edu/zju/supersql/testutil/EmbeddedZkServerFactory.java` for embedded ZooKeeper in all Java tests.
- Do not instantiate `new TestingServer(true)` directly in module tests; this can reintroduce CI warning noise.
- `EmbeddedZkServerFactory` already sets both `maxCnxns` and `maxClientCnxns` to avoid the recurring warning:
  - `[zkservermainrunner] WARN  o.a.z.server.ServerCnxnFactory - maxCnxns is not configured, using default value 0.`
- For Java test changes, keep this dependency direction:
  - `java-master` / `java-regionserver` / `java-client` test scope depends on `test-common`.
  - Do not duplicate `EmbeddedZkServerFactory` under module-local test packages.

### Cluster startup
```bash
docker compose up -d --build   # start full cluster (3 ZK + 3 Master + 3 RS + 1 Client)
docker compose ps               # check health
docker exec -it client java -jar /app/client.jar   # interactive SQL REPL
```

### Java module build (from repo root)
```bash
mvn install -DskipTests         # build all modules
mvn -pl java-master package     # build single module
```

### Generate Thrift stubs (run after editing supersql.thrift)
```bash
thrift --gen java -out rpc-proto/src/main/java rpc-proto/supersql.thrift
```

### ZooKeeper directory structure
```
/masters/               临时顺序节点，领导者选举
/active-master          持久节点，{epoch, masterId}，防脑裂
/region_servers/        临时节点，RS 注册 + 心跳
/meta/tables/<name>     持久节点，主副本地址 + 状态
/assignments/<name>     持久节点，3 副本地址列表
```

### Distributed architecture decisions
- **Table-level distribution**: one whole table = one Region, no row-level sharding.
- **CP over AP**: semi-sync replication (primary + ≥1 replica ACK before commit). Partition may cause write failures but never data inconsistency.
- **MiniSQL integration**: Java `RegionServer` manages the C++ `main` process via `ProcessBuilder`; all SQL goes through `stdin`/`stdout`. Do not call miniSQL's C++ code directly from Java.
- **RPC**: Thrift `TThreadPoolServer` + `TFramedTransport`. RegionServer uses `TMultiplexedProcessor` to expose `RegionService` + `RegionAdminService` + `ReplicaSyncService` on the same port (9090).
- **WAL format**: binary append-only, fields: `LSN(8B) | TxnID(8B) | TableId(4B) | OpType(1B) | PayloadLen(4B) | Payload`. `FileChannel.force(false)` after every append.
- **Client cache**: `ConcurrentHashMap<String, CachedTableLocation>`, TTL = 30 s, version-number invalidation.

### Key Java source locations (once implemented)
| Component | Path |
|---|---|
| Leader election | `java-master/…/election/LeaderElector.java` |
| Metadata management | `java-master/…/meta/MetaManager.java` |
| Load balancer | `java-master/…/balance/LoadBalancer.java` |
| Region migration | `java-master/…/migration/RegionMigrator.java` |
| miniSQL process wrapper | `java-regionserver/…/minisql/MiniSqlProcess.java` |
| WAL manager | `java-regionserver/…/wal/WalManager.java` |
| Replica sync | `java-regionserver/…/replica/ReplicaManager.java` |
| Client REPL | `java-client/…/client/SqlClient.java` |
| Route cache | `java-client/…/client/cache/RouteCache.java` |

Full sprint plan with task priorities, acceptance criteria, and technical guidance: `DEVELOPMENT_PLAN.md`.
