# superSQL

从 miniSQL 演进而来的分布式 SQL 数据库系统。

## 项目结构

- `cpp-core/`: C++ 存储引擎及核心 SQL 处理逻辑。
- `java-master/`: (计划中) 集群管理与协调服务。
- `java-client/`: (计划中) superSQL Java 客户端 SDK。
- `rpc-proto/`: 组件间的 RPC 协议定义。

## C++ 核心引擎 (`cpp-core`)

核心引擎负责缓冲池管理 (Buffer Management)、B+ 树索引 (B+ Tree Indexing) 以及记录管理 (Record Management)。

### 前置条件

- **Windows**: MinGW-w64 (g++) 和 Make。
- **Linux/Unix**: g++ 和 Make。

### 编译指南

进入 `cpp-core` 目录：

```bash
cd cpp-core
make clear_data  # 清理旧的数据库文件
# 编译主解析器 (Interpreter):
g++ -o main main.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc interpreter.cc
```

### 测试说明

我们提供了一套完整的测试套件，位于 `cpp-core/tests/` 目录下。

#### 运行测试

1. **基础功能测试**:
   ```bash
   g++ -o test_basic tests/test_basic.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_basic
   ```

2. **详尽压力测试 (4000+ 记录)**:
   该测试验证系统在高负载下的稳定性，特别是针对内存泄漏和缓冲池耗尽的检查。
   ```bash
   g++ -o test_exhaustive tests/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_exhaustive
   ```

3. **缓冲池 Pin 诊断**:
   用于调试缓冲池管理器 (Buffer Manager) 的 pin 泄漏问题。
   ```bash
   g++ -o test_pin_count tests/test_pin_count.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_pin_count
   ```

### 当前状态

- **内存管理**: ✅ **已解决**。详尽压力测试 (4000条记录) 已通过验证。我们通过重构 `IndexManager` 架构、强化 `readTuple` 解析边界检查以及将缓冲池扩展至 512 帧，彻底解决了之前的 `std::bad_alloc` (OOM) 崩溃问题。

## 未来路线图

- [ ] 实现基于 gRPC 的通信层。
- [ ] 集成 Zookeeper 进行主节点选举。
- [ ] 实现分布式查询路由。