# superSQL

Distributed SQL database system evolved from miniSQL.

## Project Structure

- `cpp-core/`: The C++ storage engine and core SQL processing logic.
- `java-master/`: (Planned) Cluster management and coordination service.
- `java-client/`: (Planned) Java client SDK for superSQL.
- `rpc-proto/`: RPC protocol definitions between components.

## C++ Core (`cpp-core`)

The core engine handles buffer management, B+ tree indexing, and record management.

### Prerequisites

- **Windows**: MinGW-w64 (g++) and Make.
- **Linux/Unix**: g++ and Make.

### Compilation

Navigate to the `cpp-core` directory:

```bash
cd cpp-core
make clear_data  # Clean previous database files
# To build the main interpreter:
g++ -o main main.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc interpreter.cc
```

### Testing

We use a custom testing suite located in `cpp-core/tests/`.

#### Running Tests

1. **Basic Functionality**:
   ```bash
   g++ -o test_basic tests/test_basic.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_basic
   ```

2. **Exhaustive Stress Test (4000+ Records)**:
   This test validates the system under high load, specifically checking for memory leaks and buffer exhaustion.
   ```bash
   g++ -o test_exhaustive tests/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_exhaustive
   ```

3. **Buffer Pin Diagnostic**:
   Used for debugging buffer manager pin leaks.
   ```bash
   g++ -o test_pin_count tests/test_pin_count.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
   ./test_pin_count
   ```

### Known Issues

- **Memory Management**: The exhaustive stress test currently encounters a `std::bad_alloc` (OOM) error during the post-insertion phase (around 4000 records). We are currently debugging the `selectRecord` and `createIndex` memory footprints.

## Future Roadmap

- [ ] Implementation of gRPC based communication layer.
- [ ] Zookeeper integration for master node election.
- [ ] Distributed query routing.