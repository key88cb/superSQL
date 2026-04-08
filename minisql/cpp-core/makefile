CXX = g++
TEST_DIR ?= tests

CORE_SRCS = interpreter.cc api.cc record_manager.cc buffer_manager.cc catalog_manager.cc index_manager.cc basic.cc log_manager.cc
CORE_OBJS = $(CORE_SRCS:.cc=.o)
MAIN_SRC = main.cc

TEST_BASIC_SRCS = $(TEST_DIR)/test_basic.cc basic.cc
TEST_BUFFER_MANAGER_SRCS = $(TEST_DIR)/test_buffer_manager.cc buffer_manager.cc log_manager.cc
TEST_CATALOG_MANAGER_SRCS = $(TEST_DIR)/test_catalog_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc
TEST_INDEX_MANAGER_SRCS = $(TEST_DIR)/test_index_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc
TEST_RECORD_MANAGER_SRCS = $(TEST_DIR)/test_record_manager.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc
TEST_API_SRCS = $(TEST_DIR)/test_api.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc
TEST_EXHAUSTIVE_SRCS = $(TEST_DIR)/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc
TEST_WAL_SRCS = $(TEST_DIR)/test_wal.cc log_manager.cc buffer_manager.cc
TEST_WAL_CRASH_RECOVERY_SRCS = $(TEST_DIR)/test_wal_crash_recovery.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc

# 调试模式开关，默认为 0 (Release)
# 使用方式: make DEBUG=1 或者 make debug
DEBUG ?= 0

CXXFLAGS = -Wall

ifeq ($(DEBUG), 1)
    CXXFLAGS += -g -O0 -DDEBUG
    $(info >>> Compiling in DEBUG mode...)
else
    CXXFLAGS += -O2
    $(info >>> Compiling in RELEASE mode...)
endif

COVERAGE_FLAGS = -fprofile-arcs -ftest-coverage

.PHONY: all debug release clear clear_coverage clear_data test

all: main

debug:
	$(MAKE) DEBUG=1

release:
	$(MAKE) DEBUG=0

main: main.o $(CORE_OBJS)
	$(CXX) -o $@ $(CXXFLAGS) main.o $(CORE_OBJS)

main.o: main.cc interpreter.h catalog_manager.h basic.h buffer_manager.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) $<

interpreter.o: interpreter.cc interpreter.h catalog_manager.h basic.h buffer_manager.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) $<

api.o: api.cc api.h buffer_manager.h record_manager.h const.h basic.h catalog_manager.h buffer_manager.h exception.h catalog_manager.h template_function.h
	$(CXX) -c $(CXXFLAGS) $<

record_manager.o: record_manager.cc record_manager.h index_manager.h catalog_manager.h buffer_manager.h bplustree.h exception.h basic.h const.h template_function.h
	$(CXX) -c $(CXXFLAGS) $<

buffer_manager.o: buffer_manager.cc buffer_manager.h const.h log_manager.h
	$(CXX) -c $(CXXFLAGS) $<

log_manager.o: log_manager.cc log_manager.h const.h
	$(CXX) -c $(CXXFLAGS) $<

catalog_manager.o: catalog_manager.cc catalog_manager.h buffer_manager.h basic.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) $<

index_manager.o: index_manager.cc index_manager.h basic.h buffer_manager.h bplustree.h const.h
	$(CXX) -c $(CXXFLAGS) $<

basic.o: basic.cc basic.h
	$(CXX) -c $(CXXFLAGS) $<

# Testing
test_basic: $(TEST_DIR)/test_basic.cc basic.cc basic.h $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_BASIC_SRCS)
	./test_basic

test_buffer_manager: $(TEST_DIR)/test_buffer_manager.cc buffer_manager.cc buffer_manager.h log_manager.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_BUFFER_MANAGER_SRCS)
	./test_buffer_manager

test_catalog_manager: $(TEST_DIR)/test_catalog_manager.cc catalog_manager.cc catalog_manager.h buffer_manager.cc buffer_manager.h log_manager.cc $(TEST_DIR)/test_macros.h basic.cc
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_CATALOG_MANAGER_SRCS)
	./test_catalog_manager

test_index_manager: $(TEST_DIR)/test_index_manager.cc index_manager.cc index_manager.h catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_INDEX_MANAGER_SRCS)
	./test_index_manager

test_record_manager: $(TEST_DIR)/test_record_manager.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_RECORD_MANAGER_SRCS)
	./test_record_manager

test_api: $(TEST_DIR)/test_api.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_API_SRCS)
	./test_api

test_exhaustive: $(TEST_DIR)/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_EXHAUSTIVE_SRCS)
	./test_exhaustive

test_wal: $(TEST_DIR)/test_wal.cc log_manager.cc log_manager.h buffer_manager.cc buffer_manager.h $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_WAL_SRCS)
	./test_wal

test_wal_crash_recovery: $(TEST_DIR)/test_wal_crash_recovery.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc $(TEST_DIR)/test_macros.h
	$(CXX) -o $@ $(CXXFLAGS) $(COVERAGE_FLAGS) $(TEST_WAL_CRASH_RECOVERY_SRCS)
	./test_wal_crash_recovery

test: clear_coverage test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api test_exhaustive test_wal test_wal_crash_recovery

coverage: clear_coverage test
	gcov basic.cc buffer_manager.cc catalog_manager.cc index_manager.cc record_manager.cc api.cc

# 清理编译产生的临时文件和可执行文件
clear:

ifeq ($(OS),Windows_NT)
	-del /f /q *.o main.exe test_basic.exe test_buffer_manager.exe test_catalog_manager.exe test_index_manager.exe test_record_manager.exe test_api.exe test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api 2>nul
else
	-rm -f *.o main test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api test_exhaustive test_wal test_wal_crash_recovery
endif

# 清理覆盖率数据文件，防止 Data file mismatch 错误
clear_coverage:

ifeq ($(OS),Windows_NT)
	-del /f /q *.gcda *.gcno 2>nul
else
	-rm -f *.gcda *.gcno
endif

# 清理数据库数据文件
clear_data:

ifeq ($(OS),Windows_NT)
	-del /f /q database\data\* database\index\* database\catalog\* 2>nul
else
	-rm -f database/data/* database/index/* database/catalog/*
endif