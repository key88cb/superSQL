CXX = g++
CXXFLAGS = -g -Wall
COVERAGE_FLAGS = -fprofile-arcs -ftest-coverage

OBJS = interpreter.o api.o record_manager.o buffer_manager.o catalog_manager.o index_manager.o basic.o

main: main.o $(OBJS)
	$(CXX) -o main $(CXXFLAGS) main.o $(OBJS)

main.o: main.cc interpreter.h catalog_manager.h basic.h buffer_manager.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) main.cc

interpreter.o: interpreter.cc interpreter.h catalog_manager.h basic.h buffer_manager.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) interpreter.cc

api.o: api.cc api.h buffer_manager.h record_manager.h const.h basic.h catalog_manager.h buffer_manager.h exception.h catalog_manager.h template_function.h
	$(CXX) -c $(CXXFLAGS) api.cc

record_manager.o: record_manager.cc record_manager.h index_manager.h catalog_manager.h buffer_manager.h bplustree.h exception.h basic.h const.h template_function.h
	$(CXX) -c $(CXXFLAGS) record_manager.cc

buffer_manager.o: buffer_manager.cc buffer_manager.h const.h
	$(CXX) -c $(CXXFLAGS) buffer_manager.cc

catalog_manager.o: catalog_manager.cc catalog_manager.h buffer_manager.h basic.h exception.h const.h
	$(CXX) -c $(CXXFLAGS) catalog_manager.cc

index_manager.o: index_manager.cc index_manager.h basic.h buffer_manager.h bplustree.h const.h
	$(CXX) -c $(CXXFLAGS) index_manager.cc

basic.o: basic.cc basic.h
	$(CXX) -c $(CXXFLAGS) basic.cc

# Testing
test_basic: tests/test_basic.cc basic.cc basic.h tests/test_macros.h
	$(CXX) -o test_basic $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_basic.cc basic.cc
	./test_basic

test_buffer_manager: tests/test_buffer_manager.cc buffer_manager.cc buffer_manager.h tests/test_macros.h
	$(CXX) -o test_buffer_manager $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_buffer_manager.cc buffer_manager.cc
	./test_buffer_manager

test_catalog_manager: tests/test_catalog_manager.cc catalog_manager.cc catalog_manager.h buffer_manager.cc buffer_manager.h tests/test_macros.h basic.cc
	$(CXX) -o test_catalog_manager $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_catalog_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
	./test_catalog_manager

test_index_manager: tests/test_index_manager.cc index_manager.cc index_manager.h catalog_manager.cc buffer_manager.cc basic.cc tests/test_macros.h
	$(CXX) -o test_index_manager $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_index_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
	./test_index_manager

test_record_manager: tests/test_record_manager.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc tests/test_macros.h
	$(CXX) -o test_record_manager $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_record_manager.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
	./test_record_manager

test_api: tests/test_api.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc tests/test_macros.h
	$(CXX) -o test_api $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_api.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
	./test_api

test_exhaustive: tests/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc tests/test_macros.h
	$(CXX) -o test_exhaustive $(CXXFLAGS) $(COVERAGE_FLAGS) tests/test_exhaustive.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc basic.cc
	./test_exhaustive

test: test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api test_exhaustive

coverage: clear_coverage test
	gcov basic.cc buffer_manager.cc catalog_manager.cc index_manager.cc record_manager.cc api.cc

# 清理编译产生的临时文件和可执行文件
clear:
	-del /f /q *.o main.exe test_basic.exe test_buffer_manager.exe test_catalog_manager.exe test_index_manager.exe test_record_manager.exe test_api.exe test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api 2>nul

# 清理覆盖率数据文件，防止 Data file mismatch 错误
clear_coverage:
	-del /f /q *.gcda *.gcno 2>nul

# 清理数据库数据文件
clear_data:
	-del /f /q database\data\* database\index\* database\catalog\* 2>nul