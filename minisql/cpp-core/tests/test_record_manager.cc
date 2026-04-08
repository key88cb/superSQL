#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include "../record_manager.h"
#include "../catalog_manager.h"
#include "../buffer_manager.h"
#include "../index_manager.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

BufferManager* buffer_manager_ptr = new BufferManager(); // Initialize the global pointer

static void run_cmd_or_fail(const char* cmd) {
    int rc = system(cmd);
    if (rc == -1) {
        std::cerr << "Failed to run command: " << cmd << std::endl;
        std::exit(1);
    }
}

void setup_env() {
#ifdef _WIN32
    run_cmd_or_fail("if not exist database\\data mkdir database\\data");
    run_cmd_or_fail("if not exist database\\index mkdir database\\index");
    run_cmd_or_fail("if not exist database\\catalog mkdir database\\catalog");
    run_cmd_or_fail("del /q database\\data\\* 2>nul");
    run_cmd_or_fail("del /q database\\index\\* 2>nul");
    run_cmd_or_fail("del /q database\\catalog\\* 2>nul");
#else
    run_cmd_or_fail("mkdir -p database/data database/index database/catalog");
    run_cmd_or_fail("rm -f database/data/* database/index/* database/catalog/*");
#endif
    // Clear buffer manager cache
    buffer_manager.clear();
    
    // Initialize catalog file
    std::string cat_path = "./database/catalog/catalog_file";
    std::ofstream ofs(cat_path, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    ofs.write(buf, PAGESIZE);
    ofs.close();
}

void test_record_insert_select() {
    std::cout << "Running test_record_insert_select..." << std::endl;
    RecordManager rm;
    CatalogManager cm;
    
    std::string table_name = "students";
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "age"; attr.type[1] = -1; attr.unique[1] = false;
    attr.primary_key = 0;
    attr.has_index[0] = false;
    attr.has_index[1] = false;
    Index idx; idx.num = 0;

    cm.createTable(table_name, attr, 0, idx);
    rm.createTableFile(table_name);

    // Insert records
    Tuple t1;
    Data d1a; d1a.type = -1; d1a.datai = 1;
    Data d1b; d1b.type = -1; d1b.datai = 20;
    t1.addData(d1a); t1.addData(d1b);
    
    rm.insertRecord(table_name, t1);

    Tuple t2;
    Data d2a; d2a.type = -1; d2a.datai = 2;
    Data d2b; d2b.type = -1; d2b.datai = 22;
    t2.addData(d2a); t2.addData(d2b);
    
    rm.insertRecord(table_name, t2);

    // Select all
    Table res = rm.selectRecord(table_name);
    ASSERT_EQ(res.getTuple().size(), 2);
    
    // Select with where
    Where w;
    w.data.type = -1;
    w.data.datai = 1;
    w.relation_character = EQUAL;
    
    Table res_cond = rm.selectRecord(table_name, "id", w);
    ASSERT_EQ(res_cond.getTuple().size(), 1);
    ASSERT_EQ(res_cond.getTuple()[0].getData()[0].datai, 1);

    // Delete with where
    int deleted = rm.deleteRecord(table_name, "id", w);
    ASSERT_EQ(deleted, 1);
    
    Table res_after_del = rm.selectRecord(table_name);
    ASSERT_EQ(res_after_del.getTuple().size(), 1);
    ASSERT_EQ(res_after_del.getTuple()[0].getData()[0].datai, 2);

    cm.dropTable(table_name);
    rm.dropTableFile(table_name);
}

void test_record_conflicts() {
    std::cout << "Running test_record_conflicts..." << std::endl;
    RecordManager rm;
    CatalogManager cm;
    
    std::string table_name = "unique_table";
    Attribute attr;
    attr.num = 1;
    attr.name[0] = "val"; attr.type[0] = -1; attr.unique[0] = true;
    attr.primary_key = 0;
    Index idx; idx.num = 0;

    cm.createTable(table_name, attr, 0, idx);
    rm.createTableFile(table_name);

    Tuple t1;
    Data d; d.type = -1; d.datai = 100;
    t1.addData(d);
    
    rm.insertRecord(table_name, t1);

    // Attempt to insert duplicate primary key
    bool caught = false;
    try {
        rm.insertRecord(table_name, t1);
    } catch (const primary_key_conflict& e) {
        caught = true;
    }
    ASSERT_TRUE(caught);

    cm.dropTable(table_name);
    rm.dropTableFile(table_name);
}

int main() {
    setup_env();
    test_record_insert_select();
    setup_env(); // Reset for conflicts test
    test_record_conflicts();
    TEST_REPORT();
}
