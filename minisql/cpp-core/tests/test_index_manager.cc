#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include "../index_manager.h"
#include "../catalog_manager.h"
#include "../buffer_manager.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

BufferManager buffer_manager; // Extern

void setup_env() {
    std::string path = "./database/catalog/catalog_file";
    std::ofstream ofs(path, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    ofs.write(buf, PAGESIZE);
    ofs.close();
}

void test_index_int() {
    std::cout << "Running test_index_int..." << std::endl;
    CatalogManager cm;
    std::string table_name = "int_table";
    Attribute attr;
    attr.num = 1;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.primary_key = 0;
    attr.has_index[0] = true;
    Index idx; idx.num = 0;
    
    cm.createTable(table_name, attr, 0, idx);
    cm.createIndex(table_name, "id", "id_index");

    IndexManager im(table_name);
    std::string index_path = "INDEX_FILE_id_" + table_name;
    
    Data d1; d1.type = -1; d1.datai = 100;
    Data d2; d2.type = -1; d2.datai = 200;
    
    im.insertIndex(index_path, d1, 42); // block_id 42
    im.insertIndex(index_path, d2, 84); // block_id 84

    int res1 = im.findIndex(index_path, d1);
    ASSERT_EQ(res1, 42);
    
    int res2 = im.findIndex(index_path, d2);
    ASSERT_EQ(res2, 84);

    // Range search
    std::vector<int> results;
    im.searchRange(index_path, d1, d2, results);
    // [100, 200]
    ASSERT_EQ(results.size(), 2);

    // Delete
    im.deleteIndexByKey(index_path, d1);
    ASSERT_EQ(im.findIndex(index_path, d1), -1);

    cm.dropTable(table_name);
}

void test_index_string() {
    std::cout << "Running test_index_string..." << std::endl;
    CatalogManager cm;
    std::string table_name = "str_table";
    Attribute attr;
    attr.num = 1;
    attr.name[0] = "name"; attr.type[0] = 10; attr.unique[0] = true; // char(10)
    attr.primary_key = 0;
    attr.has_index[0] = true;
    Index idx; idx.num = 0;
    
    cm.createTable(table_name, attr, 0, idx);
    cm.createIndex(table_name, "name", "name_idx");

    IndexManager im(table_name);
    std::string index_path = "INDEX_FILE_name_" + table_name;

    Data s1; s1.type = 10; s1.datas = "alice";
    Data s2; s2.type = 10; s2.datas = "bob";

    im.insertIndex(index_path, s1, 1);
    im.insertIndex(index_path, s2, 2);

    ASSERT_EQ(im.findIndex(index_path, s1), 1);
    ASSERT_EQ(im.findIndex(index_path, s2), 2);

    cm.dropTable(table_name);
}

int main() {
    setup_env();
    test_index_int();
    test_index_string();
    TEST_REPORT();
}
