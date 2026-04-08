#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include "../catalog_manager.h"
#include "../buffer_manager.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

BufferManager* buffer_manager_ptr = new BufferManager(); // Initialize the global pointer

void setup_catalog_file() {
    std::string path = "./database/catalog/catalog_file";
    std::ofstream ofs(path, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    ofs.write(buf, PAGESIZE);
    ofs.close();
}

void test_catalog_table_basic() {
    std::cout << "Running test_catalog_table_basic..." << std::endl;
    CatalogManager cm;
    std::string table_name = "test_table";
    
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "score"; attr.type[1] = 0; attr.unique[1] = false;
    attr.primary_key = 0;

    Index idx;
    idx.num = 0;

    // Create table metadata
    try {
        cm.createTable(table_name, attr, 0, idx);
        ASSERT_TRUE(cm.hasTable(table_name));
        ASSERT_TRUE(cm.hasAttribute(table_name, "id"));
        ASSERT_TRUE(cm.hasAttribute(table_name, "score"));
        ASSERT_FALSE(cm.hasAttribute(table_name, "nonexistent"));

        Attribute fetched_attr = cm.getAttribute(table_name);
        ASSERT_EQ(fetched_attr.num, 2);
        ASSERT_EQ(fetched_attr.name[0], std::string("id"));
        ASSERT_EQ(fetched_attr.primary_key, 0);

        // Drop table
        cm.dropTable(table_name);
        ASSERT_FALSE(cm.hasTable(table_name));

    } catch (const std::exception& e) {
        std::cerr << "Unexpected exception: " << std::endl;
        ASSERT_TRUE(false);
    }
}

void test_catalog_index() {
    std::cout << "Running test_catalog_index..." << std::endl;
    CatalogManager cm;
    std::string table_name = "idx_table";
    
    Attribute attr;
    attr.num = 1;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.primary_key = 0;
    Index idx; idx.num = 0;

    cm.createTable(table_name, attr, 0, idx);
    
    // Create index
    cm.createIndex(table_name, "id", "id_index");
    
    std::string attr_name = cm.IndextoAttr(table_name, "id_index");
    ASSERT_EQ(attr_name, std::string("id"));
    
    cm.dropIndex(table_name, "id_index");
    // IndextoAttr should probably throw now
    bool caught = false;
    try {
        cm.IndextoAttr(table_name, "id_index");
    } catch (...) {
        caught = true;
    }
    ASSERT_TRUE(caught);
    
    cm.dropTable(table_name);
}

int main() {
    setup_catalog_file();
    test_catalog_table_basic();
    test_catalog_index();
    TEST_REPORT();
}
