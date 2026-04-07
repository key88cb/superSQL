#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <iomanip>
#include "../api.h"
#include "../basic.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

BufferManager buffer_manager; // Extern

void setup_env() {
    // Portably clear directories
    system("del /q database\\data\\* 2>nul");
    system("del /q database\\index\\* 2>nul");
    system("del /q database\\catalog\\* 2>nul");
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

void test_exhaustive_stress() {
    std::cout << "Running test_exhaustive_stress (Large Volume & Buffer Overflow)..." << std::endl;
    API api;
    
    std::string table_name = "stress_table";
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "data"; attr.type[1] = 100; attr.unique[1] = false; // Large string
    attr.primary_key = 0;
    Index idx; idx.num = 0;

    ASSERT_TRUE(api.createTable(table_name, attr, 0, idx));

    const int INSERT_COUNT = 4000;
    std::cout << "Inserting " << INSERT_COUNT << " records..." << std::endl;
    
    for (int i = 0; i < INSERT_COUNT; ++i) {
        Tuple t;
        Data d1; d1.type = -1; d1.datai = i;
        Data d2; d2.type = 100;
        // Generate a recognizable string
        std::stringstream ss;
        ss << "data_payload_" << std::setw(5) << std::setfill('0') << i;
        d2.datas = ss.str();
        
        t.addData(d1);
        t.addData(d2);
        api.insertRecord(table_name, t);
        
        if (i > 0 && i % 1000 == 0) {
            std::cout << "  Inserted " << i << " records..." << std::endl;
        }
    }

    std::cout << "Verifying all " << INSERT_COUNT << " records..." << std::endl;
    Table res_all = api.selectRecord(table_name, {}, {}, 'a');
    ASSERT_EQ(res_all.getTuple().size(), INSERT_COUNT);

    // Verify some specific values to ensure data integrity after overflow
    int check_points[] = {0, 500, 1000, 2000, 3999};
    for (int cp : check_points) {
        bool found = false;
        for (auto& tuple : res_all.getTuple()) {
            if (tuple.getData()[0].datai == cp) {
                std::stringstream ss;
                ss << "data_payload_" << std::setw(5) << std::setfill('0') << cp;
                ASSERT_EQ(tuple.getData()[1].datas, ss.str());
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found);
    }

    std::cout << "Testing partial deletion (even IDs)..." << std::endl;
    Where w;
    w.data.type = -1;
    w.relation_character = EQUAL; 
    // We don't have a "mod" operator, so we delete in a loop or use a range
    // Let's delete the first 2000 records using LESS condition
    Where w_range;
    w_range.data.type = -1;
    w_range.data.datai = 2000;
    w_range.relation_character = LESS;
    
    api.deleteRecord(table_name, "id", w_range);
    
    std::cout << "Verifying remaining 2000 records..." << std::endl;
    Table res_after_del = api.selectRecord(table_name, {}, {}, 'a');
    ASSERT_EQ(res_after_del.getTuple().size(), 2000);

    // Check boundary
    for (auto& tuple : res_after_del.getTuple()) {
        ASSERT_GE(tuple.getData()[0].datai, 2000);
    }

    std::cout << "Re-inserting deleted range with index stress..." << std::endl;
    ASSERT_TRUE(api.createIndex(table_name, "id_idx", "id"));
    
    for (int i = 0; i < 500; ++i) {
        Tuple t;
        Data d1; d1.type = -1; d1.datai = i; // Re-insert 0-499
        Data d2; d2.type = 100;
        d2.datas = "reinserted_data";
        t.addData(d1);
        t.addData(d2);
        api.insertRecord(table_name, t);
    }

    Table res_final = api.selectRecord(table_name, {}, {}, 'a');
    ASSERT_EQ(res_final.getTuple().size(), 2500);

    std::cout << "Exhaustive stress test passed!" << std::endl;
}

int main() {
    setup_env();
    try {
        test_exhaustive_stress();
    } catch (const std::exception& e) {
        std::cerr << "Caught exception: " << e.what() << std::endl;
        return 1;
    }
    TEST_REPORT();
    return 0;
}
