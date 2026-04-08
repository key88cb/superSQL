#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <iomanip>
#include <sstream>
#include "../api.h"
#include "../basic.h"

int total_tests = 0;
int passed_tests = 0;
BufferManager* buffer_manager_ptr = new BufferManager(); // Initialize the global pointer

void setup_env() {
    system("del /q database\\data\\* 2>nul");
    system("del /q database\\index\\* 2>nul");
    system("del /q database\\catalog\\* 2>nul");
    buffer_manager.clear();
    std::string cat_path = "./database/catalog/catalog_file";
    std::ofstream ofs(cat_path, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    ofs.write(buf, PAGESIZE);
    ofs.close();
}

int main() {
    setup_env();
    API api;
    std::string table_name = "stress_table";
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "data"; attr.type[1] = 100; attr.unique[1] = false;
    attr.primary_key = 0;
    Index idx; idx.num = 0;
    api.createTable(table_name, attr, 0, idx);

    std::cout << "After createTable: pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    for (int i = 0; i < 4000; ++i) {
        Tuple t;
        Data d1; d1.type = -1; d1.datai = i;
        Data d2; d2.type = 100;
        std::stringstream ss;
        ss << "data_payload_" << std::setw(5) << std::setfill('0') << i;
        d2.datas = ss.str();
        t.addData(d1);
        t.addData(d2);
        
        try {
            api.insertRecord(table_name, t);
        } catch (const std::exception& e) {
            std::cerr << "CRASH at record " << i << ": " << e.what() 
                      << " | pinned=" << buffer_manager.getPinnedCount() << std::endl;
            return 1;
        }
        
        if (i % 100 == 0) {
            int pinned = buffer_manager.getPinnedCount();
            std::cout << "i=" << i << " pinned=" << pinned << std::endl;
            if (pinned > 50) {
                std::cout << "WARNING: pin count growing!" << std::endl;
            }
        }
    }
    std::cout << "All 4000 inserts succeeded! Final pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    // Step 2: select all
    std::cout << "[STEP2] select all records..." << std::endl;
    Table res_all = api.selectRecord(table_name, {}, {}, 'a');
    std::cout << "[STEP2 DONE] tuples=" << res_all.getTuple().size() << " pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    // Step 3: delete first 2000
    std::cout << "[STEP3] deleting first 2000 records..." << std::endl;
    Where w_range;
    w_range.data.type = -1;
    w_range.data.datai = 2000;
    w_range.relation_character = LESS;
    api.deleteRecord(table_name, "id", w_range);
    std::cout << "[STEP3 DONE] pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    // Step 3b: verify remaining
    std::cout << "[STEP3b] verifying remaining 2000 records..." << std::endl;
    Table res_after = api.selectRecord(table_name, {}, {}, 'a');
    std::cout << "[STEP3b DONE] tuples=" << res_after.getTuple().size() << " pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    // Step 4: createIndex
    std::cout << "[STEP4] creating index..." << std::endl;
    try {
        api.createIndex(table_name, "id_idx", "id");
        std::cout << "[STEP4 DONE] createIndex succeeded! pinned=" << buffer_manager.getPinnedCount() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "CRASH at createIndex: " << e.what() << " | pinned=" << buffer_manager.getPinnedCount() << std::endl;
        return 1;
    }
    
    // Step 5: re-insert 500
    std::cout << "[STEP5] re-inserting 500 records with index..." << std::endl;
    for (int i = 0; i < 500; ++i) {
        Tuple t2;
        Data d1; d1.type = -1; d1.datai = i;
        Data d2; d2.type = 100; d2.datas = "reinserted_data";
        t2.addData(d1); t2.addData(d2);
        try {
            api.insertRecord(table_name, t2);
        } catch (const std::exception& e) {
            std::cerr << "CRASH at re-insert " << i << ": " << e.what() << " | pinned=" << buffer_manager.getPinnedCount() << std::endl;
            return 1;
        }
        if (i % 100 == 0)
            std::cout << "  re-insert i=" << i << " pinned=" << buffer_manager.getPinnedCount() << std::endl;
    }
    
    // Step 6: final verify
    std::cout << "[STEP6] final verify..." << std::endl;
    Table res_final = api.selectRecord(table_name, {}, {}, 'a');
    std::cout << "[STEP6 DONE] tuples=" << res_final.getTuple().size() << " pinned=" << buffer_manager.getPinnedCount() << std::endl;
    
    std::cout << "All steps succeeded! Test passed." << std::endl;
    return 0;
}
