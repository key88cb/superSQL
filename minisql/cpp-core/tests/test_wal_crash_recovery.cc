#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <cstdlib>
#include "../buffer_manager.h"
#include "../record_manager.h"
#include "../catalog_manager.h"
#include "../log_manager.h"
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

void prepare_env() {
#ifdef _WIN32
    run_cmd_or_fail("if not exist database\\data mkdir database\\data");
    run_cmd_or_fail("if not exist database\\catalog mkdir database\\catalog");
    run_cmd_or_fail("if not exist database\\index mkdir database\\index");
    run_cmd_or_fail("del /q database\\data\\* 2>nul");
    run_cmd_or_fail("del /q database\\index\\* 2>nul");
    run_cmd_or_fail("del /q database\\catalog\\* 2>nul");
#else
    run_cmd_or_fail("mkdir -p database/data database/catalog database/index");
    run_cmd_or_fail("rm -f database/data/* database/index/* database/catalog/*");
#endif
    remove("super_sql.log");
    
    std::string cat_path = "./database/catalog/catalog_file";
    std::ofstream ofs(cat_path, std::ios::binary);
    char buf[PAGESIZE] = {0};
    ofs.write(buf, PAGESIZE);
    ofs.close();

    buffer_manager.clear();
}

int main() {
    std::cout << "Starting Comprehensive WAL Crash & Recovery Test..." << std::endl;
    prepare_env();

    // The buffer_manager is a global instance that was initialized before main().
    // We can assume it is clean.

    std::cout << "\n[1] Creating Table and Inserting Data..." << std::endl;
    RecordManager rm;
    CatalogManager cm;

    std::string table_name = "crash_table";
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "score"; attr.type[1] = -1; attr.unique[1] = false;
    attr.primary_key = 0;
    attr.has_index[0] = false;
    attr.has_index[1] = false;
    Index idx; idx.num = 0;

    cm.createTable(table_name, attr, 0, idx);
    rm.createTableFile(table_name);

    // FORCE FLUSH DDL: Since our WAL implementation currently focuses on table records (DML),
    // we must manually sync the catalog structural metadata to disk before the simulated crash.
    int cat_page = buffer_manager.getPageId("./database/catalog/catalog_file", 0);
    if (cat_page != -1) {
        buffer_manager.flushPage(cat_page, "./database/catalog/catalog_file", 0);
    }

    // Insert 3 tuples
    for (int i = 1; i <= 3; i++) {
        Tuple t;
        Data d1; d1.type = -1; d1.datai = i;
        Data d2; d2.type = -1; d2.datai = i * 100;
        t.addData(d1); t.addData(d2);
        rm.insertRecord(table_name, t);
    }
    
    // Simulate COMMIT log to force flush the logs
    buffer_manager.getLogManager()->appendLog(LOG_COMMIT, "commit_marker", 0, 0, 0, nullptr, nullptr);

    std::cout << "\n[2] Verifying records in memory before crash..." << std::endl;
    Table res_before = rm.selectRecord(table_name);
    ASSERT_EQ(res_before.getTuple().size(), 3);

    std::cout << "\n[3] SIMULATING CRASH (Power Loss)..." << std::endl;
    // By calling clear(), all dirty pages are zeroed out and NOT flushed to disk.
    buffer_manager.clear();
    
    // To verify the disaster, we directly read the .db file without buffer manager
    FILE* db_check = fopen(("./database/data/" + table_name).c_str(), "r");
    char db_buf[PAGESIZE] = {0};
    if (db_check) {
        size_t read_count = fread(db_buf, PAGESIZE, 1, db_check);
        ASSERT_TRUE(read_count == 1);
        fclose(db_check);
    }
    ASSERT_TRUE(db_buf[0] == '\0'); // The DB file MUST be empty!
    std::cout << "Disaster confirmed: The .db file is blank! All un-flushed data was lost from memory." << std::endl;


    std::cout << "\n[4] SYSTEM REBOOT & REDO RECOVERY..." << std::endl;
    // We re-initialize the BufferManager framework (simulating server restart).
    // This will spawn a new LogManager and automatically trigger recoverRedo().
    LogManager* old_lm = buffer_manager.getLogManager();
    if (old_lm) delete old_lm;
    buffer_manager.initialize(MAXFRAMESIZE); // This reads super_sql.log and restores .db!

    std::cout << "\n[5] Verifying Recovered Data via RecordManager API..." << std::endl;
    Table res_after = rm.selectRecord(table_name);
    
    std::cout << "Records found after recovery: " << res_after.getTuple().size() << std::endl;
    for (size_t i = 0; i < res_after.getTuple().size(); i++) {
        std::cout << "  Row " << i+1 << ": ID=" << res_after.getTuple()[i].getData()[0].datai << ", Score=" << res_after.getTuple()[i].getData()[1].datai << std::endl;
    }

    ASSERT_EQ(res_after.getTuple().size(), 3);
    if (res_after.getTuple().size() == 3) {
        ASSERT_EQ(res_after.getTuple()[0].getData()[0].datai, 1);
        ASSERT_EQ(res_after.getTuple()[2].getData()[1].datai, 300);
    }

    std::cout << "\nComprehensive Crash Recovery Test PASSED!" << std::endl;

    cm.dropTable(table_name);
    rm.dropTableFile(table_name);
    TEST_REPORT();
}
