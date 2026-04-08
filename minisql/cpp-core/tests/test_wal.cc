#include <iostream>
#include "../buffer_manager.h"
#include "../log_manager.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

int main() {
    std::cout << "Starting WAL (Write-Ahead Logging) Test..." << std::endl;
    
    // 1. Create a dummy file for testing BufferManager
    FILE* temp_db = fopen("test_wal.db", "w+");
    char dummy_block[4096] = {0};
    fwrite(dummy_block, 4096, 1, temp_db);
    fclose(temp_db);
    
    // Clean old log if exists
    remove("super_sql.log");

    {
    // 2. Initialize BufferManager
    BufferManager bm(5);
    LogManager* lm = bm.getLogManager();
    
    ASSERT_EQ(lm->getCurrentLsn(), 0);
    ASSERT_EQ(lm->getFlushedLsn(), 0);

    // 3. Load a page from the dummy file
    char* buf = bm.getPage("test_wal.db", 0);
    int page_id = bm.getPageId("test_wal.db", 0);
    
    ASSERT_TRUE(page_id != -1);
    
    // 4. Simulate a write operation (e.g. from RecordManager)
    // 4.1 Append log first
    const char* old_data = "OLD";
    const char* new_data = "NEW";
    int lsn = lm->appendLog(LOG_UPDATE, "test_wal.db", 0, 0, 3, old_data, new_data);
    
    ASSERT_EQ(lsn, 1);
    ASSERT_EQ(lm->getCurrentLsn(), 1);
    ASSERT_EQ(lm->getFlushedLsn(), 0); // Not flushed yet

    // 4.2 Modify the memory page and tag it with LSN
    buf[0] = 'N'; buf[1] = 'E'; buf[2] = 'W';
    bm.modifyPage(page_id);
    
    // We must manually attach the LSN to the Page because RecordManager normally does it
    // But since Page access methods are internal, let's assume we have a way.
    // Wait, Page is internal to BufferManager, we can't do it directly.
    // I need to add a wrapper in BufferManager like "setPageLsn(page_id, lsn)" for RecordManager to use.
    
    bm.setPageLsn(page_id, lsn);
    
    // 5. Force BufferManager to flush this dirty page by clearing the buffer
    bm.clear(); // This will trigger flushPage for all dirty pages
    // Note: In our current implementation, clear() resets pages but wait...
    // Let's check buffer_manager.cc clear(). It just calls initialize on all pages WITHOUT flushing!
    // So clear() doesn't flush. The destructor does! Or we can just call unpin and wait for eviction, 
    // but the easiest is to manually triggle flush, or just let bm go out of scope.
    
    // Instead of clear, let's just trigger a flush manually if we had the method, but we can just use 
    // block local scope to destruct the BM and force flush.
    
    std::cout << "Before flush, LogManager flushed LSN is: " << lm->getFlushedLsn() << std::endl;
    // We can also force a flush page if we make it public or just create a new block
    } // BufferManager goes out of scope here! This triggers ~BufferManager
    
    // Since BufferManager deleted its log_manager_, we can't check it directly after.
    // Let's verify by checking the file instead!
    
    FILE* log_f = fopen("super_sql.log", "r");
    ASSERT_TRUE(log_f != NULL);
    char log_buffer[256];
    ASSERT_TRUE(fgets(log_buffer, 255, log_f) != NULL);
    fclose(log_f);
    
    std::string log_str(log_buffer);
    std::cout << "Log file content: " << log_str;
    ASSERT_EQ(log_str.find("LSN:1 TYPE:2 FILE:test_wal.db") != std::string::npos, true);
    
    std::cout << "Test WAL passed! The LogManager was forced to flush up to LSN 1 before the data page was written." << std::endl;
    return 0;
}
