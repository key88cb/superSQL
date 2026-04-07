#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <fstream>
#include "../buffer_manager.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

void create_dummy_file(const std::string& name, int pages) {
    std::ofstream ofs(name, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    for(int i = 0; i < pages; ++i) {
        ofs.write(buf, PAGESIZE);
    }
    ofs.close();
}

void test_buffer_page_basic() {
    std::cout << "Running test_buffer_page_basic..." << std::endl;
    BufferManager bm;
    std::string test_file = "bm_test_file";
    
    // Ensure file exists with enough size
    create_dummy_file(test_file, 2);

    char* page_handle = bm.getPage(test_file, 0);
    ASSERT_TRUE(page_handle != nullptr);
    
    int page_id = bm.getPageId(test_file, 0);
    ASSERT_TRUE(page_id != -1);

    // Write some data
    const char* data = "Hello Buffer Manager";
    std::memcpy(page_handle, data, std::strlen(data) + 1);
    
    bm.modifyPage(page_id);
    int res = bm.flushPage(page_id, test_file, 0);
    ASSERT_EQ(res, 0);

    // Verify file content
    std::ifstream ifs(test_file, std::ios::binary);
    char read_buf[100];
    ifs.read(read_buf, std::strlen(data) + 1);
    ASSERT_EQ(std::string(read_buf), std::string(data));
    ifs.close();
    
    std::remove(test_file.c_str());
}

void test_pin_unpin() {
    std::cout << "Running test_pin_unpin..." << std::endl;
    BufferManager bm;
    std::string test_file = "pin_test";
    create_dummy_file(test_file, 1);
    
    /* char* p = */ bm.getPage(test_file, 0);
    int pid = bm.getPageId(test_file, 0);
    
    // getPage pins it once (pin_count=1)
    bm.pinPage(pid); // pin_count=2
    
    int res = bm.unpinPage(pid); // pin_count=1
    ASSERT_EQ(res, 0);
    
    res = bm.unpinPage(pid); // pin_count=0
    ASSERT_EQ(res, 0);
    
    res = bm.unpinPage(pid); // already 0
    ASSERT_EQ(res, -1);
    
    std::remove(test_file.c_str());
}

void test_buffer_replacement() {
    std::cout << "Running test_buffer_replacement..." << std::endl;
    // MAXFRAMESIZE is 100.
    BufferManager bm;
    std::string test_file = "repl_test";
    create_dummy_file(test_file, 110);
    
    // Load 101 different pages to force replacement
    for (int i = 0; i < 101; ++i) {
        bm.getPage(test_file, i);
        int pid = bm.getPageId(test_file, i);
        bm.unpinPage(pid); 
    }
    
    // Now page 0 should have been replaced or at least ref=false
    // We can't easily check internal state, but getting it again should work.
    char* p = bm.getPage(test_file, 0);
    ASSERT_TRUE(p != nullptr);
    
    std::remove(test_file.c_str());
}

int main() {
    test_buffer_page_basic();
    test_pin_unpin();
    test_buffer_replacement();
    TEST_REPORT();
}
