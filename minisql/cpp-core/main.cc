//
//  main.cpp
//  interpreter
//
//  Created by Sr on 2017/5/20.
//  Copyright © 2017年 Sr. All rights reserved.
//

#include <iostream>
#include <cstdlib>
#ifdef _WIN32
#include <direct.h>
#define chdir _chdir
#else
#include <unistd.h>
#endif
#include "interpreter.h"
#include "buffer_manager.h"

// Note: Global objects are initialized before main. 
// However, since we want to chdir() before BufferManager initializes (and runs recovery),
// we move BufferManager inside main or ensure it doesn't open files in constructor.
// Based on current code, BufferManager() calls initialize() which opens nothing 
// but creates LogManager, which might open wal.log.

// Use a pointer to control initialization order
BufferManager* buffer_manager_ptr = nullptr;

// Cleanup function to ensure dirty pages are flushed on exit
void cleanup_engine() {
    if (buffer_manager_ptr) {
        delete buffer_manager_ptr;
        buffer_manager_ptr = nullptr;
    }
}

int main(int argc, const char * argv[]) {
    // 1. Handle Data Directory
    const char* data_dir = std::getenv("MINISQL_DATA_DIR");
    if (data_dir) {
        if (chdir(data_dir) == 0) {
            std::cout << ">>> Working directory changed to: " << data_dir << std::endl;
        } else {
            std::cerr << ">>> Warning: Failed to change directory to: " << data_dir << std::endl;
        }
    }

    // 2. Register cleanup
    std::atexit(cleanup_engine);

    // 3. Initialize Engine (triggers recovery)
    buffer_manager_ptr = new BufferManager(); 

    std::cout<<">>> Welcome to MiniSQL"<<std::endl;
    while(1){
        Interpreter query;
        query.getQuery();
        query.EXEC();
    }
    return 0;
}

