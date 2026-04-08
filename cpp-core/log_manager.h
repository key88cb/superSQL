#ifndef _LOG_MANAGER_H_
#define _LOG_MANAGER_H_

#include <string>
#include <vector>
#include <cstdio>
#include "const.h"

// Log Record Types
enum LogType {
    LOG_INSERT = 0,
    LOG_DELETE = 1,
    LOG_UPDATE = 2,
    LOG_BEGIN = 3,
    LOG_COMMIT = 4,
    LOG_ABORT = 5
};

// Log Record structure
struct LogRecord {
    int lsn;
    LogType type;
    std::string file_name;
    int block_id;
    int offset;
    int length;
    std::vector<char> old_data;
    std::vector<char> new_data;
};

// LogManager class to handle WAL (Write-Ahead Logging)
class LogManager {
public:
    LogManager(std::string log_file_name = "super_sql.log");
    ~LogManager();

    // Appends a log and returns its assigned LSN
    int appendLog(LogType type, std::string file_name, int block_id, int offset, int length, const char* old_data, const char* new_data);
    
    // Force write logs up to given LSN to disk
    void flush(int lsn);

    // Apply REDO Recovery from log file to disk
    void recoverRedo();

    // Get current global LSN
    int getCurrentLsn() const;
    
    // Get the LSN that has been durably flushed
    int getFlushedLsn() const;

    // Reset log manager
    void clear();

private:
    std::string log_file_name_;
    int global_lsn_;
    int flushed_lsn_;
    std::vector<LogRecord> log_buffer_;
};

#endif
