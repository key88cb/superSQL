#include "log_manager.h"
#include <iostream>
#include <cstring>
#include <sstream>

LogManager::LogManager(std::string log_file_name) {
    log_file_name_ = log_file_name;
    global_lsn_ = 0;
    flushed_lsn_ = 0;
}

LogManager::~LogManager() {
    flush(global_lsn_);
}

int LogManager::appendLog(LogType type, std::string file_name, int block_id, int offset, int length, const char* old_data, const char* new_data) {
    global_lsn_++;
    int current_lsn = global_lsn_;
    
    LogRecord record;
    record.lsn = current_lsn;
    record.type = type;
    record.file_name = file_name;
    record.block_id = block_id;
    record.offset = offset;
    record.length = length;
    
    if (new_data != NULL && length > 0) {
        record.new_data.assign(new_data, new_data + length);
    }
    
    log_buffer_.push_back(record);
    
    if (type == LOG_COMMIT) {
        flush(current_lsn);
    }
    
    return current_lsn;
}

void LogManager::flush(int lsn) {
    if (lsn <= flushed_lsn_) return;
    
    FILE* f = fopen(log_file_name_.c_str(), "ab");
    if (f == NULL) {
        std::cerr << "Fail to open log file." << std::endl;
        return;
    }
    
    for (size_t i = 0; i < log_buffer_.size(); i++) {
        if (log_buffer_[i].lsn > flushed_lsn_ && log_buffer_[i].lsn <= lsn) {
            LogRecord& r = log_buffer_[i];
            fprintf(f, "LSN:%d TYPE:%d FILE:%s BLK:%d OFF:%d LEN:%d\n", 
                    r.lsn, (int)r.type, r.file_name.c_str(), r.block_id, r.offset, r.length);
            if (r.length > 0 && r.new_data.size() == r.length) {
                fwrite(r.new_data.data(), 1, r.length, f);
            }
            fprintf(f, "\n");
        }
    }
    
    fflush(f);
    fclose(f);
    
    flushed_lsn_ = lsn;
    // Optional: remove flushed from buffer to save mem
}

void LogManager::recoverRedo() {
    FILE* f = fopen(log_file_name_.c_str(), "rb");
    if (f == NULL) return; // No log to recover
    
    char header_buf[1024];
    while (fgets(header_buf, sizeof(header_buf), f) != NULL) {
        int lsn = 0, type_int = 0, block_id = 0, offset = 0, length = 0;
        char file_path[256];
        
        if (sscanf(header_buf, "LSN:%d TYPE:%d FILE:%s BLK:%d OFF:%d LEN:%d\n",
            &lsn, &type_int, file_path, &block_id, &offset, &length) == 6) {
            
            std::vector<char> new_data(length);
            if (length > 0) {
                fread(new_data.data(), 1, length, f);
                fgetc(f); // Read the trailing \n
            }
            
            // Reapply operation (REDO)
            // Just directly write to the file mapping
            if ((type_int == LOG_INSERT || type_int == LOG_DELETE || type_int == LOG_UPDATE) && length > 0) {
                FILE* db_file = fopen(file_path, "rb+");
                if (db_file != NULL) {
                    fseek(db_file, block_id * PAGESIZE + offset, SEEK_SET); // PAGESIZE comes from const.h
                    fwrite(new_data.data(), 1, length, db_file);
                    fclose(db_file);
                }
            }
            
            global_lsn_ = lsn;
            flushed_lsn_ = lsn;
        }
    }
    fclose(f);
    std::cout << "REDO Recovery Complete up to LSN: " << global_lsn_ << std::endl;
}

int LogManager::getCurrentLsn() const {
    return global_lsn_;
}

int LogManager::getFlushedLsn() const {
    return flushed_lsn_;
}

void LogManager::clear() {
    global_lsn_ = 0;
    flushed_lsn_ = 0;
    log_buffer_.clear();
    FILE* f = fopen(log_file_name_.c_str(), "w");
    if (f != NULL) {
        fclose(f);
    }
}

