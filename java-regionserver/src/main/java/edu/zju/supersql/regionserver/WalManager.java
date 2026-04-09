package edu.zju.supersql.regionserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Manages the WAL (Write-Ahead Log) directory and associated metadata.
 */
public class WalManager {
    private static final Logger log = LoggerFactory.getLogger(WalManager.class);
    
    // Checkpoint 触发阈值：1000 条写操作
    private static final int CHECKPOINT_THRESHOLD = 1000;
    private final java.util.concurrent.atomic.AtomicInteger writeCount = new java.util.concurrent.atomic.AtomicInteger(0);
    
    private String walDir;

    public WalManager(String walDir) {
        this.walDir = walDir;
    }

    /**
     * Initializes the WAL directory.
     */
    public void init() {
        log.info("Initializing WalManager with directory: {}", walDir);
        File dir = new File(walDir);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                log.info("Created WAL directory: {}", walDir);
            } else {
                log.error("Failed to create WAL directory: {}", walDir);
            }
        }
    }

    public String getWalDir() {
        return walDir;
    }

    /**
     * Increments the write counter and checks if checkpoint should be triggered.
     * @return true if threshold reached
     */
    public boolean incrementWriteCount() {
        int count = writeCount.incrementAndGet();
        return count >= CHECKPOINT_THRESHOLD;
    }

    /**
     * Resets the write counter after a successful checkpoint.
     */
    public void resetWriteCount() {
        writeCount.set(0);
    }

    /**
     * Performs a WAL checkpoint.
     * 1. Tells C++ engine to flush all dirty pages.
     * 2. Clears the WAL log file since all data is persisted.
     */
    public void performCheckpoint(MiniSqlProcess process) {
        log.info("Starting WAL checkpoint (Current write count: {})...", writeCount.get());
        try {
            // 1. 调用 C++ 引擎进行强制刷盘，获取当前持久化的最大 LSN
            long checkpointLsn = process.checkpoint();
            if (checkpointLsn >= 0) {
                log.info("C++ engine checkpointed at LSN: {}. Cleaning WAL logs...", checkpointLsn);
                
                // 2. 清理 C++ 内部日志文件（如果存在）
                process.execute("clear log;");
                
                // 3. 物理清理 Java 侧的 WAL 日志文件
                // 约定格式：wal-{tableName}-{startLSN}.log
                File dir = new File(walDir);
                File[] files = dir.listFiles((d, name) -> name.startsWith("wal-") && name.endsWith(".log"));
                
                if (files != null) {
                    int deleteCount = 0;
                    for (File f : files) {
                        try {
                            // 解析文件名获取起始 LSN
                            // 示例: wal-reg-100.log -> 100
                            String name = f.getName();
                            String[] parts = name.split("-");
                            if (parts.length >= 3) {
                                String lsnPart = parts[2].replace(".log", "");
                                long fileStartLsn = Long.parseLong(lsnPart);
                                
                                // 如果该文件记录的起始 LSN 小于当前 Checkpoint LSN，说明该文件中的数据大部分（或全部）已落盘
                                // 注意：为了安全，通常保留最近的一个文件，或者增加一定的水位线
                                if (fileStartLsn < checkpointLsn) {
                                    if (f.delete()) {
                                        deleteCount++;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse or delete WAL file: {}", f.getName());
                        }
                    }
                    log.info("Physical WAL cleanup finished. Deleted {} files.", deleteCount);
                }

                // 4. 重置计数器
                resetWriteCount();
                log.info("WAL checkpoint completed successfully.");
            } else {
                log.error("C++ engine failed to perform checkpoint.");
            }
        } catch (Exception e) {
            log.error("Error during WAL checkpoint: ", e);
        }
    }
}
