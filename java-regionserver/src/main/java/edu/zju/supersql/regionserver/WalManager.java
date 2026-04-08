package edu.zju.supersql.regionserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Manages the WAL (Write-Ahead Log) directory and associated metadata.
 */
public class WalManager {
    private static final Logger log = LoggerFactory.getLogger(WalManager.class);
    
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
     * Performs a WAL checkpoint.
     * 1. Tells C++ engine to flush all dirty pages.
     * 2. Clears the WAL log file since all data is persisted.
     */
    public void performCheckpoint(MiniSqlProcess process) {
        log.info("Starting WAL checkpoint...");
        try {
            long lsn = process.checkpoint();
            if (lsn >= 0) {
                log.info("C++ engine checkpointed at LSN: {}. Clearing WAL logs...", lsn);
                process.execute("clear log;");
                log.info("WAL checkpoint completed successfully.");
            } else {
                log.error("C++ engine failed to perform checkpoint.");
            }
        } catch (Exception e) {
            log.error("Error during WAL checkpoint: ", e);
        }
    }
}
