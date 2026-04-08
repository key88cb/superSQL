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
}
