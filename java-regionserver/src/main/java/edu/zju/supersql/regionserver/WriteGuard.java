package edu.zju.supersql.regionserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-table write-pause guard used during region migration.
 *
 * <p>When Master issues a {@code pauseTableWrite} command before transferring a region,
 * write operations on the paused table block (or immediately fail with MOVING) until
 * {@code resumeTableWrite} is called.
 */
public class WriteGuard {

    private static final Logger log = LoggerFactory.getLogger(WriteGuard.class);

    /** Tables that are currently paused. Value is always {@code Boolean.TRUE}. */
    private final ConcurrentHashMap<String, Boolean> paused = new ConcurrentHashMap<>();

    /**
     * Pauses writes to the given table.
     * Subsequent {@link #isPaused(String)} calls will return {@code true} until
     * {@link #resume(String)} is invoked.
     */
    public void pause(String tableName) {
        paused.put(tableName, Boolean.TRUE);
        log.info("WriteGuard: writes paused for table '{}'", tableName);
    }

    /**
     * Resumes writes to the given table.
     */
    public void resume(String tableName) {
        paused.remove(tableName);
        log.info("WriteGuard: writes resumed for table '{}'", tableName);
    }

    /**
     * Returns {@code true} if the table is currently paused for writes.
     */
    public boolean isPaused(String tableName) {
        return paused.containsKey(tableName);
    }

    /**
     * Blocks the calling thread until the table is no longer paused, or the timeout expires.
     *
     * @param tableName  target table
     * @param timeoutMs  maximum wait in milliseconds
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitWritable(String tableName, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (isPaused(tableName) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }
}
