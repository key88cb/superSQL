package edu.zju.supersql.regionserver;

import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WAL (Write-Ahead Log) manager.
 *
 * <p>File layout: one file per table at {@code {walDir}/{tableName}.wal}
 *
 * <p>Binary entry format per record:
 * <pre>
 *   [8B lsn][8B txnId][1B opType][1B status][4B sqlLen][sqlBytes]
 * </pre>
 *
 * <p>Durability: {@link FileChannel#force(boolean) FileChannel.force(false)} after every write.
 */
public class WalManager {

    private static final Logger log = LoggerFactory.getLogger(WalManager.class);

    // Entry header: lsn(8) + txnId(8) + opType(1) + status(1) + sqlLen(4) = 22 bytes
    private static final int HEADER_SIZE = 22;
    private static final byte STATUS_PREPARE = 0;
    private static final byte STATUS_COMMITTED = 1;
    private static final byte STATUS_ABORTED = 2;

    /** Checkpoint trigger threshold (write operations). */
    private static final int CHECKPOINT_THRESHOLD = 1000;

    private final String walDir;
    private final AtomicLong globalLsn = new AtomicLong(0);
    private final AtomicInteger writeCount = new AtomicInteger(0);

    /** Per-table append lock to prevent interleaved writes. */
    private final ConcurrentHashMap<String, ReentrantLock> tableLocks = new ConcurrentHashMap<>();

    /** For uncommitted entries: tableName -> (lsn -> statusByteFileOffset) */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> uncommittedOffsets = new ConcurrentHashMap<>();

    public WalManager(String walDir) {
        this.walDir = walDir;
    }

    // ─────────────────────── lifecycle ────────────────────────────────────────

    /**
     * Initialises the WAL directory and recovers the maximum LSN from existing files.
     */
    public void init() {
        File dir = new File(walDir);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                log.info("Created WAL directory: {}", walDir);
            } else {
                log.error("Failed to create WAL directory: {}", walDir);
                return;
            }
        }

        // Recover max LSN from all existing .wal files
        long maxLsn = 0L;
        File[] walFiles = dir.listFiles((d, name) -> name.endsWith(".wal"));
        if (walFiles != null) {
            for (File f : walFiles) {
                try {
                    long fileLsn = scanMaxLsn(f);
                    if (fileLsn > maxLsn) {
                        maxLsn = fileLsn;
                    }
                } catch (IOException e) {
                    log.warn("Failed to scan WAL file for max LSN: {}", f.getName());
                }
            }
        }
        globalLsn.set(maxLsn);
        log.info("WalManager initialized: dir={} recoveredMaxLsn={}", walDir, maxLsn);
    }

    // ─────────────────────── write ────────────────────────────────────────────

    /** Returns a globally monotonic LSN. */
    public long nextLsn() {
        return globalLsn.incrementAndGet();
    }

    /**
     * Appends one entry to {@code {walDir}/{tableName}.wal}.
     *
     * @param tableName target table
     * @param lsn       monotonic LSN (obtain via {@link #nextLsn()})
     * @param txnId     transaction / statement ID
     * @param opType    operation type enum
     * @param sql       SQL statement (stored as UTF-8 payload)
     * @throws IOException on file I/O failure
     */
    public void appendEntry(String tableName, long lsn, long txnId, WalOpType opType, String sql)
            throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + sqlBytes.length);
        buf.putLong(lsn);
        buf.putLong(txnId);
        buf.put((byte) opType.getValue());
        buf.put(STATUS_PREPARE);
        buf.putInt(sqlBytes.length);
        buf.put(sqlBytes);
        buf.flip();

        Path walFile = walFilePath(tableName);
        ReentrantLock lock = tableLocks.computeIfAbsent(tableName, k -> new ReentrantLock());
        lock.lock();
        try (FileChannel ch = FileChannel.open(walFile,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            long statusOffset = ch.size() + 17; // lsn(8) + txnId(8) + opType(1) = 17
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(false);
            
            uncommittedOffsets.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                              .put(lsn, statusOffset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * S4-05: Updates the status of the specified LSN to COMMITTED in the WAL file.
     */
    public void commit(String tableName, long lsn) {
        ConcurrentHashMap<Long, Long> offsets = uncommittedOffsets.get(tableName);
        if (offsets == null) return;
        Long offset = offsets.remove(lsn);
        if (offset == null) return;

        Path walFile = walFilePath(tableName);
        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "rw")) {
            raf.seek(offset);
            raf.writeByte(STATUS_COMMITTED);
        } catch (IOException e) {
            log.error("Failed to commit WAL entry for table={} lsn={}", tableName, lsn, e);
        }
    }

    /**
     * Marks the status of the specified LSN as ABORTED in the WAL file.
     */
    public void abort(String tableName, long lsn) {
        ConcurrentHashMap<Long, Long> offsets = uncommittedOffsets.get(tableName);
        if (offsets == null) return;
        Long offset = offsets.remove(lsn);
        if (offset == null) return;

        Path walFile = walFilePath(tableName);
        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "rw")) {
            raf.seek(offset);
            raf.writeByte(STATUS_ABORTED);
        } catch (IOException e) {
            log.error("Failed to abort WAL entry for table={} lsn={}", tableName, lsn, e);
        }
    }

    // ─────────────────────── read ─────────────────────────────────────────────

    /**
     * Reads all entries whose LSN >= {@code startLsn} from the table's WAL file.
     *
     * @param tableName target table
     * @param startLsn  inclusive lower bound
     * @return ordered list of matching {@link WalEntry} objects
     * @throws IOException on file I/O failure
     */
    public List<WalEntry> readEntriesAfter(String tableName, long startLsn) throws IOException {
        List<WalEntry> result = new ArrayList<>();
        Path walFile = walFilePath(tableName);
        if (!Files.exists(walFile)) {
            return result;
        }

        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "r")) {
            long fileLen = raf.length();
            while (raf.getFilePointer() < fileLen) {
                if (fileLen - raf.getFilePointer() < HEADER_SIZE) {
                    break; // truncated header — stop reading
                }
                long lsn    = raf.readLong();
                long txnId  = raf.readLong();
                byte opByte = raf.readByte();
                byte status = raf.readByte();
                int sqlLen  = raf.readInt();

                if (sqlLen < 0 || sqlLen > 1_048_576) {
                    log.warn("WAL entry has invalid sqlLen={} at table={}, stopping read", sqlLen, tableName);
                    break;
                }
                byte[] sqlBytes = new byte[sqlLen];
                int read = raf.read(sqlBytes);
                if (read != sqlLen) {
                    break; // truncated payload
                }

                // S4-05: Only replay COMMITTED entries.
                if (lsn >= startLsn && status == STATUS_COMMITTED) {
                    WalOpType opType = WalOpType.findByValue(opByte);
                    WalEntry entry = new WalEntry(lsn, txnId, tableName,
                            opType != null ? opType : WalOpType.INSERT,
                            System.currentTimeMillis());
                    entry.setAfterRow(sqlBytes);
                    result.add(entry);
                }
            }
        }
        return result;
    }

    /**
    * Reads all uncommitted (status=PREPARE) entries for a specific table.
     * Used by secondary replicas during startup to restore pending logs.
     */
    public List<WalEntry> readUncommittedEntries(String tableName) throws IOException {
        List<WalEntry> result = new ArrayList<>();
        Path walFile = walFilePath(tableName);
        if (!Files.exists(walFile)) {
            return result;
        }

        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "r")) {
            long fileLen = raf.length();
            while (raf.getFilePointer() < fileLen) {
                if (fileLen - raf.getFilePointer() < HEADER_SIZE) break;
                
                long pos = raf.getFilePointer();
                long lsn    = raf.readLong();
                long txnId  = raf.readLong();
                byte opByte = raf.readByte();
                byte status = raf.readByte();
                int sqlLen  = raf.readInt();

                if (sqlLen < 0 || sqlLen > 1_048_576) break;
                
                byte[] sqlBytes = new byte[sqlLen];
                int read = raf.read(sqlBytes);
                if (read != sqlLen) break;

                if (status == STATUS_PREPARE) {
                    WalOpType opType = WalOpType.findByValue(opByte);
                    WalEntry entry = new WalEntry(lsn, txnId, tableName,
                            opType != null ? opType : WalOpType.INSERT,
                            System.currentTimeMillis());
                    entry.setAfterRow(sqlBytes);
                    result.add(entry);
                    
                    // Also restore the offset mapping so it can be committed later
                    long statusOffset = pos + 17; // lsn(8) + txnId(8) + opType(1) = 17
                    uncommittedOffsets.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                                      .put(lsn, statusOffset);
                }
            }
        }
        return result;
    }

    // ─────────────────────── checkpoint & cleanup ─────────────────────────────

    /**
     * Increments the write counter.
     *
     * @return {@code true} if the checkpoint threshold has been reached
     */
    public boolean incrementWriteCount() {
        return writeCount.incrementAndGet() >= CHECKPOINT_THRESHOLD;
    }

    /** Resets the write counter after a successful checkpoint. */
    public void resetWriteCount() {
        writeCount.set(0);
    }

    /**
     * S4-05: Recovers state from WAL files after a crash.
     * Replays entries with LSN > checkpointLsn.
     */
    public void recover(MiniSqlProcess process) {
        log.info("Starting Crash Recovery from WAL logs...");
        try {
            long checkpointLsn = process.checkpoint(); // Gets the persisted safe watermark from engine
            log.info("Current persisted PageLSN (watermark) is: {}", checkpointLsn);

            File dir = new File(walDir);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
            
            if (files == null || files.length == 0) {
                log.info("No WAL files found. Recovery complete.");
                return;
            }

            int recoveredCount = 0;
            // Iterate over all table files
            for (File file : files) {
                String tableName = file.getName().replace(".wal", "");
                List<WalEntry> entriesToReplay = readEntriesAfter(tableName, checkpointLsn + 1);
                
                for (WalEntry entry : entriesToReplay) {
                    if (entry.getAfterRow() != null) {
                        String rawSql = new String(entry.getAfterRow(), StandardCharsets.UTF_8);
                        log.info("REDO LSN {}: {}", entry.getLsn(), rawSql);
                        process.execute(rawSql);
                        recoveredCount++;
                    }
                }
            }

            log.info("Crash Recovery finished. Replayed {} missing entries.", recoveredCount);

            if (recoveredCount > 0) {
                // Force sync the newly recovered state back to disk
                log.info("Triggering checkpoint to persist recovered state...");
                performCheckpoint(process);
            }

        } catch (Exception e) {
            log.error("Critical error during Crash Recovery: ", e);
        }
    }

    /**
     * Performs a WAL checkpoint:
     * <ol>
     *   <li>Calls C++ engine {@code checkpoint;} to flush dirty pages and get the current LSN.</li>
     *   <li>Physically deletes WAL files whose max LSN is below the checkpoint LSN.</li>
     * </ol>
     */
    public synchronized void performCheckpoint(MiniSqlProcess process) {
        log.info("Starting WAL checkpoint (writeCount={})...", writeCount.get());
        try {
            long checkpointLsn = process.checkpoint();
            if (checkpointLsn < 0) {
                log.error("C++ engine failed to perform checkpoint.");
                return;
            }
            log.info("C++ engine checkpointed at LSN={}. Cleaning WAL logs...", checkpointLsn);

            File dir = new File(walDir);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
            if (files != null) {
                int deleted = 0;
                for (File f : files) {
                    try {
                        long fileLsn = scanMaxLsn(f);
                        if (fileLsn < checkpointLsn) {
                            if (f.delete()) {
                                deleted++;
                            }
                        }
                    } catch (IOException e) {
                        log.warn("Failed to inspect/delete WAL file: {}", f.getName());
                    }
                }
                log.info("Physical WAL cleanup: deleted {} file(s) (checkpoint LSN={})",
                        deleted, checkpointLsn);
            }

            resetWriteCount();
            log.info("WAL checkpoint completed successfully.");
        } catch (Exception e) {
            log.error("Error during WAL checkpoint", e);
        }
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    public String getWalDir() {
        return walDir;
    }

    private Path walFilePath(String tableName) {
        return Paths.get(walDir, tableName + ".wal");
    }

    /**
     * Scans a WAL file to find the maximum LSN stored in it.
     * Returns 0 if the file is empty or unreadable.
     */
    long scanMaxLsn(File f) throws IOException {
        long maxLsn = 0L;
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            long fileLen = raf.length();
            while (raf.getFilePointer() < fileLen) {
                if (fileLen - raf.getFilePointer() < HEADER_SIZE) break;
                long lsn   = raf.readLong();
                raf.readLong();              // txnId
                raf.readByte();              // opType
                raf.readByte();              // status
                int sqlLen = raf.readInt();
                if (sqlLen < 0 || sqlLen > 1_048_576) break;
                raf.skipBytes(sqlLen);
                if (lsn > maxLsn) maxLsn = lsn;
            }
        }
        return maxLsn;
    }
}
