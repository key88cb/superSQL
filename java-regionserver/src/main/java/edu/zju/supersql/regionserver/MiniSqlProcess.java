package edu.zju.supersql.regionserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the underlying MiniSQL C++ process.
 * Acts as a bridge between Java RegionServer and C++ core.
 */
public class MiniSqlProcess {
    private static final Logger log = LoggerFactory.getLogger(MiniSqlProcess.class);
    private static final long MONITOR_INTERVAL_MS = 1_000L;
    
    private Process process;
    private BufferedWriter stdin;
    private BufferedReader stdout;
    private final String dataDir;
    private final String binPath;
    private volatile boolean desiredRunning;
    private Thread monitorThread;

    public MiniSqlProcess(String binPath, String dataDir) {
        this.binPath = binPath;
        this.dataDir = dataDir;
    }

    /**
     * Starts the MiniSQL process and triggers self-recovery.
     */
    public synchronized void start() throws IOException {
        desiredRunning = true;
        startMonitorIfNeeded();
        startProcessIfNeeded();
    }

    private void startProcessIfNeeded() throws IOException {
        if (process != null && process.isAlive()) {
            return;
        }

        log.info("Starting MiniSQL engine: {} with DATA_DIR={}", binPath, dataDir);
        
        ProcessBuilder pb = new ProcessBuilder(binPath);
        pb.environment().put("MINISQL_DATA_DIR", dataDir);
        pb.redirectErrorStream(true); // Merge stderr into stdout

        // Ensure data directory and the miniSQL subdirectories exist.
        // miniSQL's buffer_manager writes to database/{catalog,data,index}
        // under the working directory. When those subdirectories are absent
        // (e.g. a fresh volume on first boot), the C++ engine segfaults on
        // the first CREATE TABLE. Pre-create them so the engine can persist.
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        for (String sub : new String[]{"database/catalog", "database/data", "database/index"}) {
            File child = new File(dir, sub);
            if (!child.exists()) {
                child.mkdirs();
            }
        }

        process = pb.start();
        stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));

        // Drain the startup banner up to the first prompt. miniSQL emits the
        // prompt (">>> ") without a trailing newline, so we read byte-by-byte
        // and rely on a short quiet period after the banner to mark "ready".
        String banner = readUntilQuiet("startup", STARTUP_TIMEOUT_MS);
        if (banner.isEmpty() && (process == null || !process.isAlive())) {
            throw new IOException("Failed to start MiniSQL engine: Unexpected EOF");
        }
        for (String line : banner.split("\\r?\\n")) {
            if (!line.isBlank()) {
                log.info("Engine: {}", line.trim());
            }
        }
        log.info("MiniSQL engine started and recovered successfully.");
    }

    private static final int STARTUP_TIMEOUT_MS = 10_000;
    private static final int EXECUTE_TIMEOUT_MS = 30_000;
    private static final int QUIET_MS = 150;

    /**
     * Reads from stdout until the engine stops producing output for
     * {@code QUIET_MS}. miniSQL marks "ready for next command" by emitting
     * a ">>> " prompt without a trailing newline; the quiet window detects
     * that state without relying on a fragile terminator search.
     */
    private String readUntilQuiet(String phase, int overallTimeoutMs) throws IOException {
        StringBuilder output = new StringBuilder();
        long overallDeadline = System.currentTimeMillis() + overallTimeoutMs;
        // Wait for the first byte of output.
        while (System.currentTimeMillis() < overallDeadline && !stdout.ready()) {
            if (!process.isAlive()) {
                break;
            }
            try { Thread.sleep(5); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return output.toString();
            }
        }
        long lastActivity = System.currentTimeMillis();
        while (System.currentTimeMillis() < overallDeadline) {
            if (stdout.ready()) {
                int c = stdout.read();
                if (c == -1) break;
                output.append((char) c);
                lastActivity = System.currentTimeMillis();
            } else {
                if (!process.isAlive()) {
                    // Drain any remaining buffered output before returning.
                    while (stdout.ready()) {
                        int c = stdout.read();
                        if (c == -1) break;
                        output.append((char) c);
                    }
                    break;
                }
                if (System.currentTimeMillis() - lastActivity >= QUIET_MS) {
                    break;
                }
                try { Thread.sleep(5); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return output.toString();
                }
            }
        }
        if (System.currentTimeMillis() >= overallDeadline) {
            log.warn("{} drain hit {}ms hard timeout", phase, overallTimeoutMs);
        }
        return output.toString();
    }

    private void startMonitorIfNeeded() {
        if (monitorThread != null) {
            return;
        }
        monitorThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(MONITOR_INTERVAL_MS);
                    recoverIfCrashed();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "MiniSqlProcess-Monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private synchronized void recoverIfCrashed() {
        if (!desiredRunning || process == null || process.isAlive()) {
            return;
        }
        int exitCode;
        try {
            exitCode = process.exitValue();
        } catch (IllegalThreadStateException e) {
            return;
        }
        log.warn("MiniSQL engine exited unexpectedly with code {}. Restarting...", exitCode);
        cleanupHandles();
        process = null;
        try {
            startProcessIfNeeded();
        } catch (IOException e) {
            log.error("Failed to restart MiniSQL engine after crash: {}", e.getMessage(), e);
        }
    }

    /**
     * Executes a SQL command and returns the output.
     *
     * miniSQL's prompt (">>> ") is emitted without a trailing newline, so we
     * cannot rely on readLine() to break on the terminating prompt. Instead
     * we flush the command and read until the engine goes quiet — that is
     * how the engine signals it has finished emitting the command's response
     * and is waiting for the next one.
     */
    public synchronized String execute(String sql) throws IOException {
        if (process == null || !process.isAlive()) {
            start();
        }

        stdin.write(sql.endsWith(";") ? sql : sql + ";");
        stdin.newLine();
        stdin.flush();

        String output = readUntilQuiet("execute", EXECUTE_TIMEOUT_MS);
        if (output.isEmpty() && (process == null || !process.isAlive())) {
            throw new IOException("miniSQL engine exited before producing output");
        }
        return output;
    }

    public synchronized void restart() throws IOException {
        log.info("Restarting MiniSQL engine...");
        desiredRunning = true;
        destroyProcess();
        startMonitorIfNeeded();
        startProcessIfNeeded();
    }

    /**
     * Triggers a checkpoint and returns the reported LSN.
     * Returns -1 if checkpoint failed.
     */
    public synchronized long checkpoint() throws IOException {
        String output = execute("checkpoint;");
        // Looking for: ">>> Checkpoint SUCCESS. Data flushed up to LSN: 123"
        int lsnIndex = output.indexOf("LSN: ");
        if (lsnIndex != -1) {
            String lsnStr = output.substring(lsnIndex + 5).trim().split("\\s+")[0];
            try {
                return Long.parseLong(lsnStr);
            } catch (NumberFormatException e) {
                log.error("Failed to parse LSN from output: {}", output);
                return -1;
            }
        }
        return -1;
    }

    public synchronized void stop() {
        desiredRunning = false;
        destroyProcess();
    }

    public boolean isAlive() {
        return process != null && process.isAlive();
    }

    private void destroyProcess() {
        if (process == null) {
            return;
        }
        log.info("Stopping MiniSQL engine...");
        try {
            if (stdin != null) {
                stdin.write("exit;");
                stdin.newLine();
                stdin.flush();
            }
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroy();
            }
        } catch (Exception e) {
            process.destroy();
        } finally {
            cleanupHandles();
            process = null;
        }
    }

    private void cleanupHandles() {
        closeQuietly(stdout);
        closeQuietly(stdin);
        stdout = null;
        stdin = null;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            log.debug("Ignore close failure: {}", e.getMessage());
        }
    }
}
