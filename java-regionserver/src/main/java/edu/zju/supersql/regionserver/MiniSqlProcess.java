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

        // Ensure data directory exists
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        process = pb.start();
        stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));

        // Check if it started successfully and wait for welcome message
        String line = stdout.readLine();
        if (line == null || !line.contains(">>> Working directory changed to") && !line.contains("Welcome to MiniSQL")) {
             // We might need to read more lines as it might print directory change first
             while (line != null && !line.contains("Welcome to MiniSQL")) {
                 log.info("Engine: {}", line);
                 line = stdout.readLine();
             }
        }
        
        if (line != null) {
            log.info("Engine: {}", line);
            log.info("MiniSQL engine started and recovered successfully.");
        } else {
            throw new IOException("Failed to start MiniSQL engine: Unexpected EOF");
        }
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
     */
    public synchronized String execute(String sql) throws IOException {
        if (process == null || !process.isAlive()) {
            start();
        }

        stdin.write(sql.endsWith(";") ? sql : sql + ";");
        stdin.newLine();
        stdin.flush();

        StringBuilder output = new StringBuilder();
        String line;
        // The engine prints "miniSQL> " as a prompt. We wait for it.
        // Note: This is a synchronous simplified bridge.
        while ((line = stdout.readLine()) != null) {
            output.append(line).append("\n");
            if (line.contains(">>> ")) { // Changed to match my new prompt in main.cc or similar
                break;
            }
        }
        return output.toString();
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
