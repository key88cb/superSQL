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
    
    private Process process;
    private BufferedWriter stdin;
    private BufferedReader stdout;
    private String dataDir;
    private String binPath;

    public MiniSqlProcess(String binPath, String dataDir) {
        this.binPath = binPath;
        this.dataDir = dataDir;
    }

    /**
     * Starts the MiniSQL process and triggers self-recovery.
     */
    public synchronized void start() throws IOException {
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
        
        // Start a thread to keep logging engine output in background if needed
        // For now, we manually read it in execute()
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
            if (line.contains("miniSQL>")) {
                break;
            }
        }
        return output.toString();
    }

    public synchronized void stop() {
        if (process != null) {
            log.info("Stopping MiniSQL engine...");
            try {
                stdin.write("exit;");
                stdin.newLine();
                stdin.flush();
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroy();
                }
            } catch (Exception e) {
                process.destroy();
            }
            process = null;
        }
    }

    public boolean isAlive() {
        return process != null && process.isAlive();
    }
}
