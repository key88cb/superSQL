package edu.zju.supersql.regionserver.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.regionserver.ZkPaths;
import edu.zju.supersql.rpc.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RegionAdminService implementation.
 *
 * <p>Handles per-table write pause/resume (for migration), local table deletion,
 * client cache invalidation, and table data transfer between RegionServers.
 */
public class RegionAdminServiceImpl implements RegionAdminService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionAdminServiceImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CHUNK_SIZE = 4096;
    private static final int COPY_CHUNK_MAX_RETRIES = 3;
    private static final String STAGING_SUFFIX = ".part";

    private final WriteGuard writeGuard;
    private final CuratorFramework zkClient;
    private final String dataDir;
    private final String rsId;
    private final Path dataRootPath;
    private final Map<String, Long> nextExpectedOffsets = new ConcurrentHashMap<>();

    public RegionAdminServiceImpl(WriteGuard writeGuard,
                                  CuratorFramework zkClient,
                                  String dataDir,
                                  String rsId) {
        this.writeGuard = writeGuard;
        this.zkClient   = zkClient;
        this.dataDir    = dataDir;
        this.rsId       = rsId;
        this.dataRootPath = new File(dataDir).toPath().toAbsolutePath().normalize();
    }

    @Override
    public Response pauseTableWrite(String tableName) throws TException {
        log.info("pauseTableWrite: table={}", tableName);
        writeGuard.pause(tableName);
        Response r = new Response(StatusCode.OK);
        r.setMessage("write paused for " + tableName);
        return r;
    }

    @Override
    public Response resumeTableWrite(String tableName) throws TException {
        log.info("resumeTableWrite: table={}", tableName);
        writeGuard.resume(tableName);
        Response r = new Response(StatusCode.OK);
        r.setMessage("write resumed for " + tableName);
        return r;
    }

    @Override
    public Response deleteLocalTable(String tableName) throws TException {
        log.info("deleteLocalTable: table={}", tableName);
        try {
            // Delete local data files matching tableName*
            File dir = new File(dataDir);
            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles(f -> f.getName().startsWith(tableName));
                if (files != null) {
                    for (File f : files) {
                        if (f.delete()) {
                            log.debug("deleteLocalTable: deleted {}", f.getName());
                        } else {
                            log.warn("deleteLocalTable: failed to delete {}", f.getName());
                        }
                    }
                }
            }

            // Remove only this RS from assignment replicas; keep other replicas intact.
            if (zkClient != null) {
                String path = ZkPaths.assignment(tableName);
                if (zkClient.checkExists().forPath(path) != null) {
                    byte[] data = zkClient.getData().forPath(path);
                    if (data != null && data.length > 0) {
                        Map<?, ?> root = MAPPER.readValue(data, Map.class);
                        List<?> replicas = (List<?>) root.get("replicas");
                        List<Object> filteredReplicas = new ArrayList<>();
                        boolean thisRsListed = false;
                        if (replicas != null) {
                            for (Object item : replicas) {
                                if (item instanceof Map<?, ?> rs && rsId.equals(String.valueOf(rs.get("id")))) {
                                        thisRsListed = true;
                                        continue;
                                }
                                filteredReplicas.add(item);
                            }
                        }
                        if (thisRsListed) {
                            if (filteredReplicas.isEmpty()) {
                                zkClient.delete().forPath(path);
                                log.info("deleteLocalTable: removed assignment node for table={} after removing rs={}",
                                        tableName, rsId);
                            } else {
                                ((Map<String, Object>) root).put("replicas", filteredReplicas);
                                byte[] updated = MAPPER.writeValueAsString(root).getBytes(StandardCharsets.UTF_8);
                                zkClient.setData().forPath(path, updated);
                                log.info("deleteLocalTable: removed rs={} from assignment of table={} remainingReplicas={}",
                                        rsId, tableName, filteredReplicas.size());
                            }
                        }
                    }
                }
            }

            Response r = new Response(StatusCode.OK);
            r.setMessage("deleted local table " + tableName);
            return r;
        } catch (Exception e) {
            log.error("deleteLocalTable failed for table={}", tableName, e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("deleteLocalTable failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response invalidateClientCache(String tableName) throws TException {
        try {
            if (zkClient == null) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("cache invalidation skipped (zk unavailable) for " + tableName);
                return r;
            }

            String path = ZkPaths.tableMeta(tableName);
            if (zkClient.checkExists().forPath(path) == null) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("cache invalidation skipped (table meta missing) for " + tableName);
                return r;
            }

            byte[] data = zkClient.getData().forPath(path);
            if (data == null || data.length == 0) {
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("table meta is empty for " + tableName);
                return r;
            }

            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            long oldVersion = toLong(root.get("version"), 0L);
            long newVersion = oldVersion + 1L;
            root.put("version", newVersion);
            root.put("cacheInvalidatedAt", System.currentTimeMillis());

            zkClient.setData().forPath(path, MAPPER.writeValueAsString(root).getBytes(StandardCharsets.UTF_8));

            log.info("invalidateClientCache: table={} version {} -> {}", tableName, oldVersion, newVersion);
            Response r = new Response(StatusCode.OK);
            r.setMessage("cache invalidation broadcast for " + tableName + " version=" + newVersion);
            return r;
        } catch (Exception e) {
            log.error("invalidateClientCache failed for table={}", tableName, e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("invalidateClientCache failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response transferTable(String tableName, String targetHost, int targetPort) throws TException {
        log.info("transferTable: table={} → {}:{}", tableName, targetHost, targetPort);
        try {
            File dir = new File(dataDir);
            if (!dir.exists() || !dir.isDirectory()) {
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("dataDir not found: " + dataDir);
                return r;
            }

            File[] tableFiles = dir.listFiles(f -> f.getName().startsWith(tableName)
                    && !f.getName().endsWith(STAGING_SUFFIX));
            if (tableFiles == null || tableFiles.length == 0) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("No files found for table: " + tableName);
                return r;
            }

            // Open Thrift connection to target RS
            org.apache.thrift.transport.TSocket socket =
                    new org.apache.thrift.transport.TSocket(targetHost, targetPort, 10000);
            org.apache.thrift.transport.layered.TFramedTransport transport =
                    new org.apache.thrift.transport.layered.TFramedTransport(socket);
            transport.open();
            try {
                org.apache.thrift.protocol.TBinaryProtocol protocol =
                        new org.apache.thrift.protocol.TBinaryProtocol(transport);
                org.apache.thrift.protocol.TMultiplexedProtocol mux =
                        new org.apache.thrift.protocol.TMultiplexedProtocol(protocol, "RegionAdminService");
                RegionAdminService.Client client = new RegionAdminService.Client(mux);

                for (File file : tableFiles) {
                    streamFile(client, tableName, file);
                }
            } finally {
                transport.close();
            }

            Response r = new Response(StatusCode.OK);
            r.setMessage("transfer completed for " + tableName);
            return r;
        } catch (Exception e) {
            log.error("transferTable failed for table={}", tableName, e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("transferTable failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response copyTableData(DataChunk chunk) throws TException {
        if (chunk == null || !chunk.isSetFileName()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid chunk: missing fileName");
            return r;
        }
        if (chunk.getOffset() < 0) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid chunk: negative offset");
            return r;
        }
        try {
            long expectedOffset = nextExpectedOffsets.getOrDefault(chunk.getFileName(), 0L);
            if (chunk.getOffset() == 0L && expectedOffset > 0L) {
                // Restarting from offset 0 resets unfinished transfer state.
                expectedOffset = 0L;
                resetTransferState(chunk.getFileName());
            }
            if (chunk.getOffset() != expectedOffset) {
                resetTransferState(chunk.getFileName());
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Invalid chunk: unexpected offset, expected=" + expectedOffset
                        + ", actual=" + chunk.getOffset());
                return r;
            }

            Path targetPath = resolveDataPath(chunk.getFileName());
            Path stagingPath = resolveDataPath(chunk.getFileName() + STAGING_SUFFIX);
            File targetFile = targetPath.toFile();
            File stagingFile = stagingPath.toFile();
            targetFile.getParentFile().mkdirs();

            ByteBuffer dataBuf = chunk.bufferForData();
            byte[] bytes = new byte[dataBuf.remaining()];
            dataBuf.get(bytes);

            if (chunk.getOffset() == 0L && stagingFile.exists() && !stagingFile.delete()) {
                throw new IOException("failed to reset staging file " + stagingFile.getName());
            }

            try (RandomAccessFile raf = new RandomAccessFile(stagingFile, "rw")) {
                raf.seek(chunk.getOffset());
                raf.write(bytes);
            }

            long nextOffset = chunk.getOffset() + bytes.length;
            if (chunk.isIsLast()) {
                nextExpectedOffsets.remove(chunk.getFileName());
                publishCompletedFile(stagingPath, targetPath);
            } else {
                nextExpectedOffsets.put(chunk.getFileName(), nextOffset);
            }

            if (chunk.isIsLast()) {
                log.info("copyTableData: completed file={}", chunk.getFileName());
            }

            Response r = new Response(StatusCode.OK);
            r.setMessage("chunk written to " + chunk.getFileName());
            return r;
        } catch (IOException | InvalidPathException | SecurityException e) {
            log.error("copyTableData failed for file={}", chunk.getFileName(), e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("copyTableData failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response registerRegionServer(RegionServerInfo info) throws TException {
        log.info("registerRegionServer: {}", info.getId());
        Response r = new Response(StatusCode.OK);
        r.setMessage("registered");
        return r;
    }

    @Override
    public Response heartbeat(RegionServerInfo info) throws TException {
        log.debug("heartbeat from {}", info.getId());
        return new Response(StatusCode.OK);
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private void streamFile(RegionAdminService.Client client, String tableName, File file) throws Exception {
        long fileSize = file.length();
        if (fileSize == 0) {
            sendChunkWithRetry(client, tableName, file.getName(), 0L, new byte[0], true);
            return;
        }

        long offset = 0;
        byte[] buffer = new byte[CHUNK_SIZE];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (offset < fileSize) {
                int bytesRead = raf.read(buffer);
                if (bytesRead <= 0) break;

                byte[] data = bytesRead == buffer.length ? buffer : copyOf(buffer, bytesRead);
                long nextOffset = offset + bytesRead;
                boolean isLast = (nextOffset >= fileSize);

                sendChunkWithRetry(client, tableName, file.getName(), offset, data, isLast);

                offset = nextOffset;
            }
        }
    }

    private void sendChunkWithRetry(RegionAdminService.Client client,
                                    String tableName,
                                    String fileName,
                                    long offset,
                                    byte[] data,
                                    boolean isLast) throws Exception {
        Response lastResponse = null;
        Exception lastException = null;

        for (int attempt = 1; attempt <= COPY_CHUNK_MAX_RETRIES; attempt++) {
            DataChunk chunk = new DataChunk(tableName, fileName, offset, ByteBuffer.wrap(data), isLast);
            try {
                Response response = client.copyTableData(chunk);
                if (response.getCode() == StatusCode.OK) {
                    return;
                }
                lastResponse = response;
                log.warn("copyTableData attempt failed file={} offset={} attempt={}/{} code={} msg={}",
                        fileName, offset, attempt, COPY_CHUNK_MAX_RETRIES,
                        response.getCode(), response.getMessage());
            } catch (Exception e) {
                lastException = e;
                log.warn("copyTableData attempt exception file={} offset={} attempt={}/{} cause={}",
                        fileName, offset, attempt, COPY_CHUNK_MAX_RETRIES, e.getMessage());
            }
        }

        if (lastResponse != null) {
            throw new IOException("copyTableData rejected file=" + fileName
                    + " offset=" + offset
                    + " code=" + lastResponse.getCode()
                    + " msg=" + lastResponse.getMessage()
                    + " after retries=" + COPY_CHUNK_MAX_RETRIES);
        }
        if (lastException != null) {
            throw new IOException("copyTableData failed file=" + fileName
                    + " offset=" + offset
                    + " after retries=" + COPY_CHUNK_MAX_RETRIES,
                    lastException);
        }
        throw new IOException("copyTableData failed file=" + fileName
                + " offset=" + offset
                + " after retries=" + COPY_CHUNK_MAX_RETRIES);
    }

    private static byte[] copyOf(byte[] src, int len) {
        byte[] dst = new byte[len];
        System.arraycopy(src, 0, dst, 0, len);
        return dst;
    }

    private static long toLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static void publishCompletedFile(Path stagingPath, Path finalPath) throws IOException {
        try {
            Files.move(stagingPath, finalPath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(stagingPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Path resolveDataPath(String fileName) {
        Path resolved = dataRootPath.resolve(fileName).normalize();
        if (!resolved.startsWith(dataRootPath)) {
            throw new SecurityException("unsafe fileName: " + fileName);
        }
        return resolved;
    }

    private void resetTransferState(String fileName) {
        nextExpectedOffsets.remove(fileName);
        try {
            Path stagingPath = resolveDataPath(fileName + STAGING_SUFFIX);
            Files.deleteIfExists(stagingPath);
        } catch (Exception e) {
            log.warn("copyTableData resetTransferState failed file={} cause={}", fileName, e.getMessage());
        }
    }
}
