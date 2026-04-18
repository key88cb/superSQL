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
                                Map<String, Object> updatedRoot = copyToStringObjectMap(root);
                                updatedRoot.put("replicas", filteredReplicas);
                                byte[] updated = MAPPER.writeValueAsString(updatedRoot).getBytes(StandardCharsets.UTF_8);
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

            Map<?, ?> rawRoot = MAPPER.readValue(data, Map.class);
            Map<String, Object> root = copyToStringObjectMap(rawRoot);
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
            Path targetPath = resolveDataPath(chunk.getFileName());
            Path stagingPath = resolveDataPath(chunk.getFileName() + STAGING_SUFFIX);
            File targetFile = targetPath.toFile();
            File stagingFile = stagingPath.toFile();
            targetFile.getParentFile().mkdirs();

            ByteBuffer dataBuf = chunk.bufferForData();
            byte[] bytes = new byte[dataBuf.remaining()];
            dataBuf.get(bytes);

            String transferKey = transferKey(chunk.getTableName(), chunk.getFileName());
            long expectedOffset = nextExpectedOffsets.getOrDefault(transferKey, 0L);

            // If final chunk response was lost after publish, accept duplicate retry as success.
            if (expectedOffset == 0L && chunk.isIsLast()
                    && isDuplicateChunk(targetPath, chunk.getOffset(), bytes)) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("duplicate chunk acknowledged for " + chunk.getFileName());
                return r;
            }

            // If non-final chunk response was lost after write, accept duplicate retry as success.
            if (chunk.getOffset() < expectedOffset
                    && isDuplicateChunk(stagingPath, chunk.getOffset(), bytes)) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("duplicate chunk acknowledged for " + chunk.getFileName());
                return r;
            }

            // Duplicate offset but different content likely means a stale/corrupted retry.
            // Reject it explicitly and keep current in-flight progress for subsequent valid chunks.
            if (chunk.getOffset() < expectedOffset) {
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Invalid chunk: conflicting duplicate offset, expected=" + expectedOffset
                        + ", actual=" + chunk.getOffset());
                return r;
            }

            if (chunk.getOffset() == 0L && expectedOffset > 0L) {
                // Restarting from offset 0 resets unfinished transfer state.
                expectedOffset = 0L;
                resetTransferState(chunk.getTableName(), chunk.getFileName());
            }
            if (chunk.getOffset() != expectedOffset) {
                resetTransferState(chunk.getTableName(), chunk.getFileName());
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Invalid chunk: unexpected offset, expected=" + expectedOffset
                        + ", actual=" + chunk.getOffset());
                return r;
            }

            if (chunk.getOffset() == 0L && stagingFile.exists() && !stagingFile.delete()) {
                throw new IOException("failed to reset staging file " + stagingFile.getName());
            }

            try (RandomAccessFile raf = new RandomAccessFile(stagingFile, "rw")) {
                raf.seek(chunk.getOffset());
                raf.write(bytes);
            }

            long nextOffset = chunk.getOffset() + bytes.length;
            if (chunk.isIsLast()) {
                nextExpectedOffsets.remove(transferKey);
                publishCompletedFile(stagingPath, targetPath);
            } else {
                nextExpectedOffsets.put(transferKey, nextOffset);
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

    private static boolean isDuplicateChunk(Path filePath, long offset, byte[] incoming) {
        if (incoming == null) {
            return false;
        }
        if (offset < 0) {
            return false;
        }
        if (!Files.exists(filePath)) {
            return false;
        }
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            long required = offset + incoming.length;
            if (raf.length() < required) {
                return false;
            }
            raf.seek(offset);
            byte[] existing = new byte[incoming.length];
            int read = raf.read(existing);
            if (read != incoming.length) {
                return false;
            }
            for (int i = 0; i < incoming.length; i++) {
                if (existing[i] != incoming[i]) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Response registerRegionServer(RegionServerInfo info) throws TException {
        if (info == null || info.getId() == null || info.getId().isBlank()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("registerRegionServer failed: missing region server id");
            return r;
        }
        log.info("registerRegionServer: {}", info.getId());

        if (zkClient == null) {
            Response r = new Response(StatusCode.OK);
            r.setMessage("register skipped (zk unavailable)");
            return r;
        }

        try {
            String path = ZkPaths.regionServer(info.getId());
            byte[] payload = regionServerInfoBytes(info, System.currentTimeMillis());
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                        .forPath(path, payload);
            } else {
                zkClient.setData().forPath(path, payload);
            }
            Response r = new Response(StatusCode.OK);
            r.setMessage("registered " + info.getId());
            return r;
        } catch (Exception e) {
            log.error("registerRegionServer failed for rs={}", info.getId(), e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("registerRegionServer failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response heartbeat(RegionServerInfo info) throws TException {
        if (info == null || info.getId() == null || info.getId().isBlank()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("heartbeat failed: missing region server id");
            return r;
        }

        if (zkClient == null) {
            return new Response(StatusCode.OK);
        }

        try {
            String path = ZkPaths.regionServer(info.getId());
            byte[] payload = regionServerInfoBytes(info, System.currentTimeMillis());
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                        .forPath(path, payload);
            } else {
                zkClient.setData().forPath(path, payload);
            }
            return new Response(StatusCode.OK);
        } catch (Exception e) {
            log.error("heartbeat failed for rs={}", info.getId(), e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("heartbeat failed: " + e.getMessage());
            return r;
        }
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

    private static Map<String, Object> copyToStringObjectMap(Map<?, ?> source) {
        Map<String, Object> copy = new ConcurrentHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            copy.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return copy;
    }

    private byte[] regionServerInfoBytes(RegionServerInfo info, long heartbeatTs) throws Exception {
        Map<String, Object> payload = new ConcurrentHashMap<>();
        payload.put("id", info.getId());
        payload.put("host", info.getHost());
        payload.put("port", info.getPort());
        payload.put("tableCount", info.isSetTableCount() ? info.getTableCount() : 0);
        payload.put("qps1min", info.isSetQps1min() ? info.getQps1min() : 0.0);
        payload.put("cpuUsage", info.isSetCpuUsage() ? info.getCpuUsage() : 0.0);
        payload.put("memUsage", info.isSetMemUsage() ? info.getMemUsage() : 0.0);
        payload.put("lastHeartbeat", heartbeatTs);
        return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
    }

    private static void publishCompletedFile(Path stagingPath, Path finalPath) throws IOException {
        try {
            Files.move(stagingPath, finalPath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            log.debug("Atomic move not supported for {} -> {}, fallback to non-atomic move: {}",
                    stagingPath, finalPath, e.getMessage());
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

    private String transferKey(String tableName, String fileName) {
        return (tableName == null ? "" : tableName) + "::" + fileName;
    }

    private void resetTransferState(String tableName, String fileName) {
        nextExpectedOffsets.remove(transferKey(tableName, fileName));
        try {
            Path stagingPath = resolveDataPath(fileName + STAGING_SUFFIX);
            Files.deleteIfExists(stagingPath);
        } catch (Exception e) {
            log.warn("copyTableData resetTransferState failed file={} cause={}", fileName, e.getMessage());
        }
    }
}
