package edu.zju.supersql.regionserver.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.regionserver.ZkPaths;
import edu.zju.supersql.rpc.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

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
    private static final int STATUS_FAILURE_MESSAGE_MAX_LEN = 256;
    private static final int TRANSFER_FAILURE_HISTORY_MAX_SIZE = 8;
    private static final String STAGING_SUFFIX = ".part";
    private static final String TRANSFER_MANIFEST_PREFIX = "__supersql_transfer_manifest__.";

    private final WriteGuard writeGuard;
    private final CuratorFramework zkClient;
    private final String dataDir;
    private final String rsId;
    private final Path dataRootPath;
    private final Map<String, Long> nextExpectedOffsets = new ConcurrentHashMap<>();
    private final AtomicLong manifestVerificationTotal = new AtomicLong();
    private final AtomicLong manifestVerificationSuccess = new AtomicLong();
    private final AtomicLong manifestVerificationFailure = new AtomicLong();
    private final AtomicLong manifestVerificationDuplicateAcks = new AtomicLong();
    private final AtomicLong manifestVerificationLastSuccessTs = new AtomicLong();
    private final AtomicLong manifestVerificationLastFailureTs = new AtomicLong();
    private final AtomicLong manifestFailureInvalidManifest = new AtomicLong();
    private final AtomicLong manifestFailureScopeViolation = new AtomicLong();
    private final AtomicLong manifestFailureFileMissing = new AtomicLong();
    private final AtomicLong manifestFailureSizeMismatch = new AtomicLong();
    private final AtomicLong manifestFailureChecksumMismatch = new AtomicLong();
    private final AtomicLong manifestFailureOther = new AtomicLong();
    private volatile String manifestVerificationLastFailureReason = "";
    private volatile String manifestVerificationLastFailureMessage = "";
    private final Map<String, Long> manifestVerifiedDigestByTable = new ConcurrentHashMap<>();
    private final AtomicLong transferTableTotal = new AtomicLong();
    private final AtomicLong transferTableSuccess = new AtomicLong();
    private final AtomicLong transferTableFailure = new AtomicLong();
    private final AtomicLong transferTableLastSuccessTs = new AtomicLong();
    private final AtomicLong transferTableFailureTableNotFound = new AtomicLong();
    private final AtomicLong transferTableFailureTargetReject = new AtomicLong();
    private final AtomicLong transferTableFailureTransportError = new AtomicLong();
    private final AtomicLong transferTableFailureSourceIoError = new AtomicLong();
    private final AtomicLong transferTableFailureOther = new AtomicLong();
    private final AtomicLong transferTableLastFailureTs = new AtomicLong();
    private final AtomicLong transferTableRecentFailuresDropped = new AtomicLong();
    private volatile String transferTableLastFailureReason = "";
    private volatile String transferTableLastFailureMessage = "";
    private final Object transferFailureHistoryLock = new Object();
    private final Deque<Map<String, Object>> transferTableRecentFailures = new ArrayDeque<>();

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
        transferTableTotal.incrementAndGet();
        try {
            File dir = new File(dataDir);
            if (!dir.exists() || !dir.isDirectory()) {
                return transferTableFailed(
                        StatusCode.ERROR,
                        "other",
                        "dataDir not found: " + dataDir);
            }

            File[] tableFiles = dir.listFiles(f -> f.getName().startsWith(tableName)
                    && !f.getName().endsWith(STAGING_SUFFIX));
            if (tableFiles == null || tableFiles.length == 0) {
                return transferTableFailed(
                    StatusCode.TABLE_NOT_FOUND,
                    "table_not_found",
                    "No files found for table: " + tableName);
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

                sendTransferManifest(client, tableName, tableFiles);
            } finally {
                transport.close();
            }

            transferTableSuccess.incrementAndGet();
            transferTableLastSuccessTs.set(System.currentTimeMillis());
            Response r = new Response(StatusCode.OK);
            r.setMessage("transfer completed for " + tableName);
            return r;
        } catch (Exception e) {
            log.error("transferTable failed for table={}", tableName, e);
            return transferTableFailed(
                    StatusCode.ERROR,
                    classifyTransferFailureReason(e),
                    "transferTable failed: " + e.getMessage());
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

        if (isTransferManifestChunk(chunk)) {
            return validateTransferManifestChunk(chunk);
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
            Long inMemoryExpectedOffset = nextExpectedOffsets.get(transferKey);
            long expectedOffset = (inMemoryExpectedOffset != null)
                    ? inMemoryExpectedOffset
                    : recoverExpectedOffsetFromStaging(stagingPath, chunk.getOffset());
            if (inMemoryExpectedOffset == null && expectedOffset > 0L) {
                nextExpectedOffsets.put(transferKey, expectedOffset);
            }

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

    private void sendTransferManifest(RegionAdminService.Client client,
                                      String tableName,
                                      File[] tableFiles) throws Exception {
        List<Map<String, Object>> files = new ArrayList<>();
        if (tableFiles != null) {
            for (File file : tableFiles) {
                Map<String, Object> item = new ConcurrentHashMap<>();
                item.put("fileName", file.getName());
                item.put("size", file.length());
                item.put("crc32", computeCrc32(resolveDataPath(file.getName())));
                files.add(item);
            }
        }
        Map<String, Object> manifest = new ConcurrentHashMap<>();
        manifest.put("tableName", tableName);
        manifest.put("files", files);
        byte[] payload = MAPPER.writeValueAsBytes(manifest);
        sendChunkWithRetry(client,
                tableName,
                transferManifestFileName(tableName),
                0L,
                payload,
                true);
    }

    private Response validateTransferManifestChunk(DataChunk chunk) {
        manifestVerificationTotal.incrementAndGet();
        if (chunk.getOffset() != 0L || !chunk.isIsLast()) {
            return manifestVerificationFailed("invalid_manifest", "Invalid transfer manifest chunk metadata");
        }

        try {
            ByteBuffer dataBuf = chunk.bufferForData();
            byte[] manifestBytes = new byte[dataBuf.remaining()];
            dataBuf.get(manifestBytes);

            Map<?, ?> manifest = MAPPER.readValue(manifestBytes, Map.class);
            String manifestTable = String.valueOf(manifest.get("tableName"));
            if (chunk.isSetTableName() && manifestTable != null
                    && !manifestTable.isBlank()
                    && !manifestTable.equals(chunk.getTableName())) {
                return manifestVerificationFailed("invalid_manifest", "transfer manifest table mismatch");
            }

            Object rawFiles = manifest.get("files");
            if (!(rawFiles instanceof List<?> files)) {
                return manifestVerificationFailed("invalid_manifest", "transfer manifest missing files list");
            }
            if (files.isEmpty()) {
                return manifestVerificationFailed("invalid_manifest", "transfer manifest has empty files list");
            }

            String expectedTablePrefix = null;
            if (manifestTable != null && !manifestTable.isBlank()) {
                expectedTablePrefix = manifestTable;
            } else if (chunk.isSetTableName() && chunk.getTableName() != null && !chunk.getTableName().isBlank()) {
                expectedTablePrefix = chunk.getTableName();
            }
            if (expectedTablePrefix == null) {
                return manifestVerificationFailed("invalid_manifest", "transfer manifest missing tableName");
            }

            Set<String> seenFileNames = new HashSet<>();
            long manifestDigest = computeCrc32(manifestBytes);
            Long lastDigest = manifestVerifiedDigestByTable.get(expectedTablePrefix);
            if (lastDigest != null && lastDigest == manifestDigest) {
                return manifestVerificationDuplicate(files.size());
            }

            for (Object raw : files) {
                if (!(raw instanceof Map<?, ?> fileItem)) {
                    return manifestVerificationFailed("invalid_manifest", "transfer manifest has invalid file item");
                }
                String fileName = String.valueOf(fileItem.get("fileName"));
                if (fileName == null || fileName.isBlank()) {
                    return manifestVerificationFailed("invalid_manifest", "transfer manifest has empty fileName");
                }
                if (fileName.endsWith(STAGING_SUFFIX)
                        || fileName.startsWith(TRANSFER_MANIFEST_PREFIX)) {
                    return manifestVerificationFailed("scope_violation", "transfer manifest has invalid data fileName " + fileName);
                }
                if (!fileName.startsWith(expectedTablePrefix)) {
                    return manifestVerificationFailed("scope_violation", "transfer manifest file outside table scope " + fileName);
                }
                if (!seenFileNames.add(fileName)) {
                    return manifestVerificationFailed("scope_violation", "transfer manifest has duplicate fileName " + fileName);
                }
                long expectedSize = toLong(fileItem.get("size"), -1L);
                if (expectedSize < 0L) {
                    return manifestVerificationFailed("invalid_manifest", "transfer manifest has invalid size for " + fileName);
                }
                long expectedCrc32 = toLong(fileItem.get("crc32"), -1L);
                if (expectedCrc32 < 0L) {
                    return manifestVerificationFailed("invalid_manifest", "transfer manifest has invalid crc32 for " + fileName);
                }

                Path filePath = resolveDataPath(fileName);
                if (!Files.exists(filePath)) {
                    return manifestVerificationFailed("file_missing", "transfer manifest verification failed: missing " + fileName);
                }

                long actualSize = Files.size(filePath);
                if (actualSize != expectedSize) {
                    return manifestVerificationFailed("size_mismatch", "transfer manifest verification failed: size mismatch for "
                            + fileName + " expected=" + expectedSize + " actual=" + actualSize);
                }

                long actualCrc32 = computeCrc32(filePath);
                if (actualCrc32 != expectedCrc32) {
                    return manifestVerificationFailed("checksum_mismatch", "transfer manifest verification failed: checksum mismatch for "
                            + fileName + " expected=" + expectedCrc32 + " actual=" + actualCrc32);
                }
            }

            manifestVerifiedDigestByTable.put(expectedTablePrefix, manifestDigest);
            return manifestVerificationSucceeded(files.size());
        } catch (Exception e) {
            log.error("transfer manifest verification failed for table={}", chunk.getTableName(), e);
            return manifestVerificationFailed("other", "transfer manifest verification failed: " + e.getMessage());
        }
    }

    public Map<String, Object> getTransferManifestVerificationStats() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("total", manifestVerificationTotal.get());
        payload.put("success", manifestVerificationSuccess.get());
        payload.put("failure", manifestVerificationFailure.get());
        payload.put("duplicateAcks", manifestVerificationDuplicateAcks.get());
        payload.put("lastSuccessTs", manifestVerificationLastSuccessTs.get());
        payload.put("lastFailureTs", manifestVerificationLastFailureTs.get());
        Map<String, Object> reasons = new LinkedHashMap<>();
        reasons.put("invalid_manifest", manifestFailureInvalidManifest.get());
        reasons.put("scope_violation", manifestFailureScopeViolation.get());
        reasons.put("file_missing", manifestFailureFileMissing.get());
        reasons.put("size_mismatch", manifestFailureSizeMismatch.get());
        reasons.put("checksum_mismatch", manifestFailureChecksumMismatch.get());
        reasons.put("other", manifestFailureOther.get());
        payload.put("failureReasons", reasons);
        payload.put("lastFailureReason", manifestVerificationLastFailureReason);
        payload.put("lastFailureMessage", manifestVerificationLastFailureMessage);
        return payload;
    }

    public Map<String, Object> getTransferTableStats() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("total", transferTableTotal.get());
        payload.put("success", transferTableSuccess.get());
        payload.put("failure", transferTableFailure.get());
        payload.put("lastSuccessTs", transferTableLastSuccessTs.get());

        Map<String, Object> reasons = new LinkedHashMap<>();
        reasons.put("table_not_found", transferTableFailureTableNotFound.get());
        reasons.put("target_reject", transferTableFailureTargetReject.get());
        reasons.put("transport_error", transferTableFailureTransportError.get());
        reasons.put("source_io_error", transferTableFailureSourceIoError.get());
        reasons.put("other", transferTableFailureOther.get());

        payload.put("failureReasons", reasons);
        payload.put("lastFailureTs", transferTableLastFailureTs.get());
        payload.put("lastFailureReason", transferTableLastFailureReason);
        payload.put("lastFailureMessage", transferTableLastFailureMessage);
        payload.put("recentFailures", snapshotRecentTransferFailures());
        payload.put("recentFailuresDropped", transferTableRecentFailuresDropped.get());
        return payload;
    }

    private Response manifestVerificationSucceeded(int fileCount) {
        manifestVerificationSuccess.incrementAndGet();
        manifestVerificationLastSuccessTs.set(System.currentTimeMillis());
        Response r = new Response(StatusCode.OK);
        r.setMessage("transfer manifest verified files=" + fileCount);
        return r;
    }

    private Response manifestVerificationDuplicate(int fileCount) {
        manifestVerificationSuccess.incrementAndGet();
        manifestVerificationDuplicateAcks.incrementAndGet();
        manifestVerificationLastSuccessTs.set(System.currentTimeMillis());
        Response r = new Response(StatusCode.OK);
        r.setMessage("duplicate transfer manifest acknowledged files=" + fileCount);
        return r;
    }

    private Response manifestVerificationFailed(String reason, String message) {
        manifestVerificationFailure.incrementAndGet();
        manifestVerificationLastFailureTs.set(System.currentTimeMillis());
        manifestVerificationLastFailureReason = reason == null ? "other" : reason;
        manifestVerificationLastFailureMessage = sanitizeStatusMessage(message);

        switch (manifestVerificationLastFailureReason) {
            case "invalid_manifest" -> manifestFailureInvalidManifest.incrementAndGet();
            case "scope_violation" -> manifestFailureScopeViolation.incrementAndGet();
            case "file_missing" -> manifestFailureFileMissing.incrementAndGet();
            case "size_mismatch" -> manifestFailureSizeMismatch.incrementAndGet();
            case "checksum_mismatch" -> manifestFailureChecksumMismatch.incrementAndGet();
            default -> manifestFailureOther.incrementAndGet();
        }

        Response r = new Response(StatusCode.ERROR);
        r.setMessage(message);
        return r;
    }

    private Response transferTableFailed(StatusCode code, String reason, String message) {
        transferTableFailure.incrementAndGet();
        long now = System.currentTimeMillis();
        transferTableLastFailureTs.set(now);
        transferTableLastFailureReason = reason;
        String sanitizedMessage = sanitizeStatusMessage(message);
        transferTableLastFailureMessage = sanitizedMessage;
        recordTransferFailure(now, code, reason, sanitizedMessage);

        switch (reason) {
            case "table_not_found" -> transferTableFailureTableNotFound.incrementAndGet();
            case "target_reject" -> transferTableFailureTargetReject.incrementAndGet();
            case "transport_error" -> transferTableFailureTransportError.incrementAndGet();
            case "source_io_error" -> transferTableFailureSourceIoError.incrementAndGet();
            default -> transferTableFailureOther.incrementAndGet();
        }

        Response r = new Response(code);
        r.setMessage(message);
        return r;
    }

    private List<Map<String, Object>> snapshotRecentTransferFailures() {
        synchronized (transferFailureHistoryLock) {
            return new ArrayList<>(transferTableRecentFailures);
        }
    }

    private void recordTransferFailure(long ts, StatusCode code, String reason, String message) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("ts", ts);
        event.put("reason", reason == null ? "" : reason);
        event.put("code", code == null ? "" : code.name());
        event.put("message", message == null ? "" : message);

        synchronized (transferFailureHistoryLock) {
            transferTableRecentFailures.addLast(event);
            while (transferTableRecentFailures.size() > TRANSFER_FAILURE_HISTORY_MAX_SIZE) {
                transferTableRecentFailures.removeFirst();
                transferTableRecentFailuresDropped.incrementAndGet();
            }
        }
    }

    private static String sanitizeStatusMessage(String message) {
        if (message == null) {
            return "";
        }
        String normalized = message
                .replace('\r', ' ')
                .replace('\n', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        if (normalized.length() <= STATUS_FAILURE_MESSAGE_MAX_LEN) {
            return normalized;
        }
        return normalized.substring(0, STATUS_FAILURE_MESSAGE_MAX_LEN);
    }

    private static String classifyTransferFailureReason(Exception e) {
        if (e == null) {
            return "other";
        }
        if (e instanceof TTransportException) {
            return "transport_error";
        }
        String message = e.getMessage();
        if (message != null && message.contains("copyTableData rejected")) {
            return "target_reject";
        }
        if (e instanceof IOException) {
            return "source_io_error";
        }
        Throwable cause = e.getCause();
        if (cause instanceof TTransportException) {
            return "transport_error";
        }
        if (cause instanceof IOException) {
            return "source_io_error";
        }
        return "other";
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

    private static long computeCrc32(Path filePath) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[8192];
        try (InputStream in = Files.newInputStream(filePath)) {
            int read;
            while ((read = in.read(buffer)) > 0) {
                crc32.update(buffer, 0, read);
            }
        }
        return crc32.getValue();
    }

    private static long computeCrc32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data, 0, data.length);
        return crc32.getValue();
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

    private static boolean isTransferManifestChunk(DataChunk chunk) {
        return chunk != null
                && chunk.isSetFileName()
                && chunk.getFileName().startsWith(TRANSFER_MANIFEST_PREFIX);
    }

    private static String transferManifestFileName(String tableName) {
        return TRANSFER_MANIFEST_PREFIX + tableName + ".json";
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

    private static long recoverExpectedOffsetFromStaging(Path stagingPath, long incomingOffset) {
        if (incomingOffset <= 0L) {
            return 0L;
        }
        try {
            return Files.exists(stagingPath) ? Files.size(stagingPath) : 0L;
        } catch (IOException e) {
            return 0L;
        }
    }
}
