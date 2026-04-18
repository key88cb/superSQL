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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private final WriteGuard writeGuard;
    private final CuratorFramework zkClient;
    private final String dataDir;
    private final String rsId;

    public RegionAdminServiceImpl(WriteGuard writeGuard,
                                  CuratorFramework zkClient,
                                  String dataDir,
                                  String rsId) {
        this.writeGuard = writeGuard;
        this.zkClient   = zkClient;
        this.dataDir    = dataDir;
        this.rsId       = rsId;
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

            File[] tableFiles = dir.listFiles(f -> f.getName().startsWith(tableName));
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
                    streamFile(client, file);
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
        try {
            File targetFile = new File(dataDir, chunk.getFileName());
            targetFile.getParentFile().mkdirs();

            ByteBuffer dataBuf = chunk.bufferForData();
            byte[] bytes = new byte[dataBuf.remaining()];
            dataBuf.get(bytes);

            try (RandomAccessFile raf = new RandomAccessFile(targetFile, "rw")) {
                raf.seek(chunk.getOffset());
                raf.write(bytes);
            }

            if (chunk.isIsLast()) {
                log.info("copyTableData: completed file={}", chunk.getFileName());
            }

            Response r = new Response(StatusCode.OK);
            r.setMessage("chunk written to " + chunk.getFileName());
            return r;
        } catch (IOException e) {
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

    private void streamFile(RegionAdminService.Client client, File file) throws Exception {
        // tableName is inferred from the file name (first segment before '_' or dot)
        String tableName = file.getName().replaceAll("[._].*", "");
        long fileSize = file.length();
        long offset = 0;
        byte[] buffer = new byte[CHUNK_SIZE];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (offset < fileSize) {
                int bytesRead = raf.read(buffer);
                if (bytesRead <= 0) break;

                byte[] data = bytesRead == buffer.length ? buffer : copyOf(buffer, bytesRead);
                long nextOffset = offset + bytesRead;
                boolean isLast = (nextOffset >= fileSize);

                DataChunk chunk = new DataChunk(tableName, file.getName(), offset,
                        ByteBuffer.wrap(data), isLast);
                client.copyTableData(chunk);

                offset = nextOffset;
            }
        }
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
}
