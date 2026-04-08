package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared runtime context for Master-side services.
 */
public final class MasterRuntimeContext {

    private static final Logger log = LoggerFactory.getLogger(MasterRuntimeContext.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static volatile CuratorFramework zkClient;
    private static volatile String masterId;
    private static volatile String masterAddress;

    private MasterRuntimeContext() {
    }

    public static void initialize(CuratorFramework client, String id, int thriftPort) {
        zkClient = client;
        masterId = id;
        masterAddress = id + ":" + thriftPort;
    }

    public static CuratorFramework getZkClient() {
        return zkClient;
    }

    public static String getMasterId() {
        return masterId;
    }

    public static String getMasterAddress() {
        return masterAddress;
    }

    public static boolean isReady() {
        return zkClient != null;
    }

    public static void tryBootstrapActiveMaster() {
        if (!isReady()) {
            return;
        }
        try {
            byte[] existing = zkClient.getData().forPath("/active-master");
            if (existing == null || existing.length == 0) {
                writeActiveMaster(1L);
                return;
            }

            Map<?, ?> data = MAPPER.readValue(existing, Map.class);
            Object id = data.get("masterId");
            if (id == null || String.valueOf(id).isBlank()) {
                long epoch = toLong(data.get("epoch"), 0L) + 1L;
                writeActiveMaster(epoch);
            }
        } catch (Exception e) {
            log.warn("Bootstrap active master failed: {}", e.getMessage());
        }
    }

    public static boolean isActiveMaster() {
        if (!isReady()) {
            return true;
        }
        try {
            byte[] bytes = zkClient.getData().forPath("/active-master");
            if (bytes == null || bytes.length == 0) {
                return true;
            }
            Map<?, ?> data = MAPPER.readValue(bytes, Map.class);
            Object id = data.get("masterId");
            return id != null && masterId != null && masterId.equals(String.valueOf(id));
        } catch (Exception e) {
            log.warn("Read active-master failed, fallback as active: {}", e.getMessage());
            return true;
        }
    }

    public static String readActiveMasterAddress() {
        if (!isReady()) {
            return masterAddress;
        }
        try {
            byte[] bytes = zkClient.getData().forPath("/active-master");
            if (bytes == null || bytes.length == 0) {
                return masterAddress;
            }
            Map<?, ?> data = MAPPER.readValue(bytes, Map.class);
            Object address = data.get("address");
            if (address != null && !String.valueOf(address).isBlank()) {
                return String.valueOf(address);
            }
            Object id = data.get("masterId");
            return id == null ? masterAddress : String.valueOf(id);
        } catch (Exception e) {
            log.warn("Read active master address failed: {}", e.getMessage());
            return masterAddress;
        }
    }

    private static void writeActiveMaster(long epoch) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("epoch", epoch);
        payload.put("masterId", masterId);
        payload.put("address", masterAddress);
        payload.put("ts", System.currentTimeMillis());
        byte[] bytes = MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        zkClient.setData().forPath("/active-master", bytes);
        log.info("Active master bootstrap write success: {}", masterAddress);
    }

    private static long toLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }
}