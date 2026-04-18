package edu.zju.supersql.regionserver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class RegionServerConfigTest {

    @Test
    void shouldReadDefaultsWhenEnvMissing() {
        RegionServerConfig config = RegionServerConfig.fromEnv(Map.of());

        Assertions.assertEquals("rs-1", config.rsId());
        Assertions.assertEquals("rs-1", config.rsHost());
        Assertions.assertEquals(9090, config.thriftPort());
        Assertions.assertEquals(10_000L, config.heartbeatIntervalMs());
        Assertions.assertEquals(1, config.minReplicaAcks());
    }

    @Test
    void shouldUseUnifiedZkConnectAndCustomHeartbeatWhenPresent() {
        RegionServerConfig config = RegionServerConfig.fromEnv(Map.of(
                "ZK_CONNECT", "zk-live:2181",
                "RS_ZK_CONNECT", "zk-legacy:2181",
            "RS_HEARTBEAT_INTERVAL_MS", "3000",
            "RS_MIN_REPLICA_ACKS", "2"
        ));

        Assertions.assertEquals("zk-live:2181", config.zkConnect());
        Assertions.assertEquals(3_000L, config.heartbeatIntervalMs());
        Assertions.assertEquals(2, config.minReplicaAcks());
    }
}
