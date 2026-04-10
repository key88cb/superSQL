package edu.zju.supersql.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class ClientConfigTest {

    @Test
    void shouldReadDefaultsWhenEnvMissing() {
        ClientConfig config = ClientConfig.fromEnv(Map.of());

        Assertions.assertEquals("zk1:2181,zk2:2181,zk3:2181", config.zkConnect());
        Assertions.assertEquals("master-1:8080", config.masterFallback());
        Assertions.assertEquals(30_000L, config.cacheTtlMs());
        Assertions.assertEquals(5_000, config.masterRpcTimeoutMs());
        Assertions.assertEquals(10_000, config.regionRpcTimeoutMs());
    }

    @Test
    void shouldFallbackToLegacyClientZkKeyWhenUnifiedMissing() {
        ClientConfig config = ClientConfig.fromEnv(Map.of(
                "CLIENT_ZK_CONNECT", "zk-client:2181",
                "CLIENT_CACHE_TTL_MS", "45000"
        ));

        Assertions.assertEquals("zk-client:2181", config.zkConnect());
        Assertions.assertEquals(45_000L, config.cacheTtlMs());
    }
}
