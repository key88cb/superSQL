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
        Assertions.assertEquals(5, config.movingRetryMaxAttempts());
        Assertions.assertEquals(300, config.movingRetryInitialBackoffMs());
        Assertions.assertEquals(200, config.movingRetryBackoffStepMs());
        Assertions.assertEquals(ClientConfig.ReadConsistency.EVENTUAL, config.readConsistency());
    }

    @Test
    void shouldFallbackToLegacyClientZkKeyWhenUnifiedMissing() {
        ClientConfig config = ClientConfig.fromEnv(Map.of(
                "CLIENT_ZK_CONNECT", "zk-client:2181",
                "CLIENT_CACHE_TTL_MS", "45000",
                "CLIENT_MOVING_RETRY_MAX_ATTEMPTS", "7",
                "CLIENT_MOVING_RETRY_INITIAL_BACKOFF_MS", "120",
                "CLIENT_MOVING_RETRY_BACKOFF_STEP_MS", "80",
                "CLIENT_READ_CONSISTENCY", "STRONG"
        ));

        Assertions.assertEquals("zk-client:2181", config.zkConnect());
        Assertions.assertEquals(45_000L, config.cacheTtlMs());
        Assertions.assertEquals(7, config.movingRetryMaxAttempts());
        Assertions.assertEquals(120, config.movingRetryInitialBackoffMs());
        Assertions.assertEquals(80, config.movingRetryBackoffStepMs());
        Assertions.assertEquals(ClientConfig.ReadConsistency.STRONG, config.readConsistency());
    }
}
