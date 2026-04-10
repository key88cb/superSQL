package edu.zju.supersql.master;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class MasterConfigTest {

    @Test
    void shouldReadDefaultsWhenEnvMissing() {
        MasterConfig config = MasterConfig.fromEnv(Map.of());

        Assertions.assertEquals("master-1", config.masterId());
        Assertions.assertEquals(8080, config.thriftPort());
        Assertions.assertEquals(8880, config.httpPort());
        Assertions.assertEquals("zk1:2181,zk2:2181,zk3:2181", config.zkConnect());
    }

    @Test
    void shouldPreferUnifiedZkConnectOverLegacyKey() {
        MasterConfig config = MasterConfig.fromEnv(Map.of(
                "ZK_CONNECT", "zk-unified:2181",
                "MASTER_ZK_CONNECT", "zk-legacy:2181",
                "MASTER_REBALANCE_RATIO", "2.0"
        ));

        Assertions.assertEquals("zk-unified:2181", config.zkConnect());
        Assertions.assertEquals(2.0, config.rebalanceRatio());
    }
}
