package edu.zju.supersql.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("TDD spec for planned client retry and redirect features that are not implemented yet")
class SqlClientPlannedFeaturesTddTest {

    @Test
    void shouldFollowNotLeaderRedirectAutomatically() {
        Assertions.assertTrue(true, "Client should transparently follow Master NOT_LEADER redirects");
    }

    @Test
    void shouldRetryWhenTableIsMovingUntilNewRouteBecomesVisible() {
        Assertions.assertTrue(true, "Client should retry MOVING responses with bounded backoff");
    }

    @Test
    void shouldInvalidateRouteCacheWhenMasterBroadcastsVersionChange() {
        Assertions.assertTrue(true, "Client should react to route version changes without waiting for TTL");
    }
}
