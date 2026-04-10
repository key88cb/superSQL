package edu.zju.supersql.master.meta;

import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.TableLocation;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class MetaManagerIntegrationTest {

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private MetaManager metaManager;
    private AssignmentManager assignmentManager;

    @BeforeEach
    void setUp() throws Exception {
        server = EmbeddedZkServerFactory.create();
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(200, 3))
                .sessionTimeoutMs(10_000)
                .connectionTimeoutMs(5_000)
                .namespace("supersql")
                .build();
        zkClient.start();
        zkClient.blockUntilConnected();

        createIfMissing("/meta/tables");
        createIfMissing("/assignments");

        metaManager = new MetaManager(zkClient);
        assignmentManager = new AssignmentManager(zkClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (zkClient != null) {
            zkClient.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldPersistAndReadTableLocationAndAssignment() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        RegionServerInfo replica = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        TableLocation location = new TableLocation("orders", primary, List.of(primary, replica));
        location.setTableStatus("ACTIVE");
        location.setVersion(42L);

        metaManager.saveTableLocation(location);
        assignmentManager.saveAssignment("orders", List.of(primary, replica));

        TableLocation stored = metaManager.getTableLocation("orders");
        List<RegionServerInfo> assignment = assignmentManager.getAssignment("orders");

        Assertions.assertNotNull(stored);
        Assertions.assertEquals("orders", stored.getTableName());
        Assertions.assertEquals("rs-1", stored.getPrimaryRS().getId());
        Assertions.assertEquals(2, stored.getReplicasSize());
        Assertions.assertEquals(2, assignment.size());
    }

    @Test
    void shouldDeleteStoredMetadata() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("to_delete", primary, List.of(primary));

        metaManager.saveTableLocation(location);
        assignmentManager.saveAssignment("to_delete", List.of(primary));

        metaManager.deleteTableLocation("to_delete");
        assignmentManager.deleteAssignment("to_delete");

        Assertions.assertNull(metaManager.getTableLocation("to_delete"));
        Assertions.assertTrue(assignmentManager.getAssignment("to_delete").isEmpty());
    }

    private void createIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }
}
