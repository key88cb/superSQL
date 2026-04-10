package edu.zju.supersql.master.balance;

import edu.zju.supersql.rpc.RegionServerInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class LoadBalancerTest {

    private final LoadBalancer loadBalancer = new LoadBalancer();

    @Test
    void shouldSelectLeastLoadedReplicas() {
        RegionServerInfo rs1 = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        rs1.setTableCount(9);
        RegionServerInfo rs2 = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        rs2.setTableCount(1);
        RegionServerInfo rs3 = new RegionServerInfo("rs-3", "127.0.0.1", 9092);
        rs3.setTableCount(2);

        List<RegionServerInfo> replicas = loadBalancer.selectReplicas(List.of(rs1, rs2, rs3), 2);

        Assertions.assertEquals(List.of("rs-2", "rs-3"),
                replicas.stream().map(RegionServerInfo::getId).toList());
    }

    @Test
    void shouldDetectBalancedCluster() {
        RegionServerInfo rs1 = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        rs1.setTableCount(2);
        RegionServerInfo rs2 = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        rs2.setTableCount(2);
        RegionServerInfo rs3 = new RegionServerInfo("rs-3", "127.0.0.1", 9092);
        rs3.setTableCount(3);

        Assertions.assertTrue(loadBalancer.isBalanced(List.of(rs1, rs2, rs3), 1.5));
    }
}
