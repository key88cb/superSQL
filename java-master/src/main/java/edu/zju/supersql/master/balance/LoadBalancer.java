package edu.zju.supersql.master.balance;

import edu.zju.supersql.rpc.RegionServerInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Minimal load-balancing helper used by Master metadata workflows.
 */
public class LoadBalancer {

    public List<RegionServerInfo> selectReplicas(List<RegionServerInfo> candidates, int replicaCount) {
        if (candidates == null || candidates.isEmpty() || replicaCount <= 0) {
            return List.of();
        }
        List<RegionServerInfo> sorted = new ArrayList<>(candidates);
        sorted.sort(Comparator
                .comparingInt((RegionServerInfo rs) -> rs.isSetTableCount() ? rs.getTableCount() : 0)
                .thenComparing(RegionServerInfo::getId));
        return new ArrayList<>(sorted.subList(0, Math.min(replicaCount, sorted.size())));
    }

    public boolean isBalanced(List<RegionServerInfo> regionServers, double ratio) {
        if (regionServers == null || regionServers.isEmpty()) {
            return true;
        }
        double avg = regionServers.stream()
                .mapToInt(rs -> rs.isSetTableCount() ? rs.getTableCount() : 0)
                .average()
                .orElse(0.0);
        if (avg == 0.0) {
            return true;
        }
        return regionServers.stream()
                .mapToInt(rs -> rs.isSetTableCount() ? rs.getTableCount() : 0)
                .max()
                .orElse(0) <= avg * ratio;
    }

    public RegionServerInfo hottest(List<RegionServerInfo> regionServers) {
        return regionServers.stream()
                .max(Comparator
                        .comparingInt((RegionServerInfo rs) -> rs.isSetTableCount() ? rs.getTableCount() : 0)
                        .thenComparing(RegionServerInfo::getId))
                .orElse(null);
    }

    public RegionServerInfo leastLoadedExcluding(List<RegionServerInfo> regionServers, List<String> excludedIds) {
        return regionServers.stream()
                .filter(rs -> excludedIds == null || !excludedIds.contains(rs.getId()))
                .min(Comparator
                        .comparingInt((RegionServerInfo rs) -> rs.isSetTableCount() ? rs.getTableCount() : 0)
                        .thenComparing(RegionServerInfo::getId))
                .orElse(null);
    }
}
