package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.List;

/**
 * ReplicaSyncService stub — all methods return ERROR/defaults until Sprint 3 implementation.
 */
public class ReplicaSyncServiceImpl implements ReplicaSyncService.Iface {

    private static final Logger log = LoggerFactory.getLogger(ReplicaSyncServiceImpl.class);
    private static final Map<String, ConcurrentSkipListMap<Long, WalEntry>> WAL_BY_TABLE = new ConcurrentHashMap<>();
    private static final Map<String, Set<Long>> COMMITTED_LSNS = new ConcurrentHashMap<>();

    static void resetForTests() {
        WAL_BY_TABLE.clear();
        COMMITTED_LSNS.clear();
    }

    private static WalEntry cloneEntry(WalEntry entry) {
        return new WalEntry(entry);
    }

    private static ConcurrentSkipListMap<Long, WalEntry> tableWal(String tableName) {
        return WAL_BY_TABLE.computeIfAbsent(tableName, key -> new ConcurrentSkipListMap<>());
    }

    @Override
    public Response syncLog(WalEntry entry) throws TException {
        if (entry == null || !entry.isSetTableName() || !entry.isSetLsn()) {
            Response response = new Response(StatusCode.ERROR);
            response.setMessage("Invalid wal entry");
            return response;
        }

        ConcurrentSkipListMap<Long, WalEntry> wal = tableWal(entry.getTableName());
        wal.put(entry.getLsn(), cloneEntry(entry));

        Response response = new Response(StatusCode.OK);
        response.setMessage("ACK lsn=" + entry.getLsn());
        return response;
    }

    @Override
    public List<WalEntry> pullLog(String tableName, long startLsn) throws TException {
        if (tableName == null || tableName.isBlank()) {
            return Collections.emptyList();
        }
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || wal.isEmpty()) {
            return Collections.emptyList();
        }

        List<WalEntry> result = new ArrayList<>();
        for (WalEntry entry : wal.tailMap(startLsn).values()) {
            result.add(cloneEntry(entry));
        }
        result.sort(Comparator.comparingLong(WalEntry::getLsn));
        return result;
    }

    @Override
    public long getMaxLsn(String tableName) throws TException {
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || wal.isEmpty()) {
            return -1L;
        }
        return wal.lastKey();
    }

    @Override
    public Response commitLog(String tableName, long lsn) throws TException {
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || !wal.containsKey(lsn)) {
            Response response = new Response(StatusCode.TABLE_NOT_FOUND);
            response.setMessage("No wal entry found for table=" + tableName + " lsn=" + lsn);
            return response;
        }

        COMMITTED_LSNS.computeIfAbsent(tableName, key -> ConcurrentHashMap.newKeySet()).add(lsn);

        Response response = new Response(StatusCode.OK);
        response.setMessage("COMMITTED lsn=" + lsn);
        return response;
    }
}
