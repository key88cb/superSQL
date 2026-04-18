package edu.zju.supersql.regionserver;

import edu.zju.supersql.regionserver.rpc.ReplicaSyncServiceImpl;
import edu.zju.supersql.rpc.ReplicaSyncService;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Primary-side replica synchronisation coordinator.
 *
 * <p>For each write operation:
 * <ol>
 *   <li>Calls {@code ReplicaSyncService.syncLog(entry)} on all replica RegionServers in parallel.</li>
 *   <li>Waits up to {@value #SYNC_TIMEOUT_MS} ms for ≥ 1 ACK (semi-sync replication).</li>
 *   <li>Calls {@code commitLog} asynchronously on replicas that acknowledged the entry.</li>
 * </ol>
 */
public class ReplicaManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    /** Per-replica connection timeout in milliseconds. */
    private static final int CONNECT_TIMEOUT_MS = 3_000;
    /** Time to wait for ≥ 1 ACK during semi-sync replication. */
    private static final int SYNC_TIMEOUT_MS    = 3_000;

    private final ExecutorService executor =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "replica-sync");
                t.setDaemon(true);
                return t;
            });

    /**
     * Sends a WAL entry to all replicas and waits for ≥ 1 ACK.
     *
     * @param entry     the WAL entry to replicate
     * @param addresses replica addresses in {@code host:port} format
     * @return number of successful ACKs received (0 if all timed out or failed)
     */
    public int syncToReplicas(WalEntry entry, List<String> addresses) {
        return syncToReplicas(entry, addresses, 1);
    }

    /**
     * Sends a WAL entry to all replicas and waits until required ACKs are reached
     * or timeout is exceeded.
     *
     * @param entry        the WAL entry to replicate
     * @param addresses    replica addresses in {@code host:port} format
     * @param requiredAcks required ACK count; if {@code <= 0}, the sync is fire-and-forget
     * @return number of successful ACKs received before returning
     */
    public int syncToReplicas(WalEntry entry, List<String> addresses, int requiredAcks) {
        if (addresses == null || addresses.isEmpty()) {
            return 0;
        }

        if (requiredAcks <= 0) {
            for (String addr : addresses) {
                executor.submit(() -> syncOne(entry, addr));
            }
            return 0;
        }

        int targetAcks = Math.min(requiredAcks, addresses.size());
        ExecutorCompletionService<Boolean> completion = new ExecutorCompletionService<>(executor);
        for (String addr : addresses) {
            completion.submit(() -> syncOne(entry, addr));
        }

        AtomicInteger acks = new AtomicInteger(0);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SYNC_TIMEOUT_MS);
        int finished = 0;

        while (finished < addresses.size() && acks.get() < targetAcks) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            try {
                Future<Boolean> f = completion.poll(remainingNanos, TimeUnit.NANOSECONDS);
                if (f == null) {
                    break;
                }
                finished++;
                if (Boolean.TRUE.equals(f.get())) {
                    acks.incrementAndGet();
                }
            } catch (Exception e) {
                finished++;
                log.warn("syncToReplicas: failed while waiting ACK (lsn={}): {}",
                        entry.getLsn(), e.getMessage());
            }
        }

        if (acks.get() < targetAcks) {
            log.warn("syncToReplicas: insufficient ACKs before timeout (lsn={} required={} actual={} replicas={})",
                    entry.getLsn(), targetAcks, acks.get(), addresses.size());
        }
        return acks.get();
    }

    /**
     * Sends a {@code commitLog} notification to all replicas asynchronously (best-effort).
     *
     * @param tableName target table
     * @param lsn       the LSN to commit
     * @param addresses replica addresses in {@code host:port} format
     */
    public void commitOnReplicas(String tableName, long lsn, List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            return;
        }
        for (String addr : addresses) {
            executor.submit(() -> commitOne(tableName, lsn, addr));
        }
    }

    // ─────────────────────── private helpers ──────────────────────────────────

    private boolean syncOne(WalEntry entry, String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            log.warn("Invalid replica address: {}", address);
            return false;
        }
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            log.warn("Invalid port in replica address: {}", address);
            return false;
        }

        try (TTransport transport = new TFramedTransport(
                new TSocket(host, port, CONNECT_TIMEOUT_MS))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            Response resp = client.syncLog(entry);
            if (resp.getCode() == StatusCode.OK) {
                log.debug("syncLog ACK from {} for lsn={}", address, entry.getLsn());
                return true;
            } else {
                log.warn("syncLog NACK from {} for lsn={}: {}", address, entry.getLsn(),
                        resp.getMessage());
                return false;
            }
        } catch (Exception e) {
            log.warn("syncLog to {} failed for lsn={}: {}", address, entry.getLsn(),
                    e.getMessage());
            return false;
        }
    }

    private void commitOne(String tableName, long lsn, String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) return;
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return;
        }

        try (TTransport transport = new TFramedTransport(
                new TSocket(host, port, CONNECT_TIMEOUT_MS))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            Response resp = client.commitLog(tableName, lsn);
            log.debug("commitLog to {} for table={} lsn={}: code={}",
                    address, tableName, lsn, resp.getCode());
        } catch (Exception e) {
            log.warn("commitLog to {} failed for table={} lsn={}: {}",
                    address, tableName, lsn, e.getMessage());
        }
    }
}
