package edu.zju.supersql.regionserver;

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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private static final int COMMIT_RETRY_ATTEMPTS = 3;
    private static final long COMMIT_RETRY_BACKOFF_MS = 200L;
    private static final long PENDING_COMMIT_RETRY_INTERVAL_MS = 1_000L;
    private static final long PENDING_COMMIT_MAX_AGE_MS = TimeUnit.MINUTES.toMillis(30);
    private static final int PENDING_COMMIT_MAX_QUEUE_SIZE = 50_000;
    private static final long PENDING_COMMIT_MIN_BACKOFF_MS = 500L;
    private static final long PENDING_COMMIT_MAX_BACKOFF_MS = 30_000L;
    private static final long PENDING_COMMIT_STALLED_ATTEMPTS = 20L;
    private static final long PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD = 8L;
    private static final long PENDING_COMMIT_DECISION_CANDIDATE_ATTEMPTS = 16L;
    private static final long PENDING_COMMIT_TRANSPORT_ESCALATION_COOLDOWN_MS = 120_000L;
    private static final long PENDING_COMMIT_DECISION_CANDIDATE_COOLDOWN_MS = 300_000L;
    private static final String COMMIT_ERR_TABLE_NOT_FOUND = "table_not_found";
    private static final String COMMIT_ERR_INVALID_ADDRESS = "invalid replica address";
    private static final String COMMIT_ERR_INVALID_PORT = "invalid replica port";
    private static final String COMMIT_ERR_TRANSPORT = "transport_error";
    private static final String COMMIT_ERR_RESPONSE = "response_error";

    private record CommitAttempt(boolean success, String error) {
    }

    private static final class PendingCommit {
        private final String tableName;
        private final long lsn;
        private final String address;
        private final List<String> replicaAddresses;
        private final long firstQueuedAtMs;
        private final AtomicLong attempts = new AtomicLong(0L);
        private final AtomicLong consecutiveTransportFailures = new AtomicLong(0L);
        private volatile boolean decisionCandidateMarked;
        private volatile long lastAttemptAtMs;
        private volatile long nextRetryAtMs;
        private volatile String lastError;

        private PendingCommit(String tableName,
                              long lsn,
                              String address,
                              List<String> replicaAddresses,
                              long nowMs,
                              String initialError) {
            this.tableName = tableName;
            this.lsn = lsn;
            this.address = address;
            this.replicaAddresses = List.copyOf(replicaAddresses);
            this.firstQueuedAtMs = nowMs;
            this.lastAttemptAtMs = nowMs;
            this.nextRetryAtMs = nowMs;
            this.lastError = initialError;
            this.decisionCandidateMarked = false;
        }

        private boolean markAttemptFailure(String error, long nowMs) {
            long currentAttempts = attempts.incrementAndGet();
            long transportFailures;
            if (isTransportError(error)) {
                transportFailures = consecutiveTransportFailures.incrementAndGet();
            } else {
                consecutiveTransportFailures.set(0L);
                transportFailures = 0L;
            }
            lastAttemptAtMs = nowMs;
            long retryBackoffMs = computePendingRetryBackoffMs(currentAttempts);
            boolean escalated = transportFailures >= PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD;
            if (escalated) {
                retryBackoffMs = Math.max(retryBackoffMs, PENDING_COMMIT_TRANSPORT_ESCALATION_COOLDOWN_MS);
            }
            nextRetryAtMs = nowMs + retryBackoffMs;
            lastError = error;
            return escalated && transportFailures == PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD;
        }

        private boolean shouldRetry(long nowMs) {
            return nowMs >= nextRetryAtMs;
        }

        private long ageMs(long nowMs) {
            return Math.max(0L, nowMs - firstQueuedAtMs);
        }

        private boolean markDecisionCandidateIfNeeded(long nowMs) {
            if (decisionCandidateMarked) {
                return false;
            }
            if (consecutiveTransportFailures.get() < PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD) {
                return false;
            }
            if (attempts.get() < PENDING_COMMIT_DECISION_CANDIDATE_ATTEMPTS) {
                return false;
            }
            decisionCandidateMarked = true;
            nextRetryAtMs = Math.max(nextRetryAtMs, nowMs + PENDING_COMMIT_DECISION_CANDIDATE_COOLDOWN_MS);
            return true;
        }
    }

    private final ExecutorService executor =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "replica-sync");
                t.setDaemon(true);
                return t;
            });
    private final ScheduledExecutorService pendingCommitRetryExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "replica-commit-retry");
                t.setDaemon(true);
                return t;
            });
    private final ConcurrentHashMap<String, PendingCommit> pendingCommits = new ConcurrentHashMap<>();
    private final AtomicLong pendingCommitEnqueuedCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitRecoveredCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitRetryAttemptCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitDroppedCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitThrottledSkipCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitLastSuccessAtMs = new AtomicLong(0L);
    private final AtomicLong pendingCommitLastFailureAtMs = new AtomicLong(0L);
    private final AtomicLong pendingCommitEscalatedCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitDecisionCandidateCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitLastDecisionCandidateAtMs = new AtomicLong(0L);
    private final AtomicLong pendingCommitDecisionCandidateCooldownAppliedCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitRecoveredFromEscalationCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitLastRecoveredFromEscalationAtMs = new AtomicLong(0L);
    private final AtomicLong pendingCommitRepairTriggeredCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitRepairSuccessCount = new AtomicLong(0L);
    private final AtomicLong pendingCommitRepairFailureCount = new AtomicLong(0L);
    private final ConcurrentHashMap<String, AtomicLong> pendingCommitErrorBreakdown = new ConcurrentHashMap<>();
    private volatile String pendingCommitLastError = "";

    public ReplicaManager() {
        this(true);
    }

    ReplicaManager(boolean startRetryWorker) {
        if (startRetryWorker) {
            pendingCommitRetryExecutor.scheduleWithFixedDelay(
                    this::retryPendingCommits,
                    PENDING_COMMIT_RETRY_INTERVAL_MS,
                    PENDING_COMMIT_RETRY_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
        }
    }

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
            executor.submit(() -> {
                boolean committed = commitOneWithRetry(tableName, lsn, addr);
                if (!committed) {
                    enqueuePendingCommit(tableName,
                            lsn,
                            addr,
                            addresses,
                            "initial commit retries exhausted");
                }
            });
        }
    }

    public Map<String, Object> getCommitRetryStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pendingCount", pendingCommits.size());
        stats.put("enqueuedCount", pendingCommitEnqueuedCount.get());
        stats.put("recoveredCount", pendingCommitRecoveredCount.get());
        stats.put("retryAttemptCount", pendingCommitRetryAttemptCount.get());
        stats.put("droppedCount", pendingCommitDroppedCount.get());
        stats.put("throttledSkipCount", pendingCommitThrottledSkipCount.get());
        stats.put("escalatedCount", pendingCommitEscalatedCount.get());
        stats.put("decisionCandidateCount", pendingCommitDecisionCandidateCount.get());
        stats.put("lastDecisionCandidateAtMs", pendingCommitLastDecisionCandidateAtMs.get());
        stats.put("decisionCandidateCooldownAppliedCount", pendingCommitDecisionCandidateCooldownAppliedCount.get());
        stats.put("decisionCandidateCooldownMs", PENDING_COMMIT_DECISION_CANDIDATE_COOLDOWN_MS);
        stats.put("recoveredFromEscalationCount", pendingCommitRecoveredFromEscalationCount.get());
        stats.put("lastRecoveredFromEscalationAtMs", pendingCommitLastRecoveredFromEscalationAtMs.get());
        stats.put("repairTriggeredCount", pendingCommitRepairTriggeredCount.get());
        stats.put("repairSuccessCount", pendingCommitRepairSuccessCount.get());
        stats.put("repairFailureCount", pendingCommitRepairFailureCount.get());
        stats.put("lastSuccessAtMs", pendingCommitLastSuccessAtMs.get());
        stats.put("lastFailureAtMs", pendingCommitLastFailureAtMs.get());
        stats.put("lastError", pendingCommitLastError);
        long now = System.currentTimeMillis();
        long stalledCount = 0L;
        long oldestPendingAgeMs = 0L;
        long activeEscalatedCount = 0L;
        long activeDecisionCandidateCount = 0L;
        long maxConsecutiveTransportFailures = 0L;
        for (PendingCommit pendingCommit : pendingCommits.values()) {
            if (pendingCommit.attempts.get() >= PENDING_COMMIT_STALLED_ATTEMPTS) {
                stalledCount++;
            }
            oldestPendingAgeMs = Math.max(oldestPendingAgeMs, pendingCommit.ageMs(now));
            long transportFailures = pendingCommit.consecutiveTransportFailures.get();
            if (transportFailures >= PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD) {
                activeEscalatedCount++;
            }
            if (pendingCommit.decisionCandidateMarked) {
                activeDecisionCandidateCount++;
            }
            maxConsecutiveTransportFailures = Math.max(maxConsecutiveTransportFailures, transportFailures);
        }
        stats.put("stalledCount", stalledCount);
        stats.put("oldestPendingAgeMs", oldestPendingAgeMs);
        stats.put("activeEscalatedCount", activeEscalatedCount);
        stats.put("activeDecisionCandidateCount", activeDecisionCandidateCount);
        stats.put("maxConsecutiveTransportFailures", maxConsecutiveTransportFailures);
        List<Map<String, Object>> decisionCandidatesPreview = pendingCommits.values().stream()
            .filter(pendingCommit -> pendingCommit.decisionCandidateMarked)
            .sorted((left, right) -> Long.compare(right.attempts.get(), left.attempts.get()))
            .limit(5)
            .map(pendingCommit -> {
                Map<String, Object> candidate = new LinkedHashMap<>();
                candidate.put("table", pendingCommit.tableName);
                candidate.put("lsn", pendingCommit.lsn);
                candidate.put("address", pendingCommit.address);
                candidate.put("attempts", pendingCommit.attempts.get());
                candidate.put("consecutiveTransportFailures", pendingCommit.consecutiveTransportFailures.get());
                candidate.put("ageMs", pendingCommit.ageMs(now));
                    candidate.put("nextRetryAtMs", pendingCommit.nextRetryAtMs);
                return candidate;
            })
            .toList();
        stats.put("decisionCandidatesPreview", decisionCandidatesPreview);
        Map<String, Long> errorBreakdown = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> entry : pendingCommitErrorBreakdown.entrySet()) {
            errorBreakdown.put(entry.getKey(), entry.getValue().get());
        }
        stats.put("errorBreakdown", errorBreakdown);
        return stats;
    }

    void retryPendingCommitsNow() {
        retryPendingCommits();
    }

    void retryPendingCommitsNowIgnoringBackoff() {
        for (PendingCommit task : pendingCommits.values()) {
            task.nextRetryAtMs = 0L;
        }
        retryPendingCommits();
    }

    /**
     * Tries to repair lagging replicas by pulling missing logs from the most up-to-date replica.
     * This runs asynchronously and is best-effort by design.
     */
    public void reconcileReplicasAsync(String tableName, long committedLsn, List<String> addresses) {
        if (addresses == null || addresses.size() < 2 || tableName == null || tableName.isBlank()) {
            return;
        }
        executor.submit(() -> reconcileReplicas(tableName, committedLsn, addresses));
    }

    void reconcileReplicas(String tableName, long committedLsn, List<String> addresses) {
        try {
            Map<String, Long> maxLsnByReplica = new HashMap<>();
            long highestObservedLsn = -1L;
            for (String address : addresses) {
                long maxLsn = getMaxLsn(tableName, address);
                maxLsnByReplica.put(address, maxLsn);
                highestObservedLsn = Math.max(highestObservedLsn, maxLsn);
            }

            if (highestObservedLsn < 0L) {
                return;
            }

            long replayUpperBound = Math.max(committedLsn, highestObservedLsn);
            for (String target : addresses) {
                long targetMaxLsn = maxLsnByReplica.getOrDefault(target, -1L);
                if (targetMaxLsn >= replayUpperBound) {
                    continue;
                }

                List<String> donorCandidates = selectDonorCandidates(maxLsnByReplica, target);
                if (donorCandidates.isEmpty()) {
                    continue;
                }

                long highestAppliedLsn = targetMaxLsn;
                long nextStartLsn = targetMaxLsn + 1L;
                for (String donor : donorCandidates) {
                    if (nextStartLsn > replayUpperBound) {
                        break;
                    }

                    List<WalEntry> backlog = pullLogs(donor, tableName, nextStartLsn);
                    if (backlog.isEmpty()) {
                        continue;
                    }
                    backlog.sort(Comparator.comparingLong(WalEntry::getLsn));
                    List<WalEntry> contiguousBacklog = selectContiguousBacklog(backlog, nextStartLsn, replayUpperBound);
                    if (contiguousBacklog.isEmpty()) {
                        continue;
                    }

                    int repaired = 0;
                    for (WalEntry entry : contiguousBacklog) {
                        if (!syncOne(entry, target)) {
                            break;
                        }
                        if (commitOneWithRetry(entry.getTableName(), entry.getLsn(), target)) {
                            repaired++;
                            highestAppliedLsn = Math.max(highestAppliedLsn, entry.getLsn());
                        }
                    }

                    if (repaired > 0) {
                        log.info("reconcileReplicas repaired table={} target={} donor={} repairedEntries={} rangeStart={}",
                                tableName, target, donor, repaired, nextStartLsn);
                    }
                    nextStartLsn = highestAppliedLsn + 1L;
                }
            }
        } catch (Exception e) {
            log.warn("reconcileReplicas failed table={} committedLsn={} cause={}",
                    tableName, committedLsn, e.getMessage());
        }
    }

    private static List<WalEntry> selectContiguousBacklog(List<WalEntry> backlog,
                                                          long startLsn,
                                                          long replayUpperBound) {
        if (backlog == null || backlog.isEmpty() || startLsn > replayUpperBound) {
            return List.of();
        }

        List<WalEntry> contiguous = new ArrayList<>();
        long expectedLsn = startLsn;
        for (WalEntry entry : backlog) {
            long lsn = entry.getLsn();
            if (lsn < expectedLsn) {
                continue;
            }
            if (lsn > replayUpperBound) {
                break;
            }
            if (lsn != expectedLsn) {
                break;
            }
            contiguous.add(entry);
            expectedLsn++;
        }
        return contiguous;
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

    boolean commitOneWithRetry(String tableName, long lsn, String address) {
        String lastError = "";
        for (int attempt = 1; attempt <= COMMIT_RETRY_ATTEMPTS; attempt++) {
            CommitAttempt commitAttempt = commitOneAttempt(tableName, lsn, address);
            if (commitAttempt.success()) {
                return true;
            }
            lastError = commitAttempt.error();
            if (attempt < COMMIT_RETRY_ATTEMPTS) {
                try {
                    Thread.sleep(COMMIT_RETRY_BACKOFF_MS * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        pendingCommitLastFailureAtMs.set(System.currentTimeMillis());
        pendingCommitLastError = lastError;
        return false;
    }

    private CommitAttempt commitOneAttempt(String tableName, long lsn, String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            return new CommitAttempt(false, COMMIT_ERR_INVALID_ADDRESS);
        }
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return new CommitAttempt(false, COMMIT_ERR_INVALID_PORT);
        }

        try (TTransport transport = new TFramedTransport(
                new TSocket(host, port, CONNECT_TIMEOUT_MS))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            Response resp = client.commitLog(tableName, lsn);
                if (resp.getCode() == StatusCode.OK) {
                log.debug("commitLog to {} for table={} lsn={}: code={} attempt=success",
                    address, tableName, lsn, resp.getCode());
                return new CommitAttempt(true, "");
                }
                log.warn("commitLog to {} for table={} lsn={} returned code={} msg={}",
                    address, tableName, lsn, resp.getCode(), resp.getMessage());
                if (resp.getCode() == StatusCode.TABLE_NOT_FOUND) {
                    return new CommitAttempt(false, COMMIT_ERR_TABLE_NOT_FOUND);
                }
                return new CommitAttempt(false,
                        COMMIT_ERR_RESPONSE + ": " + resp.getCode() + " msg=" + resp.getMessage());
        } catch (Exception e) {
            log.warn("commitLog to {} failed for table={} lsn={}: {}",
                    address, tableName, lsn, e.getMessage());
                return new CommitAttempt(false, COMMIT_ERR_TRANSPORT + ": " + e.getMessage());
        }
    }

    private void enqueuePendingCommit(String tableName,
                                     long lsn,
                                     String address,
                                     List<String> replicaAddresses,
                                     String initialError) {
        if (tableName == null || tableName.isBlank() || address == null || address.isBlank()) {
            return;
        }
        if (pendingCommits.size() >= PENDING_COMMIT_MAX_QUEUE_SIZE) {
            pendingCommitDroppedCount.incrementAndGet();
            pendingCommitLastFailureAtMs.set(System.currentTimeMillis());
            pendingCommitLastError = "pending commit queue overflow";
            return;
        }

        String key = pendingCommitKey(tableName, lsn, address);
        PendingCommit pendingCommit = new PendingCommit(
                tableName,
                lsn,
                address,
            sanitizeReplicaAddresses(replicaAddresses),
                System.currentTimeMillis(),
                initialError);
        PendingCommit existing = pendingCommits.putIfAbsent(key, pendingCommit);
        if (existing == null) {
            pendingCommitEnqueuedCount.incrementAndGet();
        } else {
            if (existing.markAttemptFailure(initialError, System.currentTimeMillis())) {
                pendingCommitEscalatedCount.incrementAndGet();
            }
        }
        pendingCommitLastFailureAtMs.set(System.currentTimeMillis());
        pendingCommitLastError = initialError;
        recordCommitError(initialError);
    }

    private void retryPendingCommits() {
        if (pendingCommits.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (Map.Entry<String, PendingCommit> entry : pendingCommits.entrySet()) {
            String key = entry.getKey();
            PendingCommit task = entry.getValue();
            if (!task.shouldRetry(now)) {
                pendingCommitThrottledSkipCount.incrementAndGet();
                continue;
            }
            CommitAttempt commitAttempt = commitOneAttempt(task.tableName, task.lsn, task.address);
            pendingCommitRetryAttemptCount.incrementAndGet();

            if (commitAttempt.success()) {
                if (task.consecutiveTransportFailures.getAndSet(0L) >= PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD) {
                    pendingCommitRecoveredFromEscalationCount.incrementAndGet();
                    pendingCommitLastRecoveredFromEscalationAtMs.set(System.currentTimeMillis());
                }
                if (pendingCommits.remove(key, task)) {
                    pendingCommitRecoveredCount.incrementAndGet();
                    pendingCommitLastSuccessAtMs.set(System.currentTimeMillis());
                }
                continue;
            }

            recordCommitError(commitAttempt.error());

            if (isTableNotFoundError(commitAttempt.error())) {
                pendingCommitRepairTriggeredCount.incrementAndGet();
                boolean repaired = repairMissingReplicaEntry(task);
                if (repaired && pendingCommits.remove(key, task)) {
                    if (task.consecutiveTransportFailures.getAndSet(0L) >= PENDING_COMMIT_TRANSPORT_ESCALATION_THRESHOLD) {
                        pendingCommitRecoveredFromEscalationCount.incrementAndGet();
                        pendingCommitLastRecoveredFromEscalationAtMs.set(System.currentTimeMillis());
                    }
                    pendingCommitRecoveredCount.incrementAndGet();
                    pendingCommitRepairSuccessCount.incrementAndGet();
                    pendingCommitLastSuccessAtMs.set(System.currentTimeMillis());
                    continue;
                }
                if (!repaired) {
                    pendingCommitRepairFailureCount.incrementAndGet();
                }
            }

            if (task.markAttemptFailure(commitAttempt.error(), now)) {
                pendingCommitEscalatedCount.incrementAndGet();
            }
            if (task.markDecisionCandidateIfNeeded(now)) {
                pendingCommitDecisionCandidateCount.incrementAndGet();
                pendingCommitLastDecisionCandidateAtMs.set(System.currentTimeMillis());
                pendingCommitDecisionCandidateCooldownAppliedCount.incrementAndGet();
            }
            pendingCommitLastFailureAtMs.set(System.currentTimeMillis());
            pendingCommitLastError = commitAttempt.error();

            if (now - task.firstQueuedAtMs > PENDING_COMMIT_MAX_AGE_MS) {
                if (pendingCommits.remove(key, task)) {
                    pendingCommitDroppedCount.incrementAndGet();
                }
            }
        }
    }

    private static String pendingCommitKey(String tableName, long lsn, String address) {
        return tableName + "#" + lsn + "@" + address;
    }

    private static boolean isTableNotFoundError(String error) {
        if (error == null) {
            return false;
        }
        return COMMIT_ERR_TABLE_NOT_FOUND.equals(error)
                || error.toLowerCase().contains(COMMIT_ERR_TABLE_NOT_FOUND);
    }

    private static List<String> sanitizeReplicaAddresses(List<String> replicaAddresses) {
        if (replicaAddresses == null || replicaAddresses.isEmpty()) {
            return List.of();
        }
        Set<String> seen = ConcurrentHashMap.newKeySet();
        List<String> cleaned = new ArrayList<>();
        for (String address : replicaAddresses) {
            if (address == null || address.isBlank()) {
                continue;
            }
            if (seen.add(address)) {
                cleaned.add(address);
            }
        }
        return cleaned;
    }

    private static long computePendingRetryBackoffMs(long attempts) {
        long normalizedAttempts = Math.max(1L, attempts);
        int exponent = (int) Math.min(6L, normalizedAttempts - 1L);
        long multiplier = 1L << exponent;
        long backoff = PENDING_COMMIT_MIN_BACKOFF_MS * multiplier;
        return Math.min(backoff, PENDING_COMMIT_MAX_BACKOFF_MS);
    }

    private void recordCommitError(String error) {
        String bucket = classifyError(error);
        pendingCommitErrorBreakdown
                .computeIfAbsent(bucket, k -> new AtomicLong(0L))
                .incrementAndGet();
    }

    private static String classifyError(String error) {
        if (error == null || error.isBlank()) {
            return "unknown";
        }
        String lowered = error.toLowerCase();
        if (lowered.contains(COMMIT_ERR_TABLE_NOT_FOUND)) {
            return COMMIT_ERR_TABLE_NOT_FOUND;
        }
        if (lowered.contains(COMMIT_ERR_TRANSPORT)) {
            return COMMIT_ERR_TRANSPORT;
        }
        if (lowered.contains(COMMIT_ERR_INVALID_ADDRESS)) {
            return COMMIT_ERR_INVALID_ADDRESS;
        }
        if (lowered.contains(COMMIT_ERR_INVALID_PORT)) {
            return COMMIT_ERR_INVALID_PORT;
        }
        if (lowered.contains(COMMIT_ERR_RESPONSE)) {
            return COMMIT_ERR_RESPONSE;
        }
        return "other";
    }

    private static boolean isTransportError(String error) {
        if (error == null) {
            return false;
        }
        return error.toLowerCase().contains(COMMIT_ERR_TRANSPORT);
    }

    private boolean repairMissingReplicaEntry(PendingCommit task) {
        if (task == null || task.replicaAddresses == null || task.replicaAddresses.isEmpty()) {
            return false;
        }

        List<String> donors = new ArrayList<>();
        for (String address : task.replicaAddresses) {
            if (!address.equals(task.address)) {
                donors.add(address);
            }
        }
        if (donors.isEmpty()) {
            return false;
        }

        for (String donor : donors) {
            List<WalEntry> backlog = pullLogs(donor, task.tableName, task.lsn);
            if (backlog.isEmpty()) {
                continue;
            }
            backlog.sort(Comparator.comparingLong(WalEntry::getLsn));
            WalEntry candidate = null;
            for (WalEntry walEntry : backlog) {
                if (walEntry.getLsn() == task.lsn) {
                    candidate = walEntry;
                    break;
                }
            }
            if (candidate == null) {
                continue;
            }

            if (!syncOne(candidate, task.address)) {
                continue;
            }

            if (commitOneWithRetry(task.tableName, task.lsn, task.address)) {
                log.info("repairMissingReplicaEntry succeeded table={} lsn={} target={} donor={}",
                        task.tableName, task.lsn, task.address, donor);
                return true;
            }
        }
        return false;
    }

    private List<String> selectDonorCandidates(Map<String, Long> maxLsnByReplica, String excludedReplica) {
        return maxLsnByReplica.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(excludedReplica))
                .filter(entry -> entry.getValue() != null && entry.getValue() >= 0L)
                .sorted((left, right) -> Long.compare(right.getValue(), left.getValue()))
                .map(Map.Entry::getKey)
                .toList();
    }

    private long getMaxLsn(String tableName, String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            return -1L;
        }
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return -1L;
        }

        try (TTransport transport = new TFramedTransport(
                new TSocket(host, port, CONNECT_TIMEOUT_MS))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            return client.getMaxLsn(tableName);
        } catch (Exception e) {
            log.warn("getMaxLsn from {} failed for table={}: {}", address, tableName, e.getMessage());
            return -1L;
        }
    }

    private List<WalEntry> pullLogs(String address, String tableName, long startLsn) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            return List.of();
        }
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return List.of();
        }

        try (TTransport transport = new TFramedTransport(
                new TSocket(host, port, CONNECT_TIMEOUT_MS))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            List<WalEntry> pulled = client.pullLog(tableName, startLsn);
            return pulled == null ? List.of() : new ArrayList<>(pulled);
        } catch (Exception e) {
            log.warn("pullLog from {} failed for table={} startLsn={}: {}",
                    address, tableName, startLsn, e.getMessage());
            return List.of();
        }
    }
}
