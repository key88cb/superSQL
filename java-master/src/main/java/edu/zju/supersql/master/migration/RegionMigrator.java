package edu.zju.supersql.master.migration;

import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.master.rpc.RegionAdminExecutor;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Encapsulates the region migration orchestration for rebalance.
 */
public final class RegionMigrator {

    public record MigrationContext(String attemptId,
                                   String sourceReplicaId,
                                   String targetReplicaId) {
    }

    public record MigrationSnapshot(long attemptCount,
                                    long successCount,
                                    long failureCount,
                                    long rebalanceAttemptCount,
                                    long rebalanceSuccessCount,
                                    long rebalanceFailureCount,
                                    long recoveryAttemptCount,
                                    long recoverySuccessCount,
                                    long recoveryFailureCount,
                                    long lastAttemptAtMs,
                                    long lastSuccessAtMs,
                                    long lastFailureAtMs,
                                    String lastError,
                                    String lastRebalanceError,
                                    String lastRecoveryError) {
    }

    @FunctionalInterface
    public interface MigrationContextReader {
        MigrationContext read(String tableName);
    }

    @FunctionalInterface
    public interface RecoveryResolver {
        RegionServerInfo resolve(TableLocation location, String replicaId);
    }

    @FunctionalInterface
    public interface MigrationContextWriter {
        void write(String tableName, String migrationAttemptId, String sourceReplicaId, String targetReplicaId);
    }

    private static final Logger log = LoggerFactory.getLogger(RegionMigrator.class);
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_PREPARING = "PREPARING";
    private static final String STATUS_MOVING = "MOVING";
    private static final String STATUS_FINALIZING = "FINALIZING";
    private static final String STATUS_ROLLBACK = "ROLLBACK";

    private final MetaManager metaManager;
    private final AssignmentManager assignmentManager;
    private final RegionAdminExecutor regionAdminExecutor;
    private final LongSupplier clockMs;
    private final MigrationContextWriter migrationContextWriter;
    private final Consumer<String> migrationContextClearer;
    private final Consumer<String> statusUpdatedAtToucher;
    private final AtomicLong attemptCount = new AtomicLong(0L);
    private final AtomicLong successCount = new AtomicLong(0L);
    private final AtomicLong failureCount = new AtomicLong(0L);
    private final AtomicLong rebalanceAttemptCount = new AtomicLong(0L);
    private final AtomicLong rebalanceSuccessCount = new AtomicLong(0L);
    private final AtomicLong rebalanceFailureCount = new AtomicLong(0L);
    private final AtomicLong recoveryAttemptCount = new AtomicLong(0L);
    private final AtomicLong recoverySuccessCount = new AtomicLong(0L);
    private final AtomicLong recoveryFailureCount = new AtomicLong(0L);
    private final AtomicLong lastAttemptAtMs = new AtomicLong(-1L);
    private final AtomicLong lastSuccessAtMs = new AtomicLong(-1L);
    private final AtomicLong lastFailureAtMs = new AtomicLong(-1L);
    private volatile String lastError;
    private volatile String lastRebalanceError;
    private volatile String lastRecoveryError;

    public RegionMigrator(MetaManager metaManager,
                          AssignmentManager assignmentManager,
                          RegionAdminExecutor regionAdminExecutor,
                          LongSupplier clockMs,
                          MigrationContextWriter migrationContextWriter,
                          Consumer<String> migrationContextClearer,
                          Consumer<String> statusUpdatedAtToucher) {
        this.metaManager = metaManager;
        this.assignmentManager = assignmentManager;
        this.regionAdminExecutor = regionAdminExecutor;
        this.clockMs = clockMs;
        this.migrationContextWriter = migrationContextWriter;
        this.migrationContextClearer = migrationContextClearer;
        this.statusUpdatedAtToucher = statusUpdatedAtToucher;
    }

    public MigrationSnapshot snapshot() {
        return new MigrationSnapshot(
                attemptCount.get(),
                successCount.get(),
                failureCount.get(),
            rebalanceAttemptCount.get(),
            rebalanceSuccessCount.get(),
            rebalanceFailureCount.get(),
            recoveryAttemptCount.get(),
            recoverySuccessCount.get(),
            recoveryFailureCount.get(),
                lastAttemptAtMs.get(),
                lastSuccessAtMs.get(),
                lastFailureAtMs.get(),
            lastError,
            lastRebalanceError,
            lastRecoveryError);
    }

    public Response rebalanceReplica(TableLocation location,
                                     RegionServerInfo source,
                                     RegionServerInfo target) throws Exception {
        recordRebalanceAttempt();
        RegionServerInfo primary = location.getPrimaryRS();
        String tableName = location.getTableName();
        String migrationAttemptId = buildMigrationAttemptId(tableName, source, target);
        TableLocation originalLocation = new TableLocation(location);
        List<RegionServerInfo> originalReplicas = new ArrayList<>(location.getReplicas());
        List<String> beforeReplicas = location.getReplicas().stream().map(RegionServerInfo::getId).toList();
        log.info("triggerRebalance executing table={} primary={} source={} target={} replicasBefore={}",
                tableName, primary.getId(), source.getId(), target.getId(), beforeReplicas);

        if (!markTableStatus(tableName,
                primary,
                location.getReplicas(),
                STATUS_PREPARING,
                migrationAttemptId,
                source.getId(),
                target.getId())) {
            Response error = new Response(StatusCode.ERROR);
            error.setMessage("Failed to mark table as PREPARING before pause: " + tableName);
            return recordRebalanceFailureResponse(error,
                    "rebalance preparing mark failed table=" + tableName);
        }

        Response pause = regionAdminExecutor.pauseTableWrite(primary, tableName);
        if (pause.getCode() != StatusCode.OK) {
            log.warn("triggerRebalance pause failed table={} primary={} code={}",
                    tableName, primary.getId(), pause.getCode());
            rollbackRebalanceMetadata(tableName,
                    originalLocation,
                    originalReplicas,
                    migrationAttemptId,
                    source.getId(),
                    target.getId());
                return recordRebalanceFailureResponse(pause,
                "rebalance pause failed table=" + tableName + " code=" + pause.getCode());
        }

        try {
            if (!markTableStatus(tableName,
                    primary,
                    location.getReplicas(),
                    STATUS_MOVING,
                    migrationAttemptId,
                    source.getId(),
                    target.getId())) {
                rollbackRebalanceMetadata(tableName,
                        originalLocation,
                        originalReplicas,
                        migrationAttemptId,
                        source.getId(),
                        target.getId());
                Response error = new Response(StatusCode.ERROR);
                error.setMessage("Failed to mark table as MOVING before transfer: " + tableName);
                return recordRebalanceFailureResponse(error,
                    "rebalance moving mark failed table=" + tableName);
            }

            Response transfer = regionAdminExecutor.transferTable(source, tableName, target);
            if (transfer.getCode() != StatusCode.OK) {
                rollbackRebalanceMetadata(tableName,
                        originalLocation,
                        originalReplicas,
                        migrationAttemptId,
                        source.getId(),
                        target.getId());
                cleanupTargetReplicaBestEffort(target, tableName, "transfer_failed");
                return recordRebalanceFailureResponse(transfer,
                    "rebalance transfer failed table=" + tableName + " code=" + transfer.getCode());
            }

            List<RegionServerInfo> updatedReplicas = new ArrayList<>();
            for (RegionServerInfo replica : location.getReplicas()) {
                updatedReplicas.add(replica.getId().equals(source.getId()) ? target : replica);
            }

            if (!markTableStatus(tableName,
                    primary,
                    updatedReplicas,
                    STATUS_FINALIZING,
                    migrationAttemptId,
                    source.getId(),
                    target.getId())) {
                rollbackRebalanceMetadata(tableName,
                        originalLocation,
                        originalReplicas,
                        migrationAttemptId,
                        source.getId(),
                        target.getId());
                cleanupTargetReplicaBestEffort(target, tableName, "finalizing_mark_failed");
                Response error = new Response(StatusCode.ERROR);
                error.setMessage("Failed to mark table as FINALIZING before metadata finalize: " + tableName);
                return recordRebalanceFailureResponse(error,
                    "rebalance finalizing mark failed table=" + tableName);
            }

            try {
                assignmentManager.saveAssignment(tableName, updatedReplicas);
                touchStatusUpdatedAtBestEffort(tableName);
            } catch (Exception metadataError) {
                log.error("triggerRebalance metadata persist failed table={}, rolling back to original replicas={} cause={}",
                        tableName,
                        originalReplicas.stream().map(RegionServerInfo::getId).toList(),
                        metadataError.getMessage());
                rollbackRebalanceMetadata(tableName,
                        originalLocation,
                        originalReplicas,
                        migrationAttemptId,
                        source.getId(),
                        target.getId());
                cleanupTargetReplicaBestEffort(target, tableName, "metadata_persist_failed");
                throw metadataError;
            }
            log.info("triggerRebalance metadata updated table={} replicasAfter={}",
                    tableName, updatedReplicas.stream().map(RegionServerInfo::getId).toList());

            Response delete = regionAdminExecutor.deleteLocalTable(source, tableName);
            if (delete.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance deleteLocalTable failed table={} source={} code={}",
                        tableName, source.getId(), delete.getCode());
                rollbackRebalanceMetadata(tableName,
                        originalLocation,
                        originalReplicas,
                        migrationAttemptId,
                        source.getId(),
                        target.getId());
                cleanupTargetReplicaBestEffort(target, tableName, "source_delete_failed");
                return recordRebalanceFailureResponse(delete,
                    "rebalance source cleanup failed table=" + tableName + " code=" + delete.getCode());
            }

            TableLocation updatedLocation = new TableLocation(tableName, primary, updatedReplicas);
            updatedLocation.setTableStatus(STATUS_ACTIVE);
            updatedLocation.setVersion(clockMs.getAsLong());
            try {
                metaManager.saveTableLocation(updatedLocation);
                assignmentManager.saveAssignment(tableName, updatedReplicas);
                touchStatusUpdatedAtBestEffort(tableName);
                clearMigrationContextBestEffort(tableName);
            } catch (Exception metadataError) {
                log.error("triggerRebalance final ACTIVE persist failed table={} cause={}",
                        tableName, metadataError.getMessage());
                throw metadataError;
            }

            invalidateClientCacheBestEffort(primary, tableName);
            invalidateClientCacheBestEffort(target, tableName);
            invalidateClientCacheBestEffort(source, tableName);

            Response ok = new Response(StatusCode.OK);
            ok.setMessage("Rebalanced table " + tableName + " from " + source.getId() + " to " + target.getId());
            log.info("triggerRebalance success table={} source={} target={}", tableName, source.getId(), target.getId());
            return recordRebalanceSuccessResponse(ok);
        } catch (Exception e) {
            recordRebalanceFailure("rebalance exception table=" + tableName + " cause=" + e.getMessage());
            throw e;
        } finally {
            resumeWriteBestEffort(primary, tableName);
        }
    }

    private boolean markTableStatus(String tableName,
                                    RegionServerInfo primary,
                                    List<RegionServerInfo> replicas,
                                    String status,
                                    String migrationAttemptId,
                                    String sourceReplicaId,
                                    String targetReplicaId) {
        try {
            TableLocation intermediate = new TableLocation(tableName, primary, replicas);
            intermediate.setTableStatus(status);
            intermediate.setVersion(clockMs.getAsLong());
            metaManager.saveTableLocation(intermediate);
            touchStatusUpdatedAtBestEffort(tableName);
            migrationContextWriter.write(tableName, migrationAttemptId, sourceReplicaId, targetReplicaId);
            log.info("triggerRebalance marked table {} table={} replicas={}",
                    status, tableName, replicas.stream().map(RegionServerInfo::getId).toList());
            return true;
        } catch (Exception e) {
            log.error("triggerRebalance failed to mark {} table={} cause={}",
                    status, tableName, e.getMessage(), e);
            return false;
        }
    }

    private void rollbackRebalanceMetadata(String tableName,
                                           TableLocation originalLocation,
                                           List<RegionServerInfo> originalReplicas,
                                           String migrationAttemptId,
                                           String sourceReplicaId,
                                           String targetReplicaId) {
        if (originalLocation != null && originalLocation.isSetPrimaryRS()) {
            markTableStatus(tableName,
                    originalLocation.getPrimaryRS(),
                    originalReplicas,
                    STATUS_ROLLBACK,
                    migrationAttemptId,
                    sourceReplicaId,
                    targetReplicaId);
        }
        try {
            metaManager.saveTableLocation(originalLocation);
            assignmentManager.saveAssignment(tableName, originalReplicas);
            touchStatusUpdatedAtBestEffort(tableName);
            clearMigrationContextBestEffort(tableName);
            log.info("triggerRebalance metadata rollback completed table={} replicasRestored={}",
                    tableName,
                    originalReplicas.stream().map(RegionServerInfo::getId).toList());
        } catch (Exception rollbackError) {
            log.error("triggerRebalance metadata rollback failed table={} replicas={} cause={}",
                    tableName,
                    originalReplicas.stream().map(RegionServerInfo::getId).toList(),
                    rollbackError.getMessage(),
                    rollbackError);
        }
    }

    private void touchStatusUpdatedAtBestEffort(String tableName) {
        try {
            statusUpdatedAtToucher.accept(tableName);
        } catch (Exception e) {
            log.warn("triggerRebalance touch statusUpdatedAt failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }

    private void clearMigrationContextBestEffort(String tableName) {
        try {
            migrationContextClearer.accept(tableName);
        } catch (Exception e) {
            log.warn("triggerRebalance clear migration context failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }

    private void invalidateClientCacheBestEffort(RegionServerInfo regionServer, String tableName) {
        try {
            Response response = regionAdminExecutor.invalidateClientCache(regionServer, tableName);
            if (response.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance cache invalidation failed table={} rs={} code={} msg={}",
                        tableName, regionServer.getId(), response.getCode(), response.getMessage());
            }
        } catch (Exception e) {
            log.warn("triggerRebalance cache invalidation exception table={} rs={} cause={}",
                    tableName, regionServer.getId(), e.getMessage());
        }
    }

    private boolean cleanupTargetReplicaBestEffort(RegionServerInfo target, String tableName, String reason) {
        return cleanupReplicaBestEffort(target, tableName, reason, "target");
    }

    private boolean cleanupReplicaBestEffort(RegionServerInfo replica,
                                             String tableName,
                                             String reason,
                                             String role) {
        if (replica == null || !replica.isSetId()) {
            return true;
        }
        try {
            Response response = regionAdminExecutor.deleteLocalTable(replica, tableName);
            if (response.getCode() == StatusCode.OK || response.getCode() == StatusCode.TABLE_NOT_FOUND) {
                log.info("triggerRebalance cleanup {} replica table={} replica={} reason={} code={}",
                        role, tableName, replica.getId(), reason, response.getCode());
                return true;
            } else {
                log.warn("triggerRebalance cleanup {} replica failed table={} replica={} reason={} code={} msg={}",
                        role, tableName, replica.getId(), reason, response.getCode(), response.getMessage());
                return false;
            }
        } catch (Exception e) {
            log.warn("triggerRebalance cleanup {} replica exception table={} replica={} reason={} cause={}",
                    role, tableName, replica.getId(), reason, e.getMessage());
            return false;
        }
    }

    private void resumeWriteBestEffort(RegionServerInfo primary, String tableName) {
        try {
            Response response = regionAdminExecutor.resumeTableWrite(primary, tableName);
            if (response.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance resume writes failed table={} primary={} code={} msg={}",
                        tableName, primary.getId(), response.getCode(), response.getMessage());
            } else {
                log.info("triggerRebalance resume writes table={} primary={}", tableName, primary.getId());
            }
        } catch (Exception e) {
            log.warn("triggerRebalance resume writes exception table={} primary={} cause={}",
                    tableName, primary.getId(), e.getMessage());
        }
    }

    private String buildMigrationAttemptId(String tableName,
                                           RegionServerInfo source,
                                           RegionServerInfo target) {
        return tableName + "-" + source.getId() + "-" + target.getId() + "-" + clockMs.getAsLong();
    }

    public TableLocation recoverStuckMigrationBestEffort(TableLocation location,
                                                          long migrationStuckTimeoutMs,
                                                          MigrationContextReader migrationContextReader,
                                                          RecoveryResolver recoveryResolver) {
        if (location == null || !location.isSetTableName()) {
            return location;
        }
        String currentStatus = location.isSetTableStatus() ? location.getTableStatus() : null;
        if (!isTransientMigrationStatus(currentStatus)) {
            return location;
        }
        if (migrationStuckTimeoutMs <= 0L) {
            return location;
        }
        long version = location.isSetVersion() ? location.getVersion() : -1L;
        long now = clockMs.getAsLong();
        if (version <= 0L || now - version < migrationStuckTimeoutMs) {
            return location;
        }

        MigrationContext migrationContext = migrationContextReader.read(location.getTableName());
        if (migrationContext == null
                || migrationContext.attemptId() == null
                || migrationContext.attemptId().isBlank()) {
            return location;
        }
        recordRecoveryAttempt();

        String attemptId = migrationContext.attemptId();
        String sourceReplicaId = migrationContext.sourceReplicaId();
        String targetReplicaId = migrationContext.targetReplicaId();
        boolean recoveryBlockedByCompensationFailure = false;

        if (STATUS_FINALIZING.equalsIgnoreCase(currentStatus)
                && sourceReplicaId != null
                && !sourceReplicaId.isBlank()) {
            RegionServerInfo sourceReplica = recoveryResolver.resolve(location, sourceReplicaId);
            if (sourceReplica == null) {
                recoveryBlockedByCompensationFailure = true;
                log.warn("recoverStuckMigration missing source replica for compensation table={} sourceReplicaId={} status={}",
                        location.getTableName(),
                        sourceReplicaId,
                        currentStatus);
            } else {
                boolean sourceCleaned = cleanupReplicaBestEffort(sourceReplica,
                        location.getTableName(),
                        "recover_stuck_finalizing",
                        "source");
                if (!sourceCleaned) {
                    recoveryBlockedByCompensationFailure = true;
                }
            }
        }

        if ((STATUS_PREPARING.equalsIgnoreCase(currentStatus)
                || STATUS_MOVING.equalsIgnoreCase(currentStatus)
                || STATUS_ROLLBACK.equalsIgnoreCase(currentStatus))
                && targetReplicaId != null
                && !targetReplicaId.isBlank()) {
            RegionServerInfo targetReplica = recoveryResolver.resolve(location, targetReplicaId);
            if (targetReplica == null) {
                recoveryBlockedByCompensationFailure = true;
                log.warn("recoverStuckMigration missing target replica for compensation table={} targetReplicaId={} status={}",
                        location.getTableName(),
                        targetReplicaId,
                        currentStatus);
            } else {
                boolean targetCleaned = cleanupReplicaBestEffort(targetReplica,
                        location.getTableName(),
                        "recover_stuck_" + currentStatus.toLowerCase(),
                        "target");
                if (!targetCleaned) {
                    recoveryBlockedByCompensationFailure = true;
                }
            }
        }

        if (recoveryBlockedByCompensationFailure) {
            log.warn("recoverStuckMigration blocked by compensation failure table={} status={} attemptId={} source={} target={}",
                    location.getTableName(),
                    currentStatus,
                    attemptId,
                    sourceReplicaId,
                    targetReplicaId);
            return recordRecoveryFailureLocation(location,
                "stuck recovery compensation blocked table=" + location.getTableName() + " status=" + currentStatus);
        }

        TableLocation recovered = new TableLocation(location);
        recovered.setTableStatus(STATUS_ACTIVE);
        recovered.setVersion(now);
        try {
            metaManager.saveTableLocation(recovered);
            assignmentManager.saveAssignment(recovered.getTableName(), recovered.getReplicas());
            touchStatusUpdatedAtBestEffort(recovered.getTableName());
            clearMigrationContextBestEffort(recovered.getTableName());
            log.warn("recoverStuckMigration finalized timed-out migration table={} status={} stuckFor={}ms attemptId={} source={} target={}",
                    recovered.getTableName(),
                    currentStatus,
                    now - version,
                    attemptId,
                    sourceReplicaId,
                    targetReplicaId);
                return recordRecoverySuccessLocation(recovered);
        } catch (Exception e) {
            log.error("recoverStuckMigration failed table={} status={} cause={}",
                    location.getTableName(),
                    currentStatus,
                    e.getMessage());
                return recordRecoveryFailureLocation(location,
                    "stuck recovery persist failed table=" + location.getTableName() + " status=" + currentStatus + " cause=" + e.getMessage());
        }
    }

    private boolean isTransientMigrationStatus(String status) {
        if (status == null || status.isBlank()) {
            return false;
        }
        String normalized = status.toUpperCase();
        return STATUS_PREPARING.equals(normalized)
                || STATUS_MOVING.equals(normalized)
                || STATUS_FINALIZING.equals(normalized)
                || STATUS_ROLLBACK.equals(normalized);
    }

    private void recordAttempt() {
        attemptCount.incrementAndGet();
        lastAttemptAtMs.set(clockMs.getAsLong());
    }

    private void recordSuccess() {
        successCount.incrementAndGet();
        lastSuccessAtMs.set(clockMs.getAsLong());
    }

    private void recordFailure(String error) {
        failureCount.incrementAndGet();
        lastFailureAtMs.set(clockMs.getAsLong());
        lastError = error;
    }

    private void recordRebalanceAttempt() {
        recordAttempt();
        rebalanceAttemptCount.incrementAndGet();
    }

    private void recordRecoveryAttempt() {
        recordAttempt();
        recoveryAttemptCount.incrementAndGet();
    }

    private void recordRebalanceSuccess() {
        recordSuccess();
        rebalanceSuccessCount.incrementAndGet();
    }

    private void recordRecoverySuccess() {
        recordSuccess();
        recoverySuccessCount.incrementAndGet();
    }

    private void recordRebalanceFailure(String error) {
        recordFailure(error);
        rebalanceFailureCount.incrementAndGet();
        lastRebalanceError = error;
    }

    private void recordRecoveryFailure(String error) {
        recordFailure(error);
        recoveryFailureCount.incrementAndGet();
        lastRecoveryError = error;
    }

    private Response recordRebalanceSuccessResponse(Response response) {
        recordRebalanceSuccess();
        return response;
    }

    private Response recordRebalanceFailureResponse(Response response, String error) {
        recordRebalanceFailure(error);
        return response;
    }

    private TableLocation recordRecoverySuccessLocation(TableLocation location) {
        recordRecoverySuccess();
        return location;
    }

    private TableLocation recordRecoveryFailureLocation(TableLocation location, String error) {
        recordRecoveryFailure(error);
        return location;
    }
}
