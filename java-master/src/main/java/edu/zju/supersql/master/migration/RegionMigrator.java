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
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Encapsulates the region migration orchestration for rebalance.
 */
public final class RegionMigrator {

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

    public Response rebalanceReplica(TableLocation location,
                                     RegionServerInfo source,
                                     RegionServerInfo target) throws Exception {
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
            return error;
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
            return pause;
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
                return error;
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
                return transfer;
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
                return error;
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
                return delete;
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
            return ok;
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
}
