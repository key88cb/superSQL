package edu.zju.supersql.master.rpc;

import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;

/**
 * Executes RegionAdmin operations against RegionServers.
 */
public interface RegionAdminExecutor {

    Response pauseTableWrite(RegionServerInfo regionServer, String tableName) throws Exception;

    Response resumeTableWrite(RegionServerInfo regionServer, String tableName) throws Exception;

    Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) throws Exception;

    Response deleteLocalTable(RegionServerInfo regionServer, String tableName) throws Exception;

    Response invalidateClientCache(RegionServerInfo regionServer, String tableName) throws Exception;
}
