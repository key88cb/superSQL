package edu.zju.supersql.master.rpc;

import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;

/**
 * Executes table-level DDL on a specific RegionServer.
 */
public interface RegionDdlExecutor {

    Response execute(RegionServerInfo regionServer, String tableName, String ddl) throws Exception;
}
