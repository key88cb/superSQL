package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class ReplicaSyncServiceImplTest {

    @BeforeEach
    void setUp() {
        ReplicaSyncServiceImpl.resetForTests();
    }

    @Test
    void syncAndPullAndCommitShouldWork() throws Exception {
        ReplicaSyncServiceImpl service = new ReplicaSyncServiceImpl();

        WalEntry entry = new WalEntry(10L, 200L, "t_user", WalOpType.INSERT, System.currentTimeMillis());
        Response syncResponse = service.syncLog(entry);

        Assertions.assertEquals(StatusCode.OK, syncResponse.getCode());
        Assertions.assertTrue(syncResponse.getMessage().contains("ACK"));

        long maxLsn = service.getMaxLsn("t_user");
        Assertions.assertEquals(10L, maxLsn);

        List<WalEntry> entries = service.pullLog("t_user", 1L);
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(10L, entries.get(0).getLsn());

        Response commitResponse = service.commitLog("t_user", 10L);
        Assertions.assertEquals(StatusCode.OK, commitResponse.getCode());
        Assertions.assertTrue(commitResponse.getMessage().contains("COMMITTED"));
    }

    @Test
    void commitMissingLsnShouldReturnTableNotFound() throws Exception {
        ReplicaSyncServiceImpl service = new ReplicaSyncServiceImpl();
        Response response = service.commitLog("missing_table", 1L);
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, response.getCode());
    }
}
