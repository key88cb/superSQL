package edu.zju.supersql.master.rpc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MasterServiceImplTest {

    @Test
    void parseCreateTableNameShouldWork() {
        String ddl = "CREATE TABLE users(id int, name char(16), primary key(id));";
        String table = MasterServiceImpl.parseTableNameFromCreateDdl(ddl);
        Assertions.assertEquals("users", table);
    }

    @Test
    void parseCreateTableNameShouldReturnNullForInvalidDdl() {
        String ddl = "DROP TABLE users;";
        String table = MasterServiceImpl.parseTableNameFromCreateDdl(ddl);
        Assertions.assertNull(table);
    }
}
