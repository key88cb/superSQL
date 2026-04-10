package edu.zju.supersql.regionserver;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class OutputParserTest {

    @Test
    void shouldParseSuccessResponse() {
        QueryResult result = OutputParser.parse(">>> SUCCESS\n>>> ");

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals("Success", result.getStatus().getMessage());
        Assertions.assertEquals(0, result.getRowsSize());
    }

    @Test
    void shouldParseErrorResponse() {
        QueryResult result = OutputParser.parse(">>> Error: syntax error near 'from'\n>>> ");

        Assertions.assertEquals(StatusCode.ERROR, result.getStatus().getCode());
        Assertions.assertEquals("syntax error near 'from'", result.getStatus().getMessage());
    }

    @Test
    void shouldParseTableOutput() {
        String raw = """
                >>> Welcome to MiniSQL
                  id | name
                -------------
                  1  | Alice
                  2  | Bob
                >>> 
                """;

        QueryResult result = OutputParser.parse(raw);

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(List.of("id", "name"), result.getColumnNames());
        Assertions.assertEquals(2, result.getRowsSize());
        Assertions.assertEquals(List.of("1", "Alice"), result.getRows().get(0).getValues());
        Assertions.assertEquals(List.of("2", "Bob"), result.getRows().get(1).getValues());
    }
}
