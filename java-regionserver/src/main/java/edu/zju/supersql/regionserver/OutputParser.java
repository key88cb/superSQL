package edu.zju.supersql.regionserver;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.Row;
import edu.zju.supersql.rpc.StatusCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parses MiniSQL stdout text into Thrift QueryResult.
 */
public class OutputParser {

    public static QueryResult parse(String raw) {
        Response status = new Response(StatusCode.OK);
        QueryResult result = new QueryResult(status);
        
        if (raw.contains(">>> SUCCESS")) {
            status.setMessage("Success");
            return result;
        }
        
        if (raw.contains(">>> Error:")) {
            status.setCode(StatusCode.ERROR);
            int start = raw.indexOf(">>> Error:") + ">>> Error:".length();
            int end = raw.indexOf("\n", start);
            if (end == -1) end = raw.length();
            status.setMessage(raw.substring(start, end).trim());
            return result;
        }

        // Parse Table Output
        String[] lines = raw.split("\n");
        List<Row> rows = new ArrayList<>();
        
        boolean headerFound = false;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith(">>>") || line.contains("Welcome") || line.contains("Bye bye")) continue;
            
            if (line.contains("---") || line.contains("---+---")) {
                continue;
            }

            String[] parts = line.split("\\|");
            List<String> values = Arrays.stream(parts).map(String::trim).collect(Collectors.toList());

            if (!headerFound) {
                result.setColumnNames(values);
                headerFound = true;
            } else {
                rows.add(new Row(values));
            }
        }
        
        result.setRows(rows);
        return result;
    }
}
