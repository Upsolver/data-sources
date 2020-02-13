package com.upsolver.datasources.jdbc.utils;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

public class NamedPreparedStatment implements AutoCloseable {
    static final private Pattern namedVariable = Pattern.compile(":(\\w+)");

    private PreparedStatement prepStmt;
    private Map<String, List<Integer>> fields = new HashMap<>();

    public NamedPreparedStatment(Connection conn, String sql) throws SQLException {
        var finalSql = namedVariable.matcher(sql).replaceAll(new Function<>() {
            private int index = 1;

            @Override
            public String apply(MatchResult matchResult) {
                var name = matchResult.group(1);
                fields.putIfAbsent(name, new ArrayList<>());
                var list = fields.get(name);
                list.add(index);
                index += 1;
                return "?";
            }
        });

        prepStmt = conn.prepareStatement(finalSql);
    }


    public ResultSet executeQuery() throws SQLException {
        return prepStmt.executeQuery();
    }

    public void close() throws SQLException {
        prepStmt.close();
    }

    public void setInt(String name, int value) throws SQLException {
        for (int i : getIndices(name)) {
            prepStmt.setInt(i, value);
        }
    }

    public void setString(String name, String value) throws SQLException {
        for (int i : getIndices(name)) {
            prepStmt.setString(i, value);
        }
    }

    public void setLong(String name, long value) throws SQLException {
        for (int i : getIndices(name)) {
            prepStmt.setLong(i, value);
        }
    }

    public void setTime(String name, Instant time) throws SQLException {
        var timestamp = Timestamp.from(time);
        for (int i: getIndices(name)) {
            prepStmt.setTimestamp(i, timestamp);
        }
    }


    private List<Integer> getIndices(String name) {
        return fields.getOrDefault(name, new ArrayList<>());
    }

}