package com.upsolver.datasources.jdbc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;


public class NamedPreparedStatment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(NamedPreparedStatment.class);

    static final private Pattern namedVariable = Pattern.compile(":(\\w+)");

    private final PreparedStatement prepStmt;
    private final Map<String, List<Integer>> fields = new HashMap<>();

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
        logger.debug("Executing query: " + prepStmt.toString());
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
        for (int i : getIndices(name)) {
            prepStmt.setTimestamp(i, timestamp);
        }
    }


    private List<Integer> getIndices(String name) {
        return fields.getOrDefault(name, new ArrayList<>());
    }

}
