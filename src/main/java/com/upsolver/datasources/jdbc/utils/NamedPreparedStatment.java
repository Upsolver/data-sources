package com.upsolver.datasources.jdbc.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamedPreparedStatment implements AutoCloseable {
    private PreparedStatement prepStmt;
    private Map<String, List<Integer>> fields = new HashMap<>();

    public NamedPreparedStatment(Connection conn, String sql) throws SQLException {
        int pos;
        var index = 1;
        while((pos = sql.indexOf(":")) != -1) {
            int end = sql.substring(pos).indexOf(" ");
            if (end == -1)
                end = sql.length();
            else
                end += pos;
            var name = sql.substring(pos+1,end);
            fields.putIfAbsent(name, new ArrayList<>());
            var list = fields.get(name);
            list.add(index);
            index += 1;
            sql = sql.substring(0, pos) + "?" + sql.substring(end);
        }
        prepStmt = conn.prepareStatement(sql);
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


    private List<Integer> getIndices(String name) {
        return fields.getOrDefault(name, new ArrayList<>());
    }

}
