package com.upsolver.datasources.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetReadLimiter {
    private final ResultSet underlying;
    private final int limit;

    private int count = 0;

    public ResultSetReadLimiter(ResultSet underlying, int limit) {
        this.underlying = underlying;
        this.limit = limit;
    }

    public boolean next() throws SQLException {
        if (count >= limit) return false;
        count++;
        return underlying.next();
    }

}
