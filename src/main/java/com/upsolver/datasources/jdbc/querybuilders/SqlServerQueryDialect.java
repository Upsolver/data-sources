package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.SQLException;

public class SqlServerQueryDialect extends DefaultQueryDialect {
    @Override
    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("SELECT DATEDIFF(second, GETDATE(), GETUTCDATE());").executeQuery();
        rs.next();
        return rs.getLong(1);
    }

    @Override
    protected String topLimit(long amount) {
        return amount >= 0 ? "top " + amount : "";
    }

    @Override
    protected String endLimit(long amount) {
        return "";
    }
}
