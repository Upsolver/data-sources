package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;

public class QueryDialectProvider {

    public static QueryDialect forConnection(Connection connection) {
        var driverName = connection.getClass().getName();
        if (driverName.contains("sqlserver")) {
            return new SqlServerQueryDialect();
        }
        return new DefaultQueryDialect();

    }
}
