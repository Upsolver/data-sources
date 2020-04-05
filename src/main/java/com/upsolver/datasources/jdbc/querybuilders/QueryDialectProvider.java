package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;

public class QueryDialectProvider {

    public static QueryDialect forConnection(String connectionString) {
        if (connectionString.toLowerCase().contains("sqlserver")) {
            return new SqlServerQueryDialect();
        }
        return new DefaultQueryDialect();

    }
}
