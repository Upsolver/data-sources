package com.upsolver.datasources.jdbc.querybuilders;

public class QueryDialectProvider {

    public static QueryDialect forConnection(String connectionString) {
        if (connectionString.toLowerCase().startsWith("jdbc:sqlserver")) {
            return new SqlServerQueryDialect();
        } else if (connectionString.toLowerCase().startsWith("jdbc:oracle")) {
            return new OracleQueryDialect();
        }
        return new DefaultQueryDialect();

    }
}
