package com.upsolver.datasources.jdbc.querybuilders;

public class QueryDialectProvider {

    public static QueryDialect forConnection(String connectionString) {
        String connStr = connectionString.toLowerCase();
        if (connStr.startsWith("jdbc:sqlserver")) {
            return new SqlServerQueryDialect();
        } else if (connStr.startsWith("jdbc:oracle")) {
            return new OracleQueryDialect();
        } else if (connStr.startsWith("jdbc:redshift")) {
            return new RedshiftQueryDialect();
        } else if (connStr.startsWith("jdbc:postgresql")) {
            return new PostgreSqlQueryDialect();
        } else if (connStr.startsWith("jdbc:snowflake")) {
            return new SnowflakeQueryDialect();
        }
        return new DefaultQueryDialect();

    }
}
