package com.upsolver.datasources.jdbc.querybuilders;

import java.util.Collections;

public class QueryDialectProvider {

    public static QueryDialect forConnection(String connectionString, boolean keepTypes) {
        String connStr = connectionString.toLowerCase();
        if (connStr.startsWith("jdbc:sqlserver")) {
            return new SqlServerQueryDialect(keepTypes);
        } else if (connStr.startsWith("jdbc:oracle")) {
            return new OracleQueryDialect(keepTypes);
        } else if (connStr.startsWith("jdbc:redshift")) {
            return new RedshiftQueryDialect(keepTypes);
        } else if (connStr.startsWith("jdbc:postgresql")) {
            return new PostgreSqlQueryDialect(keepTypes);
        } else if (connStr.startsWith("jdbc:snowflake")) {
            return new SnowflakeQueryDialect(keepTypes);
        }
        return new DefaultQueryDialect(keepTypes, Collections.emptyMap());
    }
}
