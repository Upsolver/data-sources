package com.upsolver.datasources.jdbc.querybuilders;

public class PostgreSqlQueryDialect extends PostgreSqlLikeQueryDialect {
    public PostgreSqlQueryDialect() {
        // Example default value: nextval('test_identity_id_seq'::regclass)
        super("jdbc:postgresql:", "nextval(");
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }
}
