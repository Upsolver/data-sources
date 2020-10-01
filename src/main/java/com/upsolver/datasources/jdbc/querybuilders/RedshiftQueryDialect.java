package com.upsolver.datasources.jdbc.querybuilders;

public class RedshiftQueryDialect extends PostgreSqlLikeQueryDialect {
    public RedshiftQueryDialect() {
        // Example default value: "identity"(707455, 0, '1,1'::text)
        super("jdbc:redshift:", "\"identity\"(");
    }
}
