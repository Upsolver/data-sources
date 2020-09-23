package com.upsolver.datasources.jdbc.querybuilders;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryDialectProviderTest {
    @Test
    public void testDefault() {
        test("jdbc:unknown", DefaultQueryDialect.class);
    }


    @Test
    public void sqlServer() {
        test("jdbc:sqlserver://database-1.foo.us-east-1.rds.amazonaws.com:1433", SqlServerQueryDialect.class);
    }

    @Test
    public void oracle() {
        test("jdbc:oracle:thin:@//temp-oracle.foo.us-east-1.rds.amazonaws.com:1521/test", OracleQueryDialect.class);
    }

    @Test
    public void redshift() {
        test("jdbc:redshift://redshift-cluster-1.foo.us-east-1.redshift.amazonaws.com:5439/dev", RedshiftQueryDialect.class);
    }

    @Test
    public void postgres() {
        test("jdbc:postgresql://1.1.1.1:5432/test_db", PostgreSqlQueryDialect.class);
    }

    @Test
    public void snowflake() {
        test("jdbc:snowflake://oka43275.us-east-1.snowflakecomputing.com?db=demo_db&warehouse=test1", SnowflakeQueryDialect.class);
    }

    private void test(String url, Class<? extends QueryDialect> expectedDialectClass) {
        assertEquals(expectedDialectClass, QueryDialectProvider.forConnection(url).getClass());
    }
}
