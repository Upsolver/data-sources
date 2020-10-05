package com.upsolver.datasources.jdbc.querybuilders;

import oracle.jdbc.OracleType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.upsolver.datasources.jdbc.querybuilders.DefaultQueryDialect.getDate;
import static com.upsolver.datasources.jdbc.querybuilders.DefaultQueryDialect.getObject;
import static com.upsolver.datasources.jdbc.querybuilders.DefaultQueryDialect.getString;
import static com.upsolver.datasources.jdbc.querybuilders.DefaultQueryDialect.getTime;
import static com.upsolver.datasources.jdbc.querybuilders.DefaultQueryDialect.getTimestamp;
import static com.upsolver.datasources.jdbc.querybuilders.OracleQueryDialect.blobAsString;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class QueryDialectProviderTest {
    private static final Set<Integer> dateTimeTypes = new HashSet<>(Arrays.asList(Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME, Types.TIME_WITH_TIMEZONE));
    private final boolean keepTypes;

    public QueryDialectProviderTest(boolean keepTypes) {
        this.keepTypes = keepTypes;
    }

    @Parameterized.Parameters(name = "keepTypes={0}" )
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Test
    public void testDefault() {
        test("jdbc:unknown", DefaultQueryDialect.class, dateTimeTypes);
    }

    @Test
    public void sqlServer() {
        test("jdbc:sqlserver://database-1.foo.us-east-1.rds.amazonaws.com:1433", SqlServerQueryDialect.class, dateTimeTypes);
    }

    @Test
    public void oracle() {
        Collection<Integer> specialTypes = Stream.of(dateTimeTypes, singleton(Types.BLOB)).flatMap(Collection::stream).collect(toList());
        QueryDialect dialect = test("jdbc:oracle:thin:@//temp-oracle.foo.us-east-1.rds.amazonaws.com:1521/test", OracleQueryDialect.class, specialTypes);
        if (keepTypes) {
            assertEquals(getTimestamp, dialect.getValueGetter(OracleType.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber()));
            assertEquals(getTimestamp, dialect.getValueGetter(OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getVendorTypeNumber()));
            assertEquals(blobAsString, dialect.getValueGetter(Types.BLOB));
        }
    }

    @Test
    public void redshift() {
        test("jdbc:redshift://redshift-cluster-1.foo.us-east-1.redshift.amazonaws.com:5439/dev", RedshiftQueryDialect.class, dateTimeTypes);
    }

    @Test
    public void postgres() {
        test("jdbc:postgresql://1.1.1.1:5432/test_db", PostgreSqlQueryDialect.class, dateTimeTypes);
    }

    @Test
    public void snowflake() {
        test("jdbc:snowflake://oka43275.us-east-1.snowflakecomputing.com?db=demo_db&warehouse=test1", SnowflakeQueryDialect.class, dateTimeTypes);
    }

    private QueryDialect test(String url, Class<? extends QueryDialect> expectedDialectClass, Collection<Integer> specialTypes) {
        QueryDialect dialect = QueryDialectProvider.forConnection(url, keepTypes);
        assertEquals(expectedDialectClass, dialect.getClass());
        assertGetters(dialect, specialTypes);
        return dialect;
    }

    private void assertGetters(QueryDialect dialect, Collection<Integer> specialTypes) {
        if (keepTypes) {
            assertEquals(getDate, dialect.getValueGetter(Types.DATE));
            assertEquals(getTimestamp, dialect.getValueGetter(Types.TIMESTAMP));
            assertEquals(getTimestamp, dialect.getValueGetter(Types.TIMESTAMP_WITH_TIMEZONE));
            assertEquals(getTime, dialect.getValueGetter(Types.TIME));
            assertEquals(getTime, dialect.getValueGetter(Types.TIME_WITH_TIMEZONE));
            Arrays.stream(JDBCType.values())
                    .filter(t -> !specialTypes.contains(t.getVendorTypeNumber()))
                    .forEach(t -> assertEquals(t.getName(), getObject, dialect.getValueGetter(t.getVendorTypeNumber())));
        } else {
            Arrays.stream(JDBCType.values()).forEach(t -> assertEquals(getString, dialect.getValueGetter(t.getVendorTypeNumber())));
        }
    }
}
