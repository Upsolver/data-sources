package com.upsolver.datasources.jdbc.querybuilders;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import oracle.jdbc.OracleType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class QueryDialectTest {
    private static final SQLType[] timestampTypes = new SQLType[] {JDBCType.DATE, JDBCType.TIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE};
    @ClassRule public static final JdbcDatabaseContainer<?> mysql = new MySQLContainer<>("mysql:5.7.22"); //.withCommand("create table test (id int, ts timestamp, text varchar(16))");
    @ClassRule public static final JdbcDatabaseContainer<?> postgresql = new PostgreSQLContainer<>("postgres:9.6.12");
    @ClassRule public static final JdbcDatabaseContainer<?> sqlserver = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-CU12");
    @ClassRule public static final JdbcDatabaseContainer<?> oracle = new OracleContainer("oracleinanutshell/oracle-xe-11g:1.0.0");


    private final String url;
    private final long expectedOffset;
    private final boolean expectedUpperCase;
    private final boolean acceptAnyUrl;
    private final SQLType[] additionalTimestampTypes;
    private final String expectedDriverName;

    public QueryDialectTest(String url, long expectedOffset, boolean expectedUpperCase, boolean acceptAnyUrl, SQLType[] additionalTimestampTypes, String expectedDriverName) {
        this.url = url;
        this.expectedOffset = expectedOffset;
        this.expectedUpperCase = expectedUpperCase;
        this.acceptAnyUrl = acceptAnyUrl;
        this.additionalTimestampTypes = additionalTimestampTypes;
        this.expectedDriverName = expectedDriverName;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<?> primeNumbers() {
        int offset = TimeZone.getDefault().getRawOffset() / 1000;
        return Arrays.asList(new Object[][] {
                { "jdbc:mysql://host", 0L, false, true, new SQLType[0], null},
                { "jdbc:postgresql://host", offset, false, false, new SQLType[0], "org.postgresql.Driver" },
                { "jdbc:sqlserver://host", 0L, false, false, new SQLType[0], null },
                { "jdbc:oracle://host", offset, true, false, new SQLType[] {OracleType.TIMESTAMP, OracleType.TIMESTAMP_WITH_TIME_ZONE, OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE}, null },
        });
    }

    @Test
    public void overriders() throws SQLException, ReflectiveOperationException {
        QueryDialect dialect = QueryDialectProvider.forConnection(url);
        String dbName = url.split(":")[1];
        JdbcDatabaseContainer<?> container = (JdbcDatabaseContainer<?>)getClass().getField(dbName).get(null);
        DataSource ds = getDataSource(container);

        assertEquals(expectedOffset, dialect.utcOffsetSeconds(ds.getConnection()));
        assertTrue(dialect.acceptsURL(url));
        assertEquals(acceptAnyUrl, dialect.acceptsURL("jdbc:nothing"));
        assertEquals(expectedUpperCase, dialect.requiresUppercaseNames());

        Set<SQLType> timestampTypesSet = Arrays.stream(timestampTypes).collect(Collectors.toSet());
        for (SQLType type : JDBCType.values()) {
            assertEquals(timestampTypesSet.contains(type), dialect.isTimeType(type));
        }
        for (SQLType type : additionalTimestampTypes) {
            assertTrue(dialect.isTimeType(type));
        }
        assertEquals(expectedDriverName, dialect.getDriverClassName());
    }

    protected DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());

        return new HikariDataSource(hikariConfig);
    }
}
