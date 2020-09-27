package com.upsolver.datasources.jdbc.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SQLDriversTest {
    @Test
    public void getPrefix() {
        assertEquals("jdbc:postgresql", SQLDrivers.getPrefix("jdbc:postgresql://localhost:5432/test_db"));
    }

    @Test
    public void getDrivers() {
        Collection<SQLDriver> drivers = new SQLDrivers().getDrivers();
        Set<String> expectedDriverNames = new HashSet<>(Arrays.asList("MySQL Driver,Amazon Redshift,Microsoft MSSQL Server JDBC Driver,Snowflake Database,Oracle DB,PostgreSQL".split("\\s*,\\s*")));
        Set<String> actualDriverNames = drivers.stream().map(SQLDriver::getName).collect(Collectors.toSet());
        assertEquals(expectedDriverNames, actualDriverNames);
    }
}