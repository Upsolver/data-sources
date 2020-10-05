package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;

public class SnowflakeQueryDialect extends DefaultQueryDialect {
    public SnowflakeQueryDialect() {
        super(false);
    }

    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("show parameters like 'TIMEZONE'").executeQuery();
        rs.next();
        return TimeZone.getTimeZone(rs.getString(1)).getRawOffset() / 1000;
    }

    @Override
    public boolean requiresUppercaseNames() {
        return true;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:snowflake:");
    }
}
