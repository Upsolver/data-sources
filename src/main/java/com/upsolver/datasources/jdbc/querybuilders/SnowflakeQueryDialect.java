package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;

public class SnowflakeQueryDialect extends DefaultQueryDialect {
    public SnowflakeQueryDialect() {
        super(Collections.emptyMap(), IdentifierNormalizer.TO_UPPER_CASE);
    }

    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("show parameters like 'TIMEZONE'").executeQuery();
        rs.next();
        return TimeZone.getTimeZone(rs.getString(1)).getRawOffset() / 1000;
    }
}
