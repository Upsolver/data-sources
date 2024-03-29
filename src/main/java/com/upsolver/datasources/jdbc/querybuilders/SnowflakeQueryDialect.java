package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;

public class SnowflakeQueryDialect extends DefaultQueryDialect {
    public SnowflakeQueryDialect(boolean keepType) {
        super(keepType, Collections.emptyMap());
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
}
