package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;

public class H2QueryDialect extends DefaultQueryDialect {
    @Override
    public long utcOffsetSeconds(Connection connection) throws SQLException {
        return TimeZone.getDefault().getRawOffset() / 1000; // H2 uses current timezone
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:h2:");
    }

}
