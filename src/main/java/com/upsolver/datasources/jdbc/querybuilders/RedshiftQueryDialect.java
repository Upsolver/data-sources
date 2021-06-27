package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;

public class RedshiftQueryDialect extends DefaultQueryDialect {
    public RedshiftQueryDialect(boolean keepType) {
        super(keepType, Collections.emptyMap());
    }

    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("select current_setting('TIMEZONE')").executeQuery();
        rs.next();
        return TimeZone.getTimeZone(rs.getString(1)).getRawOffset() / 1000;
    }

    @Override
    public boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException {
        var def = columnsResultSet.getString("COLUMN_DEF");
        // Example default value: "identity"(707455, 0, '1,1'::text)
        return def != null && def.startsWith("\"identity\"(");
    }
}
