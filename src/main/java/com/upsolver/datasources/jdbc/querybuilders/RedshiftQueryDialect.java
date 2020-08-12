package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

public class RedshiftQueryDialect extends DefaultQueryDialect {
    public long utcOffset(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("select current_setting('TIMEZONE')").executeQuery();
        return rs.next() ? TimeZone.getTimeZone(rs.getString(1)).getRawOffset() : 0;
    }

    @Override
    public boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException {
        var def = columnsResultSet.getString("COLUMN_DEF");
        // Example default value: "identity"(707455, 0, '1,1'::text)
        return def != null && def.startsWith("\"identity\"(");
    }
}
