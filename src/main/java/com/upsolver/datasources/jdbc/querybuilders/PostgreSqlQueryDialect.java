package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

public class PostgreSqlQueryDialect extends DefaultQueryDialect {
    public PostgreSqlQueryDialect(boolean keepType) {
        super(keepType);
    }

    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("select current_setting('TIMEZONE')").executeQuery();
        rs.next();
        return TimeZone.getTimeZone(rs.getString(1)).getRawOffset() / 1000;
    }

    @Override
    public boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException {
        var def = columnsResultSet.getString("COLUMN_DEF");
        // Example default value: nextval('test_identity_id_seq'::regclass)
        return def != null && def.startsWith("nextval(");
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }
}
