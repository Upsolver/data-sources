package com.upsolver.datasources.jdbc.querybuilders;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleQueryDialect extends DefaultQueryDialect {
    @Override
    public long utcOffset(Connection connection) throws SQLException {
        var rs =
                connection.prepareStatement("SELECT extract(day from (SYSTIMESTAMP - sys_extract_utc(systimestamp)) * 24 * 60 * 60) FROM DUAL")
                        .executeQuery();
        rs.next();
        return rs.getLong(1);
    }

    @Override
    protected String rownumCondition(long amount, boolean includeAnd) {
        if (amount >= 0) {
            var and = includeAnd ? " AND " : "";
            return and + " ROWNUM <= " + amount + " ";
        } else {
            return "";
        }
    }

    @Override
    public boolean requiresUppercaseNames() {
        return true;
    }

    @Override
    protected String endLimit(long amount) {
        return "";
    }

    @Override
    public boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException {
        var def = columnsResultSet.getString("COLUMN_DEF");
        // Example default value: "ADMIN"."ISEQ$$_20599".nextval
        return def != null && def.toUpperCase().endsWith(".NEXTVAL") && def.toUpperCase().contains("ISEQ$$");
    }
}
