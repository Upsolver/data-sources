package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.utils.ThrowingBiFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class PostgreSqlQueryDialect extends DefaultQueryDialect {


    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getStruct = (rs, i) -> {
        var ts = rs.getString(i);
        return ts;
    };
    protected static final Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> additionalGetter = new HashMap<>();

    static {
        dateTimeGetters.put(Types.STRUCT, getStruct);
        dateTimeGetters.put(Types.ARRAY, getStruct);
        dateTimeGetters.put(Types.OTHER, getStruct);
    }


    public PostgreSqlQueryDialect(boolean keepType) {
        super(keepType, additionalGetter);
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
