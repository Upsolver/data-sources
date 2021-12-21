package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.utils.ThrowingBiFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class MysqlQueryDialect extends DefaultQueryDialect {

    protected static final Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> additionalGetter = new HashMap<>();

    static {
        additionalGetter.put(Types.TIME, getString);
    }

    public MysqlQueryDialect(boolean keepType) {
        super(keepType, additionalGetter);
    }
}
