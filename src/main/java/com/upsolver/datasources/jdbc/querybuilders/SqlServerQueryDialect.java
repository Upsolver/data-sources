package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.metadata.SimpleSqlType;
import microsoft.sql.Types;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlServerQueryDialect extends DefaultQueryDialect {
    private static final Map<Integer, SQLType> sqlServerTypes = Arrays.stream(Types.class.getFields())
            .map(f -> new SimpleSqlType(f.getName(), "microsoft", getIntValue(f))).collect(Collectors.toMap(SimpleSqlType::getVendorTypeNumber, t -> t));
    private static final Collection<Integer> sqlServerTimeTypeCodes = new HashSet<>(Arrays.asList(Types.DATETIMEOFFSET, Types.DATETIME, Types.SMALLDATETIME));
    private static final Collection<SQLType> sqlServerTimeTypes = sqlServerTypes.values().stream()
            .filter(f -> sqlServerTimeTypeCodes.contains(f.getVendorTypeNumber())).collect(Collectors.toSet());

    public SqlServerQueryDialect(boolean keepType) {
        super(keepType);
    }

    @Override
    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("SELECT DATEDIFF(second, GETDATE(), GETUTCDATE());").executeQuery();
        rs.next();
        return rs.getLong(1);
    }

    @Override
    protected String topLimit(long amount) {
        return amount >= 0 ? "top " + amount : "";
    }

    @Override
    protected String endLimit(long amount) {
        return "";
    }


    @Override
    public SQLType getSqlType(int code) {
        return Optional.ofNullable(sqlServerTypes.get(code)).orElseGet(() -> JDBCType.valueOf(code));
    }

    @Override
    public boolean isTimeType(SQLType sqlType) throws SQLException {
        return sqlServerTimeTypes.contains(sqlType) || super.isTimeType(sqlType);
    }

    private static int getIntValue(Field field) {
        try {
            return field.getInt(null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
