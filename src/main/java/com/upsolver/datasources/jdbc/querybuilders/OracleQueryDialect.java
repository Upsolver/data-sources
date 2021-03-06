package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.utils.ThrowingBiFunction;
import oracle.jdbc.OracleType;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

public class OracleQueryDialect extends DefaultQueryDialect {
    private static final Collection<SQLType> oracleTimeTypes = new HashSet<>(Arrays.asList(
            OracleType.TIMESTAMP,
            OracleType.TIMESTAMP_WITH_TIME_ZONE,
            OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE
    ));

    private static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> blobAsString = (rs, i) -> Optional.ofNullable(rs.getBytes(i)).map(bytes -> new BigInteger(1, bytes).toString(16)).orElse(null);
    private static final Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> getters = new HashMap<>();
    static {
        getters.put(Types.DATE, getDate);
        getters.put(Types.TIME, getTime);
        getters.put(Types.TIMESTAMP, getTimestamp);
        getters.put(OracleType.TIMESTAMP.getVendorTypeNumber(), getTimestamp);
        getters.put(Types.TIME_WITH_TIMEZONE, getTime);
        getters.put(Types.TIMESTAMP_WITH_TIMEZONE, getTimestamp);
        getters.put(OracleType.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber(), getTimestamp);
        getters.put(OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getVendorTypeNumber(), getTimestamp);
        getters.put(Types.BLOB, blobAsString);
    }

    public OracleQueryDialect(boolean keepType) {
        super(keepType ? getters : Collections.emptyMap(), keepType ? getObject : getString);
    }

    @Override
    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs =
                connection.prepareStatement("SELECT extract(day from (SYSTIMESTAMP - sys_extract_utc(systimestamp)) * 24 * 60 * 60) FROM DUAL")
                        .executeQuery();
        rs.next();
        return rs.getLong(1);
    }

    @Override
    protected String rownumCondition(long amount, boolean includeAnd, boolean includeWhere) {
        if (amount >= 0) {
            var where = includeWhere ? " WHERE " : "";
            var and = !includeWhere && includeAnd ? " AND " : "";
            return where + and + " ROWNUM <= " + amount + " ";
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


    @Override
    public SQLType getSqlType(int code) throws SQLException {
        return Optional.ofNullable((SQLType)OracleType.toOracleType(code)).orElseGet(() -> JDBCType.valueOf(code));
    }


    @Override
    public boolean isTimeType(SQLType sqlType) throws SQLException {
        return oracleTimeTypes.contains(sqlType) || super.isTimeType(sqlType);
    }

    @Override
    public SQLType getJdbcType(SQLType sqlType) {
        if (sqlType instanceof OracleType) {
            Integer oracleCode = sqlType.getVendorTypeNumber();
            // most of the oracle specific codes are equal to standard JDBC codes.
            // So, we do the best effort to return standard JDBCType that to case super.isTimeType() to work correctly.
            if (oracleCode != null) {
                try {
                    return JDBCType.valueOf(oracleCode);
                } catch (IllegalArgumentException e) {
                    return sqlType;
                }
            }
        }
        return sqlType;
    }
}
