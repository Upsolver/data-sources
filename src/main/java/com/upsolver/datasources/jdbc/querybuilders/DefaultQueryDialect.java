package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.SimpleSqlType;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;
import com.upsolver.datasources.jdbc.utils.ThrowingBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class DefaultQueryDialect implements QueryDialect {
    private static final Logger logger = LoggerFactory.getLogger(DefaultQueryDialect.class);
    private static final Collection<SQLType> timeTypes = new HashSet<>(Arrays.asList(
            JDBCType.DATE,
            JDBCType.TIMESTAMP,
            JDBCType.TIME,
            JDBCType.TIME_WITH_TIMEZONE,
            JDBCType.TIMESTAMP_WITH_TIMEZONE
    ));

    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getObject = ResultSet::getObject;
    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getString = ResultSet::getString;
    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getDate = (rs, i) -> {
        var ts = rs.getDate(i);
        return ts != null ? ts.getTime() : null;
    };
    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getTime = (rs, i) -> {
        var ts = rs.getTime(i);
        return ts != null ? ts.getTime() : null;
    };
    protected static final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getTimestamp = (rs, i) -> {
        var ts = rs.getTimestamp(i);
        return ts != null ? ts.getTime() : null;
    };

    protected static final Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> dateTimeGetters = new HashMap<>();

    static {
        dateTimeGetters.put(Types.DATE, getDate);
        dateTimeGetters.put(Types.TIME, getTime);
        dateTimeGetters.put(Types.TIMESTAMP, getTimestamp);
        dateTimeGetters.put(Types.TIME_WITH_TIMEZONE, getTime);
        dateTimeGetters.put(Types.TIMESTAMP_WITH_TIMEZONE, getTimestamp);
    }

    protected final Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> valueGetters;
    private final ThrowingBiFunction<ResultSet, Integer, Object, SQLException> defaultValueGetter;

    public DefaultQueryDialect(boolean keepType, Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> additionalGetters) {
        this(getAllGetters(keepType, additionalGetters), keepType ? getObject : getString);
    }

    private static Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> getAllGetters(boolean keepType, Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> additionalGetters) {
        Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> allGetters = new java.util.HashMap();
        if (keepType) {
            allGetters.putAll(dateTimeGetters);
        }
        allGetters.putAll(additionalGetters);
        return allGetters;
    }

    protected DefaultQueryDialect(Map<Integer, ThrowingBiFunction<ResultSet, Integer, Object, SQLException>> valueGetters, ThrowingBiFunction<ResultSet, Integer, Object, SQLException> defaultValueGetter) {
        this.valueGetters = valueGetters;
        this.defaultValueGetter = defaultValueGetter;
    }

    @Override
    public long utcOffsetSeconds(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)").executeQuery();
        rs.next();
        return rs.getTime(1).getTime() / 1000;
    }

    @Override
    public Instant getStartTime(TableInfo tableInfo, Connection connection) throws SQLException {
        String coalescedTimes = coalesceTimeColumns(tableInfo);
        String query = "SELECT MIN(" + coalescedTimes + ") AS min_time" +
                " FROM " + fullTableName(tableInfo);
        var rs = connection.prepareStatement(query).executeQuery();
        if (rs.next()) {
            var ts = rs.getTimestamp("min_time");
            return ts != null ? ts.toInstant().truncatedTo(ChronoUnit.MINUTES) : null;
        } else {
            return null;
        }
    }

    @Override
    public NamedPreparedStatment taskInfoByInc(TableInfo tableInfo,
                                               JDBCTaskMetadata metadata,
                                               Connection connection) throws SQLException {
        String incColumn = tableInfo.getIncColumn();
        String query = "SELECT MIN(" + incColumn + ") AS MIN," +
                " MAX(" + incColumn + ") AS MAX" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + incColumn + " >= :startFrom" +
                " HAVING MIN( " + incColumn + ") IS NOT NULL";
        var statement = new NamedPreparedStatment(connection, query);
        statement.setLong("startFrom", metadata.getExclusiveEnd());
        return statement;
    }

    @Override
    public NamedPreparedStatment taskInfoByTime(TableInfo tableInfo,
                                                JDBCTaskMetadata metadata,
                                                Instant maxTime,
                                                Connection connection) throws SQLException {
        String coalescedTimes = coalesceTimeColumns(tableInfo);
        String query = "SELECT MAX(" + coalescedTimes + ") AS last_time" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalescedTimes + " < :maxTime" +
                " AND " + coalescedTimes + " > :startTime" +
                " HAVING MAX( " + coalescedTimes + ") IS NOT NULL";
        var statement = new NamedPreparedStatment(connection, query);
        statement.setTime("startTime", metadata.getEndTime());
        statement.setTime("maxTime", maxTime);
        return statement;
    }

    @Override
    public NamedPreparedStatment taskInfoByIncAndTime(TableInfo tableInfo,
                                                      JDBCTaskMetadata metadata,
                                                      Instant maxTime,
                                                      Connection connection) throws SQLException {
        String coalescedTimes = coalesceTimeColumns(tableInfo);
        String incColumn = tableInfo.getIncColumn();
        String query = "SELECT MIN(" + incColumn + ") AS MIN," +
                " MAX(" + incColumn + ") AS MAX," +
                " MAX(" + coalescedTimes + ") AS last_time" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalescedTimes + " < :maxTime" +
                " AND ((" + coalescedTimes + " = :startTime AND " + incColumn + " >= :startFrom)" +
                " OR (" + coalescedTimes + " > :startTime))" +
                " HAVING MIN( " + incColumn + ") IS NOT NULL";
        var statement = new NamedPreparedStatment(connection, query);
        statement.setLong("startFrom", metadata.getExclusiveEnd());
        statement.setTime("startTime", metadata.getEndTime());
        statement.setTime("maxTime", maxTime);
        return statement;
    }

    @Override
    public NamedPreparedStatment queryByIncAndTime(TableInfo tableInfo,
                                                   JDBCTaskMetadata metadata,
                                                   int limit,
                                                   Connection connection) throws SQLException {
        String coalesce = coalesceTimeColumns(tableInfo);
        String incColumn = tableInfo.getIncColumn();
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalesce + " < :endTime" +
                " AND ((" + coalesce + " = :startTime AND " + incColumn + " >= :incStart)" +
                " OR (" + coalesce + " > :startTime))" +
                rownumCondition(limit, true, false) +
                " ORDER BY " + coalesce + ", " + incColumn + " ASC" +
                " " + endLimit(limit);
        var statement = new NamedPreparedStatment(connection, query);
        statement.setLong("incStart", metadata.getInclusiveStart());
        statement.setTime("startTime", metadata.getStartTime());
        statement.setTime("endTime", metadata.getEndTime());
        return statement;

    }

    @Override
    public NamedPreparedStatment queryByTime(TableInfo tableInfo,
                                             JDBCTaskMetadata metadata,
                                             int limit,
                                             Connection connection) throws SQLException {
        String coalesce = coalesceTimeColumns(tableInfo);
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalesce + " > :startTime AND " + coalesce + " <= :endTime" +
                rownumCondition(limit, true, false) +
                " ORDER BY " + coalesce + " ASC" +
                " " + endLimit(limit);
        var statement = new NamedPreparedStatment(connection, query);
        statement.setTime("startTime", metadata.getStartTime());
        statement.setTime("endTime", metadata.getEndTime());
        return statement;

    }

    @Override
    public NamedPreparedStatment queryByInc(TableInfo tableInfo,
                                            JDBCTaskMetadata metadata,
                                            int limit,
                                            Connection connection) throws SQLException {
        String incColumn = tableInfo.getIncColumn();
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + incColumn + " BETWEEN :incStart AND :incEnd" +
                rownumCondition(limit, true, false) +
                " " + endLimit(limit);
        var statement = new NamedPreparedStatment(connection, query);
        statement.setLong("incStart", metadata.getInclusiveStart());
        statement.setLong("incEnd", metadata.getExclusiveEnd() - 1);
        return statement;
    }

    @Override
    public NamedPreparedStatment queryFullTable(TableInfo tableInfo,
                                                JDBCTaskMetadata metadata,
                                                int limit,
                                                Connection connection) throws SQLException {
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                rownumCondition(limit, false, true) +
                " " + endLimit(limit);
        return new NamedPreparedStatment(connection, query);
    }

    @Override
    public PreparedStatement getCurrentTimestamp(Connection connection) throws SQLException {
        return connection.prepareStatement(currentTimeQuery());
    }

    protected String currentTimeQuery() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    private String coalesce(String columns) {
        return "COALESCE(" + columns + ")";
    }

    private String coalesceTimeColumns(TableInfo tableInfo) {
        return tableInfo.getTimeColumns().length > 1 ? coalesce(tableInfo.getTimeColumnsAsString()) : tableInfo.getTimeColumnsAsString();
    }

    protected String topLimit(long amount) {
        return "";
    }

    protected String rownumCondition(long amount, boolean includeAnd, boolean includeWhere) {
        return "";
    }

    @Override
    public boolean requiresUppercaseNames() {
        return false;
    }

    @Override
    public String toUpperCaseIfRequired(String s) {
        return s != null && requiresUppercaseNames() ? s.toUpperCase() : s;
    }

    protected String endLimit(long amount) {
        return amount >= 0 ? "limit " + amount : "";
    }

    @Override
    public boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException {
        return "yes".equalsIgnoreCase(columnsResultSet.getString("IS_AUTOINCREMENT"));
    }

    @Override
    public boolean isTimeType(SQLType sqlType) throws SQLException {
        return timeTypes.contains(getJdbcType(sqlType));
    }

    @Override
    public SQLType getSqlType(int code) throws SQLException {
        try {
            return JDBCType.valueOf(code);
        } catch (IllegalArgumentException e) {
            logger.warn("Illegal data type {}", code);
            return new SimpleSqlType("unknown", "default", code);
        }
    }

    @Override
    public SQLType getJdbcType(SQLType sqlType) {
        return sqlType;
    }

    @Override
    public String fullTableName(TableInfo tableInfo) {
        if (tableInfo.getSchema() != null && !tableInfo.getSchema().isEmpty()) {
            return tableInfo.getSchema() + "." + tableInfo.getName();
        } else {
            return tableInfo.getName();
        }
    }


    @Override
    public Connection getConnection(String url, java.util.Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    @Override
    public String getDriverClassName() {
        return null;
    }

    @Override
    public ThrowingBiFunction<ResultSet, Integer, Object, SQLException> getValueGetter(int sqlType) {
        return valueGetters.getOrDefault(sqlType, defaultValueGetter);
    }
}
