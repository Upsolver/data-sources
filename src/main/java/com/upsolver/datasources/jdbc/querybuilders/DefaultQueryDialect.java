package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.SimpleSqlType;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

public class DefaultQueryDialect implements QueryDialect {
    private static final Logger logger = LoggerFactory.getLogger(DefaultQueryDialect.class);
    private static final Collection<SQLType> timeTypes = new HashSet<>(Arrays.asList(
            JDBCType.DATE,
            JDBCType.TIMESTAMP,
            JDBCType.TIME,
            JDBCType.TIME_WITH_TIMEZONE,
            JDBCType.TIMESTAMP_WITH_TIMEZONE
    ));

    @Override
    public long utcOffset(Connection connection) throws SQLException {
        var rs = connection.prepareStatement("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)").executeQuery();
        rs.next();
        return rs.getTime(1).getSeconds();
    }


    @Override
    public NamedPreparedStatment taskInfoByInc(TableInfo tableInfo,
                                               JDBCTaskMetadata metadata,
                                               Connection connection) throws SQLException {
        String query = "SELECT MIN(" + tableInfo.getIncColumn() + ") AS min," +
                " MAX(" + tableInfo.getIncColumn() + ") AS max" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + tableInfo.getIncColumn() + " >= :startFrom" +
                " HAVING MIN( " + tableInfo.getIncColumn() + ") IS NOT NULL";
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
        String query = "SELECT MIN(" + tableInfo.getIncColumn() + ") AS min," +
                " MAX(" + tableInfo.getIncColumn() + ") AS max," +
                " MAX(" + coalescedTimes + ") AS last_time" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalescedTimes + " < :maxTime" +
                " AND ((" + coalescedTimes + " = :startTime AND " + tableInfo.getIncColumn() + " >= :startFrom)" +
                " OR (" + coalescedTimes + " > :startTime))" +
                " HAVING MIN( " + tableInfo.getIncColumn() + ") IS NOT NULL";
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
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + coalesce + " < :endTime" +
                " AND ((" + coalesce + " = :startTime AND " + tableInfo.getIncColumn() + " >= :incStart)" +
                " OR (" + coalesce + " > :startTime))" +
                rownumCondition(limit, true, false) +
                " ORDER BY " + coalesce + ", " + tableInfo.getIncColumn() + " ASC" +
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
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + fullTableName(tableInfo) +
                " WHERE " + tableInfo.getIncColumn() + " BETWEEN :incStart AND :incEnd" +
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
        if(tableInfo.getSchema() != null  && !tableInfo.getSchema().isEmpty()){
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
}
