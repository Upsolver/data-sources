package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

public class DefaultQueryDialect implements QueryDialect {

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
                " FROM " + tableInfo.getName() +
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
        String coalescedTimes = coalesce(tableInfo.getTimeColumnsAsString());
        String query = "SELECT MAX(" + coalescedTimes + ") AS last_time" +
                " FROM " + tableInfo.getName() +
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
        String coalescedTimes = coalesce(tableInfo.getTimeColumnsAsString());
        String query = "SELECT MIN(" + tableInfo.getIncColumn() + ") AS min," +
                " MAX(" + tableInfo.getIncColumn() + ") AS max," +
                " MAX(" + coalescedTimes + ") AS last_time" +
                " FROM " + tableInfo.getName() +
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
        String coalesce = coalesce(tableInfo.getTimeColumnsAsString());
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + tableInfo.getName() +
                " WHERE " + coalesce + " < :endTime" +
                " AND ((" + coalesce + " = :startTime AND " + tableInfo.getIncColumn() + " >= :incStart)" +
                " OR (" + coalesce + " > :startTime))" +
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
        String coalesce = coalesce(tableInfo.getTimeColumnsAsString());
        String query = "SELECT " + topLimit(limit) + " *" +
                " FROM " + tableInfo.getName() +
                " WHERE " + coalesce + " > :startTime AND " + coalesce + " <= :endTime" +
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
                " FROM " + tableInfo.getName() +
                " WHERE " + tableInfo.getIncColumn() + " BETWEEN :incStart AND :incEnd" +
                " " + endLimit(limit);
        var statement = new NamedPreparedStatment(connection, query);
        statement.setLong("incStart", metadata.getInclusiveStart());
        statement.setLong("incEnd", metadata.getExclusiveEnd() - 1);
        return statement;
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

    protected String topLimit(long amount) {
        return "";
    }

    protected String endLimit(long amount) {
        return amount >= 0 ? "limit " + amount : "";
    }
}
