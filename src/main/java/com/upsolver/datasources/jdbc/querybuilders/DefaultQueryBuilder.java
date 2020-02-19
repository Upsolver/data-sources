package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

public class DefaultQueryBuilder implements QueryBuilder {

    @Override
    public PreparedStatement utcOffset(Connection connection) throws SQLException {
        return connection.prepareStatement("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)");
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
                " HAVING MAX( " + coalescedTimes  + ") IS NOT NULL";
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
        String query = "SELECT *" +
                " FROM " + tableInfo.getName() +
                " WHERE " + tableInfo.getIncColumn() + " BETWEEN :incStart AND :incEnd" +
                " AND " + coalesce + " BETWEEN :startTime AND :endTime" +
                " ORDER BY " + coalesce + ", " + tableInfo.getIncColumn() + " ASC" +
                "";
        if (limit > 0) {
            query = query + " limit " + limit;
        }
        try {
            var statement = new NamedPreparedStatment(connection, query);
            statement.setLong("incStart", metadata.getInclusiveStart());
            statement.setLong("incEnd", metadata.getExclusiveEnd() - 1);
            statement.setTime("startTime", metadata.getStartTime());
            statement.setTime("endTime", metadata.getEndTime());
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
    }

    @Override
    public NamedPreparedStatment queryByTime(TableInfo tableInfo,
                                             JDBCTaskMetadata metadata,
                                             int limit,
                                             Connection connection) throws SQLException {
        String coalesce = coalesce(tableInfo.getTimeColumnsAsString());
        String query = "SELECT *" +
                " FROM " + tableInfo.getName() +
                " WHERE " + coalesce + " > :startTime AND " + coalesce + " <= :endTime" +
                " ORDER BY " + coalesce + " ASC" +
                "";
        if (limit > 0) {
            query = query + " limit " + limit;
        }
        try {
            var statement = new NamedPreparedStatment(connection, query);
            statement.setTime("startTime", metadata.getStartTime());
            statement.setTime("endTime", metadata.getEndTime());
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
    }

    @Override
    public NamedPreparedStatment queryByInc(TableInfo tableInfo,
                                            JDBCTaskMetadata metadata,
                                            int limit,
                                            Connection connection) throws SQLException {
        String query = "SELECT *" +
                " FROM " + tableInfo.getName() +
                " WHERE " + tableInfo.getIncColumn() + " BETWEEN :incStart AND :incEnd";
        if (limit > 0) {
            query = query + " limit " + limit;
        }
        try {
            var statement = new NamedPreparedStatment(connection, query);
            statement.setLong("incStart", metadata.getInclusiveStart());
            statement.setLong("incEnd", metadata.getExclusiveEnd() - 1);
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
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
}
