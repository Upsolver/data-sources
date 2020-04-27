package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

public interface QueryDialect {

    long utcOffset(Connection connection) throws SQLException;

    boolean requiresUppercaseNames();

    boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException;

    NamedPreparedStatment taskInfoByInc(TableInfo tableInfo,
                                        JDBCTaskMetadata metadata,
                                        Connection connection) throws SQLException;

    NamedPreparedStatment taskInfoByTime(TableInfo tableInfo,
                                               JDBCTaskMetadata metadata,
                                               Instant maxTime,
                                               Connection connection) throws SQLException;

    NamedPreparedStatment taskInfoByIncAndTime(TableInfo tableInfo,
                                               JDBCTaskMetadata metadata,
                                               Instant maxTime,
                                               Connection connection) throws SQLException;

    NamedPreparedStatment queryByIncAndTime(TableInfo tableInfo,
                                            JDBCTaskMetadata metadata,
                                            int limit,
                                            Connection connection) throws SQLException;

    NamedPreparedStatment queryByTime(TableInfo tableInfo,
                                            JDBCTaskMetadata metadata,
                                            int limit,
                                            Connection connection) throws SQLException;

    NamedPreparedStatment queryByInc(TableInfo tableInfo,
                                     JDBCTaskMetadata metadata,
                                     int limit,
                                     Connection connection) throws SQLException;

    PreparedStatement getCurrentTimestamp(Connection connection) throws SQLException;

}
