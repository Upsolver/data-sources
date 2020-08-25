package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.Instant;

public interface QueryDialect {

    long utcOffset(Connection connection) throws SQLException;

    boolean requiresUppercaseNames();

    boolean isAutoIncrementColumn(ResultSet columnsResultSet) throws SQLException;

    boolean isTimeType(SQLType sqlType) throws SQLException;

    SQLType getSqlType(int code) throws SQLException;

    SQLType getJdbcType(SQLType sqlType) throws SQLException;

    String fullTableName(TableInfo tableInfo);

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

    NamedPreparedStatment queryFullTable(TableInfo tableInfo,
                                         JDBCTaskMetadata metadata,
                                         int limit,
                                         Connection connection) throws SQLException;

    PreparedStatement getCurrentTimestamp(Connection connection) throws SQLException;

    Connection getConnection(String url, java.util.Properties info) throws SQLException;

    String getDriverClassName();

}
