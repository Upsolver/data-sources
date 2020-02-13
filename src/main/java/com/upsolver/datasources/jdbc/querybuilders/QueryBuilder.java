package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

public interface QueryBuilder {

    PreparedStatement utcOffset(Connection connection) throws SQLException;

    NamedPreparedStatment taskInfoQueryInc(TableInfo tableInfo,
                                           JDBCTaskMetadata metadata,
                                           Connection connection) throws SQLException;

    NamedPreparedStatment taskInfoQueryIncAndTime(TableInfo tableInfo,
                                                  JDBCTaskMetadata metadata,
                                                  Instant maxTime,
                                                  Connection connection) throws SQLException;

    NamedPreparedStatment queryWithIncAndTime(TableInfo tableInfo,
                                              JDBCTaskMetadata metadata,
                                              int limit,
                                              Connection connection) throws SQLException;

    NamedPreparedStatment queryWithInc(TableInfo tableInfo,
                                       JDBCTaskMetadata metadata,
                                       int limit,
                                       Connection connection) throws SQLException;

    PreparedStatement getCurrentTimestamp(Connection connection) throws SQLException;

}
