package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RowReader implements AutoCloseable {
    private final TableInfo tableInfo;
    private final ResultSet underlying;
    private final JDBCTaskMetadata metadata;
    private final Timestamp timeLimit;
    private final Timestamp lowerTimeLimit;
    private Connection connection;

    private boolean readValues;
    private long lastIncValue;
    private Timestamp lastTimestampValue;

    /**
     * Exposes a similar interface to ResultSet but allows us to limit reading.
     * Gets the connection to close after reading is complete. Closing the connection returns it to the connection pool
     */
    public RowReader(TableInfo tableInfo, ResultSet underlying, JDBCTaskMetadata metadata, Connection connection) {
        this.tableInfo = tableInfo;
        this.underlying = underlying;
        this.metadata = metadata;
        this.timeLimit = Timestamp.from(metadata.getEndTime());
        this.lowerTimeLimit = Timestamp.from(metadata.getStartTime());
        this.connection = connection;
    }

    public boolean next() throws SQLException {
        var result = underlying.next();
        if (result) {
            var newTimestamp = tableInfo.hasTimeColumns() ? extractTimestamp() : null;
            var newIncValue = tableInfo.hasIncColumn() ? extractIncValue() : 0L;
            if (exceedsLimits(newTimestamp, newIncValue)) {
                underlying.previous();
                result = false;
            }
            if (result) {
                readValues = true;
                lastTimestampValue = newTimestamp;
                lastIncValue = newIncValue;
            }
            if (precedesLimits(newTimestamp)){
                // Do this after saving the updated timestamp values
                return next();
            }
        }
        return result;
    }

    private boolean precedesLimits(Timestamp newTimestamp) {
        return (tableInfo.hasTimeColumns() && newTimestamp.compareTo(lowerTimeLimit) < 0);
    }

    private boolean exceedsLimits(Timestamp newTimestamp, long newIncValue) {
        return (tableInfo.hasTimeColumns() && newTimestamp.compareTo(timeLimit) >= 0) ||
                (tableInfo.hasIncColumn() && newIncValue >= metadata.getExclusiveEnd());
    }

    public String[] getValues() throws SQLException {
        var result = new String[tableInfo.getColumnCount()];
        for (int i = 0; i < tableInfo.getColumnCount(); i++) {
            result[i] = underlying.getString(i + 1); // Column indices start at 1 (☉_☉)
        }
        return result;
    }

    public long extractIncValue() throws SQLException {
        return underlying.getLong(tableInfo.getIncColumn());
    }

    public Timestamp extractTimestamp() throws SQLException {
        for (String timeColumn : tableInfo.getTimeColumns()) {
            var ts = underlying.getTimestamp(timeColumn);
            if (ts != null) {
                return ts;
            }
        }
        throw new IllegalStateException("Every row must contain a timestamp");
    }

    public long getLastIncValue() {
        return lastIncValue;
    }

    public Timestamp getLastTimestampValue() {
        return lastTimestampValue;
    }

    public boolean readValues() {
        return readValues;
    }

    @Override
    public void close() throws Exception {
        underlying.close();
        connection.close();
    }
}
