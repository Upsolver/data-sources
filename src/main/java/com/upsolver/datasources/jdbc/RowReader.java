package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RowReader implements AutoCloseable {
    private final TableInfo tableInfo;
    private ResultSetValuesGetter valuesGetter;
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
    public RowReader(TableInfo tableInfo, ResultSetValuesGetter valuesGetter, JDBCTaskMetadata metadata, Connection connection) {
        this.tableInfo = tableInfo;
        this.valuesGetter = valuesGetter;
        this.metadata = metadata;
        this.timeLimit = Timestamp.from(metadata.getEndTime());
        this.lowerTimeLimit = Timestamp.from(metadata.getStartTime());
        this.connection = connection;
    }

    public boolean next() throws SQLException {
        var result = valuesGetter.next();
        if (result) {
            var newTimestamp = tableInfo.hasTimeColumns() ? valuesGetter.extractTimestamp() : null;
            var newIncValue = tableInfo.hasIncColumn() ? valuesGetter.extractIncValue() : 0L;
            if (exceedsLimits(newTimestamp, newIncValue)) {
                valuesGetter.previous();
                result = false;
            }
            if (result) {
                readValues = true;
                lastTimestampValue = newTimestamp;
                lastIncValue = newIncValue;
            }
            if (precedesLimits(newTimestamp)) {
                // Do this after saving the updated timestamp values
                return next();
            }
        }
        return result;
    }

    public String[] getValues() throws SQLException {
        return valuesGetter.getValues();
    }

    private boolean precedesLimits(Timestamp newTimestamp) {
        return (tableInfo.hasTimeColumns() && newTimestamp.compareTo(lowerTimeLimit) < 0);
    }

    private boolean exceedsLimits(Timestamp newTimestamp, long newIncValue) {
        return (tableInfo.hasTimeColumns() && newTimestamp.compareTo(timeLimit) >= 0) ||
                (tableInfo.hasIncColumn() && newIncValue >= metadata.getExclusiveEnd());
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
        valuesGetter.close();
        connection.close();
    }
}

