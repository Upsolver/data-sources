package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RowReader implements AutoCloseable {
    private final TableInfo tableInfo;
    private final ResultSet underlying;
    private final int readLimit;
    private final Timestamp timeLimit;

    private long lastIncValue;
    private Timestamp lastTimestampValue;

    private int count = 0;

    public RowReader(TableInfo tableInfo, ResultSet underlying, int readLimit, Timestamp timeLimit) {
        this.tableInfo = tableInfo;
        this.underlying = underlying;
        this.readLimit = readLimit;
        this.timeLimit = timeLimit;
    }

    public boolean next() throws SQLException {
        if (readLimit >= 0 && count >= readLimit) return false;
        var result = underlying.next();
        if (result){
            var newTimestamp  = tableInfo.hasTimeColumns() ? extractTimestamp() : null;
            var newIncValue =  tableInfo.hasIncColumn() ? extractIncValue() : 0L;
            if (timeLimit != null){
                if (newTimestamp.compareTo(timeLimit) >= 0) {
                    underlying.previous();
                    result =false;
                }
            }
            if (result) {
                lastTimestampValue = newTimestamp;
                lastIncValue = newIncValue;
                count++;
            }
        }
        return result;
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
        return null;
    }

    public long getLastIncValue() {
        return lastIncValue;
    }

    public Timestamp getLastTimestampValue() {
        return lastTimestampValue;
    }

    @Override
    public void close() throws Exception {
        underlying.close();
    }
}
