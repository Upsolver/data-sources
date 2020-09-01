package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.querybuilders.QueryDialect;
import com.upsolver.datasources.jdbc.utils.ThrowingBiFunction;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

class ResultSetValuesGetter implements AutoCloseable {
    private final TableInfo tableInfo;
    private final ResultSet underlying;
    private final List<ThrowingBiFunction<ResultSet, Integer, String, SQLException>> valueGetters;

    private String[] nextValues = null;
    private long nextIncValue;
    private Timestamp nextTimestampValue;
    private boolean onNextValues = false;

    ResultSetValuesGetter(TableInfo tableInfo, ResultSet underlying, QueryDialect queryDialect) {
        this.tableInfo = tableInfo;
        this.underlying = underlying;
        valueGetters = initValueGetters(queryDialect);
    }

    public boolean next() throws SQLException {
        if (nextValues == null) {
            return underlying.next();
        } else {
            if (onNextValues) {
                onNextValues = false;
                nextValues = null;
                return next();
            } else {
                onNextValues = true;
                return true;
            }
        }
    }

    public void previous() throws SQLException {
        nextValues = getValues();
        if (tableInfo.hasIncColumn()){
            nextIncValue = extractIncValue();
        }
        if (tableInfo.hasTimeColumns()) {
            nextTimestampValue = extractTimestamp();
        }
        onNextValues = false;
    }

    public long extractIncValue() throws SQLException {
        if (onNextValues) {
            return nextIncValue;
        } else {
            return underlying.getLong(tableInfo.getIncColumn());
        }
    }

    public Timestamp extractTimestamp() throws SQLException {
        if (onNextValues) {
            return nextTimestampValue;
        } else {
            for (String timeColumn : tableInfo.getTimeColumns()) {
                var ts = underlying.getTimestamp(timeColumn);
                if (ts != null) {
                    return ts;
                }
            }
            throw new IllegalStateException("Every row must contain a timestamp");
        }
    }

    public String[] getValues() throws SQLException {
        if (nextValues != null) {
            return nextValues;
        } else {
            var result = new String[tableInfo.getColumnCount()];
            for (int i = 0; i < tableInfo.getColumnCount(); i++) {
                result[i] = valueGetters.get(i).apply(underlying, i + 1); // Column indices start at 1 (☉_☉)
            }
            return result;
        }
    }

    @Override
    public void close() throws Exception {
        underlying.close();
    }

    private List<ThrowingBiFunction<ResultSet, Integer, String, SQLException>> initValueGetters(QueryDialect queryDialect) {
        try {
            ResultSetMetaData md = underlying.getMetaData();
            int n = md.getColumnCount();
            List<ThrowingBiFunction<ResultSet, Integer, String, SQLException>> valueGetters = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                valueGetters.add(queryDialect.getStringValueGetter(md.getColumnType(i + 1)));
            }
            return valueGetters;
        } catch (SQLException e) {
            throw new RuntimeException("Error while retrieving table metadata", e);
        }
    }
}
