package com.upsolver.datasources.jdbc.metadata;

import java.sql.JDBCType;

public class ColumnInfo {
    private String name;
    private JDBCType type;
    private boolean isIncCol;


    public ColumnInfo(String name, JDBCType type, boolean isIncCol) {
        this.name = name;
        this.type = type;
        this.isIncCol = isIncCol;
    }

    public String getName() {
        return name;
    }

    public JDBCType getType() {
        return type;
    }

    public boolean isTimeType() {
        return type == JDBCType.DATE ||
                type == JDBCType.TIMESTAMP ||
                type == JDBCType.TIME ||
                type == JDBCType.TIME_WITH_TIMEZONE ||
                type == JDBCType.TIMESTAMP_WITH_TIMEZONE;
    }

    public boolean isIncCol() {
        return isIncCol;
    }
}
