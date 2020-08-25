package com.upsolver.datasources.jdbc.metadata;

import java.sql.SQLType;

public class ColumnInfo {
    private String name;
    private SQLType type;
    private boolean isIncCol;
    private boolean isTimeType;


    public ColumnInfo(String name, SQLType type, boolean isIncCol, boolean isTimeType) {
        this.name = name;
        this.type = type;
        this.isIncCol = isIncCol;
        this.isTimeType = isTimeType;
    }

    public String getName() {
        return name;
    }

    public SQLType getType() {
        return type;
    }

    public boolean isTimeType() {
        return isTimeType;
    }

    public boolean isIncCol() {
        return isIncCol;
    }
}
