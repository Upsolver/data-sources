package com.upsolver.datasources.jdbc.metadata;

import java.util.Arrays;

public class TableInfo {
    private String catalog;
    private String schema;
    private String name;

    private String incColumn;
    private ColumnInfo[] columns;
    private String[] timeColumns;

    public TableInfo(String catalog, String schema, String name,
                     ColumnInfo[] columns) {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "catalog='" + catalog + '\'' +
                ", schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                ", incColumn='" + incColumn + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", timeColumns=" + Arrays.toString(timeColumns) +
                '}';
    }

    public String getSchema() {
        return schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getName() {
        return name;
    }

    public String getIncColumn() {
        return incColumn;
    }

    public void setIncColumn(String incColumn) {
        this.incColumn = incColumn;
    }

    public String getTimeColumnsAsString() {
        return String.join(",", timeColumns);
    }

    public String[] getTimeColumns() {
        return timeColumns;
    }

    public int getColumnCount() {
        return columns.length;
    }

    public ColumnInfo[] getColumns() { return columns; }

    public ColumnInfo getColumn(String name) {
        return Arrays.stream(columns).filter(x -> x.getName().equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    public boolean hasTimeColumns() {
        return timeColumns != null;
    }

    public boolean hasIncColumn() {
        return incColumn != null;
    }

    public void setTimeColumns(String[] timeColumns) {
        this.timeColumns = timeColumns;
    }
}

