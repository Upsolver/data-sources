package com.upsolver.datasources.jdbc.metadata;

public class TableInfo {
    private String catalog;
    private String schema;
    private String name;

    private String incColumn;
    private String[] allColumns;
    private String[] timeColumns;

    public TableInfo(String catalog, String schema, String name, String incColumn, String[] allColumns, String[] timeColumns) {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
        this.incColumn = incColumn;
        this.allColumns = allColumns;
        this.timeColumns = timeColumns;
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

    public void setColumns(String[] columns) {
        allColumns = columns;
    }

    public String getTimeColumnsAsString() {
        return String.join(",", timeColumns);
    }

    public String[] getTimeColumns() {
        return timeColumns;
    }

    public int getColumnCount() {
        return allColumns.length;
    }

    public String[] getColumns() {
        return allColumns;
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
