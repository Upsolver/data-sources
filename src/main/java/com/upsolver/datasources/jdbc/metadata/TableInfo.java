package com.upsolver.datasources.jdbc.metadata;

public class TableInfo {
    private String catalog;
    private String schema;
    private String name;

    public TableInfo(String catalog, String schema, String name) {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
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
}
