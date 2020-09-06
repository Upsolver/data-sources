package com.upsolver.datasources.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonRowConverter implements RowConverter {
    private final TableInfo tableInfo;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonRowConverter(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public Boolean hasHeader() {
        return false;
    }

    @Override
    public void writeHeader(OutputStream os) throws IOException {
        // no headers
    }

    @Override
    public void convertRow(Object[] values, OutputStream os) throws IOException {
        ColumnInfo[] columns = tableInfo.getColumns();
        int n = columns.length;
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            map.put(columns[i].getName(), values[i]);
        }
        mapper.writeValue(os, map);
    }
}
