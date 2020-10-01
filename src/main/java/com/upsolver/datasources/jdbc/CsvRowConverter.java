package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.io.IOException;
import java.io.OutputStream;

class CsvRowConverter implements RowConverter {

    private final TableInfo tableInfo;

    CsvRowConverter(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public Boolean hasHeader() {
        return true;
    }


    @Override
    public void convertRow(Object[] values, OutputStream os) throws IOException {
        for (int i = 0; i < values.length; i++) {
            var value = getEscapedValue(values, i);
            os.write(value.getBytes());
            writeCommaOrNewLine(os, i);
        }
    }

    private String getEscapedValue(Object[] values, int i) {
        var v =  values[i] != null ? values[i].toString() : "";
        if (v.contains(",")) {
            return '"' + v.replaceAll("\"", "\"\"") + '"';
        } else {
            return v;
        }

    }

    private void writeCommaOrNewLine(OutputStream byteArrayOutputStream, int i) throws IOException {
        if (i < tableInfo.getColumnCount() - 1) {
            byteArrayOutputStream.write(',');
        } else {
            byteArrayOutputStream.write('\n');
        }
    }

    @Override
    public void writeHeader(OutputStream os) throws IOException {
        ColumnInfo[] columns = tableInfo.getColumns();
        for (int i = 0; i < columns.length; i++) {
            os.write(columns[i].getName().getBytes());
            writeCommaOrNewLine(os, i);
        }

    }
}
