package com.upsolver.datasources.jdbc;

import java.io.IOException;
import java.io.OutputStream;

interface RowConverter {
    Boolean hasHeader();

    void writeHeader(OutputStream os) throws IOException;

    void convertRow(Object[] values, OutputStream os) throws IOException;
}
