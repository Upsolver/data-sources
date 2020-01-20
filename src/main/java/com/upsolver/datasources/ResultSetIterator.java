package com.upsolver.datasources;

import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetIterator implements Iterator<byte[]> {
    private ResultSet rs;
    private int colCount;
    private String header;

    private boolean didNext = false;
    private boolean hasNext = false;

    public ResultSetIterator(String header, ResultSet rs) throws SQLException {
        this.header = header;
        this.rs = rs;
        this.colCount = rs.getMetaData().getColumnCount();
    }


    @Override
    public boolean hasNext() {
        if (header != null) return true;
        try {
            if (!didNext) {
                hasNext = rs.next();
                if (!hasNext) {
                    rs.close();
                }
                didNext = true;
            }
            return hasNext;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] next() {
        try {
            if (header != null) {
                var byteArrayOutputStream = new ByteArrayOutputStream();
                byteArrayOutputStream.write(header.getBytes());
                byteArrayOutputStream.write('\n');
                header = null;
                return byteArrayOutputStream.toByteArray();
            }
            if (!didNext) {
                rs.next();
            }
            didNext = false;
            var byteArrayOutputStream = new ByteArrayOutputStream();
            for (int i = 0; i < colCount; i++) {
                byteArrayOutputStream.write(rs.getString(i + 1).getBytes()); // Column indices start at 1 (☉_☉)
                if (i < colCount - 1) {
                    byteArrayOutputStream.write(',');
                } else {
                    byteArrayOutputStream.write('\n');
                }
            }
            byteArrayOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
