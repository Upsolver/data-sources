package com.upsolver.datasources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetIterator implements Iterator<byte[]> {
    private ResultSet rs;
    private int colCount;

    private boolean didNext = false;
    private boolean hasNext = false;

    public ResultSetIterator(ResultSet rs) throws SQLException {
        this.rs = rs;
        this.colCount = rs.getMetaData().getColumnCount();
    }


    @Override
    public boolean hasNext() {
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
