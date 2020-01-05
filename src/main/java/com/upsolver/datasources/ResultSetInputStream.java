package com.upsolver.datasources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetInputStream extends InputStream {
    private ResultSet rs;
    private int colCount;

    private byte[] buffer;
    private int position;

    public ResultSetInputStream(ResultSet rs) throws SQLException {
        this.rs = rs;
        this.colCount = rs.getMetaData().getColumnCount();
    }

    @Override
    public int read() throws IOException {
        try {
            if (buffer == null) {
                if (rs.next()) {
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
                    position = 0;
                    buffer = byteArrayOutputStream.toByteArray();
                } else {
                    return -1;
                }
            }
            if (position < buffer.length) {
                return buffer[position++];
            } else {
                buffer = null;
                return read();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            rs.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
