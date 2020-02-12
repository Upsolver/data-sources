package com.upsolver.datasources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetInputStream extends InputStream {
    private ResultSet rs;
    private ResultSetReadLimiter limiter;
    private int colCount;

    private byte[] buffer;
    private int position;
    private boolean wroteHeader = false;

    public ResultSetInputStream(ResultSet rs, int readLimit) {
        this.rs = rs;
        this.limiter = new ResultSetReadLimiter(rs, readLimit);
        try {
            this.colCount = rs.getMetaData().getColumnCount();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get column count. " + e.getMessage(), e);
        }
    }

    private boolean ensureBuffer() throws SQLException, IOException {
        if (buffer != null && position < buffer.length) {
            return true;
        } else {
            if (limiter.next()) {
                var byteArrayOutputStream = new ByteArrayOutputStream();
                if (!wroteHeader) {
                    writeHeader(byteArrayOutputStream);
                    wroteHeader = true;
                }
                for (int i = 0; i < colCount; i++) {
                    byteArrayOutputStream.write(rs.getString(i + 1).getBytes()); // Column indices start at 1 (☉_☉)
                    writeCommaOrNewLine(byteArrayOutputStream, i);
                }
                byteArrayOutputStream.close();
                position = 0;
                buffer = byteArrayOutputStream.toByteArray();
                return true;
            }
            return false;
        }
    }

    private void writeCommaOrNewLine(ByteArrayOutputStream byteArrayOutputStream, int i) {
        if (i < colCount - 1) {
            byteArrayOutputStream.write(',');
        } else {
            byteArrayOutputStream.write('\n');
        }
    }

    private void writeHeader(ByteArrayOutputStream byteArrayOutputStream) throws SQLException, IOException {
        ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < colCount; i++) {
            byteArrayOutputStream.write(metadata.getColumnName(i + 1).getBytes());
            writeCommaOrNewLine(byteArrayOutputStream, i);
        }
    }

    @Override
    public int read() throws IOException {
        try {
            if (ensureBuffer()) {
                return buffer[position++];
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            if (ensureBuffer()) {
                int toRead = Math.min(len, buffer.length - position);
                System.arraycopy(buffer, position, b, off, toRead);
                position += toRead;
                return toRead;
            }
            return -1;
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
