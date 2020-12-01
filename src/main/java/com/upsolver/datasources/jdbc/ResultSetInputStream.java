package com.upsolver.datasources.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public class ResultSetInputStream extends InputStream {
    private RowConverter rowConverter;
    private final RowReader rowReader;

    private byte[] buffer;
    private int position;
    private boolean wroteHeader = false;

    // If we had an error we close the stream regardless of the passed closeStream value
    private boolean hadError = false;
    private final boolean closeStream;


    public ResultSetInputStream(RowConverter rowConverter, RowReader rowReader, boolean closeStream) {
        this.rowConverter = rowConverter;
        this.rowReader = rowReader;
        this.closeStream = closeStream;
    }

    private boolean ensureBuffer() throws SQLException, IOException {
        try {
            if (buffer != null && position < buffer.length) {
                return true;
            } else {
                if (rowReader.next()) {
                    var byteArrayOutputStream = new ByteArrayOutputStream();
                    if (!wroteHeader && rowConverter.hasHeader()) {
                        rowConverter.writeHeader(byteArrayOutputStream);
                        wroteHeader = true;
                    }
                    Object[] values = rowReader.getValues();
                    rowConverter.convertRow(values, byteArrayOutputStream);
                    byteArrayOutputStream.close();
                    position = 0;
                    buffer = byteArrayOutputStream.toByteArray();
                    return true;
                }
                return false;
            }
        } catch (Exception e) {
            hadError = true;
            throw e;
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
    public int available() throws IOException {
        try {
            if (ensureBuffer()) {
                return buffer.length - position;
            }
            return 0;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (hadError || closeStream) {
                rowReader.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

