package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public class ResultSetInputStream extends InputStream {
    private TableInfo tableInfo;
    private final RowReader rowReader;

    private byte[] buffer;
    private int position;
    private boolean wroteHeader = false;
    private boolean closeStream;

    public ResultSetInputStream(TableInfo tableInfo, RowReader rowReader, boolean closeStream) {
        this.tableInfo = tableInfo;
        this.rowReader = rowReader;
        this.closeStream = closeStream;
    }

    private boolean ensureBuffer() throws SQLException, IOException {
        if (buffer != null && position < buffer.length) {
            return true;
        } else {
            if (rowReader.next()) {
                var byteArrayOutputStream = new ByteArrayOutputStream();
                if (!wroteHeader) {
                    writeHeader(byteArrayOutputStream);
                    wroteHeader = true;
                }
                String[] values = rowReader.getValues();
                for (int i = 0; i < values.length; i++) {
                    byteArrayOutputStream.write(values[i].getBytes());
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
        if (i < tableInfo.getColumnCount() - 1) {
            byteArrayOutputStream.write(',');
        } else {
            byteArrayOutputStream.write('\n');
        }
    }

    private void writeHeader(ByteArrayOutputStream byteArrayOutputStream) throws IOException {
        ColumnInfo[] columns = tableInfo.getColumns();
        for (int i = 0; i < columns.length; i++) {
            byteArrayOutputStream.write(columns[i].getName().getBytes());
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
            if (closeStream){
                rowReader.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
