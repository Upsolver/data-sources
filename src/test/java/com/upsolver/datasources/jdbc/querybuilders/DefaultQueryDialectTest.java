package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;
import org.junit.Test;
import org.mockito.Mockito;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultQueryDialectTest {
    private final Connection conn = mock(Connection.class);
    private final PreparedStatement ps = mock(PreparedStatement.class);

    @Test
    public void taskInfoByInc() throws SQLException {
        int end = 123;
        JDBCTaskMetadata md = new JDBCTaskMetadata(0, end, Instant.now(), Instant.now());
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT MIN(id) AS MIN, MAX(id) AS MAX FROM schema.table WHERE id >= ? HAVING MIN( id) IS NOT NULL");
        NamedPreparedStatment nps = new DefaultQueryDialect().taskInfoByInc(tableInfo, md, conn);
        assertNotNull(nps);
        verify(ps).setLong(1, end);
        validateMockitoUsage();
    }

    @Test
    public void taskInfoByTime() throws SQLException {
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        JDBCTaskMetadata md = new JDBCTaskMetadata(0, 0, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT MAX(ts) AS last_time FROM schema.table WHERE ts < ? AND ts > ? HAVING MAX( ts) IS NOT NULL");
        NamedPreparedStatment nps = new DefaultQueryDialect().taskInfoByTime(tableInfo, md, now, conn);
        assertNotNull(nps);

        verify(ps).setTimestamp(1, Timestamp.from(now));
        verify(ps).setTimestamp(2, Timestamp.from(endTime));
        validateMockitoUsage();
    }

    @Test
    public void taskInfoByIncAndTime() throws SQLException {
        int end = 456;
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        JDBCTaskMetadata md = new JDBCTaskMetadata(0, end, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT MIN(id) AS MIN, MAX(id) AS MAX, MAX(ts) AS last_time FROM schema.table WHERE ts < ? AND ((ts = ? AND id >= ?) OR (ts > ?)) HAVING MIN( id) IS NOT NULL");
        NamedPreparedStatment nps = new DefaultQueryDialect().taskInfoByIncAndTime(tableInfo, md, now, conn);
        assertNotNull(nps);

        verify(ps).setTimestamp(1, Timestamp.from(now));
        verify(ps).setTimestamp(2, Timestamp.from(endTime));
        verify(ps).setLong(3, end);
        validateMockitoUsage();
    }

    @Test
    public void queryByIncAndTime() throws SQLException {
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        int start = 432;
        int limit = 678;
        JDBCTaskMetadata md = new JDBCTaskMetadata(start, limit, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT  * FROM schema.table WHERE ts < ? AND ((ts = ? AND id >= ?) OR (ts > ?)) ORDER BY ts, id ASC limit " + limit);
        NamedPreparedStatment nps = new DefaultQueryDialect().queryByIncAndTime(tableInfo, md, limit, conn);
        assertNotNull(nps);

        verify(ps).setTimestamp(1, Timestamp.from(startTime));
        verify(ps).setTimestamp(2, Timestamp.from(startTime));
        verify(ps).setTimestamp(4, Timestamp.from(startTime));
        verify(ps).setLong(3, start);
        validateMockitoUsage();
    }

    @Test
    public void queryByTime() throws SQLException {
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        int start = 432;
        int limit = 678;
        JDBCTaskMetadata md = new JDBCTaskMetadata(start, limit, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT  * FROM schema.table WHERE ts > ? AND ts <= ? ORDER BY ts ASC limit " + limit);
        NamedPreparedStatment nps = new DefaultQueryDialect().queryByTime(tableInfo, md, limit, conn);
        assertNotNull(nps);

        verify(ps).setTimestamp(1, Timestamp.from(startTime));
        verify(ps).setTimestamp(2, Timestamp.from(startTime));
        validateMockitoUsage();
    }

    @Test
    public void queryByInc() throws SQLException {
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        int start = 123;
        int end = 456;
        int limit = 678;
        JDBCTaskMetadata md = new JDBCTaskMetadata(start, end, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT  * FROM schema.table WHERE id BETWEEN ? AND ? limit " + limit);
        NamedPreparedStatment nps = new DefaultQueryDialect().queryByInc(tableInfo, md, limit, conn);
        assertNotNull(nps);

        verify(ps).setLong(1, start);
        verify(ps).setLong(2, end - 1);
        validateMockitoUsage();
    }

    @Test
    public void queryFullTable() throws SQLException {
        Instant now = Instant.now();
        Instant startTime = now.minus(1, ChronoUnit.HOURS);
        Instant endTime = now.minus(1, ChronoUnit.HOURS);
        int start = 123;
        int end = 456;
        int limit = 678;
        JDBCTaskMetadata md = new JDBCTaskMetadata(start, end, startTime, endTime);
        TableInfo tableInfo = createTableInfo(conn, ps, "SELECT  * FROM schema.table limit " + limit);
        NamedPreparedStatment nps = new DefaultQueryDialect().queryFullTable(tableInfo, md, limit, conn);
        assertNotNull(nps);

        Mockito.verifyNoMoreInteractions(ps);
    }

    @Test
    public void getCurrentTimestamp() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement("SELECT CURRENT_TIMESTAMP")).thenReturn(ps);
        assertSame(ps, new DefaultQueryDialect().getCurrentTimestamp(conn));
    }

    /**
     * This test case verifies correctness of some methods implemented in {@code DefaultQueryDialect}.
     * The methods however are not final and can be overridden in subclasses. This test verifies that no-one of these
     * methods is overridden in any subclass. This guarantees that even if such method is overridden on one of the
     * subclasses the relevant test must be implemented.
     *
     * Actually the methods can be marked as final, so their re-implementation will be prevented by compiler. However
     * final modifier may be added by human and may be removed by human. This test will help to avoid going forward
     * without appropriate tests of such overridden method.
     */
    @Test
    public void checkNoOtherImplementations() {
        Reflections reflections = new Reflections(QueryDialect.class.getPackageName());
        Set<Class<? extends DefaultQueryDialect>> dialects = reflections.getSubTypesOf(DefaultQueryDialect.class);

        Set<String> names = new HashSet<>(Arrays.asList("taskInfoByInc", "taskInfoByTime", "taskInfoByIncAndTime", "queryByIncAndTime", "queryByTime", "queryByInc", "queryFullTable", "getCurrentTimestamp"));
        for (Class<? extends QueryDialect> dialect : dialects) {
            assertEquals(dialect.getName(), Collections.emptyList(), Arrays.stream(dialect.getDeclaredMethods()).map(Method::getName).filter(names::contains).collect(Collectors.toList()));
        }
    }

    private TableInfo createTableInfo(Connection conn, PreparedStatement ps, String expectedQuery) throws SQLException {
        when(conn.prepareStatement(expectedQuery)).thenReturn(ps);
        TableInfo tableInfo = new TableInfo("catalog", "schema", "table", new ColumnInfo[0]);
        tableInfo.setIncColumn("id");
        tableInfo.setTimeColumns(new String[] {"ts"});

        return tableInfo;
    }
}