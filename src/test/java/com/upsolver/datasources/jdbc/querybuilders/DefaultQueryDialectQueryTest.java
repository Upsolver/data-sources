package com.upsolver.datasources.jdbc.querybuilders;

import com.upsolver.datasources.jdbc.JDBCTaskMetadata;
import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultQueryDialectQueryTest {
    private static Connection conn;
    private static final String[][] data = {
            {"2020-09-29 09:00:00", "zero"},
            {"2020-09-29 09:01:00", "one"},
            {"2020-09-29 09:02:00", "two"},
            {"2020-09-29 09:03:00", "three"},
            {"2020-09-29 09:04:00", "four"},
            {"2020-09-29 09:05:00", "five"},
            {"2020-09-29 09:06:00", "six"},
            {"2020-09-29 09:07:00", "seven"},
            {"2020-09-29 09:08:00", "eight"},
            {"2020-09-29 09:09:00", "nine"},
    };
    private static final DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @BeforeClass
    public static void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test");
        conn.createStatement().execute("create table test(id bigint auto_increment, ts timestamp, text varchar(255))");

        for (String[] row : data) {
            conn.createStatement().execute(format("insert into test (ts, text) values ('%s', '%s')", row[0], row[1]));
        }
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        conn.close();
    }

    @Test
    public void getCurrentTimestamp() throws SQLException {
        long before = System.currentTimeMillis();
        ResultSet rs = new DefaultQueryDialect().getCurrentTimestamp(conn).executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getMetaData().getColumnCount());
        long actual = rs.getTimestamp(1).getTime();
        assertFalse(rs.next());

        long after = System.currentTimeMillis();
        assertTrue(format("Timestamp must be between %d and %d but was %d", before, after, actual), actual >= before && actual <= after);
    }


    @Test
    public void queryFullTableLimit0() throws SQLException {
        queryFullTable(1);
    }

    @Test
    public void queryFullTableLimit1() throws SQLException {
        queryFullTable(1);
    }

    @Test
    public void queryFullTableLimit2() throws SQLException {
        queryFullTable(2);
    }

    @Test
    public void queryFullTableLimit100() throws SQLException {
        queryFullTable(100);
    }

    @Test
    public void queryByInc2_5() throws SQLException {
        queryByInc(2, 5);
    }

    @Test
    public void queryByInc1_100() throws SQLException {
        queryByInc(1, 100);
    }

    @Test
    public void queryByTime() throws SQLException, ParseException {
        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        queryByTime(fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:06:00").toInstant(), Arrays.asList("three", "four", "five"));
    }


    @Test
    public void queryByIncAndTimeLimitByTime() throws SQLException, ParseException {
        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        queryByIncAndTime(0, 100, fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:06:00").toInstant(), Arrays.asList("three", "four", "five"));
    }

    @Test
    public void queryByIncAndTimeLimitByTimeAndInc() throws SQLException, ParseException {
        queryByIncAndTime(3, 100, fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:06:00").toInstant(), Arrays.asList("three", "four", "five"));
    }

    @Test
    public void taskInfoByInc() throws SQLException {
        taskInfoByInc(1, 5, new int[] {5});
        taskInfoByInc(7, 5, new int[] {5}); // start id is not used for the calculations
    }

    @Test
    public void taskInfoByIncTooBig() throws SQLException {
        taskInfoByInc(1, 11, new int[0]);
    }

    @Test
    public void taskInfoByIncAndTime() throws ParseException, SQLException {
        taskInfoByIncAndTime(1, 5, fmt.parse("2020-09-29 09:01:00").toInstant(), fmt.parse("2020-09-29 09:05:00").toInstant(), new int[] {3});
    }

    @Test
    public void taskInfoByIncAndTimeIncEq() throws ParseException, SQLException {
        taskInfoByIncAndTime(1, 4, fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:05:00").toInstant(), new int[] {4});
    }

    @Test
    public void taskInfoByIncAndTimeNoInfo() throws ParseException, SQLException {
        taskInfoByIncAndTime(4, 5, fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:03:00").toInstant(), new int[0]);
    }

    @Test
    public void taskInfoByTime() throws ParseException, SQLException {
        taskInfoByTime(fmt.parse("2020-09-29 09:01:00").toInstant(), fmt.parse("2020-09-29 09:05:00").toInstant(), new String[] {"2020-09-29 09:05:00"});
    }

    @Test
    public void taskInfoByTimeTimeTooBig() throws ParseException, SQLException {
        taskInfoByTime(fmt.parse("2020-09-29 09:01:00").toInstant(), fmt.parse("2020-09-29 21:05:00").toInstant(), new String[] {"2020-09-29 09:09:00"});
    }

    @Test
    public void taskInfoByTimeNoInfo() throws ParseException, SQLException {
        taskInfoByTime(fmt.parse("2020-09-29 09:03:00").toInstant(), fmt.parse("2020-09-29 09:03:00").toInstant(), new String[0]);
    }

    private void queryFullTable(int limit) throws SQLException {
        Collection<String> actual = read(new DefaultQueryDialect().queryFullTable(new TableInfo(null, null, "test", new ColumnInfo[0]), new JDBCTaskMetadata(0, 0), limit, conn).executeQuery(), "text");
        Collection<String> expected = Arrays.stream(data).limit(limit).map(p -> p[1]).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    private void queryByInc(int start, int end) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setIncColumn("id");
        Collection<String> actual = read(new DefaultQueryDialect().queryByInc(ti, new JDBCTaskMetadata(start, end), 100, conn).executeQuery(), "text");
        Collection<String> expected = Arrays.stream(data).skip(start - 1).limit(end - start).map(p -> p[1]).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    private void queryByTime(Instant startTime, Instant endTime, Collection<String> expected) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setTimeColumns(new String[] {"ts"});
        Collection<String> actual = read(new DefaultQueryDialect().queryByTime(ti, new JDBCTaskMetadata(0, 0, startTime, endTime), 100, conn).executeQuery(), "text");
        assertEquals(expected, actual);
    }

    private void queryByIncAndTime(int start, int end, Instant startTime, Instant endTime, Collection<String> expected) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setIncColumn("id");
        ti.setTimeColumns(new String[] {"ts"});
        Collection<String> actual = read(new DefaultQueryDialect().queryByIncAndTime(ti, new JDBCTaskMetadata(start, end, startTime, endTime), 100, conn).executeQuery(), "text");
        assertEquals(expected, actual);
    }

    private void taskInfoByInc(int start, int end, int[] expected) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setIncColumn("id");
        int[] actual = read(new DefaultQueryDialect().taskInfoByInc(ti, new JDBCTaskMetadata(start, end), conn).executeQuery(), 1).stream().mapToInt(Integer::parseInt).toArray();
        assertArrayEquals(expected, actual);
    }

    private void taskInfoByTime(Instant endTime, Instant maxTime, String[] expected) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setTimeColumns(new String[] {"ts"});
        String[] actual = read(new DefaultQueryDialect().taskInfoByTime(ti, new JDBCTaskMetadata(0, 0, null, endTime), maxTime, conn).executeQuery(), 1).toArray(new String[0]);
        assertArrayEquals(expected, actual);
    }

    private void taskInfoByIncAndTime(int start, int end, Instant endTime, Instant maxTime, int[] expected) throws SQLException {
        TableInfo ti = new TableInfo(null, null, "test", new ColumnInfo[0]);
        ti.setIncColumn("id");
        ti.setTimeColumns(new String[] {"ts"});
        int[] actual = read(new DefaultQueryDialect().taskInfoByIncAndTime(ti, new JDBCTaskMetadata(start, end, null, endTime), maxTime, conn).executeQuery(), 1).stream().mapToInt(Integer::parseInt).toArray();
        assertArrayEquals(expected, actual);
    }
    // "taskInfoByInc", "taskInfoByTime", "taskInfoByIncAndTime"

    private Collection<String> read(ResultSet rs, String name) throws SQLException {
        Collection<String> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getString(name));
        }
        return result;
    }

    private Collection<String> read(ResultSet rs, int index) throws SQLException {
        Collection<String> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getString(index));
        }
        return result;
    }
}
