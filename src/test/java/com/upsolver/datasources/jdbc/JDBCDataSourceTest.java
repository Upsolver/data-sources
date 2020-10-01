package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.ExternalDataSource;
import com.upsolver.common.datasources.LoadedData;
import com.upsolver.common.datasources.PropertyError;
import com.upsolver.common.datasources.ShardDefinition;
import com.upsolver.common.datasources.TaskRange;
import com.upsolver.common.datasources.contenttypes.CSVContentType;
import com.upsolver.datasources.jdbc.utils.DataUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.upsolver.datasources.jdbc.JDBCDataSource.connectionStringProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.fullLoadIntervalProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.passwordProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.tableNameProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.timestampColumnsProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.userNameProp;
import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JDBCDataSourceTest {
    private static Connection conn;
    private static final String[][] data = {
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
    private static final String[] allLabels = Arrays.stream(data).map(x -> x[1]).toArray(String[]::new);
    private static final DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @BeforeClass
    public static void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test");
        conn.createStatement().execute("create table TEST_ID_TS(ID bigint auto_increment, TS timestamp, TEXT varchar(255))");
        conn.createStatement().execute("create table TEST_TS(ID bigint, TS timestamp, TEXT varchar(255))");
        conn.createStatement().execute("create table TEST_EMPTY(ID bigint auto_increment, TS timestamp, TEXT varchar(255))");

        for (int i = 0; i < data.length; i++) {
            String[] row = data[i];
            int id = i + 1;
            conn.createStatement().execute(format("insert into TEST_ID_TS (TS, TEXT) values ('%s', '%s')", row[0], row[1]));
            conn.createStatement().execute(format("insert into TEST_TS (ID, TS, TEXT) values (%d, '%s', '%s')", id, row[0], row[1]));
        }

    }

    @AfterClass
    public static void cleanup() throws SQLException {
        conn.close();
    }

    @Test
    public void trivial() {
        JDBCDataSource ds = new JDBCDataSource();
        assertNotNull(ds.getDataSourceDescription());
        assertEquals(JDBCDataSourceDescription.class, ds.getDataSourceDescription().getClass());
        assertEquals(1, ds.getMaxShards());
        assertNull(ds.getStartTime());
        ds.getPropertyDescriptions().forEach(p -> {
            assertNotNull(p);
            assertNotNull(p.getName());
            assertNotNull(p.getDescription());
        });
        assertEquals(CSVContentType.class, ds.getContentType().getClass());
    }

    @Test
    public void validateNoProps() {
        validateBadConnectionString(null, "", "");
    }

    @Test
    public void validateEmptyConnectionString() {
        validateBadConnectionString("", "", "");
    }

    @Test
    public void validateWrongConnectionString() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:qqqqq", "", ""));
        props.put(tableNameProp, "test");
        validate(props, new String[] {connectionStringProp});
    }

    @Test
    public void validateNoSuitableDriver() {
        Map<String, String> props = new HashMap<>(props("jdbc:wrong://host", "", ""));
        props.put(tableNameProp, "test");
        validate(props, new String[] {connectionStringProp});
    }

    @Test
    public void validateNotTable() {
        validate(props("jdbc:h2:mem:test", "", ""), new String[] {tableNameProp});
    }

    @Test
    public void validateTableDoesNotExist() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "not_existing_table");
        validate(props, new String[] {tableNameProp});
    }

    public void validateBadConnectionString(String connectionString, String user, String password) {
        validate(props(connectionString, user, password), new String[] {connectionStringProp});
    }

    @Test
    public void validateMinimalGoodProps() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST_EMPTY");
        validate(props, new String[0]);
    }

    @Test
    public void startTimeNoFullLoadInterval() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST_EMPTY");
        ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> ds = new JDBCDataSource();
        ds.setProperties(props);
        assertNull(ds.getStartTime());
    }

    @Test
    public void startTimeWithFullLoadInterval() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST_EMPTY");
        props.put(fullLoadIntervalProp, "3");
        ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> ds = new JDBCDataSource();
        ds.setProperties(props);
        long before = System.currentTimeMillis();
        long start = ds.getStartTime().toEpochMilli();
        long after = System.currentTimeMillis();
        long time = start + 3 * 60 * 1000;
        assertTrue(time >= before && time <= after);
    }

    @Test
    public void taskInfoEmptyTable() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        assertEquals(0, loadData("TEST_EMPTY", null, 2,
                new JDBCTaskMetadata(0L, 0L, Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new JDBCTaskMetadata(0L, 0L, now, now)).length);
    }

    @Test
    public void taskInfoSmallStartId() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        String[] actual = loadData("TEST_ID_TS", null, 2,
                new JDBCTaskMetadata(0L, 0L, Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new JDBCTaskMetadata(0L, 0L, now, Instant.EPOCH));
        assertArrayEquals(allLabels, actual);
    }

    @Test
    public void taskInfo() throws ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        String[] actual = loadData("TEST_ID_TS", null, 2,
                new JDBCTaskMetadata(1, 5, Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new TaskRange(Instant.EPOCH, now),
                new JDBCTaskMetadata(1, 5, now, Instant.EPOCH));
        assertArrayEquals(allLabels, actual);
    }

    @Test
    public void taskInfoIdTsFullLoad() throws ParseException, ExecutionException, InterruptedException, IOException {
        Instant now = Instant.now();
        String[] actual = loadData("TEST_ID_TS", "TS", 2,
                new JDBCTaskMetadata(1, 5, fmt.parse("2020-09-29 09:07:00").toInstant(), now),
                new TaskRange(Instant.EPOCH, Instant.EPOCH),
                new TaskRange(fmt.parse("2020-09-29 09:02:00").toInstant(), fmt.parse("2020-09-29 09:09:00").toInstant()),
                new JDBCTaskMetadata(1, 5, now, now));
        assertArrayEquals(allLabels, actual);
    }

    private void validate(Map<String, String> props, String[] expectedErrors) {
        List<PropertyError> errors = new JDBCDataSource().validate(props);
        Set<String> expected = new HashSet<>(Arrays.asList(expectedErrors));
        Set<String> actual = errors.stream().map(PropertyError::getPropertyName).collect(Collectors.toSet());
        assertEquals(expected, actual);
    }

    private Map<String, String> props(String connectionString, String user, String password) {
        Map<String, String> props = new HashMap<>();
        props.put(connectionStringProp, connectionString);
        props.put(userNameProp, user);
        props.put(passwordProp, password);
        return props;
    }

    private String[] loadData(String table, String timestampColumns, Integer fulllLoadInterval, JDBCTaskMetadata md, TaskRange prevRange, TaskRange nextRange, JDBCTaskMetadata expected) throws ExecutionException, InterruptedException, IOException {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, table);
        if (fulllLoadInterval != null) {
            props.put(fullLoadIntervalProp, "" + fulllLoadInterval);
        }
        if (timestampColumns != null) {
            props.put(timestampColumnsProp, timestampColumns);
        }
        ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> ds = new JDBCDataSource();
        ds.setProperties(props);
        ShardDefinition sd = new ShardDefinition(1, 1);

        String res = ds.getTaskInfo(md, prevRange, sd).thenCompose(ti -> {
            assertSame(prevRange, ti.getTaskRange());
            JDBCTaskMetadata md2 = ti.getMetadata();
            assertEquals(expected.getStartTime(), md2.getStartTime());
            assertEquals(expected.getEndTime(), md2.getEndTime());
            //assertEquals(expected.getInclusiveStart(), md2.getInclusiveStart());
            //assertEquals(expected.getExclusiveEnd(), md2.getExclusiveEnd());


            String content;
            try {
                content = ds.getDataLoaders(ti, Collections.singletonList(ti.getTaskRange()), Collections.singletonList(nextRange), new ShardDefinition(1, 1)).thenCompose(it -> {
                    StringBuilder buf = new StringBuilder();
                    while (it.hasNext()) {
                        Iterator<LoadedData> itData = it.next().loadData();
                        while (itData.hasNext()) {
                            LoadedData data = itData.next();
                            try {
                                buf.append(new String(data.getInputStream().readAllBytes()));
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    }
                    return CompletableFuture.completedFuture(buf.toString());
                }).toCompletableFuture().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
            return CompletableFuture.completedFuture(content);
        }).toCompletableFuture().get();

        return DataUtil.read(new BufferedReader(new StringReader(res)), true, 2).stream().map(r -> r.iterator().next()).toArray(String[]::new);
    }

}