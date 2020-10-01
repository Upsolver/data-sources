package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.ExternalDataSource;
import com.upsolver.common.datasources.PropertyError;
import com.upsolver.common.datasources.ShardDefinition;
import com.upsolver.common.datasources.TaskRange;
import com.upsolver.common.datasources.contenttypes.CSVContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.upsolver.datasources.jdbc.JDBCDataSource.connectionStringProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.fullLoadIntervalProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.passwordProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.tableNameProp;
import static com.upsolver.datasources.jdbc.JDBCDataSource.userNameProp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JDBCDataSourceTest {
    private static Connection conn;

    @BeforeClass
    public static void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test");
        conn.createStatement().execute("create table TEST(ID bigint auto_increment, TS timestamp, TEXT varchar(255))");

//        for (String[] row : data) {
//            conn.createStatement().execute(format("insert into test (ts, text) values ('%s', '%s')", row[0], row[1]));
//        }
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
        props.put(tableNameProp, "TEST");
        validate(props, new String[0]);
    }

    @Test
    public void startTimeNoFullLoadInterval() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST");
        ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> ds = new JDBCDataSource();
        ds.setProperties(props);
        assertNull(ds.getStartTime());
    }

    @Test
    public void startTimeWithFullLoadInterval() {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST");
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
    public void taskInfoSmallStartId() {
        JDBCTaskMetadata md = new JDBCTaskMetadata(0L, 0L, Instant.EPOCH, Instant.now());
        taskInfo(md, new TaskRange(Instant.EPOCH, Instant.now()), md);
    }

    @Test
    public void taskInfo() {
        JDBCTaskMetadata md = new JDBCTaskMetadata(1, 100, Instant.EPOCH, Instant.now());
        taskInfo(md, new TaskRange(Instant.EPOCH, Instant.now()), md);
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

    public void taskInfo(JDBCTaskMetadata md, TaskRange tr, JDBCTaskMetadata expected) {
        Map<String, String> props = new HashMap<>(props("jdbc:h2:mem:test", "", ""));
        props.put(tableNameProp, "TEST");
        props.put(fullLoadIntervalProp, "3");
        ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> ds = new JDBCDataSource();
        ds.setProperties(props);
        ShardDefinition sd = new ShardDefinition(1, 1);

        ds.getTaskInfo(md, tr, sd);
        ds.getTaskInfo(md, tr, sd).thenAccept(ti -> {
            assertSame(tr, ti.getTaskRange());
            JDBCTaskMetadata md2 = ti.getMetadata();
            assertEquals(expected.getEndTime(), md2.getStartTime());
            assertEquals(expected.getEndTime(), md2.getEndTime());
        });
    }

}