package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.*;
import com.upsolver.common.datasources.contenttypes.CSVContentType;
import com.upsolver.datasources.jdbc.metadata.ColumnInfo;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.querybuilders.QueryDialect;
import com.upsolver.datasources.jdbc.querybuilders.QueryDialectProvider;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class JDBCDataSource implements ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> {

    private static final String connectionStringProp = "Connection String";
    private static final String tableNameProp = "Table Name";
    private static final String incrementingColumnNameProp = "Incrementing Column";
    private static final String timestampColumnsProp = "Timestamp Columns";
    private static final String readDelayProp = "Read Delay";
    private static final String userNameProp = "User Name";
    private static final String passwordProp = "Password";

    private long readDelay;
    private TableInfo tableInfo;
    private QueryDialect queryDialect;
    private long dbTimezoneOffset;
    private long overallQueryTimeAdjustment;


    private HikariDataSource ds = null;


    @Override
    public DataSourceDescription getDataSourceDescription() {
        return new JDBCDataSourceDescription();
    }

    @Override
    public int getMaxShards() {
        return 1;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        ds = new HikariDataSource();
        String connectionString = properties.get(connectionStringProp);
        ds.setJdbcUrl(connectionString);
        ds.setUsername(properties.get(userNameProp));
        ds.setPassword(properties.get(passwordProp));

        try (Connection con = getConnection()) {
            readDelay = Long.parseLong(properties.getOrDefault(readDelayProp, "0"));
            queryDialect = QueryDialectProvider.forConnection(connectionString);
            DatabaseMetaData metadata = con.getMetaData();
            String userProvidedIncColumn = properties.get(incrementingColumnNameProp);
            tableInfo = loadTableInfo(metadata, properties.get(tableNameProp));
            var allTimeColumns = new HashSet<String>();
            if (userProvidedIncColumn != null) {
                tableInfo.setIncColumn(userProvidedIncColumn);
            }
            for (ColumnInfo column : tableInfo.getColumns()) {
                if (column.isTimeType()) {
                    allTimeColumns.add(column.getName().toUpperCase());
                } else if (tableInfo.getIncColumn() == null && column.isIncCol()) {
                    tableInfo.setIncColumn(column.getName());
                }
            }
            String[] filteredTimestampColumns =
                    Arrays.stream(properties.getOrDefault(timestampColumnsProp, "").split(","))
                            .map(String::trim)
                            .filter(x -> allTimeColumns.contains(x.toUpperCase()))
                            .toArray(String[]::new);
            if (filteredTimestampColumns.length != 0) {
                tableInfo.setTimeColumns(filteredTimestampColumns);
            }
            dbTimezoneOffset = queryDialect.utcOffset(con);
            overallQueryTimeAdjustment = dbTimezoneOffset - readDelay;
        } catch (Exception e) {
            throw new RuntimeException("Unable to connect to '" + connectionString + "'", e);
        }
    }


    private boolean isAutoInc(ResultSet columns) throws SQLException {
        return "yes".equalsIgnoreCase(columns.getString("IS_AUTOINCREMENT"));
    }

    private TableInfo loadTableInfo(DatabaseMetaData metadata, String tableName) throws SQLException {
        var tables = metadata.getTables(null, null, tableName, new String[]{"TABLE"});
        if (tables.next()) {
            var columns = new ArrayList<ColumnInfo>();
            String catalog = tables.getString(1);
            String schema = tables.getString(2);
            String dbTableName = tables.getString(3);

            var columnRs = metadata.getColumns(catalog, schema, dbTableName, null);
            while (columnRs.next()) {
                String colName = columnRs.getString("COLUMN_NAME");
                int type = columnRs.getInt("DATA_TYPE");
                var sqlType = JDBCType.valueOf(type);
                columns.add(new ColumnInfo(colName, sqlType, isAutoInc(columnRs)));
            }

            return new TableInfo(catalog, schema, dbTableName, columns.toArray(ColumnInfo[]::new));
        } else {
            throw new IllegalArgumentException("Could not find table with name: " + tableName);
        }

    }

    @Override
    public List<PropertyDescription> getPropertyDescriptions() {
        ArrayList<PropertyDescription> result = new ArrayList<>();
        result.add(new SimplePropertyDescription(connectionStringProp, "The connection string that will be used to connect to the database", false));
        result.add(new SimplePropertyDescription(userNameProp, "The user name to connect with", false));
        result.add(new SimplePropertyDescription(passwordProp, "The password to connect with", false, true));
        result.add(new SimplePropertyDescription(tableNameProp, "The name of the table to read from", false));
        result.add(new SimplePropertyDescription(incrementingColumnNameProp, "The name of the column which has an incrementing value to be used to load data sequentially", true));
        result.add(new SimplePropertyDescription(timestampColumnsProp, "Comma separated list of timestamp columns to use for loading new rows. The fist non-null value will be used. At least one of the values must not be null for each row", true));
        result.add(new SimplePropertyDescription(readDelayProp, "How long (in seconds) to wait before reading rows based on their timestamp. This allows waiting for all transactions of a certain timestamp to complete to avoid loading partial data. Default value is 0", true));
        return result;
    }

    @Override
    public DataSourceContentType getContentType() {
        return new CSVContentType(true, ',', null, null);
    }

    @Override
    public CompletionStage<LoadedData> getSample() {
        JDBCTaskMetadata sampleMetadata =
                new JDBCTaskMetadata(0L, Long.MAX_VALUE, Instant.EPOCH, toQueryTime(Instant.now()));
        Connection connection = getConnection();
        var result = queryData(sampleMetadata, 100, connection);
        var rowReader = new RowReader(tableInfo, new ResultSetValuesGetter(tableInfo, result), sampleMetadata, connection);
        var inputStream = new ResultSetInputStream(new CsvRowConverter(tableInfo), rowReader, true);
        var loadedData = new LoadedData(inputStream, Instant.now());
        return CompletableFuture.completedFuture(loadedData);
    }

    private Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get connection", e);
        }
    }

    private Instant toQueryTime(Instant time) {
        return time.plusSeconds(overallQueryTimeAdjustment);
    }

    private Instant toUtc(Instant time) {
        return time.minusSeconds(dbTimezoneOffset);
    }

    private ResultSet queryData(JDBCTaskMetadata metadata, int limit, Connection connection) {
        try {
            if (tableInfo.hasTimeColumns()) {
                if (tableInfo.getIncColumn() != null) {
                    return queryDialect.queryByIncAndTime(tableInfo, metadata, limit, connection).executeQuery();
                } else {
                    return this.queryDialect.queryByTime(this.tableInfo, metadata, limit, connection).executeQuery();
                }
            } else {
                return queryDialect.queryByInc(tableInfo, metadata, limit, connection).executeQuery();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
    }

    @Override
    public List<PropertyError> validate(Map<String, String> properties) {
        var connectionString = properties.get(connectionStringProp);
        var user = properties.get(userNameProp);
        var pass = properties.get(passwordProp);
        var timestampColString = properties.get(timestampColumnsProp);
        var timestampCols =
                timestampColString != null ?
                        Arrays.stream(timestampColString.split(",")).map(String::trim).toArray(String[]::new) : new String[0];
        try (var connection = DriverManager.getConnection(connectionString, user, pass)) {
            return validateTableInfo(connection,
                    properties.get(tableNameProp),
                    properties.get(incrementingColumnNameProp),
                    timestampCols);
        } catch (SQLException e) {
            return Collections.singletonList(new PropertyError(connectionStringProp, "Unable to connect to database, please ensure connection string and login info is correct.\n" +
                    "SqlError: " + e.getMessage()));
        }

    }

    private List<PropertyError> validateTableInfo(Connection connection,
                                                  String tableName,
                                                  String incColumn,
                                                  String[] timestampColumns) {
        var result = new ArrayList<PropertyError>();

        try {
            var connectionMetadata = connection.getMetaData();
            var tableInfo = loadTableInfo(connectionMetadata, tableName);
            if (incColumn != null) {
                var autoInc = tableInfo.getColumn(incColumn);
                if (autoInc == null) {
                    result.add(new PropertyError(incrementingColumnNameProp, "Could not find increment column " + incColumn));
                } else if (!autoInc.isIncCol()) {
                    result.add(new PropertyError(incrementingColumnNameProp, "Column " + incColumn + " is not an auto-inc column"));
                }
            }
            var foundTimeCol = false;
            for (String timestampColumn : timestampColumns) {
                var col = tableInfo.getColumn(timestampColumn);
                if (col != null) {
                    if (col.isTimeType()) {
                        foundTimeCol = true;
                    } else {
                        result.add(new PropertyError(timestampColumnsProp, "Column '" + timestampColumn + "' is not a timestamp columns"));
                    }
                }
            }
            if (timestampColumns.length > 0 && !foundTimeCol) {
                result.add(new PropertyError(timestampColumnsProp, "Non of the provided timestamp columns exist in the table"));
            }
            if (timestampColumns.length == 0) {
                if (Arrays.stream(tableInfo.getColumns()).noneMatch(ColumnInfo::isIncCol)) {
                    result.add(new PropertyError(timestampColumnsProp,
                            "The table has no auto-incrementing column, you must provide update time columns to use"));
                }
            }
        } catch (IllegalArgumentException e) {
            result.add(new PropertyError(tableNameProp, "Could load table with name: '" + tableName + "'. " + e.getMessage()));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table info", e);
        }

        return result;
    }


    @Override
    public CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> getDataLoaders(TaskInformation<JDBCTaskMetadata> taskInfo,
                                                                                  List<TaskRange> completedRanges,
                                                                                  List<TaskRange> wantedRanges,
                                                                                  ShardDefinition shardDefinition) {
        var taskCount = completedRanges.size() + wantedRanges.size();
        var itemsPerTask = (taskInfo.getMetadata().itemsPerTask(taskCount));
        if (!tableInfo.hasTimeColumns() && itemsPerTask == 0) {
            List<DataLoader<JDBCTaskMetadata>> result =
                    wantedRanges.stream().map(t -> new NoDataLoader(t, taskInfo.getMetadata())).collect(Collectors.toList());
            return CompletableFuture.completedFuture(result.iterator());
        } else {

            var runMetadatas = getRunMetadatas(taskInfo, taskCount, itemsPerTask, wantedRanges);
            var firstMetadata = runMetadatas.get(0);
            var lastMetadata = runMetadatas.get(runMetadatas.size() - 1);
            var queryMetadata = new JDBCTaskMetadata(firstMetadata.getInclusiveStart(), lastMetadata.getExclusiveEnd(),
                    firstMetadata.getStartTime(), lastMetadata.getEndTime())
                    .adjustWithDelay(dbTimezoneOffset);
            var connection = getConnection();
            var resultSet = queryData(queryMetadata, -1, connection);
            return splitData(resultSet, wantedRanges, runMetadatas, connection);
        }
    }

    private List<JDBCTaskMetadata> getRunMetadatas(TaskInformation<JDBCTaskMetadata> taskInfo,
                                                   int taskCount,
                                                   double itemsPerTask,
                                                   List<TaskRange> wantedRanges) {
        var result = new ArrayList<JDBCTaskMetadata>();
        int wantedSize = wantedRanges.size();
        var wantedIndexStart = taskCount - wantedSize;
        if (tableInfo.hasTimeColumns()) {
            for (int i = 0; i < wantedSize; i++) {
                var firstInBatch = i == 0 && taskCount == wantedSize;
                TaskRange wantedRange = wantedRanges.get(i);
                // First task does not have lower bound to ensure we don't skip data from the last point we stopped at
                var startTime =
                        firstInBatch ? taskInfo.getMetadata().getStartTime() : wantedRange.getInclusiveStartTime().minusSeconds(readDelay);
                var metadata = new JDBCTaskMetadata(taskInfo.getMetadata().getInclusiveStart(),
                        taskInfo.getMetadata().getExclusiveEnd(),
                        startTime,
                        wantedRange.getExclusiveEndTime().minusSeconds(readDelay));
                result.add(metadata);
            }
        } else {
            var start = (double) taskInfo.getMetadata().getInclusiveStart();
            // Make sure to iterate the full task count and not just wantedRanges.size() to avoid rounding error differences
            // between executions with different amounts of wantedRanges
            for (int i = 0; i < taskCount; i++) {
                var endValue = start + itemsPerTask;
                // Due to rounding of values make sure the last task gets everything remaining
                if (i == taskCount - 1) endValue = Math.max(endValue, taskInfo.getMetadata().getExclusiveEnd());
                var metadata = new JDBCTaskMetadata((long) start, (long) endValue, Instant.MIN, JDBCTaskMetadata.initalEndTime);
                if (i >= wantedIndexStart) {
                    result.add(metadata);
                }
                start = endValue;
            }
        }
        return result;
    }


    private CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> splitData(ResultSet resultSet,
                                                                              List<TaskRange> wantedRanges,
                                                                              List<JDBCTaskMetadata> runMetadatas,
                                                                              Connection connection) {
        var result = new ArrayList<DataLoader<JDBCTaskMetadata>>();
        var lastReadIncValue = new AtomicReference<>(runMetadatas.get(0).getInclusiveStart());
        var lastReadTime = new AtomicReference<>(runMetadatas.get(0).getStartTime());

        // Value getter + Some of the code in RowReader are needed only because we insist on running a single query
        // and using a single result set for all ranges. If we allow query per window a lot of the code can be simplified.
        var valueGetter = new ResultSetValuesGetter(tableInfo, resultSet);

        for (int i = 0; i < wantedRanges.size(); i++) {
            final var isLast = i == wantedRanges.size() - 1;
            final var taskRange = wantedRanges.get(i);
            final var metadata = runMetadatas.get(i);
            var loader = new DataLoader<JDBCTaskMetadata>() {
                @Override
                public TaskRange getTaskRange() {
                    return taskRange;
                }

                private final RowReader rowReader = new RowReader(tableInfo, valueGetter, metadata, connection);

                @Override
                public Iterator<LoadedData> loadData() {
                    ResultSetInputStream inputStream = new ResultSetInputStream(new CsvRowConverter(tableInfo), rowReader, isLast);
                    var result = new LoadedData(inputStream, new HashMap<>(), taskRange.getInclusiveStartTime());
                    return Collections.singleton(result).iterator();
                }

                @Override
                public JDBCTaskMetadata getCompletedMetadata() {
                    if (tableInfo.hasTimeColumns() && rowReader.readValues()) {
                        if (rowReader.readValues()) {
                            // If some data was successfully read then that's our next start point
                            lastReadTime.set(toUtc(rowReader.getLastTimestampValue().toInstant()));
                            lastReadIncValue.set(rowReader.getLastIncValue());
                        }
                        metadata.setExclusiveEnd(lastReadIncValue.get() + 1);
                        metadata.setEndTime(lastReadTime.get());
                    }

                    return metadata;
                }
            };
            result.add(loader);

        }
        return CompletableFuture.completedFuture(result.iterator());

    }


    @Override
    public CompletionStage<TaskInformation<JDBCTaskMetadata>> getTaskInfo(JDBCTaskMetadata previousTaskMetadata,
                                                                          TaskRange taskRange,
                                                                          ShardDefinition shardDefinition) {
        var previous = previousTaskMetadata != null ?
                previousTaskMetadata : new JDBCTaskMetadata(0, 0);
        var startFrom = previous.getExclusiveEnd();
        try (var connection = getConnection(); var statement = getTaskInfoQuery(previous, taskRange, connection)) {
            var rs = statement.executeQuery();
            if (rs.next()) {
                var max = tableInfo.hasIncColumn() ? rs.getLong("max") : 0;
                var min = tableInfo.hasIncColumn() ? rs.getLong("min") : 0;
                var endTime = tableInfo.hasTimeColumns() ? taskRange.getExclusiveEndTime() : null;
                return CompletableFuture.completedFuture(new TaskInformation<>(taskRange,
                        new JDBCTaskMetadata(min, max + 1, previous.getEndTime(), endTime)));
            } else {
                return CompletableFuture.completedFuture(new TaskInformation<>(taskRange,
                        new JDBCTaskMetadata(startFrom, startFrom, previous.getEndTime(), previous.getEndTime())));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get task infos", e);
        }
    }

    private NamedPreparedStatment getTaskInfoQuery(JDBCTaskMetadata metadata,
                                                   TaskRange taskRange,
                                                   Connection connection) throws SQLException {
        if (tableInfo.hasTimeColumns()) {
            Instant maxTime = toQueryTime(taskRange.getExclusiveEndTime());
            if (tableInfo.getIncColumn() != null) {
                return queryDialect.taskInfoByIncAndTime(tableInfo, metadata, maxTime, connection);
            } else {
                return queryDialect.taskInfoByTime(tableInfo, metadata, maxTime, connection);
            }
        } else {
            return queryDialect.taskInfoByInc(tableInfo, metadata, connection);
        }
    }

    @Override
    public JDBCTaskMetadata reshard(List<JDBCTaskMetadata> previousTaskMetadatas,
                                    Instant taskTime,
                                    ShardDefinition newShard) {
        var endValue = previousTaskMetadatas.stream().mapToLong(JDBCTaskMetadata::getExclusiveEnd).max().orElse(-1L);
        var endTime = previousTaskMetadatas.stream().map(JDBCTaskMetadata::getEndTime)
                .max(Comparator.naturalOrder()).orElse(null);
        return new JDBCTaskMetadata(endValue, endValue, endTime, endTime);
    }

}

