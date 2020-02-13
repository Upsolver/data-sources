package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.*;
import com.upsolver.common.datasources.contenttypes.CSVContentType;
import com.upsolver.datasources.jdbc.metadata.TableInfo;
import com.upsolver.datasources.jdbc.querybuilders.DefaultQueryBuilder;
import com.upsolver.datasources.jdbc.querybuilders.QueryBuilder;
import com.upsolver.datasources.jdbc.utils.NamedPreparedStatment;

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

    private String identifierEscaper;
    private long readDelay;
    private TableInfo tableInfo;
    private QueryBuilder queryBuilder;
    private long dbTimezoneOffset;
    private long overallQueryTimeAdjustment;

    private Connection connection = null;

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
        String connectionString = properties.get(connectionStringProp);
        String userName = properties.get(userNameProp);
        String password = properties.get(passwordProp);
        queryBuilder = new DefaultQueryBuilder();

        try {
            readDelay = Long.parseLong(properties.getOrDefault(readDelayProp, "0"));
            connection = DriverManager.getConnection(connectionString, userName, password);
            DatabaseMetaData metadata = connection.getMetaData();
            identifierEscaper = metadata.getIdentifierQuoteString();
            String userProvidedIncColumn = properties.get(incrementingColumnNameProp);
            tableInfo = loadTableInfo(metadata, properties.get(tableNameProp));
            var allColumnNames = new HashSet<String>();
            var allColumns = new ArrayList<String>();
            if (userProvidedIncColumn != null) {
                tableInfo.setIncColumn(userProvidedIncColumn);
            }
            var columns = metadata.getColumns(tableInfo.getCatalog(), tableInfo.getSchema(), tableInfo.getName(), null);
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME");
                allColumnNames.add(colName.toUpperCase());
                allColumns.add(colName);
                if (tableInfo.getIncColumn() == null && isAutoInc(columns)) {
                    tableInfo.setIncColumn(colName);
                }
            }
            tableInfo.setColumns(allColumns.toArray(String[]::new));
            String[] filteredTimestampColumns =
                    Arrays.stream(properties.getOrDefault(timestampColumnsProp, "").split(","))
                            .filter(x -> allColumnNames.contains(x.toUpperCase()))
                            .toArray(String[]::new);
            if (filteredTimestampColumns.length != 0) {
                tableInfo.setTimeColumns(filteredTimestampColumns);
            }
            dbTimezoneOffset = getDbTimezoneOffset();
            overallQueryTimeAdjustment = dbTimezoneOffset - readDelay;
        } catch (Exception e) {
            throw new RuntimeException("Unable to connect to '" + connectionString + "'", e);
        }
    }

    private long getDbTimezoneOffset() throws SQLException {
        var offsetRS = queryBuilder.utcOffset(connection).executeQuery();
        offsetRS.next();
        return offsetRS.getTime(1).getSeconds();
    }

    private boolean isAutoInc(ResultSet columns) throws SQLException {
        return "yes".equalsIgnoreCase(columns.getString("IS_AUTOINCREMENT"));
    }

    private TableInfo loadTableInfo(DatabaseMetaData metadata, String tableName) throws SQLException {
        var tables = metadata.getTables(null, null, tableName, new String[]{"TABLE"});
        if (tables.next()) {
            return new TableInfo(tables.getString(1),
                    tables.getString(2),
                    tables.getString(3),
                    null,
                    null,
                    null);
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
        var result = queryData(sampleMetadata, 100);
        var rowReader = new RowReader(tableInfo, result, 100, null);
        var inputStream = new ResultSetInputStream(tableInfo, rowReader, true);
        var loadedData = new LoadedData(inputStream, Instant.now());
        return CompletableFuture.completedFuture(loadedData);
    }

    private Instant toQueryTime(Instant time) {
        return time.plusSeconds(overallQueryTimeAdjustment);
    }

    private Instant toUtc(Instant time) {
        return time.minusSeconds(dbTimezoneOffset);
    }

    private ResultSet queryData(JDBCTaskMetadata metadata, int limit) {
        try {
            if (tableInfo.hasTimeColumns()) {
                if (tableInfo.getIncColumn() != null) {
                    return queryBuilder.queryWithIncAndTime(tableInfo, metadata, limit, connection).executeQuery();
                } else {
                    throw new UnsupportedOperationException("TODO:");
                }
            } else {
                return queryBuilder.queryWithInc(tableInfo, metadata, limit, connection).executeQuery();
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
        try (var connection = DriverManager.getConnection(connectionString, user, pass)) {
            identifierEscaper = connection.getMetaData().getIdentifierQuoteString();
            return validateTableInfo(connection,
                    secureIdentifier(properties.get(tableNameProp)),
                    properties.get(incrementingColumnNameProp));
        } catch (SQLException e) {
            return Collections.singletonList(new PropertyError(connectionStringProp, "Unable to connect to database, please ensure connection string and login info is correct.\n" +
                    "SqlError: " + e.getMessage()));
        }

    }

    private Timestamp getCurrentTimestamp() throws SQLException {
        var rs = queryBuilder.getCurrentTimestamp(connection).executeQuery();
        rs.next();
        return rs.getTimestamp(1);
    }

    private List<PropertyError> validateTableInfo(Connection connection,
                                                  String tableName,
                                                  String incColumn) {
        var result = new ArrayList<PropertyError>();

        try {
            var columns = connection.getMetaData().getColumns(null, null, tableName, null);
            var columnNames = new HashMap<String, Boolean>();
            while (columns.next()) {
                columnNames.put(columns.getString("COLUMN_NAME"), isAutoInc(columns));
            }
            if (incColumn != null) {
                var autoInc = columnNames.get(incColumn.trim());
                if (autoInc == null) {
                    result.add(new PropertyError(incrementingColumnNameProp, "Could not find increment column " + incColumn));
                } else if (!autoInc) {
                    result.add(new PropertyError(incrementingColumnNameProp, "Column " + incColumn + " is not an auto-inc column"));
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table info", e);
        }
        return result;
    }


    @Override
    public CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> getDataLoaders(TaskInformation<JDBCTaskMetadata> taskInfo,
                                                                                  JDBCTaskMetadata previousSuccessful,
                                                                                  List<TaskRange> wantedRanges,
                                                                                  ShardDefinition shardDefinition) {
        var taskCount = wantedRanges.size();
        var itemsPerTask = (taskInfo.getMetadata().itemsPerTask(taskCount));
        var previous = previousSuccessful != null ? previousSuccessful : new JDBCTaskMetadata(0, 0);
        if (itemsPerTask == 0) {
            List<DataLoader<JDBCTaskMetadata>> result =
                    wantedRanges.stream()
                            .map(t -> new NoDataLoader(t, previous))
                            .collect(Collectors.toList());
            return CompletableFuture.completedFuture(result.iterator());
        } else {
            var resultSet = queryData(taskInfo.getMetadata()
                    .limitByPrevious(previous)
                    .adjustWithDelay(overallQueryTimeAdjustment), -1);
            if (tableInfo.hasTimeColumns()) {
                return splitDataByTimes(resultSet, previous, wantedRanges);
            } else {
                return splitDataByIncColumn(resultSet, previous, taskInfo, wantedRanges, itemsPerTask);
            }
        }
    }

    private CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> splitDataByTimes(ResultSet resultSet,
                                                                                     JDBCTaskMetadata previousSuccessful,
                                                                                     List<TaskRange> wantedRanges) {
        var result = new ArrayList<DataLoader<JDBCTaskMetadata>>();

        var previousRef = new AtomicReference<>(previousSuccessful);

        for (int i = 0; i < wantedRanges.size(); i++) {
            TaskRange taskRange = wantedRanges.get(i);
            final var closeStream = i == wantedRanges.size() - 1;

            var loader = new DataLoader<JDBCTaskMetadata>() {

                private final RowReader rowReader =
                        new RowReader(tableInfo, resultSet, -1, Timestamp.from(toQueryTime(taskRange.getExclusiveEndTime())));

                @Override
                public TaskRange getTaskRange() {
                    return taskRange;
                }

                @Override
                public Iterator<LoadedData> loadData() {
                    ResultSetInputStream inputStream = new ResultSetInputStream(tableInfo, rowReader, closeStream);
                    var result = new LoadedData(inputStream, new HashMap<>(), taskRange.getInclusiveStartTime());
                    return Collections.singleton(result).iterator();
                }

                @Override
                public JDBCTaskMetadata getCompletedMetadata() {
                    try {
                        var timestamp = rowReader.getLastTimestampValue();
                        var actualEndTime = previousRef.get().getEndTime();
                        if (timestamp != null) {
                            actualEndTime = toUtc(timestamp.toInstant());
                        }
                        var incMinValue = previousRef.get().getExclusiveEnd();
                        var incColumn = rowReader.getLastIncValue();
                        previousRef.set(new JDBCTaskMetadata(incMinValue,
                                Math.max(incColumn + 1, incMinValue),
                                actualEndTime,
                                actualEndTime));
                        return previousRef.get();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to read metadata from result set", e);
                    }
                }
            };
            result.add(loader);
        }
        return CompletableFuture.completedFuture(result.iterator());
    }


    private CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> splitDataByIncColumn(ResultSet resultSet,
                                                                                         JDBCTaskMetadata previousSuccessful,
                                                                                         TaskInformation<JDBCTaskMetadata> taskInfo,
                                                                                         List<TaskRange> wantedRanges,
                                                                                         double itemsPerTask) {
        var result = new ArrayList<DataLoader<JDBCTaskMetadata>>();
        var start = (double) Math.max(taskInfo.getMetadata().getInclusiveStart(), previousSuccessful.getExclusiveEnd());
        int taskCount = wantedRanges.size();
        for (int i = 0; i < taskCount; i++) {
            final var closeStream = i == taskCount - 1;
            final var taskRange = wantedRanges.get(i);
            var endValue = start + itemsPerTask;
            // Due to rounding of values make sure the last task gets everything remaining
            if (i == taskCount - 1) endValue = Math.max(endValue, taskInfo.getMetadata().getExclusiveEnd());
            final var metadata = new JDBCTaskMetadata((long) start, (long) endValue, Instant.MIN, Instant.MAX);
            start = endValue;
            var loader = new DataLoader<JDBCTaskMetadata>() {
                @Override
                public TaskRange getTaskRange() {
                    return taskRange;
                }

                private final RowReader rowReader =
                        new RowReader(tableInfo, resultSet, metadata.itemCount(), null);

                @Override
                public Iterator<LoadedData> loadData() {
                    ResultSetInputStream inputStream = new ResultSetInputStream(tableInfo, rowReader, closeStream);
                    var result = new LoadedData(inputStream, new HashMap<>(), taskRange.getInclusiveStartTime());
                    return Collections.singleton(result).iterator();
                }

                @Override
                public JDBCTaskMetadata getCompletedMetadata() {
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
        try (var statement = getTaskInfoQuery(previous, taskRange)) {
            var rs = statement.executeQuery();
            if (rs.next()) {
                var max = rs.getLong("max");
                var min = rs.getLong("min");
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

    private NamedPreparedStatment getTaskInfoQuery(JDBCTaskMetadata metadata, TaskRange taskRange) throws SQLException {
        if (tableInfo.hasTimeColumns()) {
            if (tableInfo.getIncColumn() != null) {
                return queryBuilder.taskInfoQueryIncAndTime(tableInfo,
                        metadata,
                        toQueryTime(taskRange.getExclusiveEndTime()),
                        connection);
            } else {
                throw new UnsupportedOperationException("TODO:...");
            }
        } else {
            return queryBuilder.taskInfoQueryInc(tableInfo, metadata, connection);
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

    private String secureIdentifier(String identifier) {
        return identifierEscaper + identifier.replace(identifierEscaper, identifierEscaper + identifierEscaper) + identifierEscaper;
    }

}

