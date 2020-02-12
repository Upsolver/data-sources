package com.upsolver.datasources;

import com.upsolver.common.datasources.*;
import com.upsolver.common.datasources.contenttypes.CSVContentType;
import com.upsolver.datasources.jdbcutils.NamedPreparedStatment;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


public class JDBCDataSource implements ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> {

    private static final Set<Integer> validIncColumn = new HashSet<>(Arrays.asList(Types.BIGINT, Types.INTEGER));

    private static final String connectionStringProp = "Connection String";
    private static final String tableNameProp = "Table Name";
    private static final String incrementingColumnNameProp = "Incrementing Column";
    private static final String userNameProp = "User Name";
    private static final String passwordProp = "Password";

    private String connectionString;
    private String userName;
    private String password;
    private String tableName;
    private String incrementingColumn;
    private String identifierEscaper;


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
        connectionString = properties.get(connectionStringProp);

        userName = properties.get(userNameProp);
        password = properties.get(passwordProp);

        try {
            connection = DriverManager.getConnection(connectionString, userName, password);
            identifierEscaper = connection.getMetaData().getIdentifierQuoteString();
            tableName = secureIdentifier(properties.get(tableNameProp));
            incrementingColumn = secureIdentifier(properties.get(incrementingColumnNameProp));
        } catch (Exception e) {
            throw new RuntimeException("Unable to connect to '" + connectionString + "'", e);
        }
    }


    @Override
    public List<PropertyDescription> getPropertyDescriptions() {
        ArrayList<PropertyDescription> result = new ArrayList<>();
        result.add(new SimplePropertyDescription(connectionStringProp, "The connection string that will be used to connect to the database", false));
        result.add(new SimplePropertyDescription(userNameProp, "The user name to connect with", false));
        result.add(new SimplePropertyDescription(passwordProp, "The password to connect with", false, true));
        result.add(new SimplePropertyDescription(tableNameProp, "The name of the table to read from", false));
        result.add(new SimplePropertyDescription(incrementingColumnNameProp, "The name of the column which has an incrementing value to be used to load data sequentially", false));
        return result;
    }

    @Override
    public DataSourceContentType getContentType() {
        return new CSVContentType(true, ',', null, null);
    }

    @Override
    public CompletionStage<LoadedData> getSample() {
        var result = queryData(0L, 100000L, 100);
        var inputStream = new ResultSetInputStream(result, Integer.MAX_VALUE);
        var loadedData = new LoadedData(inputStream, Instant.now());
        return CompletableFuture.completedFuture(loadedData);
    }


    private ResultSet queryData(Long inclusiveStart, Long exclusiveEnd, int limit) {
        String query = "SELECT *" +
                " FROM " + tableName +
                " WHERE " + incrementingColumn + " BETWEEN :incStart AND :incEnd";
        if (limit > 0) {
            query = query + " limit " + limit;
        }
        try {
            var statement = new NamedPreparedStatment(connection, query);
            statement.setLong("incStart", inclusiveStart);
            statement.setLong("incEnd", exclusiveEnd - 1);
            return statement.executeQuery();
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

    private List<PropertyError> validateTableInfo(Connection connection,
                                                  String tableName,
                                                  String incColumn) {
        var result = new ArrayList<PropertyError>();
        String query = "SELECT * FROM " + tableName + " limit 0";
        try (var rs = new NamedPreparedStatment(connection, query).executeQuery()) {
            var metadata = rs.getMetaData();
            var columnNames = new HashMap<String, Integer>();
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnNames.put(metadata.getColumnName(i), i);
            }
            var incIndex = columnNames.get(incColumn.trim());
            if (incIndex == null) {
                result.add(new PropertyError(incrementingColumnNameProp, "Incrementing column '" + incColumn + "' does not exist"));
            } else if (!validIncColumn.contains(metadata.getColumnType(incIndex))) {
                result.add(new PropertyError(incrementingColumnNameProp, "Incrementing column must be of type INTEGER or BIGINT"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table info", e);
        }
        return result;
    }


    @Override
    public CompletionStage<Iterator<DataLoader<JDBCTaskMetadata>>> getDataLoaders(TaskInformation<JDBCTaskMetadata> taskInfo,
                                                                                  List<TaskRange> wantedRanges,
                                                                                  ShardDefinition shardDefinition) {
        var taskCount = wantedRanges.size();
        var itemsPerTask = (taskInfo.getMetadata().getItemsPerTask(taskCount));
        if (itemsPerTask == 0) {
            List<DataLoader<JDBCTaskMetadata>> result =
                    wantedRanges.stream()
                            .map(t -> new NoDataLoader(t, taskInfo.getMetadata()))
                            .collect(Collectors.toList());
            return CompletableFuture.completedFuture(result.iterator());
        } else {
            var result = new ArrayList<DataLoader<JDBCTaskMetadata>>();
            var start = (double) taskInfo.getMetadata().getStartValue();
            var resultSet = queryData(taskInfo.getMetadata().getStartValue(), taskInfo.getMetadata().getEndValue(), -1);
            for (int i = 0; i < wantedRanges.size(); i++) {
                final var taskRange = wantedRanges.get(i);
                var endValue = start + itemsPerTask;
                // Due to rounding of values make sure the last task gets everything remaining
                if (i == taskCount - 1) endValue = Math.max(endValue, taskInfo.getMetadata().getEndValue());
                final var metadata = new JDBCTaskMetadata((long) start, (long) endValue);
                start = endValue;
                var loader = new DataLoader<JDBCTaskMetadata>() {
                    @Override
                    public TaskRange getTaskRange() {
                        return taskRange;
                    }

                    @Override
                    public Iterator<LoadedData> loadData() {
                        var headers = new HashMap<String, String>();
                        headers.put("startValue", metadata.getStartValue().toString());
                        headers.put("endValue", metadata.getEndValue().toString());
                        var inputStream = new ResultSetInputStream(resultSet, metadata.getItemCount());
                        var result = new LoadedData(inputStream, headers, taskRange.getInclusiveStartTime());
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
    }


    @Override
    public CompletionStage<TaskInformation<JDBCTaskMetadata>> getTaskInfo(JDBCTaskMetadata previousTaskMetadata,
                                                                          TaskRange taskRange,
                                                                          ShardDefinition shardDefinition) {
        long startFrom = 0L;
        if (previousTaskMetadata != null) startFrom = previousTaskMetadata.getEndValue();
        String query = "SELECT MIN(" + incrementingColumn + ") AS min," +
                " MAX(" + incrementingColumn + ") AS max" +
                " FROM " + tableName +
                " WHERE " + incrementingColumn + " >= :startFrom" +
                " HAVING MIN( " + incrementingColumn + ") IS NOT NULL";
        try (var statement = new NamedPreparedStatment(connection, query)) {
            statement.setLong("startFrom", startFrom);
            var rs = statement.executeQuery();
            if (rs.next()) {
                var max = rs.getLong("max");
                var min = rs.getLong("min");
                return CompletableFuture.completedFuture(new TaskInformation<>(taskRange, new JDBCTaskMetadata(min, max + 1)));
            } else {
                return CompletableFuture.completedFuture(new TaskInformation<>(taskRange, new JDBCTaskMetadata(startFrom, startFrom)));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get task infos", e);
        }
    }

    @Override
    public JDBCTaskMetadata reshard(List<JDBCTaskMetadata> previousTaskMetadatas,
                                    Instant taskTime,
                                    ShardDefinition newShard) {
        var endValue = previousTaskMetadatas.stream().mapToLong(x -> x.getEndValue()).max().orElse(-1L);
        return new JDBCTaskMetadata(endValue, endValue);
    }

    private String secureIdentifier(String identifier) {
        return identifierEscaper + identifier.replace(identifierEscaper, identifierEscaper + identifierEscaper) + identifierEscaper;
    }

}

