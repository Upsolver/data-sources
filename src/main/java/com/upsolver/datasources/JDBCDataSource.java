package com.upsolver.datasources;

import com.upsolver.common.datasources.*;
import com.upsolver.common.datasources.contenttypes.CSVContentType;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


public class JDBCDataSource implements ExternalDataSource<JDBCTaskMetadata, JDBCTaskMetadata> {

    private static final String connectionStringProp = "Connection String";
    private static final String tableNameProp = "Table Name";
    private static final String incrementingColumnNameProp = "Incrementing Column";
    private static final String columnNamesProp = "Column Names";
    private static final String userNameProp = "User Name";
    private static final String passwordProp = "Password";

    private String connectionString;
    private String userName;
    private String password;
    private String tableName;
    private String incrementingColumn;
    private String columnNames;


    private Connection connection = null;

    @Override
    public DataSourceDescription getDataSourceDescription() {
        return new DataSourceDescription() {
            @Override
            public String getName() {
                return "JDBC";
            }

            @Override
            public String getDescription() {
                return "Read data from and JDBC compliant source";
            }

            @Override
            public int getMaxShards() {
                return 1;
            }
        };
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        connectionString = properties.get(connectionStringProp);
        tableName = properties.get(tableNameProp);
        incrementingColumn = properties.get(incrementingColumnNameProp);
        columnNames = properties.get(columnNamesProp);
        userName = properties.get(userNameProp);
        password = properties.get(passwordProp);


        try {
            //Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(connectionString, userName, password);
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
        result.add(new SimplePropertyDescription(columnNamesProp, "The names of the columns to load", false));
        result.add(new SimplePropertyDescription(incrementingColumnNameProp, "The name of the column which has an incrementing value to be used to load data sequentially", false));
        return result;
    }

    @Override
    public DataSourceContentType getContentType() {
        return new CSVContentType(true, ',', columnNames, null);
    }

    @Override
    public CompletionStage<InputStream> getSample() {
        InputStream result = queryData(0L, 100000L, 100);
        return CompletableFuture.completedFuture(result);
    }

    private InputStream queryData(Long inclusiveStart, Long exclusiveEnd, int limit) {
        String query = "SELECT " + columnNames + " FROM " + tableName + " WHERE " +
                incrementingColumn + " BETWEEN " + inclusiveStart + " AND " + (exclusiveEnd - 1);
        if (limit > 0) {
            query = query + " limit " + limit;
        }
        try {
            return new ResultSetInputStream(runQuery(query));
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
    }

    private ResultSet runQuery(String query) {
        try {
            Statement stmt = connection.createStatement();
            return stmt.executeQuery(query);
        } catch (Exception e) {
            throw new RuntimeException("Error while reading table", e);
        }
    }

    @Override
    public List<PropertyError> validate(Map<String, String> properties) {
        return new ArrayList<>();
    }

    @Override
    public CompletionStage<List<DataLoader<JDBCTaskMetadata>>> getDataLoaders(List<TaskInformation<JDBCTaskMetadata>> taskInfos) {
        List<DataLoader<JDBCTaskMetadata>> result = taskInfos.stream().map(t -> {
            final var metadata = t.getMetadata();
            final var taskTime = t.getTaskTime();
            return new DataLoader<JDBCTaskMetadata>() {
                @Override
                public Instant getTaskTime() {
                    return taskTime;
                }

                @Override
                public CompletionStage<Iterator<LoadedData>> loadData() {
                    InputStream stream = queryData(metadata.getStartValue(), metadata.getEndValue(), -1);
                    var headers = new HashMap<String, String>();
                    headers.put("startValue", metadata.getStartValue().toString());
                    headers.put("endValue", metadata.getEndValue().toString());
                    var result = new LoadedData(stream, headers);
                    return CompletableFuture.completedFuture(Collections.singleton(result).iterator());
                }

                @Override
                public JDBCTaskMetadata getCompletedMetadata() {
                    return metadata;
                }
            };
        }).collect(Collectors.toList());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletionStage<List<TaskInformation<JDBCTaskMetadata>>> getTaskInfos(JDBCTaskMetadata previousTaskMetadata,
                                                                                 List<Instant> taskTimes,

                                                                                 ShardDefinition shard) {
        long startFrom = 0L;
        if (previousTaskMetadata != null) startFrom = previousTaskMetadata.getEndValue();
        String query = "SELECT MIN( + " + incrementingColumn + ") AS min," +
                " MAX( + " + incrementingColumn + ") - MIN( + " + incrementingColumn + ") AS diff" +
                " FROM " + tableName +
                " WHERE " + incrementingColumn + " >= " + startFrom +
                " HAVING MIN( " + incrementingColumn + ") IS NOT NULL";
        int itemsPerTask = 0;
        try (ResultSet rs = runQuery(query)) {
            var diff = 0;
            var min = 0L;
            var hasData = rs.next();
            if (hasData) {
                diff = rs.getInt("diff");
                min = rs.getLong("min");
                if (diff > 0 && taskTimes.size() > 0) {
                    itemsPerTask = diff / taskTimes.size();
                }
            }
            var result = new ArrayList<TaskInformation<JDBCTaskMetadata>>();

            long startValue = Math.max(min, Math.max(startFrom, 0));
            for (final Instant taskTime : taskTimes) {
                if (hasData) {
                    long endValue = startValue + itemsPerTask + 1;
                    result.add(new TaskInformation<>(taskTime,new JDBCTaskMetadata(startValue, endValue)));
                    startValue = endValue;
                } else {
                    result.add(new TaskInformation<>(taskTime,new JDBCTaskMetadata(startValue, startValue)));
                }
            }
            rs.getStatement().close();
            return CompletableFuture.completedFuture(result);
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
}

