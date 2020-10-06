package com.upsolver.datasources.jdbc.querybuilders;

import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

public class QueryDialectProvider {
    public static QueryDialect forConnection(String connectionString, boolean keepTypes) {
        String connStr = connectionString.toLowerCase();
        Iterable<QueryDialect> iterable = () -> ServiceLoader.load(QueryDialect.class).iterator();
        return StreamSupport.stream(iterable.spliterator(), false)
                .filter(d -> d.acceptsURL(connStr))
                .findFirst()
                .orElseGet(DefaultQueryDialect::new)
                .keepTypes(keepTypes);
    }
}
