package com.upsolver.datasources.jdbc.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Utilities for data manipulation useful mostly for tests
 */
public class DataUtil {
    /**
     * Collects data from {@link ResultSet} to {@link Collection}.
     * @param rs - the result set
     * @param names - column names that should be collected.
     * @return collection of collections of string representation of column values read from the given result set
     * @throws SQLException if DB access fails
     */
    public static Collection<Collection<String>> read(ResultSet rs, String ... names) throws SQLException {
        return read(rs, ResultSet::getString, names);
    }

    /**
     * Collects data from {@link ResultSet} to {@link Collection}.
     * @param rs - the result set
     * @param indices - column indices of that should be collected
     * @return collection of collections of string representation of column values read from the given result set
     * @throws SQLException if DB access fails
     */
    public static Collection<Collection<String>> read(ResultSet rs, Integer ... indices) throws SQLException {
        return read(rs, ResultSet::getString, indices);
    }

    private static <T> Collection<Collection<String>> read(ResultSet rs, ThrowingBiFunction<ResultSet, T, String, SQLException> getter, T[] identifiers) throws SQLException {
        Collection<Collection<String>> result = new ArrayList<>();
        while (rs.next()) {
            Collection<String> row = new ArrayList<>();
            for (T identifier : identifiers) {
                row.add(getter.apply(rs, identifier));
            }
            result.add(row);
        }
        return result;
    }

    /**
     * Trivial implementation of CSV parser. Good for tests only. Real CSV parser should be used for production environment.
     * @param reader - the reader
     * @param skipHeader - boolean flag that indicates whether the first (header) row should be skipped
     * @param indices - indices of columns that should be extracted
     * @return collection of collections of column values read from the given CSV
     * @throws IOException if IO problems happen
     */
    public static Collection<Collection<String>> read(BufferedReader reader, boolean skipHeader, Integer ... indices) throws IOException {
        if (skipHeader) {
            reader.readLine();
        }
        Collection<Collection<String>> result = new ArrayList<>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] fields = line.split("\\s*,\\s*");
            Collection<String> selectedFields = new ArrayList<>();
            for (int index : indices) {
                selectedFields.add(fields[index]);
            }
            result.add(selectedFields);
        }
        return result;
    }
}
