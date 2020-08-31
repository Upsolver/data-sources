package com.upsolver.datasources.jdbc.utils;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

public class SQLDrivers {
    private final Collection<SQLDriver> drivers = new ArrayList<>();

    public SQLDrivers() {
        Collection<String> availableDrivers = DriverManager.drivers().map(driver -> driver.getClass().getName()).collect(Collectors.toSet());
        Properties props = new Properties();
        try {
            props.load(getClass().getResourceAsStream("/drivers.properties"));
            for (String driverClassName : props.stringPropertyNames()) {
                if (!availableDrivers.contains(driverClassName)) {
                    continue;
                }
                for(String description : props.getProperty(driverClassName).split("\\s*\\|\\s*")) {
                    String[] data = description.split("\\s*,\\s*");
                    SQLDriver driver = new SQLDriver(driverClassName, data[0], data[1], getPrefix(data[0]));
                    drivers.add(driver);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Collection<SQLDriver> getDrivers() {
        return drivers;
    }

    public static String getPrefix(String url) {
        return Arrays.stream(url.split(":")).limit(2).collect(Collectors.joining(":"));
    }
}
