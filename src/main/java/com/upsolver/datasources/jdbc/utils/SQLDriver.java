package com.upsolver.datasources.jdbc.utils;

public class SQLDriver {
    private final String className;
    private final String urlTemplate;
    private final String name;
    private final String urlPrefix;

    public SQLDriver(String className, String urlTemplate, String name, String urlPrefix) {
        this.className = className;
        this.urlTemplate = urlTemplate;
        this.name = name;
        this.urlPrefix = urlPrefix;
    }

    public String getClassName() {
        return className;
    }

    public String getUrlTemplate() {
        return urlTemplate;
    }

    public String getName() {
        return name;
    }

    public String getUrlPrefix() {
        return urlPrefix;
    }
}
