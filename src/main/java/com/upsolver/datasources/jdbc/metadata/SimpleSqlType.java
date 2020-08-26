package com.upsolver.datasources.jdbc.metadata;

import java.sql.SQLType;

public class SimpleSqlType implements SQLType {
    private final String name;
    private final String vendor;
    private final Integer vendorType;

    public SimpleSqlType(String name, String vendor, Integer vendorType) {
        this.name = name;
        this.vendor = vendor;
        this.vendorType = vendorType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVendor() {
        return vendor;
    }

    @Override
    public Integer getVendorTypeNumber() {
        return vendorType;
    }
}
