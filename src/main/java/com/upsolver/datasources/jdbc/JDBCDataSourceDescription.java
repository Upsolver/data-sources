package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.DataSourceDescription;

class JDBCDataSourceDescription implements DataSourceDescription {
    @Override
    public String getName() {
        return "JDBC";
    }

    @Override
    public String getDescription() {
        return "Read data from and JDBC compliant source";
    }

    @Override
    public String getLongDescription() {
        return "Create a new Data Source able to read streaming data from JDBC sources." +
                "\nThe table must contain an Auto-Incrementing column, which will be used to ensure exacly once processing.";
    }

//        @Override
//        public String getIconResourceName() {
//            return "jdbc-logo.svg";
//        }

}
