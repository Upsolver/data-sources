package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.DataLoader;
import com.upsolver.common.datasources.LoadedData;
import com.upsolver.common.datasources.TaskRange;

import java.util.Collections;
import java.util.Iterator;

public class NoDataLoader implements DataLoader<JDBCTaskMetadata> {
    private JDBCTaskMetadata metadata;
    private TaskRange taskRange;

    public NoDataLoader(TaskRange taskRange, JDBCTaskMetadata metadata) {
        this.taskRange = taskRange;
        this.metadata = metadata;
    }

    @Override
    public TaskRange getTaskRange() {
        return taskRange;
    }

    @Override
    public Iterator<LoadedData> loadData() {
        return Collections.emptyIterator();
    }

    @Override
    public JDBCTaskMetadata getCompletedMetadata() {
        return metadata;
    }
}
