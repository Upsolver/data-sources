package com.upsolver.datasources;

import com.upsolver.common.datasources.TaskInformation;

import java.io.Serializable;
import java.time.Instant;

public class JDBCTaskMetadata implements Serializable {

    private Long startValue;
    private Long endValue;

    public JDBCTaskMetadata(Long inclusiveStart, Long exclusiveEnd) {
        this.startValue = inclusiveStart;
        this.endValue = exclusiveEnd;
    }

    public Long getStartValue() {
        return startValue;
    }

    public Long getEndValue() {
        return endValue;
    }
}
