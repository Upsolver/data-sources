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

    public int getItemCount() {
        return (int)(endValue - startValue);
    }


    public double getItemsPerTask(long taskCount) {
        if (taskCount <= 0) {
            return 0;
        }
        return Math.max(endValue - startValue, 0) / (double)taskCount;
    }
}
