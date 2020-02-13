package com.upsolver.datasources.jdbc;

import com.upsolver.datasources.jdbc.utils.InstantMath;

import java.io.Serializable;
import java.time.Instant;

public class JDBCTaskMetadata implements Serializable {

    private long inclusiveStart;
    private long exclusiveEnd;
    private Instant endTime;
    private Instant startTime;

    public JDBCTaskMetadata() {
        // Empty Ctor for deserialization
    }

    public JDBCTaskMetadata(long inclusiveStart,
                            long exclusiveEnd,
                            Instant startTime,
                            Instant endTime) {
        this.inclusiveStart = inclusiveStart;
        this.exclusiveEnd = exclusiveEnd;
        this.startTime = startTime != null && startTime.isAfter(Instant.EPOCH) ? startTime : Instant.EPOCH;
        this.endTime = endTime != null && endTime.isAfter(Instant.EPOCH) ? endTime : Instant.EPOCH;
    }

    public JDBCTaskMetadata(long inclusiveStart,
                            long exclusiveEnd) {
        this(inclusiveStart, exclusiveEnd, null, null);
    }

    public double itemsPerTask(long taskCount) {
        if (taskCount <= 0) {
            return 0;
        }
        return Math.max(exclusiveEnd - inclusiveStart, 0) / (double) taskCount;
    }

    public int itemCount() {
        return (int) (exclusiveEnd - inclusiveStart);
    }

    public long getInclusiveStart() {
        return inclusiveStart;
    }

    public long getExclusiveEnd() {
        return exclusiveEnd;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public Instant getStartTime() {
        return startTime;
    }


    // Setters are required for deserialization
    public void setInclusiveStart(long inclusiveStart) {
        this.inclusiveStart = inclusiveStart;
    }

    public void setExclusiveEnd(long exclusiveEnd) {
        this.exclusiveEnd = exclusiveEnd;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public JDBCTaskMetadata adjustWithDelay(Long readDelay) {
        return new JDBCTaskMetadata(inclusiveStart, exclusiveEnd, startTime.plusSeconds(readDelay), endTime.plusSeconds(readDelay));
    }

    public JDBCTaskMetadata limitByPrevious(JDBCTaskMetadata previous) {
        return new JDBCTaskMetadata(
                Math.max(inclusiveStart, previous.inclusiveStart),
                Math.max(exclusiveEnd, previous.exclusiveEnd),
                InstantMath.max(startTime, previous.startTime),
                InstantMath.max(endTime, previous.endTime)
        );
    }
}


