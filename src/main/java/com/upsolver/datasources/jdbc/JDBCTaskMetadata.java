package com.upsolver.datasources.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;

public class JDBCTaskMetadata implements Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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

    public JDBCTaskMetadata adjustWithDelay(Long dbOffset) {
        Instant newStartTime = safePlusSeconds(this.startTime, dbOffset);
        Instant newEndTime = safePlusSeconds(this.endTime, dbOffset);
        return new JDBCTaskMetadata(inclusiveStart, exclusiveEnd, newStartTime, newEndTime);
    }

    private Instant safePlusSeconds(Instant time, Long offset) {
        try {
            return time.plusSeconds(offset);
        } catch (DateTimeException e) {
            String errorMessage =
                    String.format("Could not adjust date times (start: %s, end: %s) with offset: %d", startTime, endTime, offset);
            logger.error(errorMessage);
            return (offset < 0) ? Instant.MIN : Instant.MAX;
        }
    }

    public JDBCTaskMetadata truncateToStart() {
        return new JDBCTaskMetadata(inclusiveStart, inclusiveStart, startTime, startTime);
    }

}


