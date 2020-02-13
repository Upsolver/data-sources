package com.upsolver.datasources.jdbc.utils;

import java.time.Instant;

public class InstantMath {
    public static Instant min(Instant first, Instant second) {
        if (first.isBefore(second)){
            return first;
        }
        return second;
    }

    public static Instant max(Instant first, Instant second) {
        if (first.isAfter(second)){
            return first;
        }
        return second;
    }
}
