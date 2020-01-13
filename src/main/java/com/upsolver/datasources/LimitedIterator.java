package com.upsolver.datasources;

import java.util.Iterator;

public class LimitedIterator<T> implements Iterator<T> {
    private final Iterator<T> underlying;
    private final int limit;

    private int count = 0;

    public LimitedIterator(Iterator<T> underlying, int limit) {
        this.underlying = underlying;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        if (count >= limit) return false;
        return underlying.hasNext();
    }

    @Override
    public T next() {
        count++;
        return underlying.next();
    }
}
