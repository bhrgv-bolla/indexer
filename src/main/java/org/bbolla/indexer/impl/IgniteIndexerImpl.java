package org.bbolla.indexer.impl;

import org.bbolla.indexer.specification.IndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
 * Using ignite to implement the indexer.
 */
public class IgniteIndexerImpl implements IndexerSpec {

    @Override
    public void index(String key, String val, long[] rows) {

    }

    @Override
    public void index(String key, String val, long row) {

    }

    @Override
    public void adddTimeIndex(DateTime startTime, DateTime endTime, long startInclusive, long endExclusive) {

    }

    @Override
    public Map<DateTime, long[]> getRowIDs(Map<String, String> filters, Interval interval) {
        return null;
    }
}
