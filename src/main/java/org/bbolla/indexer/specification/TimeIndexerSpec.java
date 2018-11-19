package org.bbolla.indexer.specification;

import org.joda.time.Interval;

import java.util.Map;

/**
 * Indexer specs.
 * The range is stored as a long array {startInclusive, endExclusive}
 */
public interface TimeIndexerSpec {

    /**
     * Get all rows in a interval.
     * @param interval
     * @return
     */
    Map<String, long[]> getAllRowsInAnInterval(Interval interval);

    /**
     * Store all rows in an interval.
     * @param interval
     * @param startInclusiveAndEndExclusive
     */
    void storeAllRowsInAnInterval(Interval interval, long[] startInclusiveAndEndExclusive);


}
