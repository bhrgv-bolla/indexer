package org.bbolla.indexer.specification;

import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

public interface IndexerSpec {

    /**
     * Let the indexer know of multiple occurrences of a key, value
     * @param today
     * @param key
     * @param val
     * @param rows
     */
    void index(DateTime today, String key, String val, long[] rows);

    /**
     * Let the index know of a key, value occurrence.
     * @param today
     * @param key
     * @param val
     * @param row
     */
    void index(DateTime today, String key, String val, long row);

    /**
     * Inform the indexer that this row ids exist in
     * @param startTime
     * @param endTime
     * @param startInclusive
     * @param endExclusive
     */
    void addTimeIndex(DateTime startTime, DateTime endTime, long startInclusive, long endExclusive);

    /**
     * Returns a map of dates and all rows in that date.
     * @param filters
     * @param interval
     * @return
     */
    Map<DateTime, long[]> getRowIDs(Map<String, String> filters, Interval interval);


    /**
     * Returns all row ids in a particular interval.
     * The returned result can contain records that fall out of the original timestamp.
     * The exact records can be obtained by filtering the complete records.
     * @param interval
     * @return
     */
    default Map<DateTime, long[]> getAllRowIDsInAnInterval(Interval interval) {
        return getRowIDs(Maps.newHashMap(), interval);
    }



}
