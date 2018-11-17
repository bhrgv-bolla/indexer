package org.bbolla.indexer.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.bbolla.indexer.specification.TimeIndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

@Slf4j
public class TimeIndexerImpl implements TimeIndexerSpec {

    private IgniteCache<String, long[]> trMap;

    private static final String TIME_INDEXER_CACHE = "time_cache";

    private final int WINDOW_TIME_IN_MINUTES;


    public TimeIndexerImpl(Server server, int windowTimeInMinutes) {
        Ignite ignite = server.getIgniteInstance();
        this.trMap = ignite.getOrCreateCache(TIME_INDEXER_CACHE);
        this.WINDOW_TIME_IN_MINUTES = windowTimeInMinutes;
    }

    @Override
    public Map<String, long[]> getAllRowsInAnInterval(Interval interval) {
        DateTime start = findKeyBefore(interval.getStart(), WINDOW_TIME_IN_MINUTES * 60 * 1000);
        return null;
    }

    /**
     * TODO pending
     * Simple math to get the date before a start time
     * @param startTime
     * @param windowTimeInMillis
     * @return
     */
    private static DateTime findKeyBefore(DateTime startTime, long windowTimeInMillis) {
        long millisInDay = startTime.getMillis() - startTime.withTimeAtStartOfDay().getMillis();
        long offset = millisInDay / windowTimeInMillis;
        offset = offset * windowTimeInMillis;
        long result = startTime.withTimeAtStartOfDay().getMillis() + offset;
        return new DateTime(result);
    }

    @Override
    public void storeAllRowsInAnInterval(Interval interval, long[] startInclusiveAndEndExclusive) {

    }



    public static void main(String[] args) {
        DateTime result = findKeyBefore(DateTime.now(), 5* 60 * 1000);
        log.info("Result is {}", result);
    }
}
