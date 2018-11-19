package org.bbolla.indexer.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.bbolla.indexer.specification.TimeIndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.*;

@Slf4j
public class TimeIndexerImpl implements TimeIndexerSpec {

    private IgniteCache<String, long[]> trMap;

    private static final String TIME_INDEXER_CACHE = "time_cache";

    private final int WINDOW_TIME_IN_MINUTES;


    public TimeIndexerImpl(Server server, int windowTimeInMinutes) {
        //TODO configure native persistence for time indexer.
        Ignite ignite = server.getIgniteInstance();
        this.trMap = ignite.getOrCreateCache(TIME_INDEXER_CACHE);
        this.WINDOW_TIME_IN_MINUTES = windowTimeInMinutes;
    }

    @Override
    public Map<String, long[]> getAllRowsInAnInterval(Interval interval) {
        DateTime start = findKeyBefore(interval.getStart());
        DateTime end = findKeyBefore(interval.getEnd());
        // from start to end get all rows.
        List<DateTime> allKeysForAnInterval = keys(start, end);
        LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay = bucketKeysByDay(allKeysForAnInterval);
        log.debug("bucket keys: {}", bucketKeysByDay);
        //Assume every key exists.
        Map<String, long[]> result = getRowRangeForEachDayInInterval(bucketKeysByDay);
        log.debug("result: {}", result);
        return result;
    }

    /**
     * Utility method to return the row range from each partition.
     * @param bucketKeysByDay
     * @return
     */
    private Map<String,long[]> getRowRangeForEachDayInInterval(LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay) {
        Map<String, long[]> rrMap = Maps.newHashMap(); // rowRangeMap
        for(Map.Entry<DateTime, List<DateTime>> entry : bucketKeysByDay.entrySet()) { //Optimize the call; by using bulk get call?
            List<DateTime> partialKeysInADay = entry.getValue();
            Collections.sort(partialKeysInADay); //TODO see if this is unnecessary.
            long[] rowsFirst = trMap.get(partialKeysInADay.get(0).toString());
            long[] rowsLast = trMap.get(partialKeysInADay.get(partialKeysInADay.size() - 1).toString());
            long[] rowsInThisDay = new long[]{rowsFirst[0], rowsLast[1]};
            String dayKey = entry.getKey().toString();
            rrMap.put(dayKey, rowsInThisDay);
        }
        return rrMap;
    }

    /**
     * Converts keys that from a single list into a map of lists with keys as start of the day.
     * @param allKeysForAnInterval
     * @return
     */
    private static LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay(List<DateTime> allKeysForAnInterval) {
        LinkedHashMap<DateTime, List<DateTime>> result = Maps.newLinkedHashMap();
        Collections.sort(allKeysForAnInterval);
        for(DateTime current : allKeysForAnInterval) {
            DateTime startOfDay = current.withTimeAtStartOfDay();
            if(result.get(startOfDay) == null) result.put(startOfDay, Lists.newArrayList());
            result.get(startOfDay).add(current);
        }
        return result;
    }

    private List<DateTime> keys(DateTime start, DateTime end) {
        return findAllKeysInInterval(start, end, minutesToMillis(WINDOW_TIME_IN_MINUTES));
    }

    private static List<DateTime> findAllKeysInInterval(DateTime start, DateTime end, long offsetMillis) {
        List<DateTime> allKeysInInterval = Lists.newArrayList();
        // with a start and end. generate all possible keys.
        for(DateTime current = start; current.isBefore(end); current = current.plusMillis((int) offsetMillis)) {
            allKeysInInterval.add(current);
        }
        return allKeysInInterval;
    }

    private DateTime findKeyBefore(DateTime input) {
        return findKeyBefore(input, minutesToMillis(WINDOW_TIME_IN_MINUTES));
    }

    private static long minutesToMillis(int minutes) {
        return minutes * 60 * 1000;
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
        //Can be multiple saves.
        //We will store with the start time; how many rows are there in the interval.
        trMap.put(interval.getStart().toString(), startInclusiveAndEndExclusive);
    }



    public static void main(String[] args) {
        DateTime result = findKeyBefore(DateTime.now(), 5* 60 * 1000);
        log.info("Result is {}", result);
        List<DateTime> allKeys = findAllKeysInInterval(DateTime.parse("2018-10-10T20:33:00Z"),
                DateTime.parse("2018-10-12T20:33:00Z"),
                5 * 60 * 1000);
        log.info("All keys in interval {}", allKeys);

        Server server = new Server();
        int windowTime = 5;
        TimeIndexerImpl timeSpec = new TimeIndexerImpl(server, windowTime);
        Interval interval = new Interval("2018-10-10T10:00:00Z/PT5M");
        timeSpec.storeAllRowsInAnInterval(interval, new long[] {0, 1000});
        Map<String, long[]> allRows = timeSpec.getAllRowsInAnInterval(interval);
        for(Map.Entry<String, long[]> row : allRows.entrySet()) {
            log.info("All rows {} => {} : {}", interval, row.getKey(), Arrays.toString(row.getValue()));
        }

    }
}
