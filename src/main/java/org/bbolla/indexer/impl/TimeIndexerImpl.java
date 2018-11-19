package org.bbolla.indexer.impl;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.bbolla.indexer.specification.TimeIndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Read and write row ranges in a time range.
 * Collect them back very fast. ~3 ms/ 4 ms.
 * TODO tests for edge cases.
 */
@Slf4j
public class TimeIndexerImpl implements TimeIndexerSpec {

    private static final String TIME_INDEXER_CACHE = "time_cache";
    private static final String DATE_KEYS_CACHE = "date_key_cache";

    private IgniteCache<String, long[]> trMap;
    private IgniteCache<String, List<String>> dateKeys; //add date keys


    public TimeIndexerImpl(Server server) {
        //TODO configure native persistence for time indexer.
        Ignite ignite = server.getIgniteInstance();
        this.trMap = ignite.getOrCreateCache(TIME_INDEXER_CACHE);
        this.dateKeys = ignite.getOrCreateCache(DATE_KEYS_CACHE);
    }

    @Override
    public Map<String, long[]> getAllRowsInAnInterval(Interval interval) {
        // from start to end get all rows.
        List<DateTime> allKeysForAnInterval = keys(interval);
        LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay = bucketKeysByDay(allKeysForAnInterval);
        log.debug("bucket keys: {}", bucketKeysByDay);
        //Assume every key exists.
        Map<String, long[]> result = getRowRangeForEachDayInInterval(bucketKeysByDay);
        return result;
    }

    private List<DateTime> keys(Interval interval) {
        List<String> days = Lists.newArrayList();
        for(DateTime current = interval.getStart(); current.isBefore(interval.getEnd()); current = current.plusDays(1)) { //All days in the interval
            days.add(current.withTimeAtStartOfDay().toString());
        }

        Map<String, List<String>> allKeys = dateKeys.getAll(Sets.newHashSet(days));

        if(allKeys == null) return Lists.newArrayList();
        else {
            return allKeys.entrySet().stream().flatMap(e -> e.getValue()
                    .stream()
                    .map(d -> DateTime.parse(d)))
                    .filter(d -> interval.contains(d))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Utility method to return the row range from each partition.
     * @param bucketKeysByDay
     * @return
     */
    private Map<String,long[]> getRowRangeForEachDayInInterval(LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay) {
        Map<String, long[]> rrMap = Maps.newHashMap(); // rowRangeMap

        Set<String> keys = Sets.newHashSet();

        //collect keys
        for(Map.Entry<DateTime, List<DateTime>> entry : bucketKeysByDay.entrySet()) { //Optimize the call; by using bulk get call?
            List<DateTime> partialKeysInADay = entry.getValue();
            Collections.sort(partialKeysInADay);
            keys.add(partialKeysInADay.get(0).toString());
            keys.add(partialKeysInADay.get(partialKeysInADay.size() - 1).toString());
        }

        Map<String, long[]> rowRanges = trMap.getAll(keys);
        List<String> exceptions = Lists.newArrayList();

        //operate on keys
        for(Map.Entry<DateTime, List<DateTime>> entry : bucketKeysByDay.entrySet()) { //Optimize the call; by using bulk get call?
            List<DateTime> partialKeysInADay = entry.getValue();
            long[] rowsFirst = rowRanges.get(partialKeysInADay.get(0).toString());
            long[] rowsLast = rowRanges.get(partialKeysInADay.get(partialKeysInADay.size() - 1).toString());
            String dayKey = entry.getKey().toString();
            if(rowsFirst != null && rowsLast != null) {
                long[] rowsInThisDay = new long[]{rowsFirst[0], rowsLast[1]};
                rrMap.put(dayKey, rowsInThisDay);
            } else {
                exceptions.add("Couldn't find rows for day : "+ dayKey); //TODO bubble these exceptions.
            }
        }

        if(exceptions.size() > 0) log.warn("Some date keys are missing? {}", exceptions);

        return rrMap;
    }

    /**
     * Converts keys that from a single list into a map of lists with keys as start of the day.
     * @param allKeysForAnInterval
     * @return
     */
    private static LinkedHashMap<DateTime, List<DateTime>> bucketKeysByDay(List<DateTime> allKeysForAnInterval) {
        LinkedHashMap<DateTime, List<DateTime>> result = Maps.newLinkedHashMap();
        for(DateTime current : allKeysForAnInterval) {
            DateTime startOfDay = current.withTimeAtStartOfDay();
            if(result.get(startOfDay) == null) result.put(startOfDay, Lists.newArrayList());
            result.get(startOfDay).add(current);
        }
        return result;
    }


    @Override
    public void storeAllRowsInAnInterval(Interval interval, long[] startInclusiveAndEndExclusive) {
        //Can be multiple saves.
        //We will store with the start time; how many rows are there in the interval.
        trMap.put(interval.getStart().toString(), startInclusiveAndEndExclusive);

        //Add to date keys (Since a new date key only adds every once in few mins); this doesn't need to locking.
        String keyForInterval = interval.getStart().withTimeAtStartOfDay().toString();
        List<String> allKeysInDay = dateKeys.get(keyForInterval);
        if(allKeysInDay == null) allKeysInDay = Lists.newArrayList();
        allKeysInDay.add(interval.getStart().toString());
        dateKeys.put(keyForInterval, allKeysInDay);
    }



    public static void main(String[] args) {
        
        Server server = new Server();
        TimeIndexerImpl timeSpec = new TimeIndexerImpl(server);
        Stopwatch stopwatch = Stopwatch.createStarted();
        Interval interval = new Interval("2018-10-10T10:00:00Z/PT5M");
        timeSpec.storeAllRowsInAnInterval(interval, new long[] {0, 1000});
        Map<String, long[]> allRows = timeSpec.getAllRowsInAnInterval(interval);
        for(Map.Entry<String, long[]> row : allRows.entrySet()) {
            log.info("All rows {} => {} : {}", interval, row.getKey(), Arrays.toString(row.getValue()));
        }


        /**
         * Sample calls.
         */

        log.info("~~~~~~~~~~~~~~~~~~SAMPLE CALLS~~~~~~~~~~~~~~~~~~~~");

        TimeIndexerSpec ts = timeSpec;

        Interval testInterval = new Interval("2018-10-10T15:00:00Z/PT5M");

        ts.storeAllRowsInAnInterval(testInterval, new long[] {1000, 5000});

        Map<String, long[]> rs = ts.getAllRowsInAnInterval(new Interval("2018-10-01T15:00:00Z/P50D"));

        stopwatch.stop();

        log.info("Took {} ms : Result {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), Utils.toString(rs));

        server.getIgniteInstance().close();
    }
}
