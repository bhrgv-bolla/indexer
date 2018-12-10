package org.bbolla.db.indexer.impl;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.bbolla.db.indexer.specification.IndexerSpec;
import org.bbolla.db.indexer.specification.TimeIndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Using ignite to implement the indexer.
 * Save to memory <= keep only the latest records / order of insertion
 * Push the past records to disk for later access.
 * Data Affinity => Place records of same timestamp in the same node.
 * Indexer can assume that index additions come less frequently than reads (Ex: 1 new index addition in a ~min; pre-aggregations take care of active indices.)
 */
@Slf4j
public class IndexerImpl implements IndexerSpec {

    //Maximum size.
    private static final int THRESHOLD_SIZE = 1000000;
    private static final String DIMENSIONS_CACHE = "dimensions_cache";
    private Ignite ignite;

    /*
        Stores dim_val_partition bitmap.
     */
    private IgniteCache<String, SerializedBitmap> cacheMap;

    /*
        Stores information about the number of partitions.
     */
    private IgniteCache<String, List<DimensionPartitionMetaInfo>> dmMap; //Dimension Meta Info Map.
    private TimeIndexerSpec ti;


    private final static String INDEXER_CACHE = "indexer_cache";

    public IndexerImpl(Server server, TimeIndexerSpec ti) {
        this.ti = ti;

        ignite = server.getIgniteInstance();
        CacheConfiguration config = new CacheConfiguration<String, SerializedBitmap>();
        config.setAffinity(new CustomAffinityFunction());
        config.setName(INDEXER_CACHE);
        config.setCacheMode(CacheMode.PARTITIONED);
        config.setBackups(2);
        config.setReadFromBackup(true);
        config.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cacheMap = ignite.getOrCreateCache(config);

        dmMap = ignite.getOrCreateCache(DIMENSIONS_CACHE);
    }

    /**
     * Utility to prepare the key.
     *
     * @param key
     * @param val
     * @param partitionNumber
     * @return
     */
    private String key(DateTime today, String key, String val, int partitionNumber) {
        return "d_" + today.withTimeAtStartOfDay().toString() + "_p_" + key + "_v_" + val + "_pn_" + partitionNumber;
    }

    @Override
    public void index(DateTime startOfDay, String key, String val, Roaring64NavigableMap rows) {
        rows.runOptimize();

        startOfDay = startOfDay.withTimeAtStartOfDay();
        List<DimensionPartitionMetaInfo> partitions = dmMap.get(startOfDay.toString());
        if (partitions == null) {
            partitions = Lists.newArrayList();
        }
        //sort all partitions.
        Collections.sort(partitions, Comparator.comparingInt(DimensionPartitionMetaInfo::getPartitionNumber));
        if (partitions.size() == 0) partitions.add(newPartition(0));
        //last partition.
        DimensionPartitionMetaInfo lastPartition = partitions.get(partitions.size() - 1);

        int combinedSize = rows.getSizeInBytes() + lastPartition.getSizeInBytes();
        if (combinedSize > THRESHOLD_SIZE) {
            //create a new partition.
            lastPartition = newPartition(lastPartition.getPartitionNumber() + 1);
            partitions.add(lastPartition);
        }


        String theKey = key(startOfDay, key, val, lastPartition.getPartitionNumber());
        String dmKey = key(startOfDay, key, val); //meta info key

        SerializedBitmap srr = cacheMap.get(theKey); //serialized bitmap.
        Roaring64NavigableMap rr;
        if (srr != null) rr = srr.toBitMap();
        else rr = Roaring64NavigableMap.bitmapOf();

        rr.or(rows);
        rr.runOptimize();
        SerializedBitmap resultSrr = SerializedBitmap.fromBitMap(rr);
        lastPartition.setSizeInBytes(resultSrr.getSizeInBytes());

        //modify cache here.
        dmMap.put(dmKey, partitions);
        cacheMap.put(theKey, resultSrr);
    }

    private String key(DateTime startOfDay, String key, String val) {
        return "d_" + startOfDay.withTimeAtStartOfDay().toString() + "_p_" + key + "_v_" + val;
    }

    private DimensionPartitionMetaInfo newPartition(int pNum) {
        DimensionPartitionMetaInfo dp = new DimensionPartitionMetaInfo();
        dp.setPartitionNumber(pNum);
        dp.setRange(null);
        dp.setSizeInBytes(0);
        return dp;
    }

    @Override
    public void index(DateTime startOfDay, String key, String val, long row) {
        index(startOfDay, key, val, Roaring64NavigableMap.bitmapOf(row));
    }

    @Override
    public void addTimeIndex(DateTime startTime, DateTime endTime, long startInclusive, long endExclusive) {
        ti.storeAllRowsInAnInterval(new Interval(startTime, endTime), new long[]{startInclusive, endExclusive});
    }

    @Override
    public Map<DateTime, long[]> getRowIDs(Map<String, String> filters, Interval interval) {
        //Need to get the dimension bitmaps for all filters.
        //Since partitioned by day <= these can be ran in parallel; for multiple days.
        //For a single day in the interval <= apply all filters and get row ids for that day.
        List<IgniteFuture<FilterResult>> allFutures = Lists.newArrayList();
        List<DateTime> days = Utils.getDays(interval);

        Map<String, long[]> rowsInInterval = ti.getAllRowsInAnInterval(interval);

        //can be parallel across days
        for (DateTime day : days) {
            try {
                allFutures.add(
                        getRowsIDsForADay(filters, day)
                );
            } catch (NoDataExistsException e) {
                log.info("No data exists for : {}", day);
            }
        }

        //once all calls are submitted, get all responses.
        Map<DateTime, long[]> result = Maps.newHashMap();
        allFutures.forEach(
                f -> {
                    try {
                        FilterResult fr = f.get(100, TimeUnit.MILLISECONDS);
                        Roaring64NavigableMap allFiltersForDay = fr.getRows().toBitMap();
                        //one day
                        long[] rows = rowsInInterval.get(fr.date().toString());
                        Roaring64NavigableMap rowsToday = null;
                        if(rows != null) {
                            rowsToday = Roaring64NavigableMap.bitmapOf();
                            for(long i=rows[0]; i<rows[1]; i++) rowsToday.addLong(i);
                            rowsToday.runOptimize();
                            allFiltersForDay.and(rowsToday);
                        }
                        else allFiltersForDay = Roaring64NavigableMap.bitmapOf();
                        result.put(fr.date(), allFiltersForDay.toArray());
                    } catch (Exception ex) {
                        throw new RuntimeException("Cannot get all the results: ", ex);
                    }
                }
        );
        return result;
    }

    private IgniteFuture<FilterResult> getRowsIDsForADay(Map<String, String> filters, DateTime day) throws NoDataExistsException {
        /**
         * TODO Retry before failing the feature.
         */
        List<List<String>> keys = keys(filters, day);
        List<String> allKeys = keys.stream().flatMap(k -> k.stream()).collect(Collectors.toList()); //allKeys mixed
        log.info("All Keys: {}", allKeys);
        if(allKeys.size() == 0) throw new NoDataExistsException();
        IgniteFuture<FilterResult> filterResult = ignite.compute().affinityCallAsync(INDEXER_CACHE, allKeys.get(0), (IgniteCallable<FilterResult>) () -> {

            log.info("local entries: {}", cacheMap.localMetrics());

            Roaring64NavigableMap result = keys.stream().map(
                    partitionKeys -> { //union across all partition of the same k, v
                        Map<String, SerializedBitmap> dimMap = cacheMap.getAll(Sets.newHashSet(partitionKeys));
                        //union all dimMap
                        Roaring64NavigableMap unionRR = Roaring64NavigableMap.bitmapOf();
                        dimMap.forEach((k, v) -> unionRR.or(v.toBitMap()));
                        return unionRR;
                    })
                    .reduce(null, (r, e) -> {
                        if (r == null) return e;
                        else {
                            r.and(e);
                            r.runOptimize();
                            return r;
                        }
                    });

            return new FilterResult(day.toString(), SerializedBitmap.fromBitMap(result));
        });

        return filterResult;
    }

    private List<List<String>> keys(Map<String, String> filters, DateTime day) {
        Set<String> keys = Sets.newHashSet();
        for (Map.Entry<String, String> filter : filters.entrySet()) {
            keys.add(key(day.withTimeAtStartOfDay(), filter.getKey(), filter.getValue()));
        }

        //all dmKeys are in keys. <= time to get partitions.

        Map<String, List<DimensionPartitionMetaInfo>> metaInfo = dmMap.getAll(keys);

        List<List<String>> result = Lists.newArrayList();

        metaInfo.forEach(
                (k, v) -> result.add(addPartitions(k, v.size()))
        );

        return result;
    }

    private List<String> addPartitions(String key, int size) {
        List<String> partitionKeys = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            partitionKeys.add(key + "_pn_" + i);
        }
        return partitionKeys;
    }


    public static void main(String[] args) {


        Server server = new Server();
        TimeIndexerSpec ts = new TimeIndexerImpl(server);
        IndexerImpl indexer = new IndexerImpl(server, ts);


        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();

        for (int i = 0; i < 10000; i++) {
            rr.add(i);
        }


        indexer.index(DateTime.now(), "test", "24", rr);

        indexer.ti.storeAllRowsInAnInterval(new Interval(DateTime.now(), DateTime.now()), new long[] {0, 10000});

        indexer.dmMap.forEach(
                e -> log.info("dmMap Entry: {}", e)
        );

        indexer.cacheMap.forEach(
                e -> {
                    log.info("cacheMap Entry : {}", e);
                    log.info("results: {}", e.getValue().toBitMap());
                }
        );


        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<DateTime, long[]> rowIdMap = indexer.getRowIDs(ImmutableMap.of("test", "24"), new Interval(DateTime.now().withTimeAtStartOfDay(), Period.days(3)));


        stopwatch.stop();

        log.info("rowIdMap: {}", Utils.toString(rowIdMap));


        log.info("Time elapsed: {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));

        server.getIgniteInstance().close();
    }
}
