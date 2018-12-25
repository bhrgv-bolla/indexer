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
import java.util.concurrent.locks.Lock;
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
    private static final int MAX_CACHE_PARTITION_SIZE_IN_BYTES = 10000000;
    private static final String DIMENSIONS_CACHE = "dimensions_cache";
    private Ignite ignite;

    /*
        Stores dim_val_partition bitmap.
     */
    private IgniteCache<String, byte[]> cacheMap;

    /*
        Stores information about the number of partitions.
     */
    //TODO could i use the meta info; to see if it is worth pulling that partition?
    //TODO also if the time interval is less than a day <= we pull all the partitions?
    //TODO could this be reduced by maintaining not max_cache but maintaining partitions buy rows(?)
    //TODO dynamic partition size (storing the row ranges with the partition helps).
    private IgniteCache<String, List<DimensionPartitionMetaInfo>> dmMap; //Dimension Meta Info Map.
    private TimeIndexerSpec ti;


    private final static String INDEXER_CACHE = "indexer_cache";

    public IndexerImpl(Server server, TimeIndexerSpec ti) {
        this.ti = ti;

        ignite = server.getIgniteInstance();
        CacheConfiguration config = new CacheConfiguration<String, byte[]>();
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

    //TODO enable locking; let the use-case decide if locking is required; while using implementation.
    @Override
    public void index(DateTime startOfDay, String key, String val, Roaring64NavigableMap rows) {
        rows.runOptimize();

        startOfDay = startOfDay.withTimeAtStartOfDay();
        String dmKey = key(startOfDay, key, val); //meta info key

        List<DimensionPartitionMetaInfo> partitions = dmMap.get(dmKey);
        if (partitions == null) {
            partitions = Lists.newArrayList();
        }
        //sort all partitions.
        Collections.sort(partitions, Comparator.comparingInt(DimensionPartitionMetaInfo::getPartitionNumber));
        if (partitions.size() == 0) partitions.add(newPartition(0));
        //last partition.
        DimensionPartitionMetaInfo lastPartition = partitions.get(partitions.size() - 1);

        int combinedSize = rows.getSizeInBytes() + lastPartition.getSizeInBytes();
        if (combinedSize > MAX_CACHE_PARTITION_SIZE_IN_BYTES) {
            //create a new partition.
            lastPartition = newPartition(lastPartition.getPartitionNumber() + 1);
            partitions.add(lastPartition);
        }


        String theKey = key(startOfDay, key, val, lastPartition.getPartitionNumber());


        SerializedBitmap srr = new SerializedBitmap(cacheMap.get(theKey)); //serialized bitmap.
        Roaring64NavigableMap rr = srr.toBitMap();


        rr.or(rows);
        rr.runOptimize();
        SerializedBitmap resultSrr = SerializedBitmap.fromBitMap(rr);
        lastPartition.setSizeInBytes(resultSrr.sizeInBytes());

        //modify cache here.
        dmMap.put(dmKey, partitions);
        cacheMap.put(theKey, resultSrr.getBytes());
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

    private final String DELETE_KEY = "$delete", DELETE_VAL = "~set";

    @Override
    public void deleteRows(DateTime startOfDay, long[] rows) {
        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf(rows);
        //Lock for delete.
        String dmKey = key(startOfDay, DELETE_KEY, DELETE_VAL);
        Lock dmLock = dmMap.lock(dmKey);

        try {
            dmLock.tryLock(200, TimeUnit.MILLISECONDS);
            //index these
            index(startOfDay, DELETE_KEY, DELETE_VAL, rr);
        } catch (InterruptedException e) {
            throw new RuntimeException("Cannot acquire dm lock; delete failed", e);
        } finally {
            dmLock.unlock();
        }
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
                        FilterResult fr = f.get(100, TimeUnit.MILLISECONDS); //TODO get all futures in 100 millliseconds not each future in 100 milliseconds.
                        Roaring64NavigableMap allFiltersForDay = fr.getRows().toBitMap();
                        //one day
                        long[] rows = rowsInInterval.get(fr.date().toString());
                        Roaring64NavigableMap rowsToday = null;
                        if (rows != null) {
                            rowsToday = Roaring64NavigableMap.bitmapOf();
                            for (long i = rows[0]; i < rows[1]; i++) rowsToday.addLong(i);
                            rowsToday.runOptimize();
                            allFiltersForDay.and(rowsToday);
                        } else allFiltersForDay = Roaring64NavigableMap.bitmapOf();
                        result.put(fr.date(), allFiltersForDay.toArray());
                    } catch (Exception ex) {
                        throw new RuntimeException("Cannot get all the results: ", ex);
                    }
                }
        );
        return result;
    }

    //TODO add logic to use the delete set.
    //TODO ***ENHANCEMENT*** pass the row ranges to only pull those keys
    private IgniteFuture<FilterResult> getRowsIDsForADay(Map<String, String> filters, DateTime day) throws NoDataExistsException {
        /**
         * TODO Retry before failing the feature.
         */
        List<List<String>> keys = keys(filters, day);
        String sampleKeyForPartitionLookup = null; //Sample to tell ignite; where to run the block of code.


        { //This block of code is to check atleast one key exists.
            //TODO ***ENHANCEMENT*** if for a day atleast for one filter there are no rows; this means there is no data; since filters supported are AND today; no OR filters.
            List<String> allKeys = keys.stream().flatMap(k -> k.stream()).collect(Collectors.toList()); //allKeys mixed
            log.info("All Keys: {}", allKeys);
            if (allKeys.size() == 0) throw new NoDataExistsException();
            sampleKeyForPartitionLookup = allKeys.get(0);
        }


        //Runs this block closer to where data is stored.
        IgniteFuture<FilterResult> filterResult = ignite.compute().affinityCallAsync(INDEXER_CACHE, sampleKeyForPartitionLookup, (IgniteCallable<FilterResult>) () -> {

            log.info("local entries: {}", cacheMap.localMetrics());

            Roaring64NavigableMap result = keys.stream().map(
                        //union across all partitions (if any) of the same index.
                        partitionKeys -> unionBitsetsForGivenKeys(partitionKeys)
                    )
                    .reduce(null, (r, e) -> { //intersection b/w filters.
                        if (r == null) return e;
                        else {
                            r.and(e);
                            r.runOptimize();
                            return r;
                        }
                    });

            //DELETE happen's here
            Roaring64NavigableMap deleteSet = getDeleteSetForDay(day);
            result.andNot(deleteSet); //and not is minus

            return new FilterResult(day.toString(), SerializedBitmap.fromBitMap(result));
        });

        return filterResult;
    }

    private Roaring64NavigableMap unionBitsetsForGivenKeys(List<String> partitionKeys) {
        Map<String, byte[]> dimMap = Maps.newHashMap(cacheMap.getAll(Sets.newHashSet(partitionKeys)));

        //union all dimMap
        Roaring64NavigableMap unionRR = Roaring64NavigableMap.bitmapOf();

        for (Map.Entry<String, byte[]> e : dimMap.entrySet()) {
            SerializedBitmap sb = new SerializedBitmap(e.getValue());
            unionRR.or(sb.toBitMap());
        }

        return unionRR;
    }

    private Roaring64NavigableMap getDeleteSetForDay(DateTime day) {

        List<List<String>> deleteSetPartitionKeys = keys(ImmutableMap.of(DELETE_KEY, DELETE_VAL), day);

        //contains 0 or 1 List of keys.
        if(deleteSetPartitionKeys.size() == 0) {
            return Roaring64NavigableMap.bitmapOf();
        } else {
            if(deleteSetPartitionKeys.size() != 1) throw new IllegalStateException("Expecting only 1 element to exist in this list");
             return unionBitsetsForGivenKeys(deleteSetPartitionKeys.get(0));
        }
    }

    /**
     * Pull all keys (for all partitions) the filters (k,v pairs) have in a day.
     *
     * @param filters
     * @param day
     * @return
     */
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

        indexer.ti.storeAllRowsInAnInterval(new Interval(DateTime.now(), DateTime.now()), new long[]{0, 10000});

        indexer.dmMap.forEach(
                e -> log.info("dmMap Entry: {}", e)
        );

        indexer.cacheMap.forEach(
                e -> {
                    log.info("cacheMap Entry : {}", e);
                    log.info("results: {}", new SerializedBitmap(e.getValue()).toBitMap());
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
