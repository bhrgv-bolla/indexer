package org.bbolla.indexer.impl;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.bbolla.indexer.specification.IndexerSpec;
import org.bbolla.indexer.specification.TimeIndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
    private IgniteCache<String, SerializedBitmap> cacheMap;
    private IgniteCache<String, List<DimensionPartitionMetaInfo>> dmMap; //Dimension Meta Info Map.
    private TimeIndexerSpec ti;


    private final static String INDEXER_CACHE = "indexer_cache";

    public IndexerImpl(Server server, TimeIndexerSpec ti) {
        ignite = server.getIgniteInstance();
        cacheMap = ignite.getOrCreateCache(INDEXER_CACHE);
        this.ti = ti;
        dmMap = ignite.getOrCreateCache(DIMENSIONS_CACHE);
    }

    /**
     * Utility to prepare the key.
     * @param key
     * @param val
     * @param partitionNumber
     * @return
     */
    private String key(DateTime today, String key, String val, int partitionNumber) {
        return "d_"+ today.withTimeAtStartOfDay().toString() +"_p_"+ key+ "_v_" + val+"_pn_" + partitionNumber;
    }

    @Override
    public void index(DateTime startOfDay, String key, String val, Roaring64NavigableMap rows) {
        rows.runOptimize();

        startOfDay = startOfDay.withTimeAtStartOfDay();
        List<DimensionPartitionMetaInfo> partitions = dmMap.get(startOfDay.toString());
        if(partitions == null) {
            partitions = Lists.newArrayList();
        }
        //sort all partitions.
        Collections.sort(partitions, Comparator.comparingInt(DimensionPartitionMetaInfo::getPartitionNumber));
        if(partitions.size() == 0) partitions.add(newPartition(0));
        //last partition.
        DimensionPartitionMetaInfo lastPartition = partitions.get(partitions.size() - 1);

        int combinedSize = rows.getSizeInBytes() + lastPartition.getSizeInBytes();
        if(combinedSize > THRESHOLD_SIZE) {
            //create a new partition.
            lastPartition = newPartition(lastPartition.getPartitionNumber() + 1);
            partitions.add(lastPartition);
        }


        String theKey = key(startOfDay, key, val, lastPartition.getPartitionNumber());
        String dmKey = key(startOfDay, key, val); //meta info key

        SerializedBitmap srr = cacheMap.get(theKey); //serialized bitmap.
        Roaring64NavigableMap rr;
        if(srr != null) rr = srr.toBitMap();
        else rr = Roaring64NavigableMap.bitmapOf();
        log.info("Modifying / new RR : {}", rr);
        rr.or(rows);
        rr.runOptimize();
        SerializedBitmap resultSrr = SerializedBitmap.fromBitMap(rr);
        lastPartition.setSizeInBytes(resultSrr.getSizeInBytes());

        //modify cache here.
        dmMap.put(dmKey, partitions);
        cacheMap.put(theKey, resultSrr);
    }

    private String key(DateTime startOfDay, String key, String val) {
        return "d_"+ startOfDay.withTimeAtStartOfDay().toString() +"_p_"+ key+ "_v_" + val;
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
        return null;
    }



    public static void main(String[] args) {


        Server server = new Server();
        TimeIndexerSpec ts = new TimeIndexerImpl(server);
        IndexerImpl indexer = new IndexerImpl(server, ts);

        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();

        for(int i=0; i<10000; i++) {
            rr.add(i);

            indexer.index(DateTime.now(), "test", "24", i);
        }


        indexer.dmMap.forEach(
                e -> log.info("dmMap Entry: {}", e)
        );

        indexer.cacheMap.forEach(
                e -> {
                    log.info("cacheMap Entry : {}", e);
                    log.info("results: {}", e.getValue().toBitMap());
                }
        );

        SerializedBitmap rr2 = indexer.cacheMap.get("d_2018-11-22T00:00:00.000+05:30_p_test_v_24_pn_0");


//        while(iterator.hasNext()) {
//            log.info("{}", iterator.next());
//        }

        log.info("RR: {}", rr2.toBitMap());

        server.getIgniteInstance().close();
    }
}
