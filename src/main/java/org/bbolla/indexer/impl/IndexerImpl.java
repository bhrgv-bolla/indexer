package org.bbolla.indexer.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.bbolla.indexer.specification.IndexerSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Using ignite to implement the indexer.
 * Save to memory <= keep only the latest records / order of insertion
 * Push the past records to disk for later access.
 * Data Affinity => Place records of same timestamp in the same node.
 * Indexer can assume that index additions come less frequently than reads (Ex: 1 new index addition in a ~min; pre-aggregations take care of active indices.)
 */
public class IndexerImpl implements IndexerSpec {

    private Ignite ignite;
    private IgniteCache<String, Roaring64NavigableMap> cacheMap;

    /**
     * Time to id map.
     */
    private IgniteCache<String, Long> timeToIDMap;

    private final static String INDEXER_CACHE = "indexer_cache";

    public IndexerImpl(Server server) {
        ignite = server.getIgniteInstance();
        cacheMap = ignite.getOrCreateCache(INDEXER_CACHE);
    }

    @Override
    public void index(DateTime today, String key, String val, long[] rows) {
        String theKey = key(today, key, val);
        Lock lock = cacheMap.lock(theKey);
        lock.lock();
        Roaring64NavigableMap rr = cacheMap.get(theKey);
        rr.add(rows);
        rr.runOptimize();
        cacheMap.put(key(today, key, val), rr);
        lock.unlock();
    }

    /**
     * Utility to prepare the key.
     * @param key
     * @param val
     * @return
     */
    private String key(DateTime today, String key, String val) {
        return "p_"+ key+ "_v_" + val;
    }

    @Override
    public void index(DateTime today, String key, String val, long row) {
        index(today, key, val, new long[]{row});
    }

    @Override
    public void addTimeIndex(DateTime startTime, DateTime endTime, long startInclusive, long endExclusive) {
        timeToIDMap.put(endTime.toString(), endExclusive);
    }

    @Override
    public Map<DateTime, long[]> getRowIDs(Map<String, String> filters, Interval interval) {
        return null;
    }
}
