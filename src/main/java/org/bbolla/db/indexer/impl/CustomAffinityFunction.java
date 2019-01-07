package org.bbolla.db.indexer.impl;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.joda.time.DateTime;

/**
 * @author bbolla on 11/22/18
 */
public class CustomAffinityFunction extends RendezvousAffinityFunction {

    public CustomAffinityFunction() {
        super();
        setExcludeNeighbors(true); //If two instances are run from the same host; this flag will exclude setting a backup on the same host machine.
        setPartitions(366); //leap years; max days in an year; since doy is used for partitioning.
    }

    /**
     * Should return same again and again.
     * @param key
     * @return
     */
    @Override
    public int partition(Object key) { //0 to 1024 by default.
        if(!(key instanceof String)) throw new RuntimeException("Expectation failed!; expecting a string key " + key);
        String dateKey = (String) key;
        DateTime date = CacheKeyUtils.parseDate(dateKey);
        return date.getDayOfYear() % super.partitions();
    }
}
