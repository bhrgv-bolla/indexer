package org.bbolla.db.storage.impl;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.bbolla.db.utils.JsonUtils;
import org.joda.time.DateTime;

/**
 * @author bbolla on 11/22/18
 */
public class DayPartitionAffinityFunction extends RendezvousAffinityFunction {

    public DayPartitionAffinityFunction() {
        super();
        setExcludeNeighbors(true);
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
        String rowKey = (String) key;
        DateTime date = JsonUtils.deserialize(rowKey, StorageImpl.RowKey.class).date();
        return date.getDayOfYear() % super.partitions();
    }
}
