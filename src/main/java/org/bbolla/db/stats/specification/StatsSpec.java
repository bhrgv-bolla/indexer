package org.bbolla.db.stats.specification;

import java.util.List;

/**
 * @author bbolla on 12/13/18
 */
public interface StatsSpec {


    /**
     * Insights into the partitions held local to the node.
     * @return
     */
    List<PartitionInfo> localPartitions();

    /**
     * Insights into the memory (heap, off heap) and disk usage
     * @return
     */
    StorageStats storageStats();


    /**
     * Insights into how the cluster.
     * @return
     */
    ClusterStats clusterStats();
}
