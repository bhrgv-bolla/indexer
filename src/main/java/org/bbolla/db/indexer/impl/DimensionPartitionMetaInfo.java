package org.bbolla.db.indexer.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Holds dimension partition meta info.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimensionPartitionMetaInfo {
    private long[] range;
    private int partitionNumber;
    private int sizeInBytes;
}
