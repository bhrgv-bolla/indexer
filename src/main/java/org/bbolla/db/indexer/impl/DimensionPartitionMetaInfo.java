package org.bbolla.db.indexer.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Arrays;

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

    /**
     * Adjusts row range.
     * @param rows
     */
    public void adjustRange(@NonNull Roaring64NavigableMap rows) {
        long[] allRows = rows.toArray();

        //edge case
        if(allRows.length == 0) return;

        Arrays.sort(allRows);
        long smallest = allRows[0], largest = allRows[allRows.length - 1];
        if(range == null) {
            this.range = new long[] {smallest, largest + 1};
        } else {
            this.range[1] = largest + 1;
        }
    }
}
