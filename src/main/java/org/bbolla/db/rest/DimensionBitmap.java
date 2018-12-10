package org.bbolla.db.rest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bbolla.db.indexer.impl.SerializedBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * @author bbolla on 12/10/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimensionBitmap {

    private String dimensionKey;
    private String dimensionValue;
    private SerializedBitmap bitmap;


    @JsonIgnore
    public Roaring64NavigableMap bitmap() {
        return bitmap.toBitMap();
    }
}
