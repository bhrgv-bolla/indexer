package org.bbolla.db.rest;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bbolla.db.indexer.impl.SerializedBitmap;
import org.joda.time.DateTime;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Map;

/**
 * @author bbolla on 12/10/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventsRequestV2 {

    private long[] rowRange;
    private Map<Long, Row> rows;
    private String timestamp;


    public DateTime timestamp() {
        return DateTime.parse(timestamp);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class DimValKey {
        private String dim;
        private String val;
    }


    public Map<Long, String> rows() {
        Map<Long, String> result = Maps.newHashMap();
        rows.forEach(
                (k, v) -> result.put(k, v.getPayload())
        );
        return result;
    }

    public DimensionBitmap[] dimensionBitmaps() {
        Map<DimValKey, Roaring64NavigableMap> bitmaps = Maps.newHashMap();
        for (Map.Entry<Long, Row> e : rows.entrySet()) {
            Map<String, String> indexMap = e.getValue().getIndexMap();
            for (Map.Entry<String, String> dimVal : indexMap.entrySet()) {
                DimValKey key = new DimValKey(dimVal.getKey(), dimVal.getValue());
                if (bitmaps.get(key) == null) bitmaps.put(key, Roaring64NavigableMap.bitmapOf());
                bitmaps.get(key).add(e.getKey());
            }
        }
        DimensionBitmap[] dimensionBitmaps = new DimensionBitmap[bitmaps.size()];

        int current = 0;

        for (Map.Entry<DimValKey, Roaring64NavigableMap> e : bitmaps.entrySet()) {
            dimensionBitmaps[current++] = new DimensionBitmap(e.getKey().dim, e.getKey().val, SerializedBitmap.fromBitMap(e.getValue()));
        }

        return dimensionBitmaps;

    }

}
