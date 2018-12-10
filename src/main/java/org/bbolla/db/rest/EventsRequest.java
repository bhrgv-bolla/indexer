package org.bbolla.db.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * @author bbolla on 12/10/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventsRequest {

    private long[] rowRange;
    private Map<Long, String> rows;
    private String timestamp;
    private DimensionBitmap[] dimensionBitmaps;


    public DateTime timestamp() {
        return DateTime.parse(timestamp);
    }


}
