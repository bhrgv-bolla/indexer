package org.bbolla.indexer.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

/**
 * @author bbolla on 11/22/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FilterResult {
    private String date;
    private SerializedBitmap rows;

    public DateTime date() {
        return DateTime.parse(date);
    }
}
