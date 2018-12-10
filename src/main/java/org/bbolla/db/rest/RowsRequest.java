package org.bbolla.db.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.Interval;

import java.util.Map;

/**
 * @author bbolla on 12/10/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RowsRequest {

    private Map<String, String> filters;
    private String interval;

    public Interval interval() {
        return Interval.parse(interval);
    }

}
