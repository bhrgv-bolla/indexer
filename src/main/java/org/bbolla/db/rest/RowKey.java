package org.bbolla.db.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

/**
 * @author bbolla on 12/21/18
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RowKey extends JSONEnabled<RowKey> {

    private DateTime timestamp;
    private Long rowId;

}
