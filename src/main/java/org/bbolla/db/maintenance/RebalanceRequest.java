package org.bbolla.db.maintenance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author bbolla on 12/26/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RebalanceRequest {
    String[] consistendIds;
}
