package org.bbolla.db.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author bbolla on 12/10/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Row {

    private Map<String, String> indexMap;
    private String payload;
}
