package org.bbolla.indexer.impl;

import java.util.Arrays;
import java.util.Map;

/**
 * @author bbolla on 11/19/18
 */
public class Utils {
    public static String toString(Map<String, long[]> input) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n{\n");
        for(Map.Entry<String, long[]> entry : input.entrySet()) {
            sb.append("\t");
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(Arrays.toString(entry.getValue()));
            sb.append(",");
            sb.append("\n");
        }
        sb.append("}");

        return sb.toString();
    }
}
