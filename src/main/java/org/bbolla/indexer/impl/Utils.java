package org.bbolla.indexer.impl;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author bbolla on 11/19/18
 */
public class Utils {
    public static <K> String toString(Map<K, long[]> input) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n{\n");
        for(Map.Entry<K, long[]> entry : input.entrySet()) {
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

    public static List<DateTime> getDays(Interval interval) {
        List<DateTime> dates = Lists.newArrayList();
        for(DateTime current = interval.getStart().withTimeAtStartOfDay(); current.isBefore(interval.getEnd()); current = current.plusDays(1)) {
            dates.add(current);
        }
        return dates;
    }
}
