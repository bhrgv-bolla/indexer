package org.bbolla.db.indexer.impl;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author bbolla on 11/22/18
 */
@Slf4j
public class CacheKeyUtils {

    private static final String INDEX_KEY_REGEX = "d_(?<date>.*)_p_(.*)_v_(.*)_pn_(.*)";
    private static final Pattern pattern;

    static {
        pattern = Pattern.compile(INDEX_KEY_REGEX);
    }

    /**
     * Parse the date part out of the key
     * @param key
     * @return
     */
    public static DateTime parseDate(String key) {
        Matcher matcher = pattern.matcher(key);
        if(matcher.matches()) {
            String date = matcher.group("date");
            DateTime result = DateTime.parse(date);
            return result;
        } else {
            log.error("Couldn't find a match for key: {} with regex: {}", key, INDEX_KEY_REGEX);
            throw new RuntimeException("Not a valid key; didn't find a match");
        }
    }


    public static void main(String[] args) {
        String key = "d_2018-11-22T00:00:00.000+05:30_p_test_v_24_pn_0";
        log.info("Parsed date from key: {}", parseDate(key));
    }
}
