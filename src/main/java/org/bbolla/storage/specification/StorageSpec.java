package org.bbolla.storage.specification;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;

/**
 * @author bbolla on 12/9/18
 */
public interface StorageSpec {

    void store(DateTime date, Map<Long, String> rows); //continually incrementing rows.

    default void store(Map<Long, String> rows) {
        store(DateTime.now().withTimeAtStartOfDay(), rows);
    }

    Map<Long, String> retrieveRows(Set<Long> rowIds);

}
