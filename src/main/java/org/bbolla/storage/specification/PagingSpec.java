package org.bbolla.storage.specification;

import org.joda.time.DateTime;

import java.util.Set;

/**
 * @author bbolla on 12/9/18
 */
public interface PagingSpec {

     Set<PageInfo> getPageIds(DateTime dateTime, Set<Long> rowsIds);
}
