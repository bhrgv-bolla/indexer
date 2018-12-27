package org.bbolla.db.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bbolla.db.indexer.impl.Utils;
import org.bbolla.db.indexer.specification.IndexerSpec;
import org.bbolla.db.indexer.specification.TimeIndexerSpec;
import org.bbolla.db.storage.specification.StorageSpec;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.IntStream;

/**
 * @author bbolla on 12/10/18
 */
@RestController
@Slf4j
public class EventRestController {


    @Autowired
    private IndexerSpec indexer;

    @Autowired
    private StorageSpec storage;

    @Autowired
    private TimeIndexerSpec timeIndexer;

    private static final int MAXIMUM_PAGE_SIZE = 500;


    @PostMapping("/submit/events")
    public ResponseEntity<Object> postNewRows(@RequestBody EventsRequest request) { //TODO will miss what dimensions are being indexed.
        storage.store(request.timestamp(), request.getRows());
        indexer.addTimeIndex(request.timestamp(), request.timestamp(), request.getRowRange()[0], request.getRowRange()[1]);
        for (DimensionBitmap dim : request.getDimensionBitmaps()) {
            indexer.index(request.timestamp(), dim.getDimensionKey(), dim.getDimensionValue(), dim.bitmap());
        }

        return ResponseEntity.ok("Success");
    }

    @PostMapping("/test/submit/events")
    public ResponseEntity<Object> postNewRowsWithoutPartialDimensions(@RequestBody EventsRequestV2 request) {
        return postNewRows(new EventsRequest(request.getRowRange(), request.rows(), request.getTimestamp(), request.dimensionBitmaps()));
    }


    @PostMapping("/read/rowIds")
    public ResponseEntity<Object> getRowIds(@RequestBody RowsRequest request) {
        Map<DateTime, long[]> rows = indexer.getRowIDs(request.getFilters(), request.interval());
        return ResponseEntity.ok(rows);
    }


    @PostMapping("/read/rows")
    public ResponseEntity<Object> getRows(@RequestBody RowsRequest request) {
        Map<DateTime, long[]> rows = indexer.getRowIDs(request.getFilters(), request.interval());
        Map<String, String> result = Maps.newHashMap();
        rows.forEach(
                (k, v) -> {
                    Set<Long> ids = Sets.newHashSet();
                    Arrays.stream(v).forEach(l -> ids.add(l));
                    Map<DateTime, Map<Long, String>> lResult = storage.retrieveRows(k, ids);
                    lResult.forEach(
                            (date, rowMap) ->
                                    rowMap.forEach(
                                            (id, payload) -> {
                                                RowKey key = new RowKey(date, id);
                                                result.put(key.json(), payload);
                                            }
                                    )

                    );
                }
        );

        return ResponseEntity.ok(result);
    }


    @PostMapping("/read/allRowIdsInInterval")
    public ResponseEntity<Object> getRowIdsInInterval(@RequestBody RowsRequest request) {
        Map<String, long[]> rows = timeIndexer.getAllRowsInAnInterval(request.interval());
        return ResponseEntity.ok(rows);
    }

    @DeleteMapping("/delete/{rowKey}")
    public ResponseEntity<Object> deleteRows(@PathVariable("rowKey") String row) {
        RowKey rowKey = RowKey.fromJson(row);
        indexer.deleteRows(rowKey.getTimestamp(), new long[]{rowKey.getRowId()});
        return ResponseEntity.ok("Delete Successful");
    }

    /**
     * Uodates a record; TODO today doesn't check if it already exists or not. What checks should be made?
     *
     * @param key
     * @param record
     * @return
     */
    @PutMapping("/update/{rowKey}")
    public ResponseEntity<Object> updateRow(@PathVariable("rowKey") String key, @RequestBody String record) {
        RowKey rowKey = RowKey.fromJson(key);
        storage.store(rowKey.getTimestamp(), ImmutableMap.of(rowKey.getRowId(), record));
        return ResponseEntity.ok("Success");
    }


    @PostMapping("/read")
    public ResponseEntity<Object> getPaginatedRecords(@RequestBody RowsRequest request,
                                                      @RequestParam("pageNum") int pageNum,
                                                      @RequestParam(value = "pageSize", defaultValue = "10", required = false) int pageSize,
                                                      @RequestParam(value = "descending", defaultValue = "true", required = false) boolean desc) {
        if (pageSize > MAXIMUM_PAGE_SIZE)
            return ResponseEntity.badRequest().body("Page size should be not more than : " + MAXIMUM_PAGE_SIZE);
        if (pageNum <= 0 || pageSize <= 0)
            return ResponseEntity.badRequest().body("Page details should not be negative");
        Map<DateTime, long[]> rows = indexer.getRowIDs(request.getFilters(), request.interval());
        Map<DateTime, long[]> pageRows = filterRowsInPage(rows, pageNum, pageSize, desc);
        Map<String, String> result = Maps.newHashMap();
        pageRows.forEach(
                (k, v) -> {
                    Set<Long> ids = Sets.newHashSet();
                    Arrays.stream(v).forEach(l -> ids.add(l));
                    Map<DateTime, Map<Long, String>> lResult = storage.retrieveRows(k, ids);
                    lResult.forEach(
                            (date, rowMap) ->
                                    rowMap.forEach(
                                            (id, payload) -> {
                                                RowKey key = new RowKey(date, id);
                                                result.put(key.json(), payload);
                                            }
                                    )

                    );
                }
        );

        return ResponseEntity.ok(result);
    }

    //TODO ***URGENT*** add endpoint for updating / adding an index. (maintainence endpoints; not for consumer)

    //TODO ***URGENT*** for reporting row ranges (time indexer).

    //TODO ***URGENT*** add endpoint for adding a new record(s).

    private static Map<DateTime, long[]> filterRowsInPage(Map<DateTime, long[]> rows, int pageNum, int pageSize, boolean desc) {
        List<DateTime> dates = Lists.newArrayList(rows.keySet());
        if (desc) Collections.sort(dates, Collections.reverseOrder());
        else Collections.sort(dates);

        int[] rowsInDays = new int[dates.size()];

        for (int i = 0; i < dates.size(); i++) rowsInDays[i] = rows.get(dates.get(i)).length;

        long totalRecords = IntStream.of(rowsInDays).asLongStream().sum();

        if (totalRecords <= (long) pageSize * (pageNum - 1)) { // no records in this page.
            return Maps.newHashMap(); //empty record set.
        } else { //records exist to serve.
            Map<DateTime, long[]> result = Maps.newHashMap();
            //lower bound; upper bound.
            int remaining = pageSize;
            int offset = (pageNum - 1) * pageSize;// read from here.
            for (int i = 0; i < rowsInDays.length; i++) {
                if (remaining == 0) break;

                int rowsInCurrentDay = rowsInDays[i];
                DateTime currentDate = dates.get(i);
                long[] currentRows = rows.get(currentDate);
                //sort desc / ascending; Cannot sort primitive arrays in descending order without converting to boxed type.
                Arrays.sort(currentRows);

                if (offset > rowsInCurrentDay) offset -= rowsInCurrentDay;
                else if (offset + remaining <= rowsInCurrentDay) {
                    long[] rowsInPage = select(offset, offset + remaining, currentRows, desc);
                    result.put(currentDate, rowsInPage);
                    remaining = 0;
                    offset = 0;
                } else { //offset + remaining is greater than current rows in the day. so partially, fulfill and move to the next day
                    //update remaining and offset
                    //Keep on reducing remaining
                    if(rowsInCurrentDay == 0) continue; //nothing to read in this day
                    int start = offset;
                    int readUntil = rowsInCurrentDay;
                    int recordsRead = readUntil - start;
                    remaining = remaining - recordsRead;
                    offset = 0;
                    long[] rowsInPage = select(start, readUntil, currentRows, desc);
                    result.put(currentDate, rowsInPage);
                }
            }

            return result;
        }


    }

    private static long[] select(int startFrom, int readUntil, @NonNull long[] currentRows, boolean desc) {
        int resultLength = readUntil - startFrom;
        if (resultLength > currentRows.length)
            throw new IllegalArgumentException("result length cannot be greater than current rows length");
        if (startFrom < 0 || readUntil < 1)
            throw new IllegalArgumentException("cannot have -ve values for specifying range");
        if (desc) { // read from the back.
            long[] result = new long[resultLength];
            int startIndex = currentRows.length - startFrom - 1;
            int endIndex = currentRows.length - readUntil;
            for (int i = startIndex, idx = 0; i >= endIndex; i--, idx++) {
                result[idx] = currentRows[i];
            }
            return result;
        } else {
            return Arrays.copyOfRange(currentRows, startFrom, readUntil);
        }
    }


    public static void main(String[] args) {
        log.info("result : {}", Arrays.toString(select(0, 3, new long[]{2, 3, 4}, true)));

        Map<DateTime, long[]> result = filterRowsInPage(ImmutableMap.of(DateTime.now(), new long[]{1, 2, 3},
                DateTime.now().minusDays(1), new long[]{}),
                1, 2, true);

        log.info("result :{}", Utils.toString(result));
    }


}
