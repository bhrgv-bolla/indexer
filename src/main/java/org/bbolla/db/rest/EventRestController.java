package org.bbolla.db.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
        for(DimensionBitmap dim : request.getDimensionBitmaps()) {
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
    public ResponseEntity<Object> deleteRows(@RequestParam("rowKey") String row) {
        RowKey rowKey = RowKey.fromJson(row);
        indexer.deleteRows(rowKey.getTimestamp(), new long[] {rowKey.getRowId()});
        return ResponseEntity.ok("Delete Successful");
    }

    /**
     * Uodates a record; TODO today doesn't check if it already exists or not. What checks should be made?
     * @param key
     * @param record
     * @return
     */
    @PutMapping("/update/{rowKey}")
    public ResponseEntity<Object> updateRow(@RequestParam("rowKey") String key, @RequestBody String record) {
        RowKey rowKey = RowKey.fromJson(key);
        storage.store(rowKey.getTimestamp(), ImmutableMap.of(rowKey.getRowId(), record));
        return ResponseEntity.ok("Success");
    }


    @PostMapping("/read")
    public ResponseEntity<Object> getPaginatedRecords(@RequestBody RowsRequest request,
                                                      @RequestParam("pageNum") int pageNum,
                                                      @RequestParam(value = "pageSize", defaultValue = "10", required = false) int pageSize,
                                                      @RequestParam(value= "descending", defaultValue = "true", required = false) boolean desc) {
        if(pageSize > MAXIMUM_PAGE_SIZE) return ResponseEntity.badRequest().body("Page size should be not more than : "+ MAXIMUM_PAGE_SIZE);
        if(pageNum <= 0 || pageSize <=0) return ResponseEntity.badRequest().body("Page details should not be negative");
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

    private Map<DateTime,long[]> filterRowsInPage(Map<DateTime, long[]> rows, int pageNum, int pageSize, boolean desc) {
        List<DateTime> dates = Lists.newArrayList(rows.keySet());
        if(desc) Collections.sort(dates, Collections.reverseOrder());
        else Collections.sort(dates);

        int[] rowsInDays = new int[dates.size()];

        for(int i=0; i<dates.size(); i++) rowsInDays[i] = rows.get(dates.get(i)).length;

        long totalRecords = IntStream.of(rowsInDays).asLongStream().sum();

        if(totalRecords <= (long) pageSize * (pageNum - 1)) { // no records in this page.
            return Maps.newHashMap(); //empty record set.
        } else { //records exist to serve.
            Map<DateTime, long[]> result = Maps.newHashMap();
            //lower bound; upper bound.
            //
        }

    }


}
