package org.bbolla.db.rest;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

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



}
