package org.bbolla.db.rest;

import org.bbolla.db.indexer.specification.IndexerSpec;
import org.bbolla.db.storage.specification.StorageSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.awt.*;

/**
 * @author bbolla on 12/10/18
 */
@RestController
public class EventRestController {


    @Autowired
    private IndexerSpec indexer;

    @Autowired
    private StorageSpec storage;


    @PostMapping
    public ResponseEntity<Object> postNewRows(@RequestBody EventsRequest request) {
        storage.store(request.timestamp(), request.getRows());
        indexer.addTimeIndex(request.timestamp(), request.timestamp(), request.getRowRange()[0], request.getRowRange()[1]);
        for(DimensionBitmap dim : request.getDimensionBitmaps()) {
            indexer.index(request.timestamp(), dim.getDimensionKey(), dim.getDimensionValue(), dim.bitmap());
        }

        return ResponseEntity.ok("Success");
    }

}
