package org.bbolla.db.maintenance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Enables backup.
 * TODO use this to sync the keys to a db; to recover in case of complete cluster failure / catastrophies. (without minimum downtime)
 * @author bbolla on 12/27/18
 */
@RestController
@Slf4j
@RequestMapping("/maintenance/backup")
public class BackupRestController {

    @PostMapping("/cache")
    public ResponseEntity<CacheEntryResponse> getEntry(@RequestBody CacheKeyRequest cacheKey) {
        //TODO use caches.
        return null;
    }

}
