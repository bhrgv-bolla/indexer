package org.bbolla.db.storage.impl;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.bbolla.db.indexer.impl.SerializedBitmap;
import org.bbolla.db.indexer.impl.Server;
import org.bbolla.db.storage.specification.StorageSpec;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author bbolla on 12/9/18
 */
@Slf4j
public class StorageImpl implements StorageSpec {

    private Ignite ignite;

    private final IgniteCache<RowKey, String> storage;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class RowKey {
        private long id;

        @AffinityKeyMapped
        private String date;

        DateTime date() {
            return DateTime.parse(date);
        }
    }

    StorageImpl(Ignite ignite) {
        this.ignite = ignite;


        CacheConfiguration config = new CacheConfiguration<String, SerializedBitmap>();
        config.setName("row_storage");
        config.setCacheMode(CacheMode.PARTITIONED);
        config.setBackups(2);
        config.setReadFromBackup(true);
        config.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        this.storage = ignite.getOrCreateCache(config);
    }

    @Override
    public void store(DateTime date, Map<Long, String> rows) {
        Map<RowKey, String> newRows = Maps.newHashMap();

        rows.forEach(
                (k, v) -> newRows.put(new RowKey(k, date.withTimeAtStartOfDay().toString()), v)
        );

        storage.putAll(newRows);
    }

    @Override
    public Map<DateTime, Map<Long, String>> retrieveRows(DateTime date, Set<Long> rowIds) {
        //TODO create keys

        Set<RowKey> keys = Sets.newHashSet();

        rowIds.forEach(
                id -> keys.add(new RowKey(id, date.withTimeAtStartOfDay().toString()))
        );

        Map<RowKey, String> allRows = storage.getAll(keys);

        Map<DateTime, Map<Long, String>> rows = Maps.newHashMap();

        allRows.forEach(
                (k, v) -> {
                    DateTime key = k.date();
                    if(! rows.containsKey(key)) rows.put(key, Maps.newHashMap());
                    rows.get(key).put(k.getId(), v);
                }
        );

        return rows;
    }

    public static void main(String[] args) {

        Random rand = new Random();
        Server server = new Server();
        StorageImpl impl = new StorageImpl(server.getIgniteInstance());
        Map<Long, String> rows = Maps.newHashMap();
        rows.put(rand.nextLong(), "test");

        Stopwatch stopwatch = Stopwatch.createStarted();
        impl.store(DateTime.now(), rows);


        Map<DateTime, Map<Long, String>> result = impl.retrieveRows(DateTime.now(), Sets.newHashSet(1L));

        stopwatch.stop();
        log.info("result : {}; elapsed: {} ms", result, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        impl.storage.forEach(
                e -> log.info("Entry: {}", e)
        );
        impl.ignite.close();
    }
}
