package org.bbolla.db;

import org.bbolla.db.indexer.specification.IndexerSpec;
import org.bbolla.db.storage.specification.StorageSpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventIndexerConfig {

    @Bean
    public IndexerSpec indexer() {
        return null;
//        return new IndexerImpl();
    }


    @Bean
    public StorageSpec storage() {
        return null;
    }
}
