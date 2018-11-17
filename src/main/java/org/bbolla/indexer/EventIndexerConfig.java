package org.bbolla.indexer;

import org.bbolla.indexer.impl.IndexerImpl;
import org.bbolla.indexer.specification.IndexerSpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventIndexerConfig {

    @Bean
    public IndexerSpec indexer() {
        return new IndexerImpl();
    }
}
