package org.bbolla.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bbolla.db.indexer.impl.IndexerImpl;
import org.bbolla.db.indexer.impl.Server;
import org.bbolla.db.indexer.impl.TimeIndexerImpl;
import org.bbolla.db.indexer.specification.IndexerSpec;
import org.bbolla.db.indexer.specification.TimeIndexerSpec;
import org.bbolla.db.storage.impl.StorageImpl;
import org.bbolla.db.storage.specification.StorageSpec;
import org.bbolla.db.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventIndexerConfig {

    @Value("${zookeeper.hosts}")
    private String zkHosts;

    @Value("${zookeeper.root}")
    private String zkRootPath;

    @Bean
    public ObjectMapper om() {
        return JsonUtils.om();
    }

    @Bean
    public Server server() {
        return new Server(zkHosts, zkRootPath);
    }

    @Bean
    public TimeIndexerSpec timeIndexer(Server server) {
        return new TimeIndexerImpl(server);
    }

    @Bean
    public IndexerSpec indexer(Server server, TimeIndexerSpec timeIndexer) {
        return new IndexerImpl(server, timeIndexer);
    }


    @Bean
    public StorageSpec storage(Server server) {
        return new StorageImpl(server.getIgniteInstance());
    }
}
