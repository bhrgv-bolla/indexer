package org.bbolla.db;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * TODO
 *  1. Better Timer Indexer Impl. (May be more checks for now??)
 *  2. If the whole cluster should fail; how will the cluster be rebuilt from the backing store.
 *  3. How would across dc replication happen. ( How ?? ) Replay changes; build from backup.??
 *  4. ABOUT BACKUP AND REBUILDING THE WHOLE CLIUSTER:
 *      How would you check point what is written and what is not written before failure. If need be; how would you replay.
 *      How would you decide what data is lost; what data needs to be replayed? Replay entire data initially
 *  5. Is this system idempotent?? yes
 */
@EnableSwagger2
@SpringBootApplication
public class EventIndexerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventIndexerApplication.class, args);
    }
}
