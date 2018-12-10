package org.bbolla.db.indexer.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class Server {

    private Ignite ignite;

    public Server() {

        // Apache Ignite node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Ignite persistence configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Enabling the persistence.
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        // Applying settings.
        cfg.setDataStorageConfiguration(storageCfg);

        ignite = Ignition.start(cfg);

        ignite.cluster().active(true);
    }

    public Ignite getIgniteInstance() {
        return ignite;
    }


}
