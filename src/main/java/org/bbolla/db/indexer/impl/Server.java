package org.bbolla.db.indexer.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

//TODO Seperate cluster maintenance functions like cluster active. change base line topology (etc)
//TODO ***TOP PRIORITY*** topology management topology reports.
@Slf4j
public class Server {

    private Ignite ignite;

    public Server() {
        this("localhost:2181", "/testPath");
        log.warn("!!!Starting in default mode; Don't start server using this constructor in production");
    }

    public Server(String zookeeperHosts, String zkRootPath) {

        // Apache Ignite node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Ignite persistence configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Enabling the persistence.
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //start SPI
        ZookeeperDiscoverySpi zkDiscoSpi = new ZookeeperDiscoverySpi();

        zkDiscoSpi.setZkConnectionString(zookeeperHosts);

        zkDiscoSpi.setSessionTimeout(30000);

        zkDiscoSpi.setZkRootPath(zkRootPath);

        zkDiscoSpi.setJoinTimeout(10_000); //TODO ability to manage these settings from outside??

        cfg.setDiscoverySpi(zkDiscoSpi);
        //end SPI


        // Applying settings.
        cfg.setDataStorageConfiguration(storageCfg);

        ignite = Ignition.start(cfg);

        ignite.cluster().active(true);

        log.warn("Storage directory set to : {}", cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration());
    }

    public Ignite getIgniteInstance() {
        return ignite;
    }


}
