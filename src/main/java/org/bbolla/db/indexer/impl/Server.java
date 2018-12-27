package org.bbolla.db.indexer.impl;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

        log.warn("Storage directory set to : {}", cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration());
    }

    public Ignite getIgniteInstance() {
        return ignite;
    }

    public boolean activate() {
        ignite.cluster().active(true);
        return true;
    }

    public boolean deactivate() {
        ignite.cluster().active(false);
        return true;
    }

    /**
     * Add node to baseline topology
     * @param nodeId
     * @return
     */
    public Object addNode(String nodeId) {
        ignite.cluster().currentBaselineTopology();
        throw new RuntimeException("Pending Implementation");
    }


    public Object rebalance() {
        Collection<ClusterNode> aliveNodes = ignite.cluster().forServers().nodes();
        Set<String> aliveSet = ids(aliveNodes);
        Set<String> existingSet = ids(ignite.cluster().currentBaselineTopology());
        if(aliveSet.equals(existingSet)) {
            return false;
        } else {
            Object before = nodes();
            ignite.cluster()
                    .setBaselineTopology(
                            aliveNodes
                                    .stream()
                                    .filter(n -> !n.isClient())
                                    .collect(Collectors.toList())
                    );
            Object after = nodes();
            return ImmutableMap.of("before", before, "after", after);
        }
    }

    private static <T extends BaselineNode> Set<String> ids(Collection<T> clusterGroup) {
        return clusterGroup.stream().map(n -> n.consistentId().toString()).collect(Collectors.toSet());
    }

    public Object removeNode(String nodeId) {
        throw new RuntimeException("yet to be implemented");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Node {
        String ip;
        String id;
        String type;
        String hostname;
        String order;
    }


    public Object nodes() {
        return ignite.cluster().nodes().stream().map(c -> {
            Node n = new Node();
            n.setId(c.consistentId().toString());
            n.setIp(c.addresses().toString());
            n.setHostname(c.hostNames().toString());
            n.setOrder(String.valueOf(c.order()));
            n.setType(c.isClient()? "client": "server"); //TODO only two types for now. Review later.
            return n;
        }).collect(Collectors.toList());
    }

    public boolean isActive() {
        return ignite.cluster().active();
    }
}
