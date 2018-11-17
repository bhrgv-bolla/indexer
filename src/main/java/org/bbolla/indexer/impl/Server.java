package org.bbolla.indexer.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class Server {

    private Ignite ignite;

    public Server() {
        ignite = Ignition.start();
    }

    Ignite getIgniteInstance() {
        return ignite;
    }


}
