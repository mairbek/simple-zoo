package com.github.mairbek.zoo;

import com.google.common.base.Throwables;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZooClient.class);
    
    private String endpoint;
    private int timeout;

    public ZooClient endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public ZooClient timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public Zoo connect() {
        return new DefaultZoo(connectToZooKeeper());
    }

    private ZooKeeper connectToZooKeeper() {
        LOG.info("Connecting to zookeeper {} timeout {}", endpoint, timeout);
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            ZooKeeper zooKeeper = new ZooKeeper(endpoint, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });

            try {
                connectedSignal.await();
                LOG.info("Connected to zookeeper");
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }

            return zooKeeper;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
