package com.github.mairbek.zoo;

import com.google.common.base.Throwables;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public class DefaultZoo implements Zoo {
    private final ZooKeeper zooKeeper;

    public DefaultZoo(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public ZNodeBuilder nodeBuilder() {
        return new ZNodeBuilderImpl(zooKeeper);
    }

    @Override
    public void remove(String path) {
        try {
            zooKeeper.delete(path, -1);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } catch (KeeperException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> children(String path) {
        return children(path, null);
    }

    @Override
    public List<String> children(String path, Watcher watcher) {
        try {
            return zooKeeper.getChildren(path, watcher);
        } catch (KeeperException e) {
            throw Throwables.propagate(e);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean exists(String path, Watcher watcher) {
        try {
            return zooKeeper.exists(path, watcher) != null;
        } catch (KeeperException e) {
            throw Throwables.propagate(e);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class ZNodeBuilderImpl implements ZNodeBuilder {
        private final ZooKeeper zooKeeper;
        private String path;
        private int ephemeral = 0;
        private int sequential = 0;
        private byte[] data = "".getBytes();
        private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;


        private ZNodeBuilderImpl(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
        }

        @Override
        public ZNodeBuilder path(String path) {
            this.path = path;
            return this;
        }

        @Override
        public ZNodeBuilder ephemeral() {
            this.ephemeral = 1;
            return this;
        }

        @Override
        public ZNodeBuilder sequential() {
            this.sequential = 2;
            return this;
        }

        @Override
        public ZNodeBuilder data(byte[] data) {
            this.data = data;
            return this;
        }

        @Override
        public ZNodeBuilder data(String data) {
            this.data = data.getBytes();
            return this;
        }

        @Override
        public ZNodeBuilder acl(List<ACL> acl) {
            this.acl = acl;
            return this;
        }

        @Override
        public String build() {
            int flag = ephemeral + sequential;
            try {
                return zooKeeper.create(path, data, acl, CreateMode.fromFlag(flag));
            } catch (KeeperException e) {
                throw Throwables.propagate(e);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
