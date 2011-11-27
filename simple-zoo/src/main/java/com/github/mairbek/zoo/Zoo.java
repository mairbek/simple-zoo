package com.github.mairbek.zoo;

import org.apache.zookeeper.Watcher;

import java.util.List;

public interface Zoo {
    ZNodeBuilder nodeBuilder();

    void remove(String path);

    List<String> children(String path);

    public List<String> children(String path, Watcher watcher);

    boolean exists(String path, Watcher watcher);
}
