package com.github.mairbek.zoo;

import org.apache.zookeeper.data.ACL;

import java.util.List;

public interface ZNodeBuilder {
    ZNodeBuilder path(String path);

    ZNodeBuilder ephemeral();

    ZNodeBuilder sequential();

    ZNodeBuilder data(byte[] data);

    ZNodeBuilder data(String data);

    ZNodeBuilder acl(List<ACL> acl);

    String build();
}
