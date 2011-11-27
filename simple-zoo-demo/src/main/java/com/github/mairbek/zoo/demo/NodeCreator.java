package com.github.mairbek.zoo.demo;

import com.github.mairbek.zoo.Zoo;
import com.github.mairbek.zoo.ZooClient;

public class NodeCreator {

    public static void main(String[] args) {
        String node = "/election-demo";

        Zoo zoo = new ZooClient()
                .endpoint("localhost:2181")
                .timeout(2000)
                .connect();

        if (zoo.exists(node, null)) {
            zoo.remove(node);
        }
        zoo.nodeBuilder().path(node).build();
    }
}
