package com.github.mairbek.zoo;

import com.google.common.base.Objects;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LeaderElector {

    public static interface Listener {

        void electedAsLeader();

    }

    private final Zoo zoo;
    private final String path;
    private final String prefix;

    public LeaderElector(Zoo zoo, String path) {
        this(zoo, path, "node-");
    }

    public LeaderElector(Zoo zoo, String path, String prefix) {
        this.zoo = zoo;
        this.path = path;
        this.prefix = prefix;
    }

    public void participate(Listener listener) {
        String electionNode = zoo.nodeBuilder().path(path + "/" + prefix)
                .ephemeral()
                .sequential()
                .build();


        while (true) {
            
            List<String> children = zoo.children(path);
            String minNode = findMinimal(children);
            
            if (Objects.equal(minNode, electionNode)) {
                listener.electedAsLeader();
                break;
            }

            final CountDownLatch signal = new CountDownLatch(1);

            boolean exists = zoo.exists(minNode, new Watcher() {

                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        signal.countDown();
                    }
                }

            });

            if (exists) {
                try {
                    signal.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            
        }

    }

    private String findMinimal(List<String> children) {
        int min = Integer.MAX_VALUE;
        String minNode = null;
        for (String child : children) {
            int val = valueOf(child, prefix);
            if (val < min) {
                min = val;
                minNode = child;
            }
        }
        return path + "/" + minNode;
        
    }
    
    private int valueOf(String node, String prefix) {
        int i = node.indexOf(prefix);
        String substring = node.substring(i + prefix.length());
        return Integer.valueOf(substring);
    }
    
}

