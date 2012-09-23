package com.github.mairbek.zoo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZooLock implements Lock {
    private static final String DEFAULT_LOCK_NODE_PREFIX = "lock--";
    private final ThreadLocal<String> unlockNode = new ThreadLocal<String>();

    private final Zoo zoo;
    private final String path;
    private final String prefix;
    private Predicate<String> matchPrefixF = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            return input.startsWith(prefix);
        }
    };

    public ZooLock(Zoo zoo, String path) {
        this(zoo, path, DEFAULT_LOCK_NODE_PREFIX);
    }

    public ZooLock(Zoo zoo, String path, String prefix) {
        this.zoo = zoo;
        this.path = path;
        this.prefix = prefix;
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (isLocked()) {
            return;
        }

        String lockNode = createLockEntry();

        toUnlock(lockNode);

        while (true) {

            Collection<String> children = Collections2.filter(zoo.children(path), matchPrefixF);

            String closestNode = closestNode(lockNode, children);

            if (Objects.equal(closestNode, lockNode)) {
                break;
            }

            final CountDownLatch signal = new CountDownLatch(1);

            boolean exists = zoo.exists(closestNode, new Watcher() {

                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        signal.countDown();
                    }
                }

            });

            if (exists) {
                signal.await();
            }
        }

    }

    @Override
    public boolean tryLock() {
        if (isLocked()) {
            return true;
        }

        String lockNode = createLockEntry();
        List<String> children = zoo.children(path);
        String closest = closestNode(lockNode, children);

        if (Objects.equal(closest, lockNode)) {
            toUnlock(lockNode);
            return true;
        }

        zoo.remove(lockNode);
        return false;
    }

    @Override
    public void unlock() {
        String path = unlockNode.get();
        if (path != null) {
            zoo.remove(path);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private boolean isLocked() {
        return unlockNode.get() != null;
    }

    private String createLockEntry() {
        return zoo.nodeBuilder()
                .path(valuePrefix())
                .ephemeral()
                .sequential()
                .build();
    }

    private String closestNode(String node, Collection<String> children) {
        int nodeVal = valueOf(node, path + "/" + prefix);

        int min = Integer.MAX_VALUE;
        String minNode = null;
        for (String child : children) {
            int val = valueOf(child, prefix);
            if (val <= nodeVal && val < min) {
                min = val;
                minNode = child;
            }
        }
        return path + "/" + minNode;

    }

    private String valuePrefix() {
        return path + "/" + prefix;
    }

    private int valueOf(String node, String prefix) {
        int i = node.indexOf(prefix);
        String substring = node.substring(i + prefix.length());
        return Integer.valueOf(substring);
    }


    private void toUnlock(String node) {
        unlockNode.set(node);
    }
}
