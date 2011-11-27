package com.github.mairbek.zoo.demo;

import com.github.mairbek.zoo.Zoo;
import com.github.mairbek.zoo.ZooClient;
import com.github.mairbek.zoo.ZooLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockDemo {
    private static final Logger LOG = LoggerFactory.getLogger(LockDemo.class);

    public static void main(String[] args) {

        Zoo zoo = new ZooClient()
                .endpoint("localhost:2181")
                .timeout(2000)
                .connect();

        ZooLock lock = new ZooLock(zoo, "/lock-demo");

        LOG.info("Starting process");

        lock.lock();

        LOG.info("Doing work");
        sleep(10 * 1000);
        LOG.info("Doing work");

        lock.unlock();

        LOG.info("Ended process");

    }

    private static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


}
