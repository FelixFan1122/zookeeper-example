package org.apache.zookeeper.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Master implements Watcher {
    private final String host;

    public Master(String host) {
        this.host = host;
    }

    private void startZooKeeper() throws Exception {
        new ZooKeeper(host, 15000, this);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String args[]) throws Exception {
        new Master(args[0]).startZooKeeper();
        Thread.sleep(60000);
    }
}
