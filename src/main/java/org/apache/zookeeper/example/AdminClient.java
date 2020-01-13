package org.apache.zookeeper.example;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

public class AdminClient implements Watcher {
    private final String host;
    private ZooKeeper zooKeeper;

    public AdminClient(String host) {
        this.host = host;
    }

    private void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte master[] = zooKeeper.getData("/master", false, stat);
            System.out.println("Master: " + new String(master) + " since " + new Date(stat.getCtime()));
        } catch (KeeperException.NoNodeException ex) {
            System.out.println("No Master");
        }

        System.out.println("Workers:");
        for (String worker : zooKeeper.getChildren("/workers", false)) {
            System.out.println("\t" + worker + ": " +
                    new String(zooKeeper.getData("/workers/" + worker, false, null)));
        }

        System.out.println("Tasks:");
        for (String task : zooKeeper.getChildren("/assign", false)) {
            System.out.println("\t" + task);
        }
    }

    private void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(host, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String args[]) throws Exception {
        AdminClient adminClient = new AdminClient(args[0]);

        adminClient.startZooKeeper();

        adminClient.listState();
    }
}
