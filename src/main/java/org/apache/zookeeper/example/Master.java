package org.apache.zookeeper.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {
    private final String host;

    private boolean isLeader = false;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private ZooKeeper zooKeeper;

    public Master(String host) {
        this.host = host;
    }

    private boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                byte masterId[] = zooKeeper.getData("/master", false, new Stat());
                isLeader = new String(masterId).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                return false;
            } catch (KeeperException.ConnectionLossException e) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    private void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zooKeeper.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }

            if (checkMaster()) break;
        }
    }

    private void startZooKeeper() throws Exception {
        zooKeeper = new ZooKeeper(host, 15000, this);
    }

    private void stopZooKeeper() throws Exception {
        zooKeeper.close();
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String args[]) throws Exception {
        Master master = new Master(args[0]);
        master.startZooKeeper();
        master.runForMaster();
        if (isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }

        master.stopZooKeeper();
    }
}
