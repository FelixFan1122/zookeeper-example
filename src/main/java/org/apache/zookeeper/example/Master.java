package org.apache.zookeeper.example;

import org.apache.zookeeper.*;

import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {
    private final String host;

    private boolean isLeader = false;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private ZooKeeper zooKeeper;

    private AsyncCallback.DataCallback masterCheckCallBack = (code, path, context, data, stat) -> {
        switch (KeeperException.Code.get(code)) {
            case NONODE:
                runForMaster();
                return;
            case CONNECTIONLOSS:
                checkMaster();
                return;
        }
    };

    private AsyncCallback.StringCallback masterCreateCallBack = (code, path, context, name) -> {
        switch (KeeperException.Code.get(code)) {
            case OK:
                isLeader = true;
                break;
            case CONNECTIONLOSS:
                checkMaster();
                return;
            default:
                isLeader = false;
        }

        System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader");
    };

    public Master(String host) {
        this.host = host;
    }

    private void checkMaster(){
        zooKeeper.getData("/master", false, masterCheckCallBack, null);
    }

    private void runForMaster() {
            zooKeeper.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                    masterCreateCallBack, null);
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
        if (master.isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }

        master.stopZooKeeper();
    }
}
