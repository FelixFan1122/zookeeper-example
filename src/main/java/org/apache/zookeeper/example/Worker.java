package org.apache.zookeeper.example;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Worker implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    
    private String host;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private String status;
    private ZooKeeper zooKeeper;
    private AsyncCallback.StringCallback createWorkerCallback = (code, path, context, name) -> {
        switch (KeeperException.Code.get(code)) {
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                LOGGER.info("Registered successfully: " + serverId);
                break;
            case NODEEXISTS:
                LOGGER.warn("Already registered: " + serverId);
                break;
            default:
                LOGGER.error("Something went wrong " + KeeperException.create(KeeperException.Code.get(code), path));
        }
    };

    private AsyncCallback.StatCallback statusUpdateCallback = (code, path, context, stat) -> {
        switch (KeeperException.Code.get(code)) {
            case CONNECTIONLOSS:
                updateStatus((String)context);
                return;
        }
    };

    public Worker(String host) {
        this.host = host;
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private void register() {
        zooKeeper.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    private void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(host, 15000, this);
    }

    synchronized private void updateStatus(String status) {
        if (this.status == status) {
            zooKeeper.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOGGER.info(watchedEvent.toString() + ", " + host);
    }

    public static void main(String args[]) throws Exception {
        Worker worker = new Worker(args[0]);
        worker.startZooKeeper();

        worker.register();

        Thread.sleep(3000);
    }
}
