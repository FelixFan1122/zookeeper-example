package org.apache.zookeeper.example;

import org.apache.zookeeper.*;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Client implements Watcher {
    private final String host;
    private ZooKeeper zooKeeper;

    public Client(String host) {
        this.host = host;
    }

    private String queueCommand(String command) throws Exception {
        String name = "";
        while (true) {
            try {
                name = zooKeeper.create("/tasks/task-", command.getBytes(), OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.NodeExistsException ex) {
                throw new Exception(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException ex) {

            }
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
        Client client = new Client(args[0]);

        client.startZooKeeper();

        System.out.println("Created " + client.queueCommand(args[1]));
    }
}
