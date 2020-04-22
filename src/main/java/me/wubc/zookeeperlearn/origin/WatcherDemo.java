package me.wubc.zookeeperlearn.origin;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wbc
 * @date 2020/04/22
 * @desc
 **/
public class WatcherDemo {

    public static final String WATCHER_DEMO = "/watcher/demo1";

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(ZkBaseConfig.CONNECT_URL, ZkBaseConfig.TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    System.out.println("连接成功");
                    countDownLatch.countDown();
                }
            }
        });

        countDownLatch.await();

        zooKeeper.create(WATCHER_DEMO, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Stat stat = zooKeeper.exists(WATCHER_DEMO, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    System.out.println(watchedEvent.getType() + "->" + watchedEvent.getPath());
                    // 重新注册watcher
                    zooKeeper.exists(watchedEvent.getPath(), true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        zooKeeper.setData(WATCHER_DEMO, "1".getBytes(), stat.getAversion());

    }
}
