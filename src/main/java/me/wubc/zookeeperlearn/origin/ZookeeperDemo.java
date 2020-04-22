package me.wubc.zookeeperlearn.origin;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wbc
 * @date 2020/4/22
 * @desc zookeeper原生方式连接zookeeper服务
 */
public class ZookeeperDemo {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(ZkBaseConfig.CONNECT_URL, ZkBaseConfig.TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // watcher事件回调
                System.out.println("event type: " + watchedEvent.getType());
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        System.out.println("Current state：" + zooKeeper.getState());

        // 创建节点:原生方式需要父节点创建好才能创建
        zooKeeper.create(ZkBaseConfig.ZK_NAME_SPACE + ZkBaseConfig.NODE_PATH, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Stat stat = new Stat();
        //获取节点值
        byte[] data = zooKeeper.getData(ZkBaseConfig.ZK_NAME_SPACE + ZkBaseConfig.NODE_PATH, null, stat);
        System.out.println("data is: " + new String(data));

        // 删除
        zooKeeper.delete(ZkBaseConfig.ZK_NAME_SPACE + ZkBaseConfig.NODE_PATH, stat.getVersion());

        zooKeeper.close();
        System.in.read();

    }

}
