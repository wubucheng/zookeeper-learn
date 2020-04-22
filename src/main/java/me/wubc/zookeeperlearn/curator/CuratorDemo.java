package me.wubc.zookeeperlearn.curator;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @author wbc
 * @date 2020/4/22
 * @desc 使用Curator连接zookeeper服务
 */
public class CuratorDemo {


    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZkBaseConfig.CONNECT_URL)
                .sessionTimeoutMs(ZkBaseConfig.TIME_OUT)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace(ZkBaseConfig.CURATOR_NAME_SPACE)
                .build();

        // 开启连接
        curatorFramework.start();

        // 创建节点，并赋值
        curatorFramework.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(ZkBaseConfig.NODE_PATH, "0".getBytes());
        // 设置节点值
        curatorFramework.setData().forPath(ZkBaseConfig.NODE_PATH, "1".getBytes());

        // 或值值并将节点信息存放到stat
        Stat stat = new Stat();
        curatorFramework.getData().storingStatIn(stat).forPath(ZkBaseConfig.NODE_PATH);

        curatorFramework.close();

    }

}
