package me.wubc.zookeeperlearn.curator;

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

    public static final String CONNECT_URL = "127.0.0.1:2181";
    public static final String NAME_SPACE = "curator";
    public static final String NODE_PATH = "/demo/node1";

    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_URL)
                .sessionTimeoutMs(400)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace(NAME_SPACE)
                .build();

        // 开启连接
        curatorFramework.start();

        // 创建节点，并赋值
        curatorFramework.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(NODE_PATH, "0".getBytes());
        // 设置节点值
        curatorFramework.setData().forPath(NODE_PATH, "1".getBytes());

        // 或值值并将节点信息存放到stat
        Stat stat = new Stat();
        curatorFramework.getData().storingStatIn(stat).forPath(NODE_PATH);

        curatorFramework.close();

    }

}
