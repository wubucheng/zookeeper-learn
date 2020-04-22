package me.wubc.zookeeperlearn.curator;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author wbc
 * @date 2020/04/22
 * @desc
 **/
public class CuratorWatcherDemo {

    public static final String PATH = "/demo1";

    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZkBaseConfig.CONNECT_URL)
                .sessionTimeoutMs(ZkBaseConfig.TIME_OUT)
                .retryPolicy(new ExponentialBackoffRetry(4000, 3))
                .namespace(ZkBaseConfig.CURATOR_NAME_SPACE)
                .build();

        curatorFramework.start();

        addListenerWithNodeCache(curatorFramework, PATH);
        addListenerWithPathChildCache(curatorFramework, PATH);
        addListenerWithTreeCache(curatorFramework, PATH);
        Thread.sleep(1000);
        System.in.read();
    }

    /**
     * 监听当前节点的watcher事件
     *
     * @param curatorFramework
     * @param path
     */
    public static void addListenerWithNodeCache(CuratorFramework curatorFramework, String path) throws Exception {
        NodeCache nodeCache = new NodeCache(curatorFramework, path);
        NodeCacheListener nodeCacheListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("当前节点接收到的事件：" + nodeCache.getCurrentData().getPath());
            }
        };

        // 将监听加入到监听列表
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }

    /**
     * 监听子节点watcher事件
     *
     * @param curatorFramework
     * @param path
     */
    public static void addListenerWithPathChildCache(CuratorFramework curatorFramework, String path) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, path, true);
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("监听子节点watcher事件：" + pathChildrenCacheEvent.getType());
            }
        };

        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
    }

    /**
     * 监听节点和子节点watcher事件
     *
     * @param curatorFramework
     * @param path
     * @throws Exception
     */
    public static void addListenerWithTreeCache(CuratorFramework curatorFramework, String path) throws Exception {
        TreeCache treeCache = new TreeCache(curatorFramework, path);
        TreeCacheListener treeCacheListener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("监听事件：" + treeCacheEvent.getType() + "->" + treeCacheEvent.getData().getPath());
            }
        };

        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }

}
