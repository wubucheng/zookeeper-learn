package me.wubc.zookeeperlearn.distribute;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author wbc
 * @date 2020/04/25
 * @desc curator实现分布式锁
 **/
public class CuratorDistributeLock {

    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZkBaseConfig.CONNECT_URL)
                .sessionTimeoutMs(ZkBaseConfig.TIME_OUT)
                .retryPolicy(new ExponentialBackoffRetry(3000, 3))
                .namespace(ZkBaseConfig.CURATOR_NAME_SPACE)
                .build();
        InterProcessMutex interProcessMutex = new InterProcessMutex(curatorFramework, "/locks");
        try {
            interProcessMutex.acquire();
        } catch (Exception e) {
            interProcessMutex.release();
            e.printStackTrace();
        }
        ;
    }

}
