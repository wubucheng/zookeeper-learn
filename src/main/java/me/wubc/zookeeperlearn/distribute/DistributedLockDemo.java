package me.wubc.zookeeperlearn.distribute;

import me.wubc.zookeeperlearn.config.ZkBaseConfig;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author wbc
 * @date 2020/4/23
 * @desc zookeper原生方式实现分布式锁
 */
public class DistributedLockDemo implements Lock, Watcher {

    private ZooKeeper zooKeeper = null;
    private static final String ROOT_LOCK = "/locks";
    private String WAIT_LOCK;
    private String CURRENT_LOCK;
    private CountDownLatch countDownLatch;

    public DistributedLockDemo() {
        try {
            zooKeeper = new ZooKeeper(ZkBaseConfig.CONNECT_URL, ZkBaseConfig.TIME_OUT, this);
            Stat stat = zooKeeper.exists(ROOT_LOCK, true);
            if (stat == null) {
                // 当前节点不存在则创建
                zooKeeper.create(ROOT_LOCK, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if (this.tryLock()) {
            return;
        }
        waitForLock(WAIT_LOCK);


    }

    private boolean waitForLock(String prev) {
        try {
            // 需要注册watcher，对
            Stat stat = zooKeeper.exists(prev, true);
            if (stat != null) {
                System.out.println(Thread.currentThread().getName() + "->等待获取锁" + ROOT_LOCK + "/" + prev + "释放");
                CountDownLatch countDownLatch = new CountDownLatch(1);
                countDownLatch.await();
                // process的执行才会走到下面来
                System.out.println(Thread.currentThread().getName() + "->获取锁成功");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    /**
     * 尝试获取锁
     * 思想：多个客户端创建临时有序节点，节点集合中编号最小的节点的持有者，表示获取到锁。
     * 锁用完后，删除节点，表示释放锁。而剩下节点最小的也获取到锁，以此列推。子节点集合中各个节点只监听编号比自己小的节点
     *
     * @return
     */
    @Override
    public boolean tryLock() {
        try {
            CURRENT_LOCK = zooKeeper.create(ROOT_LOCK + "/", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + "->" + CURRENT_LOCK + ",尝试获取锁");

            List<String> childrenNodes = zooKeeper.getChildren(ROOT_LOCK, false);
            // 从小到大排序
            TreeSet<Object> treeSet = new TreeSet<>();
            for (String childrenNode : childrenNodes) {
                treeSet.add(ROOT_LOCK + "/" + childrenNode);
            }

            // 自己创建的顺序节点，是集合中的第一个
            if (CURRENT_LOCK.equals(treeSet.first())) {
                System.out.println(Thread.currentThread().getName() + "线程获取到锁");
                return true;
            }

            // 获取到比CURRENT_LOCK节点小的
            SortedSet<Object> lessSet = treeSet.headSet(CURRENT_LOCK);
            if (!lessSet.isEmpty()) {
                // 返回最后一个
                // 获取锁失败的节点获取当前节点上一个顺序节点，对此节点注册监听，当节点删除的时候通知当前节点
                WAIT_LOCK = (String) lessSet.last();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        System.out.println(Thread.currentThread().getName() + "->释放锁" + CURRENT_LOCK);
        try {
            zooKeeper.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (this.countDownLatch != null && Event.EventType.NodeDeleted.equals(watchedEvent.getType())) {
            // CURRENT_LOCK表示的节点被删除
            this.countDownLatch.countDown();
        }
    }
}
