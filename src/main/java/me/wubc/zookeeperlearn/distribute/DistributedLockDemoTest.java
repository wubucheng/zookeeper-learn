package me.wubc.zookeeperlearn.distribute;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wbc
 * @date 2020/04/24
 * @desc
 **/
public class DistributedLockDemoTest {

    public static void main( String[] args ) throws IOException {
        CountDownLatch countDownLatch=new CountDownLatch(10);
        for(int i=0;i<10;i++){
            new Thread(()->{
                try {
                    countDownLatch.await();
                    DistributedLockDemo distributedLock=new DistributedLockDemo();
                    distributedLock.lock(); //获得锁
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            },"Thread-"+i).start();
            countDownLatch.countDown();
        }
        System.in.read();
    }

}
