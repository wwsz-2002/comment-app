package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private ShopServiceImpl shopService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private RedissonClient redissonClient;

    //生成一个简单的十个线程的线程池，用ExecutorService来执行
    ExecutorService es = Executors.newFixedThreadPool(500);


    @Test
    void testIdWorker() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(500);//倒计时器，等待500个线程结束
        Runnable task=()->{//编写线程任务，一个线程生成100个id
            for (int i = 0; i < 100; i++) {
                Long order = redisIdWorker.nextId("order");
                System.out.println("order = " + order);
            }
            latch.countDown();//线程执行完，计数器减一
        };
        //计时
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 500; i++) {//将线程提交500次，顺便计时
            es.submit(task);
        }
        latch.await();//等待倒计时器结束
        //结束计时
        System.out.println("time = " + (System.currentTimeMillis() - begin));
    }


    @Test//测试redisson
    void testRedisson() throws InterruptedException {
        Lock lock = redissonClient.getLock("anyLock");
        boolean b = lock.tryLock();
        if (b){
            try {
                System.out.println("执行业务逻辑");
            }finally {
                lock.unlock();
            }
        }
    }
}
