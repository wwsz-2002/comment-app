package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.ILock;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private IVoucherOrderService object;

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));// 脚本位置
        SECKILL_SCRIPT.setResultType(Long.class);// 返回类型
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXCUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {//整个类一初始化完成就执行这个方法，启动阻塞队列的线程
        SECKILL_ORDER_EXCUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 线程方法，循环从消息队列中获取消息，异步处理订单
     */
    //重写线程方法
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的信息XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> voucher = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),//写组名和消费者名字
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),//每次获取一个，最多等待两秒
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())//指定读取的队列名称和位置
                    );
                    //2.判断消息获取是否成功
                    if (voucher == null || voucher.isEmpty()) {
                        //2.1如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //2.2获取成功，可以下单
                    //2.3解析订单中的信息
                    MapRecord<String, Object, Object> record = voucher.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.创建订单
                    handleVoucherOrder(voucherOrder);
                    //4.回到消息队列去确认，ACK确认，队列名字，组名，消息id
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePandingList();
                }
            }
        }

        private void handlePandingList() {
            while (true) {
                try {
                    //1.获取panding-list中的信息XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0  不在需要阻塞，最后的读取写为0
                    List<MapRecord<String, Object, Object>> voucher = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),//写组名和消费者名字
                            StreamReadOptions.empty().count(1),//每次获取一个
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))//指定读取的队列名称和位置
                    );
                    //2.判断消息获取是否成功
                    if (voucher == null || voucher.isEmpty()) {
                        //2.1如果获取失败，说明没有异常消息，直接退出循环
                        break;
                    }
                    //2.2获取成功，可以下单
                    //2.3解析订单中的信息
                    MapRecord<String, Object, Object> record = voucher.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.创建订单
                    handleVoucherOrder(voucherOrder);
                    //4.回到消息队列去确认，ACK确认，队列名字，组名，消息id
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();//如果当前没有数据，则会在此等待
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.info("阻塞队列出现异常");
//                }
//            }
//        }
        }

        /**
         * 尝试获取锁，获取成功则进入写入订单逻辑
         *
         * @param voucherOrder
         */
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //尝试创建锁对象
            Long userId = voucherOrder.getUserId();
            Long voucherId = voucherOrder.getVoucherId();
            //SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order" + userId + ":" + voucherId, stringRedisTemplate);
            RLock lock = redissonClient.getLock("lock:order" + userId + ":" + voucherId);
            //获取锁
            //boolean isLock = simpleRedisLock.tryLock(1200L);
            boolean isLock = lock.tryLock();
            if (!isLock) {
                //获取锁失败，返回错误或重试
                log.error("不允许重复下单");
            }
            try {
                //获取代理对象(事务)
                object.createVoucherOrder(voucherOrder);
            } finally {
                //释放锁
                //simpleRedisLock.unlock();
                lock.unlock();
            }
        }


        /**
         * 秒杀优惠券，查询是否可以秒杀
         * 使用reids消息队列实现异步更新
         *
         * @param voucherId
         * @return
         */
        @Override
        public Result seckIllVoucher(Long voucherId) {
            Long userId = UserHolder.getUser().getId();
            Long orderId = redisIdWorker.nextId("order");
            //1.执行lua脚本
            Long result = stringRedisTemplate.execute(
                    //0，下单成功，1，库存不足，2，重复下单或者订单不存在
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    //voucherId
                    voucherId.toString(),
                    //userid
                    userId.toString(),
                    //orderId
                    orderId.toString()
            );
            //2.根据lua脚本的结果判断是否有秒杀资格
            //2.1 秒杀资格结果不为0，没有购买资格
            if (result != 0) {
                return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
            }

            //2.5 提前获取代理对象
            object = (IVoucherOrderService) AopContext.currentProxy();
            //3. 返回订单id
            return Result.ok(orderId);
            //
        }

//    使用阻塞队列实现异步更新
//    @Override
//    public Result seckIllVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                //0，下单成功，1，库存不足，2，重复下单或者订单不存在
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                //voucherId
//                voucherId.toString(),
//                //userid
//                userId.toString());
//        //2.根据lua脚本的结果判断是否有秒杀资格
//        //2.1 秒杀资格结果不为0，没有购买资格
//        if (result != 0) {
//            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
//        }
//        //2.2 秒杀资格结果为0，有购买资格，把下单信息保存到阻塞队列中
//        Long orderId = redisIdWorker.nextId("order");
//        //2.3 创建对象储存订单信息
//        VoucherOrder order = new VoucherOrder();
//        order.setId(orderId);
//        order.setVoucherId(voucherId);
//        order.setUserId(userId);
//        //2.4 创建阻塞队列
//        orderTasks.add(order);
//        //2.5 提前获取代理对象
//        object = (IVoucherOrderService) AopContext.currentProxy();
//        //3. 返回订单id
//        return Result.ok(orderId);
//        //
//    }


//    @Override
//    public Result seckIllVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        //4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//
//        //尝试创建锁对象
//        //SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order" + userId + ":" + voucherId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order" + userId + ":" + voucherId);
//        //获取锁
//        //boolean isLock = simpleRedisLock.tryLock(1200L);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            //获取锁失败，返回错误或重试
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            //获取代理对象(事务)
//            IVoucherOrderService object = (IVoucherOrderService) AopContext.currentProxy();
//            return object.createVoucherOrder(voucherId);
//        } finally {
//            //释放锁
//            //simpleRedisLock.unlock();
//            lock.unlock();
//        }
//    }

        /**
         * 检测一人一单，然后写入订单和扣减库存
         *
         * @param order
         * @return
         */
        @Transactional//事务
        public void createVoucherOrder(VoucherOrder order) {
            Long voucherId = order.getVoucherId();
            //5.一人一单判断
            Long userId = order.getUserId();
            //5.1 查询订单
            int count = query().eq("user_id", userId)
                    .eq("voucher_id", voucherId).count();
            //5.2 判断是否已经购买过
            if (count > 0) {
                //用户已经购买过了
                log.error(userId + "该用户已经购买过");
                return;
            }
            //6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherId).gt("stock", 0)//gt表示大于
                    .update();
            if (!success) {
                //扣减失败
                log.error("库存不足");
                return;
            }
            //8.写入数据库
            save(order);
        }
    }
