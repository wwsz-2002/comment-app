package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {

    @Autowired
    private  StringRedisTemplate stringRedisTemplate;

    /**
     * 设置缓存数据，并设置普通过期时间
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 设置缓存数据，并设置逻辑过期时间
     * @param key
     * @param value
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * queryWithMutex
     * 缓存穿透获取数据
     * @param id
     * @return
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> typeReturn , Class<ID> typeID , Function<ID, R> dbFallback
    ,Long time, TimeUnit unit) {
        //1.在redis中查缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2.内存命中
        if (StrUtil.isNotBlank(json)) {
            //直接返回
            return JSONUtil.toBean(json, typeReturn);
        }
        //内存命中，但是数据为空
        if (json != null) {
            return null;
        }
        //3.缓存未命中，查数据库
        R r = dbFallback.apply(id);
        //4.数据不存在，返回错误,并在redis中写入空值
        if (r == null) {
            stringRedisTemplate.opsForValue().set(keyPrefix + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //5.数据存在，写入缓存
        this.set(keyPrefix + id, r, time, unit);
        return r;
    }

    /**
     * 尝试获取锁
     *
     * @param key
     * @return
     */
    private boolean trylock(String key) {
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    //线程池
    private final static ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 逻辑过期的方式解决缓存击穿
     * @param id
     * @return
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix,String lockName,
                                            Long setexpireTime, ID id, Class<R> typeReturn,
                                            Function<ID, R> dbFallback) {//逻辑过期的方式解决缓存击穿
        //1.在redis中查缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2.内存未命中，直接返回
        if (StrUtil.isBlank(json)) {
            return null;
        }
        //3.缓存命中，看是否过期，需要将json先反序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, typeReturn);
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回信息
            return r;
        }
        //已过期，需要缓存重建
        //尝试获取互斥锁
        String lockkey = lockName + id;
        boolean isLock = trylock(lockkey);
        //是否获取互斥锁
        if(isLock){
            //获取成功，再次检查redis是否过期
            String shopjsonlock = stringRedisTemplate.opsForValue().get(keyPrefix + id);
            RedisData redisDatalock = JSONUtil.toBean(shopjsonlock, RedisData.class);
            LocalDateTime expireTimelock = redisDatalock.getExpireTime();
            if(expireTimelock.isAfter(LocalDateTime.now())){
                //未过期，直接返回信息
                JSONObject datalock = (JSONObject) redisData.getData();
                unLock(lockkey);
                return JSONUtil.toBean(datalock, typeReturn);
            }
            //开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //缓存重建
                try {
                    //访问数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(keyPrefix+id, r1, setexpireTime, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    unLock(lockkey);
                }
            });
        }
        //获取失败，直接返回数据
        return r;
    }
}
