package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 生成Redis唯一ID
 */

@Component
public class RedisIdWorker {

    // 开始时间戳
    private final static long BEGIN_TIMESTAMP = 1725321600L;
    // 序列号位数
    private final static long COUNT_BITS = 32;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 获取下一个ID
     * @param keyPrefix
     * @return
     */
    public Long nextId(String keyPrefix) {// keyPrefix接收不同的key值，用以区分不同业务的id
        //1. 生成时间戳
        long epochSecond = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        long timestamp = epochSecond - BEGIN_TIMESTAMP;
        //2. 生成序列号,利用redis的自增长实现
        //2.1 获取当前日期
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        //2.2 获取当前日期的key
        Long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);//通过每天更改key的日期来防止key的数值溢出问题
        //3. 拼接
        return timestamp << COUNT_BITS | count;// 左移32位，将时间戳左移32位，将序列号拼接到时间戳后面,符号位一直是0不用改
    }
}
