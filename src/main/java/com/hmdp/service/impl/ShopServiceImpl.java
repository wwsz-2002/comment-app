package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient cacheClient;


    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿同时解决缓存穿透
//        Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //使用工具类解决缓存穿透问题
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, Long.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //使用工具类解决缓存击穿问题，逻辑过期
//        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, LOCK_SHOP_KEY, 10L, id, Shop.class, this::getById);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //6.返回
        return Result.ok(shop);
    }

    @Override
    @Transactional//事务
    public Result updateShop(Shop shop) {
        //判断id是否为空
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

}
