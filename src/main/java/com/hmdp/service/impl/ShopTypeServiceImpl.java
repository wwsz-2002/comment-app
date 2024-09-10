package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryForAll() {
        //查找是否在缓存
        String shoptypejson = stringRedisTemplate.opsForValue().get("cache:shop:type");
        //命中
        if (StrUtil.isNotBlank(shoptypejson)) {
            //直接返回
            return Result.ok(JSONUtil.toList(shoptypejson, ShopType.class));
        }
        //未命中,数据库查找
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //写入缓存
        stringRedisTemplate.opsForValue().set("cache:shop:type", JSONUtil.toJsonStr(shopTypes));
        //返回数据
        return Result.ok(shopTypes);
    }
}
