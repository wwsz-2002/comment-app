package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * 关注用户或者取关
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //判断是关注还是取关
        if(isFollow){
            //是关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean save = save(follow);
            if (save) {
                //把关注信息放入redis集合 sadd userId followerUserId
                stringRedisTemplate.opsForSet().add("follows:" + userId, followUserId.toString());
            }
        } else {
            //是取关，删除数据
            boolean isRemove = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isRemove) {
                stringRedisTemplate.opsForSet().remove("follows:" + userId, followUserId.toString());
            }
        }
        return Result.ok();
    }


    /**
     * 判断是否关注
     * @param id
     * @return
     */
    @Override
    public Result isFollow(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //查询是否关注
        Integer count = query().eq("user_id", userId).eq("follow_user_id", id).count();
        //判断是否关注
        return Result.ok(count>0);
    }

    @Override
    public Result followCommons(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect("follows:" + id, "follows:" + userId);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
