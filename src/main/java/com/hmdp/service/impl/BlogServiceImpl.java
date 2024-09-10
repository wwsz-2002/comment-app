package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询热门博客
     *
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        for (Blog blog : records) {
            queryBlogUser(blog);
            queryBlogLike(blog);
        }
        return Result.ok(records);
    }

    /**
     * 根据博客id查询博客详情
     *
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        //1. 查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        //2. 查询blog相关的用户
        queryBlogUser(blog);//根据blog中的信息查询作者信息并填装
        queryBlogLike(blog);
        return Result.ok(blog);
    }

    /**
     * 当前blog是否被当前用户点过赞
     *
     * @param blog
     */
    private void queryBlogLike(Blog blog) {
        if (UserHolder.getUser() == null) {
            return;
        }
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        //判断该用户是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    /*
     * 博客点赞
     * @param id
     */
    @Override
    public void likeBlog(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //判断当前用户是否登录，如果未登录就不查询点赞数据
        if (UserHolder.getUser() == null) {
            return;
        }
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        //判断该用户是否点赞
        Double member = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //未点赞，可以点赞
        if (member == null) {
            //数据库点赞数+1
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();
            if (success) {
                //redis保存点赞信息到zset,将时间戳作为分数，后续进行点赞排行榜
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {//已点赞，取消点赞
            //数据库点赞数-1
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();
            if (success) {
                //redis取消点赞信息
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
    }

    /**
     * 查询点赞排行榜（时间排序的top5）
     *
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        //查询点赞top5 zrange key 0 4
        String key = BLOG_LIKED_KEY + id;
        //获取点赞用户id,也就是key，不需要获取value也就是时间戳
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //解析出用户id
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //stream流，将数据处理为long型，使用list集合收集数据
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", top5);
        //根据用户id查询用户
        List<UserDTO> users = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD( id ," + idStr + ")").list()
                //处理user，去除敏感信息
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回
        return Result.ok(users);
    }

    /**
     * 根据blog中的信息查询作者信息并填装
     *
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
