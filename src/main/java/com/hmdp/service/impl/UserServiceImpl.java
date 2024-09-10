package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.如果手机号不合法，则返回错误信息
            return Result.fail("手机号不合法");
        }
        //3.如果手机号合法，则生成验证码
        String code = RandomUtil.randomNumbers(6);//生成6位随机数字
        //4.保存验证码到redis(手机号作key，验证码作值)
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("发送短信验证码成功，验证码：{}", code);
        //返回OK
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        //校验手机号和验证码
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            //如果手机号不合法，则返回错误信息
            return Result.fail("手机号不合法");
        }
        //2.在redis中校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + loginForm.getPhone());//从redis中获取验证码
        String code = loginForm.getCode();//从前端获取验证码
        if (cacheCode == null || !cacheCode.equals(code)) {
            //如果验证码不一致，则返回错误信息
            return Result.fail("验证码错误");
        }
        //3.手机号和验证码都一致，进入数据库根据手机号查询用户
        User user = query().eq("phone", loginForm.getPhone()).one();//mybatis代码
        //4.判断用户是否存在
        if (user == null) {
            //5.不存在，创建全新的用户
            user = createUserWithUser(loginForm.getPhone());
        }
        //6.存在，直接保存到redis中
        //6.1.随机生成token，作为登录凭证
        String token = UUID.randomUUID().toString(true);
        //6.2.将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>() ,
                CopyOptions.create().setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //6.3.存储到redis中
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL, TimeUnit.MINUTES);
        //7.返回token
        return Result.ok(token);
    }

    private User createUserWithUser(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user"+RandomUtil.randomString(8));//随机生成一个昵称
        //保存用户
        save(user);
        return user;
    }
}
