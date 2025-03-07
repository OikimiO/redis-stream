package com.redis.stream.config;

import com.redis.stream.dto.User;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Collections;

@Configuration
public class RedisConfig {
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);

        // Jackson2JsonRedisSerializer로 User 객체 직렬화 및 역직렬화 설정
        Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<>(Object.class);
        redisTemplate.setValueSerializer(serializer);  // ValueSerializer로 설정

        redisTemplate.setKeySerializer(new StringRedisSerializer());  // Key는 String으로 설정
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());  // Hash의 Key는 String으로 설정
        redisTemplate.setHashValueSerializer(serializer);  // Hash의 Value는 객체 직렬화

        return redisTemplate;
    }

}
