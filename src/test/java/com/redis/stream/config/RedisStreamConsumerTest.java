package com.redis.stream.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest
public class RedisStreamConsumerTest {

    @Autowired
    public RedisOperator redisOperator;

    @Test
    void executeRedis(){
        Map<Object, Object> messageContent = new HashMap<>();
        messageContent.put("content", "Hello, Redis Stream!");

        redisOperator.publish("mystream", messageContent);
    }
}
