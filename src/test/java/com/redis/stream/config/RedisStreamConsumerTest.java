package com.redis.stream.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest
public class RedisStreamConsumerTest {

    @Autowired
    public RedisOperator redisOperator;

    @Autowired
    public RedisStreamConsumer redisStreamConsumer;


    @Test
    void executeRedis1() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            Map<Object, Object> messageContent = new HashMap<>();
            messageContent.put("sequence"+i, "Hello, Redis Stream! My name is Kim nice to meet you, nice to meet you too." + i);

            redisOperator.publish("mystream", messageContent);
        }
    }

    @Test
    void executeRedis2() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            Map<Object, Object> messageContent = new HashMap<>();
            messageContent.put("content", "Hello, Redis Stream!" + i);

            //Thread.sleep(20);
            redisOperator.publish("mystream", messageContent);
        }
    }

    @Test
    void executeRedis3() throws InterruptedException {
        for (int i = 0; i < 300; i++) {
            Map<Object, Object> messageContent = new HashMap<>();
            messageContent.put("sequence"+i, "Hello, Redis Stream! Hello, Redis Stream! Hello, Redis Stream! ");
            for (int j = 0; j < 30; j++) {
                messageContent.put("content"+j, "Hello, Redis Stream! Hello, Redis Stream! Hello, Redis Stream! " + j);
            }

            //Thread.sleep(20);
            redisOperator.publish("mystream", messageContent);
            //redisOperator.publish("mystream2", messageContent);
        }
    }


    @Test
    void executeRedis4() throws InterruptedException {
        for (int i = 0; i < 300; i++) {
            Map<Object, Object> messageContent = new HashMap<>();
            messageContent.put("sequence"+i, "Hello, Redis Stream! Hello, Redis Stream! Hello, Redis Stream! ");
            for (int j = 0; j < 30; j++) {
                messageContent.put("content"+j, "Hello, Redis Stream! Hello, Redis Stream! Hello, Redis Stream! " + j);
            }

            //Thread.sleep(20);
            redisOperator.publish("mystream", messageContent);
            redisOperator.publish("mystream2", messageContent);
        }
    }

    @Test
    void executeRedis5() throws InterruptedException {
        redisStreamConsumer.processPendingMessage();
    }

    @Test
    void executeRedis6(){
        redisOperator.objectPublish("mystream");
    }

}
