package com.redis.stream.config;

import com.redis.stream.dto.User;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisOperator {
    private static final long MAX_NUMBER_FETCH = 300;

    private final RateLimiter rateLimiter = RateLimiter.create(200);  // 초당 200개 메시지 생성

    private final RedisTemplate<String, User> redisTemplate;

    public void objectPublish(String streamKey){
        rateLimiter.acquire();  // 메시지를 보내기 전에 RateLimiter로부터 대기

        ObjectRecord<String, User> record = StreamRecords.newRecord()
                .in(streamKey)
                .ofObject(new User("night", "angel"));

        redisTemplate.opsForStream().add(record);
    }

    public void publish(String streamKey, Map<Object, Object> messageContent){
        rateLimiter.acquire();  // 메시지를 보내기 전에 RateLimiter로부터 대기

        // MapRecord 생성
        MapRecord<String, Object, Object> record = MapRecord.create(streamKey, messageContent);

        // 스트림에 메시지 추가
        redisTemplate.opsForStream().add(record);
    }

    // RedisOperator :: 기본 StreamMessageListenerContainer 생성
    public StreamMessageListenerContainer createStreamMessageListenerContainer(){
        RedisMappingContext ctx = new RedisMappingContext();
        ctx.setInitialEntitySet(Collections.singleton(User.class));
        ObjectHashMapper objectHashMapper = new ObjectHashMapper(new MappingRedisConverter(ctx));

        return StreamMessageListenerContainer.create(
                this.redisTemplate.getConnectionFactory(),
                StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions.builder()
                        //.hashKeySerializer(new StringRedisSerializer())
                        //.hashValueSerializer(new StringRedisSerializer())
                        .objectMapper(objectHashMapper)
                        .pollTimeout(Duration.ofSeconds(2))
                        .build()
        );
    }

    /** XACK **/
    public void ackStream(String streamKey, ObjectRecord<String, User> message){
        log.info("XACK 진행중 ... ");

        this.redisTemplate.opsForStream().acknowledge(streamKey, message);
    }

    /** XDEL **/
    public void deleteStream(String streamKey, ObjectRecord<String, User> message) {
        log.info("XDEL 진행중 ... ");

        this.redisTemplate.opsForStream().delete(streamKey, message.getId());
    }

    /** XCLAIM **/
    public void claimStream(PendingMessage pendingMessage, String consumerName){
        RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                .getConnectionFactory().getConnection().getNativeConnection();

        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .add(pendingMessage.getIdAsString())
                .add(pendingMessage.getGroupName())
                .add(consumerName)
                .add("20")
                .add(pendingMessage.getIdAsString());

        commands.dispatch(CommandType.XCLAIM, new StatusOutput(StringCodec.UTF8), args);
    }

    /** XRANGE 조회 **/
    // Range 조회를 활용한 message 단 건 조회
    public MapRecord<String, Object, Object> findStreamMessageById(String streamKey, String id){
        List<MapRecord<String, Object, Object>> mapRecordList = this.findStreamMessageByRange(streamKey, id, id);
        if(mapRecordList.isEmpty()) return null;
        return mapRecordList.get(0);
    }

    public List<MapRecord<String, Object, Object>> findStreamMessageByRange(String streamKey, String startId, String endId){
        return this.redisTemplate.opsForStream().range(streamKey, Range.closed(startId, endId));
    }

    /** Consumer Group */
    public void createStreamConsumerGroup(String streamKey, String consumerGroupName){
        // Stream이 존재 하지 않으면, MKSTREAM 옵션을 통해 만들고, ConsumerGroup또한 생성한다
        if (Boolean.FALSE.equals(this.redisTemplate.hasKey(streamKey))){
            RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                    .getConnectionFactory()
                    .getConnection()
                    .getNativeConnection();

            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(CommandKeyword.CREATE)
                    .add(streamKey)
                    .add(consumerGroupName)
                    .add("0")
                    .add("MKSTREAM");

            commands.dispatch(CommandType.XGROUP, new StatusOutput(StringCodec.UTF8), args);
        }
        // Stream 존재시, ConsumerGroup 존재 여부 확인 후 ConsumerGroupd을 생성한다
        else{
            if(!isStreamConsumerGroupExist(streamKey, consumerGroupName)){
                this.redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), consumerGroupName);
            }
        }
    }

    // ConsumerGroup 존재 여부 확인
    public boolean isStreamConsumerGroupExist(String streamKey, String consumerGroupName){
        Iterator<StreamInfo.XInfoGroup> iterator = this.redisTemplate
                .opsForStream().groups(streamKey).stream().iterator();

        while(iterator.hasNext()){
            StreamInfo.XInfoGroup xInfoGroup = iterator.next();
            if(xInfoGroup.groupName().equals(consumerGroupName)){
                return true;
            }
        }
        return false;
    }

    public PendingMessages findStreamPendingMessages(String streamKey, String consumerGroupName) {
        return redisTemplate.opsForStream().pending(streamKey,
                consumerGroupName, Range.unbounded(), MAX_NUMBER_FETCH);
    }

    public Object getRedisValue(String errorCount, String idAsString) {
        return 0;
    }

    public void increaseRedisValue(String errorCount, String idAsString) {

    }
}
