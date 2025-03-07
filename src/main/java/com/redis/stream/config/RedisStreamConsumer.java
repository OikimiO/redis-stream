package com.redis.stream.config;

import com.redis.stream.dto.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisStreamConsumer implements StreamListener<String, ObjectRecord<String, User>>, InitializingBean, DisposableBean {

    private StreamMessageListenerContainer<String, ObjectRecord<String, User>> listenerContainer;
    private Subscription subscription;
    private Subscription subscription2;
    private final String streamKey = "mystream";
    private final String streamKey2 = "mystream2";
    private final String consumerGroupName = "consumerGroupName";
    private final String consumerGroupName2 = "consumerGroupName2";
    private final String consumerName = "consumerName";
    private final String consumerName2 = "consumerName2";
    // 위에 구현한 Redis Streamd에 필요한 기본 Command를 구현한 Component
    private final RedisOperator redisOperator;


    @Override
    public void destroy() throws Exception {
        if(this.subscription != null){
            this.subscription.cancel();
        }
        if(this.subscription2 != null){
            this.subscription2.cancel();
        }
        if(this.listenerContainer != null){
            this.listenerContainer.stop();
        }
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        // Consumer Group 설정
        this.redisOperator.createStreamConsumerGroup(streamKey, consumerGroupName);

        // StreamMessageListenerContainer 설정
        this.listenerContainer = this.redisOperator.createStreamMessageListenerContainer();

        //Subscription 설정
        this.subscription  = getReceive(consumerGroupName, consumerName, streamKey);
        this.subscription2 = getReceive(consumerGroupName2, consumerName2, streamKey2);

        // redis listen 시작
        this.listenerContainer.start();
    }

    private Subscription getReceive(String consumerGroupName, String consumerName, String streamKey) throws InterruptedException {
        Subscription receive = this.listenerContainer.receive(
                Consumer.from(consumerGroupName, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                this
        );

        // 2초 마다, 정보 GET
        receive.await(Duration.ofSeconds(2));

        return receive;
    }

    @Override
    public void onMessage(ObjectRecord<String, User> message) {
        String stream = message.getStream();
        String consumerGroupName = "";
        if(stream.equals("mystream")){
            consumerGroupName = this.consumerGroupName;
        }

        if(stream.equals("mystream2")){
            consumerGroupName = this.consumerGroupName2;
        }


        User value = (User) message.getValue();
        // 처리할 로직 구현 ex) service.someServiceMethod();
        log.info("consumerGroupName : {}, message : {}, stream : {}", consumerGroupName, value, stream);


        this.redisOperator.ackStream(consumerGroupName, message);
        this.redisOperator.deleteStream(stream, message);
    }

    // @Scheduled(fixedRate = 10000) // 10초 마다 작동
    public void processPendingMessage(){
        //pendingMessage(consumerGroupName, consumerName, streamKey);
        //pendingMessage(consumerGroupName2, consumerName2, streamKey2);
    }
    /*
    private void pendingMessage(String consumerGroupName, String consumerName, String streamKey) {
        // Pending message 조회
        PendingMessages pendingMessages = this.redisOperator
                .findStreamPendingMessages(streamKey, consumerGroupName);

        for(PendingMessage pendingMessage : pendingMessages){
            // claim을 통해 consumer 변경
            this.redisOperator.claimStream(pendingMessage, consumerName);
            try{
                // Stream message 조회
                ObjectRecord<String, Object> messageToProcess = this.redisOperator
                        .findStreamMessageById(streamKey, pendingMessage.getIdAsString());
                if(messageToProcess == null){
                    log.info("존재하지 않는 메시지");
                }else{
                    // 해당 메시지 에러 발생 횟수 확인
                    int errorCount = (int) this.redisOperator
                            .getRedisValue("errorCount", pendingMessage.getIdAsString());

                    // 에러 5회이상 발생
                    if(errorCount >= 5){
                        log.info("재 처리 최대 시도 횟수 초과");
                    }

                    // 두개 이상의 consumer에게 delivered 된 메시지
                    else if(pendingMessage.getTotalDeliveryCount() >= 2){
                        log.info("최대 delivery 횟수 초과");
                    }else{
                        // 처리할 로직 구현 ex) service.someServiceMethod();
                    }
                    // ack stream
                    this.redisOperator.ackStream(consumerGroupName, messageToProcess);
                    this.redisOperator.deleteStream(streamKey, messageToProcess);
                }
            }catch (Exception e){
                // 해당 메시지 에러 발생 횟수 + 1
                this.redisOperator.increaseRedisValue("errorCount", pendingMessage.getIdAsString());
            }
        }
    }*/
}
