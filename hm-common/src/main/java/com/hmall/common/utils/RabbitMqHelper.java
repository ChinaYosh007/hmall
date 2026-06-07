package com.hmall.common.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RabbitMqHelper {
    private final RabbitTemplate rabbitTemplate;

    public void sendMessage(String exchange, String routingKey, Object msg)
    {
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, msg);
        } catch (Exception e) {
            log.error("exchange : {}, routingKey : {}, msg : {}", exchange, routingKey, msg, e);
        }

    }

    public   void sendDelayMessage(String exchange, String routingKey, Object msg, long  delay){
        MessagePostProcessor messagePostProcessor = message ->
        {
//            message.getMessageProperties().setExpiration(String.valueOf(delay));
            message.getMessageProperties().setDelayLong( delay);
            return message;
        };
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, msg, messagePostProcessor);
        } catch (Exception e) {
            log.error("exchange : {}, routingKey : {}, msg : {}", exchange, routingKey, msg, e);
        }

    }

    public  void sendMessageWithConfirm(String exchange, String routingKey, Object msg, int maxRetries){
        int current = 0;
        while (current++ < maxRetries) {
            try {
                rabbitTemplate.convertAndSend(exchange, routingKey, msg);
                return;
            } catch (Exception e) {
                log.warn("exchange : {}, routingKey : {}, msg : {}", exchange, routingKey, msg, e);
            }
        }
        log.error("消息发送失败，已达最大重试次数, exchange : {}, routingKey : {}, msg : {}, maxRetries : {}", exchange, routingKey, msg, maxRetries);
    }
}