package com.hmall.common.config;

import com.hmall.common.utils.RabbitMqHelper;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@ConditionalOnClass(RabbitTemplate.class)
@Configuration
@Slf4j
public class MQConfig implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnsCallback,RabbitTemplateCustomizer {

    @Bean
    public MessageConverter rabbitMQConfig() {
        return new Jackson2JsonMessageConverter();
    }
    @Bean
    public RabbitMqHelper rabbitMqHelper(RabbitTemplate rabbitTemplate) {
        return new RabbitMqHelper(rabbitTemplate);
    }
    @Override
    public void confirm(@Nullable CorrelationData correlationData, boolean ack, @Nullable String cause) {

        if (!ack)
        {
            log.error("消息发送到交换机失败，correlationData: {}, cause: {}", correlationData, cause);
        }
    }

    @Override
    public void returnedMessage(ReturnedMessage returned) {

       Object delay = returned.getMessage().getMessageProperties().getHeader("x-delay");
       if(delay != null) return;
       log.error("消息路由到队列失败，exchange: {}, routingKey: {}, message: {}",
               returned.getExchange(), returned.getRoutingKey(), returned.getMessage());
    }
    @Override
    public void customize(RabbitTemplate rabbitTemplate) {
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnsCallback(this);
    }
}
