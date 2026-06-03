package com.hmall.common.config;

import lombok.extern.slf4j.Slf4j;
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
public class MQConfig {

    @Bean
    public MessageConverter rabbitMQConfig() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplateCustomizer rabbitTemplateCustomizer() {
        return template -> {
            template.setMandatory(true);
            template.setConfirmCallback((correlationData, ack, cause) -> {
                if (!ack) {
                    log.error("消息发送到交换机失败，correlationData: {}, cause: {}", correlationData, cause);
                }
            });
            template.setReturnsCallback(returned -> {
                // x-delayed-message 插件在消息入队时会误触发 returns，通过 x-delay header 过滤
                Object delay = returned.getMessage().getMessageProperties().getHeader("x-delay");
                if (delay != null) return;
                log.error("消息路由到队列失败，exchange: {}, routingKey: {}, message: {}",
                    returned.getExchange(), returned.getRoutingKey(), returned.getMessage());
            });
        };
    }
}
