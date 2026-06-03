package com.hmall.common.config;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnClass(MQConfig.class)
@ConditionalOnProperty(
        prefix = "spring.rabbitmq.listener.simple.retry",
        name = "enabled",
        havingValue = "true"
)
public class MqConsumeErrorAutoConfiguration  {
    /**
     * 交换机
     */
    public static final String EXCHANGE_NAME = "error.direct";

    private static final String QUEUE_NAME =  "error.queue";
    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public Exchange errorExchange() {
        return ExchangeBuilder.directExchange(EXCHANGE_NAME).durable(true).build();
    }

    @Bean
    public Queue errorQueue()
    {
        return QueueBuilder.durable(appName + "." + QUEUE_NAME).build();
    }
    @Bean
    public Binding errorBinding(Exchange errorExchange, Queue errorQueue) {
        return BindingBuilder.bind(errorQueue).to(errorExchange).with(appName).noargs();
    }
    @Bean
    public RepublishMessageRecoverer republishMessageRecoverer(RabbitTemplate rabbitTemplate) {
        return new RepublishMessageRecoverer(rabbitTemplate, EXCHANGE_NAME, appName);
    }


}
