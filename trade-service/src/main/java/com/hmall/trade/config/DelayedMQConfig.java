package com.hmall.trade.config;

import com.hmall.trade.constants.MQConstans;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class DelayedMQConfig {

    @Bean
    public CustomExchange delayedExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(MQConstans.EXCHANGE_NAME, "x-delayed-message", true, false, args);
    }

    @Bean
    public Queue delayOrderQueue() {
        return new Queue(MQConstans.QUEUE_NAME, true);
    }

    @Bean
    public Binding delayOrderBinding(Queue delayOrderQueue, CustomExchange delayedExchange) {
        return BindingBuilder.bind(delayOrderQueue).to(delayedExchange).with(MQConstans.DELAY_ORDER_KEY).noargs();
    }
}
