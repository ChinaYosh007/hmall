package com.hmall.trade.listener;

import com.hmall.trade.constants.MQConstans;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DelayOrderListener {

    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(value = @Queue(name = MQConstans.QUEUE_NAME),
            exchange = @Exchange(name = MQConstans.EXCHANGE_NAME,delayed = "true"),
            key = MQConstans.DELAY_ORDER_KEY
    ))
    public void handleDelayOrder(Long orderId) {
        log.info("收到延迟关单消息，订单ID: {}", orderId);
        orderService.cancelOrder(orderId);
    }
}
