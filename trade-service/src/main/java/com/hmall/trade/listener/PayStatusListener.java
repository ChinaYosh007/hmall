package com.hmall.trade.listener;

import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.AllArgsConstructor;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class PayStatusListener {
    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "trade.pay.success.queue", durable = "true"),
            exchange = @Exchange(name = "pay.direct", durable = "true"),
            key = "pay.success"
    ))
    public void Paylistener(Long id) {
        Order byId = orderService.getById(id);
        if(byId == null || byId.getStatus() != 1) return;

        orderService.markOrderPaySuccess(id);
    }
}
