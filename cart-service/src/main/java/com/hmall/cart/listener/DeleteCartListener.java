package com.hmall.cart.listener;

import com.hmall.api.client.UserClient;
import com.hmall.cart.service.ICartService;
import com.hmall.cart.service.impl.CartServiceImpl;
import com.hmall.common.utils.UserContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
@AllArgsConstructor
public class DeleteCartListener {
    private final ICartService cartService;
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "cart.clear.queue", durable = "true"),
            exchange = @Exchange(name = "trade.topic", type = ExchangeTypes.TOPIC),
            key = "order.create"
    ))
    public void listenCartClear(Map<String, Object> msg) {
        try {
            // 1. 获取数据
            Long userId = Long.valueOf(msg.get("userId").toString());
            UserContext.setUser(userId);
            // 这里注意类型转换，MQ 序列化后集合类型可能需要处理
            Collection<Long> itemIds = (Collection<Long>) msg.get("itemIds");
            log.info("接收到清理购物车消息，用户ID: {}, 商品IDs: {}", userId, itemIds);

            // 2. 调用 service 执行清理
            cartService.removeByItemIds(itemIds);
        }
        finally {
            UserContext.removeUser();
        }
    }
}
