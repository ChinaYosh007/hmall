package com.hmall.trade.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.client.CartClient;
import com.hmall.api.client.ItemClient;
import com.hmall.api.client.PayClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.api.dto.PayOrderDTO;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.UserContext;
import com.hmall.trade.constants.MQConstans;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.mapper.OrderMapper;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order> implements IOrderService {

    private final ItemClient itemClient;
    private final IOrderDetailService detailService;
    private final CartClient cartClient;
    private final RabbitTemplate rabbitTemplate;
    private final PayClient payClient;

    @Override
    @Transactional
    public Long createOrder(OrderFormDTO orderFormDTO) {
        // 1.订单数据
        Order order = new Order();
        // 1.1.查询商品
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        // 1.2.获取商品id和数量的Map
        Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();
        // 1.3.查询商品
        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("商品不存在");
        }
        // 1.4.基于商品价格、购买数量计算商品总价：totalFee
        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }
        order.setTotalFee(total);
        // 1.5.其它属性
        order.setPaymentType(orderFormDTO.getPaymentType());
        order.setUserId(UserContext.getUser());
        order.setStatus(1);
        order.setCreateTime(LocalDateTime.now());
        order.setCloseTime(LocalDateTime.now().plusMinutes(30));
        // 1.6.将Order写入数据库order表中
        save(order);

        // 2.保存订单详情
        List<OrderDetail> details = buildDetails(order.getId(), items, itemNumMap);
        detailService.saveBatch(details);

        try {
            // 3.1 准备消息体：必须包含 userId，因为异步线程拿不到 ThreadLocal
            Map<String, Object> msg = new HashMap<>();
            msg.put("userId", UserContext.getUser());
            msg.put("itemIds", itemIds);
            // 3.2 发送消息
            // 参数1：交换机名称；参数2：RoutingKey；参数3：消息内容
            rabbitTemplate.convertAndSend("trade.topic", "order.create", msg);
            log.info("发送清理购物车消息成功，订单ID: {}", order.getId());
        } catch (Exception e) {
            // MQ 发送失败不应该导致下单失败，所以这里记录日志即可（或者记录本地表后续重试）
            log.error("清理购物车消息发送失败，订单ID: {}", order.getId(), e);
        }
        log.info("订单创建成功，订单ID: {}, 用户ID: {}", order.getId(), UserContext.getUser());
        try {
            rabbitTemplate.convertAndSend(MQConstans.EXCHANGE_NAME, MQConstans.DELAY_ORDER_KEY, order.getId(), message -> {
                message.getMessageProperties().setDelayLong(15 * 60 * 1000L); // 15 分钟
                return message;
            });
        } catch (Exception e) {
            log.error("发送延迟关单消息失败，订单ID: {}", order.getId(), e);
        }
        return order.getId();
    }

    @Override
    public void cancelOrder(Long orderId) {
        Order order = getById(orderId);
        if (order == null || order.getStatus() != 1) return;

        PayOrderDTO payOrderDTO = payClient.queryPayOrderByBizOrderNo(orderId);
        if (payOrderDTO != null && payOrderDTO.getStatus() == 3) {
           markOrderPaySuccess(orderId);
        }
        else
        {
            /**
             * 1. 更新订单状态为取消
             * 2.更新库存
             */
            lambdaUpdate()
                    .set(Order::getStatus, 5)
                    .set(Order::getCloseTime, LocalDateTime.now())
                    .eq(Order::getId, orderId)
                    .eq(Order::getStatus, 1)
                    .update();

        }

        log.info("订单超时关闭，订单ID: {}", orderId);
    }

    @Override
    public void markOrderPaySuccess(Long orderId) {
        Order order = new Order();
        order.setId(orderId);
        order.setStatus(2);
        order.setPayTime(LocalDateTime.now());
        updateById(order);
    }

    private List<OrderDetail> buildDetails(Long orderId, List<ItemDTO> items, Map<Long, Integer> numMap) {
        List<OrderDetail> details = new ArrayList<>(items.size());
        for (ItemDTO item : items) {
            OrderDetail detail = new OrderDetail();
            detail.setName(item.getName());
            detail.setSpec(item.getSpec());
            detail.setPrice(item.getPrice());
            detail.setNum(numMap.get(item.getId()));
            detail.setItemId(item.getId());
            detail.setImage(item.getImage());
            detail.setOrderId(orderId);
            details.add(detail);
        }
        return details;
    }

}
