package com.hmall.pay.controller;

import com.hmall.api.dto.PayOrderDTO;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.BeanUtils;
import com.hmall.pay.domain.dto.PayApplyDTO;
import com.hmall.pay.domain.dto.PayOrderFormDTO;
import com.hmall.pay.domain.po.PayOrder;
import com.hmall.pay.domain.vo.PayOrderVO;
import com.hmall.pay.enums.PayType;
import com.hmall.pay.service.IPayOrderService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "支付相关接口")
@RestController
@RequestMapping("pay-orders")
@RequiredArgsConstructor
public class PayController {

    private final IPayOrderService payOrderService;

    @ApiOperation("查询支付单")
    @GetMapping
    public List<PayOrderVO> queryPayOrders(){
        return BeanUtils.copyList(payOrderService.list(), PayOrderVO.class);
    }

    @ApiOperation("生成支付单")
    @PostMapping
    public String applyPayOrder(@RequestBody PayApplyDTO applyDTO){
        if(!PayType.BALANCE.equalsValue(applyDTO.getPayType())){
            // 目前只支持余额支付
            throw new BizIllegalException("抱歉，目前只支持余额支付");
        }
        return payOrderService.applyPayOrder(applyDTO);
    }

    @ApiOperation("尝试基于用户余额支付")
    @ApiImplicitParam(value = "支付单id", name = "id")
    @PostMapping("{id}")
    public Map<String, Object> tryPayOrderByBalance(@PathVariable("id") Long id, @RequestBody PayOrderFormDTO payOrderFormDTO){
        payOrderFormDTO.setId(id);
        payOrderService.tryPayOrderByBalance(payOrderFormDTO);
        Map<String, Object> result = new HashMap<>();
        result.put("status", "200"); // 必须给前端喂这个数据！
        return result;
    }
    @ApiOperation("根据id查询支付单")
    @GetMapping("/biz/{id}")
    public PayOrderDTO queryPayOrderByBizOrderNo(@PathVariable("id") Long id){
        PayOrder payOrder = payOrderService.lambdaQuery().eq(PayOrder::getBizOrderNo, id).one();
        return BeanUtils.copyBean(payOrder, PayOrderDTO.class);
    }
}
