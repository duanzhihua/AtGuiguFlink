package com.atguigu.domain;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 23:32
 */
public class OrderResult {
    public Long orderId;
    public String resultMsg;
    public OrderResult(){}

    public OrderResult(Long orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }
}
