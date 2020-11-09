package com.atguigu.domain;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 23:30
 */
public class OrderEvent {
    public Long orderId;
    public String enentType;
    public String txId;
    public Long timestamp;
    public OrderEvent(){}

    public OrderEvent(Long orderId, String enentType, String txId, Long timestamp) {
        this.orderId = orderId;
        this.enentType = enentType;
        this.txId = txId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", enentType='" + enentType + '\'' +
                ", txId='" + txId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }


}
