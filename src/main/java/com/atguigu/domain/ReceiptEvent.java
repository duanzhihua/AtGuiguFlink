package com.atguigu.domain;

/**
 * @description:  定义到账事件样例类
 * @author: liyang
 * @date: 2020/11/1 15:08
 */
public class ReceiptEvent {
    public String txId;
    public String payChannel;
    public Long timestamp;

    public ReceiptEvent(){}

    public ReceiptEvent(String txId, String payChannel, Long timestamp) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
