package com.atguigu.MarketAnalysis.domain;

/**
 * @description: 输入数据样例类
 * @author: liyang
 * @date: 2020/10/30 13:24
 */
public class MarketUserBehavior {
    public String userId;
    public String behavior;
    public String channel;
    public Long timestamp;

    public MarketUserBehavior(String userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MarketUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
