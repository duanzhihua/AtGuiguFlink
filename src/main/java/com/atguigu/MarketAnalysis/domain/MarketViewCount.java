package com.atguigu.MarketAnalysis.domain;

/**
 * @description: 输出数据样例类
 * @author: liyang
 * @date: 2020/10/30 13:27
 */
public class MarketViewCount {
    public String windowStart;
    public String windowEnd;
    public String channel;
    public String behavior;
    public Long count;

    public MarketViewCount(String windowStart, String windowEnd, String channel, String behavior, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.channel = channel;
        this.behavior = behavior;
        this.count = count;
    }

    @Override
    public String toString() {
        return "MarketViewCount{" +
                "windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", count=" + count +
                '}';
    }
}
