package com.atguigu.networkFlowAnalysis.domain;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 21:24
 */
public class UvCount {
    public Long windowEnd;
    public Long count;

    public UvCount(){}

    public UvCount(Long windowEnd, Long count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "UvCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
