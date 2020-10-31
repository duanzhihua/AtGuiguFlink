package com.atguigu.domain;

/**
 * @description: 统计用户行为是pv个数的输出类
 * @author: liyang
 * @date: 2020/10/31 13:14
 */
public class PvCount {
    public Long windowEnd;
    public Long count;
    public PvCount(){}

    public PvCount(Long windowEnd, Long count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "PvCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
