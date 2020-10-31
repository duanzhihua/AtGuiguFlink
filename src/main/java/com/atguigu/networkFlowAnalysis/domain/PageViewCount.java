package com.atguigu.networkFlowAnalysis.domain;

/**
 * @description: 窗口聚合结果样例类
 * @author: liyang
 * @date: 2020/10/30 22:57
 */
public class PageViewCount {
    public String url;
    public Long windowEnd;
    public Long count;
    public PageViewCount(){}

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
