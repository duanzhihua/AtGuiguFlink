package com.atguigu.HotItemsAnalysis.domain;

public class ItemViewCount {
    public Long itemId;
    public Long windowEnd;
    public Long count;

    public ItemViewCount(){}
    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
