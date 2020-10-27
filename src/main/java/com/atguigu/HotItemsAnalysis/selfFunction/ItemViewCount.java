package com.atguigu.HotItemsAnalysis.selfFunction;

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
}
