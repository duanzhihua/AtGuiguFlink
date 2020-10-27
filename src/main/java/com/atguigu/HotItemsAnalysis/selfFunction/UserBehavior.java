package com.atguigu.HotItemsAnalysis.selfFunction;

public class UserBehavior {
    public Long userId;
    public Long itemId;
    public int categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior(){}
    public UserBehavior(Long userId, Long itemId, int categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
