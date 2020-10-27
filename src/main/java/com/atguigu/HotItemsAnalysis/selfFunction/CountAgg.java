package com.atguigu.HotItemsAnalysis.selfFunction;

import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {

    public Long createAccumulator() {
        return 0L;
    }

    public Long add(UserBehavior value, Long accumulator) {
        return accumulator+1;
    }

    public Long getResult(Long accumulator) {
        return accumulator;
    }

    public Long merge(Long a, Long b) {
        return a+b;
    }
}
