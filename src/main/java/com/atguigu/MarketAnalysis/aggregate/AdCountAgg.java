package com.atguigu.MarketAnalysis.aggregate;

import com.atguigu.MarketAnalysis.domain.AdClickLog;
import com.atguigu.MarketAnalysis.domain.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AdCountAgg implements AggregateFunction<BlackListUserWarning,Long,Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(BlackListUserWarning value, Long accumulator) {
        return accumulator+1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a+b;
    }
}
