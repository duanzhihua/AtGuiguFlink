package com.atguigu.networkFlowAnalysis.aggregate;

import com.atguigu.networkFlowAnalysis.domain.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 23:01
 */
public class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent apacheLogEvent, Long accumulator) {
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
