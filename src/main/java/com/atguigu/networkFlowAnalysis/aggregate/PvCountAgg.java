package com.atguigu.networkFlowAnalysis.aggregate;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @description: 统计户行为是pv的个数
 * @author: liyang
 * @date: 2020/10/31 13:33
 */
public class PvCountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
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
