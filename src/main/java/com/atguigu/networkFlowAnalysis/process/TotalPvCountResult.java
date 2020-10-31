package com.atguigu.networkFlowAnalysis.process;

import com.atguigu.domain.PvCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 17:55
 */
public class TotalPvCountResult extends KeyedProcessFunction<Long, PvCount,PvCount> {
    // 定义一个状态，保存当前所有count总和
    private transient ValueState<Long> totalCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("totalCount",Long.TYPE));
    }

    @Override
    public void processElement(PvCount value, Context ctx, Collector<PvCount> out) throws Exception {
        //每来一个数据，将count值叠加在当前的状态上
        Long currentTotalCount = 0L;
        if(totalCountState.value()!=null)
            currentTotalCount = totalCountState.value();
        totalCountState.update(currentTotalCount+value.count);

        //注册一个windowEnd+1ms后的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PvCount> out) throws Exception {
        Long totalPvCount = totalCountState.value();
        out.collect(new PvCount(ctx.getCurrentKey(), totalPvCount));
        totalCountState.clear();
    }
}
