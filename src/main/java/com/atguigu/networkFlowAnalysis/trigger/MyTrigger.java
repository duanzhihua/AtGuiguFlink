package com.atguigu.networkFlowAnalysis.trigger;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @description: // 触发器，每来一条数据，直接触发窗口计算并清空窗口状态
 * @author: liyang
 * @date: 2020/10/31 21:53
 */
public class MyTrigger extends Trigger<Tuple2<String,Long>, TimeWindow> {

    @Override
    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
