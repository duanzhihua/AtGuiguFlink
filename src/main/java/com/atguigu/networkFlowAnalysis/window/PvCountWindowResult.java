package com.atguigu.networkFlowAnalysis.window;

import com.atguigu.domain.PvCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 13:37
 */
public class PvCountWindowResult implements WindowFunction<Long, PvCount,String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<PvCount> out) throws Exception {
        out.collect(new PvCount(timeWindow.getEnd(),input.iterator().next()));
    }
}
