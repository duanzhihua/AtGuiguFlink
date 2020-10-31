package com.atguigu.networkFlowAnalysis.window;

import com.atguigu.networkFlowAnalysis.domain.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 23:04
 */
public class PageViewCountWindowResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
        Long end = timeWindow.getEnd();
        out.collect(new PageViewCount(key,end,input.iterator().next()));
    }
}
