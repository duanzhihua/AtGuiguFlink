package com.atguigu.MarketAnalysis.window;

import com.atguigu.MarketAnalysis.domain.AdClickCountByProvince;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AdCountWindowResult implements WindowFunction<Long, AdClickCountByProvince, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<AdClickCountByProvince> out) throws Exception {
        String end = new Timestamp(timeWindow.getEnd()).toString();
        out.collect(new AdClickCountByProvince(end,key,input.iterator().next()));
    }
}
