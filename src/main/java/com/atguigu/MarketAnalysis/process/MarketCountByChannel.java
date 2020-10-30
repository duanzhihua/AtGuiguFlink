package com.atguigu.MarketAnalysis.process;

import com.atguigu.MarketAnalysis.domain.MarketUserBehavior;
import com.atguigu.MarketAnalysis.domain.MarketViewCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @description: 自定义ProcessWindowFunction
 * @author: liyang
 * @date: 2020/10/30 13:23
 */
public class MarketCountByChannel extends ProcessWindowFunction<MarketUserBehavior, MarketViewCount, Tuple2<String,String>, TimeWindow> {


    @Override
    public void process(Tuple2<String, String> key, Context ctx, Iterable<MarketUserBehavior> input, Collector<MarketViewCount> out) throws Exception {
        String start = new Timestamp(ctx.window().getStart()).toString();
        String end = new Timestamp(ctx.window().getEnd()).toString();
        String channel = key.f0;
        String behavior = key.f1;
        Long count = 0L;
        Iterator<MarketUserBehavior> iterable = input.iterator();
        while (iterable.hasNext()) {
            iterable.next();
            count++;
        }
        out.collect(new MarketViewCount(start,end,channel,behavior,count));
    }
}
