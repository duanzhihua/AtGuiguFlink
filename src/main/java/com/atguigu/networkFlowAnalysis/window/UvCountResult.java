package com.atguigu.networkFlowAnalysis.window;

import com.atguigu.domain.UserBehavior;
import com.atguigu.networkFlowAnalysis.domain.UvCount;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 21:24
 */
public class UvCountResult implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<UserBehavior> input, Collector<UvCount> out) throws Exception {
        List<Long> lists = new ArrayList<>();
        Iterator<UserBehavior> iterator = input.iterator();
        while (iterator.hasNext()){
            lists.add(iterator.next().userId);
        }
        out.collect(new UvCount(timeWindow.getEnd(),Long.valueOf(lists.size())));
    }
}
