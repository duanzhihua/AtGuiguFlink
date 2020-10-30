package com.atguigu.HotItemsAnalysis.window;

import com.atguigu.HotItemsAnalysis.domain.ItemViewCount;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ItemViewWindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> aggregateResult, Collector<ItemViewCount> collector) throws Exception {
        Long itemId = tuple.getField(0);
        Long windowEnd = timeWindow.getEnd();
        Long count = aggregateResult.iterator().next();
        collector.collect(new ItemViewCount(itemId,windowEnd,count));
    }
}
