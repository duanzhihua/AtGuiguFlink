package com.atguigu.networkFlowAnalysis.process;

import com.atguigu.HotItemsAnalysis.domain.ItemViewCount;
import com.atguigu.networkFlowAnalysis.domain.PageViewCount;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 23:07
 */
public class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount,String> {
    //解决刷屏的bug，乱序数据
    private transient MapState<String,Long> pageViewCountMapState;
    private int topSize;
    public TopNHotPages(int topSize){
        this.topSize = topSize;
    }
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        pageViewCountMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Long>("pageViewCount-Map",String.class,Long.class)
        );
    }
    @Override
    public void processElement(PageViewCount pageViewCount, Context ctx, Collector<String> out) throws Exception {
        pageViewCountMapState.put(pageViewCount.url,pageViewCount.count);
        ctx.timerService().registerEventTimeTimer(pageViewCount.windowEnd+1);
        //另外注册一个定时器，一分钟之后触发，这是窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
        ctx.timerService().registerEventTimeTimer(pageViewCount.windowEnd+60000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
        if(timestamp==ctx.getCurrentKey()+60000L){
            pageViewCountMapState.clear();
            return;
        }
        List<Tuple2<String,Long>> allPageViewCounts = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter = pageViewCountMapState.entries().iterator();
        while(iter.hasNext()){
            Map.Entry<String,Long> entry = iter.next();
            allPageViewCounts.add(Tuple2.of(entry.getKey(),entry.getValue()));
        }
        //提前清空状态
        //pageViewCountMapState.clear();
        Collections.sort(allPageViewCounts, new Comparator<Tuple2<String, Long>>() {
            @Override
            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                return (int)(o1.f1 - o2.f1);
            }
        });
        Collections.reverse(allPageViewCounts);
        List<Tuple2<String,Long>> sorted = null;
        if(allPageViewCounts.size()>topSize)
            sorted = allPageViewCounts.subList(0,topSize);
        else
            sorted = allPageViewCounts;
        StringBuilder result = new StringBuilder();
        result.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n");
        int count = 0;
        for(Tuple2<String,Long> t:sorted){
            result.append("No").append(count++).append(": \t")
                    .append("页面URL = ").append(t.f0).append("\t")
                    .append("热门度 = ").append(t.f1).append("\n");
        }
        result.append("================================================");
        out.collect(result.toString());
    }
}
