package com.atguigu.HotItemsAnalysis.selfFunction;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

/*
 *求某个窗口前N名的热门点击商品，key为窗口时间戳，输出为TopN的结果字符串
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount,String> {

    private ListState<ItemViewCount> itemState;
    private int topSize = 6;
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        //命名状态变量的名字和状态变量的类型
        ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<ItemViewCount>("itemState-state", ItemViewCount.class);
        ///定义状态变量
        itemState = getRuntimeContext().getListState(itemStateDesc);
    }

    @Override
    public void processElement(ItemViewCount input, KeyedProcessFunction<Tuple, ItemViewCount,String>.Context context, Collector<String> collector) throws Exception {
        //每条数据都保存到状态中
        itemState.add(input);
        //注册windowEnd+1 的EventTime timer，当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        //也就是当程序看到windowend+1的水位线watemark时，出发onTimer回调函数
        context.timerService().registerEventTimeTimer(input.windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount,String>.OnTimerContext context, Collector<String> out) throws Exception {
        //获取所有商品的点击量
        List<ItemViewCount> allItems = new ArrayList<ItemViewCount>();
        Iterator<ItemViewCount> iter = itemState.get().iterator();
        while (iter.hasNext()){
            allItems.add(iter.next());
        }
            itemState.clear();
        Collections.sort(allItems,new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o1.count - o2.count);
            }
        });
        Collections.reverse(allItems);
        List<ItemViewCount> sorted = allItems.subList(0,topSize);
        //将排名信息格式化为String，便于打印
        StringBuilder result = new StringBuilder();
        result.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n");
        //遍历结果列表中的每个ItemViewCount, 输出到一行
        for(int i=0;i<sorted.size();i++){
            ItemViewCount currentItemViewCount = sorted.get(i);
            result.append("No").append(i+1).append(":\t").append("商品ID = ").append(currentItemViewCount.itemId)
                    .append("\t").append("热门度 = ").append(currentItemViewCount.count).append("\n");
        }
        result.append("\n⬇=>=>=>=>=>=>=>=>=>=>=>=>==>=>=>=>=>=>=>=⬇\n\n");
        out.collect(result.toString());
    }
}
