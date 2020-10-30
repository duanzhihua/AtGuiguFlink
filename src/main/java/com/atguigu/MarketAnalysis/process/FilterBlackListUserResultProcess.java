package com.atguigu.MarketAnalysis.process;

import com.atguigu.MarketAnalysis.domain.AdClickLog;
import com.atguigu.MarketAnalysis.domain.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 20:52
 */
public class FilterBlackListUserResultProcess extends ProcessFunction<AdClickLog, BlackListUserWarning> {
    //用到了对应的状态，上面已经keyBy了，是针对同一个用户+同一个广告的次数，用valueState
    //定义了状态，保存用户对广告点击量，每天0点定时清空状态二点时间戳
    private transient ValueState<Long> countState;
    private transient ValueState<Long> resetTimerState;
    //报警的数据侧输出流只需要输出一次就行了，标记当前用户是否已经进出黑名单
    private transient ValueState<Boolean> isBlackState;
    private Long maxCount;

    public FilterBlackListUserResultProcess(Long maxCount) {
        this.maxCount = maxCount;
    }


    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.TYPE));
        resetTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("reset-ts",Long.TYPE));
        isBlackState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isBlack",Boolean.TYPE));
    }
    @Override
    public void processElement(AdClickLog value, Context context, Collector<BlackListUserWarning> out) throws Exception {
        long curCount = 0;
        if(countState.value()!=null){
            curCount = countState.value();
        }
        //判断只要是第一个数据来了，直接注册0点的清空状态定时器
        if(curCount == 0){
            //明天0点，北京时间
            long ts = (context.timerService().currentProcessingTime()/(1000*24*60*60)+1)*(24*60*60*1000)-8*60*60*1000;
            resetTimerState.update(ts);
            context.timerService().registerEventTimeTimer(ts);
        }
        //判断count值是否已经达到定义的阈值，如果超过就输出到黑名单
        if(curCount>=maxCount){
            // 判断是否已经在黑名单里，没有的话才输出侧输出流
            if(isBlackState.value()==null){
                isBlackState.update(true);
                out.collect(new BlackListUserWarning(value.userId, value.adId, value.province, "Click ad over" + maxCount + " times today"));
            }else {
                if (!isBlackState.value() && isBlackState.value() != null) {
                    isBlackState.update(true);
                    out.collect(new BlackListUserWarning(value.userId, value.adId,value.province,"Click ad over" + maxCount + " times today"));
                }
                return;
            }
        }
        countState.update(curCount+1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<BlackListUserWarning> out) throws Exception {
        if(timestamp == resetTimerState.value()){
            isBlackState.clear();
            countState.clear();
        }
    }
}
