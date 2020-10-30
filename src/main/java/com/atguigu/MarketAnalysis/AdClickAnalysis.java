package com.atguigu.MarketAnalysis;

import com.atguigu.MarketAnalysis.aggregate.AdCountAgg;
import com.atguigu.MarketAnalysis.domain.AdClickCountByProvince;
import com.atguigu.MarketAnalysis.domain.AdClickLog;
import com.atguigu.MarketAnalysis.domain.BlackListUserWarning;
import com.atguigu.MarketAnalysis.process.FilterBlackListUserResult;
import com.atguigu.MarketAnalysis.process.FilterBlackListUserResultProcess;
import com.atguigu.MarketAnalysis.window.AdCountWindowResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description:   对页面点击次数进行检测，对于异常的点击拉入黑名单
 * @author: liyang
 * @date: 2020/10/30 13:17
 */
public class AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputStram = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\AdClickLog.csv");
        //转换成样例类，提取时间和watermark
        DataStream<AdClickLog> dataStream = inputStram.map(data->{
                String[] attrs = data.split(",");
                AdClickLog adClickLog = new AdClickLog(
                        Long.valueOf(attrs[0]),Long.valueOf(attrs[1]),
                        attrs[2],attrs[3],Long.valueOf(attrs[4]));
                return adClickLog;
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickLog>() {
            @Override
            public long extractAscendingTimestamp(AdClickLog adClickLog) {  //升序数据不需要考虑乱序
                return adClickLog.timestamp*1000L;
            }
        });

        //插入一步过滤操作，并将有刷单行为的用户输出的侧输出流(黑名单报警)
        SingleOutputStreamOperator<BlackListUserWarning> filterBlackListUserStream = dataStream
                .keyBy(new KeySelector<AdClickLog, Tuple2<Long,Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickLog adClickLog) throws Exception {
                        return Tuple2.of(adClickLog.userId,adClickLog.adId);
                    }
                })//同一个用户，同一个广告
                .process(new FilterBlackListUserResult(100L));
        //开窗聚合统计
        DataStream<AdClickCountByProvince> adCountResultStream = filterBlackListUserStream.keyBy(event->event.province)
                .timeWindow(Time.days(1),Time.seconds(5)).aggregate(new AdCountAgg(),new AdCountWindowResult());

        adCountResultStream.print("countResult");
        filterBlackListUserStream.print("warning");
        env.execute("ad count statistics job");

    }
}
