package com.atguigu.MarketAnalysis;

import com.atguigu.MarketAnalysis.domain.MarketUserBehavior;
import com.atguigu.MarketAnalysis.domain.MarketViewCount;
import com.atguigu.MarketAnalysis.process.MarketCountByChannel;
import com.atguigu.MarketAnalysis.source.SimulateSource;
import com.atguigu.utils.DateUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.util.Date;

/**
 * @description:   统计一些App的使用情况，包括对APP进行的操作
 * @author: liyang
 * @date: 2020/10/30 13:17
 */
public class AppMarketByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketUserBehavior> dataStream = env.addSource(new SimulateSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketUserBehavior marketUserBehavior) {
                        return marketUserBehavior.timestamp;
                    }
                });//升序

        DataStream<MarketViewCount> resultStream = dataStream.filter(d->d.behavior!=null)
                .keyBy(new KeySelector<MarketUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketUserBehavior value) throws Exception {
                        return Tuple2.of(value.channel,value.behavior);
                    }
                }).timeWindow(Time.days(1),Time.seconds(5))
                .process(new MarketCountByChannel());

        resultStream.print("diff channel:");
        env.execute("app market by chnnel job");

    }
}
