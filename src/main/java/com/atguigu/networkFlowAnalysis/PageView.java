package com.atguigu.networkFlowAnalysis;

import com.atguigu.domain.PvCount;
import com.atguigu.domain.UserBehavior;
import com.atguigu.networkFlowAnalysis.aggregate.PvCountAgg;
import com.atguigu.networkFlowAnalysis.process.TotalPvCountResult;
import com.atguigu.networkFlowAnalysis.window.PvCountWindowResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 13:21
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\UserBehavior.csv");

        DataStream<UserBehavior> dataStream = inputStream.map(value->{
            String[] attrs = value.split(",");
            UserBehavior userBehavior = new UserBehavior(Long.valueOf(attrs[0]),Long.valueOf(attrs[1]),
                    Integer.valueOf(attrs[2]),attrs[3],Long.valueOf(attrs[4]));
            //System.out.println(userBehavior);
            return userBehavior;
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.timestamp*1000L;
            }
        });
        DataStream<PvCount> pvStream = dataStream.filter(t->t.behavior.equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
                    Random r = new Random();
                    @Override
                    public Tuple2<String,Long> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of(String.valueOf(r.nextInt(10)),1L);
                    }
                }).keyBy(t->t.f0).timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(),new PvCountWindowResult());
        DataStream<PvCount> totalPvStream = pvStream.keyBy(t->t.windowEnd)
                .process(new TotalPvCountResult());
        pvStream.print("pvCount");
        totalPvStream.print("total");
        env.execute("pv job");
    }
}
