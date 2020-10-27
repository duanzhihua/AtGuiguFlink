package com.atguigu.HotItemsAnalysis;

import com.atguigu.HotItemsAnalysis.selfFunction.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import javax.xml.crypto.Data;

public class Hotltems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<UserBehavior> stream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\UserBehavior.csv")
                            .map(new MapFunction<String, UserBehavior>() {
                                public UserBehavior map(String value) throws Exception {
                                    String[] lines = value.split(",");
                                    UserBehavior userBehavior = new UserBehavior(Long.valueOf(lines[0]),Long.valueOf(lines[1]),
                                            Integer.valueOf(lines[2]),lines[3],Long.valueOf(lines[4]));
                                    return userBehavior;
                                }
                            }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<UserBehavior>() {
                    @Nullable
                    public Watermark getCurrentWatermark() {
                        return null;
                    }

                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.timestamp*1000;
                    }
                });
        SingleOutputStreamOperator<ItemViewCount> aggStream = stream
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return value.behavior=="pv";
                    }
                }).keyBy("itemId").timeWindow(Time.hours(1),Time.minutes(5))    //设置滑动窗口及逆行统计
                .aggregate(new CountAgg(),new ItemViewWindowResult());          //windowStream的方法，有返回一个dataStream
        DataStream<String> resultStream = aggStream.keyBy("windowEnd")  //按窗口进行分组，手机当前窗口内商品count数据。
                                  .process(new TopNHotItems());      //自定义处理器
        resultStream.print("").setParallelism(1);
        env.execute(" hot items");
    }

}
