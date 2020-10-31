package com.atguigu.networkFlowAnalysis;

import com.atguigu.domain.PvCount;
import com.atguigu.domain.UserBehavior;
import com.atguigu.networkFlowAnalysis.aggregate.PvCountAgg;
import com.atguigu.networkFlowAnalysis.domain.UvCount;
import com.atguigu.networkFlowAnalysis.process.TotalPvCountResult;
import com.atguigu.networkFlowAnalysis.window.PvCountWindowResult;
import com.atguigu.networkFlowAnalysis.window.UvCountResult;
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
 * @date: 2020/10/31 20:49
 */
public class UniqueVisitor {
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
        DataStream<UvCount> uvStream = dataStream.filter(t->t.behavior.equals("pv"))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        uvStream.print("pv count");
        env.execute("pv job");
    }
}
