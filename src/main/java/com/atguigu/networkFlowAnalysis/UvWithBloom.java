package com.atguigu.networkFlowAnalysis;

import com.atguigu.domain.UserBehavior;
import com.atguigu.networkFlowAnalysis.domain.UvCount;
import com.atguigu.networkFlowAnalysis.process.UvCountWithBloom;
import com.atguigu.networkFlowAnalysis.trigger.MyTrigger;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 21:43
 */
public class UvWithBloom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\UserBehavior.csv");

        // 转换成样例类类型并提取时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(data -> {
                String[] arr = data.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.valueOf(arr[0]), Long.valueOf(arr[1]), Integer.valueOf(arr[2]), arr[3], Long.valueOf(arr[4]));
                return userBehavior;
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.timestamp*1000L;
                    }
                });

        DataStream<UvCount> uvStream = dataStream
                .filter(t->t.behavior.equals("pv"))
                .map(t->{return Tuple2.of("uv",t.userId);})
                .keyBy(t->t.f0)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())    // 自定义触发器
                .process(new UvCountWithBloom());

        uvStream.print("uvCount");

        env.execute("uv with bloom job");
    }
}
