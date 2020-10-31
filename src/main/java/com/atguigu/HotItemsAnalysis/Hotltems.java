package com.atguigu.HotItemsAnalysis;

import com.atguigu.HotItemsAnalysis.aggregate.CountAgg;
import com.atguigu.HotItemsAnalysis.domain.ItemViewCount;
import com.atguigu.domain.UserBehavior;
import com.atguigu.HotItemsAnalysis.process.TopNHotItems;
import com.atguigu.HotItemsAnalysis.window.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Hotltems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        Properties props = new Properties();
        String broker = "49.235.101.149:9092";
        props.put("bootstrap.servers",broker);
        props.put("group.id","consumer-group");
        props.put("key.deserializeer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializeer","org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems",new SimpleStringSchema(),props));
        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] attrs = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.valueOf(attrs[0]),Long.valueOf(attrs[1]),
                        Integer.valueOf(attrs[2]),attrs[3],Long.valueOf(attrs[4]));
                //System.out.println(userBehavior);
                return userBehavior;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.timestamp*1000L;
            }
        });


        SingleOutputStreamOperator<ItemViewCount> aggStream = dataStream
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return value.behavior .equals("pv");
                    }
                }).keyBy("itemId").timeWindow(Time.hours(1),Time.minutes(5))    //设置滑动窗口及逆行统计
                .aggregate(new CountAgg(),new ItemViewWindowResult());          //windowStream的方法，有返回一个dataStream

        DataStream<String> resultStream = aggStream.keyBy("windowEnd")  //按窗口进行分组，手机当前窗口内商品count数据。
                                  .process(new TopNHotItems());      //自定义处理器

        dataStream.print("data");
        aggStream.print("agg");
        resultStream.print();
        env.execute(" hot items");
    }

}



