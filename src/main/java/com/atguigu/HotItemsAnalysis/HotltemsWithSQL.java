package com.atguigu.HotItemsAnalysis;

import com.atguigu.domain.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class HotltemsWithSQL {
    public static void main(String[] args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\UserBehavior.csv");
        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] attrs = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.valueOf(attrs[0]),Long.valueOf(attrs[1]),
                        Integer.valueOf(attrs[2]),attrs[3],Long.valueOf(attrs[4]));
                //System.out.println(userBehavior);
                return userBehavior;
            }
        });

        //定义表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        //tableEnv.registerDataStream("userBehavior",dataStream);
        //基于DataStream创建Table
        Table dataTable = tableEnv.fromDataStream(dataStream);

        Table aggTable = dataTable.filter(String.valueOf(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return false;
            }
        }));
    }
}
