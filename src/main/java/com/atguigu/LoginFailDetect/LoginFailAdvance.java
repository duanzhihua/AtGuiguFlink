package com.atguigu.LoginFailDetect;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import com.atguigu.LoginFailDetect.process.LoginFailWarningAdvanceResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
 * 改进，2s登录失败
 */
public class LoginFailAdvance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据，乱序数据
        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\LoginLog.csv");
        //转换成样例类类型，并提取时间戳和watermark
        DataStream<LoginEvent> loginEventStream = inputStream.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] attrs = value.split(",");
                LoginEvent loginEvent = new LoginEvent(
                        Long.valueOf(attrs[0]),attrs[1],attrs[2],Long.valueOf(attrs[3])
                );
                return loginEvent;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent loginEvent) {
                return loginEvent.timesstamp*1000L;
            }
        });
        DataStream<LoginFailWarning> loginFailWarningDataStream = loginEventStream.keyBy((event)->event.userId)
                .process(new LoginFailWarningAdvanceResult());
        loginFailWarningDataStream.print();
        env.execute("login fail detect job");
    }
}
