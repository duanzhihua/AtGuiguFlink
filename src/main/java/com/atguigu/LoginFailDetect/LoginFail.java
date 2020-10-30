package com.atguigu.LoginFailDetect;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import com.atguigu.LoginFailDetect.process.LoginFailWarningResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
 * 在一段时间内，用户出现连续多次失败行为
 */

public class LoginFail {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据

        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\LoginLog.csv");
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

        DataStream<LoginFailWarning> loginFailWarningDataStream = loginEventStream.keyBy((event)->event.userId).process(new LoginFailWarningResult(2));
        loginFailWarningDataStream.print();
        env.execute("Login fail detect job");
    }
}
