package com.atguigu.LoginFailDetect;

import com.atguigu.LoginFailDetect.cep.LoginFailEventMatch;
import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import sun.rmi.runtime.Log;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据，乱序数据
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

        Pattern<LoginEvent,LoginEvent> loginFailPattern = Pattern.begin("fail").where(new IterativeCondition() {

            @Override
            public boolean filter(Object o, Context context) throws Exception {
                LoginEvent value = (LoginEvent)o;
                return value.eventType.equals("fail");
            }
        }).times(3).consecutive()
                .within(Time.seconds(5));

        // 2. 将模式应用到数据流上，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.
                pattern(loginEventStream.keyBy((event)->event.userId), loginFailPattern);

        // 3. 检出符合模式的数据流，需要调用select
        DataStream<LoginFailWarning> failWarningDataStream = patternStream.select(new LoginFailEventMatch());
        failWarningDataStream.print();

        env.execute("login fail with cep job");
    }
}