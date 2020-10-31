package com.atguigu.networkFlowAnalysis;

import com.atguigu.networkFlowAnalysis.aggregate.PageCountAgg;
import com.atguigu.networkFlowAnalysis.domain.ApacheLogEvent;
import com.atguigu.networkFlowAnalysis.domain.PageViewCount;
import com.atguigu.networkFlowAnalysis.process.TopNHotPages;
import com.atguigu.networkFlowAnalysis.window.PageViewCountWindowResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.swing.text.html.StyleSheet;
import java.text.SimpleDateFormat;

/**
 * @description: 每隔5s，输出 最近十分钟内访问的前N个URL
 * @author: liyang
 * @date: 2020/10/30 22:53
 */
public class HotPagesNetWorkFLow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\apache.log");

        DataStream<ApacheLogEvent> dataStream = inputStream.map(data->{
            String[] attrs = data.split(" ");
            System.out.println(attrs.length);
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long ts = sdf.parse(attrs[3]).getTime();
            ApacheLogEvent apacheLogEvent = new ApacheLogEvent(attrs[0], attrs[1], ts, attrs[5], attrs[6]);
            return apacheLogEvent;
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.timestamp;
            }
        });

        SingleOutputStreamOperator<PageViewCount> aggStream = dataStream.filter(t->t.method.equals("GET"))
                .keyBy(t->t.url)
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .allowedLateness(Time.milliseconds(1))//处理乱序数据
                //.sideOutputLateData(new OutputTag<ApacheLogEvent>("late"))
                .aggregate(new PageCountAgg(),new PageViewCountWindowResult());

        //排序输出
        DataStream<String> reultStream = aggStream.keyBy(t->t.windowEnd)
                .process(new TopNHotPages(5));
        dataStream.print("data");
        aggStream.print("agg");
        //aggStream.getSideOutput(new OutputTag<ApacheLogEvent>("late")).print("late");
        reultStream.print();
        env.execute("hot pages job");
    }
}
