package com.atguigu.orderPayDetect;

import com.atguigu.domain.OrderEvent;
import com.atguigu.domain.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/31 23:35
 */
public class OrderTimeOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 0. 从文件中读取数据

        DataStream<OrderEvent> orderEventStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\OrderLog.csv")
                .map(data -> {
                String[] arr = data.split(",");
                return new OrderEvent(Long.valueOf(arr[0]), arr[1], arr[2], Long.valueOf(arr[3])); })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.timestamp*1000L;
                    }
                })
                .keyBy(t->t.orderId);


        // 1. 定义一个pattern
        Pattern<OrderEvent,OrderEvent> orderPayPattern = Pattern.begin("create").where(new IterativeCondition() {
                @Override
                public boolean filter(Object o, Context context) throws Exception {
                    OrderEvent orderEvent = (OrderEvent)o;
                    return ((OrderEvent) o).enentType.equals("create");
                }
            })
            .followedBy("pay")
            .where(new IterativeCondition() {
                @Override
                public boolean filter(Object o, Context context) throws Exception {
                    OrderEvent orderEvent = (OrderEvent)o;
                    return ((OrderEvent) o).enentType.equals("pay");
                }
            })
            .within(Time.minutes(15));

        // 2. 将pattern应用到数据流上，进行模式检测
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream, orderPayPattern);

        // 3. 定义侧输出流标签，用于处理超时事件
        OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<OrderResult>("orderTimeout"){};

        // 4. 调用select方法，提取并处理匹配的成功支付事件以及超时事件
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select( orderTimeoutOutputTag,
                new OrderTimeoutSelect(),
                new OrderPaySelect()
        );

        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute("order timeout job");
  }
}

// 实现自定义的PatternTimeoutFunction以及PatternSelectFunction
class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> map, long timestamp) throws Exception {
        Long timeoutOrderId = map.get("create").iterator().next().orderId;
        OrderResult orderResult = new OrderResult(timeoutOrderId, "timeout : +" + timestamp);
        return orderResult;
    }
}

class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
        Long payedOrderId = map.get("pay").iterator().next().orderId;
        return new OrderResult(payedOrderId, "payed successfully");
    }
}