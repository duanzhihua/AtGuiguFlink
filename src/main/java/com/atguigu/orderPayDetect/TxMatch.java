package com.atguigu.orderPayDetect;

import com.atguigu.domain.OrderEvent;
import com.atguigu.domain.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description:
 * @author: liyang
 * @date: 2020/11/1 15:07
 */
public class TxMatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream<OrderEvent,String> orderEventStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\OrderLog.csv")
                .map(data -> {
                    String[] arr = data.split(",");
                    return new OrderEvent(Long.valueOf(arr[0]), arr[1], arr[2], Long.valueOf(arr[3])); })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.timestamp*1000L;
                    }
                })
                .filter(t->t.enentType.equals("pay"))
                .keyBy(t->t.txId);

        //读取到账事件数据
        KeyedStream<ReceiptEvent,String> receiptEventStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\ReceiptLog.csv")
                .map(data -> {
                    String[] arr = data.split(",");
                    return new ReceiptEvent(arr[0], arr[1], Long.valueOf(arr[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent receipt) {
                        return receipt.timestamp*1000L;
                    }
                })
                .keyBy(t->t.txId);

        //3.1合并两条流，进行处理

        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream
                .connect(receiptEventStream)
                .process(new TxPayMatchResult());
        resultStream.print("matched");
        resultStream.getSideOutput(new OutputTag<OrderEvent>("unmatchPay"){}).print("unmatchPay");
        resultStream.getSideOutput(new OutputTag<ReceiptEvent>("unmatchReceipt"){}).print("unmatchReceipt");
        /*
        //3.2 join处理
        DataStream<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream
                .intervalJoin(receiptEventStream)
                .between(Time.seconds(-3),Time.seconds(5))
                .process(new TxMatchWithJoinResult());
        resultStream.print("matched");
         */
        env.execute("tx match job");

    }
}

class TxPayMatchResult extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{
    //定义状态，保存当前交易对应的订单支付事件和到账事件
    private ValueState<OrderEvent> payEventState;
    private ValueState<ReceiptEvent> receiptEventState;
    //侧输出流标签
    private OutputTag<OrderEvent> unmatchPayEventTag = new OutputTag<OrderEvent>("unmatchPay"){};
    private OutputTag<ReceiptEvent> unmatchReceiptEventTag = new OutputTag<ReceiptEvent>("unmatchReceipt"){};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        payEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<OrderEvent>("payEventState", TypeInformation.of(OrderEvent.class))
        );
        receiptEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<ReceiptEvent>("receiptEventState", Types.POJO(ReceiptEvent.class))
        );
    }

    @Override
    public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        //订单支付已到，查看是否到账
        ReceiptEvent receipt = receiptEventState.value();
        if(receipt!=null){
            //如果已有receipt，正常输出
            out.collect(Tuple2.of(value,receipt));
            receiptEventState.clear();
            payEventState.clear();
        }else {
            //如果没有收到账，注册定时器开始等待5s;
            ctx.timerService().registerEventTimeTimer(value.timestamp*1000L + 5000L);
            //更新状态
            payEventState.update(value);
        }
    }

    @Override
    public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        //到账时间来了，要判断之前是否有pay事件
        OrderEvent pay = payEventState.value();
        if(pay!=null){
            //如果已有pay，正常输出
            out.collect(Tuple2.of(pay,value));
            receiptEventState.clear();
            payEventState.clear();
        }else{
            //如果没来，注册定时器开始等待3s;
            ctx.timerService().registerEventTimeTimer(value.timestamp*1000L + 3000L);
            //更新状态
            receiptEventState.update(value);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        //定时器触发，判断状态中哪个还存在，就代表另一个没来，输出到侧输出流
        if(payEventState.value()!=null){
            ctx.output(unmatchPayEventTag,payEventState.value());
        }
        if(receiptEventState.value()!=null){
            ctx.output(unmatchReceiptEventTag,receiptEventState.value());
        }
        receiptEventState.clear();
        payEventState.clear();
    }
}
class TxMatchWithJoinResult extends ProcessJoinFunction<OrderEvent,ReceiptEvent,Tuple2<OrderEvent,ReceiptEvent>>{
    //是能输出成功匹配的
    @Override
    public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        out.collect(Tuple2.of(orderEvent,receiptEvent));
    }
}