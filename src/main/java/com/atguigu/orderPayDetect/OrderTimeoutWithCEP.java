package com.atguigu.orderPayDetect;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import com.atguigu.domain.OrderEvent;
import com.atguigu.domain.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.planner.expressions.Or;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description:
 * @author: liyang
 * @date: 2020/11/1 12:03
 */
public class OrderTimeoutWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> orderEventStream = env.readTextFile("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\OrderLog.csv")
                .map(data -> {
                    String[] arr = data.split(",");
                    return new OrderEvent(Long.valueOf(arr[0]), arr[1], arr[2], Long.valueOf(arr[3])); })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.timestamp*1000L;
                    }
                });
        //自定义ProcessFunction进行复杂时间的检测
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(t->t.orderId)
                .process(new OrderPayMathchResult());
        resultStream.print("payed");
        resultStream.getSideOutput(new OutputTag<OrderResult>("timeout"){}).print("timeout");
        env.execute("order timeout job");
    }
}

class OrderPayMathchResult extends KeyedProcessFunction<Long,OrderEvent, OrderResult>{
    private ValueState<Boolean> isCreatedState;
    private ValueState<Boolean> isPayedState;
    private ValueState<Long> timeTsState;
    //定义侧输出流
    private OutputTag<OrderResult> orderResultOutputTag = new OutputTag<OrderResult>("timeout"){};
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isCreatedState", Types.BOOLEAN,false));
        isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isPayed",Types.BOOLEAN,false));
        timeTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeTsState",Types.LONG,0L));

    }

    @Override
    public void processElement(OrderEvent value, KeyedProcessFunction<Long,OrderEvent, OrderResult>.Context ctx, Collector<OrderResult> out) throws Exception {
        Boolean isPayed = isPayedState.value();
        Boolean isCreated = isCreatedState.value();
        Long timerTs = timeTsState.value();
        System.out.println(isPayed);
        System.out.println(isCreated);
        System.out.println(timerTs);
        //判断当前订单时提交订单还是已支付
        //1.来的时create要判断是否已支付
        if(value.enentType.equals("create")){
            if (isPayed){
                //1.1如果已经支付过了，正常支付，输出匹配成功的结果
                out.collect(new OrderResult(value.orderId,"payed successfully"));
                //已经处理完毕，清空状态和定时器
                isCreatedState.clear();
                isPayedState.clear();
                timeTsState.clear();
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerTs);
            }else {
                //1.2如果还没有支付，注册定时器，等待15分钟
                Long ts = value.timestamp*1000L+900*1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                //更新状态
                timeTsState.update(ts);
                isCreatedState.update(true);
            }
        }
        //2.如果当前来的是pay，要判断是否create过
        else if(value.enentType.equals("pay")){
            if(isCreated){
                //2.1 如果已经create，匹配成功，还要判断pay支付的时间是否超过了定时器时间
                if(value.timestamp*1000L<timerTs){
                    //2.1.1正常
                    out.collect(new OrderResult(value.orderId,"payed successfully"));
                }else {
                    //2.1.2已经超时，输出超时
                    ctx.output(orderResultOutputTag,new OrderResult(value.orderId,"payed timeout"));
                }
                //necessary
                isCreatedState.clear();
                isPayedState.clear();
                timeTsState.clear();
                ctx.timerService().deleteEventTimeTimer(timerTs);
            }
        }else {
            //2.2如果create没有来，注册定时器，等到pay的时间就可以
            ctx.timerService().registerEventTimeTimer(value.timestamp*1000L);
            timeTsState.update(value.timestamp*1000L);
            isPayedState.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long,OrderEvent, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        if(isPayedState.value()){
            //1.pay来了，但么有create
            ctx.output(orderResultOutputTag,new OrderResult(ctx.getCurrentKey(),"payed but not found create log"));
        }else {
            //2.create来了，但没有pay
            ctx.output(orderResultOutputTag,new OrderResult(ctx.getCurrentKey(),"order timeout"));
        }

        //清空状态
        isCreatedState.clear();
        isPayedState.clear();
        timeTsState.clear();
    }
}