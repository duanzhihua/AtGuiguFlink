package com.atguigu.LoginFailDetect.process;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginFailWarningResult extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
    private ListState<LoginEvent> loginFailListState;
    private ValueState<Long> timerTsState;
    private int failTimes;  //限制失败次数
    public LoginFailWarningResult(int failTimes){
        this.failTimes = failTimes;
    }
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<>("login-list", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.TYPE));
    }

    @Override
    public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long,LoginEvent,LoginFailWarning>.Context context, Collector<LoginFailWarning> collector) throws Exception {
        //判断当前登录事件是成功还是失败
        //System.out.println(loginEvent.toString());
        if(loginEvent.eventType .equals("fail")){
            loginFailListState.add(loginEvent);
            //如果没有定时器，那么注册一个2s后的定时器S
            if(timerTsState.value() == null){
                Long ts = loginEvent.timesstamp*1000L+2000L;
                context.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        } else {
            //如果是成功，那么直接清空状态和定时器，重新开始
            if(timerTsState.value() != null)
                context.timerService().deleteEventTimeTimer(timerTsState.value());
            loginFailListState.clear();
            timerTsState.clear();
        }
    }

    //定时器触发
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long,LoginEvent,LoginFailWarning>.OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<LoginEvent> allLoginFailList = new ArrayList<LoginEvent>();
        Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
        while(iterator.hasNext()){
            allLoginFailList.add(iterator.next());
        }
        //判断登录失败事件的个数，如果超过了上限，输出报警信息
        if(allLoginFailList.size()>=failTimes){
            out.collect(new LoginFailWarning(allLoginFailList.get(0).userId,
                    allLoginFailList.get(0).timesstamp,
                    allLoginFailList.get(allLoginFailList.size()-1).timesstamp,
                    "login fail in 2s for " + allLoginFailList.size()+" times"));
        }
        //清空状态
        loginFailListState.clear();
        timerTsState.clear();
    }
}
