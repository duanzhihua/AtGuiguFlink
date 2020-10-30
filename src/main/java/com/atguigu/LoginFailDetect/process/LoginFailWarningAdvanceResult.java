package com.atguigu.LoginFailDetect.process;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class LoginFailWarningAdvanceResult extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
    private ListState<LoginEvent> loginFailListState;
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginfail-list",LoginEvent.class));
    }
    @Override
    public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context context, Collector<LoginFailWarning> out) throws Exception {
        if (loginEvent.eventType.equals("fail")) {
            Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
            if (iterator.hasNext()) {
                LoginEvent first = iterator.next();
                if (loginEvent.timesstamp < first.timesstamp+2) {
                    //如果在2s之内，输出报警
                    out.collect(
                            new LoginFailWarning(loginEvent.userId, first.timesstamp, loginEvent.timesstamp,
                                    "login fail 2 times in seconds")
                    );
                }
                //不管包不包经，当前处理都已完毕，将状态更新为最近一次登录失败
                loginFailListState.clear();
                loginFailListState.add(loginEvent);
            } else {
                //如果没有，直接把当前事件添加到liststate中
                loginFailListState.add(loginEvent);
            }
        } else {
            //登录成功,直接清空状态
            loginFailListState.clear();
        }
    }
}
