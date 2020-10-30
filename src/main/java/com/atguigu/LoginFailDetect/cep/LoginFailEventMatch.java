package com.atguigu.LoginFailDetect.cep;

import com.atguigu.LoginFailDetect.domain.LoginEvent;
import com.atguigu.LoginFailDetect.domain.LoginFailWarning;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

// 实现自定义PatternSelectFunction
public class LoginFailEventMatch implements PatternSelectFunction<LoginEvent, LoginFailWarning> {

    @Override
    public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
            Iterator<LoginEvent> iterable = map.get("fail").iterator();
            LoginEvent first = iterable.next();
            LoginEvent second = iterable.next();
            LoginEvent third = iterable.next();
            return new LoginFailWarning(
                    first.userId,first.timesstamp,third.timesstamp,"login fail"
            );
    }
}