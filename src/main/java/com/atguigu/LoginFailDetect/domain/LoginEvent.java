package com.atguigu.LoginFailDetect.domain;

public class LoginEvent {
    public Long userId;
    public String ip;
    public String eventType;
    public Long timesstamp;

    public LoginEvent(){}

    public LoginEvent(Long userId, String ip, String eventType, Long timesstamp) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.timesstamp = timesstamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timesstamp=" + timesstamp +
                '}';
    }
}
