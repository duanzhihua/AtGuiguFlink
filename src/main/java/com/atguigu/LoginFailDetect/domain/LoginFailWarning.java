package com.atguigu.LoginFailDetect.domain;

public class LoginFailWarning {
    public Long userId;
    public Long firstFailTime;
    public Long lastFailTime;
    public String warningMessage;
    public LoginFailWarning(){}

    public LoginFailWarning(Long userId, Long firstFailTime, Long lastFailTime, String warningMessage) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMessage = warningMessage;
    }

    @Override
    public String toString() {
        return "LoginFailWarning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMessage='" + warningMessage + '\'' +
                '}';
    }
}
