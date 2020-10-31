package com.atguigu.networkFlowAnalysis.domain;

/**
 * @description: 输入数据样例类
 * @author: liyang
 * @date: 2020/10/30 22:52
 */
public class ApacheLogEvent {
    public String ip;
    public String userId;
    public Long timestamp;
    public String method;
    public String url;

    public ApacheLogEvent(){}

    public ApacheLogEvent(String ip, String userId, Long timestamp, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.timestamp = timestamp;
        this.method = method;
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
