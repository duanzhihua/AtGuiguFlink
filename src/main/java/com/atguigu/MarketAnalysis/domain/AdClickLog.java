package com.atguigu.MarketAnalysis.domain;

/*
 * 输入输出类
 */
public class AdClickLog {
    public Long userId;
    public Long adId;
    public String province;
    public String city;
    public Long timestamp;
    public Long count;
    public AdClickLog(){}

    public AdClickLog(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
        this.count = 1L;
    }

    @Override
    public String toString() {
        return "AdClickLog{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + timestamp +
                ",count=" + count + "}";
    }
}
