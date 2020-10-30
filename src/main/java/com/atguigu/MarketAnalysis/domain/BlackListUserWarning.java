package com.atguigu.MarketAnalysis.domain;

public class BlackListUserWarning {
    public Long userId;
    public Long adId;
    public String msg;
    public String province;
    public BlackListUserWarning(){}

    public BlackListUserWarning(Long userId, Long adId,  String province,String msg) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
