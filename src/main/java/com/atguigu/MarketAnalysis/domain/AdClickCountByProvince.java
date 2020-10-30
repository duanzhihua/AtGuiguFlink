package com.atguigu.MarketAnalysis.domain;

public class AdClickCountByProvince {
    public String windowEnd;
    public String province;
    public Long count;
    public AdClickCountByProvince(){}

    public AdClickCountByProvince(String windowEnd, String provine, Long count) {
        this.windowEnd = windowEnd;
        this.province = provine;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdClickCountByProvince{" +
                "windowEnd='" + windowEnd + '\'' +
                ", provine='" + province + '\'' +
                ", count=" + count +
                '}';
    }
}
