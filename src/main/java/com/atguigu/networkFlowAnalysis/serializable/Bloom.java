package com.atguigu.networkFlowAnalysis.serializable;

import java.io.Serializable;

/**
 * @description: // 自定义一个布隆过滤器，主要就是一个位图和hash函数
 * @author: liyang
 * @date: 2020/10/31 22:09
 */
public class Bloom implements Serializable {
    private Long cap; // 默认cap应该是2的整次幂
    public Bloom(Long cap){
        this.cap = cap;
    }
    public Long hash(String value, int seed){
        Long result = 0L;
        for(int i=0;i<value.length();i++){
            result = result*seed + value.charAt(i);
        }
        return (cap - 1)&result;
    }
}
