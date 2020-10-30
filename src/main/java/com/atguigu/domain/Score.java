package com.atguigu.domain;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 20:33
 */
public class Score {
    public String name;
    public String coure;
    public float score;
    public Score(){}
    public Score(String name, String coure, float score) {
        this.name = name;
        this.coure = coure;
        this.score = score;
    }

    @Override
    public String toString() {
        return "score{" +
                "name='" + name + '\'' +
                ", coure='" + coure + '\'' +
                ", score=" + score +
                '}';
    }
}
