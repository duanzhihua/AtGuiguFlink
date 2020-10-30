package com.atguigu;

import com.atguigu.domain.Score;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.Rand;

import java.util.Random;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 20:22
 */
public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Score> dataStreamSource = env.fromElements(
                new Score("Li","English",95),
                new Score("Li","math",96),
                new Score("Liu", "Math", 91),
                new Score("Wang", "Math", 92),
                new Score("Li", "Math", 85),
                new Score("Wang", "English", 88)
        );
        dataStreamSource.print();
        dataStreamSource.keyBy("name").sum("score").print("keyby");
        env.execute("satrt");
        Random random = new Random();
        System.out.println(random.nextInt(4)+","+random.nextInt(4));
        System.out.println(random.nextInt(4)+","+random.nextInt(4));
    }
}
