package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerConfig {
    public static KafkaProducer<String,String> getKafkaProducer(){
        Properties props = new Properties();
        String broker = "49.235.101.149:9092";
        //String borker = "49.235.101.149:9092,192.168.221.145:9092";
        props.put("bootstrap.servers", broker);
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        // kafka集群，briker-list
        props.put("acks","all");
        //props.put(ProducerConfig.ACKS_CONFIG,"all");
        // 重试次数
        props.put("retries",1);
        //props.put(ProducerConfig.RETRIES_CONFIG,1);
        props.put("batch.size",16384);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put("linger.ms",1);
        //props.put(ProducerConfig.LINGER_MS_CONFIG,1);
        props.put("buffer.memory",33554432);
        //props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);

        //key 和 value的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }
}
