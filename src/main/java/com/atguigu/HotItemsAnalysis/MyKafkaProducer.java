package com.atguigu.HotItemsAnalysis;

import com.atguigu.utils.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class MyKafkaProducer {
    public static void main(String[] args) throws IOException {
        String topic = "hotitems";
        KafkaProducer producer = KafkaProducerConfig.getKafkaProducer();
        BufferedReader buffer = new BufferedReader(new FileReader("D:\\IDEAWorkplace\\AtGuiguFlink\\src\\main\\resources\\UserBehavior.csv"));
        String line = null;
        while ((line = buffer.readLine()) != null){
            System.out.println(line);
            ProducerRecord record = new ProducerRecord(topic,line);
            producer.send(record);
        }
    }
}
