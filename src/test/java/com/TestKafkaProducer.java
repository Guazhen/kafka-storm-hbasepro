package com;




import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TestKafkaProducer {

    public static void TestProducer(){
        System.out.println("entering producer");
        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "192.168.0.123:9092");

        //请求时候需要验证
        props.put("acks", "all");

        //请求失败时候需要重试
        props.put("retries", 0);

        //内存缓存区大小
        props.put("buffer.memory", 33554432);

        //指定消息key序列化方式
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //指定消息本身的序列化方式
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>("calllog", Integer.toString(i), Integer.toString(i)));

        System.out.println("Message sent successfully");
        producer.close();
    }
//    public static void TestProducer(){
//        Properties props = new Properties();
//        //broker列表
//        props.put("metadata.broker.list", "192.168.0.130:9092");
//        //串行化
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        //
//        props.put("request.required.acks", "1");
//        // Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
//        //创建生产者配置对象
////        ProducerConfig config = new ProducerConfig(props);
//
//        //创建生产者
//         Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
////        kafka.javaapi.producer.Producer<String, String> producer = new Producer<String, String>(config);
//
//        KeyedMessage<String, String> msg = new KeyedMessage<String, String>("test","100" ,"hello world tomas100");
//        producer.send(msg);
//        System.out.println("send over!");
//    }

    public static void TestProducer1() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.131:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));

        producer.close();
    }

    public static void main(String[] args) {
        TestProducer1();
//        TestProducer();
    }
}
